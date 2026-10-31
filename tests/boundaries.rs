use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use resource_monitor::{
    auth::Auth,
    domain::{now_ms, Batch, Sample},
    kubernetes::Kubernetes,
    store::Store,
    web::{self, App},
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::Semaphore;
use tower::ServiceExt;

async fn fixture() -> anyhow::Result<(
    tempfile::TempDir,
    App,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
)> {
    let dir = tempfile::tempdir()?;
    let state = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
    .route("/api/v1/nodes/{name}",get(|axum::extract::Path(name):axum::extract::Path<String>|async move {axum::Json(serde_json::json!({"metadata":{"name":name,"uid":if name=="alpha" {"00000000-0000-0000-0000-000000000001"} else {"00000000-0000-0000-0000-000000000002"},"labels":{"kubernetes.io/os":"linux","kubernetes.io/arch":"amd64"}},"status":{"nodeInfo":{"architecture":"amd64","operatingSystem":"linux","kernelVersion":"6.1"},"conditions":[{"type":"Ready","status":"True"}]}}))}))
    .route(
        "/api/user/",
        get({
            let state = state.clone();
            move || {
                let mode = state.load(Ordering::SeqCst);
                async move {
                    match mode {
                        0 => (
                            StatusCode::OK,
                            "{\"id\":1,\"services\":{\"monitor\":true}}".to_owned(),
                        ),
                        1 => (
                            StatusCode::OK,
                            "{\"id\":1,\"services\":{\"monitor\":false}}".to_owned(),
                        ),
                        2 => (StatusCode::UNAUTHORIZED, "{}".to_owned()),
                        3 => (
                            StatusCode::OK,
                            "{\"id\":1,\"services\":{\"monitor\":\"true\"}}".to_owned(),
                        ),
                        4 => (StatusCode::OK, "{".to_owned()),
                        5 => (StatusCode::OK, "x".repeat(65537)),
                        6 => (
                            StatusCode::OK,
                            "{\"id\":1,\"id\":2,\"services\":{\"monitor\":true}}".to_owned(),
                        ),
                        8 => (StatusCode::OK, r#"[{"id":7,"username":"admin","services":{"monitor":true}}]"#.to_owned()),
                        9 => (StatusCode::OK, r#"[{"id":7,"services":{"monitor":false}}]"#.to_owned()),
                        10 => (StatusCode::OK, "[]".to_owned()),
                        11 => (StatusCode::OK, r#"[{"id":1,"services":{"monitor":true}},{"id":2,"services":{"monitor":true}}]"#.to_owned()),
                        12 => (StatusCode::OK, r#"[{"id":1,"id":2,"services":{"monitor":true}}]"#.to_owned()),
                        13 => (StatusCode::OK, r#"[{"id":7,"services":{"monitor":"true"}}]"#.to_owned()),
                        _ => (StatusCode::SERVICE_UNAVAILABLE, "{}".to_owned()),
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let api_token = dir.path().join("api-token");
    std::fs::write(&api_token, "k".repeat(64))?;
    let native = Arc::new(BTreeMap::from([
        (
            "alpha".into(),
            "00000000-0000-0000-0000-000000000001".into(),
        ),
        (
            "gamma".into(),
            "00000000-0000-0000-0000-000000000002".into(),
        ),
    ]));
    let app = App {
        kube: Kubernetes {
            client: resource_monitor::config::client().build()?,
            base: format!("http://{addr}"),
            token_file: api_token.to_string_lossy().into(),
            namespace: "public-page".into(),
            service_account: "monitor-agent".into(),
            daemonset: "monitor-agent".into(),
            native,
            limit: Arc::new(Semaphore::new(8)),
        },
        auth: Auth {
            client: resource_monitor::config::client().build()?,
            url: format!("http://{addr}/api/user/"),
            limit: Arc::new(Semaphore::new(2)),
        },
        store: Store::open(
            dir.path()
                .join("db.sqlite3")
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("path"))?,
            0,
        )
        .await?,
        tokens: Arc::new(BTreeMap::from([
            ("alpha".into(), "a".repeat(64)),
            ("gamma".into(), "b".repeat(64)),
        ])),
        ingest_limit: Arc::new(Semaphore::new(4)),
        credential_gate: Default::default(),
        read_limit: Arc::new(Semaphore::new(4)),
        quota: Default::default(),
    };
    let nodes = vec![
        app.kube.live_node("alpha").await?,
        app.kube.live_node("gamma").await?,
    ];
    app.store.reconcile(&nodes).await?;
    Ok((dir, app, state, server))
}
#[tokio::test]
async fn live_auth_cookie_denial_revocation_and_malformed() -> anyhow::Result<()> {
    let (_dir, app, state, server) = fixture().await?;
    let router = web::public(app);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/nodes")
                .header("cookie", "access_token=pretend")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()["cache-control"], "no-store");
    for (mode, status) in [
        (0, 200),
        (1, 403),
        (2, 401),
        (3, 503),
        (4, 503),
        (5, 503),
        (6, 503),
        (7, 503),
        (8, 200),
        (9, 403),
        (10, 503),
        (11, 503),
        (12, 503),
        (13, 503),
    ] {
        state.store(mode, Ordering::SeqCst);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/nodes")
                    .header("authorization", "Bearer same-token")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status().as_u16(), status, "mode {mode}");
    }
    server.abort();
    Ok(())
}
fn sample(node: &str) -> Sample {
    Sample {
        node: node.into(),
        boot_id: uuid::Uuid::new_v4().to_string(),
        process_id: uuid::Uuid::new_v4().to_string(),
        seq: 0,
        timestamp_ms: now_ms(),
        gpu: Default::default(),
        metrics: BTreeMap::from([("cpu.used_percent".into(), Some(10.))]),
    }
}
#[tokio::test]
async fn ingest_listener_identity_batch_skew_quota_and_no_public_route() -> anyhow::Result<()> {
    let (_dir, app, _state, server) = fixture().await?;
    let public = web::public(app.clone());
    let private = web::private(app.clone());
    let response = public
        .oneshot(Request::post("/v1/samples").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let mut stale = sample("alpha");
    stale.timestamp_ms -= 121_000;
    let mut future = sample("alpha");
    future.timestamp_ms += 31_000;
    for (samples, token, status) in [
        (vec![sample("gamma")], "a".repeat(64), 400),
        (vec![sample("alpha")], "human".into(), 401),
        (vec![], "a".repeat(64), 400),
        (vec![sample("alpha"); 7], "a".repeat(64), 400),
        (vec![stale], "a".repeat(64), 400),
        (vec![future], "a".repeat(64), 400),
        (vec![sample("alpha")], "a".repeat(64), 204),
        (vec![sample("alpha")], "a".repeat(64), 429),
    ] {
        let response = private
            .clone()
            .oneshot(
                Request::post("/v1/samples")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(serde_json::to_vec(&Batch { samples })?))?,
            )
            .await?;
        assert_eq!(response.status().as_u16(), status);
    }
    let response = private
        .clone()
        .oneshot(
            Request::post("/v1/samples")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", "b".repeat(64)))
                .header("content-encoding", "gzip")
                .body(Body::from(serde_json::to_vec(&Batch {
                    samples: vec![sample("gamma")],
                })?))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let response = private
        .oneshot(
            Request::post("/v1/samples")
                .header("content-type", "application/json")
                .body(Body::from(" ".repeat(128 * 1024 + 1)))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    server.abort();
    Ok(())
}
#[tokio::test]
async fn query_bounds_and_fs_reserve() -> anyhow::Result<()> {
    let (_dir, app, _state, server) = fixture().await?;
    let router = web::public(app.clone());
    for uri in [
        "/api/history?node=alpha&hours=721",
        "/api/history?node=alpha&hours=0",
        "/api/history?node=forged&hours=1",
        "/api/history?node=alpha&hours=1&sql=drop",
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("authorization", "Bearer same")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    let mut store = app.store;
    store.reserve = u64::MAX;
    assert!(store.insert(&[sample("alpha")]).await.is_err());
    assert!(store.latest("alpha").await?.is_none());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn private_tls_valid_ca_wrong_ca_hostname_expiry_and_revocation() -> anyhow::Result<()> {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (_dir, app, _state, auth_server) = fixture().await?;
    let mut params = CertificateParams::new(vec![])?;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_key = KeyPair::generate()?;
    let ca = params.self_signed(&ca_key)?;
    let issuer = Issuer::new(params, ca_key);
    for expired in [false, true] {
        let mut leaf = CertificateParams::new(vec!["collector.test".into()])?;
        if expired {
            leaf.not_before = rcgen::date_time_ymd(2000, 1, 1);
            leaf.not_after = rcgen::date_time_ymd(2001, 1, 1);
        }
        let key = KeyPair::generate()?;
        let cert = leaf.signed_by(&key, &issuer)?;
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem(
            cert.pem().into_bytes(),
            key.serialize_pem().into_bytes(),
        )
        .await?;
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let handle = axum_server::Handle::new();
        let task = tokio::spawn(
            axum_server::from_tcp_rustls(listener, tls)
                .handle(handle.clone())
                .serve(web::private(app.clone()).into_make_service()),
        );
        let client = resource_monitor::config::client()
            .tls_built_in_root_certs(false)
            .add_root_certificate(reqwest::Certificate::from_pem(ca.pem().as_bytes())?)
            .resolve("collector.test", addr)
            .resolve("wrong.test", addr)
            .build()?;
        let url = format!("https://collector.test:{}/v1/samples", addr.port());
        let batch = Batch {
            samples: vec![sample("gamma")],
        };
        let response = client
            .post(&url)
            .bearer_auth("b".repeat(64))
            .json(&batch)
            .send()
            .await;
        if expired {
            assert!(response.is_err());
        } else {
            assert_eq!(response?.status(), StatusCode::NO_CONTENT);
            let response = client
                .post(&url)
                .bearer_auth("revoked-token")
                .json(&batch)
                .send()
                .await?;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(client
                .post(format!("https://wrong.test:{}/v1/samples", addr.port()))
                .json(&batch)
                .send()
                .await
                .is_err());
            let untrusted = resource_monitor::config::client()
                .resolve("collector.test", addr)
                .build()?;
            assert!(untrusted.post(&url).json(&batch).send().await.is_err());
        }
        handle.shutdown();
        task.await??;
    }
    auth_server.abort();
    Ok(())
}

#[tokio::test]
async fn concurrency_fails_closed_before_work() -> anyhow::Result<()> {
    let (_dir, app, _state, server) = fixture().await?;
    let _read = app.read_limit.acquire_many(4).await?;
    let response = web::public(app.clone())
        .oneshot(
            Request::builder()
                .uri("/api/nodes")
                .header("authorization", "Bearer same")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let _ingest = app.ingest_limit.acquire_many(4).await?;
    let response = web::private(app.clone())
        .oneshot(Request::post("/v1/samples").body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    let _auth = app.auth.limit.acquire_many(2).await?;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", "Bearer same".parse()?);
    assert_eq!(
        app.auth.check(&headers).await,
        Err(StatusCode::SERVICE_UNAVAILABLE)
    );
    server.abort();
    Ok(())
}
#[tokio::test]
async fn auth_timeout_and_no_redirect() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = Router::new()
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                "{}"
            }),
        )
        .route(
            "/redirect",
            get(|| async { axum::response::Redirect::temporary("/valid") }),
        )
        .route(
            "/valid",
            get(|| async { "{\"id\":1,\"services\":{\"monitor\":true}}" }),
        );
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("authorization", "Bearer same".parse()?);
    for path in ["redirect", "slow"] {
        let auth = Auth {
            client: resource_monitor::config::client().build()?,
            url: format!("http://{addr}/{path}"),
            limit: Arc::new(Semaphore::new(1)),
        };
        let start = std::time::Instant::now();
        assert_eq!(
            auth.check(&headers).await,
            Err(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert!(start.elapsed() < std::time::Duration::from_secs(6));
    }
    task.abort();
    Ok(())
}
