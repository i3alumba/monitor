use axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::{Request as HttpRequest, StatusCode},
    response::IntoResponse,
    Router,
};
use resource_monitor::{
    auth::Auth,
    domain::{now_ms, Batch, Sample},
    kubernetes::Kubernetes,
    store::Store,
    web::{self, App},
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{Mutex, Semaphore};
use tower::ServiceExt;
const UID: &str = "10000000-0000-0000-0000-000000000001";
const OLD: &str = "20000000-0000-0000-0000-000000000001";
const POD: &str = "30000000-0000-0000-0000-000000000001";
const DS: &str = "40000000-0000-0000-0000-000000000001";
const TOKEN: &str = "projected-token-aaaaaaaaaaaaaaaaaaaaaaaa";
#[derive(Clone)]
struct Mock {
    resources: BTreeMap<String, Value>,
    review: Value,
    outage: bool,
    tokens: Vec<String>,
    review_hold: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}
fn node(name: &str, uid: &str, arch: &str) -> Value {
    json!({"metadata":{"name":name,"uid":uid,"labels":{"monitor/native":"true"}},"status":{"nodeInfo":{"architecture":arch,"operatingSystem":"linux","kernelVersion":"6.1"},"conditions":[{"type":"Ready","status":"True"}]}})
}
async fn handler(
    State(state): State<Arc<Mutex<Mock>>>,
    request: Request,
) -> axum::response::Response {
    let path = request.uri().path().to_owned();
    let mut state = state.lock().await;
    if state.outage {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if path == "/apis/authentication.k8s.io/v1/tokenreviews" {
        let bytes = to_bytes(request.into_body(), 16384).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["spec"]["audiences"], json!(["monitor-ingest"]));
        state
            .tokens
            .push(body["spec"]["token"].as_str().unwrap().into());
        let review = state.review.clone();
        let hold = state.review_hold.clone();
        drop(state);
        if let Some((started, release)) = hold {
            started.notify_one();
            release.notified().await;
        }
        return axum::Json(review).into_response();
    }
    if path == "/api/user/" {
        return axum::Json(json!([{"id":7,"services":{"monitor":true}}])).into_response();
    }
    state
        .resources
        .get(&path)
        .cloned()
        .map(axum::Json)
        .map(IntoResponse::into_response)
        .unwrap_or_else(|| StatusCode::NOT_FOUND.into_response())
}
async fn fixture() -> anyhow::Result<(
    tempfile::TempDir,
    App,
    Arc<Mutex<Mock>>,
    tokio::task::JoinHandle<()>,
)> {
    let dir = tempfile::tempdir()?;
    let n = node("new-node", UID, "amd64");
    let mock = Mock {
        resources: BTreeMap::from([
            (
                "/api/v1/nodes".into(),
                json!({"metadata":{},"items":[n.clone()]}),
            ),
            ("/api/v1/nodes/new-node".into(), n),
            (
                "/api/v1/namespaces/public-page/pods/agent-pod".into(),
                json!({"metadata":{"name":"agent-pod","namespace":"public-page","uid":POD,"ownerReferences":[{"apiVersion":"apps/v1","kind":"DaemonSet","name":"monitor-agent-amd64","uid":DS,"controller":true}]},"spec":{"serviceAccountName":"monitor-agent","nodeName":"new-node"}}),
            ),
            (
                "/apis/apps/v1/namespaces/public-page/daemonsets/monitor-agent-amd64".into(),
                json!({"metadata":{"uid":DS}}),
            ),
        ]),
        review: json!({"status":{"authenticated":true,"audiences":["monitor-ingest"],"user":{"username":"system:serviceaccount:public-page:monitor-agent","extra":{"authentication.kubernetes.io/pod-name":["agent-pod"],"authentication.kubernetes.io/pod-uid":[POD]}}}}),
        outage: false,
        tokens: vec![],
        review_hold: None,
    };
    let state = Arc::new(Mutex::new(mock));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let router = Router::new().fallback(handler).with_state(state.clone());
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    let token = dir.path().join("api-token");
    std::fs::write(&token, "k".repeat(64))?;
    let client = resource_monitor::config::client().build()?;
    let app = App {
        auth: Auth {
            client: client.clone(),
            url: format!("http://{addr}/api/user/"),
            limit: Arc::new(Semaphore::new(16)),
        },
        kube: Kubernetes {
            client,
            base: format!("http://{addr}"),
            token_file: token.to_string_lossy().into(),
            namespace: "public-page".into(),
            service_account: "monitor-agent".into(),
            daemonset: "monitor-agent".into(),
            native: Arc::new(BTreeMap::from([
                ("alpha".into(), OLD.into()),
                (
                    "gamma".into(),
                    "20000000-0000-0000-0000-000000000002".into(),
                ),
            ])),
            limit: Arc::new(Semaphore::new(8)),
        },
        store: Store::open(dir.path().join("db").to_str().unwrap(), 0).await?,
        tokens: Arc::new(BTreeMap::from([
            ("alpha".into(), "a".repeat(64)),
            ("gamma".into(), "b".repeat(64)),
        ])),
        ingest_limit: Arc::new(Semaphore::new(4)),
        credential_gate: Default::default(),
        read_limit: Arc::new(Semaphore::new(4)),
        quota: Default::default(),
    };
    Ok((dir, app, state, task))
}
fn sample(uid: &str) -> Sample {
    Sample {
        node: uid.into(),
        boot_id: uuid::Uuid::nil().to_string(),
        process_id: uuid::Uuid::nil().to_string(),
        seq: 0,
        timestamp_ms: now_ms(),
        metrics: BTreeMap::from([("cpu.used_percent".into(), Some(10.))]),
        gpu: Default::default(),
    }
}
async fn post(
    app: &App,
    path: &str,
    token: &str,
    body: Value,
) -> anyhow::Result<axum::response::Response> {
    Ok(web::private(app.clone())
        .oneshot(
            HttpRequest::post(path)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body)?))?,
        )
        .await?)
}
#[tokio::test]
async fn new_node_pending_enrollment_rotation_upload_uid_history_and_outage() -> anyhow::Result<()>
{
    let (_dir, app, state, task) = fixture().await?;
    app.store.reconcile(&app.kube.list().await?).await?;
    assert_eq!(app.store.inventory().await?[0].collection, "pending");
    let response = post(&app, "/v1/kubernetes/enroll", TOKEN, json!({})).await?;
    assert_eq!(response.status(), 200);
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 16384).await?)?;
    assert_eq!(value["node_uid"], UID);
    assert_eq!(value["collect"], true);
    let body = serde_json::to_value(Batch {
        samples: vec![sample(UID)],
    })?;
    assert_eq!(
        post(
            &app,
            "/v1/kubernetes/samples",
            "rotated-token-bbbbbbbbbbbbbbbbbbbbbbb",
            body
        )
        .await?
        .status(),
        204
    );
    assert_eq!(app.store.inventory().await?[0].collection, "fresh");
    let response = web::public(app.clone())
        .oneshot(
            HttpRequest::get(format!("/api/history?node={UID}&hours=1"))
                .header("authorization", "Bearer human")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), 200);
    let value: Value = serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await?)?;
    assert_eq!(value["node"], UID);
    assert!(value["points"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["metrics"]["cpu.used_percent"] == 10.));
    {
        let mut state = state.lock().await;
        assert_eq!(state.tokens.len(), 2);
        assert_ne!(state.tokens[0], state.tokens[1]);
        state.outage = true;
    }
    assert!(app.kube.list().await.is_err());
    assert!(app.kube.authenticate(TOKEN).await.is_err());
    assert_eq!(app.store.inventory().await?[0].node.lifecycle, "present");
    task.abort();
    Ok(())
}
#[tokio::test]
async fn forged_bindings_are_rejected_on_every_request() -> anyhow::Result<()> {
    let (_dir, app, state, task) = fixture().await?;
    let baseline = state.lock().await.clone();
    for case in 0..10 {
        let mut bad = baseline.clone();
        match case {
            0 => bad.review["status"]["audiences"] = json!(["api"]),
            1 => {
                bad.review["status"]["user"]["username"] =
                    json!("system:serviceaccount:other:monitor-agent")
            }
            2 => bad.review["status"]["user"]["extra"] = json!({}),
            3 => {
                bad.resources
                    .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
                    .unwrap()["metadata"]["uid"] = json!(OLD)
            }
            4 => {
                bad.resources
                    .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
                    .unwrap()["metadata"]["deletionTimestamp"] = json!("now")
            }
            5 => {
                bad.resources
                    .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
                    .unwrap()["spec"]["serviceAccountName"] = json!("foreign")
            }
            6 => {
                bad.resources
                    .get_mut("/apis/apps/v1/namespaces/public-page/daemonsets/monitor-agent-amd64")
                    .unwrap()["metadata"]["uid"] = json!(OLD)
            }
            7 => {
                bad.resources.get_mut("/api/v1/nodes/new-node").unwrap()["metadata"]
                    ["deletionTimestamp"] = json!("now")
            }
            8 => bad.review["status"]["authenticated"] = json!(false),
            _ => {
                bad.resources
                    .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
                    .unwrap()["metadata"]["ownerReferences"][0]["name"] =
                    json!("monitor-agent-arm64")
            }
        }
        *state.lock().await = bad;
        assert!(app.kube.authenticate(TOKEN).await.is_err(), "case {case}");
    }
    *state.lock().await = baseline;
    app.store.reconcile(&app.kube.list().await?).await?;
    assert_eq!(
        post(
            &app,
            "/v1/kubernetes/samples",
            TOKEN,
            serde_json::to_value(Batch {
                samples: vec![sample(OLD)]
            })?
        )
        .await?
        .status(),
        400
    );
    task.abort();
    Ok(())
}
#[tokio::test]
async fn native_name_replacement_never_inherits_key_or_provider_and_arch_is_api_derived(
) -> anyhow::Result<()> {
    let (_dir, app, state, task) = fixture().await?;
    let native = node("alpha", OLD, "amd64");
    {
        let mut s = state.lock().await;
        s.resources.insert(
            "/api/v1/nodes".into(),
            json!({"metadata":{},"items":[native.clone()]}),
        );
        s.resources.insert("/api/v1/nodes/alpha".into(), native);
    }
    app.store.reconcile(&app.kube.list().await?).await?;
    assert_eq!(app.store.inventory().await?[0].node.provider, "native");
    {
        let mut s = state.lock().await;
        s.resources
            .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
            .unwrap()["spec"]["nodeName"] = json!("alpha");
    }
    let idle = post(&app, "/v1/kubernetes/enroll", TOKEN, json!({})).await?;
    assert_eq!(idle.status(), 200);
    let idle: Value = serde_json::from_slice(&to_bytes(idle.into_body(), 16384).await?)?;
    assert_eq!(idle["collect"], false);
    assert_eq!(
        post(
            &app,
            "/v1/kubernetes/samples",
            TOKEN,
            serde_json::to_value(Batch {
                samples: vec![sample(OLD)]
            })?
        )
        .await?
        .status(),
        403
    );
    assert_eq!(
        post(
            &app,
            "/v1/samples",
            &"a".repeat(64),
            serde_json::to_value(Batch {
                samples: vec![sample("alpha")]
            })?
        )
        .await?
        .status(),
        204
    );
    let replacement = node("alpha", UID, "arm64");
    {
        let mut s = state.lock().await;
        s.resources.insert(
            "/api/v1/nodes".into(),
            json!({"metadata":{},"items":[replacement.clone()]}),
        );
        s.resources
            .insert("/api/v1/nodes/alpha".into(), replacement);
        let pod = s
            .resources
            .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
            .unwrap();
        pod["spec"]["nodeName"] = json!("alpha");
        pod["metadata"]["ownerReferences"][0]["name"] = json!("monitor-agent-arm64");
        s.resources.insert(
            "/apis/apps/v1/namespaces/public-page/daemonsets/monitor-agent-arm64".into(),
            json!({"metadata":{"uid":DS}}),
        );
    }
    app.store.reconcile(&app.kube.list().await?).await?;
    let inventory = app.store.inventory().await?;
    assert_eq!(inventory.len(), 2);
    assert!(inventory
        .iter()
        .any(|n| n.node.uid == OLD && n.node.lifecycle == "deleted"));
    assert!(inventory
        .iter()
        .any(|n| n.node.uid == UID && n.node.provider == "kubernetes"));
    assert_eq!(
        post(
            &app,
            "/v1/samples",
            &"a".repeat(64),
            serde_json::to_value(Batch {
                samples: vec![sample("alpha")]
            })?
        )
        .await?
        .status(),
        403
    );
    assert!(app.kube.authenticate(TOKEN).await.is_ok());
    // Replaced node's copied informational native label is irrelevant.
    assert_eq!(
        post(&app, "/v1/kubernetes/enroll", TOKEN, json!({}))
            .await?
            .status(),
        200
    );
    task.abort();
    Ok(())
}

#[tokio::test]
async fn reviewed_bound_claims_armv7_and_unsupported_windows() -> anyhow::Result<()> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let (_dir, app, state, task) = fixture().await?;
    let claims =
        json!({"kubernetes.io":{"namespace":"public-page","pod":{"name":"agent-pod","uid":POD}}});
    let token = format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?)
    );
    state.lock().await.review["status"]["user"]["extra"] = json!({});
    assert!(app.kube.authenticate(&token).await.is_ok());
    {
        let mut s = state.lock().await;
        s.resources.get_mut("/api/v1/nodes/new-node").unwrap()["status"]["nodeInfo"]
            ["architecture"] = json!("arm");
        s.resources
            .get_mut("/api/v1/namespaces/public-page/pods/agent-pod")
            .unwrap()["metadata"]["ownerReferences"][0]["name"] = json!("monitor-agent-armv7");
        s.resources.insert(
            "/apis/apps/v1/namespaces/public-page/daemonsets/monitor-agent-armv7".into(),
            json!({"metadata":{"uid":DS}}),
        );
    }
    let arm = app.kube.authenticate(&token).await?;
    assert!(arm.capabilities.iter().any(|c| c.contains("ARMv7 minimum")));
    state
        .lock()
        .await
        .resources
        .get_mut("/api/v1/nodes/new-node")
        .unwrap()["status"]["nodeInfo"]["kernelVersion"] = json!("6.1-armv6l");
    assert!(app.kube.authenticate(&token).await.is_err());
    let mut windows = node("windows", UID, "amd64");
    windows["status"]["nodeInfo"]["operatingSystem"] = json!("windows");
    state.lock().await.resources.insert(
        "/api/v1/nodes".into(),
        json!({"metadata":{},"items":[windows]}),
    );
    let unsupported = app.kube.list().await?;
    assert_eq!(unsupported[0].lifecycle, "unsupported");
    task.abort();
    Ok(())
}

#[tokio::test]
async fn duplicate_credential_is_rejected_before_validation_or_global_slot() -> anyhow::Result<()> {
    let (_dir, app, state, task) = fixture().await?;
    app.store.reconcile(&app.kube.list().await?).await?;
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    state.lock().await.review_hold = Some((started.clone(), release.clone()));
    let first_app = app.clone();
    let first =
        tokio::spawn(
            async move { post(&first_app, "/v1/kubernetes/enroll", TOKEN, json!({})).await },
        );
    tokio::time::timeout(std::time::Duration::from_secs(2), started.notified()).await?;
    assert_eq!(app.ingest_limit.available_permits(), 3);
    let mut duplicates = tokio::task::JoinSet::new();
    for i in 0..8 {
        let app = app.clone();
        duplicates.spawn(async move {
            let path = if i % 2 == 0 {
                "/v1/kubernetes/enroll"
            } else {
                "/v1/kubernetes/samples"
            };
            post(&app, path, TOKEN, json!({"samples":[]}))
                .await
                .map(|response| response.status())
        });
    }
    while let Some(response) = duplicates.join_next().await {
        assert_eq!(response??, 429);
    }
    assert_eq!(app.ingest_limit.available_permits(), 3);
    assert_eq!(state.lock().await.tokens.len(), 1);
    // Distinct credentials can still enter the independent remaining global slots.
    state.lock().await.review_hold = None;
    let other = post(
        &app,
        "/v1/kubernetes/enroll",
        "different-projected-credential-bbbbbbbbb",
        json!({}),
    )
    .await?;
    assert_eq!(other.status(), 200);
    assert_eq!(state.lock().await.tokens.len(), 2);
    release.notify_one();
    assert_eq!(first.await??.status(), 429); // Its UID quota remains authoritative.
    assert_eq!(app.ingest_limit.available_permits(), 4);
    app.quota.lock().await.clear();
    assert_eq!(
        post(&app, "/v1/kubernetes/enroll", TOKEN, json!({}))
            .await?
            .status(),
        200
    );
    assert_eq!(state.lock().await.tokens.len(), 3); // No cached authentication result.
    task.abort();
    Ok(())
}
