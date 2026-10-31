use crate::{
    auth::{bearer, Auth},
    domain::{known_node, now_ms, Batch},
    kubernetes::{Enrollment, Kubernetes},
    store::Store,
};
use axum::{
    extract::{DefaultBodyLimit, Query, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, Semaphore};
/// Ephemeral admission state only: no stored credential or authorization result.
#[derive(Clone, Default)]
pub struct CredentialGate {
    active: Arc<std::sync::Mutex<std::collections::BTreeSet<[u8; 32]>>>,
}
struct CredentialPermit {
    gate: CredentialGate,
    fingerprint: [u8; 32],
}
impl CredentialGate {
    fn acquire(&self, token: &str) -> Result<CredentialPermit, StatusCode> {
        let fingerprint: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let mut active = self
            .active
            .lock()
            .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
        if active.len() >= 64 || !active.insert(fingerprint) {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
        Ok(CredentialPermit {
            gate: self.clone(),
            fingerprint,
        })
    }
}
impl Drop for CredentialPermit {
    fn drop(&mut self) {
        if let Ok(mut active) = self.gate.active.lock() {
            active.remove(&self.fingerprint);
        }
    }
}
#[derive(Clone)]
pub struct App {
    pub auth: Auth,
    pub kube: Kubernetes,
    pub store: Store,
    pub tokens: Arc<BTreeMap<String, String>>,
    pub ingest_limit: Arc<Semaphore>,
    pub credential_gate: CredentialGate,
    pub read_limit: Arc<Semaphore>,
    pub quota: Arc<Mutex<BTreeMap<String, Instant>>>,
}
async fn security(request: axum::extract::Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    for (name,value) in [
        ("cache-control","no-store"),
        ("x-content-type-options","nosniff"),
        ("x-frame-options","DENY"),
        ("referrer-policy","no-referrer"),
        ("content-security-policy","default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self' https://auth.i3alumba.ru; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
    ] { response.headers_mut().insert(axum::http::HeaderName::from_static(name), axum::http::HeaderValue::from_static(value)); }
    response
}
pub fn public(app: App) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/",
            get(|| async { Html(include_str!("../assets/index.html")) }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript")],
                    include_str!("../assets/app.js"),
                )
            }),
        )
        .route(
            "/style.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css")],
                    include_str!("../assets/style.css"),
                )
            }),
        )
        .route("/api/nodes", get(nodes))
        .route("/api/history", get(history))
        .layer(middleware::from_fn_with_state(app.clone(), data_guard))
        .with_state(app)
        .layer(middleware::from_fn(security))
}
pub fn private(app: App) -> Router {
    Router::new()
        .route("/v1/samples", post(ingest))
        .route("/v1/kubernetes/enroll", post(enroll))
        .route("/v1/kubernetes/samples", post(kubernetes_ingest))
        .layer(DefaultBodyLimit::max(128 * 1024))
        .layer(middleware::from_fn_with_state(app.clone(), ingest_guard))
        .with_state(app)
}
async fn data_guard(
    State(app): State<App>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !request.uri().path().starts_with("/api/") {
        return next.run(request).await;
    }
    let Ok(_permit) = app.read_limit.try_acquire() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match tokio::time::timeout(Duration::from_secs(12), next.run(request)).await {
        Ok(response) => response,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
async fn ingest_guard(
    State(app): State<App>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    // Acquire before the global slot and any live API validation. The same bearer
    // shares one slot across enrollment, upload and native ingestion routes.
    let _credential = match bearer(request.headers()) {
        Ok(token) => match app.credential_gate.acquire(token) {
            Ok(permit) => Some(permit),
            Err(status) => return status.into_response(),
        },
        Err(_) => None, // Existing route/extractor authorization handles missing bearers.
    };
    let Ok(_permit) = app.ingest_limit.try_acquire() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    match tokio::time::timeout(Duration::from_secs(10), next.run(request)).await {
        Ok(response) => response,
        Err(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}
async fn nodes(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    app.auth.check(&headers).await?;
    let nodes = app
        .store
        .inventory()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let error = app
        .store
        .registry_error()
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(
        serde_json::json!({"nodes":nodes,"discovery_error":error}),
    ))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryQuery {
    node: String,
    hours: u32,
}
async fn history(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<HistoryQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    app.auth.check(&headers).await?;
    if uuid::Uuid::parse_str(&query.node).is_err()
        || !(1..=720).contains(&query.hours)
        || !app
            .store
            .has_uid(&query.node)
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let history = tokio::time::timeout(
        Duration::from_secs(5),
        app.store.history(&query.node, query.hours),
    )
    .await
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(history))
}
async fn ingest(
    State(app): State<App>,
    headers: HeaderMap,
    Json(mut batch): Json<Batch>,
) -> Result<StatusCode, StatusCode> {
    if headers.contains_key("content-encoding") {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let token = bearer(&headers)?;
    let node = app
        .tokens
        .iter()
        .find_map(|(node, expected)| {
            bool::from(expected.as_bytes().ct_eq(token.as_bytes())).then_some(node)
        })
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if batch.samples.is_empty()
        || batch.samples.len() > 6
        || batch
            .samples
            .iter()
            .any(|s| s.node != *node || !s.valid(now_ms()))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let live = app
        .kube
        .live_node(node)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if app.kube.native.get(node) != Some(&live.uid) {
        return Err(StatusCode::FORBIDDEN);
    }
    app.store
        .assigned(&live, "native")
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?;
    for sample in &mut batch.samples {
        sample.node = live.uid.clone();
    }
    quota(&app, &live.uid).await?;
    app.store
        .insert(&batch.samples)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn quota(app: &App, uid: &str) -> Result<(), StatusCode> {
    let mut quotas = app.quota.lock().await;
    quotas.retain(|_, last| last.elapsed() < Duration::from_secs(2));
    if quotas
        .get(uid)
        .is_some_and(|last| last.elapsed() < Duration::from_secs(1))
    {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    if quotas.len() >= 128 && !quotas.contains_key(uid) {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    quotas.insert(uid.into(), Instant::now());
    Ok(())
}
async fn enroll(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<Enrollment>, StatusCode> {
    if headers.contains_key("content-encoding") {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let node = app
        .kube
        .authenticate(bearer(&headers)?)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    app.store
        .assigned(&node, &node.provider)
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?;
    quota(&app, &format!("enroll:{}", node.uid)).await?;
    Ok(Json(node.into()))
}
async fn kubernetes_ingest(
    State(app): State<App>,
    headers: HeaderMap,
    Json(batch): Json<Batch>,
) -> Result<StatusCode, StatusCode> {
    if headers.contains_key("content-encoding") {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let node = app
        .kube
        .authenticate(bearer(&headers)?)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    app.store
        .assigned(&node, "kubernetes")
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?;
    if batch.samples.is_empty()
        || batch.samples.len() > 6
        || batch
            .samples
            .iter()
            .any(|s| s.node != node.uid || !s.valid(now_ms()))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    quota(&app, &node.uid).await?;
    app.store
        .insert(&batch.samples)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(StatusCode::NO_CONTENT)
}
pub async fn serve() -> anyhow::Result<()> {
    use crate::config::{client, env};
    let tokens: BTreeMap<String, String> = serde_json::from_slice(&std::fs::read(env(
        "MONITOR_NODE_TOKENS_FILE",
        "/run/monitor/nodes.json",
    ))?)?;
    anyhow::ensure!(
        tokens.len() == 2
            && tokens.keys().all(|k| known_node(k))
            && tokens.values().all(|v| v.len() >= 32
                && v.len() <= 512
                && v.bytes().all(|c| c.is_ascii_graphic())),
        "invalid node credential map"
    );
    anyhow::ensure!(
        tokens.get("alpha") != tokens.get("gamma"),
        "node credentials must differ"
    );
    let url = env("MONITOR_AUTH_URL", "http://auth:8000/api/user/");
    let parsed = reqwest::Url::parse(&url)?;
    anyhow::ensure!(
        parsed.path() == "/api/user/"
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.username().is_empty(),
        "Auth endpoint must be fixed /api/user/"
    );
    let app = App {
        kube: Kubernetes::from_env()?,
        auth: Auth {
            client: client().build()?,
            url,
            limit: Arc::new(Semaphore::new(16)),
        },
        store: Store::open(
            &env("MONITOR_DB", "/data/monitor.sqlite3"),
            1024 * 1024 * 1024,
        )
        .await?,
        tokens: Arc::new(tokens),
        ingest_limit: Arc::new(Semaphore::new(4)),
        credential_gate: Default::default(),
        read_limit: Arc::new(Semaphore::new(4)),
        quota: Default::default(),
    };
    let store = app.store.clone();
    let kube = app.kube.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let result = async {
                let nodes = kube.list().await?;
                store.reconcile(&nodes).await
            }
            .await;
            if let Err(error) = result {
                let _ = store.discovery_error(Some(&error.to_string())).await;
                eprintln!("Node discovery unavailable");
            }
            if let Err(e) = store.retain().await {
                eprintln!("retention unavailable: {e}");
            }
        }
    });
    let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(
        env("MONITOR_TLS_CERT_FILE", "/run/monitor/tls.crt"),
        env("MONITOR_TLS_KEY_FILE", "/run/monitor/tls.key"),
    )
    .await?;
    let private_addr = env("MONITOR_INGEST_ADDR", "0.0.0.0:8443").parse()?;
    let listener = tokio::net::TcpListener::bind(env("MONITOR_HTTP_ADDR", "0.0.0.0:8080")).await?;
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    let public = axum::serve(listener, public(app.clone())).with_graceful_shutdown(async move {
        shutdown().await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(5)));
    });
    let private = axum_server::bind_rustls(private_addr, tls)
        .handle(handle)
        .serve(private(app).into_make_service());
    tokio::try_join!(async { public.await }, private)?;
    Ok(())
}
async fn shutdown() {
    #[cfg(unix)]
    {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {_ = signal.recv()=>{}, _ = tokio::signal::ctrl_c()=>{}};
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod credential_gate_tests {
    use super::*;
    #[test]
    fn gate_bounds_entries_and_releases_on_drop() {
        let gate = CredentialGate::default();
        let permits: Vec<_> = (0..64)
            .map(|i| gate.acquire(&format!("token-{i}")).unwrap())
            .collect();
        assert!(gate.acquire("token-0").is_err());
        assert!(gate.acquire("overflow").is_err());
        drop(permits);
        assert!(gate.acquire("token-0").is_ok());
    }
}
