//! Live Kubernetes identity validation; no positive token or Pod authorization cache.
use crate::config;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::Semaphore;

pub const AUDIENCE: &str = "monitor-ingest";
#[derive(Clone)]
pub struct Kubernetes {
    pub client: reqwest::Client,
    pub base: String,
    pub token_file: String,
    pub namespace: String,
    pub service_account: String,
    pub daemonset: String,
    pub native: Arc<BTreeMap<String, String>>,
    pub limit: Arc<Semaphore>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub uid: String,
    pub name: String,
    pub lifecycle: String,
    pub provider: String,
    pub capabilities: Vec<String>,
}
pub fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 253
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b".-".contains(&c))
}
fn text<'a>(v: &'a Value, path: &str) -> Result<&'a str> {
    v.pointer(path)
        .and_then(Value::as_str)
        .context("missing Kubernetes identity field")
}
impl Kubernetes {
    pub fn from_env() -> Result<Self> {
        let base = config::env("MONITOR_KUBE_API", "https://kubernetes.default.svc");
        let url = reqwest::Url::parse(&base)?;
        ensure!(
            url.scheme() == "https"
                && url.username().is_empty()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Kubernetes API must be HTTPS"
        );
        let ca = reqwest::Certificate::from_pem(&std::fs::read(config::env(
            "MONITOR_KUBE_CA_FILE",
            "/run/kubernetes/ca.crt",
        ))?)?;
        let native: BTreeMap<String, String> =
            serde_json::from_slice(&std::fs::read(config::env(
                "MONITOR_NATIVE_NODES_FILE",
                "/run/monitor-native/nodes.json",
            ))?)?;
        ensure!(
            native.len() == 2
                && native.keys().all(|n| crate::domain::known_node(n))
                && native.values().all(|u| uuid::Uuid::parse_str(u).is_ok())
                && native.get("alpha") != native.get("gamma"),
            "invalid native UID owner map"
        );
        let namespace = config::env("MONITOR_NAMESPACE", "public-page");
        let service_account = config::env("MONITOR_AGENT_SERVICE_ACCOUNT", "monitor-agent");
        let daemonset = config::env("MONITOR_AGENT_DAEMONSET", "monitor-agent");
        ensure!(
            [&namespace, &service_account, &daemonset]
                .iter()
                .all(|s| valid_name(s)),
            "invalid Kubernetes resource names"
        );
        Ok(Self {
            client: config::client()
                .tls_built_in_root_certs(false)
                .add_root_certificate(ca)
                .build()?,
            base: base.trim_end_matches('/').into(),
            token_file: config::env("MONITOR_KUBE_TOKEN_FILE", "/run/kubernetes/token"),
            namespace,
            service_account,
            daemonset,
            native: Arc::new(native),
            limit: Arc::new(Semaphore::new(8)),
        })
    }
    async fn request(&self, path: &str, body: Option<Value>) -> Result<Value> {
        let _permit = self
            .limit
            .try_acquire()
            .context("Kubernetes concurrency exhausted")?;
        let token = config::projected_token(&self.token_file)?;
        let request = match body {
            Some(body) => self.client.post(format!("{}{path}", self.base)).json(&body),
            None => self.client.get(format!("{}{path}", self.base)),
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut response = request.bearer_auth(token).send().await?;
            ensure!(
                response.status().is_success(),
                "Kubernetes API denied or unavailable"
            );
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    bytes.len() + chunk.len() <= 4 * 1024 * 1024,
                    "Kubernetes response too large"
                );
                bytes.extend_from_slice(&chunk);
            }
            Ok(serde_json::from_slice(&bytes)?)
        })
        .await
        .context("Kubernetes API deadline")?
    }
    fn node(&self, value: &Value) -> Result<Node> {
        let uid = text(value, "/metadata/uid")?;
        let name = text(value, "/metadata/name")?;
        ensure!(
            uuid::Uuid::parse_str(uid).is_ok() && valid_name(name),
            "invalid Node identity"
        );
        let architecture = value
            .pointer("/status/nodeInfo/architecture")
            .and_then(Value::as_str)
            .unwrap_or("");
        let operating_system = value
            .pointer("/status/nodeInfo/operatingSystem")
            .and_then(Value::as_str)
            .unwrap_or("");
        let kernel = value
            .pointer("/status/nodeInfo/kernelVersion")
            .and_then(Value::as_str)
            .unwrap_or("");
        let supported = operating_system == "linux"
            && matches!(architecture, "amd64" | "arm64" | "arm")
            && !kernel.contains("armv6");
        let ready = value
            .pointer("/status/conditions")
            .and_then(Value::as_array)
            .is_some_and(|a| {
                a.iter()
                    .any(|c| c["type"] == "Ready" && c["status"] == "True")
            });
        let lifecycle = if !value["metadata"]["deletionTimestamp"].is_null() {
            "deleted"
        } else if !supported {
            "unsupported"
        } else if !ready {
            "not-ready"
        } else {
            "present"
        };
        let provider = if self.native.get(name).is_some_and(|v| v == uid) {
            "native"
        } else {
            "kubernetes"
        };
        let mut capabilities = if provider == "native" {
            vec!["Native approved GPU/root/HDD profile; unsupported fields remain null".into()]
        } else {
            vec!["Baseline host CPU/RAM/block IO/network; probe_root is its containing filesystem, not necessarily /".into(),"GPU and extra filesystem capacity unavailable in automatic profile".into()]
        };
        if architecture == "arm" {
            capabilities.push("ARMv7 minimum; architecture=arm does not attest ISA variant; coverage pending until compatible collector runs".into());
        }
        if let Some(taints) = value.pointer("/spec/taints").and_then(Value::as_array) {
            if taints
                .iter()
                .any(|t| matches!(t["effect"].as_str(), Some("NoSchedule" | "NoExecute")))
            {
                capabilities.push(
                    "Taints may block collector scheduling; no automatic taint bypass".into(),
                );
            }
        }
        Ok(Node {
            uid: uid.into(),
            name: name.into(),
            lifecycle: lifecycle.into(),
            provider: provider.into(),
            capabilities,
        })
    }
    pub async fn list(&self) -> Result<Vec<Node>> {
        let value = self.request("/api/v1/nodes?limit=257", None).await?;
        ensure!(
            value["metadata"].is_object(),
            "malformed Node list metadata"
        );
        ensure!(
            match value.pointer("/metadata/continue") {
                None | Some(Value::Null) => true,
                Some(Value::String(s)) => s.is_empty(),
                _ => false,
            },
            "incomplete Node list or capacity exceeded"
        );
        let items = value["items"].as_array().context("malformed Node list")?;
        ensure!(items.len() <= 64, "active Node capacity exceeded (64)");
        let nodes: Vec<_> = items.iter().map(|v| self.node(v)).collect::<Result<_>>()?;
        let unique: std::collections::BTreeSet<_> = nodes.iter().map(|n| &n.uid).collect();
        ensure!(unique.len() == nodes.len(), "duplicate Node UID");
        let names: std::collections::BTreeSet<_> = nodes.iter().map(|n| &n.name).collect();
        ensure!(names.len() == nodes.len(), "duplicate Node name");
        Ok(nodes)
    }
    pub async fn live_node(&self, name: &str) -> Result<Node> {
        ensure!(valid_name(name), "invalid Node name");
        let value = self.request(&format!("/api/v1/nodes/{name}"), None).await?;
        let node = self.node(&value)?;
        ensure!(
            node.name == name && node.lifecycle == "present",
            "Node unavailable"
        );
        Ok(node)
    }
    pub async fn authenticate(&self, token: &str) -> Result<Node> {
        let review=self.request("/apis/authentication.k8s.io/v1/tokenreviews",Some(json!({"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","spec":{"token":token,"audiences":[AUDIENCE]}}))).await?;
        ensure!(
            review.pointer("/status/authenticated") == Some(&Value::Bool(true)),
            "token denied"
        );
        ensure!(
            review
                .pointer("/status/audiences")
                .and_then(Value::as_array)
                .is_some_and(|a| a.iter().any(|s| s.as_str() == Some(AUDIENCE))),
            "wrong audience"
        );
        ensure!(
            text(&review, "/status/user/username")?
                == format!(
                    "system:serviceaccount:{}:{}",
                    self.namespace, self.service_account
                ),
            "wrong ServiceAccount"
        );
        let extra = review.pointer("/status/user/extra");
        let bound = |key: &str| {
            extra
                .and_then(|e| e.get(key))
                .and_then(Value::as_array)
                .filter(|a| a.len() == 1)
                .and_then(|a| a[0].as_str())
        };
        let (pod_name, pod_uid) = match (
            bound("authentication.kubernetes.io/pod-name"),
            bound("authentication.kubernetes.io/pod-uid"),
        ) {
            (Some(n), Some(u)) => (n.to_owned(), u.to_owned()),
            _ => {
                // Claims are usable ONLY after successful review of this exact token.
                let mut parts = token.split('.');
                let _ = parts.next();
                let payload = parts.next().context("unbound token")?;
                let bytes = URL_SAFE_NO_PAD
                    .decode(payload)
                    .context("invalid bound token")?;
                let claims: Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    text(&claims, "/kubernetes.io/namespace")? == self.namespace,
                    "foreign namespace"
                );
                (
                    text(&claims, "/kubernetes.io/pod/name")?.into(),
                    text(&claims, "/kubernetes.io/pod/uid")?.into(),
                )
            }
        };
        ensure!(
            valid_name(&pod_name) && uuid::Uuid::parse_str(&pod_uid).is_ok(),
            "unbound Pod identity"
        );
        let pod = self
            .request(
                &format!("/api/v1/namespaces/{}/pods/{pod_name}", self.namespace),
                None,
            )
            .await?;
        ensure!(
            text(&pod, "/metadata/name")? == pod_name
                && text(&pod, "/metadata/namespace")? == self.namespace
                && text(&pod, "/metadata/uid")? == pod_uid
                && pod["metadata"]["deletionTimestamp"].is_null(),
            "Pod replaced or deleted"
        );
        ensure!(
            text(&pod, "/spec/serviceAccountName")? == self.service_account,
            "foreign Pod ServiceAccount"
        );
        let owners = pod
            .pointer("/metadata/ownerReferences")
            .and_then(Value::as_array)
            .context("Pod not owned")?;
        let controllers: Vec<_> = owners.iter().filter(|o| o["controller"] == true).collect();
        ensure!(controllers.len() == 1, "invalid controller");
        let owner = controllers[0];
        let node_name = text(&pod, "/spec/nodeName")?;
        ensure!(valid_name(node_name), "invalid Node name");
        let live = self
            .request(&format!("/api/v1/nodes/{node_name}"), None)
            .await?;
        let node = self.node(&live)?;
        ensure!(
            node.name == node_name && node.lifecycle == "present",
            "Node unavailable"
        );
        let suffix = match text(&live, "/status/nodeInfo/architecture")? {
            "amd64" => "amd64",
            "arm64" => "arm64",
            "arm" => "armv7",
            _ => anyhow::bail!("unsupported architecture"),
        };
        let expected_ds = format!("{}-{suffix}", self.daemonset);
        ensure!(
            owner["kind"] == "DaemonSet"
                && owner["apiVersion"] == "apps/v1"
                && owner["name"] == expected_ds,
            "foreign controller"
        );
        let ds = self
            .request(
                &format!(
                    "/apis/apps/v1/namespaces/{}/daemonsets/{expected_ds}",
                    self.namespace
                ),
                None,
            )
            .await?;
        ensure!(
            text(&ds, "/metadata/uid")? == text(owner, "/uid")?
                && ds["metadata"]["deletionTimestamp"].is_null(),
            "DaemonSet replaced"
        );
        Ok(node)
    }
}
#[derive(Serialize, Deserialize)]
pub struct Enrollment {
    pub node_uid: String,
    pub provider: String,
    pub collect: bool,
    pub capabilities: Vec<String>,
}
impl From<Node> for Enrollment {
    fn from(node: Node) -> Self {
        Self {
            collect: node.provider == "kubernetes",
            node_uid: node.uid,
            provider: node.provider,
            capabilities: node.capabilities,
        }
    }
}
