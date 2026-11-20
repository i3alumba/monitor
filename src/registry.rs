use crate::{
    domain::now_ms,
    kubernetes::Node,
    store::{decode, encoded, key, Point, Store, Writes, ACTIVE, IMPORT, RETENTION},
};
use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Entry {
    pub node: Node,
    pub first_seen: i64,
    pub last_seen: i64,
    pub last_sample: Option<i64>,
}
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Registry {
    pub nodes: BTreeMap<String, Entry>,
    pub error: Option<String>,
    pub cursor: i64,
    #[serde(default)]
    pub history_grace_until_ms: i64,
}
#[derive(Serialize)]
pub struct RegisteredNode {
    #[serde(flatten)]
    pub node: Node,
    pub first_seen: i64,
    pub last_seen: i64,
    pub last_sample: Option<i64>,
    pub collection: String,
    pub latest: Option<Point>,
}
impl Store {
    pub(crate) async fn registry(&self) -> Result<Registry> {
        let generation = self.generation().await?;
        let value = self
            .read(&[key(&generation, "registry", "", 0)])
            .await?
            .remove(0);
        ensure!(value.is_some(), "registry missing");
        decode(&value)
    }
    async fn update_registry(&self, update: impl Fn(&mut Registry) -> Result<()>) -> Result<()> {
        for _ in 0..8 {
            let generation = self.generation().await?;
            let registry_key = key(&generation, "registry", "", 0);
            let keys = vec![ACTIVE.into(), IMPORT.into(), registry_key.clone()];
            let values = self.read(&keys).await?;
            ensure!(
                values[0].as_deref() == Some(&generation)
                    && values[1].is_none()
                    && values[2].is_some(),
                "storage generation changed"
            );
            let mut registry = decode(&values[2])?;
            update(&mut registry)?;
            let writes = Writes::from([(registry_key, (encoded(&registry)?, 0))]);
            if self.commit(&keys, &values, writes).await? {
                return Ok(());
            }
        }
        bail!("registry contention")
    }
    pub async fn discovery_error(&self, error: Option<&str>) -> Result<()> {
        ensure!(
            error.is_none_or(|e| e.len() <= 4096),
            "discovery error too large"
        );
        self.update_registry(|r| {
            r.error = error.map(str::to_owned);
            Ok(())
        })
        .await
    }
    pub async fn registry_error(&self) -> Result<Option<String>> {
        Ok(self.registry().await?.error)
    }
    pub async fn reconcile(&self, nodes: &[Node]) -> Result<()> {
        ensure!(nodes.len() <= 64, "active Node capacity exceeded (64)");
        ensure!(
            nodes.iter().map(|n| &n.uid).collect::<BTreeSet<_>>().len() == nodes.len(),
            "duplicate Node UID"
        );
        self.update_registry(|r| {
            ensure!(
                r.nodes.len()
                    + nodes
                        .iter()
                        .filter(|n| !r.nodes.contains_key(&n.uid))
                        .count()
                    <= 256,
                "retained Node capacity exceeded (256)"
            );
            for entry in r.nodes.values_mut() {
                entry.node.lifecycle = "deleted".into();
            }
            for n in nodes {
                ensure!(
                    uuid::Uuid::parse_str(&n.uid).is_ok()
                        && n.name.len() <= 253
                        && n.capabilities.len() <= 32
                        && n.capabilities.iter().all(|c| c.len() <= 512),
                    "invalid Node metadata"
                );
                if let Some(prior) = r.nodes.get(&n.uid) {
                    ensure!(
                        prior.node.provider == n.provider,
                        "provider ownership conflict"
                    );
                }
                let entry = r.nodes.entry(n.uid.clone()).or_insert_with(|| Entry {
                    node: n.clone(),
                    first_seen: now_ms(),
                    last_seen: now_ms(),
                    last_sample: None,
                });
                entry.node = n.clone();
                entry.last_seen = now_ms();
            }
            r.error = None;
            Ok(())
        })
        .await
    }
    pub async fn assigned(&self, node: &Node, provider: &str) -> Result<bool> {
        let registry = self.registry().await?;
        Ok(registry.nodes.get(&node.uid).is_some_and(|entry| {
            entry.node.provider == provider
                && node.provider == provider
                && entry.node.lifecycle == "present"
                && entry.node.name == node.name
        }))
    }
    pub async fn has_uid(&self, uid: &str) -> Result<bool> {
        Ok(self.registry().await?.nodes.contains_key(uid))
    }
    pub async fn inventory(&self) -> Result<Vec<RegisteredNode>> {
        let registry = self.registry().await?;
        ensure!(registry.nodes.len() <= 256, "registry capacity exceeded");
        let mut nodes = Vec::new();
        for (uid, entry) in registry.nodes {
            let collection = if entry.node.lifecycle != "present" {
                "blocked"
            } else {
                match entry.last_sample {
                    None => "pending",
                    Some(t) if now_ms() - t > 60_000 => "stale",
                    _ => "fresh",
                }
            };
            nodes.push(RegisteredNode {
                node: entry.node,
                first_seen: entry.first_seen,
                last_seen: entry.last_seen,
                last_sample: entry.last_sample,
                collection: collection.into(),
                latest: self.latest(&uid).await?,
            });
        }
        nodes.sort_by(|a, b| (&a.node.name, &a.node.uid).cmp(&(&b.node.name, &b.node.uid)));
        Ok(nodes)
    }
    pub(crate) async fn maintain_registry(&self) -> Result<u64> {
        // Metric TTLs are authoritative; bounded control state is pruned only
        // after the latest possible associated observation has expired.
        let mut removed = 0;
        for _ in 0..8 {
            let generation = self.generation().await?;
            let registry_key = key(&generation, "registry", "", 0);
            let keys = vec![ACTIVE.into(), IMPORT.into(), registry_key.clone()];
            let values = self.read(&keys).await?;
            ensure!(
                values[0].as_deref() == Some(&generation)
                    && values[1].is_none()
                    && values[2].is_some(),
                "storage generation changed"
            );
            let mut registry: Registry = decode(&values[2])?;
            let expired: Vec<_> = registry
                .nodes
                .iter()
                .filter(|(_, e)| {
                    registry.history_grace_until_ms < now_ms()
                        && e.node.lifecycle == "deleted"
                        && e.last_seen.max(e.last_sample.unwrap_or(0)) + RETENTION < now_ms()
                })
                .map(|(uid, _)| uid.clone())
                .take(64)
                .collect();
            for uid in &expired {
                registry.nodes.remove(uid);
            }
            let mut writes = Writes::from([(registry_key, (encoded(&registry)?, 0))]);
            for uid in expired.iter().take(64) {
                writes.insert(key(&generation, "control", uid, 0), (String::new(), -1));
            }
            if self.commit(&keys, &values, writes).await? {
                removed = expired.len() as u64;
                break;
            }
        }
        Ok(removed)
    }
}
