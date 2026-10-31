use crate::{
    domain::now_ms,
    kubernetes::Node,
    store::{Point, Store},
};
use anyhow::{ensure, Result};
use serde::Serialize;
use sqlx::Row;
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
    pub async fn discovery_error(&self, error: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE registry_status SET error=? WHERE id=1")
            .bind(error)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn registry_error(&self) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT error FROM registry_status WHERE id=1")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    pub async fn reconcile(&self, nodes: &[Node]) -> Result<()> {
        ensure!(nodes.len() <= 64, "active Node capacity exceeded (64)");
        let mut tx = self.pool.begin().await?;
        let existing: Vec<String> = sqlx::query_scalar("SELECT uid FROM nodes")
            .fetch_all(&mut *tx)
            .await?;
        ensure!(
            existing.len() + nodes.iter().filter(|n| !existing.contains(&n.uid)).count() <= 256,
            "retained Node capacity exceeded (256)"
        );
        let now = now_ms();
        sqlx::query("UPDATE nodes SET lifecycle='deleted' WHERE lifecycle!='deleted'")
            .execute(&mut *tx)
            .await?;
        for n in nodes {
            // Provider changes are never a fallback decision. A changed operator map requires review.
            let prior: Option<String> =
                sqlx::query_scalar("SELECT provider FROM nodes WHERE uid=?")
                    .bind(&n.uid)
                    .fetch_optional(&mut *tx)
                    .await?;
            ensure!(
                prior.as_ref().is_none_or(|p| p == &n.provider),
                "provider ownership conflict"
            );
            sqlx::query("INSERT INTO nodes VALUES (?,?,?,?,?,?,?,NULL) ON CONFLICT(uid) DO UPDATE SET name=excluded.name,lifecycle=excluded.lifecycle,capabilities=excluded.capabilities,last_seen=excluded.last_seen")
                .bind(&n.uid).bind(&n.name).bind(&n.lifecycle).bind(&n.provider).bind(serde_json::to_string(&n.capabilities)?).bind(now).bind(now).execute(&mut *tx).await?;
            if n.provider == "native" {
                // Additive v1 migration: bind prior name-keyed native history only to approved UID.
                for table in ["samples", "rollups", "metric_keys"] {
                    sqlx::query(&format!("UPDATE OR IGNORE {table} SET node=? WHERE node=?"))
                        .bind(&n.uid)
                        .bind(&n.name)
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::query("UPDATE nodes SET last_sample=coalesce((SELECT max(time) FROM samples WHERE node=?),last_sample) WHERE uid=?").bind(&n.uid).bind(&n.uid).execute(&mut *tx).await?;
            }
        }
        sqlx::query("UPDATE registry_status SET error=NULL WHERE id=1")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn assigned(&self, node: &Node, provider: &str) -> Result<()> {
        let row = sqlx::query("SELECT provider,lifecycle,name FROM nodes WHERE uid=?")
            .bind(&node.uid)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Node discovery pending"))?;
        ensure!(
            row.try_get::<String, _>("provider")? == provider
                && node.provider == provider
                && row.try_get::<String, _>("lifecycle")? == "present"
                && row.try_get::<String, _>("name")? == node.name,
            "inactive provider or Node"
        );
        Ok(())
    }
    pub async fn has_uid(&self, uid: &str) -> Result<bool> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM nodes WHERE uid=?")
                .bind(uid)
                .fetch_one(&self.pool)
                .await?
                > 0,
        )
    }
    pub async fn inventory(&self) -> Result<Vec<RegisteredNode>> {
        let rows = sqlx::query("SELECT * FROM nodes ORDER BY name,uid LIMIT 256")
            .fetch_all(&self.pool)
            .await?;
        let mut nodes = Vec::new();
        for row in rows {
            let uid: String = row.try_get("uid")?;
            let lifecycle: String = row.try_get("lifecycle")?;
            let last_sample: Option<i64> = row.try_get("last_sample")?;
            let collection = if lifecycle != "present" {
                "blocked"
            } else {
                match last_sample {
                    None => "pending",
                    Some(t) if now_ms() - t > 60_000 => "stale",
                    _ => "fresh",
                }
            };
            nodes.push(RegisteredNode {
                node: Node {
                    uid: uid.clone(),
                    name: row.try_get("name")?,
                    lifecycle,
                    provider: row.try_get("provider")?,
                    capabilities: serde_json::from_str(row.try_get("capabilities")?)?,
                },
                first_seen: row.try_get("first_seen")?,
                last_seen: row.try_get("last_seen")?,
                last_sample,
                collection: collection.into(),
                latest: self.latest(&uid).await?,
            });
        }
        Ok(nodes)
    }
    pub(crate) async fn maintain_registry(&self) -> Result<u64> {
        let cutoff = now_ms() - 30 * 86400 * 1000;
        let mut tx = self.pool.begin().await?;
        let nodes:Vec<String>=sqlx::query_scalar("SELECT uid FROM nodes UNION SELECT DISTINCT node FROM metric_keys ORDER BY 1 LIMIT 258").fetch_all(&mut *tx).await?;
        let cursor: i64 = sqlx::query_scalar("SELECT cursor FROM registry_status WHERE id=1")
            .fetch_one(&mut *tx)
            .await?;
        let slots = nodes.len() * 3;
        let mut removed = 0u64;
        let mut advanced = 0;
        for i in 0..slots.min(64) {
            if removed >= 1000 {
                break;
            }
            let slot = (cursor as usize + i) % slots;
            let node = &nodes[slot / 3];
            let budget = (1000 - removed).min(100) as i64;
            let result=match slot%3 {
                0=>sqlx::query("DELETE FROM samples WHERE rowid IN (SELECT rowid FROM samples WHERE node=? AND time<? LIMIT ?)").bind(node).bind(cutoff).bind(budget).execute(&mut *tx).await?,
                1=>sqlx::query("DELETE FROM rollups WHERE rowid IN (SELECT rowid FROM rollups WHERE node=? AND time<? LIMIT ?)").bind(node).bind(cutoff).bind(budget).execute(&mut *tx).await?,
                _=>sqlx::query("DELETE FROM metric_keys WHERE rowid IN (SELECT rowid FROM metric_keys WHERE node=? AND EXISTS (SELECT 1 FROM nodes WHERE uid=? AND lifecycle='deleted' AND last_seen<?) AND NOT EXISTS (SELECT 1 FROM samples WHERE node=?) AND NOT EXISTS (SELECT 1 FROM rollups WHERE node=?) LIMIT ?)").bind(node).bind(node).bind(cutoff).bind(node).bind(node).bind(budget).execute(&mut *tx).await?,
            };
            removed += result.rows_affected();
            advanced = i + 1;
        }
        sqlx::query("UPDATE registry_status SET cursor=? WHERE id=1")
            .bind(if slots == 0 {
                0
            } else {
                ((cursor as usize + advanced) % slots) as i64
            })
            .execute(&mut *tx)
            .await?;
        removed+=sqlx::query("DELETE FROM nodes WHERE uid IN (SELECT uid FROM nodes WHERE lifecycle='deleted' AND last_seen<? AND NOT EXISTS(SELECT 1 FROM samples WHERE node=nodes.uid) AND NOT EXISTS(SELECT 1 FROM rollups WHERE node=nodes.uid) AND NOT EXISTS(SELECT 1 FROM metric_keys WHERE node=nodes.uid) LIMIT ?)").bind(cutoff).bind((1000-removed) as i64).execute(&mut *tx).await?.rows_affected();
        tx.commit().await?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(i: u128) -> Node {
        Node {
            uid: uuid::Uuid::from_u128(i + 1).to_string(),
            name: format!("node-{i}"),
            lifecycle: "present".into(),
            provider: "kubernetes".into(),
            capabilities: vec![],
        }
    }
    #[tokio::test]
    async fn v1_native_history_migrates_to_only_approved_uid() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let file = dir.path().join("legacy");
        let path = file.to_str().unwrap();
        let store = Store::open(path, 0).await?;
        sqlx::query("ALTER TABLE samples DROP COLUMN provider")
            .execute(&store.pool)
            .await?;
        sqlx::query("PRAGMA user_version=1")
            .execute(&store.pool)
            .await?;
        let time = now_ms();
        sqlx::query("INSERT INTO samples VALUES ('alpha','boot','process',0,?,?,?,?)")
            .bind(time)
            .bind(time)
            .bind(r#"{"cpu.used_percent":42}"#)
            .bind(r#"{"uuid":null,"reason":"native"}"#)
            .execute(&store.pool)
            .await?;
        store.pool.close().await;
        let store = Store::open(path, 0).await?;
        let mut approved = node(0);
        approved.name = "alpha".into();
        approved.provider = "native".into();
        store.reconcile(&[approved.clone()]).await?;
        assert!(store.latest("alpha").await?.is_none());
        assert_eq!(
            store.latest(&approved.uid).await?.unwrap().metrics["cpu.used_percent"],
            Some(42.)
        );
        assert_eq!(store.inventory().await?[0].last_sample, Some(time));
        let provider: String = sqlx::query_scalar("SELECT provider FROM samples")
            .fetch_one(&store.pool)
            .await?;
        assert_eq!(provider, "native");
        Ok(())
    }
    #[tokio::test]
    async fn registry_capacity_and_global_fair_maintenance_budget() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(dir.path().join("db").to_str().unwrap(), 0).await?;
        let nodes: Vec<_> = (0..64).map(node).collect();
        store.reconcile(&nodes).await?;
        assert!(store
            .reconcile(&(0..65).map(node).collect::<Vec<_>>())
            .await
            .is_err());
        let cutoff = now_ms() - 31 * 86400 * 1000;
        for n in &nodes {
            sqlx::query("WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<29) INSERT INTO samples (node,boot,process,seq,time,received,metrics,gpu,provider) SELECT ?,'boot','process',i,?,?,'{}','{}','kubernetes' FROM n")
    .bind(&n.uid).bind(cutoff).bind(now_ms()).execute(&store.pool).await?;
            sqlx::query("WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<29) INSERT INTO rollups SELECT ?,?+i,'cpu.used_percent',42,1 FROM n").bind(&n.uid).bind(cutoff).execute(&store.pool).await?;
        }
        for _ in 0..10 {
            let before: i64 = sqlx::query_scalar(
                "SELECT (SELECT count(*) FROM samples)+(SELECT count(*) FROM rollups)",
            )
            .fetch_one(&store.pool)
            .await?;
            let removed = store.maintain_registry().await?;
            assert!(removed <= 1000);
            let after: i64 = sqlx::query_scalar(
                "SELECT (SELECT count(*) FROM samples)+(SELECT count(*) FROM rollups)",
            )
            .fetch_one(&store.pool)
            .await?;
            assert_eq!(before - after, removed as i64);
        }
        let remaining: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM samples)+(SELECT count(*) FROM rollups)",
        )
        .fetch_one(&store.pool)
        .await?;
        assert_eq!(remaining, 0);
        // 256 retained UIDs are allowed across completed discovery generations.
        for group in 1..4 {
            store
                .reconcile(&(group * 64..(group + 1) * 64).map(node).collect::<Vec<_>>())
                .await?;
        }
        assert_eq!(store.inventory().await?.len(), 256);
        assert!(store.reconcile(&[node(256)]).await.is_err());
        assert_eq!(store.inventory().await?.len(), 256);
        Ok(())
    }
}
