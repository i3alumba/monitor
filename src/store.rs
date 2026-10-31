use crate::domain::{now_ms, GpuStatus, Metrics, Sample};
use anyhow::{bail, Result};
use serde::Serialize;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
#[derive(Clone)]
pub struct Store {
    pub(crate) pool: SqlitePool,
    path: PathBuf,
    pub reserve: u64,
}
#[derive(Serialize)]
pub struct Point {
    pub timestamp_ms: i64,
    pub metrics: Metrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuStatus>,
}
#[derive(Serialize)]
pub struct History {
    pub node: String,
    pub bucket_ms: i64,
    pub points: Vec<Point>,
}
#[allow(clippy::useless_conversion)] // GNU statvfs field widths differ on 32-bit targets.
pub fn available(path: &Path) -> Result<u64> {
    let name = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())?;
    let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: name is NUL terminated; statvfs initializes info on success.
    if unsafe { libc::statvfs(name.as_ptr(), info.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful statvfs initialized the structure.
    let info = unsafe { info.assume_init() };
    Ok(u64::from(info.f_bavail).saturating_mul(u64::from(info.f_frsize)))
}
impl Store {
    pub async fn open(path: &str, reserve: u64) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(path)?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(2))
            .pragma("max_page_count", "2097152") // 8 GiB at 4096-byte pages
            .pragma("journal_size_limit", "16777216")
            .pragma("wal_autocheckpoint", "256");
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
            .fetch_one(&pool)
            .await?;
        if page_size != 4096 {
            bail!("database page size must be 4096");
        }
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await?;
        if version > 2 {
            bail!("unsupported database schema");
        }
        sqlx::query("CREATE TABLE IF NOT EXISTS samples (node TEXT NOT NULL, boot TEXT NOT NULL, process TEXT NOT NULL, seq INTEGER NOT NULL, time INTEGER NOT NULL, received INTEGER NOT NULL, metrics TEXT NOT NULL, gpu TEXT NOT NULL, PRIMARY KEY(node,boot,process,seq))").execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS samples_time ON samples(node,time)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS metric_keys (node TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(node,key))").execute(&pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS rollups (node TEXT NOT NULL, time INTEGER NOT NULL, key TEXT NOT NULL, total REAL NOT NULL, count INTEGER NOT NULL, PRIMARY KEY(node,time,key))").execute(&pool).await?;
        let mut migration = pool.begin().await?;
        if version < 2 {
            sqlx::query("ALTER TABLE samples ADD COLUMN provider TEXT NOT NULL DEFAULT 'native'")
                .execute(&mut *migration)
                .await?;
        }
        sqlx::query("CREATE TABLE IF NOT EXISTS nodes (uid TEXT PRIMARY KEY,name TEXT NOT NULL,lifecycle TEXT NOT NULL,provider TEXT NOT NULL,capabilities TEXT NOT NULL,first_seen INTEGER NOT NULL,last_seen INTEGER NOT NULL,last_sample INTEGER)").execute(&mut *migration).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS registry_status (id INTEGER PRIMARY KEY,error TEXT,cursor INTEGER NOT NULL)").execute(&mut *migration).await?;
        sqlx::query("INSERT OR IGNORE INTO registry_status VALUES (1,NULL,0)")
            .execute(&mut *migration)
            .await?;
        sqlx::query("PRAGMA user_version=2")
            .execute(&mut *migration)
            .await?;
        migration.commit().await?;
        Ok(Self {
            pool,
            path: path.into(),
            reserve,
        })
    }
    pub async fn insert(&self, samples: &[Sample]) -> Result<()> {
        let parent = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if available(parent)? < self.reserve {
            bail!("storage reserve reached");
        }
        let wal = PathBuf::from(format!("{}-wal", self.path.display()));
        if std::fs::metadata(wal).is_ok_and(|m| m.len() > 32 * 1024 * 1024) {
            bail!("WAL pressure");
        }
        let mut tx = self.pool.begin().await?;
        for s in samples {
            let last: Option<i64> = sqlx::query_scalar(
                "SELECT max(seq) FROM samples WHERE node=? AND boot=? AND process=?",
            )
            .bind(&s.node)
            .bind(&s.boot_id)
            .bind(&s.process_id)
            .fetch_one(&mut *tx)
            .await?;
            // Out-of-order retry batches are safe: older already-seen identities are ignored.
            if last.is_some_and(|last| s.seq < last - 12) {
                bail!("sequence outside retry window");
            }
            for key in s.metrics.keys() {
                sqlx::query("INSERT OR IGNORE INTO metric_keys VALUES (?,?)")
                    .bind(&s.node)
                    .bind(key)
                    .execute(&mut *tx)
                    .await?;
            }
            let keys: i64 = sqlx::query_scalar("SELECT count(*) FROM metric_keys WHERE node=?")
                .bind(&s.node)
                .fetch_one(&mut *tx)
                .await?;
            if keys > 128 {
                bail!("persistent metric cardinality exceeded");
            }
            let inserted = sqlx::query("INSERT OR IGNORE INTO samples (node,boot,process,seq,time,received,metrics,gpu,provider) VALUES (?,?,?,?,?,?,?,?,coalesce((SELECT provider FROM nodes WHERE uid=?),'native'))")
                .bind(&s.node)
                .bind(&s.boot_id)
                .bind(&s.process_id)
                .bind(s.seq)
                .bind(s.timestamp_ms)
                .bind(now_ms())
                .bind(serde_json::to_string(&s.metrics)?)
                .bind(serde_json::to_string(&s.gpu)?)
                .bind(&s.node)
                .execute(&mut *tx)
                .await?
                .rows_affected();
            if inserted == 1 {
                for (key, value) in &s.metrics {
                    sqlx::query("INSERT INTO rollups VALUES (?,?,?,?,?) ON CONFLICT(node,time,key) DO UPDATE SET total=total+excluded.total,count=count+excluded.count")
                        .bind(&s.node).bind(s.timestamp_ms / 300_000 * 300_000).bind(key).bind(value.unwrap_or(0.)).bind(i64::from(value.is_some())).execute(&mut *tx).await?;
                }
            }
        }
        for s in samples {
            sqlx::query("UPDATE nodes SET last_sample=max(coalesce(last_sample,0),?) WHERE uid=?")
                .bind(s.timestamp_ms)
                .bind(&s.node)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn retain(&self) -> Result<()> {
        self.maintain_registry().await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn history(&self, node: &str, hours: u32) -> Result<History> {
        let end = now_ms();
        let start = end - i64::from(hours) * 3_600_000;
        let bucket = if hours > 6 {
            ((i64::from(hours) * 3_600_000 + 1198) / 1199 + 299_999) / 300_000 * 300_000
        } else {
            ((i64::from(hours) * 3_600_000 + 1198) / 1199).max(10_000)
        };
        // Persistent five-minute aggregates bound long-range queries; raw samples preserve short gaps.
        let rows = if hours > 6 {
            sqlx::query("SELECT ((time-?)/?) AS bucket, key, sum(total)/nullif(sum(count),0) AS value FROM rollups WHERE node=? AND time>=? AND time<=? GROUP BY bucket,key ORDER BY bucket LIMIT 153600")
                .bind(start).bind(bucket).bind(node).bind(start).bind(end).fetch_all(&self.pool).await?
        } else {
            sqlx::query("SELECT ((time-?)/?) AS bucket, j.key AS key, avg(CASE WHEN j.type IN ('integer','real') THEN j.value END) AS value FROM samples, json_each(samples.metrics) j WHERE node=? AND time>=? AND time<=? GROUP BY bucket,j.key ORDER BY bucket LIMIT 153600")
                .bind(start).bind(bucket).bind(node).bind(start).bind(end).fetch_all(&self.pool).await?
        };
        let mut points = BTreeMap::<i64, Metrics>::new();
        for row in rows {
            let index: i64 = row.try_get("bucket")?;
            points
                .entry(index)
                .or_default()
                .insert(row.try_get("key")?, row.try_get("value")?);
        }
        // Explicit empty buckets mark missing observations, never fabricated zeroes.
        let points = (0..=((end - start) / bucket))
            .map(|i| Point {
                timestamp_ms: start + i * bucket,
                metrics: points.remove(&i).unwrap_or_default(),
                gpu: None,
            })
            .collect();
        Ok(History {
            node: node.into(),
            bucket_ms: bucket,
            points,
        })
    }
    pub async fn latest(&self, node: &str) -> Result<Option<Point>> {
        let row = sqlx::query(
            "SELECT time,metrics,gpu FROM samples WHERE node=? ORDER BY time DESC LIMIT 1",
        )
        .bind(node)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            Ok(Point {
                timestamp_ms: r.try_get("time")?,
                metrics: serde_json::from_str(r.try_get("metrics")?)?,
                gpu: Some(serde_json::from_str(r.try_get("gpu")?)?),
            })
        })
        .transpose()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn persistence_dedup_restart() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db.sqlite3");
        let path = path.to_str().ok_or_else(|| anyhow::anyhow!("path"))?;
        let s = Sample {
            node: "alpha".into(),
            boot_id: uuid::Uuid::new_v4().to_string(),
            process_id: uuid::Uuid::new_v4().to_string(),
            seq: 0,
            timestamp_ms: now_ms(),
            gpu: GpuStatus::default(),
            metrics: BTreeMap::from([("cpu.used_percent".into(), Some(42.))]),
        };
        let store = Store::open(path, 0).await?;
        store.insert(&[s.clone(), s]).await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM samples")
            .fetch_one(&store.pool)
            .await?;
        assert_eq!(count, 1);
        store.pool.close().await;
        let store = Store::open(path, 0).await?;
        assert!(store.latest("alpha").await?.is_some());
        let history = store.history("alpha", 720).await?;
        assert!(history.points.len() <= 1200);
        assert!(history.points.iter().any(|p| p.metrics.is_empty()));
        Ok(())
    }
}

#[cfg(test)]
mod safety_tests {
    use super::*;
    fn sample(seq: i64) -> Sample {
        Sample {
            node: "alpha".into(),
            boot_id: uuid::Uuid::nil().to_string(),
            process_id: uuid::Uuid::nil().to_string(),
            seq,
            timestamp_ms: now_ms(),
            metrics: BTreeMap::from([("cpu.used_percent".into(), Some(42.))]),
            gpu: GpuStatus::default(),
        }
    }
    #[tokio::test]
    async fn retention_uses_node_time_indexes_with_full_retained_history() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("retention.sqlite3");
        let store = Store::open(path.to_str().ok_or_else(|| anyhow::anyhow!("path"))?, 0).await?;
        sqlx::query("INSERT INTO metric_keys VALUES ('alpha','cpu.used_percent'),('gamma','cpu.used_percent')").execute(&store.pool).await?;
        let now = now_ms();
        // Same full 30-day sample count, narrow payload: maintenance depends on row count,
        // not on metric width. Do not regenerate the multi-GiB history/load fixture.
        sqlx::query("WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<518399) INSERT INTO samples (node,boot,process,seq,time,received,metrics,gpu) SELECT CASE WHEN i%2=0 THEN 'alpha' ELSE 'gamma' END,'boot','process',i,?- (i/2)*9990,?,'{}','{}' FROM n")
            .bind(now).bind(now).execute(&store.pool).await?;
        sqlx::query("WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<17279) INSERT INTO rollups SELECT CASE WHEN i%2=0 THEN 'alpha' ELSE 'gamma' END,?-(i/2)*299000,'cpu.used_percent',42,1 FROM n")
            .bind(now).execute(&store.pool).await?;
        for table in ["samples", "rollups"] {
            let plan = sqlx::query(&format!(
                "EXPLAIN QUERY PLAN SELECT rowid FROM {table} WHERE node=? AND time < ? LIMIT 500"
            ))
            .bind("alpha")
            .bind(now - 30 * 86400 * 1000)
            .fetch_all(&store.pool)
            .await?;
            let details: Vec<String> = plan
                .iter()
                .map(|row| row.try_get("detail"))
                .collect::<Result<_, _>>()?;
            assert!(
                details
                    .iter()
                    .any(|d| d.contains("SEARCH") && d.contains("node=? AND time<?")),
                "{table}: {details:?}"
            );
            assert!(
                !details.iter().any(|d| d.contains("SCAN")),
                "{table}: {details:?}"
            );
            eprintln!("retention {table} plan: {details:?}");
        }
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&store.pool)
            .await?;
        let before = std::time::Instant::now();
        store.retain().await?;
        let elapsed = before.elapsed();
        eprintln!("no-expiry retention with 518400 samples and 17280 rollups: {elapsed:?}");
        assert!(elapsed < Duration::from_secs(2));
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM samples")
            .fetch_one(&store.pool)
            .await?;
        assert_eq!(count, 518400);
        Ok(())
    }
    #[tokio::test]
    async fn retention_rollup_dedup_and_cardinality_rollback() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = Store::open(
            dir.path()
                .join("db")
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("path"))?,
            0,
        )
        .await?;
        let mut old = sample(0);
        old.timestamp_ms -= 31 * 86400 * 1000;
        store.insert(&[old.clone(), old]).await?;
        let count: i64 = sqlx::query_scalar("SELECT sum(count) FROM rollups")
            .fetch_one(&store.pool)
            .await?;
        assert_eq!(count, 1);
        store.retain().await?;
        assert!(store.latest("alpha").await?.is_none());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM rollups")
            .fetch_one(&store.pool)
            .await?;
        assert_eq!(count, 0);
        let mut full = sample(1);
        for i in 0..127 {
            full.metrics.insert(format!("net.metric{i}"), Some(1.));
        }
        store.insert(&[full]).await?;
        let mut overflow = sample(2);
        overflow.metrics.insert("net.extra".into(), Some(1.));
        assert!(store.insert(&[overflow]).await.is_err());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM metric_keys")
            .fetch_one(&store.pool)
            .await?;
        assert_eq!(count, 128);
        Ok(())
    }
    #[tokio::test]
    async fn sqlite_full_rolls_back_and_reopens() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("db");
        let path = path.to_str().ok_or_else(|| anyhow::anyhow!("path"))?;
        let store = Store::open(path, 0).await?;
        store.insert(&[sample(0)]).await?;
        let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
            .fetch_one(&store.pool)
            .await?;
        sqlx::query(&format!("PRAGMA max_page_count={pages}"))
            .execute(&store.pool)
            .await?;
        let mut huge = sample(1);
        for i in 0..127 {
            huge.metrics
                .insert(format!("net.{}.{i}", "x".repeat(80)), Some(1.));
        }
        assert!(store.insert(&[huge]).await.is_err());
        store.pool.close().await;
        let store = Store::open(path, 0).await?;
        assert_eq!(
            store
                .latest("alpha")
                .await?
                .ok_or_else(|| anyhow::anyhow!("sample missing"))?
                .metrics
                .len(),
            1
        );
        Ok(())
    }
    #[tokio::test]
    #[ignore = "30-day representative load/size fixture; run explicitly"]
    async fn thirty_day_size_and_rollup_latency() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("load.sqlite3");
        let path = path.to_str().ok_or_else(|| anyhow::anyhow!("path"))?;
        let store = Store::open(path, 0).await?;
        let metrics: Metrics = (0..64)
            .map(|i| (format!("net.interface_metric_{i}"), Some(123456789.25)))
            .collect();
        let json = serde_json::to_string(&metrics)?;
        let start = now_ms() - 30 * 86400 * 1000;
        let before = std::time::Instant::now();
        sqlx::query("WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<518399) INSERT INTO samples (node,boot,process,seq,time,received,metrics,gpu) SELECT CASE WHEN i%2=0 THEN 'alpha' ELSE 'gamma' END,'boot','process',i,?+(i/2)*10000,?,?,'{}' FROM n")
            .bind(start).bind(now_ms()).bind(json).execute(&store.pool).await?;
        sqlx::query("INSERT INTO rollups SELECT node,(time/300000)*300000,j.key,sum(j.value),count(j.value) FROM samples,json_each(samples.metrics) j GROUP BY node,(time/300000),j.key").execute(&store.pool).await?;
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&store.pool)
            .await?;
        let size = std::fs::metadata(path)?.len();
        let query = std::time::Instant::now();
        let history = store.history("alpha", 720).await?;
        let latency = query.elapsed();
        eprintln!(
            "518400 samples, 64 metrics: bytes={size}, build={:?}, history={latency:?}, points={}",
            before.elapsed(),
            history.points.len()
        );
        assert!(size < 8 * 1024 * 1024 * 1024);
        assert!(latency < Duration::from_secs(5));
        assert!(history.points.len() <= 1200);
        Ok(())
    }
}

#[cfg(test)]
mod wal_recovery_tests {
    use super::*;
    #[tokio::test]
    async fn committed_wal_survives_crash_snapshot() -> Result<()> {
        let source = tempfile::tempdir()?;
        let recovered = tempfile::tempdir()?;
        let path = source.path().join("db");
        let path = path.to_str().ok_or_else(|| anyhow::anyhow!("path"))?;
        let store = Store::open(path, 0).await?;
        let sample = Sample {
            node: "gamma".into(),
            boot_id: uuid::Uuid::nil().to_string(),
            process_id: uuid::Uuid::nil().to_string(),
            seq: 0,
            timestamp_ms: now_ms(),
            metrics: BTreeMap::new(),
            gpu: GpuStatus::default(),
        };
        store.insert(&[sample]).await?;
        // Copy only durable database+WAL at a quiescent committed boundary, no SHM.
        // This models reopening after loss of the process and its shared-memory index.
        std::fs::copy(path, recovered.path().join("db"))?;
        std::fs::copy(format!("{path}-wal"), recovered.path().join("db-wal"))?;
        let db = Store::open(
            recovered
                .path()
                .join("db")
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("path"))?,
            0,
        )
        .await?;
        assert!(db.latest("gamma").await?.is_some());
        Ok(())
    }
}
