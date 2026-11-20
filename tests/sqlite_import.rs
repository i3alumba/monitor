mod common;
use anyhow::Result;
use resource_monitor::{
    domain::{now_ms, Sample},
    import::{self, ImportArgs},
    store::Store,
};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row,
};
use std::path::{Path, PathBuf};
const UID: &str = "00000000-0000-0000-0000-000000000001";
async fn snapshot(dir: &Path, samples: u32) -> Result<PathBuf> {
    let path = dir.join("snapshot.sqlite3");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true),
        )
        .await?;
    for sql in [
        "CREATE TABLE samples (node TEXT NOT NULL,boot TEXT NOT NULL,process TEXT NOT NULL,seq INTEGER NOT NULL,time INTEGER NOT NULL,received INTEGER NOT NULL,metrics TEXT NOT NULL,gpu TEXT NOT NULL,provider TEXT NOT NULL,PRIMARY KEY(node,boot,process,seq))",
        "CREATE INDEX samples_time ON samples(node,time)",
        "CREATE TABLE rollups (node TEXT NOT NULL,time INTEGER NOT NULL,key TEXT NOT NULL,total REAL NOT NULL,count INTEGER NOT NULL,PRIMARY KEY(node,time,key))",
        "CREATE TABLE metric_keys (node TEXT NOT NULL,key TEXT NOT NULL,PRIMARY KEY(node,key))",
        "CREATE TABLE nodes (uid TEXT PRIMARY KEY,name TEXT NOT NULL,lifecycle TEXT NOT NULL,provider TEXT NOT NULL,capabilities TEXT NOT NULL,first_seen INTEGER NOT NULL,last_seen INTEGER NOT NULL,last_sample INTEGER)",
        "CREATE TABLE registry_status (id INTEGER PRIMARY KEY,error TEXT,cursor INTEGER NOT NULL)",
        "INSERT INTO registry_status VALUES (1,'test discovery diagnostic',17)",
        "PRAGMA user_version=2",
    ] { sqlx::query(sql).execute(&pool).await?; }
    let now = now_ms();
    sqlx::query("INSERT INTO nodes VALUES (?,'alpha','present','native','[\"cpu\"]',?,?,?)")
        .bind(UID)
        .bind(now - 60_000)
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await?;
    for key in [
        "cpu.used_percent",
        "gpu.nvidia.temperature_celsius",
        "net.old_key",
    ] {
        sqlx::query("INSERT INTO metric_keys VALUES (?,?)")
            .bind(UID)
            .bind(key)
            .execute(&pool)
            .await?;
    }
    for i in 0..samples {
        sqlx::query("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?)")
            .bind(UID)
            .bind(uuid::Uuid::nil().to_string())
            .bind(uuid::Uuid::nil().to_string())
            .bind(i64::MAX - i64::from(samples) + i64::from(i))
            .bind(now - i64::from(samples - i) * 10_000)
            .bind(now - 1)
            .bind(r#"{"cpu.used_percent":42.0,"gpu.nvidia.temperature_celsius":null}"#)
            .bind(r#"{"uuid":null,"reason":"unavailable"}"#)
            .bind("native")
            .execute(&pool)
            .await?;
    }
    // Deliberately differs from raw-derived totals. Import must preserve it.
    sqlx::query("INSERT INTO rollups VALUES (?,?,?,700,7),(?,?,?,0,0)")
        .bind(UID)
        .bind(now / 300_000 * 300_000)
        .bind("cpu.used_percent")
        .bind(UID)
        .bind(now / 300_000 * 300_000)
        .bind("gpu.nvidia.temperature_celsius")
        .execute(&pool)
        .await?;
    pool.close().await;
    Ok(path)
}
fn args(
    path: &Path,
    attempt: uuid::Uuid,
    resume: bool,
    verify_only: bool,
    activate: bool,
    takeover: bool,
) -> Result<ImportArgs> {
    Ok(ImportArgs {
        snapshot: path.into(),
        sha256: import::snapshot_digest(path)?,
        attempt,
        resume,
        verify_only,
        activate,
        takeover,
    })
}
async fn conn(f: &common::Fixture) -> Result<redis::aio::MultiplexedConnection> {
    Ok(redis::Client::open(f.url.as_str())?
        .get_multiplexed_async_connection()
        .await?)
}
async fn get(f: &common::Fixture, k: &str) -> Result<Option<String>> {
    Ok(redis::cmd("GET")
        .arg(k)
        .query_async(&mut conn(f).await?)
        .await?)
}
async fn generation(f: &common::Fixture) -> Result<String> {
    let lock: serde_json::Value =
        serde_json::from_str(&get(f, "monitor:v1:import").await?.unwrap())?;
    Ok(lock["generation"].as_str().unwrap().into())
}
#[tokio::test]
async fn full_import_verify_activate_original_aggregates_and_source_integrity() -> Result<()> {
    let f = common::Fixture::new().await?;
    let path = snapshot(f.path(), 6).await?;
    let before = import::snapshot_digest(&path)?;
    let attempt = uuid::Uuid::new_v4();
    import::run(args(&path, attempt, false, false, false, false)?, &f.url).await?;
    let generation = generation(&f).await?;
    let store = Store::open(&f.url).await?;
    assert!(store.ready().await.is_err());
    assert!(store.inventory().await.is_err());
    import::run(args(&path, attempt, true, true, false, false)?, &f.url).await?;
    import::run(args(&path, attempt, true, false, true, false)?, &f.url).await?;
    store.ready().await?;
    assert_eq!(
        store.registry_error().await?.as_deref(),
        Some("test discovery diagnostic")
    );
    let inventory = store.inventory().await?;
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].node.uid, UID);
    assert_eq!(inventory[0].node.provider, "native");
    assert_eq!(
        store.latest(UID).await?.unwrap().metrics["cpu.used_percent"],
        Some(42.)
    );
    let history = store.history(UID, 720).await?;
    assert!(history
        .points
        .iter()
        .any(|p| p.metrics.get("cpu.used_percent") == Some(&Some(100.))));
    let proof_text = get(&f, "monitor:v1:import-proof").await?.unwrap();
    let proof: serde_json::Value = serde_json::from_str(&proof_text)?;
    assert_eq!(proof["manifest"]["tables"]["samples"]["retained_rows"], 6);
    assert_eq!(proof["manifest"]["tables"]["metric_keys"]["rows"], 3);
    assert_eq!(proof["import_status"], "verified");
    let k = format!("monitor:v1:{generation}:control:{UID}:0");
    let control = get(&f, &k).await?.unwrap();
    assert!(control.contains("net.old_key"));
    assert!(control.contains(&(i64::MAX - 1).to_string()));
    let raw_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&path)
                .read_only(true)
                .immutable(true),
        )
        .await?;
    let row = sqlx::query("SELECT * FROM samples ORDER BY seq DESC LIMIT 1")
        .fetch_one(&raw_pool)
        .await?;
    let replay = Sample {
        node: UID.into(),
        boot_id: row.try_get("boot")?,
        process_id: row.try_get("process")?,
        seq: row.try_get("seq")?,
        timestamp_ms: row.try_get("time")?,
        metrics: serde_json::from_str(row.try_get("metrics")?)?,
        gpu: serde_json::from_str(row.try_get("gpu")?)?,
    };
    store.insert(&[replay]).await?;
    assert_eq!(
        get(&f, "monitor:v1:import-proof").await?.unwrap(),
        proof_text
    );
    assert_eq!(import::snapshot_digest(&path)?, before);
    assert!(!PathBuf::from(format!("{}-wal", path.display())).exists());
    assert!(!PathBuf::from(format!("{}-shm", path.display())).exists());
    raw_pool.close().await;
    Ok(())
}
#[tokio::test]
async fn mismatch_corruption_and_idempotent_resume_are_fail_closed() -> Result<()> {
    let f = common::Fixture::new().await?;
    let path = snapshot(f.path(), 2).await?;
    let attempt = uuid::Uuid::new_v4();
    let mut wrong = args(&path, attempt, false, false, false, false)?;
    wrong.sha256 = "0".repeat(64);
    assert!(import::run(wrong, &f.url).await.is_err());
    assert!(get(&f, "monitor:v1:import").await?.is_none());
    import::run(args(&path, attempt, false, false, false, false)?, &f.url).await?;
    assert!(import::run(
        args(&path, uuid::Uuid::new_v4(), true, true, false, false)?,
        &f.url
    )
    .await
    .is_err());
    let generation = generation(&f).await?;
    let control = format!("monitor:v1:{generation}:control:{UID}:0");
    let _: () = redis::cmd("SET")
        .arg(&control)
        .arg("corrupt")
        .query_async(&mut conn(&f).await?)
        .await?;
    assert!(
        import::run(args(&path, attempt, true, true, false, false)?, &f.url)
            .await
            .is_err()
    );
    assert!(Store::open(&f.url).await?.ready().await.is_err());
    assert!(
        import::run(args(&path, attempt, true, false, false, false)?, &f.url)
            .await
            .is_err()
    ); // crashed owner remains
    import::run(args(&path, attempt, true, false, false, true)?, &f.url).await?;
    import::run(args(&path, attempt, true, false, true, false)?, &f.url).await?;
    assert!(
        import::run(args(&path, attempt, false, false, false, false)?, &f.url)
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn interrupted_transfer_requires_explicit_takeover_and_same_run_id() -> Result<()> {
    let f = common::Fixture::new().await?;
    let path = snapshot(f.path(), 512).await?;
    let attempt = uuid::Uuid::new_v4();
    let input = args(&path, attempt, false, false, false, false)?;
    let url = f.url.clone();
    let worker = tokio::spawn(async move { import::run(input, &url).await });
    for _ in 0..1000 {
        if get(&f, "monitor:v1:import").await?.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    worker.abort();
    let _ = worker.await;
    assert!(Store::open(&f.url).await?.ready().await.is_err());
    assert!(
        import::run(args(&path, attempt, true, false, false, false)?, &f.url)
            .await
            .is_err()
    );
    import::run(args(&path, attempt, true, false, false, true)?, &f.url).await?;
    let mut lock: serde_json::Value =
        serde_json::from_str(&get(&f, "monitor:v1:import").await?.unwrap())?;
    lock["run_id"] = "stale-run-id".into();
    let _: () = redis::cmd("SET")
        .arg("monitor:v1:import")
        .arg(lock.to_string())
        .query_async(&mut conn(&f).await?)
        .await?;
    assert!(
        import::run(args(&path, attempt, true, true, false, false)?, &f.url)
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn eviction_capacity_failure_cannot_activate_partial_import() -> Result<()> {
    let f = common::Fixture::new().await?;
    let path = snapshot(f.path(), 512).await?;
    let attempt = uuid::Uuid::new_v4();
    import::run(args(&path, attempt, false, false, false, false)?, &f.url).await?;
    let mut c = conn(&f).await?;
    let _: () = redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory", "16mb"])
        .query_async(&mut c)
        .await?;
    for i in 0..96 {
        let _: () = redis::cmd("SET")
            .arg(format!("pressure:{i}"))
            .arg(vec![b'x'; 256 * 1024])
            .arg(&["PX", "60000"])
            .query_async(&mut c)
            .await?;
    }
    assert!(
        import::run(args(&path, attempt, true, false, true, false)?, &f.url)
            .await
            .is_err()
    );
    assert!(get(&f, "monitor:v1:active").await?.is_none());
    assert!(Store::open(&f.url).await?.ready().await.is_err());
    Ok(())
}
#[tokio::test]
async fn rejects_unknown_schema_and_snapshot_sidecars_without_source_changes() -> Result<()> {
    let f = common::Fixture::new().await?;
    let path = snapshot(f.path(), 1).await?;
    let before = import::snapshot_digest(&path)?;
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    std::fs::write(&wal, [])?;
    assert!(import::run(
        args(&path, uuid::Uuid::new_v4(), false, false, false, false)?,
        &f.url
    )
    .await
    .is_err());
    assert_eq!(before, import::snapshot_digest(&path)?);
    std::fs::remove_file(wal)?;
    let pool = SqlitePoolOptions::new()
        .connect_with(SqliteConnectOptions::new().filename(&path))
        .await?;
    sqlx::query("PRAGMA user_version=3").execute(&pool).await?;
    pool.close().await;
    assert!(import::run(
        args(&path, uuid::Uuid::new_v4(), false, false, false, false)?,
        &f.url
    )
    .await
    .is_err());
    Ok(())
}

#[test]
fn receive_snapshot_bounds_digest_and_no_overwrite() -> Result<()> {
    use sha2::{Digest, Sha256};
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    let dir = tempfile::tempdir()?;
    let data = b"isolated snapshot delivery fixture";
    let sha = format!("{:x}", Sha256::digest(data));
    let run = |output: &Path, sha: &str, limit: &str| -> Result<bool> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_resource-monitor"))
            .args([
                "receive-snapshot",
                "--output",
                output.to_str().unwrap(),
                "--sha256",
                sha,
                "--max-bytes",
                limit,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        child.stdin.take().unwrap().write_all(data)?;
        Ok(child.wait()?.success())
    };
    let output = dir.path().join("final.sqlite3");
    assert!(!run(&output, &sha, "1")?);
    assert!(!output.exists());
    assert!(!run(&output, &"0".repeat(64), "100")?);
    assert!(!output.exists());
    assert!(run(&output, &sha, "100")?);
    assert_eq!(std::fs::read(&output)?, data);
    assert!(!run(&output, &sha, "100")?);
    assert_eq!(std::fs::read(&output)?, data);
    assert_eq!(std::fs::read_dir(dir.path())?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn all_fenced_rows_and_lifecycles_survive_fixed_grace_activation_and_maintenance(
) -> Result<()> {
    const ARCHIVED: &str = "00000000-0000-0000-0000-000000000002";
    let f = common::Fixture::new().await?;
    let path = snapshot(f.path(), 2).await?;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(SqliteConnectOptions::new().filename(&path))
        .await?;
    let now = now_ms();
    let retention = 30 * 86400 * 1000;
    let ancient = now - 31 * 86400 * 1000;
    let aging = now - retention + 50;
    sqlx::query("INSERT INTO nodes VALUES (?,'archived','deleted','kubernetes','[]',?,?,?)")
        .bind(ARCHIVED)
        .bind(ancient - 1000)
        .bind(ancient)
        .bind(aging)
        .execute(&pool)
        .await?;
    for (uid, state) in [
        ("00000000-0000-0000-0000-000000000003", "unsupported"),
        ("00000000-0000-0000-0000-000000000004", "not-ready"),
    ] {
        sqlx::query("INSERT INTO nodes VALUES (?,? ,?,'kubernetes','[]',?,?,NULL)")
            .bind(uid)
            .bind(state)
            .bind(state)
            .bind(now)
            .bind(now)
            .execute(&pool)
            .await?;
    }
    sqlx::query("INSERT INTO metric_keys VALUES (?,'cpu.used_percent')")
        .bind(ARCHIVED)
        .execute(&pool)
        .await?;
    for (seq, time) in [(10, ancient), (11, aging)] {
        sqlx::query("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,'kubernetes')")
            .bind(ARCHIVED)
            .bind(uuid::Uuid::nil().to_string())
            .bind(uuid::Uuid::nil().to_string())
            .bind(seq)
            .bind(time)
            .bind(time + 1)
            .bind(r#"{"cpu.used_percent":21.0}"#)
            .bind(r#"{"uuid":null,"reason":"historic"}"#)
            .execute(&pool)
            .await?;
        sqlx::query("INSERT INTO rollups VALUES (?,?,'cpu.used_percent',123,3)")
            .bind(ARCHIVED)
            .bind(time / 300_000 * 300_000)
            .execute(&pool)
            .await?;
    }
    pool.close().await;
    let digest = import::snapshot_digest(&path)?;
    let attempt = uuid::Uuid::new_v4();
    import::run(args(&path, attempt, false, false, false, false)?, &f.url).await?;
    let lock: serde_json::Value =
        serde_json::from_str(&get(&f, "monitor:v1:import").await?.unwrap())?;
    let grace = lock["grace_until_ms"].as_i64().unwrap();
    assert_eq!(grace - lock["started_ms"].as_i64().unwrap(), 6 * 3_600_000);
    // The near-boundary row ages beyond the normal horizon while fenced.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(aging + retention < now_ms());
    import::run(args(&path, attempt, true, true, false, false)?, &f.url).await?;
    let resumed: serde_json::Value =
        serde_json::from_str(&get(&f, "monitor:v1:import").await?.unwrap())?;
    assert_eq!(resumed["grace_until_ms"], grace);
    let generation = generation(&f).await?;
    import::run(args(&path, attempt, true, false, true, false)?, &f.url).await?;
    let proof: serde_json::Value =
        serde_json::from_str(&get(&f, "monitor:v1:import-proof").await?.unwrap())?;
    for (table, count) in [
        ("samples", 4),
        ("rollups", 4),
        ("nodes", 4),
        ("metric_keys", 4),
        ("registry_status", 1),
    ] {
        assert_eq!(proof["manifest"]["tables"][table]["rows"], count);
        assert_eq!(proof["manifest"]["tables"][table]["retained_rows"], count);
    }
    assert_eq!(proof["manifest"]["tables"]["samples"]["min_time"], ancient);
    let mut c = conn(&f).await?;
    for time in [ancient, aging] {
        for (kind, step) in [("raw", 10_000), ("rollup", 300_000)] {
            let key = format!(
                "monitor:v1:{generation}:{kind}:{ARCHIVED}:{}",
                time / step * step
            );
            assert!(get(&f, &key).await?.is_some());
            let expiry: i64 = redis::cmd("PEXPIRETIME")
                .arg(&key)
                .query_async(&mut c)
                .await?;
            assert_eq!(expiry, grace);
        }
    }
    let store = Store::open(&f.url).await?;
    store.retain().await?;
    let inventory = store.inventory().await?;
    assert_eq!(inventory.len(), 4);
    assert!(inventory.iter().any(|n| n.node.lifecycle == "unsupported"));
    assert!(inventory.iter().any(|n| n.node.lifecycle == "not-ready"));
    let archived = inventory.iter().find(|n| n.node.uid == ARCHIVED).unwrap();
    assert_eq!(archived.last_seen, ancient);
    assert_eq!(archived.last_sample, Some(aging));
    let control_key = format!("monitor:v1:{generation}:control:{ARCHIVED}:0");
    let control: serde_json::Value = serde_json::from_str(&get(&f, &control_key).await?.unwrap())?;
    assert!(control["sessions"]
        .as_object()
        .unwrap()
        .values()
        .all(|s| s["until"].as_i64().unwrap() >= grace));
    let mut live = archived.node.clone();
    live.lifecycle = "present".into();
    store.reconcile(&[live]).await?;
    let replay = Sample {
        node: ARCHIVED.into(),
        boot_id: uuid::Uuid::nil().to_string(),
        process_id: uuid::Uuid::nil().to_string(),
        seq: 11,
        timestamp_ms: now_ms(),
        metrics: std::collections::BTreeMap::from([("cpu.used_percent".into(), Some(21.))]),
        gpu: Default::default(),
    };
    store.insert(&[replay]).await?;
    let latest_key = format!("monitor:v1:{generation}:latest:{ARCHIVED}:0");
    let expiry: i64 = redis::cmd("PEXPIRETIME")
        .arg(&latest_key)
        .query_async(&mut c)
        .await?;
    assert_eq!(expiry, grace);
    assert_eq!(store.latest(ARCHIVED).await?.unwrap().timestamp_ms, aging);
    assert_eq!(import::snapshot_digest(&path)?, digest);
    Ok(())
}
