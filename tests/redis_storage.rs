mod common;
use anyhow::Result;
use resource_monitor::{
    domain::{now_ms, Sample},
    kubernetes::Node,
    store::Store,
};
use std::collections::BTreeMap;

const UID: &str = "00000000-0000-0000-0000-000000000001";
fn node(i: u128) -> Node {
    Node {
        uid: uuid::Uuid::from_u128(i + 1).to_string(),
        name: format!("node-{i}"),
        lifecycle: "present".into(),
        provider: "kubernetes".into(),
        capabilities: vec![],
    }
}
fn sample(seq: i64) -> Sample {
    Sample {
        node: UID.into(),
        boot_id: uuid::Uuid::nil().to_string(),
        process_id: uuid::Uuid::nil().to_string(),
        seq,
        timestamp_ms: now_ms(),
        metrics: BTreeMap::from([
            ("cpu.used_percent".into(), Some(42.)),
            ("gpu.nvidia.temperature_celsius".into(), None),
        ]),
        gpu: Default::default(),
    }
}
async fn connection(f: &common::Fixture) -> Result<redis::aio::MultiplexedConnection> {
    Ok(redis::Client::open(f.url.as_str())?
        .get_multiplexed_async_connection()
        .await?)
}
async fn key(f: &common::Fixture, kind: &str, time: i64) -> Result<String> {
    let generation: String = redis::cmd("GET")
        .arg("monitor:v1:active")
        .query_async(&mut connection(f).await?)
        .await?;
    Ok(format!("monitor:v1:{generation}:{kind}:{UID}:{time}"))
}
async fn value(f: &common::Fixture, key: &str) -> Result<Option<String>> {
    Ok(redis::cmd("GET")
        .arg(key)
        .query_async(&mut connection(f).await?)
        .await?)
}
async fn delete(f: &common::Fixture, key: &str) -> Result<()> {
    let _: i64 = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut connection(f).await?)
        .await?;
    Ok(())
}

#[tokio::test]
async fn exact_sequences_atomic_batches_and_eviction_safe_dedup() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let mut s = sample(i64::MAX - 1);
    s.timestamp_ms -= 15_000;
    store.insert(&[s.clone(), s.clone()]).await?;
    let rollup = key(&f, "rollup", s.timestamp_ms / 300_000 * 300_000).await?;
    let before = value(&f, &rollup).await?;
    let raw = key(&f, "raw", s.timestamp_ms / 10_000 * 10_000).await?;
    delete(&f, &raw).await?;
    store.insert(std::slice::from_ref(&s)).await?;
    assert_eq!(value(&f, &rollup).await?, before);
    assert!(store
        .history(UID, 1)
        .await?
        .points
        .iter()
        .all(|p| p.metrics.is_empty()));
    delete(&f, &rollup).await?;
    delete(&f, &key(&f, "latest", 0).await?).await?;
    store.insert(std::slice::from_ref(&s)).await?;
    assert!(value(&f, &rollup).await?.is_none());
    assert!(store.latest(UID).await?.is_none());
    let mut last = s.clone();
    last.seq = i64::MAX;
    last.timestamp_ms += 10_000;
    store.insert(std::slice::from_ref(&last)).await?;
    assert!(store.latest(UID).await?.is_some());
    let control = key(&f, "control", 0).await?;
    let before = value(&f, &control).await?;
    let mut too_old = s.clone();
    too_old.seq = i64::MAX - 13;
    assert!(store.insert(&[last.clone(), too_old]).await.is_err());
    assert_eq!(value(&f, &control).await?, before);
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let store = store.clone();
            let last = last.clone();
            tokio::spawn(async move { store.insert(&[last]).await })
        })
        .collect();
    for task in tasks {
        task.await??;
    }
    let rollup: serde_json::Value = serde_json::from_str(&value(&f, &rollup).await?.unwrap())?;
    assert_eq!(rollup["cpu.used_percent"]["count"], 1);
    let reopened = Store::open(&f.url).await?;
    reopened.insert(&[last]).await?;
    assert_eq!(reopened.inventory().await?[0].node.provider, "kubernetes");
    Ok(())
}
#[tokio::test]
async fn cardinality_chunk_and_overflow_fail_without_partial_write() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let mut s = sample(0);
    s.metrics = (0..128)
        .map(|i| (format!("net.metric{i}"), Some(1.)))
        .collect();
    store.insert(std::slice::from_ref(&s)).await?;
    let control = key(&f, "control", 0).await?;
    let before = value(&f, &control).await?;
    let mut overflow = s.clone();
    overflow.seq = 1;
    overflow.metrics = BTreeMap::from([("net.extra".into(), Some(1.))]);
    assert!(store.insert(&[s, overflow]).await.is_err());
    assert_eq!(value(&f, &control).await?, before);
    let mut s = sample(0);
    s.process_id = uuid::Uuid::new_v4().to_string();
    s.metrics = BTreeMap::from([("net.metric0".into(), Some(f64::MAX))]);
    store.insert(std::slice::from_ref(&s)).await?;
    let before = value(&f, &control).await?;
    s.seq = 1;
    assert!(store.insert(&[s]).await.is_err());
    assert_eq!(value(&f, &control).await?, before);
    // Same timestamp cannot grow a deterministic raw chunk without bound.
    let fresh = common::Fixture::new().await?;
    let store = Store::open(&fresh.url).await?;
    store.reconcile(&[node(0)]).await?;
    let s = sample(0);
    for i in 0..128 {
        let mut s = s.clone();
        s.seq = i;
        store.insert(&[s]).await?;
    }
    let mut s = s;
    s.seq = 128;
    assert!(store.insert(&[s]).await.is_err());
    Ok(())
}
#[tokio::test]
async fn redis_oom_rolls_back_entire_ingest_and_real_eviction_protects_controls() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let s = sample(0);
    store.insert(std::slice::from_ref(&s)).await?;
    let control = key(&f, "control", 0).await?;
    let before = value(&f, &control).await?;
    let mut conn = connection(&f).await?;
    let _: () = redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory", "16mb"])
        .query_async(&mut conn)
        .await?;
    // Many volatile strings force actual maxmemory eviction, not just DEL simulation.
    for i in 0..96 {
        let _: () = redis::cmd("SET")
            .arg(format!("fixture:volatile:{i}"))
            .arg(vec![b'x'; 256 * 1024])
            .arg(&["PX", "60000"])
            .query_async(&mut conn)
            .await?;
    }
    let info: String = redis::cmd("INFO")
        .arg("stats")
        .query_async(&mut conn)
        .await?;
    assert!(info.lines().any(|l| l.starts_with("evicted_keys:")
        && l.trim().split(':').nth(1).unwrap().parse::<u64>().unwrap() > 0));
    assert_eq!(value(&f, &control).await?, before);
    assert!(store.assigned(&node(0), "kubernetes").await?);
    // Fill protected metadata until Redis refuses more allocations. Ingest must
    // not update its control record or resurrect a duplicate after eviction.
    for i in 0..128 {
        let result: redis::RedisResult<()> = redis::cmd("SET")
            .arg(format!("fixture:protected:{i}"))
            .arg(vec![b'p'; 256 * 1024])
            .query_async(&mut conn)
            .await;
        if result.is_err() {
            break;
        }
    }
    for i in 0..512 {
        let result: redis::RedisResult<()> = redis::cmd("SET")
            .arg(format!("fixture:protected-small:{i}"))
            .arg(vec![b'p'; 1024])
            .query_async(&mut conn)
            .await;
        if result.is_err() {
            break;
        }
    }
    store.validate_config().await?; // Still the supported 16MiB volatile-lru configuration.
    let before = value(&f, &control).await?;
    let mut next = s;
    next.seq = 1;
    assert!(store.insert(&[next]).await.is_err());
    assert_eq!(value(&f, &control).await?, before);
    Ok(())
}
#[tokio::test]
async fn registry_capacity_provider_and_staging_are_atomic() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    for group in 0..4 {
        store
            .reconcile(&(group * 64..(group + 1) * 64).map(node).collect::<Vec<_>>())
            .await?;
    }
    assert_eq!(store.inventory().await?.len(), 256);
    assert!(store.reconcile(&[node(256)]).await.is_err());
    let mut conflict = node(0);
    conflict.provider = "native".into();
    assert!(store.reconcile(&[conflict]).await.is_err());
    assert_eq!(store.inventory().await?.len(), 256);
    let _: () = redis::cmd("SET")
        .arg(&["monitor:v1:import", "pending"])
        .query_async(&mut connection(&f).await?)
        .await?;
    assert!(store.ready().await.is_err());
    assert!(store.reconcile(&[node(0)]).await.is_err());
    assert!(store.discovery_error(None).await.is_err());
    assert!(store.retain().await.is_err());
    assert!(store.insert(&[sample(0)]).await.is_err());
    assert!(store.latest(UID).await.is_err());
    assert!(store.history(UID, 1).await.is_err());
    assert!(store.inventory().await.is_err());
    Ok(())
}
#[tokio::test]
async fn weighted_rollups_null_gaps_ttl_and_corruption() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let s = sample(0);
    store.insert(std::slice::from_ref(&s)).await?;
    let raw = key(&f, "raw", s.timestamp_ms / 10_000 * 10_000).await?;
    let expiry: i64 = redis::cmd("PEXPIRETIME")
        .arg(&raw)
        .query_async(&mut connection(&f).await?)
        .await?;
    assert!(expiry <= s.timestamp_ms + 30 * 86400 * 1000);
    let history = store.history(UID, 6).await?;
    assert!(history.points.len() <= 1200);
    assert!(history
        .points
        .iter()
        .any(|p| p.metrics.get("cpu.used_percent") == Some(&Some(42.)))); // checked below without indexing gaps
    delete(
        &f,
        &key(&f, "rollup", s.timestamp_ms / 300_000 * 300_000).await?,
    )
    .await?;
    let now = now_ms();
    let mut conn = connection(&f).await?;
    let first = now / 300_000 * 300_000 - 600_000;
    for (time, total, count) in [(first, 100., 1), (first + 300_000, 0., 9)] {
        let k = key(&f, "rollup", time).await?;
        let v=serde_json::json!({"cpu.used_percent":{"total":total,"count":count},"gpu.nvidia.temperature_celsius":{"total":0.,"count":0}}).to_string();
        let _: () = redis::cmd("SET")
            .arg(k)
            .arg(v)
            .arg(&["PX", "60000"])
            .query_async(&mut conn)
            .await?;
    }
    let history = store.history(UID, 720).await?;
    assert!(history.points.len() <= 1200);
    assert!(history
        .points
        .iter()
        .any(|p| p.metrics.get("cpu.used_percent") == Some(&Some(10.))));
    assert!(history
        .points
        .iter()
        .any(|p| p.metrics.get("gpu.nvidia.temperature_celsius") == Some(&None)));
    delete(&f, &raw).await?;
    let _: i64 = redis::cmd("LPUSH")
        .arg(&raw)
        .arg("corrupt")
        .query_async(&mut conn)
        .await?;
    assert!(store.history(UID, 1).await.is_err());
    Ok(())
}

#[tokio::test]
async fn redis_restart_recovers_empty_without_replaying_registry() -> Result<()> {
    let mut f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    store.insert(&[sample(0)]).await?;
    let old = key(&f, "control", 0).await?;
    f.restart().await?;
    store.ready().await?;
    assert!(store.inventory().await?.is_empty());
    assert!(store.latest(UID).await?.is_none());
    assert!(value(&f, &old).await?.is_none());
    assert!(!store.assigned(&node(0), "kubernetes").await?);
    store.reconcile(&[node(0)]).await?;
    store.insert(&[sample(0)]).await?;
    assert!(store.latest(UID).await?.is_some());
    Ok(())
}

#[tokio::test]
async fn representative_thirty_day_rollup_query_is_bounded() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let mut conn = connection(&f).await?;
    let _: () = redis::cmd("CONFIG")
        .arg(&["SET", "maxmemory", "256mb"])
        .query_async(&mut conn)
        .await?;
    let value: serde_json::Value = (0..64)
        .map(|i| {
            (
                format!("net.interface_metric_{i}"),
                serde_json::json!({"total":123456789.25,"count":30}),
            )
        })
        .collect::<serde_json::Map<_, _>>()
        .into();
    let value = value.to_string();
    let now = now_ms() / 300_000 * 300_000;
    let prefix = key(&f, "rollup", 0).await?;
    let prefix = prefix.strip_suffix('0').unwrap();
    for page in (0..8640).collect::<Vec<_>>().chunks(128) {
        let mut pipe = redis::pipe();
        for i in page {
            pipe.cmd("SET")
                .arg(format!("{prefix}{}", now - i * 300_000))
                .arg(&value)
                .arg(&["PX", "60000"])
                .ignore();
        }
        let _: () = pipe.query_async(&mut conn).await?;
    }
    let start = std::time::Instant::now();
    let history = store.history(UID, 720).await?;
    let elapsed = start.elapsed();
    assert!(history.points.len() <= 1200);
    assert!(elapsed < std::time::Duration::from_secs(5));
    let info: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut conn)
        .await?;
    eprintln!(
        "30d 8640 rollups x64 metrics: {elapsed:?}; {}",
        info.lines()
            .find(|l| l.starts_with("used_memory_peak:"))
            .unwrap_or("memory missing")
    );
    Ok(())
}

#[tokio::test]
async fn evicted_latest_is_not_replaced_by_an_older_new_observation() -> Result<()> {
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let mut first = sample(0);
    first.timestamp_ms -= 10_000;
    store.insert(std::slice::from_ref(&first)).await?;
    delete(&f, &key(&f, "latest", 0).await?).await?;
    let mut older = first.clone();
    older.seq = 1;
    older.timestamp_ms -= 10_000;
    store.insert(&[older]).await?;
    assert!(store.latest(UID).await?.is_none());
    assert_eq!(
        store.inventory().await?[0].last_sample,
        Some(first.timestamp_ms)
    );
    let mut newer = first;
    newer.seq = 2;
    newer.timestamp_ms += 10_000;
    store.insert(std::slice::from_ref(&newer)).await?;
    assert_eq!(
        store.latest(UID).await?.unwrap().timestamp_ms,
        newer.timestamp_ms
    );
    Ok(())
}

#[tokio::test]
async fn configured_existing_generation_rejects_persistence_and_wrong_eviction() -> Result<()> {
    let f = common::Fixture::new().await?;
    let original = Store::open(&f.url).await?;
    original.reconcile(&[node(0)]).await?;
    original.insert(&[sample(0)]).await?;
    let control_key = key(&f, "control", 0).await?;
    let before = value(&f, &control_key).await?;
    let mut conn = connection(&f).await?;
    for (setting, incompatible, compatible) in [
        ("save", "60 1", ""),
        ("maxmemory-policy", "allkeys-lru", "volatile-lru"),
    ] {
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg(setting)
            .arg(incompatible)
            .query_async(&mut conn)
            .await?;
        let runtime = Store::configured(&f.url)?;
        assert!(runtime.ready().await.is_err());
        assert!(runtime.inventory().await.is_err());
        assert!(runtime.registry_error().await.is_err());
        assert!(runtime.reconcile(&[node(0)]).await.is_err());
        assert!(runtime.discovery_error(None).await.is_err());
        assert!(runtime.retain().await.is_err());
        assert!(runtime.insert(&[sample(1)]).await.is_err());
        assert!(runtime.latest(UID).await.is_err());
        assert!(runtime.history(UID, 1).await.is_err());
        assert_eq!(value(&f, &control_key).await?, before);
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg(setting)
            .arg(compatible)
            .query_async(&mut conn)
            .await?;
        runtime.ready().await?;
        assert!(runtime.latest(UID).await?.is_some());
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_unseen_identities_commit_exact_raw_rollup_and_dedup() -> Result<()> {
    use std::sync::Arc;
    let f = common::Fixture::new().await?;
    let store = Store::open(&f.url).await?;
    store.reconcile(&[node(0)]).await?;
    let mut base = sample(0);
    base.timestamp_ms -= 10_000;
    for identical in [true, false] {
        let barrier = Arc::new(tokio::sync::Barrier::new(4));
        let mut tasks = Vec::new();
        for i in 0..4 {
            let store = store.clone();
            let barrier = barrier.clone();
            let mut s = base.clone();
            if !identical {
                s.seq = i + 1;
            }
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                store.insert(&[s]).await
            }));
        }
        for task in tasks {
            task.await??;
        }
    }
    let rollup_key = key(&f, "rollup", base.timestamp_ms / 300_000 * 300_000).await?;
    let raw_key = key(&f, "raw", base.timestamp_ms / 10_000 * 10_000).await?;
    let rollup: serde_json::Value = serde_json::from_str(&value(&f, &rollup_key).await?.unwrap())?;
    assert_eq!(rollup["cpu.used_percent"]["total"], 210.0);
    assert_eq!(rollup["cpu.used_percent"]["count"], 5);
    let raw: Vec<serde_json::Value> = serde_json::from_str(&value(&f, &raw_key).await?.unwrap())?;
    assert_eq!(raw.len(), 5);
    assert_eq!(
        raw.iter()
            .map(|r| r["sample"]["seq"].as_i64().unwrap())
            .collect::<std::collections::BTreeSet<_>>(),
        (0..5).collect()
    );
    let control: serde_json::Value =
        serde_json::from_str(&value(&f, &key(&f, "control", 0).await?).await?.unwrap())?;
    let session = control["sessions"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    assert_eq!(session["high"], 4);
    assert_eq!(session["seen"], serde_json::json!([0, 1, 2, 3, 4]));
    let retry: Vec<_> = (0..5)
        .map(|seq| {
            let mut s = base.clone();
            s.seq = seq;
            s
        })
        .collect();
    store.insert(&retry).await?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&value(&f, &rollup_key).await?.unwrap())?,
        rollup
    );
    Ok(())
}
