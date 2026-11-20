use crate::domain::{now_ms, GpuStatus, Metrics, Sample};
use anyhow::{bail, ensure, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const ROOT: &str = "monitor:v1";
pub(crate) const ACTIVE: &str = "monitor:v1:active";
pub(crate) const IMPORT: &str = "monitor:v1:import";
pub(crate) const RETENTION: i64 = 30 * 86400 * 1000;
const MAX_CHUNK_BYTES: usize = 512 * 1024;
const MAX_QUERY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
pub struct Store {
    pub(crate) client: redis::Client,
}
#[derive(Clone, Serialize, Deserialize)]
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
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Session {
    pub high: i64,
    pub seen: BTreeSet<i64>,
    pub until: i64,
}
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct Control {
    pub keys: BTreeSet<String>,
    pub sessions: BTreeMap<String, Session>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Raw {
    pub sample: Sample,
    pub received: i64,
    pub provider: String,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Aggregate {
    pub total: f64,
    pub count: i64,
}
pub(crate) type Rollup = BTreeMap<String, Aggregate>;
pub(crate) type Writes = BTreeMap<String, (String, i64)>;

pub(crate) fn decode<T: DeserializeOwned + Default>(value: &Option<String>) -> Result<T> {
    value
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map(|v| v.unwrap_or_default())
        .context("corrupt Redis data")
}
pub(crate) fn encoded<T: Serialize>(value: &T) -> Result<String> {
    let value = serde_json::to_string(value)?;
    ensure!(
        value.len() <= MAX_CHUNK_BYTES,
        "storage chunk capacity exceeded"
    );
    Ok(value)
}
pub(crate) fn key(generation: &str, kind: &str, node: &str, time: i64) -> String {
    format!("{ROOT}:{generation}:{kind}:{node}:{time}")
}
impl Store {
    pub fn configured(url: &str) -> Result<Self> {
        let parsed = reqwest::Url::parse(url)?;
        ensure!(
            parsed.scheme() == "redis"
                && parsed.username().is_empty()
                && parsed.password().is_none()
                && parsed.path() == "/0"
                && parsed.query().is_none()
                && parsed.fragment().is_none()
                && parsed.host_str().is_some(),
            "Redis URL must be redis://host:port/0 without credentials or options"
        );
        Ok(Self {
            client: redis::Client::open(url)?,
        })
    }
    pub async fn connect(url: &str) -> Result<Self> {
        let store = Self::configured(url)?;
        store.validate_config().await?;
        Ok(store)
    }
    pub async fn open(url: &str) -> Result<Self> {
        let store = Self::connect(url).await?;
        store.initialize().await?;
        Ok(store)
    }
    pub(crate) async fn connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        Ok(tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.client.get_multiplexed_async_connection(),
        )
        .await??)
    }
    pub async fn validate_config(&self) -> Result<()> {
        let mut conn = self.connection().await?;
        let config: BTreeMap<String, String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg(&[
                "save",
                "appendonly",
                "maxmemory-policy",
                "maxmemory",
                "databases",
            ])
            .query_async(&mut conn)
            .await?;
        ensure!(
            config.get("save").is_some_and(String::is_empty)
                && config.get("appendonly").is_some_and(|v| v == "no")
                && config
                    .get("maxmemory-policy")
                    .is_some_and(|v| v == "volatile-lru")
                && config
                    .get("maxmemory")
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|v| v >= 16 * 1024 * 1024),
            "Redis requires save '', appendonly no, volatile-lru and maxmemory >=16MiB"
        );
        let info: String = redis::cmd("INFO")
            .arg("replication")
            .query_async(&mut conn)
            .await?;
        ensure!(
            info.lines().any(|l| l.trim() == "role:master"),
            "Redis must be a dedicated writable primary"
        );
        Ok(())
    }
    async fn initialize(&self) -> Result<()> {
        let values = self.read(&[ACTIVE.into(), IMPORT.into()]).await?;
        if values[0].is_none() && values[1].is_none() {
            self.validate_config().await?;
            let generation = uuid::Uuid::new_v4().to_string();
            let mut writes = Writes::new();
            writes.insert(ACTIVE.into(), (generation.clone(), 0));
            writes.insert(
                key(&generation, "registry", "", 0),
                (encoded(&crate::registry::Registry::default())?, 0),
            );
            self.commit(&[ACTIVE.into(), IMPORT.into()], &values, writes)
                .await?;
        }
        Ok(())
    }
    pub async fn ready(&self) -> Result<()> {
        self.registry().await.map(|_| ())
    }
    pub(crate) async fn generation(&self) -> Result<String> {
        self.validate_config().await?;
        // A Redis restart loses both metrics and control together. Never replay a
        // cached registry into a new instance; initialize an empty generation.
        self.initialize().await?;
        let values = self.read(&[ACTIVE.into(), IMPORT.into()]).await?;
        ensure!(values[1].is_none(), "history import pending");
        let generation = values[0].clone().context("storage not initialized")?;
        ensure!(
            uuid::Uuid::parse_str(&generation).is_ok(),
            "unsupported storage generation"
        );
        Ok(generation)
    }
    pub(crate) async fn read(&self, keys: &[String]) -> Result<Vec<Option<String>>> {
        ensure!(keys.len() <= 256, "read page too large");
        if keys.is_empty() {
            return Ok(vec![]);
        }
        Ok(redis::Script::new("#!lua flags=no-writes\nlocal values = {} for i=1,#KEYS do values[i] = redis.call('GET',KEYS[i]) end return values")
            .key(keys)
            .invoke_async(&mut self.connection().await?)
            .await?)
    }
    pub(crate) async fn commit(
        &self,
        keys: &[String],
        expected: &[Option<String>],
        writes: Writes,
    ) -> Result<bool> {
        ensure!(
            keys.len() == expected.len() && writes.len() <= 128,
            "commit bounds"
        );
        let script = redis::Script::new(include_str!("redis_commit.lua"));
        let mut invocation = script.prepare_invoke();
        for k in keys {
            invocation.key(k);
        }
        for k in writes.keys() {
            invocation.key(k);
        }
        invocation.arg(keys.len());
        for v in expected {
            invocation
                .arg(if v.is_some() { "1" } else { "0" })
                .arg(v.as_deref().unwrap_or(""));
        }
        invocation.arg(writes.len());
        for (value, expiry) in writes.values() {
            ensure!(*expiry >= -1, "invalid expiration");
            invocation.arg(value).arg(*expiry);
        }
        Ok(invocation
            .invoke_async::<i64>(&mut self.connection().await?)
            .await?
            == 1)
    }
    pub async fn insert(&self, samples: &[Sample]) -> Result<()> {
        ensure!(
            !samples.is_empty() && samples.len() <= 6 && samples.iter().all(|s| s.valid(now_ms())),
            "invalid batch"
        );
        let node = &samples[0].node;
        ensure!(samples.iter().all(|s| &s.node == node), "mixed Node batch");
        for _ in 0..8 {
            let generation = self.generation().await?;
            let registry_key = key(&generation, "registry", "", 0);
            let control_key = key(&generation, "control", node, 0);
            let latest_key = key(&generation, "latest", node, 0);
            let mut keys = vec![
                ACTIVE.into(),
                IMPORT.into(),
                registry_key.clone(),
                control_key.clone(),
                latest_key.clone(),
            ];
            for s in samples {
                keys.push(key(
                    &generation,
                    "raw",
                    node,
                    s.timestamp_ms / 10_000 * 10_000,
                ));
                keys.push(key(
                    &generation,
                    "rollup",
                    node,
                    s.timestamp_ms / 300_000 * 300_000,
                ));
            }
            keys.sort();
            keys.dedup();
            let values = self.read(&keys).await?;
            let originals: BTreeMap<_, _> =
                keys.iter().cloned().zip(values.iter().cloned()).collect();
            ensure!(
                originals[ACTIVE].as_deref() == Some(&generation) && originals[IMPORT].is_none(),
                "storage generation changed"
            );
            let mut registry: crate::registry::Registry = decode(&originals[&registry_key])?;
            let registered = registry
                .nodes
                .get_mut(node)
                .context("Node discovery pending")?;
            ensure!(registered.node.lifecycle == "present", "inactive Node");
            ensure!(
                originals[&control_key].is_some() || registered.last_sample.is_none(),
                "protected Node control missing"
            );
            let mut control: Control = decode(&originals[&control_key])?;
            control
                .sessions
                .retain(|_, session| session.until >= now_ms());
            let mut writes = Writes::new();
            let mut latest_changed = false;
            let mut latest: Option<Raw> = originals[&latest_key]
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?;
            for s in samples {
                control.keys.extend(s.metrics.keys().cloned());
                ensure!(
                    control.keys.len() <= 128,
                    "persistent metric cardinality exceeded"
                );
                let session_id = format!("{}:{}", s.boot_id, s.process_id);
                let session = control.sessions.entry(session_id).or_default();
                ensure!(
                    s.seq >= session.high.saturating_sub(12),
                    "sequence outside retry window"
                );
                if session.seen.contains(&s.seq) {
                    continue;
                }
                session.high = session.high.max(s.seq);
                session.seen.insert(s.seq);
                session
                    .seen
                    .retain(|seq| *seq >= session.high.saturating_sub(12));
                session.until = session.until.max(s.timestamp_ms + RETENTION);
                ensure!(control.sessions.len() <= 1024, "session capacity exceeded");
                let raw_key = key(&generation, "raw", node, s.timestamp_ms / 10_000 * 10_000);
                let raw_value = writes
                    .get(&raw_key)
                    .map(|(v, _)| Some(v.clone()))
                    .unwrap_or_else(|| originals[&raw_key].clone());
                let mut raw: Vec<Raw> = decode(&raw_value)?;
                ensure!(raw.len() < 128, "raw chunk capacity exceeded");
                let record = Raw {
                    sample: s.clone(),
                    received: now_ms(),
                    provider: registered.node.provider.clone(),
                };
                raw.push(record.clone());
                // Expire at chunk START: no sample can exceed the 30-day upper bound.
                writes.insert(
                    raw_key,
                    (encoded(&raw)?, s.timestamp_ms / 10_000 * 10_000 + RETENTION),
                );
                let rollup_key = key(
                    &generation,
                    "rollup",
                    node,
                    s.timestamp_ms / 300_000 * 300_000,
                );
                let rollup_value = writes
                    .get(&rollup_key)
                    .map(|(v, _)| Some(v.clone()))
                    .unwrap_or_else(|| originals[&rollup_key].clone());
                let mut rollup: Rollup = decode(&rollup_value)?;
                for (name, value) in &s.metrics {
                    let aggregate = rollup.entry(name.clone()).or_default();
                    aggregate.total += value.unwrap_or(0.);
                    aggregate.count = aggregate
                        .count
                        .checked_add(i64::from(value.is_some()))
                        .context("aggregate count overflow")?;
                    ensure!(aggregate.total.is_finite(), "aggregate overflow");
                }
                writes.insert(
                    rollup_key,
                    (
                        encoded(&rollup)?,
                        s.timestamp_ms / 300_000 * 300_000 + RETENTION,
                    ),
                );
                if s.timestamp_ms >= registered.last_sample.unwrap_or(i64::MIN)
                    && latest
                        .as_ref()
                        .is_none_or(|r| r.sample.timestamp_ms <= s.timestamp_ms)
                {
                    latest = Some(record);
                    latest_changed = true;
                }
                registered.last_sample =
                    Some(registered.last_sample.unwrap_or(0).max(s.timestamp_ms));
            }
            if let Some(latest) = latest.filter(|_| latest_changed) {
                writes.insert(
                    latest_key,
                    (encoded(&latest)?, latest.sample.timestamp_ms + RETENTION),
                );
            }
            writes.insert(control_key, (encoded(&control)?, 0));
            writes.insert(registry_key, (encoded(&registry)?, 0));
            if self.commit(&keys, &values, writes).await? {
                return Ok(());
            }
        }
        bail!("storage contention")
    }
    pub async fn retain(&self) -> Result<()> {
        self.maintain_registry().await.map(|_| ())
    }
    pub async fn history(&self, node: &str, hours: u32) -> Result<History> {
        ensure!((1..=720).contains(&hours), "invalid history range");
        let generation = self.generation().await?;
        ensure!(self.has_uid(node).await?, "unknown Node");
        let end = now_ms();
        let start = end - i64::from(hours) * 3_600_000;
        let bucket = if hours > 6 {
            ((i64::from(hours) * 3_600_000 + 1198) / 1199 + 299_999) / 300_000 * 300_000
        } else {
            ((i64::from(hours) * 3_600_000 + 1198) / 1199).max(10_000)
        };
        let step = if hours > 6 { 300_000 } else { 10_000 };
        let kind = if hours > 6 { "rollup" } else { "raw" };
        let keys: Vec<_> = (start / step..=end / step)
            .map(|i| key(&generation, kind, node, i * step))
            .collect();
        let mut points = BTreeMap::<i64, Rollup>::new();
        let mut bytes = 0;
        for page in keys.chunks(128) {
            let values = self.read(page).await?;
            for (chunk_key, value) in page.iter().zip(values) {
                let Some(value) = value else {
                    continue;
                };
                bytes += value.len();
                ensure!(
                    bytes <= MAX_QUERY_BYTES && value.len() <= MAX_CHUNK_BYTES,
                    "query byte budget exceeded"
                );
                if hours > 6 {
                    let time: i64 = chunk_key
                        .rsplit(':')
                        .next()
                        .context("chunk time")?
                        .parse()?;
                    if time < start || time > end {
                        continue;
                    }
                    let rollup: Rollup = serde_json::from_str(&value)?;
                    merge(points.entry((time - start) / bucket).or_default(), &rollup)?;
                } else {
                    let raw: Vec<Raw> = serde_json::from_str(&value)?;
                    ensure!(raw.len() <= 128, "raw chunk capacity exceeded");
                    for r in raw {
                        if r.sample.timestamp_ms < start || r.sample.timestamp_ms > end {
                            continue;
                        }
                        let rollup = r
                            .sample
                            .metrics
                            .into_iter()
                            .map(|(k, v)| {
                                (
                                    k,
                                    Aggregate {
                                        total: v.unwrap_or(0.),
                                        count: i64::from(v.is_some()),
                                    },
                                )
                            })
                            .collect();
                        merge(
                            points
                                .entry((r.sample.timestamp_ms - start) / bucket)
                                .or_default(),
                            &rollup,
                        )?;
                    }
                }
            }
        }
        ensure!(
            self.generation().await? == generation,
            "storage generation changed"
        );
        Ok(History {
            node: node.into(),
            bucket_ms: bucket,
            points: (0..=(end - start) / bucket)
                .map(|i| Point {
                    timestamp_ms: start + i * bucket,
                    metrics: points
                        .remove(&i)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(k, a)| {
                            (
                                k,
                                if a.count == 0 {
                                    None
                                } else {
                                    Some(a.total / a.count as f64)
                                },
                            )
                        })
                        .collect(),
                    gpu: None,
                })
                .collect(),
        })
    }
    pub async fn latest(&self, node: &str) -> Result<Option<Point>> {
        let generation = self.generation().await?;
        let value = self
            .read(&[key(&generation, "latest", node, 0)])
            .await?
            .remove(0);
        let raw: Option<Raw> = value.as_deref().map(serde_json::from_str).transpose()?;
        Ok(raw.map(|r| Point {
            timestamp_ms: r.sample.timestamp_ms,
            metrics: r.sample.metrics,
            gpu: Some(r.sample.gpu),
        }))
    }
}
fn merge(target: &mut Rollup, source: &Rollup) -> Result<()> {
    ensure!(source.len() <= 128, "aggregate cardinality exceeded");
    for (key, value) in source {
        ensure!(
            value.total.is_finite() && value.total >= 0. && value.count >= 0,
            "corrupt aggregate"
        );
        let entry = target.entry(key.clone()).or_default();
        entry.total += value.total;
        entry.count = entry
            .count
            .checked_add(value.count)
            .context("aggregate count overflow")?;
        ensure!(entry.total.is_finite(), "aggregate overflow");
    }
    ensure!(target.len() <= 128, "aggregate cardinality exceeded");
    Ok(())
}
