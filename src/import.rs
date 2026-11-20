//! Explicit, read-only SQLite snapshot migration. Never called by `serve`.
use crate::{
    domain::{now_ms, GpuStatus, Metrics, Sample},
    registry::{Entry, Registry},
    store::{
        encoded, key, Aggregate, Control, Raw, Rollup, Store, Writes, ACTIVE, IMPORT, RETENTION,
    },
};
use anyhow::{ensure, Context, Result};
use clap::Args;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

const WORKER: &str = "monitor:v1:import-worker";
const GRACE: i64 = 6 * 3_600_000;
const MAX_SNAPSHOT: u64 = 8 * 1024 * 1024 * 1024;
#[derive(Args)]
pub struct ImportArgs {
    #[arg(long)]
    pub snapshot: PathBuf,
    #[arg(long)]
    pub sha256: String,
    #[arg(long)]
    pub attempt: uuid::Uuid,
    /// Replay deterministic writes from the beginning of the same attempt.
    #[arg(long)]
    pub resume: bool,
    /// Assert that the previous importer process has terminated before takeover.
    #[arg(long, requires = "resume")]
    pub takeover: bool,
    #[arg(long, requires = "resume", conflicts_with = "activate")]
    pub verify_only: bool,
    /// Reverify a completed import and atomically activate it.
    #[arg(long, requires = "resume")]
    pub activate: bool,
}
#[derive(Args)]
pub struct ReceiveArgs {
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub sha256: String,
    #[arg(long, default_value_t = MAX_SNAPSHOT)]
    pub max_bytes: u64,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableProof {
    pub rows: i64,
    pub retained_rows: i64,
    pub min_time: Option<i64>,
    pub max_time: Option<i64>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub schema_version: i64,
    pub schema_sha256: String,
    pub source_sha256: String,
    pub retained_at_ms: i64,
    pub tables: BTreeMap<String, TableProof>,
    pub keys: u64,
    pub canonical_sha256: String,
}
#[derive(Clone, Serialize, Deserialize)]
struct Attempt {
    schema: u32,
    attempt: uuid::Uuid,
    generation: String,
    source_sha256: String,
    run_id: String,
    started_ms: i64,
    grace_until_ms: i64,
    phase: String,
    manifest: Option<Manifest>,
    evicted_keys: u64,
}
#[derive(Serialize)]
struct ImportProof<'a> {
    phase: &'a str,
    import_status: &'a str,
    evicted_keys: u64,
    attempt: uuid::Uuid,
    generation: &'a str,
    redis_run_id: &'a str,
    verified_at_ms: i64,
    verification_sha256: String,
    manifest: &'a Manifest,
}
fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
pub fn snapshot_digest(path: &Path) -> Result<String> {
    let metadata = std::fs::symlink_metadata(path)?;
    ensure!(
        metadata.file_type().is_file() && metadata.len() <= MAX_SNAPSHOT,
        "snapshot must be a regular file <=8GiB"
    );
    let mut file = std::fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub fn receive(args: ReceiveArgs) -> Result<()> {
    ensure!(
        valid_digest(&args.sha256) && args.max_bytes > 0 && args.max_bytes <= MAX_SNAPSHOT,
        "invalid snapshot bounds/digest"
    );
    ensure!(!args.output.exists(), "destination already exists");
    let partial = args
        .output
        .with_extension(format!("{}.partial", uuid::Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&partial)?;
        let mut input = std::io::stdin().lock();
        let mut count = 0u64;
        let mut hash = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            count += n as u64;
            ensure!(count <= args.max_bytes, "snapshot byte limit exceeded");
            hash.update(&buffer[..n]);
            file.write_all(&buffer[..n])?;
        }
        ensure!(
            format!("{:x}", hash.finalize()) == args.sha256,
            "snapshot SHA-256 mismatch"
        );
        file.sync_all()?;
        // Hard-link finalization refuses an existing destination atomically.
        std::fs::hard_link(&partial, &args.output)?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&partial);
    result
}
async fn snapshot(path: &Path) -> Result<SqlitePool> {
    let path = path.canonicalize()?;
    // A standalone backup only: reject adjacent WAL/journal instead of ignoring it.
    ensure!(
        !PathBuf::from(format!("{}-wal", path.display())).exists()
            && !PathBuf::from(format!("{}-journal", path.display())).exists(),
        "snapshot must be standalone (no WAL/journal)"
    );
    let options = SqliteConnectOptions::from_str(path.to_str().context("snapshot path")?)?
        .read_only(true)
        .immutable(true)
        .pragma("query_only", "ON");
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await?;
    ensure!(
        version == 2,
        "import requires schema v2 with UID/provider ownership (no implicit v1 migration)"
    );
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await?;
    ensure!(integrity == "ok", "snapshot integrity check failed");
    let oversized: i64 = sqlx::query_scalar("SELECT count(*) FROM samples WHERE length(metrics)>32768 OR length(gpu)>1024 OR length(node)>36 OR length(boot)>36 OR length(process)>36").fetch_one(&pool).await?;
    ensure!(oversized == 0, "snapshot contains oversized sample fields");
    for sql in [
        "SELECT count(*) FROM nodes WHERE length(uid)>36 OR length(name)>253 OR length(capabilities)>131072",
        "SELECT count(*) FROM metric_keys WHERE length(node)>36 OR length(key)>96",
        "SELECT count(*) FROM rollups WHERE length(node)>36 OR length(key)>96",
        "SELECT count(*) FROM registry_status WHERE length(error)>4096",
    ] {
        let oversized: i64 = sqlx::query_scalar(sql).fetch_one(&pool).await?;
        ensure!(oversized == 0, "snapshot contains oversized metadata fields");
    }
    Ok(pool)
}
async fn redis_identity(store: &Store) -> Result<(String, u64)> {
    let info: String = redis::cmd("INFO")
        .query_async(&mut store.connection().await?)
        .await?;
    let fields: BTreeMap<_, _> = info
        .lines()
        .filter_map(|l| l.trim().split_once(':'))
        .collect();
    Ok((
        fields
            .get("run_id")
            .context("Redis run_id missing")?
            .to_string(),
        fields
            .get("evicted_keys")
            .context("Redis eviction stats missing")?
            .parse()?,
    ))
}
async fn capacity_headroom(store: &Store) -> Result<()> {
    let mut conn = store.connection().await?;
    let info: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut conn)
        .await?;
    let fields: BTreeMap<_, _> = info
        .lines()
        .filter_map(|l| l.trim().split_once(':'))
        .collect();
    let used: u64 = fields
        .get("used_memory")
        .context("Redis used_memory missing")?
        .parse()?;
    let max: u64 = fields
        .get("maxmemory")
        .context("Redis maxmemory missing")?
        .parse()?;
    ensure!(
        max.saturating_sub(used) >= 8 * 1024 * 1024,
        "import requires at least 8MiB free Redis commit headroom; capacity insufficient"
    );
    Ok(())
}
struct Transfer<'a> {
    store: &'a Store,
    attempt: &'a Attempt,
    lock: String,
    worker: String,
    verify: bool,
    restore: bool,
    hash: Sha256,
    count: u64,
}
impl Transfer<'_> {
    async fn item<T: Serialize>(
        &mut self,
        kind: &str,
        node: &str,
        time: i64,
        data: &T,
        expiry: i64,
    ) -> Result<()> {
        ensure!(
            now_ms() + 60_000 < self.attempt.grace_until_ms,
            "fixed import grace deadline exhausted; do not extend a failed attempt"
        );
        let key = key(&self.attempt.generation, kind, node, time);
        let value = encoded(data)?;
        // Preserve every fenced-source row through the fixed cutover grace,
        // including rows already older than the normal runtime horizon.
        let expiry = if expiry == 0 {
            0
        } else {
            expiry.max(self.attempt.grace_until_ms)
        };
        let canonical_key = format!("{kind}:{node}:{time}");
        for bytes in [
            canonical_key.as_bytes(),
            value.as_bytes(),
            &expiry.to_be_bytes(),
        ] {
            self.hash.update((bytes.len() as u64).to_be_bytes());
            self.hash.update(bytes);
        }
        self.count += 1;
        let expected = vec![Some(self.lock.clone()), Some(self.worker.clone())];
        if self.verify {
            let values = self
                .store
                .read(&[IMPORT.into(), WORKER.into(), key.clone()])
                .await?;
            ensure!(
                values[0..2] == expected && values[2].as_deref() == Some(&value),
                "import verification mismatch at {canonical_key}"
            );
            let ttl: i64 = redis::cmd("PEXPIRETIME")
                .arg(&key)
                .query_async(&mut self.store.connection().await?)
                .await?;
            ensure!(
                ttl == if expiry == 0 {
                    -1
                } else {
                    self.attempt.grace_until_ms
                },
                "import expiry mismatch at {canonical_key}"
            );
        } else if self.restore {
            // Keep the non-TTL staging lock throughout TTL restoration. A crash
            // requires replay + complete verification, never automatic serving.
            let script = redis::Script::new("if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[2]) ~= ARGV[2] then return 0 end if ARGV[3] ~= '0' then redis.call('PEXPIREAT',KEYS[3],ARGV[3]) end return 1");
            let result: i64 = script
                .key(IMPORT)
                .key(WORKER)
                .key(&key)
                .arg(&self.lock)
                .arg(&self.worker)
                .arg(expiry)
                .invoke_async(&mut self.store.connection().await?)
                .await?;
            ensure!(result == 1, "import ownership lost");
        } else {
            let writes = Writes::from([(
                key,
                (
                    value,
                    if expiry == 0 {
                        0
                    } else {
                        self.attempt.grace_until_ms
                    },
                ),
            )]);
            ensure!(
                self.store
                    .commit(&[IMPORT.into(), WORKER.into()], &expected, writes)
                    .await?,
                "import ownership lost"
            );
        }
        Ok(())
    }
}
async fn enumerate(pool: &SqlitePool, transfer: &mut Transfer<'_>) -> Result<Manifest> {
    let mut registry = Registry {
        history_grace_until_ms: transfer.attempt.grace_until_ms,
        ..Registry::default()
    };
    let rows = sqlx::query("SELECT * FROM nodes ORDER BY uid LIMIT 257")
        .fetch_all(pool)
        .await?;
    ensure!(rows.len() <= 256, "retained Node capacity exceeded");
    for row in rows {
        let uid: String = row.try_get("uid")?;
        ensure!(
            uuid::Uuid::parse_str(&uid).is_ok(),
            "snapshot has unassigned name-keyed history"
        );
        let entry = Entry {
            node: crate::kubernetes::Node {
                uid: uid.clone(),
                name: row.try_get("name")?,
                lifecycle: row.try_get("lifecycle")?,
                provider: row.try_get("provider")?,
                capabilities: serde_json::from_str(row.try_get("capabilities")?)?,
            },
            first_seen: row.try_get("first_seen")?,
            last_seen: row.try_get("last_seen")?,
            last_sample: row.try_get("last_sample")?,
        };
        ensure!(
            matches!(entry.node.provider.as_str(), "native" | "kubernetes")
                && matches!(
                    entry.node.lifecycle.as_str(),
                    "present" | "deleted" | "unsupported" | "not-ready"
                )
                && entry.node.name.len() <= 253,
            "invalid registry metadata"
        );
        registry.nodes.insert(uid, entry);
    }
    ensure!(
        registry
            .nodes
            .values()
            .filter(|n| n.node.lifecycle != "deleted")
            .count()
            <= 64,
        "active Node capacity exceeded"
    );
    let statuses = sqlx::query("SELECT * FROM registry_status")
        .fetch_all(pool)
        .await?;
    ensure!(
        statuses.len() == 1 && statuses[0].try_get::<i64, _>("id")? == 1,
        "invalid registry status"
    );
    registry.error = statuses[0].try_get("error")?;
    registry.cursor = statuses[0].try_get("cursor")?;
    let mut controls: BTreeMap<String, Control> = registry
        .nodes
        .keys()
        .map(|uid| (uid.clone(), Control::default()))
        .collect();
    let mut keys = sqlx::query("SELECT node,key FROM metric_keys ORDER BY node,key").fetch(pool);
    while let Some(row) = keys.try_next().await? {
        let uid: String = row.try_get("node")?;
        let key: String = row.try_get("key")?;
        ensure!(
            crate::domain::valid_key(&key),
            "invalid persisted metric key"
        );
        let control = controls
            .get_mut(&uid)
            .context("metric keys without registered UID")?;
        control.keys.insert(key);
        ensure!(
            control.keys.len() <= 128,
            "persistent metric cardinality exceeded"
        );
    }
    drop(keys);
    let mut latest: BTreeMap<String, Raw> = BTreeMap::new();
    let mut raw_chunk: Vec<Raw> = Vec::new();
    let mut chunk_id: Option<(String, i64)> = None;
    let mut raw_rows =
        sqlx::query("SELECT * FROM samples ORDER BY node,time,boot,process,seq").fetch(pool);
    while let Some(row) = raw_rows.try_next().await? {
        let uid: String = row.try_get("node")?;
        let time: i64 = row.try_get("time")?;
        ensure!(
            time >= 0 && time <= transfer.attempt.started_ms + 30_000,
            "snapshot contains future sample"
        );
        let sample = Sample {
            node: uid.clone(),
            boot_id: row.try_get("boot")?,
            process_id: row.try_get("process")?,
            seq: row.try_get("seq")?,
            timestamp_ms: time,
            metrics: serde_json::from_str::<Metrics>(row.try_get("metrics")?)?,
            gpu: serde_json::from_str::<GpuStatus>(row.try_get("gpu")?)?,
        };
        // Import preserves historic timestamps, but retains all ordinary bounds.
        ensure!(sample.valid(time), "invalid stored sample");
        let provider: String = row.try_get("provider")?;
        ensure!(
            registry
                .nodes
                .get(&uid)
                .is_some_and(|n| n.node.provider == provider),
            "sample provider ownership mismatch"
        );
        let control = controls
            .get_mut(&uid)
            .context("sample without registered UID")?;
        ensure!(
            sample.metrics.keys().all(|k| control.keys.contains(k)),
            "sample metric missing from persistent key union"
        );
        let session = control
            .sessions
            .entry(format!("{}:{}", sample.boot_id, sample.process_id))
            .or_default();
        session.high = session.high.max(sample.seq);
        session.seen.insert(sample.seq);
        session
            .seen
            .retain(|s| *s >= session.high.saturating_sub(12));
        session.until = session
            .until
            .max(time + RETENTION)
            .max(transfer.attempt.grace_until_ms);
        ensure!(
            control.sessions.len() <= 1024,
            "import session capacity exceeded"
        );
        let id = (uid.clone(), time / 10_000 * 10_000);
        if chunk_id.as_ref().is_some_and(|prior| prior != &id) {
            let (prior_uid, prior_time) = chunk_id.take().context("raw chunk identity")?;
            transfer
                .item(
                    "raw",
                    &prior_uid,
                    prior_time,
                    &raw_chunk,
                    prior_time + RETENTION,
                )
                .await?;
            raw_chunk.clear();
        }
        chunk_id = Some(id);
        ensure!(raw_chunk.len() < 128, "import raw chunk capacity exceeded");
        let record = Raw {
            sample,
            received: row.try_get("received")?,
            provider,
        };
        latest.insert(uid, record.clone());
        raw_chunk.push(record);
    }
    drop(raw_rows);
    if let Some((uid, time)) = chunk_id {
        transfer
            .item("raw", &uid, time, &raw_chunk, time + RETENTION)
            .await?;
    }
    // Import original sum/count rows, NEVER derive aggregates from raw history.
    let mut rollup_rows = sqlx::query("SELECT * FROM rollups ORDER BY node,time,key").fetch(pool);
    let mut chunk: Rollup = BTreeMap::new();
    let mut chunk_id: Option<(String, i64)> = None;
    while let Some(row) = rollup_rows.try_next().await? {
        let uid: String = row.try_get("node")?;
        let time: i64 = row.try_get("time")?;
        ensure!(
            time >= 0 && time % 300_000 == 0 && time <= transfer.attempt.started_ms + 30_000,
            "invalid rollup time"
        );
        let name: String = row.try_get("key")?;
        ensure!(
            controls.get(&uid).is_some_and(|c| c.keys.contains(&name)),
            "rollup key without registered UID/key union"
        );
        let aggregate = Aggregate {
            total: row.try_get("total")?,
            count: row.try_get("count")?,
        };
        ensure!(
            aggregate.total.is_finite() && aggregate.total >= 0. && aggregate.count >= 0,
            "invalid original aggregate"
        );
        let id = (uid.clone(), time);
        if chunk_id.as_ref().is_some_and(|prior| prior != &id) {
            let (uid, time) = chunk_id.take().context("rollup identity")?;
            transfer
                .item("rollup", &uid, time, &chunk, time + RETENTION)
                .await?;
            chunk.clear();
        }
        chunk_id = Some(id);
        chunk.insert(name, aggregate);
        ensure!(chunk.len() <= 128, "rollup capacity exceeded");
    }
    drop(rollup_rows);
    if let Some((uid, time)) = chunk_id {
        transfer
            .item("rollup", &uid, time, &chunk, time + RETENTION)
            .await?;
    }
    for (uid, control) in controls {
        transfer.item("control", &uid, 0, &control, 0).await?;
    }
    for (uid, raw) in latest {
        transfer
            .item("latest", &uid, 0, &raw, raw.sample.timestamp_ms + RETENTION)
            .await?;
    }
    transfer.item("registry", "", 0, &registry, 0).await?;
    let mut tables = BTreeMap::new();
    // Table names are hardcoded, never user input.
    for table in [
        "samples",
        "rollups",
        "metric_keys",
        "nodes",
        "registry_status",
    ] {
        let timed = matches!(table, "samples" | "rollups");
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await?;
        let proof = if timed {
            let row = sqlx::query(&format!(
                "SELECT count(*) AS n,min(time) AS lo,max(time) AS hi FROM {table}"
            ))
            .fetch_one(pool)
            .await?;
            TableProof {
                rows,
                retained_rows: row.try_get("n")?,
                min_time: row.try_get("lo")?,
                max_time: row.try_get("hi")?,
            }
        } else {
            TableProof {
                rows,
                retained_rows: rows,
                min_time: None,
                max_time: None,
            }
        };
        tables.insert(table.into(), proof);
    }
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name",
    )
    .fetch_all(pool)
    .await?;
    Ok(Manifest {
        schema_version: 2,
        schema_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&schemas)?)),
        source_sha256: transfer.attempt.source_sha256.clone(),
        retained_at_ms: transfer.attempt.started_ms,
        tables,
        keys: transfer.count,
        canonical_sha256: format!("{:x}", transfer.hash.clone().finalize()),
    })
}
pub async fn run(args: ImportArgs, url: &str) -> Result<()> {
    ensure!(
        valid_digest(&args.sha256),
        "expected lowercase SHA-256 required"
    );
    ensure!(
        snapshot_digest(&args.snapshot)? == args.sha256,
        "source SHA-256 mismatch"
    );
    let pool = snapshot(&args.snapshot).await?;
    let store = Store::connect(url).await?;
    let (run_id, evicted_keys) = redis_identity(&store).await?;
    let original = store
        .read(&[IMPORT.into(), ACTIVE.into(), WORKER.into()])
        .await?;
    let mut attempt = if args.resume {
        let lock = original[0]
            .as_deref()
            .context("no staged import to resume")?;
        let attempt: Attempt = serde_json::from_str(lock)?;
        ensure!(
            attempt.schema == 1
                && attempt.attempt == args.attempt
                && attempt.source_sha256 == args.sha256
                && attempt.run_id == run_id,
            "resume attempt/source/Redis identity mismatch"
        );
        ensure!(
            args.takeover || original[2].is_none(),
            "previous importer owns attempt; --takeover requires confirming its termination"
        );
        attempt
    } else {
        ensure!(
            original.iter().all(Option::is_none),
            "import requires a pristine destination, or matching --resume"
        );
        let size: i64 = redis::cmd("DBSIZE")
            .query_async(&mut store.connection().await?)
            .await?;
        ensure!(
            size == 0,
            "import requires an empty dedicated Redis database"
        );
        let now = now_ms();
        Attempt {
            schema: 1,
            attempt: args.attempt,
            generation: uuid::Uuid::new_v4().to_string(),
            source_sha256: args.sha256.clone(),
            run_id,
            started_ms: now,
            grace_until_ms: now + GRACE,
            phase: "staging".into(),
            manifest: None,
            evicted_keys,
        }
    };
    ensure!(
        attempt.evicted_keys == evicted_keys,
        "destination evicted keys during this import; capacity insufficient"
    );
    ensure!(
        now_ms() + 60_000 < attempt.grace_until_ms,
        "import grace deadline expired"
    );
    let worker = uuid::Uuid::new_v4().to_string();
    let lock = encoded(&attempt)?;
    let writes = Writes::from([
        (IMPORT.into(), (lock.clone(), 0)),
        (WORKER.into(), (worker.clone(), 0)),
    ]);
    ensure!(
        store
            .commit(
                &[IMPORT.into(), ACTIVE.into(), WORKER.into()],
                &original,
                writes
            )
            .await?,
        "import acquisition raced another writer"
    );
    let work = async {
        let mut transfer = Transfer {
            store: &store,
            attempt: &attempt,
            lock: lock.clone(),
            worker: worker.clone(),
            verify: false,
            restore: false,
            hash: Sha256::new(),
            count: 0,
        };
        let manifest = if args.verify_only || args.activate {
            attempt
                .manifest
                .clone()
                .context("import not yet completed; replay with --resume first")?
        } else {
            enumerate(&pool, &mut transfer).await?
        };
        let mut verifier = Transfer {
            verify: true,
            hash: Sha256::new(),
            count: 0,
            ..transfer
        };
        let verified = enumerate(&pool, &mut verifier).await?;
        ensure!(manifest == verified, "source/manifest mismatch");
        ensure!(
            snapshot_digest(&args.snapshot)? == args.sha256,
            "source changed during import"
        );
        let (current_run, current_evictions) = redis_identity(&store).await?;
        ensure!(
            current_run == attempt.run_id && current_evictions == attempt.evicted_keys,
            "Redis restarted or evicted data during import verification"
        );
        let size: u64 = redis::cmd("DBSIZE")
            .query_async(&mut store.connection().await?)
            .await?;
        ensure!(
            size == manifest.keys + 2,
            "unexpected or missing destination keys"
        );
        Ok::<_, anyhow::Error>(manifest)
    }
    .await;
    // On failure leave the protected lock/worker in place; only an explicit,
    // same-attempt takeover can recover it. Never silently serve partial data.
    let manifest = work?;
    capacity_headroom(&store).await?;
    attempt.manifest = Some(manifest.clone());
    attempt.phase = "verified".into();
    let verified_lock = encoded(&attempt)?;
    ensure!(
        store
            .commit(
                &[IMPORT.into(), WORKER.into()],
                &[Some(lock), Some(worker.clone())],
                Writes::from([(IMPORT.into(), (verified_lock.clone(), 0))])
            )
            .await?,
        "import ownership lost"
    );
    let proof = ImportProof {
        phase: if args.activate { "active" } else { "verified" },
        import_status: "verified",
        evicted_keys: attempt.evicted_keys,
        attempt: attempt.attempt,
        generation: &attempt.generation,
        redis_run_id: &attempt.run_id,
        verified_at_ms: now_ms(),
        verification_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&manifest)?)),
        manifest: &manifest,
    };
    if args.activate {
        let mut restore = Transfer {
            store: &store,
            attempt: &attempt,
            lock: verified_lock.clone(),
            worker: worker.clone(),
            verify: false,
            restore: true,
            hash: Sha256::new(),
            count: 0,
        };
        let restored = enumerate(&pool, &mut restore).await?;
        ensure!(
            restored == manifest,
            "source changed during expiry restoration"
        );
        let (current_run, current_evictions) = redis_identity(&store).await?;
        ensure!(
            current_run == attempt.run_id && current_evictions == attempt.evicted_keys,
            "Redis restarted/evicted during activation"
        );
        capacity_headroom(&store).await?;
        let script = redis::Script::new("if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[2]) ~= ARGV[2] or redis.call('EXISTS',KEYS[3]) ~= 0 then return 0 end redis.call('MSET',KEYS[3],ARGV[3],KEYS[4],ARGV[4]) redis.call('DEL',KEYS[1],KEYS[2]) return 1");
        let activated: i64 = script
            .key(IMPORT)
            .key(WORKER)
            .key(ACTIVE)
            .key("monitor:v1:import-proof")
            .arg(&verified_lock)
            .arg(&worker)
            .arg(&attempt.generation)
            .arg(serde_json::to_string(&proof)?)
            .invoke_async(&mut store.connection().await?)
            .await?;
        ensure!(activated == 1, "activation ownership lost");
    } else {
        let script = redis::Script::new("if redis.call('GET',KEYS[1]) ~= ARGV[1] or redis.call('GET',KEYS[2]) ~= ARGV[2] then return 0 end redis.call('DEL',KEYS[2]) return 1");
        let released: i64 = script
            .key(IMPORT)
            .key(WORKER)
            .arg(&verified_lock)
            .arg(&worker)
            .invoke_async(&mut store.connection().await?)
            .await?;
        ensure!(released == 1, "import ownership lost");
    }
    println!("{}", serde_json::to_string(&proof)?);
    pool.close().await;
    Ok(())
}
