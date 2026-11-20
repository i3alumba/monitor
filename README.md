# Resource monitor

One native GNU/Linux Rust binary: `resource-monitor serve`, `agent`, and explicit
operator-only `receive-snapshot` / `import-sqlite` migration commands. No remote execution, agent listener, GPU control,
container runtime integration, or automatic host configuration. This directory
contains application sources and installation templates; deploying them requires
operator authorization and the parent project's release gates.

## Automatic-node amendment (current deployment contract)

Approved confined Kubernetes collectors extend the two native agents without
reprovisioning existing PV/Secret/CA/native tokens/probes/backups. The parent
project owns alpha/amd64 placement and an **independent private ephemeral Redis**
Deployment (not a sidecar). Monitor upgrades must not recreate Redis. Both old
SQLite volumes remain frozen protected checkpoints; neither is mounted by the
new server. Source preparation is not deployment authorization.

Registry/history use **Node UID**, not node name. Existing native alpha/gamma
bearers retain their exact values and authorize only the operator's approved UID
plus the current API Node UID. Same-name replacements get separate Kubernetes
provider histories; copied native labels do not confer ownership. Provider
conflicts are rejected, never silently merged/fallback.

Supported automatic targets: Ready Linux **amd64, arm64, ARMv7 minimum**. The parent
provides separate matching OCI images/DaemonSets, not an amd64 image on ARM:
`monitor-agent-amd64`, `monitor-agent-arm64`, `monitor-agent-armv7`. Expected suffix
is derived only from live API `Node.status.nodeInfo.architecture` (arm -> armv7),
not body/labels. Windows/other architectures and known armv6 kernel identity are
unsupported. Standard architecture=arm does not attest ISA variant; capability
text states ARMv7 minimum and coverage stays pending until a compatible collector
runs. ARM cross-compilation/execution is **not validated** by amd64 native tests.
GNU ARM builds need the correct linker/sysroot and target C compiler for bundled
SQLite/ring; no QEMU, toolchain or host runtime changes are performed here.

### Server additions

```text
MONITOR_KUBE_API=https://kubernetes.default.svc
MONITOR_KUBE_TOKEN_FILE=/run/kubernetes/token
MONITOR_KUBE_CA_FILE=/run/kubernetes/ca.crt
MONITOR_NAMESPACE=public-page
MONITOR_AGENT_SERVICE_ACCOUNT=monitor-agent
MONITOR_AGENT_DAEMONSET=monitor-agent
MONITOR_NATIVE_NODES_FILE=/run/monitor-native/nodes.json
```

`MONITOR_AGENT_DAEMONSET` is a **prefix** for the three architecture-specific names.
Native owner file is nonsecret JSON `{"alpha":"approved-Node-UID","gamma":"approved-Node-UID"}`.
No native token rotation. Server API token has the Kubernetes API audience;
agent token has exactly the requested `monitor-ingest` audience. Projected files
are reread for every request, with bounded token lengths and no token logging/DB.

Complete bounded Node LIST reconciliation runs every ten seconds (no watch or
node writes). Failed/incomplete/malformed lists never imply deletion. API outages
and admission-capacity errors appear as `discovery_error` in the nodes response.
Active registry bound 64, retained bound 256; no silent active eviction. Deleted
UIDs retain history, then metadata expires only after its history/keys are gone.
Redis expiry removes evictable observations; bounded maintenance removes deleted
registry/control entries only after their latest possible metric expiry. A 30-day
TTL is an upper bound, **not guaranteed history**. Steady-state eviction is allowed;
backend/capacity failures return 503 rather than fabricated readings.

Private routes, absent from the public router:

- `POST /v1/kubernetes/enroll`: projected bearer, empty body; returns
  `{node_uid,provider,collect,capabilities}`. Native-owned UID returns collect=false;
  its DaemonSet agent idles and checks assignment again every ten seconds.
- `POST /v1/kubernetes/samples`: same Batch shape, but `Sample.node` is the enrolled
  **UID**, never a request-selected display name. Existing native `/v1/samples`
  still accepts alpha/gamma names and maps to approved/live UID before persistence.

Every accepted Kubernetes request performs fresh TokenReview requesting
`monitor-ingest`, requires returned audience and exact ServiceAccount namespace,
then verifies bound Pod name/UID (review extras or same reviewed token claims),
live Pod namespace/UID/SA/nondeletion/nodeName, single controlling architecture-
selected DaemonSet owner and its current UID, and live Node identity/readiness.
No unbound/legacy token, positive authorization cache, provider fallback or
request-controlled API destination. API failure denies ingestion. Four global
ingest requests before body processing, per-node quota and API concurrency/deadline
bounds apply. Before acquiring a global slot or performing live validation, an
ephemeral SHA-256 credential-fingerprint gate rejects concurrent reuse of the same
bearer across enrollment/upload/native routes with 429. At most 64 fingerprints
are retained, only while requests run; cancellation, timeout and completion release
them. This is not an authorization cache: admitted requests still receive fresh
live validation and authoritative UID/provider/quota checks afterward. Namespace workload writers able to use this ServiceAccount remain
trusted: owner references alone are not cryptographic controller provenance.

Human `/api/nodes` now returns `{nodes:[{uid,name,lifecycle,collection,provider,
capabilities,first_seen,last_seen,last_sample,latest}],discovery_error}`. Collection
states pending/fresh/stale/blocked are explicit; fresh means sample age <=60s.
`/api/history?node=<UID>&hours=1` uses UID, including retained deleted nodes. UI
option values preserve UID identity, including archived nodes and duplicate names.
Option labels are exactly the node name; lifecycle, collection, provider, UID and
capability limits are shown outside the menu.

### Confined automatic agent profile

```text
MONITOR_AGENT_MODE=kubernetes
MONITOR_NODE=<Downward API spec.nodeName>
MONITOR_ENDPOINT=https://monitor-collector.public-page.svc.cluster.local:8443/v1/kubernetes/samples
MONITOR_TOKEN_FILE=/run/monitor-identity/token
MONITOR_CA_FILE=/run/monitor-ca/ca.crt
```

Native mode remains default; native `MONITOR_SERVER_IP` is required and its
client-local Service DNS mapping is unchanged. In Kubernetes mode server IP is
optional and normal cluster DNS is used. Enrollment is derived as fixed sibling
`/v1/kubernetes/enroll`; token file is reread before enrollment and upload/retry.

Automatic profile reads only these specific mounted host sources:

| Agent path             | Host source                                          |
| ---------------------- | ---------------------------------------------------- |
| `/host/proc/stat`      | `/proc/stat`                                         |
| `/host/proc/meminfo`   | `/proc/meminfo`                                      |
| `/host/proc/diskstats` | `/proc/diskstats`                                    |
| `/host/proc/boot_id`   | `/proc/sys/kernel/random/boot_id`                    |
| `/host/proc/net_dev`   | **`/proc/1/net/dev`**, not pod/self procfs network   |
| `/host/probes/root`    | new empty `/var/lib/public-page/monitor-probes/root` |

Host sources are read-only, no broad host root/proc/sys mount, hostPID/network,
GPU device, privileged init container or runtime socket. Operator-owned manifests
use nonroot/drop-all/no escalation/readonly root/seccomp; only the approved empty
probe may be automatically created by kubelet. Safe physical/WWAN/Wi-Fi/WireGuard
NIC patterns cap at four; whole sd/nvme/vd/xvd/mmcblk devices cap at eight. No veth/CNI/bridge/
loopback discovery. Truncation/unreadable-source diagnostics are explicit in
sample status. Missing counters invalidate baselines; no inflated recovery rate.
Metric namespace `fs.probe_root.*` describes the probe's containing filesystem,
not necessarily `/`. GPU telemetry is null with explicit unavailable reason;
extra filesystem capacity is not mounted/discovered. Native gamma's fixed
read-only NVIDIA query and native approved probes remain unchanged.

## Build and local checks

Rust 1.99.1 is pinned in `rust-toolchain.toml`; the declared minimum is 1.90.
The lockfile is required. Redis 7.4 is the runtime backend. SQLx/bundled SQLite
is used only by the explicit read-only snapshot importer, never by serve/agent.

Run from this standalone repository root (or the parent's `monitor/` submodule).
Rust tests require a real disposable `redis-server` executable; absence is an
error, not a skipped test. Set `REDIS_SERVER=/temporary/path/redis-server` if it
is not on PATH. Docker checks stages install it; runtime images do not contain
Redis server or a SQLite database. No existing Redis/data/production is used:

```sh
# Node 22 LTS, npm; always build embedded assets before ANY cargo check/test/build.
npm ci
npm run lint
npm run build
npm run test:unit
CHROMIUM_PATH=/usr/bin/google-chrome-stable npm run test:ui
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo test --locked representative_thirty_day_rollup_query_is_bounded -- --nocapture
# Build context is this standalone repo root, not the parent stack root.
docker build -t resource-monitor:local .
docker build -f Dockerfile.arm64 -t resource-monitor-agent:local-arm64 .
docker build -f Dockerfile.armv7 -t resource-monitor-agent:local-armv7 .
docker run --rm --network none resource-monitor:local --help
```

The standalone React 19 / TypeScript / MUI 7 / Vite frontend lives in
`src/{api,hooks,components,pages,ui}` alongside unchanged Rust modules. Vite emits
fixed `dist/index.html`, `dist/app.js`, and `dist/style.css`; Rust embeds them at
compile time. `dist/` and `node_modules/` are generated and ignored. `npm run dev`
is a local UI-only development server, not an Auth/API proxy or production server.
Browser tests serve the built assets with strict nonce CSP and intercept every
Auth/telemetry request; no production fixture, credentials or database is used.
The old `node --test tests/ui.test.cjs` hook is replaced by `npm run test:unit`
and `npm run test:ui`; parent CI must run npm build before Rust gates.

All three Dockerfiles use the same immutable amd64 Node 22 Bookworm build-only
stage (`npm ci`, lint, build), followed by the pinned Rust Bookworm checks stage.
Node is absent from the final image. Rust fmt/clippy/tests and release compilation
are mandatory. ARM64/ARMv7 builds preserve the cross-linker, target ELF checks,
explicit disposable QEMU `--help` checks and matching target-root final metadata;
no foreign-platform RUN or host binfmt/QEMU setup is required. Cross builds require
the same apt/rustup/crates network access as before. Source inputs include package
manifests/lock, Vite/TypeScript/ESLint configs, index, all frontend TS/TSX/CSS and
Rust sources, tests, Cargo lock/toolchain and the three Dockerfiles.

In environments without Docker bridge egress, local
builds can use `docker build --network=host ...`; no host DNS/firewall changes are
necessary. The runtime is Debian Bookworm slim, CA certificates, UID/GID 65532,
with `/usr/local/bin/resource-monitor`. The binary uses GNU glibc (not musl),
compatible with modern x86_64 Arch glibc. Do not copy a locally built Arch binary
into the Debian runtime. Extract native release artifacts from the tested image
using `docker create` / `docker cp` / `docker rm` on that temporary container;
record the image digest and binary checksum before authorized installation.

## Serve configuration

Runtime configuration is environment-based. Import commands additionally require
explicit CLI arguments; see the migration procedure below.

| Variable                   | Default / contract                                                                              |
| -------------------------- | ----------------------------------------------------------------------------------------------- |
| `MONITOR_HTTP_ADDR`        | `0.0.0.0:8080`, public shell/read-only human API                                                |
| `MONITOR_INGEST_ADDR`      | `0.0.0.0:8443`, separate private **TLS** listener                                               |
| `MONITOR_REDIS_URL`        | `redis://127.0.0.1:6379/0`; private dedicated endpoint, DB 0, no credentials/query/fragment     |
| `MONITOR_AUTH_URL`         | `http://auth:8000/api/user/`, trusted fixed configuration, exact path; never request-controlled |
| `MONITOR_NODE_TOKENS_FILE` | `/run/monitor/nodes.json`                                                                       |
| `MONITOR_TLS_CERT_FILE`    | `/run/monitor/tls.crt`, PEM server certificate/chain                                            |
| `MONITOR_TLS_KEY_FILE`     | `/run/monitor/tls.key`, PEM private key                                                         |

The credential file is an object with **exactly** `alpha` and `gamma` keys and
distinct high-entropy tokens (32–512 printable ASCII characters), generated
out-of-band. No example working credentials or private keys are committed.
Mount it and TLS files read-only, readable only by the service UID. Agent
credentials are not human credentials. Configuration/credential rotation requires
a bounded restart; credential revocation is immediate once the replacement map
has loaded. No secrets in environment variables, image layers, URLs or logs.

For an isolated local Docker run, prepare a disposable dedicated Redis with
`--save "" --appendonly no --maxmemory 64mb --maxmemory-policy volatile-lru`, test
TLS certificate/key and test node map outside the repository, plus an isolated
Auth fixture. Do not connect tests to production Auth or mount existing data:

```sh
docker run --rm --name monitor-local \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,noexec,size=16m \
  -p 127.0.0.1:8080:8080 -p 127.0.0.1:8443:8443 \
  -v "$PWD/local-runtime:/run/monitor:ro" \
  -e MONITOR_AUTH_URL=http://auth-fixture:8000/api/user/ \
  -e MONITOR_REDIS_URL=redis://redis-fixture:6379/0 \
  --network monitor-test resource-monitor:local serve
```

`local-runtime`, `redis-fixture` and `monitor-test` are operator-created **new test**
resources, not committed fixtures or instructions to reuse production paths.
The fixture must be attached to that pre-existing isolated test network. Only
publish 8080 through production ingress; the 8443 Service is private ClusterIP,
never an Ingress route. The public router does not register `/v1/samples`.

## Human API and authorization

- `GET /healthz`: public process-only `200 ok`, no storage, node, or Auth details.
- `GET /readyz`: bounded backend/config/schema/import-state readiness; unavailable
  or staging returns 503. Redis outages do not turn liveness into a restart loop.
- `GET /`, `/app.js`, `/style.css`: public data-free embedded assets; no CDN.
- `GET /api/nodes`: dynamic UID inventory described above; `latest` is null before
  ingestion. `gpu` has `uuid` (nullable) and `reason`.
- `GET /api/history?node=<UID>&hours=1`: `{node,bucket_ms,points:[{timestamp_ms,metrics}]}`.
  Hours must be an integer 1–720, registered UID only; unknown/duplicate query
  fields are rejected. Each key is a series; values are nonnegative numbers or
  null. An empty metrics object is an explicitly missing bucket.

Every data request requires exactly one explicit `Authorization: Bearer ...`.
Cookies are ignored. Each request makes a fresh internal Auth GET; no positive
cache. A successful response is a JSON user object or an exactly-one-element
user array (the current unpaginated Auth list response), with a positive integer
`id` and exact boolean `services.monitor`. Empty/multiple-user arrays, duplicate
known JSON fields, string booleans and missing/malformed fields fail closed. Missing/invalid bearer returns 401, absent
current grant returns 403, unavailable/malformed Auth returns 503. The same
bearer is denied on the next request after live staff revocation.

Auth response is capped at 64 KiB and five seconds including streamed reads;
no redirects, environment proxies or request-controlled destination. Four
concurrent data operations, sixteen Auth validations and a twelve-second whole
API deadline bound work. All public responses, including errors, are no-store.
CSP allows local scripts, nonce-bearing Emotion styles, local CSS and exactly
`https://auth.i3alumba.ru` connections;
framing, MIME sniffing, referrers and inline script are disabled. No CORS bypass.

The UI restores Auth `/api/session/` with credentials, uses the returned CSRF token
on POST `/api/session/token/`, keeps bearer/rotating refresh in memory, refreshes
single-flight through `/api/token/refresh/` and logs out via CSRF-protected
`/api/session/logout/` with its **current owned** refresh (including an in-flight
rotation). No URL/storage/cookie token persistence. Sign-in remains on Dashboard.
401/403/503 and network failure clear charts; null/missing buckets break lines.
Axes use actual timestamps, units, ticks, per-series labels and hover values.
Default readouts show the last recorded bucket and its explicit timestamp, skipping
only trailing empty intervals. A real record with null or missing selected metrics
still says unavailable: older values are never resurrected. Hovering a gap shows
that gap, and chart paths still break across it.

The shared theme preference is the only cookie this frontend writes across
subdomains. Canonical `src/ui/sharedTheme.ts`, `themeStorage.ts` and
`ThemeToggle.tsx` match the unified UI contract. Cookie preference wins;
storage-denied operation falls back to memory. Monitor does not persist auth
tokens in cookies, URLs or storage; existing legacy cookies are ignored. Each public response has a fresh
OS-random style nonce; HTML injects it into the Emotion cache. Scripts stay
self-hosted with no unsafe-inline/eval, external fonts or CDN. Five-minute averages
can conceal sub-bucket gaps. Chromium tests exercise actual bundled React/MUI and
canvas under CSP with isolated network mocks; real Auth CORS/CSRF/browser HTTPS
acceptance remains an operator gate, not proven by those fixtures.

The default CPU view renders only selected-resource charts (at most four), with
node/resource/contextual device or interface/history selectors. Network interfaces
are not summed. Disk throughput, operations and utilization remain distinct.
Filesystem probe capacity is not guaranteed root/PVC quota. Unsupported/null-only
GPU telemetry shows unavailable, never fabricated zeros. Lines break at null,
empty and irregular missing buckets; x coordinates and hover use timestamps.
Loading, empty, denial, discovery failure, stale collection and retry states are
explicit. Selection changes abort/invalidate old requests; every error clears
measurements. Logout waits for in-flight session issuance/refresh rotation before
revoking the current owned refresh and clearing local state.

Existing Auth JWT access tokens are not globally invalidated by session logout;
their normal lifetime still applies. Previously downloaded metrics cannot be
recalled. Internal Auth HTTP and existing ingress transport are unchanged.

### Historical validation and limitations (React interface change)

Local locked Rust checks: 30 tests passed, one expensive representative 30-day
size/rollup test remains ignored in the default suite (not rerun for UI-only work).
Frontend: four auth unit tests and 20 mocked Chromium tests passed. All three
Dockerfiles built locally with their mandatory checks; ARM target ELF/ABI and
explicit QEMU help gates passed. Final architectures were amd64/arm64/arm;
Docker did not report Variant (ARMv7 keeps the existing hard-float/loader gates). Builds used `--network=host` after the default-network Rust component
download timed out; this did not change host networking configuration. No images
were pushed or deployed. Tests cover
CSRF/bearer/session restoration, refresh single-flight and logout ownership,
denial/network clearing, late node/time/resource/auth responses, archived/duplicate
UID identities, resource/device selectors, empty/null/gap/irregular timestamp
semantics, shared theme and denied storage, mobile layout, and nonce CSP.

Versions intentionally match the existing Dashboard generation without a broad
dependency upgrade. `npm audit` currently reports 12 development-tool findings
(11 moderate, one high, including Vite dev-server advisories). Vite is build-only
and its dev command binds loopback; the Rust runtime does not ship Node/Vite or a
dev server. Review targeted tooling patches separately before exposing development
servers. This is not a claim of dependency-security acceptance or production
verification. Parent CI/build-input and ARM policy integration is required.

## Agent configuration and collection

| Variable                   | Default / contract                                                                    |
| -------------------------- | ------------------------------------------------------------------------------------- |
| `MONITOR_NODE`             | required `alpha` or `gamma`                                                           |
| `MONITOR_ENDPOINT`         | `https://monitor-collector.public-page.svc.cluster.local:8443/v1/samples`             |
| `MONITOR_SERVER_IP`        | required assigned private Service IP                                                  |
| `MONITOR_CA_FILE`          | `/etc/public-page-monitor/ca.crt`                                                     |
| `MONITOR_TOKEN_FILE`       | systemd credential path (`%d/token` in supplied unit)                                 |
| `MONITOR_INTERVAL_SECONDS` | `10`, accepted 10–60                                                                  |
| `MONITOR_FILESYSTEMS_JSON` | `{"root":"/var/lib/public-page/monitor-probes/root","hdd":"/run/monitor-probes/hdd"}` |
| `MONITOR_INTERFACES`       | empty; comma-separated explicit physical NIC allowlist, maximum four                  |

The HTTPS URL must have exact `/v1/samples` path, no credentials/query/fragment.
The client uses only the configured CA, verifies hostname/expiry, disables
proxies/redirects, and locally resolves the URL hostname to `MONITOR_SERVER_IP`.
The URL host must match the server certificate SAN. No `/etc/hosts`, global DNS,
firewall or route modifications; existing private host-to-Service connectivity
must be verified before installation. No inbound agent port exists.

Metrics and semantics:

- `cpu.used_percent`: delta of first eight `/proc/stat` counters; guest times
  already included in user/nice, so not counted again. Idle includes iowait.
- `memory.{total,available,used}_bytes`: MemAvailable, not MemFree; swap separate.
- `fs.{root,hdd}.{total,free,available}_bytes`: **statvfs only**; free versus
  unprivileged available exposes reserved space. Not PVC/btrfs quota accounting.
- `disk.{nvme0n1,sda}.{read_bytes_per_second,write_bytes_per_second,read_ops_per_second,write_ops_per_second,busy_percent}`:
  whole-device diskstats, **512-byte sectors**, not block-size assumptions;
  no partitions summed. Busy uses millisecond counter deltas.
- `net.<interface>.{rx,tx}_{bytes,packets,errors,drops}_per_second`: native host
  network namespace; no auto-discovery/summing bridges, veth or container traffic.
- `gpu.nvidia.{available,utilization_percent,memory_used_bytes,memory_total_bytes,temperature_celsius,power_watts}`:
  gamma executes ONLY `/usr/bin/nvidia-smi` with fixed read-only CSV query,
  absolute trusted executable, clean environment, no shell, 4096-byte output cap,
  two-second deadline and kill-on-drop. Exactly one expected GPU; UUID persisted
  on latest sample. Unsupported readings null. `available` is a status flag,
  not a utilization measurement. Alpha NVIDIA is explicitly unavailable; Intel
  telemetry fields are unsupported/null. No driver/toolkit installation or controls.

All rates use `Instant` elapsed time; first sample, counter decrease, invalid or

> 120-second interval yields null. Boot/process identities are independent UUIDs;
> agent restart starts new baselines. A disappeared allowlisted device has null
> rates; subsequent lower counters reset its baseline. UTC timestamps are for
> storage/display, not rates. Hardware replacement with unchanged/increased counters
> cannot be inferred from procfs alone; operator restarts the agent after replacing
> hardware. GPU UUID identifies the observed gamma card.

### Native systemd installation boundary

Templates are in `systemd/`; **do not install/start them without authorization**.
The same template runs separately on alpha and gamma with separate node/env/token
files. DynamicUser is nonroot, with no capabilities, strict filesystem protection,
private tmp, protected kernel/control groups, bounded memory/tasks/CPU and
NoNewPrivileges. LoadCredential keeps the token out of environment variables.
The binary and fixed nvidia-smi must be root-owned/immutable to the agent.

Before creating probes, operator verifies the real HDD mount. Create only new
empty probe directories, root-owned/searchable, never existing application data.
Bind **only** alpha `/home/i3alumba/HDD/.monitor-probe` or gamma
`/media/hdd/.monitor-probe` read-only to `/run/monitor-probes/hdd` using a unit
drop-in (`BindReadOnlyPaths=SOURCE:/run/monitor-probes/hdd`). Do not bind HDD roots.
Also bind the new rootfs probe to `/run/monitor-probes/root`; its source must
be on rootfs. Native units override `MONITOR_FILESYSTEMS_JSON` to
`{"root":"/run/monitor-probes/root","hdd":"/run/monitor-probes/hdd"}`, as shown
in `agent.env.example`. These binds avoid traversing mode-0700 source parents or
paths hidden by ProtectHome; do not broaden parent permissions or expose roots.
A missing probe reports null, never silently uses root. Check mount identity
before installation and after host mount changes.

Alpha uses PrivateDevices. Gamma's **candidate** `gamma-gpu.conf` lists only
`/dev/nvidiactl` and `/dev/nvidia0`, never `/dev/nvidia*`, modeset, UVM or compute
capabilities/groups. Existing driver ioctls require rw device opens; a device
cgroup cannot prove ioctl read-only. The adapter itself permits only the fixed
telemetry command. Operator must verify exact nodes and successful telemetry
under this confined nonroot unit. If it needs more access, stop and request a
review; do not broaden the sandbox. Real gamma GPU and sandbox acceptance has
not been performed by local fixture tests.

## Ingestion and storage contract

Private TLS `POST /v1/samples`, application/json, no compression:

```json
{
  "samples": [
    {
      "node": "alpha",
      "boot_id": "UUID",
      "process_id": "UUID",
      "seq": 0,
      "timestamp_ms": 0,
      "metrics": { "cpu.used_percent": null },
      "gpu": {
        "uuid": null,
        "reason": "NVIDIA driver unavailable; Intel utilization unsupported"
      }
    }
  ]
}
```

Replace UUID/timestamp in actual data; the example is intentionally not an
accepted sample. `gpu` may be omitted by synthetic older clients (empty default).
Fixed alpha/gamma bearer maps to body identity with constant-time token comparison;
cross-node payloads fail. Limit 128 KiB, 1–6 samples, 128 finite numeric/null keys,
96-character safe family-prefixed keys, bounded GPU metadata. Keys are capped
**persistently** at 128 per node, including label churn. Four concurrent requests
before body decoding, 10-second request deadline, one request/second/node.
No decompression. Bad identity/auth 401; invalid sample 400; quota 429; storage
pressure 503; accepted/replayed batch 204.

Allowed backfill is 120 seconds and future skew 30 seconds. Dedup identity is
(node UID, boot, process, signed i64 sequence). Rust handles sequences exactly,
including values above 2^53. Protected per-session high-water/13-position windows
survive independent raw/rollup eviction. Reordered retries may lag at most twelve;
already seen identities never increment surviving aggregates or reconstruct lost
chunks. Per-UID control holds at most 1,024 sessions and 128 historical metric keys;
metadata admission fails closed when full. Expired sessions are pruned on ingest.

Redis uses the fixed versioned `monitor:v1` namespace. Registry (64 active / 256
retained UIDs), ownership, metric-key union, session windows and import proof have
**no TTL** and are protected by `volatile-lru`. Raw ten-second chunks (<=128
records, <=512KiB), five-minute sum/count chunks and latest observations have
absolute expiry at interval start +30 days (latest uses its timestamp), except
the bounded one-time full-snapshot migration grace described below. Missing
raw/aggregate keys become gaps; missing latest returns null, never an older
substitute. New observations may create partial aggregates after eviction.

Rust prepares bounded replacements; a Lua CAS checks all inputs including the
active generation and staging lock before **one MSET**, then applies prevalidated
absolute expiries. Lua isolation is not treated as rollback: there are no fallible
incremental metric/control writes before MSET. Redis OOM rejects the write rather
than committing a batch prefix. Batch/cardinality/overflow failures leave the
entire previous state intact. A timed-out commit may have succeeded; retries are
safe. Non-TTL metadata can exhaust memory; 503 remains possible with eviction.

Queries <=6h enumerate deterministic raw chunks, longer queries use original
five-minute weighted sums/counts, never averages of averages. Hours remain 1..720,
<=1,200 points and <=128 keys. Pages <=128 chunks and a 64MiB total input budget
bound decoding/work, plus the existing five-second API deadline. An exceeded
budget/corrupt backend returns 503, not successful gaps; absent keys alone are
gaps. Unsupported/null-only measurements remain null.

Before every runtime operation, including existing-generation reads/discovery,
configuration is validated: a writable primary with `save ""`, `appendonly no`, explicit
`maxmemory >=16MiB` and `maxmemory-policy volatile-lru`. It does not reconfigure
Redis or silently fall back to SQLite. The dedicated private endpoint must be
policy-isolated (namespace workload writers remain trusted); no public Redis
Service/Ingress, replication or credentials in URLs. Parent deployment owns
actual memory headroom, Redis image pin and network policy.

Monitor restart with Redis intact preserves history/dedup. Redis restart loses
**all** in-memory observations/control/proof together: normal serve initializes
empty, rebuilds live discovery and never reimports an old snapshot automatically.
An independent Redis Deployment avoids monitor Recreate losing imported history.
SIGTERM/SIGINT drains listeners for up to five seconds.

### Explicit SQLite snapshot import

Initial migration must import every accumulated metric row in the final fenced snapshot. Never use a cold
start as proof of migration. Fence the source writer and create a standalone
SQLite-aware backup. Do not copy an active DB file without its WAL, alter either
old PVC, or give the new server a production SQLite mount. Schema **v2** is
required: older name-keyed v1 history must first undergo its separately reviewed
UID migration; unknown schemas fail. SQLx opens the standalone snapshot read-only,
immutable and query-only, rejects WAL/journal sidecars, checks integrity and
verifies SHA-256 before and after the operation.

Use an isolated temporary operator importer Pod with disk emptyDir, reviewed
monitor image, no service-account token and private Redis access. No public
upload/import HTTP endpoint exists. Example commands (all deployment/source
fencing still separately authorized):

```sh
# Stream a <=8GiB standalone snapshot; output must not already exist.
resource-monitor receive-snapshot --output /staging/final.sqlite3 --sha256 "$SHA" < final.sqlite3
# MONITOR_REDIS_URL is the exact independent target Redis instance.
resource-monitor import-sqlite --snapshot /staging/final.sqlite3 --sha256 "$SHA" --attempt "$ATTEMPT"
resource-monitor import-sqlite --snapshot /staging/final.sqlite3 --sha256 "$SHA" --attempt "$ATTEMPT" --resume --verify-only
resource-monitor import-sqlite --snapshot /staging/final.sqlite3 --sha256 "$SHA" --attempt "$ATTEMPT" --resume --activate
```

`receive-snapshot` enforces `--max-bytes` (default/hard maximum 8GiB), hashes
bounded stdin, fsyncs a mode-0600 partial and finalizes without overwriting an
existing destination. Delete the temporary staging Pod after acceptance.

The destination must initially be pristine, dedicated DB 0. **Acquire staging
before any monitor server initializes that destination.** A non-expiring lock
blocks readiness, discovery, maintenance, reads and ingest throughout import.
Deterministic replacements make replay idempotent; there are no additive rollup
replays. A crashed importer leaves staging blocked. Resume must match snapshot,
attempt UUID, generation and Redis run_id. After confirming the old importer is
terminated, `--resume --takeover` explicitly replaces its worker token. No TTL
lock or automatic timeout enables a partial generation. There is no automatic
replacement/reset/abort command; a failed attempt requires operator review.

Fidelity scope is **every raw and original rollup row in the fenced snapshot**,
without an import-time cutoff, plus all metric keys, every registry UID/provider/
capability/lifecycle/timestamp, and registry error/cursor. Supported lifecycle
states `present`, `deleted`, `unsupported` and `not-ready` are preserved. Raw
receive times, identities/sequences/GPU/provider and original rollup totals/counts
are preserved; rollups are **not** rederived from raw. Manifest `retained_rows`
equals `rows` for every table; min/max span the full source. The compatibility
field `retained_at_ms` records import start, not a filter.

Hidden metric keys use one fixed **six-hour migration grace**, never extended
by resume. Complete verification compares every expected value, key count and
expiry with a fresh read-only source enumeration, plus source/schema SHA-256,
full table counts/ranges and a canonical key checksum. Redis run_id and
`evicted_keys` must remain unchanged; >=8MiB free Redis commit headroom is
required. Capacity/eviction/mismatch/grace failure stops while staging remains
blocked, never silently accepting lost history or truncating the source. Resume
replays from the beginning in bounded chunks, not an unbounded in-memory image.

Activation re-verifies everything, sets each metric expiry to **max(original
absolute expiry, fixed grace deadline)** and atomically selects the generation.
This bounded one-time migration exception keeps even ancient source chunks
through initial activation/cutover acceptance. Imported dedup sessions and
archived UID/control metadata are protected through that same deadline without
fabricating public timestamps. Duplicate-only ingest does not shorten imported
latest TTLs. Complete acceptance before the fixed deadline; resume does not
extend it. New runtime observations retain the ordinary <=30-day horizon and
API queries remain <=720 hours (ancient copied rows are not added to API ranges).
A crash during expiry restoration requires full replay and verification again.
Import does not deploy or issue a Kubernetes checkpoint.

JSON stdout contains `import_status=verified`, `phase=verified|active`, attempt,
generation, `redis_run_id`, initial `evicted_keys`, `verified_at_ms`,
`verification_sha256` and the complete manifest. Successful activation stores
that immutable JSON in protected `monitor:v1:import-proof`; later observations
do not invalidate it. The operator binds proof to actual target Redis Pod UID,
container ID/restart count/run_id and a fresh fenced-source heartbeat. Before
cutover acceptance, require the same run_id/proof and no new eviction; CI without
Redis access cannot independently attest run_id. Redis restart invalidates proof.

Local real-Redis tests cover eviction/OOM, exact i64 sequences, atomic failures,
weighted null/gap queries, registry bounds, restart empty recovery and import
interruption/resume/mismatch/capacity/source integrity. A representative 8,640
rollups x64 metrics thirty-day debug query measured about 1s with Redis peak
~39MiB on the implementation host, not a production guarantee. Full protected
production-snapshot fidelity/capacity, all three OCI builds and native ARM hardware
remain separate release acceptance gates.

## Release/rollback gates and remaining acceptance

Do not publish, deploy, install collectors, change DNS/network/storage/drivers or
restart Auth without the operator's authorization. Publish changes through a
feature branch from `dev`, a PR into `dev`, then a separate `dev` -> `master` PR.
The parent stack pins a remotely available monitor commit as a Git submodule and
owns Auth, Dashboard, CI/image inventory, Service/Ingress/storage and deployment.

Before production, verify real Auth CORS/CSRF/rotation/revocation, dedicated TLS CA
and certificate identity, host private connectivity, native GNU compatibility,
actual nonroot device access/RTX 3060 readings, host metric comparisons, mount
probes, 30-day bounds, restart recovery and browser HTTPS. Alpha's broken driver
is not repaired. Rollback stops only new workload/agents and retains both old DB/PVC checkpoints;
never delete history or downgrade an unsupported schema automatically.
