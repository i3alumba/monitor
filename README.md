# Resource monitor

One native GNU/Linux Rust binary, two modes: `resource-monitor serve` and
`resource-monitor agent`. No remote execution, agent listener, GPU control,
container runtime integration, or automatic host configuration. This directory
contains application sources and installation templates; deploying them requires
operator authorization and the parent project's release gates.

## Automatic-node amendment (current deployment contract)

Approved confined Kubernetes collectors extend the two native agents without
reprovisioning existing PV/Secret/CA/native tokens/probes/backups. Server remains
on alpha. Registry/history use **Node UID**, not node name. Existing native
alpha/gamma bearers retain their exact values and authorize only the operator's
approved UID plus the current API Node UID. Same-name replacements get separate
Kubernetes-provider histories; copied native labels do not exclude scheduling or
confer ownership. Provider conflicts are rejected, not silently merged/fallback.

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
Global maintenance deletes <=1000 rows/cycle across all nodes/tables with a fair
persisted cursor and indexed node/time predicates. The unchanged 8GiB DB/free/WAL
guards still apply: 64 admitted nodes is **not** a guarantee that 30 days of their
full-resolution metrics fit. Storage pressure refuses new ingestion explicitly.

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
The lockfile is required. SQLx uses bundled SQLite, a single bounded connection,
and runtime parameterized SQL (no build-time production database).

Run from this standalone repository root (or the parent's `monitor/` submodule):

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
cargo test --locked thirty_day_size_and_rollup_latency -- --ignored --nocapture
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

All configuration is environment-based; only `serve` / `agent` are CLI modes.

| Variable                   | Default / contract                                                                              |
| -------------------------- | ----------------------------------------------------------------------------------------------- |
| `MONITOR_HTTP_ADDR`        | `0.0.0.0:8080`, public shell/read-only human API                                                |
| `MONITOR_INGEST_ADDR`      | `0.0.0.0:8443`, separate private **TLS** listener                                               |
| `MONITOR_DB`               | `/data/monitor.sqlite3`, plain absolute filesystem path                                         |
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

For an isolated local Docker run, prepare a new writable data directory, test
TLS certificate/key and test node map outside the repository, plus an isolated
Auth fixture. Do not connect tests to production Auth or mount existing data:

```sh
docker run --rm --name monitor-local \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,noexec,size=16m \
  -p 127.0.0.1:8080:8080 -p 127.0.0.1:8443:8443 \
  -v "$PWD/local-data:/data" \
  -v "$PWD/local-runtime:/run/monitor:ro" \
  -e MONITOR_AUTH_URL=http://auth-fixture:8000/api/user/ \
  --network monitor-test resource-monitor:local serve
```

`local-data`, `local-runtime` and `monitor-test` are operator-created **new test**
resources, not committed fixtures or instructions to reuse production paths.
The fixture must be attached to that pre-existing isolated test network. Only
publish 8080 through production ingress; the 8443 Service is private ClusterIP,
never an Ingress route. The public router does not register `/v1/samples`.

## Human API and authorization

- `GET /healthz`: public `200 ok`, no storage, node, or Auth details.
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
Axes use actual timestamps, units, ticks, per-series labels and hover values;
the shared theme preference is the only cross-subdomain cookie. Its canonical
`src/ui/sharedTheme.ts` and `themeStorage.ts` are byte-identical to the unified UI
contract. Cookie preference wins; storage-denied operation falls back to memory.
No auth tokens enter cookies, URLs or storage. Each public response has a fresh
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

### Local validation and limitations (React interface change)

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

Allowed backfill is 120 seconds and future skew 30 seconds. Dedup primary key is
(node, boot, process, seq), committed with raw sample, server receive timestamp,
and five-minute sum/count rollups in one FULL-synchronous WAL transaction.
Reordered retry sequence may lag at most twelve behind current process maximum;
duplicate identities are ignored, not added to rollups. Agent queue holds six
samples, drops oldest on overflow and expires old data, retry backoff bounded
at about 32 seconds with per-process jitter. No persistent secret-bearing spool;
restarts/outages longer than the queue produce visible missing data.

Schema version 2 additively migrates v1 native history to approved UIDs and records
provider ownership; unknown newer schema refuses
startup. Raw and rollup history retained thirty days. Every ten seconds bounded
a globally budgeted 1000-row cleanup runs across tables/nodes, followed by a bounded-connection checkpoint;
no routine VACUUM. Queries <=6h use raw samples, longer windows use persisted
five-minute rollups with dynamic bucket aggregation, <=1200 points per series.
At most 128 series/node; long-range averaging is weighted sum/count, not
average-of-averages. Responses are bounded by point/series/key limits; five-second
query timeout plus single-connection DB serialization; no SQL interpolation of
user input. Null-only buckets remain null, empty buckets remain gaps.

SQLite page cap is 8 GiB at enforced 4096-byte pages. WAL auto-checkpoints every
256 pages, retained journal target 16 MiB, ingestion refuses above a 32 MiB WAL
high-water mark; free-space reserve is 1 GiB. These are hardcoded safety defaults,
not unsupported environment knobs. A bounded in-flight transaction can briefly
increase WAL beyond the target before the next guard. Busy/full/reserve pressure
returns 503 and preserves committed history; agent retries safely. No silent
retention shortening, deletion of database, or automatic rollback. Back up using
SQLite's supported online backup procedure or stop only this new service before
copying DB/WAL; never copy an active DB file alone. SIGTERM/SIGINT drains listeners
for up to five seconds; crash reopening recovers committed WAL.

A local representative fixture (518400 node samples, 64 metrics, thirty days)
measured 2,275,676,160 bytes DB and ~0.69s 30-day history on the implementation
host. This is not a production latency guarantee or a filesystem quota. New
alpha-local 10 GiB accounting allocation and actual host free-space monitoring
remain operator responsibilities; no existing storage is reused.

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
is not repaired. Rollback stops only new workload/agents and retains DB/PVC;
never delete history or downgrade an unsupported schema automatically.
