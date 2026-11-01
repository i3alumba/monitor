# Agent instructions

This repository owns the Rust server/collectors and self-hosted React, TypeScript
and Material UI interface. Cargo and npm run from this repository root. Read
`README.md` before changes. `src/` contains Rust modules and typed frontend
subdirectories; `tests/` contains Rust, auth-unit and browser fixtures.

## Changes and publication

Check the branch and working tree first. Do not move somebody else's edits, stash,
reset or clean them. Update `dev` with fetch and ff-only pull, then use a separate
feature/fix branch. If `dev` is absent/diverged or switching is unsafe, stop and ask.
All releases use **feature → PR into dev → verified dev → separate dev-to-master
PR**. Never substitute a feature-to-master PR or direct target-branch commit.
Commits, pushes, PRs, merges and deployment need operator authorization. The parent
`LumbaBalumba/public-page` pins a remotely available commit as a Git submodule;
child publication alone does not deploy production.

## Implementation boundaries

- Preserve live server-side administrator authorization, explicit bearer-only data
  requests, CSRF session bridge, memory-only browser tokens and owned refresh/logout.
- Preserve private verified TLS ingestion, Node UID histories, projected-token /
  Pod UID / architecture-specific DaemonSet UID attestation and native ownership.
- Names are display labels, not identities. Missing/unsupported/reset counters are
  null/gaps, never fabricated zero readings. Do not sum physical/WireGuard traffic
  or confuse probe-filesystem capacity with rootfs/PVC quota.
- Render only the selected resource; keep node-option labels name-only. Preserve
  responsive Material controls and per-response nonce CSP without unsafe scripts.
- `src/ui/sharedTheme.ts`, `themeStorage.ts` and `ThemeToggle.tsx` follow the shared
  parent-stack contract. Coordinate changes with Dashboard, Drive and Auth; do not
  add a build-time dependency on a sibling checkout or a new package registry.
- Do not introduce broad host mounts, privileged collectors, GPU controls, drivers,
  runtime changes, secret logging, new credential persistence or data-reset logic.
- Keep SQLite history/PVCs and existing identities/credentials. Production, DNS,
  networking, native installations and restarts require separate explicit scope.

## Checks

```sh
npm ci
npm run lint
npm run build # Required before any Cargo check/build/test: Rust embeds dist/.
npm run test:unit
CHROMIUM_PATH=/usr/bin/google-chrome-stable npm run test:ui
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

Format only changed TS/JS/CSS/HTML/JSON/Markdown with Prettier and Rust with rustfmt.
Do not broadly upgrade dependencies. Keep Node build-only, immutable build stages,
mandatory Rust checks and correct amd64/arm64/ARMv7 image metadata, ELF/ABI/loader
and disposable-QEMU gates. Cross/QEMU validation is not native hardware acceptance.
No host binfmt/QEMU installation is needed. Report actual checks, skipped tests,
limitations, branch/publication state and deployment separately.
