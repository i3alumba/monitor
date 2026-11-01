FROM --platform=linux/amd64 node:22-bookworm-slim@sha256:9f0dde62d10e7ff7a310ecaebd66225f6b46c17e6cb778449a77b05d3541bea2 AS frontend
WORKDIR /frontend
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json vite.config.ts eslint.config.js index.html ./
COPY src/ src/
RUN npm run lint && npm run build

# GNU/glibc binary: usable on modern Arch hosts and Debian Bookworm.
FROM rust:1.99-bookworm@sha256:0097cc72a939f26665ab11380e995aa9476735ec9516867e14ff5dfa8318b068 AS checks
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src/ src/
COPY --from=frontend /frontend/dist/ dist/
COPY tests/ tests/
RUN cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked
RUN cargo build --locked --release

FROM debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && mkdir /data && chown 65532:65532 /data
COPY --from=checks /build/target/release/resource-monitor /usr/local/bin/resource-monitor
USER 65532:65532
EXPOSE 8080 8443
ENTRYPOINT ["/usr/local/bin/resource-monitor"]
CMD ["serve"]
