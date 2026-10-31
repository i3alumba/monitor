# GNU/glibc binary: usable on modern Arch hosts and Debian Bookworm.
FROM rust:1.99-bookworm@sha256:0097cc72a939f26665ab11380e995aa9476735ec9516867e14ff5dfa8318b068 AS checks
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src/ src/
COPY assets/ assets/
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
