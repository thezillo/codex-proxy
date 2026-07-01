# --- build stage ---
# musl instead of glibc: a statically-linked binary sidesteps the
# builder/runtime glibc-version matching that bit us before (bookworm vs
# trixie -> "GLIBC_2.39 not found"), and musl's single-arena allocator keeps
# RSS flatter under concurrent load than glibc's per-thread arenas. Safe here
# because the dependency tree is pure-Rust: crypto provider is `ring` (no
# aws-lc-rs/cmake), no openssl-sys or other C deps, DNS goes through musl's
# own getaddrinfo (no hickory-dns).
FROM rust:1.95-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY . .
RUN cargo build --release

# --- runtime stage ---
FROM alpine:3.20
# rustls bundles its own roots, but keep ca-certificates for any system TLS path.
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/codex-proxy /usr/local/bin/codex-proxy

# Bind on all interfaces inside the container; credentials live on a mounted
# volume — point CODEXPROXY_DATA_DIR at its mount path (e.g. -v codex_data:/data
# with CODEXPROXY_DATA_DIR=/data) so rotated tokens survive restarts.
ENV CODEXPROXY_HOST=0.0.0.0 \
    CODEXPROXY_PORT=8787
# /metrics stays on its own port, loopback-only by default (see
# CODEXPROXY_METRICS_HOST/PORT) — deliberately NOT set to 0.0.0.0 here, so a
# deploy that just copies this image doesn't silently expose metrics
# alongside the public API without an explicit choice to do so.
EXPOSE 8787 9090
ENTRYPOINT ["codex-proxy"]
