# --- build stage ---
# Pin to bookworm so the binary links against the same glibc (2.36) as the
# runtime image below. The default `rust:1.95-slim` tracks Debian trixie
# (glibc >=2.39), which produces a binary that won't run on bookworm-slim.
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release

# --- runtime stage ---
FROM debian:bookworm-slim
# rustls bundles its own roots, but keep ca-certificates for any system TLS path.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
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
