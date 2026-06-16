# --- build stage ---
FROM rust:1.95-slim AS builder
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
# volume at /data (set CODEXPROXY_CODEX_HOME=/data in fly.toml).
ENV CODEXPROXY_HOST=0.0.0.0 \
    CODEXPROXY_PORT=8787
EXPOSE 8787
ENTRYPOINT ["codex-proxy"]
