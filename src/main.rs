//! codex-proxy: a minimal local proxy that exposes the Codex (ChatGPT
//! subscription) Responses API over an OpenAI-compatible endpoint, guarded by
//! client API keys.

mod auth;
mod config;
mod error;
mod server;
mod translate;
mod upstream;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

use crate::auth::AuthManager;
use crate::config::Config;
use crate::server::AppState;
use crate::upstream::Upstream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::var("CODEXPROXY_CONFIG").unwrap_or_else(|_| "config.toml".into());
    let config = Config::load(&config_path)?;

    init_logging(&config.logging.level);

    // One shared HTTP client for both refresh and forwarding.
    //
    // We deliberately do NOT set a total `.timeout()`: every forward is a
    // long-lived SSE stream, and a total timeout would abort a slow-but-healthy
    // generation mid-stream. `read_timeout` instead bounds *idle* time between
    // chunks, which is the failure we actually want to catch. The OAuth refresh
    // sets its own short per-request timeout (see auth::manager).
    let mut http_builder = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(config.upstream.connect_timeout_secs))
        .read_timeout(Duration::from_secs(config.upstream.request_timeout_secs));

    // Optional outbound proxy for all upstream traffic (e.g. to reach OpenAI
    // from a blocked region/IP). Applies to both forwarding and token refresh.
    if let Some(proxy_url) = &config.upstream.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)
            .context("invalid upstream.proxy URL (expected http://, https://, or socks5://)")?;
        http_builder = http_builder.proxy(proxy);
        // Never log the URL itself — it may carry credentials.
        tracing::info!("routing upstream traffic through configured proxy");
    }

    let http = http_builder.build().context("building HTTP client")?;

    let codex_home = config.codex_home_path();
    tracing::info!(codex_home = %codex_home.display(), "loading credentials");
    let auth = AuthManager::load(&config.upstream, codex_home, http.clone())
        .context("loading Codex credentials (run `codex login` first)")?;

    let upstream = Arc::new(Upstream::new(&config.upstream, http, auth));

    if config.client_auth.require
        && config
            .client_auth
            .keys
            .iter()
            .any(|k| k == "sk-local-changeme")
    {
        tracing::warn!("client_auth still uses the default key 'sk-local-changeme' — change it");
    }

    let state = AppState {
        config: Arc::new(config.clone()),
        upstream,
    };

    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!("listening on http://{addr}");

    axum::serve(listener, server::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    Ok(())
}

fn init_logging(level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("codex_proxy={level},tower_http=warn")));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
