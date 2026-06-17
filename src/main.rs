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
    // `use_rustls_tls()` pins the TLS backend to rustls (matching the real Codex
    // client); the connection-pool/keepalive settings mirror it too. The actual
    // ClientHello fingerprint comes from the reqwest/rustls versions+features in
    // Cargo.toml, not from anything configured here.
    let mut http_builder = reqwest::Client::builder()
        .use_rustls_tls()
        .pool_max_idle_per_host(4)
        .tcp_keepalive(Duration::from_secs(30))
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
    // Cloud-deploy convenience: write auth.json from a secret if it's missing.
    auth::store::seed_from_env_if_absent(&codex_home)
        .context("seeding auth.json from CODEXPROXY_AUTH_JSON")?;
    let auth = AuthManager::load(&config.upstream, codex_home, http.clone())
        .context("loading Codex credentials (run `codex login` first)")?;

    let upstream = Arc::new(Upstream::new(&config.upstream, http, auth));

    // Guard against shipping an open door. The runtime image carries no
    // config.toml, so a cloud deploy that forgets CODEXPROXY_API_KEYS falls back
    // to the built-in default key while binding 0.0.0.0 — anyone who knows the
    // public placeholder could then spend the subscription. Refuse to start when
    // exposed (non-loopback) without real auth; only warn on a loopback bind.
    let uses_default_key = config
        .client_auth
        .keys
        .iter()
        .any(|k| k == config::DEFAULT_CLIENT_KEY);
    if is_loopback_host(&config.server.host) {
        if config.client_auth.require && uses_default_key {
            tracing::warn!(
                "client_auth still uses the default key 'sk-local-changeme' — change it"
            );
        }
    } else if !config.client_auth.require {
        anyhow::bail!(
            "refusing to start: binding non-loopback host {} with client_auth.require=false — \
             set CODEXPROXY_API_KEYS and keep auth enabled",
            config.server.host
        );
    } else if uses_default_key {
        anyhow::bail!(
            "refusing to start: binding non-loopback host {} with the built-in default API key \
             'sk-local-changeme' — set CODEXPROXY_API_KEYS to strong secret(s)",
            config.server.host
        );
    } else if config.client_auth.keys.is_empty() {
        anyhow::bail!(
            "refusing to start: binding non-loopback host {} with an empty client key set — \
             set CODEXPROXY_API_KEYS",
            config.server.host
        );
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

/// Whether `host` binds only the local machine. Treats the IPv4/IPv6 loopback
/// literals and `localhost` as loopback; everything else (notably `0.0.0.0`,
/// `::`, or a public address) counts as exposed and triggers the auth guard.
fn is_loopback_host(host: &str) -> bool {
    match host.trim().parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        // Not an IP literal: only the well-known hostname resolves to loopback.
        Err(_) => host.eq_ignore_ascii_case("localhost"),
    }
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

#[cfg(test)]
mod tests {
    use super::is_loopback_host;

    #[test]
    fn loopback_hosts_are_recognized() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("LocalHost"));
        assert!(is_loopback_host(" 127.0.0.1 "));
    }

    #[test]
    fn exposed_hosts_are_not_loopback() {
        // The Fly bind address and any public/wildcard address must count as
        // exposed so the default-key guard fires.
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("::"));
        assert!(!is_loopback_host("10.0.0.5"));
        assert!(!is_loopback_host("example.com"));
    }
}
