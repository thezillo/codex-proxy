//! Upstream client: forwards request bodies to the Codex Responses API with
//! the exact headers the official client sends.

use std::sync::Arc;

use crate::auth::AuthManager;
use crate::config::UpstreamConfig;
use crate::error::ProxyError;

pub struct Upstream {
    http: reqwest::Client,
    auth: Arc<AuthManager>,
    responses_url: String,
    originator: String,
    user_agent: String,
}

impl Upstream {
    pub fn new(cfg: &UpstreamConfig, http: reqwest::Client, auth: Arc<AuthManager>) -> Self {
        let responses_url = format!(
            "{}{}",
            cfg.base_url.trim_end_matches('/'),
            cfg.responses_path
        );
        let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
        tracing::info!(%responses_url, %user_agent, "upstream configured");
        Self {
            http,
            auth,
            responses_url,
            originator: cfg.originator.clone(),
            user_agent,
        }
    }

    /// Forward a raw JSON body to `/responses` and return the upstream response
    /// (streamed — we do not buffer the body).
    pub async fn forward_responses(
        &self,
        body: bytes::Bytes,
    ) -> Result<reqwest::Response, ProxyError> {
        let auth = self.auth.headers().await?;

        let mut req = self
            .http
            .post(&self.responses_url)
            .header("Authorization", format!("Bearer {}", auth.bearer))
            .header("originator", &self.originator)
            .header("User-Agent", &self.user_agent)
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .body(body);

        if let Some(account_id) = auth.account_id {
            req = req.header("ChatGPT-Account-ID", account_id);
        }

        req.send()
            .await
            .map_err(|e| ProxyError::Upstream(format!("forward to responses failed: {e}")))
    }
}

/// Build a User-Agent byte-for-byte identical to the official Codex CLI:
///   `{originator}/{cli_version} ({OsType} {os_version}; {arch})`
///
/// No `codex-proxy` suffix (that would fingerprint the proxy to ChatGPT), and
/// `cli_version` is the impersonated Codex CLI release from config — not this
/// crate's own version. The OS/arch are read from `os_info` at runtime so the
/// string always reflects the real host instead of a hardcoded guess.
fn build_user_agent(originator: &str, cli_version: &str) -> String {
    let info = os_info::get();
    format!(
        "{}/{} ({} {}; {})",
        originator,
        cli_version,
        info.os_type(),
        info.version(),
        info.architecture().unwrap_or("unknown"),
    )
}
