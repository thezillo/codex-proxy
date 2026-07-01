//! In-memory token manager: loads auth.json, refreshes the access token a bit
//! before it expires, and hands out the `(bearer, account_id)` pair the
//! upstream request needs.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::auth::{jwt, store};
use crate::config::UpstreamConfig;
use crate::error::ProxyError;

/// What an upstream request needs to authenticate.
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    pub bearer: String,
    pub account_id: Option<String>,
}

#[derive(Debug, Clone)]
struct TokenState {
    access_token: String,
    refresh_token: String,
    id_token: String,
    account_id: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

pub struct AuthManager {
    codex_home: PathBuf,
    issuer: String,
    client_id: String,
    refresh_skew_secs: i64,
    http: reqwest::Client,
    state: RwLock<TokenState>,
    /// Serializes refreshes so concurrent requests don't all hit the OAuth
    /// endpoint at once (single-flight).
    refresh_lock: Mutex<()>,
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    client_id: &'a str,
    grant_type: &'static str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
struct RefreshResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

impl AuthManager {
    /// Load auth.json and build the manager. Fails fast if there are no tokens
    /// (the user must run `codex login` first).
    pub fn load(
        cfg: &UpstreamConfig,
        codex_home: PathBuf,
        http: reqwest::Client,
    ) -> anyhow::Result<Arc<Self>> {
        let auth = store::load(&codex_home)?;
        let tokens = auth.tokens.ok_or_else(|| {
            anyhow::anyhow!("auth.json has no `tokens` — run `codex login` first")
        })?;

        let expires_at = match jwt::expiration(&tokens.access_token) {
            Ok(exp) => exp,
            Err(e) => {
                tracing::warn!(
                    "could not parse access-token expiry ({e}); will refresh on first request"
                );
                None
            }
        };
        if expires_at.is_none() {
            tracing::warn!("access token has no parseable `exp`; relying on refresh before use");
        }
        let account_id = tokens
            .account_id
            .clone()
            .or_else(|| jwt::account_id(&tokens.id_token));

        let state = TokenState {
            access_token: tokens.access_token,
            refresh_token: tokens.refresh_token,
            id_token: tokens.id_token,
            account_id,
            expires_at,
        };

        Ok(Arc::new(Self {
            codex_home,
            issuer: cfg.issuer.clone(),
            client_id: cfg.client_id.clone(),
            refresh_skew_secs: cfg.refresh_skew_secs,
            http,
            state: RwLock::new(state),
            refresh_lock: Mutex::new(()),
        }))
    }

    /// Returns a valid bearer + account id, refreshing first if the token is
    /// expired or about to expire.
    pub async fn headers(&self) -> Result<AuthHeaders, ProxyError> {
        if self.needs_refresh().await {
            self.refresh_if_needed().await?;
        }
        self.current_headers().await
    }

    /// Force a refresh regardless of the cached expiry, then return the
    /// refreshed headers. For when the upstream itself rejects the current
    /// bearer with 401 despite our client-side check saying it should still
    /// be valid (clock skew, early revocation, ...) — mirrors the real Codex
    /// CLI's own refresh-and-retry-once behavior on 401.
    pub async fn force_refresh_headers(&self) -> Result<AuthHeaders, ProxyError> {
        self.force_refresh().await?;
        self.current_headers().await
    }

    async fn current_headers(&self) -> Result<AuthHeaders, ProxyError> {
        let state = self.state.read().await;
        Ok(AuthHeaders {
            bearer: state.access_token.clone(),
            account_id: state.account_id.clone(),
        })
    }

    async fn needs_refresh(&self) -> bool {
        let state = self.state.read().await;
        match state.expires_at {
            // Unknown expiry: we cannot prove the token is still valid, so
            // refresh rather than forward a possibly-dead bearer forever. The
            // refresh is single-flight, so this costs at most one OAuth call.
            None => true,
            Some(exp) => {
                let threshold = Utc::now() + chrono::Duration::seconds(self.refresh_skew_secs);
                exp <= threshold
            }
        }
    }

    /// Refresh only if the cached expiry says it's actually needed —
    /// single-flight: whoever grabs the lock refreshes, others re-check.
    async fn refresh_if_needed(&self) -> Result<(), ProxyError> {
        let _guard = self.refresh_lock.lock().await;
        if !self.needs_refresh().await {
            // Someone else refreshed while we waited for the lock.
            return Ok(());
        }
        self.perform_refresh().await
    }

    /// Refresh unconditionally, bypassing the cached-expiry check — for the
    /// reactive 401 path, where the expiry check already said the token
    /// looked fine. Still single-flight against concurrent callers, but keyed
    /// on the access token itself rather than `needs_refresh()` (which would
    /// trivially say "no" right after any refresh, including someone else's
    /// concurrent one, defeating the "unconditional" part): if the token
    /// already changed while we waited for the lock, someone else's refresh
    /// landed first, so skip our own OAuth call and use theirs.
    async fn force_refresh(&self) -> Result<(), ProxyError> {
        let token_before = self.state.read().await.access_token.clone();
        let _guard = self.refresh_lock.lock().await;
        if self.state.read().await.access_token != token_before {
            return Ok(());
        }
        self.perform_refresh().await
    }

    /// The actual OAuth refresh call. Callers must hold `refresh_lock`.
    async fn perform_refresh(&self) -> Result<(), ProxyError> {
        let refresh_token = self.state.read().await.refresh_token.clone();
        let endpoint = format!("{}/oauth/token", self.issuer.trim_end_matches('/'));
        let req = RefreshRequest {
            client_id: &self.client_id,
            grant_type: "refresh_token",
            refresh_token: &refresh_token,
        };

        tracing::info!("refreshing Codex access token");
        let resp = self
            .http
            .post(&endpoint)
            .header("Content-Type", "application/json")
            .json(&req)
            // Unlike the streaming forward, refresh is a short unary call —
            // bound it so a stuck OAuth endpoint can't hang every request.
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| ProxyError::UpstreamAuth(format!("refresh request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ProxyError::UpstreamAuth(format!(
                "token refresh rejected ({status}): {body}"
            )));
        }

        let parsed: RefreshResponse = resp
            .json()
            .await
            .map_err(|e| ProxyError::UpstreamAuth(format!("malformed refresh response: {e}")))?;

        self.apply_refresh(parsed).await?;
        Ok(())
    }

    async fn apply_refresh(&self, resp: RefreshResponse) -> Result<(), ProxyError> {
        let mut state = self.state.write().await;
        if let Some(access) = resp.access_token {
            state.expires_at = jwt::expiration(&access).ok().flatten();
            state.access_token = access;
        }
        if let Some(refresh) = resp.refresh_token {
            state.refresh_token = refresh;
        }
        if let Some(id_token) = resp.id_token {
            if let Some(acc) = jwt::account_id(&id_token) {
                state.account_id = Some(acc);
            }
            state.id_token = id_token;
        }

        // Persist back so a restart (and the real codex CLI) sees fresh tokens.
        // Reload the on-disk file and mutate only tokens/last_refresh so we
        // preserve OPENAI_API_KEY and any other fields (ours or codex's) — and
        // pick up concurrent codex CLI edits — instead of overwriting the file.
        let mut snapshot = store::load(&self.codex_home).unwrap_or_default();
        snapshot.tokens = Some(store::Tokens {
            id_token: state.id_token.clone(),
            access_token: state.access_token.clone(),
            refresh_token: state.refresh_token.clone(),
            account_id: state.account_id.clone(),
        });
        snapshot.last_refresh = Some(Utc::now());
        if let Err(e) = store::save(&self.codex_home, &snapshot) {
            // Non-fatal: tokens are still valid in memory for this process. But
            // if OpenAI rotates refresh tokens, the stale on-disk one can later
            // be rejected, forcing a surprise `codex login`. Make it actionable.
            tracing::error!(
                path = %store::auth_file(&self.codex_home).display(),
                "failed to persist refreshed auth.json ({e}); tokens valid in-memory only — \
                 on restart you may need to re-run `codex login`. Check file permissions/disk."
            );
        }
        Ok(())
    }
}
