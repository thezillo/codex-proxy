//! Upstream client: forwards request bodies to the Codex Responses API with
//! the exact headers the official client sends, distributing requests across
//! a round-robin pool of ChatGPT accounts.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::auth::AuthManager;
use crate::config::UpstreamConfig;
use crate::error::ProxyError;

/// One ChatGPT account in the round-robin pool.
struct PoolEntry {
    auth: Arc<AuthManager>,
    label: Arc<str>,
    /// Set when this account fails with 401 (post-retry)/403/429, so
    /// round-robin selection skips it until the cooldown elapses — no active
    /// health-checking, just "don't immediately retry a request we just saw
    /// fail". A plain `std::sync::Mutex` is correct here (never held across
    /// an `.await`), not `tokio::sync::Mutex`.
    cooldown_until: Mutex<Option<Instant>>,
}

impl PoolEntry {
    fn is_cooling_down(&self, now: Instant) -> bool {
        matches!(*self.cooldown_until.lock().unwrap(), Some(until) if now < until)
    }

    fn start_cooldown(&self, duration: Duration) {
        *self.cooldown_until.lock().unwrap() = Some(Instant::now() + duration);
    }
}

pub struct Upstream {
    http: reqwest::Client,
    pool: Vec<PoolEntry>,
    /// Round-robin cursor into `pool`. `Relaxed` is enough — entries only
    /// need even distribution across concurrent requests, not a strict order.
    next: AtomicUsize,
    account_cooldown: Duration,
    responses_url: String,
    originator: String,
    user_agent: String,
}

/// What `forward_responses` produced, plus which pool account served it — so
/// callers can attribute the request in the access log without `Upstream`
/// exposing anything about the pool itself.
pub struct ForwardedResponse {
    pub response: reqwest::Response,
    pub account: Arc<str>,
}

impl Upstream {
    /// `accounts` is the round-robin pool in configured order: `(token
    /// manager, access-log label)` per ChatGPT account. Always non-empty —
    /// `Config::account_pool()` always yields at least the primary account.
    pub fn new(
        cfg: &UpstreamConfig,
        http: reqwest::Client,
        accounts: Vec<(Arc<AuthManager>, String)>,
    ) -> Self {
        assert!(
            !accounts.is_empty(),
            "upstream account pool must not be empty"
        );
        let responses_url = format!(
            "{}{}",
            cfg.base_url.trim_end_matches('/'),
            cfg.responses_path
        );
        let user_agent = build_user_agent(&cfg.originator, &cfg.cli_version);
        // Only announce a "pool" when there actually is one — a single
        // configured account should look and log exactly like before pooling.
        if accounts.len() > 1 {
            tracing::info!(
                pool_size = accounts.len(),
                "upstream account pool configured"
            );
        }
        tracing::info!(%responses_url, %user_agent, "upstream configured");
        let pool = accounts
            .into_iter()
            .map(|(auth, label)| PoolEntry {
                auth,
                label: label.into(),
                cooldown_until: Mutex::new(None),
            })
            .collect();
        Self {
            http,
            pool,
            next: AtomicUsize::new(0),
            account_cooldown: Duration::from_secs(cfg.account_cooldown_secs),
            responses_url,
            originator: cfg.originator.clone(),
            user_agent,
        }
    }

    /// Pick the next pool account, round-robin among accounts that aren't
    /// currently cooling down (falls back to the plain round-robin pick if
    /// every account is cooling — trying a shaky account beats refusing the
    /// request outright). Returns the pool index too, so a caller that later
    /// sees this account fail can start its cooldown. Returns owned handles
    /// for the rest (not a borrow of `self`) so the caller can `.await` on
    /// them freely.
    fn next_account(&self) -> (usize, Arc<AuthManager>, Arc<str>) {
        let now = Instant::now();
        let len = self.pool.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % len;
        let idx = (0..len)
            .map(|offset| (start + offset) % len)
            .find(|&i| !self.pool[i].is_cooling_down(now))
            .unwrap_or(start);
        let entry = &self.pool[idx];
        (idx, entry.auth.clone(), entry.label.clone())
    }

    /// Forward a raw JSON body to `/responses`, returning the response
    /// (streamed — we do not buffer the body) together with which pool
    /// account ultimately served it.
    ///
    /// Bounded layers of resilience, so a single client request can never
    /// storm the upstream indefinitely:
    /// - **Reactive retry**: a 401 from the picked account triggers one forced
    ///   token refresh plus one retry on that *same* account (mirrors the real
    ///   Codex CLI's own refresh-and-retry-once behavior on 401) — covers
    ///   clock skew or early revocation our proactive expiry check missed.
    /// - **Failover**: if that account still fails — 401 even after the
    ///   retry, 403 (banned, which no refresh fixes), or 429 (rate-limited,
    ///   the actual reason a multi-account pool exists) — it starts a
    ///   cooldown (see `PoolEntry::start_cooldown`) and the request moves to
    ///   the next pool account, at most once per distinct account this sweep.
    ///   If every account fails, the *last* one's response is returned as-is
    ///   — the client still sees a real upstream error, not a synthetic one.
    ///
    /// `client_headers` are the caller's own incoming request headers —
    /// relayed selectively (see `SESSION_IDENTITY_HEADERS` and
    /// `STICKY_ROUTING_REQUEST_HEADERS`) so the real Codex CLI's turn/session
    /// continuity survives being routed through this proxy's account pool.
    /// Never a source for `Authorization` or `ChatGPT-Account-ID`: those two
    /// are always the pool account's own, regardless of anything the client
    /// sent.
    pub async fn forward_responses(
        &self,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
    ) -> Result<ForwardedResponse, ProxyError> {
        let pool_len = self.pool.len();
        let mut tried = vec![false; pool_len];
        let mut last_response = None;
        let mut last_err = None;

        for _ in 0..pool_len {
            let (idx, auth_mgr, account) = self.next_account();
            if tried[idx] {
                // Cooldown-skipping wrapped back onto an account already
                // tried this sweep (possible under concurrent traffic
                // interleaving the shared round-robin cursor) — every
                // distinct account has had its shot.
                break;
            }
            tried[idx] = true;

            match self
                .try_account(&auth_mgr, &account, body.clone(), client_headers)
                .await
            {
                Ok(response) => {
                    if is_account_failure(response.status()) {
                        self.pool[idx].start_cooldown(self.account_cooldown);
                        tracing::warn!(
                            %account,
                            status = %response.status(),
                            "account failed, trying next pool account"
                        );
                        last_response = Some(ForwardedResponse { response, account });
                        continue;
                    }
                    return Ok(ForwardedResponse { response, account });
                }
                Err(e) => {
                    // Also cools the account down: without this, a transport
                    // error (unlike an HTTP failure status) leaves the
                    // account "not cooling", so under concurrent load a
                    // colliding round-robin cursor can re-pick it, hit the
                    // `tried[idx]` dedup break, and stop the sweep early —
                    // skipping healthy accounts that were never actually tried.
                    self.pool[idx].start_cooldown(self.account_cooldown);
                    last_err = Some(e);
                }
            }
        }

        // Every account failed (or errored) this sweep: prefer a real
        // upstream response over a synthetic error, since the client can
        // then see (and act on) the actual status/body.
        match last_response {
            Some(fwd) => Ok(fwd),
            None => Err(last_err
                .unwrap_or_else(|| ProxyError::Upstream("upstream account pool is empty".into()))),
        }
    }

    /// Try one pool account: send once, and if the upstream says 401, force a
    /// token refresh and retry once more on this same account before
    /// reporting it as failed.
    async fn try_account(
        &self,
        auth_mgr: &AuthManager,
        account: &str,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
    ) -> Result<reqwest::Response, ProxyError> {
        let auth = auth_mgr.headers().await?;
        let response = self
            .send_once(&auth, body.clone(), client_headers, account)
            .await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        tracing::info!(%account, "401 from upstream; forcing token refresh and retrying once");
        let refreshed = auth_mgr.force_refresh_headers().await?;
        self.send_once(&refreshed, body, client_headers, account)
            .await
    }

    async fn send_once(
        &self,
        auth: &crate::auth::AuthHeaders,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
        account: &str,
    ) -> Result<reqwest::Response, ProxyError> {
        let mut req = self
            .http
            .post(&self.responses_url)
            .header("Authorization", format!("Bearer {}", auth.bearer))
            .header("originator", &self.originator)
            .header("User-Agent", &self.user_agent)
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .body(body);

        if let Some(account_id) = &auth.account_id {
            req = req.header("ChatGPT-Account-ID", account_id.clone());
        }

        for name in SESSION_IDENTITY_HEADERS {
            if let Some(value) = client_headers.get(*name) {
                req = req.header(*name, value.clone());
            }
        }
        // `x-codex-turn-state` is a sticky-routing token tied to whichever
        // account issued it (see the const's docs) — only safe to replay
        // upstream when the pool has exactly one account, where "the account
        // handling this request" and "the account that issued the token" are
        // guaranteed to be the same.
        if self.pool.len() == 1 {
            for name in STICKY_ROUTING_REQUEST_HEADERS {
                if let Some(value) = client_headers.get(*name) {
                    req = req.header(*name, value.clone());
                }
            }
        }

        req.send().await.map_err(|e| {
            // No CompletionLog reaches emit() on this path — attribute the
            // failure here or a transport error becomes invisible to
            // per-account rate-limit debugging.
            tracing::warn!(%account, error = %e, "forward to responses failed");
            ProxyError::Upstream(format!("forward to responses failed: {e}"))
        })
    }
}

/// Upstream statuses that mean "this account can't serve the request right
/// now" and should trigger failover to the next pool account, rather than
/// being relayed to the client as-is: 401 (persisting even after our own
/// refresh-and-retry on the same account), 403 (banned/rejected — no refresh
/// fixes that), and 429 (rate-limited — the whole reason a multi-account pool
/// exists is to have headroom when one account is throttled). All three are
/// "request never processed, no tokens spent" statuses, so trying the same
/// body against a different account is safe.
pub(crate) fn is_account_failure(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::UNAUTHORIZED
            | reqwest::StatusCode::FORBIDDEN
            | reqwest::StatusCode::TOO_MANY_REQUESTS
    )
}

/// Client/session-identifying headers the real Codex CLI attaches to
/// Responses-API requests (`codex-rs/core/src/client.rs`), generated by its
/// core client regardless of which `model_provider` it's pointed at — so they
/// arrive here unchanged when a real client is routed through this proxy.
/// Verified by running an actual `codex exec` against a local `model_provider`
/// override and inspecting the headers it sent (not just reading the source):
/// `session-id`/`thread-id` are plain headers (no `x-` prefix, unlike the
/// `X_CODEX_*` constant names in codex-rs might suggest), and
/// `x-codex-turn-metadata` is a client-generated JSON blob (thread/session/
/// window/turn ids, sandbox, timestamp) — not a server-issued token, so
/// (unlike `x-codex-turn-state` below) it's safe to relay to any pool account.
///
/// Deliberately an explicit allowlist, not a `x-codex-*`/`x-openai-*`
/// wildcard: only forward names verified against the real client's behavior,
/// so an arbitrary caller can't smuggle unvetted headers — e.g.
/// `x-openai-internal-codex-residency`, an enterprise residency-enforcement
/// header that has no business being set by an untrusted client — into a
/// pooled-account upstream request.
const SESSION_IDENTITY_HEADERS: &[&str] = &[
    "session-id",
    "thread-id",
    "x-client-request-id",
    "x-codex-turn-metadata",
    "x-codex-parent-thread-id",
    "x-codex-window-id",
    "x-codex-beta-features",
    "x-openai-subagent",
    "x-openai-memgen-request",
];

/// Sticky-routing tokens the real Codex CLI captures from a previous
/// response and replays on the next request in the same turn, so the
/// backend can route it to the same replica/session
/// (`codex-rs/core/src/client.rs`'s `X_CODEX_TURN_STATE_HEADER` doc comment).
/// Relaying a token issued by one pool account to a *different* account would
/// be meaningless at best and rejected at worst — see the pool-size check at
/// this const's only call site.
const STICKY_ROUTING_REQUEST_HEADERS: &[&str] = &["x-codex-turn-state"];

/// The full session-continuity header set relayed in the *response* ->
/// client direction, where there's no cross-account risk (the client just
/// holds onto whatever token it's given for its next request). Used by
/// `server.rs`'s `/v1/responses` handler. Hand-maintained union of
/// `SESSION_IDENTITY_HEADERS` + `STICKY_ROUTING_REQUEST_HEADERS` above — keep
/// in sync if either changes, there's no shared source of truth.
pub(crate) const CODEX_SESSION_RESPONSE_HEADERS: &[&str] = &[
    "session-id",
    "thread-id",
    "x-client-request-id",
    "x-codex-turn-metadata",
    "x-codex-parent-thread-id",
    "x-codex-window-id",
    "x-codex-beta-features",
    "x-openai-subagent",
    "x-openai-memgen-request",
    "x-codex-turn-state",
];

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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::{HeaderMap, HeaderValue};
    use axum::response::Response;
    use axum::routing::post;
    use axum::Router;
    use tokio::sync::mpsc;

    use super::*;
    use crate::config::Config;
    use crate::test_support::write_test_auth_json;

    /// Fake upstream capturing the `ChatGPT-Account-ID` of every request it
    /// receives, in order — enough to assert a round-robin sequence. Distinct
    /// from `server.rs`'s `FakeUpstream` (which asserts on a single request's
    /// full headers/body): this one only cares about a *sequence* of account
    /// ids across many requests, so a shared abstraction isn't worth it.
    struct FakeAccountLog {
        base_url: String,
        rx: mpsc::Receiver<Option<String>>,
    }

    impl FakeAccountLog {
        async fn recv(&mut self) -> Option<String> {
            self.rx.recv().await.expect("fake upstream request")
        }
    }

    async fn start_fake_account_log(capacity: usize) -> FakeAccountLog {
        let (tx, rx) = mpsc::channel(capacity);
        let app = Router::new()
            .route("/codex/responses", post(capture_account_id))
            .with_state(tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FakeAccountLog {
            base_url: format!("http://{addr}"),
            rx,
        }
    }

    async fn capture_account_id(
        State(tx): State<mpsc::Sender<Option<String>>>,
        headers: HeaderMap,
    ) -> Response {
        let account_id = headers
            .get("chatgpt-account-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        tx.send(account_id).await.unwrap();
        Response::builder()
            .status(200)
            .body(Body::from(r#"{"ok":true}"#))
            .unwrap()
    }

    /// Build an `Upstream` with `n` fake accounts (distinct `chatgpt_account_id`
    /// claims, labelled "account-0".."account-{n-1}") pointed at `base_url`.
    async fn test_pool(base_url: &str, n: usize) -> Upstream {
        let mut cfg = Config::default();
        cfg.upstream.base_url = base_url.to_string();
        // Same fake server also serves /oauth/token (see ScriptedState),
        // so a forced refresh in the retry-on-401 path resolves locally
        // instead of reaching the real OpenAI OAuth endpoint.
        cfg.upstream.issuer = base_url.to_string();
        let http = reqwest::Client::new();

        let mut accounts = Vec::with_capacity(n);
        for i in 0..n {
            let codex_home = write_test_auth_json(&format!("acct-{i}"));
            let auth =
                AuthManager::load(&cfg.upstream, codex_home, http.clone()).expect("load test auth");
            accounts.push((auth, format!("account-{i}")));
        }
        Upstream::new(&cfg.upstream, http, accounts)
    }

    #[tokio::test]
    async fn single_account_forward_uses_that_account() {
        let mut fake = start_fake_account_log(1).await;
        let upstream = test_pool(&fake.base_url, 1).await;

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(&*fwd.account, "account-0");
        assert_eq!(fake.recv().await.as_deref(), Some("acct-0"));
    }

    #[tokio::test]
    async fn round_robin_cycles_through_pool_in_order() {
        let mut fake = start_fake_account_log(6).await;
        let upstream = test_pool(&fake.base_url, 3).await;

        let mut served = Vec::new();
        for _ in 0..6 {
            let fwd = upstream
                .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
                .await
                .unwrap();
            served.push(fwd.account.to_string());
        }
        assert_eq!(
            served,
            vec![
                "account-0",
                "account-1",
                "account-2",
                "account-0",
                "account-1",
                "account-2"
            ]
        );

        let mut seen_account_ids = Vec::new();
        for _ in 0..6 {
            seen_account_ids.push(fake.recv().await);
        }
        assert_eq!(
            seen_account_ids,
            vec![
                Some("acct-0".to_string()),
                Some("acct-1".to_string()),
                Some("acct-2".to_string()),
                Some("acct-0".to_string()),
                Some("acct-1".to_string()),
                Some("acct-2".to_string()),
            ]
        );
    }

    struct FakeHeaderEcho {
        base_url: String,
        rx: mpsc::Receiver<CapturedHeaders>,
    }

    struct CapturedHeaders {
        turn_state: Option<String>,
        residency: Option<String>,
        client_request_id: Option<String>,
    }

    impl FakeHeaderEcho {
        async fn recv(&mut self) -> CapturedHeaders {
            self.rx.recv().await.expect("fake upstream request")
        }
    }

    async fn start_fake_header_echo() -> FakeHeaderEcho {
        let (tx, rx) = mpsc::channel(1);
        let app = Router::new()
            .route("/codex/responses", post(echo_headers))
            .with_state(tx);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        FakeHeaderEcho {
            base_url: format!("http://{addr}"),
            rx,
        }
    }

    /// Captures the two headers under test, then replies with its own
    /// `x-codex-turn-state` — mirroring how the real Codex backend returns a
    /// fresh sticky-routing token for the client to replay next turn.
    async fn echo_headers(
        State(tx): State<mpsc::Sender<CapturedHeaders>>,
        headers: HeaderMap,
    ) -> Response {
        let captured = CapturedHeaders {
            turn_state: headers
                .get("x-codex-turn-state")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            residency: headers
                .get("x-openai-internal-codex-residency")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            client_request_id: headers
                .get("x-client-request-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
        };
        tx.send(captured).await.unwrap();
        Response::builder()
            .status(200)
            .header("x-codex-turn-state", "server-issued-token")
            .body(Body::from(r#"{"ok":true}"#))
            .unwrap()
    }

    #[tokio::test]
    async fn allowlisted_session_headers_relayed_others_dropped() {
        let mut fake = start_fake_header_echo().await;
        let upstream = test_pool(&fake.base_url, 1).await;

        let mut client_headers = HeaderMap::new();
        client_headers.insert(
            "x-codex-turn-state",
            HeaderValue::from_static("client-turn-token"),
        );
        // Not on the allowlist (enterprise residency-enforcement) — must NOT
        // reach the upstream even though it's client-supplied and looks like
        // a legitimate codex/openai header.
        client_headers.insert(
            "x-openai-internal-codex-residency",
            HeaderValue::from_static("us"),
        );

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &client_headers)
            .await
            .unwrap();

        // The upstream's own turn-state reaches the caller, so server.rs can
        // relay it back down to the real client for the next turn.
        assert_eq!(
            fwd.response
                .headers()
                .get("x-codex-turn-state")
                .and_then(|v| v.to_str().ok()),
            Some("server-issued-token")
        );

        let captured = fake.recv().await;
        assert_eq!(captured.turn_state.as_deref(), Some("client-turn-token"));
        assert_eq!(captured.residency, None);
    }

    #[tokio::test]
    async fn sticky_routing_header_dropped_when_pool_has_multiple_accounts() {
        // With >1 pool account, a client's `x-codex-turn-state` was issued by
        // whichever specific account served the *previous* turn — relaying it
        // to a different account (round-robin's whole point) would be
        // meaningless at best. Identity headers carry no such per-account
        // meaning, so they're still relayed regardless of pool size.
        let mut fake = start_fake_header_echo().await;
        let upstream = test_pool(&fake.base_url, 2).await;

        let mut client_headers = HeaderMap::new();
        client_headers.insert(
            "x-codex-turn-state",
            HeaderValue::from_static("client-turn-token"),
        );
        client_headers.insert(
            "x-client-request-id",
            HeaderValue::from_static("thread-abc"),
        );

        upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &client_headers)
            .await
            .unwrap();

        let captured = fake.recv().await;
        assert_eq!(captured.turn_state, None);
        assert_eq!(captured.client_request_id.as_deref(), Some("thread-abc"));
    }

    /// Fake upstream that scripts a fixed sequence of statuses per account
    /// (keyed by the `ChatGPT-Account-ID` header), sticking on the last
    /// scripted status once its sequence is exhausted, and also serves
    /// `/oauth/token` unconditionally-successfully so `AuthManager`'s forced
    /// refresh in the retry-on-401 path resolves locally instead of reaching
    /// the real OpenAI OAuth endpoint.
    #[derive(Clone)]
    struct ScriptedState {
        scripts: Arc<
            tokio::sync::Mutex<std::collections::HashMap<String, std::collections::VecDeque<u16>>>,
        >,
        tx: mpsc::Sender<(String, u16)>,
    }

    struct ScriptedUpstream {
        base_url: String,
        rx: mpsc::Receiver<(String, u16)>,
    }

    impl ScriptedUpstream {
        async fn recv(&mut self) -> (String, u16) {
            self.rx.recv().await.expect("fake upstream request")
        }
    }

    async fn start_scripted_upstream(
        scripts: std::collections::HashMap<&str, Vec<u16>>,
    ) -> ScriptedUpstream {
        let (tx, rx) = mpsc::channel(16);
        let scripts = scripts
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into_iter().collect()))
            .collect();
        let state = ScriptedState {
            scripts: Arc::new(tokio::sync::Mutex::new(scripts)),
            tx,
        };
        let app = Router::new()
            .route("/codex/responses", post(scripted_responses))
            .route("/oauth/token", post(scripted_oauth))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        ScriptedUpstream {
            base_url: format!("http://{addr}"),
            rx,
        }
    }

    async fn scripted_responses(
        State(state): State<ScriptedState>,
        headers: HeaderMap,
    ) -> Response {
        let account_id = headers
            .get("chatgpt-account-id")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let status = {
            let mut scripts = state.scripts.lock().await;
            let queue = scripts.entry(account_id.clone()).or_default();
            if queue.len() > 1 {
                queue.pop_front().unwrap()
            } else {
                *queue.front().unwrap_or(&200)
            }
        };
        state.tx.send((account_id, status)).await.unwrap();
        Response::builder()
            .status(status)
            .body(Body::from(r#"{"ok":true}"#))
            .unwrap()
    }

    async fn scripted_oauth() -> Response {
        Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(Body::from(
                r#"{"access_token":"refreshed-access-token","refresh_token":"refreshed-refresh-token","id_token":"refreshed-id-token"}"#,
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn retries_once_on_401_against_the_same_account() {
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([(
            "acct-0",
            vec![401, 200],
        )]))
        .await;
        let upstream = test_pool(&fake.base_url, 1).await;

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);

        assert_eq!(fake.recv().await, ("acct-0".to_string(), 401));
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 200));
    }

    #[tokio::test]
    async fn fails_over_to_next_account_on_403() {
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([
            ("acct-0", vec![403]),
            ("acct-1", vec![200]),
        ]))
        .await;
        let upstream = test_pool(&fake.base_url, 2).await;

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);
        assert_eq!(&*fwd.account, "account-1");

        assert_eq!(fake.recv().await, ("acct-0".to_string(), 403));
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));
    }

    #[tokio::test]
    async fn fails_over_to_next_account_on_429() {
        // 429 is the actual reason a multi-account pool exists — one
        // account's rate limit shouldn't sink a request when another
        // account has headroom.
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([
            ("acct-0", vec![429]),
            ("acct-1", vec![200]),
        ]))
        .await;
        let upstream = test_pool(&fake.base_url, 2).await;

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);
        assert_eq!(&*fwd.account, "account-1");

        assert_eq!(fake.recv().await, ("acct-0".to_string(), 429));
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));
    }

    #[tokio::test]
    async fn returns_last_accounts_response_when_every_account_fails() {
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([
            ("acct-0", vec![403]),
            ("acct-1", vec![403]),
        ]))
        .await;
        let upstream = test_pool(&fake.base_url, 2).await;

        // Every account fails, but this must still be Ok (the client sees a
        // real upstream error, not a synthetic one) with the LAST account's
        // response, not an Err.
        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::FORBIDDEN);
        assert_eq!(&*fwd.account, "account-1");

        assert_eq!(fake.recv().await, ("acct-0".to_string(), 403));
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 403));
    }

    #[tokio::test]
    async fn cooldown_skips_a_recently_failed_account_on_the_next_request() {
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([
            ("acct-0", vec![403]),
            ("acct-1", vec![200]),
        ]))
        .await;
        let upstream = test_pool(&fake.base_url, 2).await;

        // First request: acct-0 fails over (403, starts its cooldown),
        // acct-1 serves it. Same behavior as `fails_over_to_next_account_on_403`.
        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 403));
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));

        // Second request, immediately after: acct-0 is still cooling down, so
        // round-robin should skip straight to acct-1 without ever hitting
        // acct-0's endpoint again — exactly one more call, to acct-1.
        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new())
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);
        assert_eq!(&*fwd.account, "account-1");
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));
        assert!(
            fake.rx.try_recv().is_err(),
            "acct-0 should not have been retried while cooling down"
        );
    }
}
