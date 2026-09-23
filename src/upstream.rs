//! Upstream client: forwards request bodies to the Codex Responses API with
//! the exact headers the official client sends, distributing requests across
//! a round-robin pool of ChatGPT accounts.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::auth::AuthManager;
use crate::config::UpstreamConfig;
use crate::error::ProxyError;
use crate::replay;

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
    /// Set when this account's 429 said `usage_limit_reached`: a hard quota
    /// with a known reset time, not a transient throttle. Kept apart from
    /// `cooldown_until` because the two signals want opposite lifetimes —
    /// a cooldown must stay short so a live account isn't lost to one
    /// hiccup, while a quota hold must last hours or days or every request
    /// wastes a round-trip on an account that can't serve it. Cleared early
    /// by the quota poller (`poll_quota_once`) when the usage report says
    /// the quota is back.
    quota_exhausted_until: Mutex<Option<Instant>>,
    /// The account's subscription quota as last reported upstream (the
    /// `x-codex-*` headers of a `/responses` call, or a usage poll), with
    /// when it was observed. Observability only: selection never reads it.
    quota: Mutex<Option<(Instant, QuotaSnapshot)>>,
}

/// One rate-limit window of an account's subscription quota.
#[derive(Debug, Clone, PartialEq)]
pub struct QuotaWindow {
    /// The window's length (`5h`, `7d`), or its slot name (`primary`,
    /// `secondary`) when upstream didn't say. Length, not slot, because the
    /// slots mean different things per plan: a pro account reports its
    /// weekly window as `primary` with no `secondary` at all, a plus account
    /// reports 5h as `primary` and weekly as `secondary`.
    pub label: String,
    pub used_percent: f64,
    /// Unix seconds.
    pub reset_at: Option<u64>,
}

/// An account's subscription quota, as the real Codex CLI's `/status`
/// shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct QuotaSnapshot {
    pub windows: Vec<QuotaWindow>,
    pub plan_type: Option<String>,
    pub credits_balance: Option<f64>,
    /// Unix seconds.
    pub observed_at: u64,
}

/// A pool account's state for the metrics scrape.
pub struct AccountStatus {
    pub account: Arc<str>,
    /// `ready`, `cooling` or `quota_held`.
    pub state: &'static str,
    pub quota: Option<QuotaSnapshot>,
}

/// Every value `AccountStatus::state` can take, so the scrape can export
/// a 0 for the states an account is NOT in.
pub const ACCOUNT_STATES: &[&str] = &["ready", "cooling", "quota_held"];

/// How usable a pool account is right now, worst last — so selection is one
/// `min_by_key` and "prefer a 30s-cooling account over one a week from its
/// quota reset" is the ordering, not a second search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Availability {
    Ready,
    Cooling,
    QuotaHeld,
}

impl Availability {
    fn as_str(self) -> &'static str {
        match self {
            Availability::Ready => "ready",
            Availability::Cooling => "cooling",
            Availability::QuotaHeld => "quota_held",
        }
    }
}

/// Time left on a timer slot, `None` once it has lapsed (or was never set).
fn remaining(slot: &Mutex<Option<Instant>>, now: Instant) -> Option<Duration> {
    match *slot.lock().unwrap() {
        Some(until) if now < until => Some(until - now),
        _ => None,
    }
}

impl PoolEntry {
    fn availability(&self, now: Instant) -> Availability {
        if self.is_quota_exhausted(now) {
            Availability::QuotaHeld
        } else if remaining(&self.cooldown_until, now).is_some() {
            Availability::Cooling
        } else {
            Availability::Ready
        }
    }

    fn start_cooldown(&self, duration: Duration) {
        *self.cooldown_until.lock().unwrap() = Some(Instant::now() + duration);
    }

    fn is_quota_exhausted(&self, now: Instant) -> bool {
        self.quota_hold_remaining(now).is_some()
    }

    /// Remaining hold, if any — for log lines and the pool-unavailable error.
    fn quota_hold_remaining(&self, now: Instant) -> Option<Duration> {
        remaining(&self.quota_exhausted_until, now)
    }

    /// Mark quota-exhausted for `hold`. Only ever extends an existing hold,
    /// never shortens it: a poll result carrying a nearer reset than the
    /// 429 did must not make us re-try the account sooner than either
    /// source said it would be usable.
    fn mark_quota_exhausted(&self, hold: Duration) {
        let until = Instant::now() + hold;
        let mut slot = self.quota_exhausted_until.lock().unwrap();
        match *slot {
            Some(existing) if existing >= until => {}
            _ => *slot = Some(until),
        }
    }

    fn clear_quota_exhausted(&self) {
        *self.quota_exhausted_until.lock().unwrap() = None;
    }

    fn observe_quota(&self, snapshot: QuotaSnapshot) {
        *self.quota.lock().unwrap() = Some((Instant::now(), snapshot));
    }

    /// No quota report within `max_age` (or ever) — the poller's cue to
    /// ask. An account serving traffic stays fresh from response headers
    /// alone and is never polled while healthy.
    fn quota_stale(&self, now: Instant, max_age: Duration) -> bool {
        match *self.quota.lock().unwrap() {
            Some((seen, _)) => now.saturating_duration_since(seen) >= max_age,
            None => true,
        }
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
    compact_url: String,
    search_url: String,
    usage_url: String,
    /// `None` = polling disabled (`quota_check_interval_secs = 0`).
    quota_poll_interval: Option<Duration>,
    originator: String,
    user_agent: String,
}

/// Hold applied to a quota-exhausted account when the 429 carried no
/// parseable reset time and polling is disabled — long enough not to
/// hammer the account, short enough that a weekly reset is never missed by
/// more than this.
const QUOTA_HOLD_DEFAULT: Duration = Duration::from_secs(600);
/// Bounds on a reset time parsed from upstream: below the floor a hold is
/// pointless churn, above the ceiling it's almost certainly a garbage
/// timestamp (the longest real Codex window is a week).
const QUOTA_HOLD_MIN: Duration = Duration::from_secs(60);
const QUOTA_HOLD_MAX: Duration = Duration::from_secs(8 * 24 * 3600);
/// Most bytes of an error body we'll classify (a 429 for a quota, a 4xx for
/// an undecryptable replay). The real errors are a few hundred bytes;
/// anything bigger is neither and isn't classified (and, when
/// `Content-Length` declares it up front, isn't buffered either).
const ERROR_BODY_MAX_BYTES: usize = 64 * 1024;
/// Whole-request timeout for one usage poll. Background work; nothing
/// waits on it except the next tick.
const USAGE_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Which upstream endpoint a pool request goes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    /// `responses_path`: the streaming Responses API.
    Responses,
    /// `compact_path`: stateless history compaction (JSON in, JSON out).
    Compact,
    /// `search_path`: Codex's standalone web search (JSON in, JSON out).
    Search,
}

impl Endpoint {
    pub fn as_str(self) -> &'static str {
        match self {
            Endpoint::Responses => "responses",
            Endpoint::Compact => "responses/compact",
            Endpoint::Search => "alpha/search",
        }
    }
}

/// What `forward_responses` produced, plus which pool account served it — so
/// callers can attribute the request in the access log without `Upstream`
/// exposing anything about the pool itself.
pub struct ForwardedResponse {
    pub response: reqwest::Response,
    pub account: Arc<str>,
    /// The pool's own reading of a failing response, when it knows more
    /// than the status alone says: a 429 the pool classified as a quota
    /// hold reports `QuotaExhausted`, so the request that *discovered* the
    /// exhaustion groups with the ones diverted after it rather than with
    /// transient throttles. `None` = derive from the status.
    pub reason: Option<FailureReason>,
}

/// Normalized cause of a pool failure, for the failover access-log line (and
/// so operators can filter by `reason` rather than parsing free-form error
/// text). Deliberately a small closed set: an unbounded label would be
/// useless to group by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    /// 429 — the account is throttled.
    RateLimit,
    /// 401/403. A 401 only reaches here after `try_account`'s forced refresh
    /// and retry already failed on this account, so it means a genuinely
    /// dead session, not an expired access token.
    Auth,
    /// Client- or gateway-side timeout (408/504, or a reqwest timeout).
    Timeout,
    /// 503 — upstream up but refusing load.
    Capacity,
    /// Any other 5xx: Codex itself erroring.
    Upstream5xx,
    /// Any other 4xx: the request is bad for every account alike.
    BadRequest,
    /// Never reached the upstream at all: connection refused, DNS, TLS. A
    /// connection that was established and then broke mid-flight sets
    /// neither reqwest predicate and lands in `Unknown` — deliberately, see
    /// `from_transport`.
    Transport,
    /// A `usage_limit_reached` 429 — the account's quota, not a throttle.
    /// Either this request is the one that discovered it (`status=429`) or
    /// the pool was skipped because every account is under such a hold and
    /// a fallback chain exists to take the request (`status=0`; see
    /// `Upstream::unavailable`).
    QuotaExhausted,
    /// The pool was never tried: every account is inside its short
    /// post-failure cooldown and a fallback chain exists.
    CoolingDown,
    Unknown,
}

impl FailureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            FailureReason::RateLimit => "rate_limit",
            FailureReason::Auth => "auth",
            FailureReason::Timeout => "timeout",
            FailureReason::Capacity => "capacity",
            FailureReason::Upstream5xx => "upstream_5xx",
            FailureReason::BadRequest => "bad_request",
            FailureReason::Transport => "transport",
            FailureReason::QuotaExhausted => "quota_exhausted",
            FailureReason::CoolingDown => "cooling_down",
            FailureReason::Unknown => "unknown",
        }
    }

    /// Classify a status the pool actually returned.
    pub fn from_status(status: reqwest::StatusCode) -> Self {
        match status.as_u16() {
            429 => FailureReason::RateLimit,
            401 | 403 => FailureReason::Auth,
            408 | 504 => FailureReason::Timeout,
            503 => FailureReason::Capacity,
            _ if status.is_server_error() => FailureReason::Upstream5xx,
            _ if status.is_client_error() => FailureReason::BadRequest,
            _ => FailureReason::Unknown,
        }
    }

    /// Classify a request that never produced a response. Takes the two
    /// `reqwest::Error` predicates rather than the error itself: `reqwest`
    /// keeps its error constructors private, so a classifier taking
    /// `&reqwest::Error` could only be tested by standing up a fake upstream
    /// that hangs or refuses — this way the mapping is a plain table test and
    /// the only untested part is the one-line call at the reqwest boundary.
    pub fn from_transport(is_timeout: bool, is_connect: bool) -> Self {
        match (is_timeout, is_connect) {
            (true, _) => FailureReason::Timeout,
            (_, true) => FailureReason::Transport,
            _ => FailureReason::Unknown,
        }
    }
}

/// A pool attempt that produced no usable response, carrying enough context
/// for the failover log line: which account was last tried and why it failed.
/// Converts into `ProxyError` for the caller that just wants to fail.
#[derive(Debug)]
pub struct PoolFailure {
    pub reason: FailureReason,
    /// Last account tried — or, when the pool was skipped (see
    /// `Upstream::unavailable`), the account that best explains why — or
    /// `None` when the pool was empty.
    pub account: Option<Arc<str>>,
    pub error: ProxyError,
}

impl PoolFailure {
    fn new(reason: FailureReason, account: Option<Arc<str>>, error: ProxyError) -> Self {
        Self {
            reason,
            account,
            error,
        }
    }
}

impl From<PoolFailure> for ProxyError {
    fn from(f: PoolFailure) -> Self {
        f.error
    }
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
        let base_url = cfg.base_url.trim_end_matches('/');
        let responses_url = format!("{base_url}{}", cfg.responses_path);
        let usage_url = format!("{base_url}{}", cfg.usage_path);
        let compact_url = format!("{base_url}{}", cfg.compact_path);
        let search_url = format!("{base_url}{}", cfg.search_path);
        let quota_poll_interval = match cfg.quota_check_interval_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        };
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
                quota_exhausted_until: Mutex::new(None),
                quota: Mutex::new(None),
            })
            .collect();
        Self {
            http,
            pool,
            next: AtomicUsize::new(0),
            account_cooldown: Duration::from_secs(cfg.account_cooldown_secs),
            responses_url,
            compact_url,
            search_url,
            usage_url,
            quota_poll_interval,
            originator: cfg.originator.clone(),
            user_agent,
        }
    }

    /// Where a request starts looking in the pool. A request with a
    /// conversation key (see `crate::affinity`) always starts at the same
    /// account — its conversation's "home" — so every turn lands on one
    /// account while it's healthy, and that account's prompt cache keeps
    /// serving the growing conversation prefix. Keyless requests round-robin
    /// from a shared cursor. A single-account pool has nothing to choose.
    fn start_index(&self, affinity: Option<u64>) -> usize {
        let len = self.pool.len();
        if len == 1 {
            return 0;
        }
        match affinity {
            Some(hash) => (hash % len as u64) as usize,
            None => self.next.fetch_add(1, Ordering::Relaxed) % len,
        }
    }

    /// Pick the next account to try, scanning the pool in order from
    /// `start` and skipping any already `tried` this sweep: the most usable
    /// one by `Availability` (ready, else merely cooling, else quota-held —
    /// when everything is unavailable, trying a shaky account beats refusing
    /// the request outright; a caller with somewhere better to send it
    /// checks `unavailable` first). `min_by_key` keeps the first best in
    /// scan order, so a healthy pool rotates evenly under round-robin and a
    /// session stays on its home account under affinity. `None` once every
    /// account has been tried.
    ///
    /// Returns the pool index too, so a caller that later sees this account
    /// fail can start its cooldown. Returns owned handles for the rest (not
    /// a borrow of `self`) so the caller can `.await` on them freely.
    fn next_account(
        &self,
        start: usize,
        tried: &[bool],
    ) -> Option<(usize, Arc<AuthManager>, Arc<str>)> {
        let now = Instant::now();
        let len = self.pool.len();
        let idx = (0..len)
            .map(|offset| (start + offset) % len)
            .filter(|&i| !tried[i])
            .min_by_key(|&i| self.pool[i].availability(now))?;
        let entry = &self.pool[idx];
        Some((idx, entry.auth.clone(), entry.label.clone()))
    }

    /// `Some` when no account can serve a request right now — every one is
    /// either quota-exhausted or inside its post-failure cooldown — so a
    /// caller with a fallback chain can skip the pool without paying for a
    /// round-trip it just watched fail. `None` means at least one account is
    /// worth trying (it may still fail; this is a selection hint, not a
    /// health guarantee).
    ///
    /// The reported reason and account are the worst-ranked entry's: a pool
    /// where one account is a week from its reset and another is 30 seconds
    /// into a cooldown is, for an operator reading the failover line, a
    /// quota problem.
    pub fn unavailable(&self) -> Option<PoolFailure> {
        let now = Instant::now();
        // Common case first, and it short-circuits on the first ready entry.
        if self
            .pool
            .iter()
            .any(|entry| entry.availability(now) == Availability::Ready)
        {
            return None;
        }
        let (entry, worst) = self
            .pool
            .iter()
            .map(|entry| (entry, entry.availability(now)))
            .max_by_key(|(_, availability)| *availability)?;
        Some(match worst {
            Availability::Ready => unreachable!("filtered above"),
            Availability::QuotaHeld => PoolFailure::new(
                FailureReason::QuotaExhausted,
                Some(entry.label.clone()),
                ProxyError::Upstream(format!(
                    "every pool account is quota-exhausted (next reset in ~{}s)",
                    entry
                        .quota_hold_remaining(now)
                        .unwrap_or_default()
                        .as_secs()
                )),
            ),
            Availability::Cooling => PoolFailure::new(
                FailureReason::CoolingDown,
                Some(entry.label.clone()),
                ProxyError::Upstream("every pool account is cooling down".into()),
            ),
        })
    }

    /// Every pool account's current state and last quota report, in pool
    /// order — read by the metrics scrape, so it's live at scrape time
    /// (a cooldown lapsing is not an event anything else would notice).
    pub fn account_statuses(&self) -> Vec<AccountStatus> {
        let now = Instant::now();
        self.pool
            .iter()
            .map(|entry| AccountStatus {
                account: entry.label.clone(),
                state: entry.availability(now).as_str(),
                quota: entry.quota.lock().unwrap().as_ref().map(|(_, q)| q.clone()),
            })
            .collect()
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
    /// - **Replay retry**: a 4xx saying the account can't decrypt replayed
    ///   `encrypted_content` (minted by another account or provider) triggers
    ///   one retry on that *same* account without those items — see
    ///   `crate::replay`. Not a failure of the account: no cooldown, no
    ///   failover.
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
    ///
    /// `affinity` is the request's conversation-key hash, when it has one
    /// (see `crate::affinity`): it picks the account the sweep starts from.
    pub async fn forward_responses(
        &self,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
        affinity: Option<u64>,
    ) -> Result<ForwardedResponse, PoolFailure> {
        self.forward(Endpoint::Responses, body, client_headers, affinity)
            .await
    }

    /// `forward_responses` for any endpoint. The auxiliary JSON ones
    /// (compact, search) go to the one account the sweep would start with
    /// and relay its answer as is: no sweep, and no cooldown or quota hold.
    /// Their limits and permissions aren't necessarily the account's, and a
    /// search 429 or 403 marking the account would push all `/v1/responses`
    /// traffic onto the paid fallback.
    pub async fn forward(
        &self,
        endpoint: Endpoint,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
        affinity: Option<u64>,
    ) -> Result<ForwardedResponse, PoolFailure> {
        let pool_len = self.pool.len();
        let mut tried = vec![false; pool_len];
        let mut last_response = None;
        let mut last_err: Option<PoolFailure> = None;

        // Chosen once per request: the sweep below walks the pool from here,
        // each account at most once.
        let start = self.start_index(affinity);
        while let Some((idx, auth_mgr, account)) = self.next_account(start, &tried) {
            tried[idx] = true;

            match self
                .try_account(endpoint, &auth_mgr, &account, body.clone(), client_headers)
                .await
            {
                Ok(response) => {
                    // Every `/responses` answer, 429s included, carries the
                    // account's quota in its headers — free, per-request
                    // freshness. Not compact/search: see the doc above.
                    if endpoint == Endpoint::Responses {
                        if let Some(quota) = quota_from_headers(response.headers(), unix_now()) {
                            self.pool[idx].observe_quota(quota);
                        }
                    }
                    if endpoint == Endpoint::Responses && is_account_failure(response.status()) {
                        self.pool[idx].start_cooldown(self.account_cooldown);
                        let (response, quota) = self.classify_rate_limit(&account, response).await;
                        if let Some(hold) = quota {
                            self.pool[idx].mark_quota_exhausted(hold.duration);
                            tracing::warn!(
                                %account,
                                hold_secs = hold.duration.as_secs(),
                                hold_source = hold.source,
                                "account quota exhausted; skipping it until reset or until usage polling clears it"
                            );
                        }
                        tracing::warn!(
                            %account,
                            status = %response.status(),
                            "account failed, trying next pool account"
                        );
                        last_response = Some(ForwardedResponse {
                            response,
                            account,
                            reason: quota.map(|_| FailureReason::QuotaExhausted),
                        });
                        continue;
                    }
                    return Ok(ForwardedResponse {
                        response,
                        account,
                        reason: None,
                    });
                }
                Err(e) if endpoint != Endpoint::Responses => return Err(e),
                Err(e) => {
                    // The only failure class with no per-account line of its
                    // own: an HTTP failure logs just above, a transport error
                    // logs in `send_once`, but a token-refresh failure logged
                    // nowhere at all — so an account with a revoked
                    // refresh_token could sit dead in the pool indefinitely
                    // while a healthy sibling served every request. The
                    // failover line only ever names the LAST failure, so
                    // without this a mixed sweep (429 here, dead token there)
                    // loses the dead account entirely.
                    tracing::warn!(
                        %account,
                        reason = e.reason.as_str(),
                        error = %e.error,
                        "account failed without a response, trying next pool account"
                    );
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
            None => Err(last_err.unwrap_or_else(|| {
                PoolFailure::new(
                    FailureReason::Unknown,
                    None,
                    ProxyError::Upstream("upstream account pool is empty".into()),
                )
            })),
        }
    }

    /// Classify a 429: a transient throttle (`None` — the short cooldown is
    /// the only consequence) or a `usage_limit_reached` quota error (the
    /// hold to apply). The body has to be read to tell them apart, so it's
    /// buffered and the response rebuilt around it — status and headers
    /// intact — for the client, which must still see the real upstream
    /// error when every account fails. Anything that isn't a small 429
    /// passes through untouched. Pure with respect to pool state: the
    /// caller, which holds the pool index, does the marking.
    async fn classify_rate_limit(
        &self,
        account: &Arc<str>,
        response: reqwest::Response,
    ) -> (reqwest::Response, Option<QuotaHold>) {
        if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
            return (response, None);
        }
        let (response, body) = buffer_error_body(account, response).await;
        let hold = body.and_then(|body| {
            usage_limit_hold(
                response.headers(),
                &body,
                unix_now(),
                self.quota_hold_default(),
            )
        });
        (response, hold)
    }

    /// Hold for a quota-exhausted account when the 429 gave no usable reset
    /// time: one poll interval when polling is on (the poll will re-check),
    /// a fixed default otherwise.
    fn quota_hold_default(&self) -> Duration {
        self.quota_poll_interval.unwrap_or(QUOTA_HOLD_DEFAULT)
    }

    /// Run the quota poller until the process exits: every interval, re-check
    /// each quota-exhausted or idle account against the usage endpoint.
    /// Spawned once from `main` when `quota_check_interval_secs > 0`.
    pub async fn quota_poll_loop(self: Arc<Self>) {
        let Some(interval) = self.quota_poll_interval else {
            return;
        };
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately: nothing is exhausted at boot,
        // but no account has a quota report yet either, so the usage
        // metrics are filled in right away rather than one interval later.
        loop {
            ticker.tick().await;
            self.poll_quota_once().await;
        }
    }

    /// One poller pass over the pool. Queried: quota-exhausted accounts (to
    /// clear the hold early), and accounts with no quota report within the
    /// poll interval (idle ones — for the usage metrics). An account serving
    /// traffic is never polled while healthy: its `/responses` headers
    /// already carry the same report. An account whose hold has already
    /// lapsed is back in rotation without a check.
    ///
    /// Fail-open policy, decided here once: a poll that errors (endpoint
    /// down, token refresh failed, unparseable body) changes nothing — the
    /// account stays exhausted and its hold expires on the schedule the 429
    /// set. Failing toward "try upstream" would reintroduce the wasted
    /// round-trip on every request for as long as the endpoint is down;
    /// failing toward "stay exhausted forever" could strand a recovered
    /// account. Letting the 429's own reset time win is the middle ground.
    pub(crate) async fn poll_quota_once(&self) {
        let now = Instant::now();
        let max_age = self.quota_hold_default();
        // Concurrent, not one after another: a stalled usage GET for one
        // account must not delay every other account's re-check.
        let checks = self
            .pool
            .iter()
            .filter(|e| e.is_quota_exhausted(now) || e.quota_stale(now, max_age))
            .map(|entry| self.poll_account(entry));
        futures_util::future::join_all(checks).await;
    }

    async fn poll_account(&self, entry: &PoolEntry) {
        let account = &entry.label;
        let was_held = entry.is_quota_exhausted(Instant::now());
        let result = self.fetch_usage(&entry.auth).await;
        if let Ok(report) = &result {
            entry.observe_quota(report.quota.clone());
        }
        match result {
            Ok(report) if report.exhausted => {
                let hold = report
                    .reset_in
                    .map(clamp_quota_hold)
                    .unwrap_or_else(|| self.quota_hold_default());
                // An idle account found exhausted is held right away, sparing
                // the next request the 429 round-trip that would learn it.
                entry.mark_quota_exhausted(hold);
                tracing::info!(
                    %account,
                    used_percent = report.max_used_percent,
                    hold_secs = hold.as_secs(),
                    "usage poll: quota exhausted"
                );
            }
            Ok(report) => {
                if was_held {
                    entry.clear_quota_exhausted();
                    tracing::info!(
                        %account,
                        used_percent = report.max_used_percent,
                        "usage poll: quota available again, account back in rotation"
                    );
                }
            }
            // An idle account's failed poll lands here too: its usage
            // metrics just keep their last value (and age, see
            // `codexproxy_account_quota_observed_timestamp_seconds`).
            Err(e) => {
                tracing::warn!(
                    %account,
                    error = %e,
                    "usage poll failed; keeping quota state until the reported reset"
                );
            }
        }
    }

    /// GET the usage endpoint as this account, with the same identity
    /// headers the responses call sends — the real Codex CLI's own `/status`
    /// makes this exact request, so it looks like nothing new.
    async fn fetch_usage(&self, auth_mgr: &AuthManager) -> Result<UsageReport, ProxyError> {
        let auth = auth_mgr.headers().await?;
        let response = self
            .identity_headers(self.http.get(&self.usage_url), &auth)
            .header("Accept", "application/json")
            // A poll must not inherit the client's streaming read timeout
            // (`request_timeout_secs`, 10 minutes by default).
            .timeout(USAGE_POLL_TIMEOUT)
            .send()
            .await
            .map_err(|e| ProxyError::Upstream(format!("usage request failed: {e}")))?;
        let status = response.status();
        // Content-type plus a bounded body snippet: a 200 HTML login page, a
        // 401 for a revoked token and a 403 edge block must not all read as
        // the same "usage poll failed" line.
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-")
            .to_string();
        let body = response
            .bytes()
            .await
            .map_err(|e| ProxyError::Upstream(format!("usage body read failed: {e}")))?;
        let snippet = || crate::observe::truncate(&String::from_utf8_lossy(&body));
        if !status.is_success() {
            return Err(ProxyError::Upstream(format!(
                "usage endpoint returned {status} ({content_type}): {}",
                snippet()
            )));
        }
        parse_usage_report(&body, unix_now()).ok_or_else(|| {
            ProxyError::Upstream(format!(
                "usage body has no rate_limit report ({content_type}): {}",
                snippet()
            ))
        })
    }

    /// Try one pool account: send once, and if the upstream says 401, force a
    /// token refresh and retry once more on this same account before
    /// reporting it as failed. If the account instead rejects encrypted
    /// state replayed from another upstream (see `crate::replay`), retry once
    /// more on it without those items.
    async fn try_account(
        &self,
        endpoint: Endpoint,
        auth_mgr: &AuthManager,
        account: &Arc<str>,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
    ) -> Result<reqwest::Response, PoolFailure> {
        let response = self
            .send_refreshing(endpoint, auth_mgr, account, body.clone(), client_headers)
            .await?;
        if !replay::may_be_undecryptable(response.status()) {
            return Ok(response);
        }
        let (response, error_body) = buffer_error_body(account, response).await;
        if !error_body.is_some_and(|b| replay::is_undecryptable(&b)) {
            return Ok(response);
        }
        let Some((stripped, dropped)) = replay::strip_encrypted_body(&body) else {
            tracing::warn!(
                %account,
                "upstream could not decrypt replayed state, but the request carries no encrypted items to drop"
            );
            return Ok(response);
        };
        tracing::warn!(
            %account,
            dropped,
            "upstream could not decrypt replayed state from another upstream; retrying once without it"
        );
        self.send_refreshing(endpoint, auth_mgr, account, stripped, client_headers)
            .await
    }

    /// Send once, and on a 401 force a token refresh and send once more.
    async fn send_refreshing(
        &self,
        endpoint: Endpoint,
        auth_mgr: &AuthManager,
        account: &Arc<str>,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
    ) -> Result<reqwest::Response, PoolFailure> {
        // A token-refresh failure is an auth failure for this account: the
        // pool can't produce credentials for it, whatever the underlying
        // cause (rejected refresh_token, or the OAuth endpoint being down).
        let auth = auth_mgr
            .headers()
            .await
            .map_err(|e| PoolFailure::new(FailureReason::Auth, Some(account.clone()), e))?;
        let response = self
            .send_once(endpoint, &auth, body.clone(), client_headers, account)
            .await?;
        if response.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        tracing::info!(%account, "401 from upstream; forcing token refresh and retrying once");
        let refreshed = auth_mgr
            .force_refresh_headers()
            .await
            .map_err(|e| PoolFailure::new(FailureReason::Auth, Some(account.clone()), e))?;
        self.send_once(endpoint, &refreshed, body, client_headers, account)
            .await
    }

    /// The account-identity headers every upstream call carries —
    /// `Authorization`, `ChatGPT-Account-ID`, `originator`, `User-Agent` —
    /// in one place so the usage poll can't drift from the responses call
    /// and start looking like a different client.
    fn identity_headers(
        &self,
        req: reqwest::RequestBuilder,
        auth: &crate::auth::AuthHeaders,
    ) -> reqwest::RequestBuilder {
        let req = req
            .header("Authorization", format!("Bearer {}", auth.bearer))
            .header("originator", &self.originator)
            .header("User-Agent", &self.user_agent);
        match &auth.account_id {
            Some(account_id) => req.header("ChatGPT-Account-ID", account_id.clone()),
            None => req,
        }
    }

    async fn send_once(
        &self,
        endpoint: Endpoint,
        auth: &crate::auth::AuthHeaders,
        body: bytes::Bytes,
        client_headers: &reqwest::header::HeaderMap,
        account: &Arc<str>,
    ) -> Result<reqwest::Response, PoolFailure> {
        let (url, accept) = match endpoint {
            Endpoint::Responses => (&self.responses_url, "text/event-stream"),
            // Both answer one JSON document, not a stream.
            Endpoint::Compact => (&self.compact_url, "application/json"),
            Endpoint::Search => (&self.search_url, "application/json"),
        };
        let mut req = self
            .identity_headers(self.http.post(url), auth)
            .header("Content-Type", "application/json")
            .header("Accept", accept)
            .body(body);

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
            let target = endpoint.as_str();
            tracing::warn!(%account, error = %e, "forward to {target} failed");
            PoolFailure::new(
                FailureReason::from_transport(e.is_timeout(), e.is_connect()),
                Some(account.clone()),
                ProxyError::Upstream(format!("forward to {target} failed: {e}")),
            )
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

/// Buffer a small error body so it can be classified, and rebuild the
/// response around it — status and headers intact — for the client, which
/// must still see the real upstream error if nothing recovers it. `None`
/// (response passed through unread) for a body declared larger than
/// `ERROR_BODY_MAX_BYTES`; `None` too, with an empty body, if it couldn't be
/// read — the client would have hit the same broken body.
///
/// Only a declared oversized body is passed through unread. reqwest strips
/// `Content-Length` when it decompresses, so a compressed or chunked error
/// reports no length and is buffered whole regardless — the post-read size
/// check then only skips classification. Acceptable: the errors worth
/// classifying are a few hundred bytes.
pub(crate) async fn buffer_error_body(
    account: &str,
    mut response: reqwest::Response,
) -> (reqwest::Response, Option<bytes::Bytes>) {
    if response
        .content_length()
        .is_some_and(|len| len > ERROR_BODY_MAX_BYTES as u64)
    {
        return (response, None);
    }
    let status = response.status();
    let version = response.version();
    let headers = std::mem::take(response.headers_mut());
    let body = match response.bytes().await {
        Ok(body) => Some(body),
        Err(e) => {
            tracing::warn!(
                %account,
                %status,
                error = %e,
                "could not read upstream error body; relaying it empty, unclassified"
            );
            None
        }
    };
    let mut rebuilt = axum::http::Response::new(body.clone().unwrap_or_default());
    *rebuilt.status_mut() = status;
    *rebuilt.version_mut() = version;
    *rebuilt.headers_mut() = headers;
    let body = body.filter(|b| b.len() <= ERROR_BODY_MAX_BYTES);
    (reqwest::Response::from(rebuilt), body)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn clamp_quota_hold(hold: Duration) -> Duration {
    hold.clamp(QUOTA_HOLD_MIN, QUOTA_HOLD_MAX)
}

/// Seconds until a reset described either as an absolute unix timestamp
/// (`resets_at`/`reset_at`) or a relative count (`resets_in_seconds`/
/// `reset_after_seconds`). `None` when absent, unparseable, or already in
/// the past — a past reset means "should have recovered", and the caller's
/// default hold plus the poller decide what to do with that, not a zero.
fn reset_in(
    obj: &serde_json::Value,
    absolute_key: &str,
    relative_key: &str,
    now: u64,
) -> Option<Duration> {
    let relative = obj
        .get(relative_key)
        .and_then(as_u64_lossy)
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs);
    let absolute = obj
        .get(absolute_key)
        .and_then(as_u64_lossy)
        .filter(|at| *at > now)
        .map(|at| Duration::from_secs(at - now));
    relative.max(absolute)
}

/// Upstream numbers arrive as ints, floats, or (occasionally) strings.
/// Lossy above 2^53 — irrelevant for unix seconds and second counts.
fn as_u64_lossy(v: &serde_json::Value) -> Option<u64> {
    as_f64_lossy(v).filter(|f| *f >= 0.0).map(|f| f as u64)
}

fn as_f64_lossy(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// A quota hold decided from a 429, with where its length came from — so
/// the warn line can tell a genuine reset from a defaulted or clamped one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuotaHold {
    pub duration: Duration,
    /// `body` (`error.resets_at`/`resets_in_seconds`), `rate_limits` (a
    /// per-window reset in `error.rate_limits`), `header`
    /// (`x-codex-*-reset-after-seconds`), `default` (no usable reset at all,
    /// or one already in the past), or `clamped` (a parsed reset outside
    /// `QUOTA_HOLD_MIN..=QUOTA_HOLD_MAX`, almost certainly garbage).
    pub source: &'static str,
}

/// Decide whether a 429 is a `usage_limit_reached` quota error and, if so,
/// how long to hold the account. The body is the primary signal: the real
/// Codex CLI keys off `error.type` there (`codex-rs/core/src/client.rs`),
/// with `error.resets_at` (unix seconds) for the reset. `error.rate_limits`
/// (per-window `resets_at`/`resets_in_seconds`) and the
/// `x-codex-{primary,secondary}-reset-after-seconds` headers are consulted
/// for the reset only, taking the latest of whatever is present: when both
/// the 5h and the weekly window are blown, the weekly one is what matters.
/// A quota 429 with no usable reset at all holds for `default_hold`.
///
/// `None` for anything else — a plain throttle 429, a body that isn't JSON,
/// an `error.type` of something else (`usage_not_included`, an org policy
/// block) — which keeps the existing short cooldown as the only effect.
fn usage_limit_hold(
    headers: &reqwest::header::HeaderMap,
    body: &[u8],
    now: u64,
    default_hold: Duration,
) -> Option<QuotaHold> {
    let parsed: serde_json::Value = serde_json::from_slice(body).ok()?;
    let error = parsed.get("error")?;
    if error.get("type").and_then(serde_json::Value::as_str) != Some("usage_limit_reached") {
        return None;
    }
    // Latest reset wins; on a tie the earlier-listed source keeps the label.
    let mut best: Option<(Duration, &'static str)> = None;
    let mut consider = |candidate: Option<Duration>, source: &'static str| {
        if let Some(d) = candidate {
            if best.is_none_or(|(b, _)| d > b) {
                best = Some((d, source));
            }
        }
    };
    consider(
        reset_in(error, "resets_at", "resets_in_seconds", now),
        "body",
    );
    if let Some(windows) = error
        .get("rate_limits")
        .and_then(serde_json::Value::as_object)
    {
        for window in windows.values() {
            consider(
                reset_in(window, "resets_at", "resets_in_seconds", now),
                "rate_limits",
            );
        }
    }
    for name in [
        "x-codex-primary-reset-after-seconds",
        "x-codex-secondary-reset-after-seconds",
    ] {
        let from_header = headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .map(Duration::from_secs);
        consider(from_header, "header");
    }
    Some(match best {
        Some((parsed, source)) => {
            let duration = clamp_quota_hold(parsed);
            QuotaHold {
                duration,
                source: if duration == parsed {
                    source
                } else {
                    "clamped"
                },
            }
        }
        None => QuotaHold {
            duration: default_hold,
            source: "default",
        },
    })
}

/// What one usage poll said about an account.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UsageReport {
    /// `rate_limit.limit_reached`, or any window at/over 100%. Both windows
    /// count: the issue that motivated this feature reads only
    /// `primary_window` (the 5h limit) while describing the weekly limit,
    /// which is `secondary_window` — a script that only checks one can miss
    /// the other.
    pub exhausted: bool,
    /// Highest `used_percent` across windows, for the log line.
    pub max_used_percent: f64,
    /// Latest reset among the windows at/over 100%, relative to the `now`
    /// the report was parsed with — the weekly window's absolute `reset_at`
    /// must not lose to the 5h window's relative `reset_after_seconds` just
    /// because of which field each one used.
    pub reset_in: Option<Duration>,
    /// The full report, for the usage metrics.
    pub quota: QuotaSnapshot,
}

/// Metric label for a quota window: its length when known (`5h`, `7d`),
/// else its slot name.
fn window_label(minutes: Option<u64>, slot: &str) -> String {
    match minutes {
        Some(m) if m > 0 && m % 1440 == 0 => format!("{}d", m / 1440),
        Some(m) if m > 0 && m % 60 == 0 => format!("{}h", m / 60),
        Some(m) if m > 0 => format!("{m}m"),
        _ => slot.to_string(),
    }
}

/// The quota report the backend attaches to every `/responses` answer —
/// the same data as the usage endpoint, and what the real Codex CLI's
/// `/status` reads between polls:
/// `x-codex-{primary,secondary}-{used-percent,window-minutes,reset-at,
/// reset-after-seconds}`, `x-codex-plan-type`, `x-codex-credits-balance`.
/// A slot the plan doesn't have still arrives, as `window-minutes: 0` and an
/// empty `reset-at` (seen live on a pro account), and is skipped. `None` when
/// no window is reported at all (fake upstreams, fallback providers).
fn quota_from_headers(headers: &reqwest::header::HeaderMap, now: u64) -> Option<QuotaSnapshot> {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };
    let mut windows = Vec::new();
    for slot in ["primary", "secondary"] {
        let Some(used_percent) =
            header(&format!("x-codex-{slot}-used-percent")).and_then(|s| s.parse::<f64>().ok())
        else {
            continue;
        };
        let minutes =
            header(&format!("x-codex-{slot}-window-minutes")).and_then(|s| s.parse::<u64>().ok());
        if minutes == Some(0) {
            continue;
        }
        let reset_at = header(&format!("x-codex-{slot}-reset-at"))
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                header(&format!("x-codex-{slot}-reset-after-seconds"))
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|secs| *secs > 0)
                    .map(|secs| now + secs)
            });
        windows.push(QuotaWindow {
            label: window_label(minutes, slot),
            used_percent,
            reset_at,
        });
    }
    if windows.is_empty() {
        return None;
    }
    Some(QuotaSnapshot {
        windows,
        plan_type: header("x-codex-plan-type").map(str::to_string),
        credits_balance: header("x-codex-credits-balance").and_then(|s| s.parse().ok()),
        observed_at: now,
    })
}

/// Parse the `/wham/usage` payload:
/// `rate_limit: { allowed, limit_reached, primary_window: { used_percent,
/// reset_at | reset_after_seconds, ... }, secondary_window: { ... } }`.
/// Field names as the Codex CLI's own `UsageResponse` deserializes them —
/// note `reset_at` here vs `resets_at` on the 429 error; the two endpoints
/// don't agree. `None` when there's no `rate_limit` object at all (a poll
/// that can't be interpreted must not clear anything).
pub(crate) fn parse_usage_report(body: &[u8], now: u64) -> Option<UsageReport> {
    let parsed: serde_json::Value = serde_json::from_slice(body).ok()?;
    let rate_limit = parsed.get("rate_limit")?.as_object()?;
    let limit_reached = rate_limit
        .get("limit_reached")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mut max_used_percent: f64 = 0.0;
    let mut reset_in = None;
    let mut windows = Vec::new();
    for (key, slot) in [
        ("primary_window", "primary"),
        ("secondary_window", "secondary"),
    ] {
        let Some(window) = rate_limit.get(key).filter(|w| w.is_object()) else {
            continue;
        };
        let used = window
            .get("used_percent")
            .and_then(as_f64_lossy)
            .unwrap_or(0.0);
        max_used_percent = max_used_percent.max(used);
        let minutes = window
            .get("limit_window_seconds")
            .and_then(as_u64_lossy)
            .map(|secs| secs / 60);
        windows.push(QuotaWindow {
            label: window_label(minutes, slot),
            used_percent: used,
            reset_at: window.get("reset_at").and_then(as_u64_lossy).or_else(|| {
                window
                    .get("reset_after_seconds")
                    .and_then(as_u64_lossy)
                    .map(|secs| now + secs)
            }),
        });
        if used >= 100.0 {
            reset_in = reset_in.max(self::reset_in(
                window,
                "reset_at",
                "reset_after_seconds",
                now,
            ));
        }
    }
    Some(UsageReport {
        exhausted: limit_reached || max_used_percent >= 100.0,
        max_used_percent,
        reset_in,
        quota: QuotaSnapshot {
            windows,
            plan_type: parsed
                .get("plan_type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            // A decimal string on the wire (`"balance": "0"`).
            credits_balance: parsed
                .get("credits")
                .and_then(|c| c.get("balance"))
                .and_then(as_f64_lossy),
            observed_at: now,
        },
    })
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
    use crate::test_support::{
        input_types, start_replay_rejecting_upstream, write_test_auth_json, FOREIGN_REPLAY_BODY,
        USAGE_LIMIT_429_BODY,
    };

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
        test_pool_with_cooldown(base_url, n, 30).await
    }

    /// `account_cooldown_secs` as a parameter: the quota tests set it to 0
    /// so a hold can be told apart from the cooldown every failure starts.
    async fn test_pool_with_cooldown(base_url: &str, n: usize, cooldown_secs: u64) -> Upstream {
        let mut cfg = Config::default();
        cfg.upstream.base_url = base_url.to_string();
        // Same fake server also serves /oauth/token (see ScriptedState),
        // so a forced refresh in the retry-on-401 path resolves locally
        // instead of reaching the real OpenAI OAuth endpoint.
        cfg.upstream.issuer = base_url.to_string();
        cfg.upstream.account_cooldown_secs = cooldown_secs;
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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
                .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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

    #[tokio::test]
    async fn a_session_stays_on_its_home_account() {
        let mut fake = start_fake_account_log(8).await;
        let upstream = test_pool(&fake.base_url, 3).await;
        // "sess-abc" hashes to index 1 of 3 (see the pinned value above).
        for _ in 0..4 {
            let fwd = upstream
                .forward_responses(
                    bytes::Bytes::from_static(b"{}"),
                    &HeaderMap::new(),
                    Some(crate::affinity::fnv1a(b"sess-abc")),
                )
                .await
                .unwrap();
            assert_eq!(&*fwd.account, "account-1");
            assert_eq!(fake.recv().await.as_deref(), Some("acct-1"));
        }
        // Session-less traffic keeps round-robinning around it.
        let mut served = Vec::new();
        for _ in 0..3 {
            let fwd = upstream
                .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
                .await
                .unwrap();
            served.push(fwd.account.to_string());
            let _ = fake.recv().await;
        }
        assert_eq!(served, ["account-0", "account-1", "account-2"]);
    }

    #[tokio::test]
    async fn a_session_moves_to_the_next_account_while_home_is_cooling() {
        // Home (acct-1) throttles once. The session fails over to the next
        // account in scan order and STAYS there for the cooldown, instead of
        // bouncing between accounts on every turn.
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([(
            "acct-1",
            vec![429, 200],
        )]))
        .await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 3, 30).await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(b"{}"),
                &HeaderMap::new(),
                Some(crate::affinity::fnv1a(b"sess-abc")),
            )
            .await
            .unwrap();
        assert_eq!(&*fwd.account, "account-2");
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 429));
        assert_eq!(fake.recv().await, ("acct-2".to_string(), 200));

        for _ in 0..2 {
            let fwd = upstream
                .forward_responses(
                    bytes::Bytes::from_static(b"{}"),
                    &HeaderMap::new(),
                    Some(crate::affinity::fnv1a(b"sess-abc")),
                )
                .await
                .unwrap();
            assert_eq!(&*fwd.account, "account-2");
            assert_eq!(fake.recv().await, ("acct-2".to_string(), 200));
        }
    }

    #[tokio::test]
    async fn a_session_returns_home_once_the_cooldown_is_over() {
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([(
            "acct-1",
            vec![429, 200],
        )]))
        .await;
        // Cooldown 0: home is usable again immediately after its failure.
        let upstream = test_pool_with_cooldown(&fake.base_url, 3, 0).await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(b"{}"),
                &HeaderMap::new(),
                Some(crate::affinity::fnv1a(b"sess-abc")),
            )
            .await
            .unwrap();
        assert_eq!(&*fwd.account, "account-2");
        let _ = fake.recv().await;
        let _ = fake.recv().await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(b"{}"),
                &HeaderMap::new(),
                Some(crate::affinity::fnv1a(b"sess-abc")),
            )
            .await
            .unwrap();
        assert_eq!(&*fwd.account, "account-1");
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));
    }

    #[tokio::test]
    async fn every_account_is_tried_once_even_with_no_cooldown() {
        // With cooldown 0 a failed account stays "ready", so selection used
        // to be able to land on it again and end the sweep early. Tried
        // accounts are now excluded outright.
        let mut fake = start_scripted_upstream(std::collections::HashMap::from([
            ("acct-0", vec![429]),
            ("acct-1", vec![429]),
            ("acct-2", vec![429]),
        ]))
        .await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 3, 0).await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(b"{}"),
                &HeaderMap::new(),
                Some(crate::affinity::fnv1a(b"sess-abc")),
            )
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), 429);
        let mut hit: Vec<String> = Vec::new();
        for _ in 0..3 {
            hit.push(fake.recv().await.0);
        }
        assert_eq!(hit, ["acct-1", "acct-2", "acct-0"]);
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &client_headers, None)
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &client_headers, None)
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
        /// Body every 429 carries. `{"ok":true}` by default (a plain
        /// throttle); the quota tests swap in a `usage_limit_reached` error.
        rate_limit_body: &'static str,
        /// The scripted `/wham/usage` reply, `(status, body)`. `None` = 404.
        usage: Arc<tokio::sync::Mutex<Option<(u16, &'static str)>>>,
        usage_calls: Arc<AtomicUsize>,
    }

    struct ScriptedUpstream {
        base_url: String,
        rx: mpsc::Receiver<(String, u16)>,
        usage: Arc<tokio::sync::Mutex<Option<(u16, &'static str)>>>,
        usage_calls: Arc<AtomicUsize>,
    }

    impl ScriptedUpstream {
        async fn recv(&mut self) -> (String, u16) {
            self.rx.recv().await.expect("fake upstream request")
        }

        /// Set the `/wham/usage` reply for every poll from now on.
        async fn script_usage(&self, reply: (u16, &'static str)) {
            *self.usage.lock().await = Some(reply);
        }

        fn usage_calls(&self) -> usize {
            self.usage_calls.load(Ordering::Relaxed)
        }
    }

    async fn start_scripted_upstream(
        scripts: std::collections::HashMap<&str, Vec<u16>>,
    ) -> ScriptedUpstream {
        start_scripted_upstream_with_429_body(scripts, r#"{"ok":true}"#).await
    }

    async fn start_scripted_upstream_with_429_body(
        scripts: std::collections::HashMap<&str, Vec<u16>>,
        rate_limit_body: &'static str,
    ) -> ScriptedUpstream {
        let (tx, rx) = mpsc::channel(16);
        let scripts = scripts
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.into_iter().collect()))
            .collect();
        let usage = Arc::new(tokio::sync::Mutex::new(None));
        let usage_calls = Arc::new(AtomicUsize::new(0));
        let state = ScriptedState {
            scripts: Arc::new(tokio::sync::Mutex::new(scripts)),
            tx,
            rate_limit_body,
            usage: usage.clone(),
            usage_calls: usage_calls.clone(),
        };
        let app = Router::new()
            .route("/codex/responses", post(scripted_responses))
            .route("/oauth/token", post(scripted_oauth))
            .route("/wham/usage", axum::routing::get(scripted_usage))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        ScriptedUpstream {
            base_url: format!("http://{addr}"),
            rx,
            usage,
            usage_calls,
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
        let body = if status == 429 {
            state.rate_limit_body
        } else {
            r#"{"ok":true}"#
        };
        let mut response = Response::builder()
            .status(status)
            .header("Content-Type", "application/json");
        // Only on success: the quota tests script their 429's reset
        // themselves, and a header reset would win over it.
        if status == 200 {
            for (name, value) in PRO_QUOTA_HEADERS {
                response = response.header(*name, *value);
            }
        }
        response.body(Body::from(body)).unwrap()
    }

    async fn scripted_usage(State(state): State<ScriptedState>, headers: HeaderMap) -> Response {
        state.usage_calls.fetch_add(1, Ordering::Relaxed);
        // The poll must identify itself exactly like a responses call.
        assert!(
            headers.get("authorization").is_some(),
            "usage poll sent no bearer"
        );
        assert!(
            headers.get("chatgpt-account-id").is_some(),
            "usage poll sent no account id"
        );
        let (status, body) = state.usage.lock().await.unwrap_or((404, "{}"));
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    const USAGE_EXHAUSTED: &str = r#"{"plan_type":"plus","rate_limit":{"allowed":false,"limit_reached":true,"primary_window":{"used_percent":100,"limit_window_seconds":18000,"reset_after_seconds":7200,"reset_at":4102444800},"secondary_window":{"used_percent":100,"reset_at":4102444800}}}"#;
    const USAGE_AVAILABLE: &str = r#"{"plan_type":"plus","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":40.5,"reset_at":4102444800},"secondary_window":{"used_percent":12,"reset_at":4102444800}}}"#;

    #[tokio::test]
    async fn usage_limit_429_marks_the_account_quota_exhausted() {
        let mut fake = start_scripted_upstream_with_429_body(
            std::collections::HashMap::from([("acct-0", vec![429])]),
            USAGE_LIMIT_429_BODY,
        )
        .await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 1, 0).await;
        assert!(
            upstream.unavailable().is_none(),
            "fresh pool must be available"
        );

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 429));
        // The client still gets the real upstream error, body intact, even
        // though the pool read that body to classify it.
        assert_eq!(
            fwd.response.status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            fwd.response.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(
            fwd.response.bytes().await.unwrap(),
            USAGE_LIMIT_429_BODY.as_bytes()
        );

        let failure = upstream.unavailable().expect("pool should be unavailable");
        assert_eq!(failure.reason, FailureReason::QuotaExhausted);
        assert_eq!(failure.account.as_deref(), Some("account-0"));
        assert!(
            failure.error.to_string().contains("quota-exhausted"),
            "{}",
            failure.error
        );
    }

    #[tokio::test]
    async fn plain_429_is_a_throttle_not_a_quota_hold() {
        let mut fake =
            start_scripted_upstream(std::collections::HashMap::from([("acct-0", vec![429])])).await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 1, 0).await;

        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        assert_eq!(
            fwd.response.status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 429));
        // Cooldown is 0 here, and a plain 429 sets no quota hold: the
        // account is immediately selectable again.
        assert!(upstream.unavailable().is_none());
    }

    #[tokio::test]
    async fn unavailable_reports_cooling_down_when_every_account_is_in_cooldown() {
        let mut fake =
            start_scripted_upstream(std::collections::HashMap::from([("acct-0", vec![403])])).await;
        // Default 30s cooldown.
        let upstream = test_pool(&fake.base_url, 1).await;
        let _ = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 403));

        let failure = upstream.unavailable().expect("pool should be unavailable");
        assert_eq!(failure.reason, FailureReason::CoolingDown);
    }

    #[tokio::test]
    async fn quota_hold_skips_the_account_while_a_sibling_serves() {
        let mut fake = start_scripted_upstream_with_429_body(
            std::collections::HashMap::from([("acct-0", vec![429]), ("acct-1", vec![200])]),
            USAGE_LIMIT_429_BODY,
        )
        .await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 2, 0).await;

        // First request: acct-0 quota 429 -> failover to acct-1.
        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        assert_eq!(&*fwd.account, "account-1");
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 429));
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));
        // The pool as a whole is still available — acct-1 is fine.
        assert!(upstream.unavailable().is_none());

        // Second request: round-robin would land on acct-0, but its quota
        // hold (not a cooldown — those are off here) skips it.
        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        assert_eq!(&*fwd.account, "account-1");
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));
        assert!(
            fake.rx.try_recv().is_err(),
            "acct-0 must not be retried under quota hold"
        );
    }

    #[tokio::test]
    async fn poll_clears_quota_hold_when_usage_reports_headroom() {
        let mut fake = start_scripted_upstream_with_429_body(
            std::collections::HashMap::from([("acct-0", vec![429])]),
            USAGE_LIMIT_429_BODY,
        )
        .await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 1, 0).await;
        let _ = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        let _ = fake.recv().await;
        assert!(upstream.unavailable().is_some());

        // Still exhausted: state kept.
        fake.script_usage((200, USAGE_EXHAUSTED)).await;
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 1);
        assert_eq!(
            upstream.unavailable().map(|f| f.reason),
            Some(FailureReason::QuotaExhausted)
        );

        // Endpoint broken: fail open toward the 429's own reset, state kept.
        fake.script_usage((500, "boom")).await;
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 2);
        assert!(upstream.unavailable().is_some());

        // Unparseable 200: same — a poll we can't read must not clear anything.
        fake.script_usage((200, r#"{"plan_type":"plus"}"#)).await;
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 3);
        assert!(upstream.unavailable().is_some());

        // Headroom is back (early/manual reset): cleared, account in rotation.
        fake.script_usage((200, USAGE_AVAILABLE)).await;
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 4);
        assert!(upstream.unavailable().is_none());

        // Nothing exhausted any more: the poller stops asking.
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 4, "healthy accounts must not be polled");
    }

    #[test]
    fn usage_limit_hold_classifies_the_429_body() {
        let headers = HeaderMap::new();
        let now = 1_000_000;
        let default = Duration::from_secs(600);
        let hold = |headers: &HeaderMap, body: &str| {
            usage_limit_hold(headers, body.as_bytes(), now, default)
        };
        let expect = |secs: u64, source: &'static str| {
            Some(QuotaHold {
                duration: Duration::from_secs(secs),
                source,
            })
        };

        // Plain throttle / non-JSON / other error types: not a quota hold.
        assert_eq!(hold(&headers, r#"{"ok":true}"#), None);
        assert_eq!(hold(&headers, "rate limited"), None);
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_not_included","message":"x"}}"#
            ),
            None
        );

        // Absolute reset: held until then.
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_at":1003600}}"#
            ),
            expect(3600, "body")
        );
        // Relative reset (older shape).
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_in_seconds":7200}}"#
            ),
            expect(7200, "body")
        );
        // A zero relative count next to a valid absolute one: the absolute wins.
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_in_seconds":0,"resets_at":1003600}}"#
            ),
            expect(3600, "body")
        );
        // Numbers as strings or floats still parse.
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_at":"1003600"}}"#
            ),
            expect(3600, "body")
        );
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_at":1003600.9}}"#
            ),
            expect(3600, "body")
        );
        // Both windows blown: the later (weekly) reset wins.
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_at":1003600,"rate_limits":{"primary_window":{"resets_at":1003600},"secondary_window":{"resets_at":1500000}}}}"#
            ),
            expect(500_000, "rate_limits")
        );
        // No reset at all, or one already in the past: the caller's default.
        assert_eq!(
            hold(&headers, r#"{"error":{"type":"usage_limit_reached"}}"#),
            expect(600, "default")
        );
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_at":5}}"#
            ),
            expect(600, "default")
        );
        // Outside the sane range: clamped, and labelled as such.
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_at":99999999999}}"#
            ),
            Some(QuotaHold {
                duration: QUOTA_HOLD_MAX,
                source: "clamped"
            })
        );
        assert_eq!(
            hold(
                &headers,
                r#"{"error":{"type":"usage_limit_reached","resets_in_seconds":5}}"#
            ),
            Some(QuotaHold {
                duration: QUOTA_HOLD_MIN,
                source: "clamped"
            })
        );
        // Header-only reset (body says quota, no timestamp in it).
        let mut with_header = HeaderMap::new();
        with_header.insert(
            "x-codex-secondary-reset-after-seconds",
            HeaderValue::from_static("86400"),
        );
        assert_eq!(
            hold(&with_header, r#"{"error":{"type":"usage_limit_reached"}}"#),
            expect(86400, "header")
        );
    }

    /// Quota headers of a real `/responses` answer for a pro account
    /// (captured 2026-09-23): one weekly window, reported as `primary`, and
    /// an unused `secondary` slot sent as zeros rather than omitted.
    const PRO_QUOTA_HEADERS: &[(&str, &str)] = &[
        ("x-codex-active-limit", "premium"),
        ("x-codex-plan-type", "pro"),
        ("x-codex-primary-used-percent", "17"),
        ("x-codex-secondary-used-percent", "0"),
        ("x-codex-primary-window-minutes", "10080"),
        ("x-codex-secondary-window-minutes", "0"),
        ("x-codex-primary-reset-after-seconds", "526623"),
        ("x-codex-secondary-reset-after-seconds", "0"),
        ("x-codex-primary-reset-at", "1790706958"),
        ("x-codex-secondary-reset-at", ""),
        ("x-codex-credits-has-credits", "False"),
        ("x-codex-credits-balance", "0"),
        ("x-codex-credits-unlimited", "False"),
    ];

    fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn quota_from_headers_reads_the_live_pro_shape() {
        let quota = quota_from_headers(&header_map(PRO_QUOTA_HEADERS), 1_000).unwrap();
        assert_eq!(
            quota,
            QuotaSnapshot {
                windows: vec![QuotaWindow {
                    label: "7d".into(),
                    used_percent: 17.0,
                    reset_at: Some(1_790_706_958),
                }],
                plan_type: Some("pro".into()),
                credits_balance: Some(0.0),
                observed_at: 1_000,
            }
        );
    }

    #[test]
    fn quota_from_headers_labels_windows_by_length_not_slot() {
        // Plus-style: 5h in `primary`, weekly in `secondary`, and only a
        // relative reset for one of them.
        let quota = quota_from_headers(
            &header_map(&[
                ("x-codex-primary-used-percent", "42.5"),
                ("x-codex-primary-window-minutes", "300"),
                ("x-codex-primary-reset-after-seconds", "600"),
                ("x-codex-secondary-used-percent", "8"),
                ("x-codex-secondary-window-minutes", "10080"),
                ("x-codex-secondary-reset-at", "5000"),
            ]),
            1_000,
        )
        .unwrap();
        let windows: Vec<_> = quota
            .windows
            .iter()
            .map(|w| (w.label.as_str(), w.used_percent, w.reset_at))
            .collect();
        assert_eq!(
            windows,
            vec![("5h", 42.5, Some(1_600)), ("7d", 8.0, Some(5_000))]
        );
        assert_eq!(quota.plan_type, None);

        // No quota headers at all (a fake upstream, a fallback provider).
        assert_eq!(quota_from_headers(&HeaderMap::new(), 1_000), None);
    }

    #[test]
    fn parse_usage_report_carries_the_quota_snapshot() {
        // The live pro-account `/wham/usage` shape (2026-09-23), trimmed.
        let body = br#"{"plan_type":"pro","rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":17,"limit_window_seconds":604800,"reset_after_seconds":526633,"reset_at":1790706958},"secondary_window":null},"credits":{"has_credits":false,"unlimited":false,"balance":"0"}}"#;
        let report = parse_usage_report(body, 1_000).unwrap();
        assert!(!report.exhausted);
        assert_eq!(
            report.quota,
            QuotaSnapshot {
                windows: vec![QuotaWindow {
                    label: "7d".into(),
                    used_percent: 17.0,
                    reset_at: Some(1_790_706_958),
                }],
                plan_type: Some("pro".into()),
                credits_balance: Some(0.0),
                observed_at: 1_000,
            }
        );
        // No window length reported: the slot name stands in.
        let report = parse_usage_report(USAGE_AVAILABLE.as_bytes(), 0).unwrap();
        let labels: Vec<_> = report
            .quota
            .windows
            .iter()
            .map(|w| w.label.as_str())
            .collect();
        assert_eq!(labels, ["primary", "secondary"]);
    }

    #[tokio::test]
    async fn responses_headers_fill_the_account_quota() {
        let mut fake = start_scripted_upstream(std::collections::HashMap::new()).await;
        let upstream = test_pool(&fake.base_url, 2).await;
        assert!(upstream
            .account_statuses()
            .iter()
            .all(|s| s.quota.is_none()));

        upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), Some(0))
            .await
            .unwrap();
        let _ = fake.recv().await;

        let statuses = upstream.account_statuses();
        assert_eq!(statuses[0].state, "ready");
        let quota = statuses[0]
            .quota
            .as_ref()
            .expect("served account has a quota");
        assert_eq!(quota.windows[0].label, "7d");
        assert!(
            statuses[1].quota.is_none(),
            "the other account wasn't asked"
        );

        // A poll asks only the account with no report; the one whose
        // headers just said it all gets no extra traffic.
        fake.script_usage((200, USAGE_AVAILABLE)).await;
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 1);
        assert!(upstream.account_statuses()[1].quota.is_some());
        upstream.poll_quota_once().await;
        assert_eq!(fake.usage_calls(), 1, "both reports are fresh now");
    }

    #[tokio::test]
    async fn poll_holds_an_idle_account_found_exhausted() {
        let fake = start_scripted_upstream(std::collections::HashMap::new()).await;
        let upstream = test_pool(&fake.base_url, 1).await;
        fake.script_usage((200, USAGE_EXHAUSTED)).await;
        upstream.poll_quota_once().await;
        assert_eq!(
            upstream.unavailable().map(|f| f.reason),
            Some(FailureReason::QuotaExhausted),
            "the next request must not pay a 429 round-trip to learn this"
        );
        assert_eq!(upstream.account_statuses()[0].state, "quota_held");
    }

    #[test]
    fn account_states_cover_every_availability() {
        for a in [
            Availability::Ready,
            Availability::Cooling,
            Availability::QuotaHeld,
        ] {
            assert!(ACCOUNT_STATES.contains(&a.as_str()), "{a:?}");
        }
    }

    #[test]
    fn parse_usage_report_reads_both_windows() {
        // Primary carries a relative 2h, secondary an absolute 2100 reset:
        // the later one wins regardless of which field shape it used.
        let report = parse_usage_report(USAGE_EXHAUSTED.as_bytes(), 4_102_444_800 - 3600).unwrap();
        assert!(report.exhausted);
        assert_eq!(report.max_used_percent, 100.0);
        assert_eq!(report.reset_in, Some(Duration::from_secs(7200)));
        let report =
            parse_usage_report(USAGE_EXHAUSTED.as_bytes(), 4_102_444_800 - 500_000).unwrap();
        assert_eq!(report.reset_in, Some(Duration::from_secs(500_000)));

        let report = parse_usage_report(USAGE_AVAILABLE.as_bytes(), 0).unwrap();
        assert!(!report.exhausted);
        assert_eq!(report.max_used_percent, 40.5);
        assert_eq!(
            report.reset_in, None,
            "no window is blown, nothing to wait for"
        );

        // Weekly (secondary) blown while the 5h window has headroom — the
        // case a primary-only check misses.
        let weekly = br#"{"rate_limit":{"limit_reached":false,"primary_window":{"used_percent":12},"secondary_window":{"used_percent":100,"reset_at":1500000}}}"#;
        let report = parse_usage_report(weekly, 1_000_000).unwrap();
        assert!(report.exhausted);
        assert_eq!(report.reset_in, Some(Duration::from_secs(500_000)));

        // `limit_reached` alone is enough.
        let flagged =
            br#"{"rate_limit":{"limit_reached":true,"primary_window":{"used_percent":99}}}"#;
        assert!(parse_usage_report(flagged, 0).unwrap().exhausted);

        // No rate_limit object at all: unreadable, not "available".
        assert_eq!(parse_usage_report(br#"{"plan_type":"plus"}"#, 0), None);
        assert_eq!(parse_usage_report(b"nope", 0), None);
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);
        assert_eq!(fake.recv().await, ("acct-0".to_string(), 403));
        assert_eq!(fake.recv().await, ("acct-1".to_string(), 200));

        // Second request, immediately after: acct-0 is still cooling down, so
        // round-robin should skip straight to acct-1 without ever hitting
        // acct-0's endpoint again — exactly one more call, to acct-1.
        let fwd = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
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

    #[test]
    fn failure_reason_classifies_upstream_statuses() {
        use reqwest::StatusCode as S;
        let cases = [
            (S::TOO_MANY_REQUESTS, "rate_limit"),
            (S::UNAUTHORIZED, "auth"),
            (S::FORBIDDEN, "auth"),
            (S::REQUEST_TIMEOUT, "timeout"),
            (S::GATEWAY_TIMEOUT, "timeout"),
            (S::SERVICE_UNAVAILABLE, "capacity"),
            (S::INTERNAL_SERVER_ERROR, "upstream_5xx"),
            (S::BAD_GATEWAY, "upstream_5xx"),
            (S::BAD_REQUEST, "bad_request"),
            (S::NOT_FOUND, "bad_request"),
            // Not a failure shape the pool produces, but the classifier must
            // still land somewhere groupable rather than panicking.
            (S::FOUND, "unknown"),
        ];
        for (status, expected) in cases {
            assert_eq!(
                FailureReason::from_status(status).as_str(),
                expected,
                "status {status}"
            );
        }
    }

    #[tokio::test]
    async fn quota_hold_only_ever_extends() {
        // 429 with no parseable reset: hold = one poll interval (600s default).
        let mut fake = start_scripted_upstream_with_429_body(
            std::collections::HashMap::from([("acct-0", vec![429])]),
            r#"{"error":{"type":"usage_limit_reached"}}"#,
        )
        .await;
        let upstream = test_pool_with_cooldown(&fake.base_url, 1, 0).await;
        let _ = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await
            .unwrap();
        let _ = fake.recv().await;
        let remaining = |upstream: &Upstream| {
            upstream.pool[0]
                .quota_hold_remaining(Instant::now())
                .expect("account should be under quota hold")
        };
        let initial = remaining(&upstream);
        assert!(initial <= Duration::from_secs(600), "{initial:?}");

        // Poll says still exhausted with a 2h reset: the hold grows past the
        // defaulted 600s (without the re-mark it would lapse while the
        // account is still exhausted).
        fake.script_usage((200, r#"{"rate_limit":{"limit_reached":true,"primary_window":{"used_percent":100,"reset_after_seconds":7200}}}"#)).await;
        upstream.poll_quota_once().await;
        let extended = remaining(&upstream);
        assert!(extended > Duration::from_secs(600), "{extended:?}");

        // A later poll carrying a nearer reset must not shorten it.
        fake.script_usage((200, r#"{"rate_limit":{"limit_reached":true,"primary_window":{"used_percent":100,"reset_after_seconds":60}}}"#)).await;
        upstream.poll_quota_once().await;
        let after = remaining(&upstream);
        assert!(after > Duration::from_secs(7000), "{after:?}");
    }

    #[test]
    fn synthetic_failure_reasons_have_stable_labels() {
        // Set by the pool's own classification (`Upstream::unavailable`, or
        // a quota 429 it just saw), not by `from_status`. Operators group
        // failover lines on these.
        assert_eq!(FailureReason::QuotaExhausted.as_str(), "quota_exhausted");
        assert_eq!(FailureReason::CoolingDown.as_str(), "cooling_down");
    }

    #[test]
    fn failure_reason_classifies_transport_errors() {
        assert_eq!(
            FailureReason::from_transport(true, false).as_str(),
            "timeout"
        );
        // A timed-out connect is still a timeout: the deadline is the
        // actionable fact, the phase it expired in isn't.
        assert_eq!(
            FailureReason::from_transport(true, true).as_str(),
            "timeout"
        );
        assert_eq!(
            FailureReason::from_transport(false, true).as_str(),
            "transport"
        );
        // Neither predicate set — a body/decode/redirect error. Must not be
        // silently folded into "transport", or the log stops distinguishing
        // "couldn't reach it" from "reached it and something else broke".
        assert_eq!(
            FailureReason::from_transport(false, false).as_str(),
            "unknown"
        );
    }

    #[tokio::test]
    async fn pool_failure_names_the_account_it_last_tried() {
        // The no-response path is the one where the caller has no
        // `ForwardedResponse` to read an account off — without `PoolFailure`
        // carrying it, the failover log would attribute the failure to "-"
        // in exactly the transport/timeout case that most needs a name.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // nothing is listening on `addr` now
        let upstream = test_pool(&format!("http://{addr}"), 1).await;

        let result = upstream
            .forward_responses(bytes::Bytes::from_static(b"{}"), &HeaderMap::new(), None)
            .await;
        let Err(failure) = result else {
            panic!("a closed port must not yield a response");
        };
        assert_eq!(failure.reason, FailureReason::Transport);
        assert_eq!(failure.account.as_deref(), Some("account-0"));
    }

    #[tokio::test]
    async fn undecryptable_replay_is_retried_once_on_the_same_account_without_it() {
        let mut fake = start_replay_rejecting_upstream("/codex/responses", false).await;
        let upstream = test_pool(&fake.base_url, 2).await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(FOREIGN_REPLAY_BODY.as_bytes()),
                &reqwest::header::HeaderMap::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::OK);

        let first = fake.rx.recv().await.unwrap();
        let retry = fake.rx.recv().await.unwrap();
        assert_eq!(
            first.headers.get("chatgpt-account-id"),
            retry.headers.get("chatgpt-account-id"),
            "the retry must stay on the account that rejected the replay"
        );
        assert_eq!(
            input_types(&retry.body),
            ["message", "function_call", "function_call_output"]
        );
        assert!(fake.rx.try_recv().is_err(), "exactly one retry");
        assert!(
            upstream.unavailable().is_none(),
            "a rejected replay is no account failure: nothing cools down"
        );
    }

    #[tokio::test]
    async fn undecryptable_replay_with_nothing_to_drop_is_relayed_without_a_retry() {
        let mut fake = start_replay_rejecting_upstream("/codex/responses", true).await;
        let upstream = test_pool(&fake.base_url, 1).await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(br#"{"model":"gpt-5.5","input":"hi"}"#),
                &reqwest::header::HeaderMap::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body = fwd.response.bytes().await.unwrap();
        assert!(
            crate::replay::is_undecryptable(&body),
            "error body relayed intact"
        );
        assert!(fake.rx.recv().await.is_some());
        assert!(fake.rx.try_recv().is_err(), "no retry");
    }

    #[tokio::test]
    async fn a_retry_that_is_rejected_again_is_relayed_not_retried_further() {
        let mut fake = start_replay_rejecting_upstream("/codex/responses", true).await;
        let upstream = test_pool(&fake.base_url, 1).await;

        let fwd = upstream
            .forward_responses(
                bytes::Bytes::from_static(FOREIGN_REPLAY_BODY.as_bytes()),
                &reqwest::header::HeaderMap::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(fwd.response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert!(fake.rx.recv().await.is_some());
        assert!(fake.rx.recv().await.is_some());
        assert!(fake.rx.try_recv().is_err(), "at most one retry");
    }
}
