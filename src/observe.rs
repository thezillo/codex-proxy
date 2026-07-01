//! Access logging & token-usage attribution.
//!
//! The proxy guards a single shared ChatGPT subscription behind client API
//! keys, so the operator's real question is "*who* is spending tokens?". This
//! module turns a presented key into a non-secret label, pulls the real client
//! IP/User-Agent out of the request, and emits two structured lines per request
//! under the `access` target:
//!
//!   * start — logged in the auth middleware for *every* protected endpoint:
//!     `client`, `ip`, `ua`, `path`. This alone answers "who", even for the
//!     `/v1/responses` passthrough and for requests the client aborts midway.
//!   * end — logged by the handler once the response is produced/streamed:
//!     `endpoint`, `model`, `status`, token counts, `duration_ms`.

use std::sync::Arc;
use std::time::Instant;

use axum::http::HeaderMap;

use crate::metrics::{Metrics, RequestOutcome};

/// Who a request is attributed to, derived from the matched client key. Carried
/// from the auth middleware into the handler via request extensions. `client`
/// is a human label (configured name or fingerprint); `ip` is the best guess at
/// the real caller's address.
#[derive(Clone, Debug)]
pub struct AccessCtx {
    pub client: String,
    pub ip: String,
}

/// Non-reversible 32-bit fingerprint of a key (FNV-1a, low 32 bits), rendered
/// as `key-XXXXXXXX`. Lets the operator tell distinct keys apart in logs
/// without the key itself ever appearing — and without pulling in a crypto
/// dependency. Used only when the matched `ClientKey` has no configured name.
pub fn key_label(key: &str, name: Option<&str>) -> String {
    if let Some(name) = name {
        return name.to_string();
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("key-{:08x}", hash & 0xffff_ffff)
}

/// Best-effort real client IP. Behind Fly (and most proxies) the TCP peer is
/// the edge, not the caller, so trust the forwarding headers: `Fly-Client-IP`
/// first, then the leftmost `X-Forwarded-For` hop. Header-only by design — it
/// keeps the `axum::serve` signature unchanged and avoids breaking tests that
/// don't set `ConnectInfo`.
pub fn client_ip(headers: &HeaderMap) -> String {
    headers
        .get("fly-client-ip")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split(',').next())
                .map(str::trim)
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("?")
        .to_string()
}

/// Caller's User-Agent, or `?` when absent.
pub fn user_agent(headers: &HeaderMap) -> String {
    headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("?")
        .to_string()
}

/// Per-request completion logger. Built in the handler, then either emitted
/// inline (buffered responses) or moved into the SSE stream and emitted when it
/// finishes (streaming/passthrough) so token usage lands once known.
pub struct CompletionLog {
    ctx: AccessCtx,
    endpoint: &'static str,
    model: String,
    /// Bounded stand-in for `model` used only for the Prometheus label — the
    /// access log's `model` field can be an arbitrary client-supplied string
    /// (fine, log lines aren't aggregated into label-indexed series), but a
    /// metric label must not be: an unrecognized value would mint a new time
    /// series per distinct string. Callers pass an already-clamped value
    /// (e.g. server.rs's `metric_model_label`, collapsing anything outside
    /// the supported set to `"other"`).
    metric_model: String,
    account: Option<Arc<str>>,
    started: Instant,
    metrics: Arc<Metrics>,
}

impl CompletionLog {
    pub fn new(
        ctx: AccessCtx,
        endpoint: &'static str,
        model: impl Into<String>,
        metric_model: impl Into<String>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            ctx,
            endpoint,
            model: model.into(),
            metric_model: metric_model.into(),
            account: None,
            started: Instant::now(),
            metrics,
        }
    }

    /// Record which upstream pool account served this request, once known
    /// (after `Upstream::forward_responses` returns). The log is created
    /// before the forward so `duration_ms` still covers the whole upstream
    /// round trip; every call site sets this immediately after forwarding
    /// succeeds, so `account` is effectively always `Some` by `emit()` time.
    pub fn set_account(&mut self, account: impl Into<Arc<str>>) {
        self.account = Some(account.into());
    }

    /// Emit the completion line. `usage` is `(prompt_tokens, completion_tokens)`
    /// when known; `None` when the upstream never reported it (e.g. an error
    /// before `response.completed`, or a passthrough body we don't parse).
    pub fn emit(&self, status: u16, usage: Option<(i64, i64)>) {
        let duration_ms = self.started.elapsed().as_millis() as u64;
        let (prompt, completion) = usage.unwrap_or((0, 0));
        tracing::info!(
            target: "access",
            client = %self.ctx.client,
            ip = %self.ctx.ip,
            account = %self.account.as_deref().unwrap_or("-"),
            endpoint = self.endpoint,
            model = %self.model,
            status,
            prompt_tokens = prompt,
            completion_tokens = completion,
            total_tokens = prompt + completion,
            usage_reported = usage.is_some(),
            duration_ms,
            "request completed"
        );
        self.metrics.record(RequestOutcome {
            endpoint: self.endpoint,
            client: &self.ctx.client,
            account: self.account.as_deref().unwrap_or("-"),
            model: &self.metric_model,
            status,
            usage,
            duration_secs: duration_ms as f64 / 1000.0,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_records_a_prometheus_sample_alongside_the_access_log_line() {
        let metrics = Arc::new(Metrics::new().unwrap());
        let ctx = AccessCtx {
            client: "alice".to_string(),
            ip: "1.2.3.4".to_string(),
        };
        let mut log = CompletionLog::new(
            ctx,
            "/v1/chat/completions",
            "gpt-5.5",
            "gpt-5.5",
            metrics.clone(),
        );
        log.set_account("primary");
        log.emit(200, Some((10, 20)));

        let (_, body) = metrics.encode();
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains("codexproxy_requests_total"));
        assert!(text.contains(r#"client="alice""#));
        assert!(text.contains(r#"account="primary""#));
        assert!(text.contains(r#"model="gpt-5.5""#));
        assert!(text.contains(r#"status="200""#));
        assert!(text.contains("codexproxy_tokens_total"));
    }

    #[test]
    fn emit_uses_the_clamped_metric_model_not_the_raw_one() {
        // `model` (raw, client-supplied, unbounded) and `metric_model`
        // (caller-clamped, bounded) are allowed to diverge — the metric must
        // only ever see the clamped value, never the raw one, however wild.
        let metrics = Arc::new(Metrics::new().unwrap());
        let ctx = AccessCtx {
            client: "bob".to_string(),
            ip: "5.6.7.8".to_string(),
        };
        let mut log = CompletionLog::new(
            ctx,
            "/v1/chat/completions",
            "totally-unrecognized-client-supplied-garbage",
            "other",
            metrics.clone(),
        );
        log.set_account("primary");
        log.emit(200, None);

        let (_, body) = metrics.encode();
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains(r#"model="other""#));
        assert!(!text.contains("totally-unrecognized-client-supplied-garbage"));
    }

    #[test]
    fn named_key_uses_label_else_fingerprint() {
        assert_eq!(key_label("sk-secret", Some("alice")), "alice");

        // No configured name -> stable, non-leaking fingerprint.
        let fp = key_label("sk-other", None);
        assert!(fp.starts_with("key-"));
        assert_eq!(fp.len(), "key-".len() + 8);
        // The raw key never appears in the label.
        assert!(!fp.contains("sk-other"));
        // Deterministic.
        assert_eq!(fp, key_label("sk-other", None));
        // Distinct keys -> distinct fingerprints.
        assert_ne!(key_label("sk-a", None), key_label("sk-b", None));
    }

    #[test]
    fn client_ip_prefers_fly_then_xff() {
        let mut h = HeaderMap::new();
        assert_eq!(client_ip(&h), "?");

        h.insert("x-forwarded-for", "1.2.3.4, 5.6.7.8".parse().unwrap());
        assert_eq!(client_ip(&h), "1.2.3.4");

        h.insert("fly-client-ip", "9.9.9.9".parse().unwrap());
        assert_eq!(client_ip(&h), "9.9.9.9");
    }
}
