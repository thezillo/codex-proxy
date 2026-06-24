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

use std::collections::HashMap;
use std::time::Instant;

use axum::http::HeaderMap;

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
/// dependency. Used only when the key has no configured `key_names` label.
pub fn key_label(key: &str, names: &HashMap<String, String>) -> String {
    if let Some(name) = names.get(key) {
        return name.clone();
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
    started: Instant,
}

impl CompletionLog {
    pub fn new(ctx: AccessCtx, endpoint: &'static str, model: impl Into<String>) -> Self {
        Self {
            ctx,
            endpoint,
            model: model.into(),
            started: Instant::now(),
        }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_key_uses_label_else_fingerprint() {
        let mut names = HashMap::new();
        names.insert("sk-secret".to_string(), "alice".to_string());
        assert_eq!(key_label("sk-secret", &names), "alice");

        // Unknown key -> stable, non-leaking fingerprint.
        let fp = key_label("sk-other", &names);
        assert!(fp.starts_with("key-"));
        assert_eq!(fp.len(), "key-".len() + 8);
        // The raw key never appears in the label.
        assert!(!fp.contains("sk-other"));
        // Deterministic.
        assert_eq!(fp, key_label("sk-other", &names));
        // Distinct keys -> distinct fingerprints.
        assert_ne!(key_label("sk-a", &names), key_label("sk-b", &names));
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
