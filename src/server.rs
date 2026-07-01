//! HTTP server: client API-key auth + OpenAI-compatible endpoints.
//!
//! `/v1/chat/completions` translates OpenAI Chat Completions to/from the Codex
//! Responses API (streaming and buffered). `/v1/responses` is a raw passthrough
//! for clients that already speak the `responses` wire format (e.g. codex
//! itself). Upstream errors are relayed with their original status and body.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures_util::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::config::{ClientKey, Config};
use crate::error::ProxyError;
use crate::fallback::FallbackChain;
use crate::metrics::Metrics;
use crate::observe::{self, AccessCtx, CompletionLog};
use crate::translate::{
    build_codex_request, collect_chat, stream_chat, tee_responses, ChatCompletionRequest,
};
use crate::upstream::{ForwardedResponse, Upstream};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub upstream: Arc<Upstream>,
    pub fallback: Arc<FallbackChain>,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    /// Try the ChatGPT account pool first; if its final response is any
    /// non-2xx — not just the 401/403/429 that trigger intra-pool failover,
    /// but also e.g. a 5xx from Codex itself being down — try the configured
    /// fallback chain and use its result instead. A narrower gate here (say,
    /// mirroring `is_account_failure`) would silently skip fallback on the
    /// single most likely real "everything's down" case: a whole-service
    /// Codex outage returns 5xx to every pool account alike, which isn't an
    /// *account* failure but is exactly when a working fallback matters most.
    /// If the fallback chain has nothing usable for this request (empty, no
    /// provider mapped for the model, or every attempted provider
    /// transport-errored), the pool's own response is returned unchanged. A
    /// pool `Err` (every account transport-errored, no response obtained at
    /// all) is treated the same way — tried against the fallback chain, with
    /// the `Err` only propagating if the chain has nothing for it either.
    async fn forward_with_fallback(
        &self,
        body: bytes::Bytes,
        client_headers: &HeaderMap,
    ) -> Result<ForwardedResponse, ProxyError> {
        match self
            .upstream
            .forward_responses(body.clone(), client_headers)
            .await
        {
            Ok(fwd) if fwd.response.status().is_success() => Ok(fwd),
            Ok(fwd) => Ok(self.fallback.run(body).await.unwrap_or(fwd)),
            Err(pool_err) => match self.fallback.run(body).await {
                Some(fallback_fwd) => Ok(fallback_fwd),
                None => Err(pool_err),
            },
        }
    }
}

pub fn router(state: AppState) -> Router {
    // Lift Axum's 2 MB default body cap to the configured limit so large
    // contexts / base64 images aren't 413'd before we can proxy them.
    let body_limit = state.config.server.max_body_bytes;
    let auth_layer = middleware::from_fn_with_state(state.config.clone(), require_client_auth);

    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
        .route("/v1/models/:model", get(model_by_id))
        .route(
            "/v1/responses",
            post(responses).route_layer(auth_layer.clone()),
        )
        .route(
            "/v1/chat/completions",
            post(chat_completions).route_layer(auth_layer),
        )
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
}

/// A separate, minimal router for `/metrics` — deliberately NOT part of
/// `router()` above. It's meant to run on its own port (see
/// `config.server.metrics_port`/`metrics_host`) so it can be network-isolated
/// independently of the client-facing API; no client-key auth, no body limit
/// (a GET with no body), nothing beyond the one route.
pub fn metrics_router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics)
}

async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> Response {
    let (content_type, body) = metrics.encode();
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build metrics response: {e}")).into_response()
        })
}

/// Authenticate before any handler body extractor runs. The POST endpoints
/// accept potentially-large JSON bodies, so doing this inside the handler would
/// let unauthenticated clients force buffering up to `max_body_bytes`.
async fn require_client_auth(
    State(config): State<Arc<Config>>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, ProxyError> {
    let client = check_client_auth(&config, &headers)?;
    let ip = observe::client_ip(&headers);

    // Start line: answers "who is calling, from where" for *every* protected
    // endpoint — including the `/v1/responses` passthrough and requests the
    // client aborts before any completion line can be emitted.
    tracing::info!(
        target: "access",
        client = %client,
        ip = %ip,
        ua = %observe::user_agent(&headers),
        method = %request.method(),
        path = %request.uri().path(),
        "request accepted"
    );

    // Hand the attribution to the handler so its completion line (model, tokens,
    // status, duration) carries the same client/ip.
    request.extensions_mut().insert(AccessCtx { client, ip });
    Ok(next.run(request).await)
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Models this proxy advertises. The Codex upstream only serves gpt-5.5 over a
/// ChatGPT account, so that's the single id exposed to clients. Both the list
/// and retrieve endpoints derive their output from this slice.
const SUPPORTED_MODELS: &[&str] = &["gpt-5.5"];

/// One OpenAI-style model object. Mirrors what `/v1/models` returns per entry,
/// so a `GET /v1/models/{id}` retrieve and a list entry stay byte-identical.
fn model_object(id: &str) -> serde_json::Value {
    json!({ "id": id, "object": "model", "owned_by": "openai" })
}

/// Clamp a client-supplied model string to a bounded metric-label value.
/// `req.model` in `/v1/chat/completions` is arbitrary client input (echoed
/// back verbatim in the response, which is fine for a JSON field but NOT for
/// a Prometheus label — an unrecognized value would mint a new time series
/// per distinct string, e.g. an attacker or a misconfigured client hammering
/// the registry/Mimir with unbounded cardinality). Anything outside
/// `SUPPORTED_MODELS` collapses to `"other"`; the access *log* still records
/// the real requested model separately (that's fine, log lines aren't
/// aggregated into label-indexed series).
fn metric_model_label(model: &str) -> &str {
    if SUPPORTED_MODELS.contains(&model) {
        model
    } else {
        "other"
    }
}

/// Static models list so OpenAI-style clients can populate a picker.
async fn models() -> impl IntoResponse {
    let data: Vec<serde_json::Value> = SUPPORTED_MODELS.iter().map(|id| model_object(id)).collect();
    Json(json!({ "object": "list", "data": data }))
}

/// OpenAI-compatible "retrieve model" (`GET /v1/models/{id}`). Codex clients
/// (e.g. cyrus's `CodexRunner`) probe this to validate a configured model before
/// a run; without it the 404 makes them fall back to an unsupported default
/// (`gpt-5.2-codex`), which the ChatGPT-account upstream then rejects. Return
/// the same object as the list endpoint for known ids, else a 404 with the
/// canonical `model_not_found` error shape.
async fn model_by_id(Path(model): Path<String>) -> Response {
    if SUPPORTED_MODELS.contains(&model.as_str()) {
        return Json(model_object(&model)).into_response();
    }

    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": {
                "message": format!("Model '{model}' not found"),
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found",
            }
        })),
    )
        .into_response()
}

/// Passthrough to the Codex Responses API. Streams the upstream response back
/// to the client as-is (preserving SSE).
async fn responses(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    let fwd = state.forward_with_fallback(body, &headers).await?;
    let upstream = fwd.response;

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    // Captured before `tee_responses` consumes `upstream` below.
    let upstream_headers = upstream.headers().clone();

    // Forward verbatim, but tee the SSE for token usage so this passthrough —
    // the path the real Codex CLI uses — is attributed too. Model isn't parsed
    // here (the body may be many MB); `-` marks "raw passthrough".
    let mut log = CompletionLog::new(ctx, "/v1/responses", "-", "-", state.metrics.clone());
    log.set_account(fwd.account);
    let stream = tee_responses(upstream, log, state.config.server.max_body_bytes);

    let mut response = Response::builder()
        .status(status)
        .body(Body::from_stream(stream))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build response: {e}")).into_response()
        });

    // Preserve Content-Type (SSE vs JSON) plus the verified Codex
    // session-continuity headers (see `CODEX_SESSION_RESPONSE_HEADERS`) so
    // the real Codex CLI can replay them on its next request. An explicit
    // allowlist rather than "relay everything except a few" — reqwest's
    // gzip/br/zstd decoding may or may not strip `Content-Encoding` from an
    // encoded upstream response, and blindly relaying it over an
    // already-decoded body would corrupt it for the client either way.
    let out_headers = response.headers_mut();
    let content_type = upstream_headers
        .get(axum::http::header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| axum::http::HeaderValue::from_static("application/json"));
    out_headers.insert(axum::http::header::CONTENT_TYPE, content_type);
    for name in crate::upstream::CODEX_SESSION_RESPONSE_HEADERS {
        if let Some(value) = upstream_headers.get(*name) {
            out_headers.insert(axum::http::HeaderName::from_static(name), value.clone());
        }
    }

    Ok(response)
}

/// OpenAI-compatible `/v1/chat/completions`: translate to the Responses API,
/// forward, then translate the response back (streaming or buffered).
async fn chat_completions(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    let req: ChatCompletionRequest = serde_json::from_slice(&body)
        .map_err(|e| ProxyError::BadRequest(format!("invalid chat request: {e}")))?;
    let client_wants_stream = req.stream.unwrap_or(false);
    let echo_model = req.model.clone();
    let defaults = state.config.defaults.clone();
    let mut log = CompletionLog::new(
        ctx,
        "/v1/chat/completions",
        echo_model.clone(),
        metric_model_label(&echo_model),
        state.metrics.clone(),
    );

    let codex_body = build_codex_request(&req, &defaults);
    let bytes = serde_json::to_vec(&codex_body)
        .map_err(|e| ProxyError::Internal(format!("serialize codex request: {e}")))?;

    let fwd = state.forward_with_fallback(bytes.into(), &headers).await?;
    log.set_account(fwd.account);
    let upstream = fwd.response;

    // Pass upstream failures straight through with their real status, body, and
    // content-type — so a 401/429/400 from Codex reaches the client unchanged
    // instead of being flattened to a generic 502.
    if !upstream.status().is_success() {
        log.emit(upstream.status().as_u16(), None);
        return Ok(passthrough_response(upstream, state.config.server.max_body_bytes).await);
    }

    let max_event_bytes = state.config.server.max_body_bytes;
    if client_wants_stream {
        // The stream owns `log` and emits the completion line (with usage) when
        // it finishes draining.
        let stream = stream_chat(upstream, echo_model, defaults, log, max_event_bytes);
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|e| {
                ProxyError::Internal(format!("failed to build stream response: {e}"))
                    .into_response()
            }))
    } else {
        let json = collect_chat(upstream, echo_model, defaults, max_event_bytes).await?;
        log.emit(StatusCode::OK.as_u16(), usage_pair(&json));
        Ok(Json(json).into_response())
    }
}

/// Pull `(prompt_tokens, completion_tokens)` from a buffered chat.completion
/// body for the access log, or `None` if the usage block is missing.
fn usage_pair(chat: &serde_json::Value) -> Option<(i64, i64)> {
    let usage = chat.get("usage")?;
    Some((
        usage.get("prompt_tokens").and_then(|v| v.as_i64())?,
        usage.get("completion_tokens").and_then(|v| v.as_i64())?,
    ))
}

/// Relay an upstream `reqwest::Response` to the client verbatim: same status
/// code, same content-type, same body. Used for error responses and the raw
/// `/v1/responses` passthrough so client-visible semantics aren't altered.
/// `max_body_bytes` bounds how much of an upstream error body this reads —
/// `reqwest::Response::bytes()` has no cap of its own, and this is a
/// non-success status from an account or fallback provider (external, only
/// somewhat trusted), so an enormous error body shouldn't buffer without
/// limit. A truncated read still gets the client something close to the real
/// error rather than nothing.
async fn passthrough_response(upstream: reqwest::Response, max_body_bytes: usize) -> Response {
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let mut body = Vec::new();
    let mut stream = upstream.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        body.extend_from_slice(&chunk);
        if body.len() > max_body_bytes {
            // Truncate exactly to the cap rather than however much this one
            // chunk happened to add past it — a single large chunk (chunking
            // is transport-dependent, not something to rely on for the bound
            // itself) must not be able to exceed the limit either.
            tracing::warn!(
                bytes = body.len(),
                limit = max_body_bytes,
                "upstream error body exceeded limit; truncating passthrough"
            );
            body.truncate(max_body_bytes);
            break;
        }
    }

    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .body(Body::from(body))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build passthrough response: {e}"))
                .into_response()
        })
}

/// Validate the client's `Authorization: Bearer <key>` against the configured
/// key list and return a non-secret label for the matched key (configured name
/// or fingerprint), used to attribute token spend in the access log. When
/// `client_auth.require = false`, auth is skipped and everyone is `anonymous`.
fn check_client_auth(config: &Config, headers: &HeaderMap) -> Result<String, ProxyError> {
    if !config.client_auth.require {
        return Ok("anonymous".to_string());
    }

    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);

    match presented {
        Some(key) => match key_matches(&config.client_auth.keys, key) {
            Some(matched) => Ok(observe::key_label(key, matched.name.as_deref())),
            None => Err(ProxyError::Unauthorized("invalid API key".into())),
        },
        None => Err(ProxyError::Unauthorized(
            "missing Authorization: Bearer <key> header".into(),
        )),
    }
}

/// Compare `presented` against every configured key in constant time — every
/// key is checked unconditionally (no early return) so neither the byte
/// comparison nor the list iteration leaks timing about how much of a key
/// matched. Returns the last matching entry (there should only ever be one).
fn key_matches<'a>(keys: &'a [ClientKey], presented: &str) -> Option<&'a ClientKey> {
    let mut matched = None;
    for k in keys {
        if key_secret_matches(&k.key, presented) {
            matched = Some(k);
        }
    }
    matched
}

/// Whether `presented` satisfies a configured secret, which is either a raw
/// key (direct constant-time compare) or a `sha256:<hex>` digest (hash
/// `presented` and compare digests) — see `ClientKey::key`.
fn key_secret_matches(secret: &str, presented: &str) -> bool {
    match secret.strip_prefix("sha256:") {
        Some(hex_digest) => {
            let Some(expected) = decode_hex(hex_digest) else {
                return false;
            };
            let computed = Sha256::digest(presented.as_bytes());
            constant_time_eq(&computed, &expected)
        }
        None => constant_time_eq(secret.as_bytes(), presented.as_bytes()),
    }
}

/// Decode a hex string into bytes, or `None` if it's malformed (odd length or
/// non-hex characters) — a misconfigured digest should fail to match, not panic.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Length-independent constant-time byte equality.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Fold the length difference into the accumulator so unequal lengths never
    // short-circuit; the loop runs over max(len) with wrap-around indexing.
    let mut diff = (a.len() ^ b.len()) as u8;
    let n = a.len().max(b.len()).max(1);
    for i in 0..n {
        let x = a.get(i % a.len().max(1)).copied().unwrap_or(0);
        let y = b.get(i % b.len().max(1)).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0 && a.len() == b.len()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{HeaderMap, Request as HttpRequest, StatusCode};
    use axum::response::Response;
    use axum::routing::post;
    use axum::Router;
    use bytes::Bytes;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    use super::{
        constant_time_eq, key_matches, key_secret_matches, metrics_router, router, AppState,
    };
    use crate::auth::AuthManager;
    use crate::config::{ClientKey, Config};
    use crate::fallback::FallbackChain;
    use crate::metrics::Metrics;
    use crate::test_support::write_test_auth_json;
    use crate::upstream::Upstream;

    fn bare_key(key: &str) -> ClientKey {
        ClientKey {
            key: key.to_string(),
            name: None,
        }
    }

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"secret", b"Secret"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn key_matches_any_configured_key() {
        let keys = vec![bare_key("k1"), bare_key("k2")];
        assert!(key_matches(&keys, "k2").is_some());
        assert!(key_matches(&keys, "k3").is_none());
        assert!(key_matches(&[], "anything").is_none());
    }

    #[test]
    fn key_secret_matches_sha256_digest() {
        let digest = format!("sha256:{}", hex_encode(&Sha256::digest(b"the-real-key")));
        assert!(key_secret_matches(&digest, "the-real-key"));
        assert!(!key_secret_matches(&digest, "wrong-key"));
        assert!(!key_secret_matches("sha256:not-hex!!", "anything")); // malformed hex doesn't panic
        assert!(!key_secret_matches("sha256:ab", "anything")); // too-short digest doesn't match
    }

    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[tokio::test]
    async fn unauthenticated_large_body_is_rejected_before_body_limit() {
        let app = test_router(8);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .body(Body::from(vec![b'a'; 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();

        // This must be 401, not 413: auth should run before `Bytes` buffers and
        // applies the configured request-body limit.
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn authenticated_large_body_still_hits_body_limit() {
        let app = test_router(8);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::from(vec![b'a'; 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn models_list_and_retrieve_agree_for_known_model() {
        let app = test_router(8);

        let list = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let list: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(list.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let listed = &list["data"][0];
        assert_eq!(listed["id"], "gpt-5.5");

        let retrieved = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models/gpt-5.5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retrieved.status(), StatusCode::OK);
        let retrieved: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(retrieved.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        // Retrieve must mirror the list entry, so model-validating clients accept it.
        assert_eq!(&retrieved, listed);
    }

    #[tokio::test]
    async fn retrieve_unknown_model_is_404() {
        let app = test_router(8);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models/gpt-5.2-codex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["error"]["code"], "model_not_found");
    }

    #[test]
    fn metric_model_label_clamps_unrecognized_client_input() {
        // A metric label must be bounded regardless of what a client sends —
        // otherwise arbitrary `model` strings would mint unbounded Prometheus
        // time series (see codexproxy_requests_total{model=...}).
        assert_eq!(super::metric_model_label("gpt-5.5"), "gpt-5.5");
        assert_eq!(
            super::metric_model_label("literally-anything-a-client-sends"),
            "other"
        );
        assert_eq!(super::metric_model_label(""), "other");
    }

    #[tokio::test]
    async fn chat_completions_forwards_to_upstream_and_collects_response() {
        let upstream_body = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}\n\n",
            "data: [DONE]\n\n"
        );
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", upstream_body).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-5-codex",
                            "messages": [
                                { "role": "system", "content": "be concise" },
                                { "role": "user", "content": "hi" }
                            ],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["object"], "chat.completion");
        assert_eq!(body["choices"][0]["message"]["content"], "Hello");
        assert_eq!(body["usage"]["prompt_tokens"], 3);
        assert_eq!(body["usage"]["completion_tokens"], 2);
        assert_eq!(body["usage"]["total_tokens"], 5);

        let captured = fake.recv().await;
        assert!(captured
            .authorization
            .as_deref()
            .is_some_and(|v| v.starts_with("Bearer ")));
        assert_eq!(captured.account_id.as_deref(), Some("acct_test"));
        assert_eq!(captured.originator.as_deref(), Some("codex_cli_rs"));
        assert_eq!(captured.accept.as_deref(), Some("text/event-stream"));

        let upstream_request: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(upstream_request["model"], "gpt-5-codex");
        assert_eq!(upstream_request["instructions"], "be concise");
        assert_eq!(upstream_request["input"][0]["role"], "user");
        assert_eq!(upstream_request["input"][0]["content"], "hi");
        assert_eq!(upstream_request["stream"], true);
        assert_eq!(upstream_request["store"], false);
        assert_eq!(upstream_request["reasoning"]["effort"], "medium");
    }

    #[tokio::test]
    async fn chat_completions_streaming_passes_through_and_finishes() {
        // Exercises the streaming path that carries the CompletionLog into
        // stream_chat: the SSE must reach the client with content + [DONE].
        let upstream_body = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":1}}}\n\n",
            "data: [DONE]\n\n"
        );
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", upstream_body).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-5-codex",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("chat.completion.chunk"));
        assert!(body.contains("\"content\":\"Hi\""));
        assert!(body.contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn responses_endpoint_passthrough_preserves_status_content_type_and_body() {
        let fake =
            start_fake_upstream(StatusCode::ACCEPTED, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"input":"raw"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":true}"#);

        let captured = fake.recv().await;
        assert_eq!(captured.account_id.as_deref(), Some("acct_test"));
        assert_eq!(captured.content_type.as_deref(), Some("application/json"));
        assert_eq!(&captured.body[..], br#"{"input":"raw"}"#);
    }

    #[tokio::test]
    async fn responses_endpoint_relays_codex_session_headers_both_ways() {
        let fake = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .header("x-codex-turn-state", "client-turn-token")
                    .body(Body::from(r#"{"input":"raw"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The real Codex CLI's own continuity header (request side) is
        // relayed to the upstream, and the upstream's fresh token (response
        // side) is relayed back down to the client — so a real Codex CLI's
        // sticky-routing state survives being routed through this proxy.
        assert_eq!(
            response
                .headers()
                .get("x-codex-turn-state")
                .and_then(|v| v.to_str().ok()),
            Some("server-issued-token")
        );

        let captured = fake.recv().await;
        assert_eq!(captured.turn_state.as_deref(), Some("client-turn-token"));
    }

    fn test_router(max_body_bytes: usize) -> axum::Router {
        test_router_with_upstream(max_body_bytes, Config::default().upstream.base_url)
    }

    fn test_router_with_upstream(max_body_bytes: usize, upstream_base_url: String) -> axum::Router {
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.server.max_body_bytes = max_body_bytes;
        config.upstream.base_url = upstream_base_url;

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback = Arc::new(
            FallbackChain::new(http, &config.fallback).expect("build empty fallback chain"),
        );
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics,
        })
    }

    /// Like `test_router_with_upstream`, but with a configurable fallback
    /// chain instead of an always-empty one — for testing the
    /// pool-exhausted-falls-over-to-fallback composition in `AppState`.
    fn test_router_with_fallback(
        upstream_base_url: String,
        fallback_cfgs: Vec<crate::config::FallbackProviderConfig>,
    ) -> axum::Router {
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = upstream_base_url;
        config.fallback = fallback_cfgs;

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics,
        })
    }

    fn fallback_provider_cfg(name: &str, base_url: &str) -> crate::config::FallbackProviderConfig {
        crate::config::FallbackProviderConfig {
            name: name.to_string(),
            base_url: base_url.to_string(),
            responses_path: "/responses".to_string(),
            auth_style: "bearer".to_string(),
            api_key: "fallback-key".to_string(),
            model_map: [("gpt-5.5".to_string(), "gpt-5.5-on-fallback".to_string())]
                .into_iter()
                .collect(),
        }
    }

    struct FakeUpstream {
        base_url: String,
        rx: mpsc::Receiver<CapturedUpstreamRequest>,
    }

    impl FakeUpstream {
        async fn recv(mut self) -> CapturedUpstreamRequest {
            self.rx.recv().await.expect("fake upstream request")
        }
    }

    #[derive(Clone)]
    struct FakeUpstreamState {
        tx: mpsc::Sender<CapturedUpstreamRequest>,
        status: StatusCode,
        content_type: &'static str,
        body: &'static str,
    }

    struct CapturedUpstreamRequest {
        authorization: Option<String>,
        account_id: Option<String>,
        originator: Option<String>,
        accept: Option<String>,
        content_type: Option<String>,
        turn_state: Option<String>,
        body: Bytes,
    }

    async fn start_fake_upstream(
        status: StatusCode,
        content_type: &'static str,
        body: &'static str,
    ) -> FakeUpstream {
        let (tx, rx) = mpsc::channel(1);
        let state = FakeUpstreamState {
            tx,
            status,
            content_type,
            body,
        };
        // Registers both the ChatGPT-pool path and the fallback-provider
        // default path (`FallbackProviderConfig::responses_path`'s default)
        // on the same handler, so this one fixture doubles as either a pool
        // upstream or a fallback provider in tests.
        let app = Router::new()
            .route("/codex/responses", post(fake_responses))
            .route("/responses", post(fake_responses))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        FakeUpstream {
            base_url: format!("http://{addr}"),
            rx,
        }
    }

    async fn fake_responses(
        axum::extract::State(state): axum::extract::State<FakeUpstreamState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let header = |name| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let captured = CapturedUpstreamRequest {
            authorization: header("authorization"),
            account_id: header("chatgpt-account-id"),
            originator: header("originator"),
            accept: header("accept"),
            content_type: header("content-type"),
            turn_state: header("x-codex-turn-state"),
            body,
        };
        state.tx.send(captured).await.unwrap();

        // A fixed reply header, unrelated to what the request carried — lets
        // tests assert the *response* side of header relaying independently
        // of the request side.
        Response::builder()
            .status(state.status)
            .header("Content-Type", state.content_type)
            .header("x-codex-turn-state", "server-issued-token")
            .body(Body::from(state.body))
            .unwrap()
    }

    fn responses_request(body: &'static str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn pool_success_never_touches_fallback() {
        let pool = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let mut fallback_fake =
            start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let _ = pool.recv().await;
        assert!(
            fallback_fake.rx.try_recv().is_err(),
            "fallback must not be touched when the pool succeeds"
        );
    }

    #[tokio::test]
    async fn pool_exhausted_falls_over_to_configured_provider() {
        let pool = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            r#"{"error":"rate limited"}"#,
        )
        .await;
        let fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);

        let pool_req = pool.recv().await;
        let pool_body: serde_json::Value = serde_json::from_slice(&pool_req.body).unwrap();
        assert_eq!(pool_body["model"], "gpt-5.5");

        let fallback_req = fallback_fake.recv().await;
        let fallback_body: serde_json::Value = serde_json::from_slice(&fallback_req.body).unwrap();
        assert_eq!(fallback_body["model"], "gpt-5.5-on-fallback");
    }

    #[tokio::test]
    async fn pool_5xx_also_falls_over_to_configured_provider() {
        // A whole-service Codex outage returns 5xx to every pool account
        // alike — not an *account* failure (`is_account_failure` is
        // 401/403/429 only), but exactly the case a fallback matters most
        // for. The outer gate must not be narrower than that.
        let pool = start_fake_upstream(
            StatusCode::SERVICE_UNAVAILABLE,
            "application/json",
            r#"{"error":"upstream down"}"#,
        )
        .await;
        let fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);

        let _ = pool.recv().await;
        let _ = fallback_fake.recv().await;
    }

    #[tokio::test]
    async fn unmapped_fallback_provider_is_skipped_client_sees_original_failure() {
        let pool = start_fake_upstream(
            StatusCode::FORBIDDEN,
            "application/json",
            r#"{"error":"banned"}"#,
        )
        .await;
        let mut fallback_fake =
            start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let mut cfg = fallback_provider_cfg("fb", &fallback_fake.base_url);
        cfg.model_map.clear();
        let app = test_router_with_fallback(pool.base_url.clone(), vec![cfg]);

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"banned"}"#);

        let _ = pool.recv().await;
        assert!(
            fallback_fake.rx.try_recv().is_err(),
            "a provider without a mapping for the requested model must never be called"
        );
    }

    #[tokio::test]
    async fn empty_fallback_chain_matches_pre_fallback_behavior() {
        let pool = start_fake_upstream(
            StatusCode::FORBIDDEN,
            "application/json",
            r#"{"error":"banned"}"#,
        )
        .await;
        let app = test_router_with_fallback(pool.base_url.clone(), vec![]);

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"banned"}"#);
    }

    #[tokio::test]
    async fn pool_transport_failure_falls_over_to_configured_provider() {
        let fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        // Nothing listens on port 1 on loopback — a connection attempt here
        // fails fast (refused), so the pool's only account errors out with a
        // transport error, not just a bad HTTP status.
        let app = test_router_with_fallback(
            "http://127.0.0.1:1".to_string(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);

        let _ = fallback_fake.recv().await;
    }

    #[tokio::test]
    async fn metrics_endpoint_reports_a_request_made_through_the_main_router() {
        let fake = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = fake.base_url.clone();

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        // The same Arc<Metrics> backs both routers, mirroring how main.rs
        // shares one Metrics instance between the client-facing API and the
        // separate metrics-port router.
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics: metrics.clone(),
        });
        let metrics_app = metrics_router(metrics);

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // `/v1/responses` streams the body; `emit()` (and so the metrics
        // recording) only fires once the SSE stream fully drains, so the
        // response must actually be read here, not just status-checked.
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let _ = fake.recv().await;

        let metrics_response = metrics_app
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("codexproxy_requests_total"));
        assert!(text.contains(r#"endpoint="/v1/responses""#));
        assert!(text.contains(r#"status="200""#));
    }

    #[tokio::test]
    async fn metrics_endpoint_clamps_an_unrecognized_client_supplied_model() {
        let upstream_body = concat!(
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
            "data: [DONE]\n\n"
        );
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", upstream_body).await;
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = fake.base_url.clone();

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics: metrics.clone(),
        });
        let metrics_app = metrics_router(metrics);

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "totally-unrecognized-client-supplied-garbage",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let _ = fake.recv().await;

        let metrics_response = metrics_app
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains(r#"model="other""#));
        assert!(!text.contains("totally-unrecognized-client-supplied-garbage"));
    }

    #[tokio::test]
    async fn buffered_chat_completions_errors_when_a_single_sse_event_exceeds_max_body_bytes() {
        // A single "data: ..." line with no "\n\n" anywhere — an event
        // boundary never arrives, the pathological case the byte cap guards
        // against. `Box::leak` just to get a genuine `&'static str` at
        // runtime for the fixture, which only accepts a literal-shaped body.
        let body: &'static str = Box::leak(format!("data: {}", "x".repeat(300)).into_boxed_str());
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", body).await;
        let app = test_router_with_upstream(200, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-5.5",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn responses_passthrough_still_forwards_full_body_when_usage_scanner_gives_up() {
        // Same pathological body as above, but through the raw `/v1/responses`
        // passthrough: the usage scanner is a side channel (see
        // `tee_responses`'s docs) — giving up on token attribution must NOT
        // truncate or otherwise affect what the client actually receives.
        let body: &'static str = Box::leak(format!("data: {}", "x".repeat(300)).into_boxed_str());
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", body).await;
        let app = test_router_with_upstream(200, fake.base_url.clone());

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-5.5","input":"hi"}"#))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let received = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&received[..], body.as_bytes());
    }

    #[tokio::test]
    async fn chat_completions_error_passthrough_truncates_an_oversized_upstream_error_body() {
        // A non-success status with a huge error body — `reqwest::Response::
        // bytes()` (the old implementation) has no cap of its own, so this
        // must be bounded the same way the success-path SSE buffers are.
        let body: &'static str = Box::leak("x".repeat(500).into_boxed_str());
        let fake = start_fake_upstream(StatusCode::BAD_REQUEST, "application/json", body).await;
        let app = test_router_with_upstream(200, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-5.5",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The real upstream status/content-type still pass through unchanged
        // — only the body length is bounded, not the error's meaning.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let received = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(received.len(), 200, "must truncate exactly to the cap");
        assert!(received.len() < body.len());
    }
}
