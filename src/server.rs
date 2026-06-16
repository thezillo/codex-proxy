//! HTTP server: client API-key auth + OpenAI-compatible endpoints.
//!
//! `/v1/chat/completions` translates OpenAI Chat Completions to/from the Codex
//! Responses API (streaming and buffered). `/v1/responses` is a raw passthrough
//! for clients that already speak the `responses` wire format (e.g. codex
//! itself). Upstream errors are relayed with their original status and body.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::TryStreamExt;
use serde_json::json;

use crate::config::Config;
use crate::error::ProxyError;
use crate::translate::{build_codex_request, collect_chat, stream_chat, ChatCompletionRequest};
use crate::upstream::Upstream;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub upstream: Arc<Upstream>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
        .route("/v1/responses", post(responses))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Minimal static models list so OpenAI-style clients can populate a picker.
async fn models() -> impl IntoResponse {
    Json(json!({
        "object": "list",
        "data": [
            { "id": "gpt-5-codex", "object": "model", "owned_by": "openai" },
            { "id": "gpt-5", "object": "model", "owned_by": "openai" },
        ]
    }))
}

/// Passthrough to the Codex Responses API. Streams the upstream response back
/// to the client as-is (preserving SSE).
async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    check_client_auth(&state.config, &headers)?;

    let upstream = state.upstream.forward_responses(body).await?;

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);

    // Preserve content-type so SSE vs JSON is signalled correctly to the client.
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let stream = upstream.bytes_stream().map_err(std::io::Error::other);

    Ok(Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .body(Body::from_stream(stream))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build response: {e}")).into_response()
        }))
}

/// OpenAI-compatible `/v1/chat/completions`: translate to the Responses API,
/// forward, then translate the response back (streaming or buffered).
async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    check_client_auth(&state.config, &headers)?;

    let req: ChatCompletionRequest = serde_json::from_slice(&body)
        .map_err(|e| ProxyError::BadRequest(format!("invalid chat request: {e}")))?;
    let client_wants_stream = req.stream.unwrap_or(false);
    let echo_model = req.model.clone();
    let defaults = state.config.defaults.clone();

    let codex_body = build_codex_request(&req, &defaults);
    let bytes = serde_json::to_vec(&codex_body)
        .map_err(|e| ProxyError::Internal(format!("serialize codex request: {e}")))?;

    let upstream = state.upstream.forward_responses(bytes.into()).await?;

    // Pass upstream failures straight through with their real status, body, and
    // content-type — so a 401/429/400 from Codex reaches the client unchanged
    // instead of being flattened to a generic 502.
    if !upstream.status().is_success() {
        return Ok(passthrough_response(upstream).await);
    }

    if client_wants_stream {
        let stream = stream_chat(upstream, echo_model, defaults);
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
        let json = collect_chat(upstream, echo_model, defaults).await?;
        Ok(Json(json).into_response())
    }
}

/// Relay an upstream `reqwest::Response` to the client verbatim: same status
/// code, same content-type, same body. Used for error responses and the raw
/// `/v1/responses` passthrough so client-visible semantics aren't altered.
async fn passthrough_response(upstream: reqwest::Response) -> Response {
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let body = upstream.bytes().await.unwrap_or_default();
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
/// key list. No-op when `client_auth.require = false`.
fn check_client_auth(config: &Config, headers: &HeaderMap) -> Result<(), ProxyError> {
    if !config.client_auth.require {
        return Ok(());
    }

    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);

    match presented {
        Some(key) if key_matches(&config.client_auth.keys, key) => Ok(()),
        Some(_) => Err(ProxyError::Unauthorized("invalid API key".into())),
        None => Err(ProxyError::Unauthorized(
            "missing Authorization: Bearer <key> header".into(),
        )),
    }
}

/// Compare `presented` against each configured key in constant time, checking
/// every key unconditionally so neither the byte comparison nor the list
/// iteration leaks timing about how much of a key matched.
fn key_matches(keys: &[String], presented: &str) -> bool {
    let mut matched = false;
    for k in keys {
        matched |= constant_time_eq(k.as_bytes(), presented.as_bytes());
    }
    matched
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
    use super::{constant_time_eq, key_matches};

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
        let keys = vec!["k1".to_string(), "k2".to_string()];
        assert!(key_matches(&keys, "k2"));
        assert!(!key_matches(&keys, "k3"));
        assert!(!key_matches(&[], "anything"));
    }
}
