//! HTTP server: client API-key auth + OpenAI-compatible endpoints.
//!
//! `/v1/chat/completions` translates OpenAI Chat Completions to/from the Codex
//! Responses API (streaming and buffered). `/v1/responses` is a raw passthrough
//! for clients that already speak the `responses` wire format (e.g. codex
//! itself). Upstream errors are relayed with their original status and body.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
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
    // Lift Axum's 2 MB default body cap to the configured limit so large
    // contexts / base64 images aren't 413'd before we can proxy them.
    let body_limit = state.config.server.max_body_bytes;
    let auth_layer = middleware::from_fn_with_state(state.config.clone(), require_client_auth);

    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
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

/// Authenticate before any handler body extractor runs. The POST endpoints
/// accept potentially-large JSON bodies, so doing this inside the handler would
/// let unauthenticated clients force buffering up to `max_body_bytes`.
async fn require_client_auth(
    State(config): State<Arc<Config>>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, ProxyError> {
    check_client_auth(&config, &headers)?;
    Ok(next.run(request).await)
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
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
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
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
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
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{HeaderMap, Request as HttpRequest, StatusCode};
    use axum::response::Response;
    use axum::routing::post;
    use axum::Router;
    use base64::Engine;
    use bytes::Bytes;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    use super::{constant_time_eq, key_matches, router, AppState};
    use crate::auth::AuthManager;
    use crate::config::Config;
    use crate::upstream::Upstream;

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

    fn test_router(max_body_bytes: usize) -> axum::Router {
        test_router_with_upstream(max_body_bytes, Config::default().upstream.base_url)
    }

    fn test_router_with_upstream(max_body_bytes: usize, upstream_base_url: String) -> axum::Router {
        let mut config = Config::default();
        config.client_auth.keys = vec!["test-key".to_string()];
        config.server.max_body_bytes = max_body_bytes;
        config.upstream.base_url = upstream_base_url;

        let http = reqwest::Client::new();
        let auth = AuthManager::load(&config.upstream, write_test_auth_json(), http.clone())
            .expect("load test auth");
        let upstream = Arc::new(Upstream::new(&config.upstream, http, auth));

        router(AppState {
            config: Arc::new(config),
            upstream,
        })
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
        let app = Router::new()
            .route("/codex/responses", post(fake_responses))
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
            body,
        };
        state.tx.send(captured).await.unwrap();

        Response::builder()
            .status(state.status)
            .header("Content-Type", state.content_type)
            .body(Body::from(state.body))
            .unwrap()
    }

    fn write_test_auth_json() -> PathBuf {
        let codex_home = unique_temp_dir();
        fs::create_dir_all(&codex_home).unwrap();

        let access_token = unsigned_jwt(json!({ "exp": 4_102_444_800_i64 }));
        let id_token = unsigned_jwt(json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct_test"
            }
        }));
        let auth_json = json!({
            "tokens": {
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": "refresh_test"
            }
        });
        fs::write(
            codex_home.join("auth.json"),
            serde_json::to_vec(&auth_json).unwrap(),
        )
        .unwrap();

        codex_home
    }

    fn unsigned_jwt(payload: serde_json::Value) -> String {
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = engine.encode(br#"{"alg":"none"}"#);
        let payload = engine.encode(serde_json::to_vec(&payload).unwrap());
        format!("{header}.{payload}.signature")
    }

    fn unique_temp_dir() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "codex-proxy-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }
}
