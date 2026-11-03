//! Unified error type, rendered as an OpenAI-style error JSON body.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("authentication with upstream failed: {0}")]
    UpstreamAuth(String),

    #[error("upstream request failed: {0}")]
    Upstream(String),

    #[error("internal error: {0}")]
    Internal(String),

    /// Answered to a `pool_only` client instead of trying the pool.
    #[error("subscription pool unavailable: {0}")]
    PoolUnavailable(String),

    /// A buffered completion that produced no content, tool call or reasoning.
    #[error("{0}")]
    EmptyResponse(String),

    /// A `pool_only` client asked for something only a paid provider serves.
    #[error("not available to a pool_only client: {0}")]
    PoolOnly(String),
}

impl ProxyError {
    /// The HTTP status this error answers with.
    pub fn status(&self) -> StatusCode {
        self.parts().0
    }

    /// `(status, type, code)`; `code` is only emitted when `Some`.
    fn parts(&self) -> (StatusCode, &'static str, Option<&'static str>) {
        match self {
            ProxyError::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request_error", None),
            ProxyError::Unauthorized(_) => {
                (StatusCode::UNAUTHORIZED, "invalid_request_error", None)
            }
            ProxyError::NotFound(_) => (StatusCode::NOT_FOUND, "invalid_request_error", None),
            ProxyError::UpstreamAuth(_) => (StatusCode::BAD_GATEWAY, "authentication_error", None),
            ProxyError::Upstream(_) => (StatusCode::BAD_GATEWAY, "upstream_error", None),
            ProxyError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", None),
            ProxyError::PoolUnavailable(_) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "server_error",
                Some("pool_unavailable"),
            ),
            ProxyError::EmptyResponse(_) => (
                StatusCode::BAD_GATEWAY,
                "server_error",
                Some("empty_response"),
            ),
            ProxyError::PoolOnly(_) => (
                StatusCode::FORBIDDEN,
                "invalid_request_error",
                Some("pool_only"),
            ),
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, kind, code) = self.parts();
        let message = self.to_string();
        tracing::warn!(%status, error = %message, "request failed");
        let mut body = json!({
            "error": {
                "message": message,
                "type": kind,
            }
        });
        if let Some(code) = code {
            body["error"]["code"] = json!(code);
        }
        (status, Json(body)).into_response()
    }
}
