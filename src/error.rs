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

    #[error("authentication with upstream failed: {0}")]
    UpstreamAuth(String),

    #[error("upstream request failed: {0}")]
    Upstream(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl ProxyError {
    fn parts(&self) -> (StatusCode, &'static str) {
        match self {
            ProxyError::BadRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
            ProxyError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "invalid_request_error"),
            ProxyError::UpstreamAuth(_) => (StatusCode::BAD_GATEWAY, "authentication_error"),
            ProxyError::Upstream(_) => (StatusCode::BAD_GATEWAY, "upstream_error"),
            ProxyError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        }
    }
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, kind) = self.parts();
        let message = self.to_string();
        tracing::warn!(%status, error = %message, "request failed");
        let body = json!({
            "error": {
                "message": message,
                "type": kind,
            }
        });
        (status, Json(body)).into_response()
    }
}
