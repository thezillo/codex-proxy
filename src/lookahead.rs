//! First-output lookahead on a pool's Responses SSE.
//!
//! Codex answers 200 and only then says, in an `error`/`response.failed`
//! event, that it is overloaded — or ends the stream without producing
//! anything. Relayed as is, that is a "successful" answer no failover ever
//! sees. So before anything reaches the client, the pool's stream is read
//! up to its first event that carries output: from there on it is
//! committed and replayed byte for byte; a failure or an empty stream before
//! that point becomes an ordinary 5xx pool response instead.

use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::{json, Value};

use crate::translate::stream::{extract_data, find_event_delimiter};
use crate::upstream::FailureReason;

/// The upstream's own words in an `error`/`response.failed` event, or why
/// none arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// Overloaded, throttled or out of capacity: 503.
    Overloaded,
    /// Any other upstream error, or a body that broke mid-read: 502.
    Error,
    /// The stream ended without producing anything: 502.
    Empty,
    /// Nothing decisive within the request timeout: 504.
    Timeout,
}

impl Failure {
    fn parts(self) -> (reqwest::StatusCode, &'static str, FailureReason) {
        match self {
            Failure::Overloaded => (
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
                "upstream_overloaded",
                FailureReason::Capacity,
            ),
            Failure::Error => (
                reqwest::StatusCode::BAD_GATEWAY,
                "upstream_error",
                FailureReason::Upstream5xx,
            ),
            Failure::Empty => (
                reqwest::StatusCode::BAD_GATEWAY,
                "empty_response",
                FailureReason::EmptyResponse,
            ),
            Failure::Timeout => (
                reqwest::StatusCode::GATEWAY_TIMEOUT,
                "upstream_timeout",
                FailureReason::Timeout,
            ),
        }
    }
}

enum Event {
    Metadata,
    Output,
    Failed(Failure, String),
}

/// The response the caller should treat as the pool's, plus the pool's
/// reading of it when the lookahead turned it into a failure.
pub struct Checked {
    pub response: reqwest::Response,
    pub reason: Option<FailureReason>,
}

/// Look ahead on a 2xx SSE response; anything else is returned untouched.
/// `max_bytes` bounds what is held back (past it, the stream is committed
/// unchecked) and `timeout` how long the first output may take.
pub async fn check_first_output(
    response: reqwest::Response,
    account: &str,
    max_bytes: usize,
    timeout: Duration,
) -> Checked {
    let is_sse = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/event-stream"));
    if !response.status().is_success() || !is_sse {
        return Checked {
            response,
            reason: None,
        };
    }

    let status = response.status();
    let version = response.version();
    let headers = response.headers().clone();
    let mut body = response.bytes_stream();
    let mut held: Vec<Bytes> = Vec::new();
    let mut held_bytes = 0usize;
    let mut scan: Vec<u8> = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout;

    let verdict = loop {
        let chunk = match tokio::time::timeout_at(deadline, body.next()).await {
            Err(_) => {
                break Event::Failed(
                    Failure::Timeout,
                    format!("no output from upstream within {}s", timeout.as_secs()),
                )
            }
            Ok(None) => break end_of_stream(&scan),
            Ok(Some(Err(e))) => {
                break Event::Failed(Failure::Error, format!("upstream stream read: {e}"))
            }
            Ok(Some(Ok(chunk))) => chunk,
        };
        held_bytes += chunk.len();
        scan.extend_from_slice(&chunk);
        held.push(chunk);

        let mut decided = None;
        while let Some((pos, dlen)) = find_event_delimiter(&scan) {
            let block: Vec<u8> = scan.drain(..pos + dlen).collect();
            match classify_block(&block) {
                Event::Metadata => {}
                other => {
                    decided = Some(other);
                    break;
                }
            }
        }
        if let Some(event) = decided {
            break event;
        }
        if held_bytes > max_bytes {
            tracing::warn!(
                %account,
                held_bytes,
                limit = max_bytes,
                "no output within the lookahead limit; relaying the stream unchecked"
            );
            break Event::Output;
        }
    };

    match verdict {
        Event::Output | Event::Metadata => {
            let replay = futures_util::stream::iter(held.into_iter().map(Ok)).chain(body);
            let mut rebuilt = axum::http::Response::new(reqwest::Body::wrap_stream(replay));
            *rebuilt.status_mut() = status;
            *rebuilt.version_mut() = version;
            *rebuilt.headers_mut() = headers;
            Checked {
                response: reqwest::Response::from(rebuilt),
                reason: None,
            }
        }
        Event::Failed(failure, message) => {
            let (status, code, reason) = failure.parts();
            tracing::warn!(
                %account,
                code,
                error = %crate::observe::truncate(&message),
                "pool stream failed before any output; treating it as a {status} pool response"
            );
            let body = json!({
                "error": { "message": message, "type": "server_error", "code": code }
            });
            let mut rebuilt = axum::http::Response::new(Bytes::from(body.to_string()));
            *rebuilt.status_mut() = status;
            rebuilt.headers_mut().insert(
                reqwest::header::CONTENT_TYPE,
                reqwest::header::HeaderValue::from_static("application/json"),
            );
            Checked {
                response: reqwest::Response::from(rebuilt),
                reason: Some(reason),
            }
        }
    }
}

/// A last event the upstream sent without its trailing blank line still
/// counts; otherwise the stream simply produced nothing.
fn end_of_stream(rest: &[u8]) -> Event {
    match classify_block(rest) {
        Event::Metadata => Event::Failed(
            Failure::Empty,
            "upstream stream ended without producing any output".into(),
        ),
        decided => decided,
    }
}

fn classify_block(block: &[u8]) -> Event {
    let block = String::from_utf8_lossy(block);
    let Some(data) = extract_data(&block) else {
        return Event::Metadata;
    };
    match serde_json::from_str::<Value>(&data) {
        Ok(evt) => classify(&evt),
        Err(_) => Event::Metadata,
    }
}

fn classify(evt: &Value) -> Event {
    let etype = evt.get("type").and_then(Value::as_str).unwrap_or("");
    let item_type = || evt.pointer("/item/type").and_then(Value::as_str);
    match etype {
        "error" | "response.failed" => {
            let (failure, message) = classify_error(evt);
            Event::Failed(failure, message)
        }
        "response.completed" | "response.incomplete" | "response.done" => {
            let produced = evt
                .pointer("/response/output")
                .and_then(Value::as_array)
                .is_some_and(|items| {
                    items
                        .iter()
                        .any(|i| i.get("type").and_then(Value::as_str) != Some("reasoning"))
                });
            if produced {
                Event::Output
            } else {
                Event::Failed(
                    Failure::Empty,
                    format!("upstream {etype} without any output"),
                )
            }
        }
        // An announced message or reasoning item has no content yet.
        "response.output_item.added" => match item_type() {
            Some("message" | "reasoning") | None => Event::Metadata,
            Some(_) => Event::Output,
        },
        "response.output_item.done" => match item_type() {
            Some("reasoning") | None => Event::Metadata,
            Some(_) => Event::Output,
        },
        t if t.ends_with(".delta")
            || t == "response.output_text.done"
            || t == "response.function_call_arguments.done" =>
        {
            Event::Output
        }
        _ => Event::Metadata,
    }
}

const OVERLOADED_CODES: &[&str] = &[
    "overloaded",
    "capacity",
    "rate_limit",
    "slow_down",
    "too_many_requests",
    "service_unavailable",
];

/// Codes too generic to rule out an overload the message describes.
const GENERIC_CODES: &[&str] = &["server_error", "internal_error", "api_error", "error"];

const OVERLOADED_PHRASES: &[&str] = &[
    "overloaded",
    "capacity",
    "rate limit",
    "too many requests",
    "try again later",
];

/// The error's code/type decide when present; its message is the fallback.
fn classify_error(evt: &Value) -> (Failure, String) {
    let field = |name: &str| {
        [
            format!("/error/{name}"),
            format!("/response/error/{name}"),
            format!("/{name}"),
        ]
        .iter()
        .find_map(|p| evt.pointer(p).and_then(Value::as_str))
        .map(str::to_string)
    };
    let message = field("message").unwrap_or_else(|| "upstream error".to_string());
    let code = field("code").unwrap_or_default().to_ascii_lowercase();
    let kind = evt
        .pointer("/error/type")
        .or_else(|| evt.pointer("/response/error/type"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let lower = message.to_ascii_lowercase();
    let code_says = |s: &str| OVERLOADED_CODES.iter().any(|c| s.contains(c));
    let generic = code.is_empty() || GENERIC_CODES.contains(&code.as_str());
    let overloaded = code_says(&code)
        || code_says(&kind)
        || (generic && OVERLOADED_PHRASES.iter().any(|p| lower.contains(p)));
    let failure = if overloaded {
        Failure::Overloaded
    } else {
        Failure::Error
    };
    (failure, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sse_response(body: &'static str) -> reqwest::Response {
        let mut r = axum::http::Response::new(Bytes::from_static(body.as_bytes()));
        r.headers_mut().insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("text/event-stream"),
        );
        reqwest::Response::from(r)
    }

    async fn check(body: &'static str) -> (u16, Option<FailureReason>, String) {
        let checked = check_first_output(
            sse_response(body),
            "acct",
            1024 * 1024,
            Duration::from_secs(5),
        )
        .await;
        let status = checked.response.status().as_u16();
        let text = checked.response.text().await.unwrap();
        (status, checked.reason, text)
    }

    #[tokio::test]
    async fn overloaded_first_event_is_a_503() {
        let (status, reason, body) = check(concat!(
            "data: {\"type\":\"response.created\",\"response\":{}}\n\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"server_error\",\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}\n\n",
        ))
        .await;
        assert_eq!(status, 503);
        assert_eq!(reason, Some(FailureReason::Capacity));
        let v: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["error"]["code"], "upstream_overloaded");
        assert_eq!(v["error"]["type"], "server_error");
        assert!(v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("overloaded"));
    }

    #[tokio::test]
    async fn overloaded_is_read_from_the_message_when_no_code() {
        let (status, _, _) = check(
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Our servers are currently overloaded.\"}}}\n\n",
        )
        .await;
        assert_eq!(status, 503);
    }

    #[tokio::test]
    async fn generic_code_with_an_overloaded_message_is_a_503() {
        let (status, _, _) = check(
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n",
        )
        .await;
        assert_eq!(status, 503);
    }

    #[tokio::test]
    async fn other_error_is_a_502() {
        let (status, reason, body) = check(
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"invalid_prompt\",\"message\":\"bad things\"}}}\n\n",
        )
        .await;
        assert_eq!(status, 502);
        assert_eq!(reason, Some(FailureReason::Upstream5xx));
        assert!(body.contains("\"upstream_error\""));
    }

    #[tokio::test]
    async fn empty_stream_is_a_502() {
        let (status, reason, body) = check(concat!(
            "data: {\"type\":\"response.created\",\"response\":{}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"message\"}}\n\n",
        ))
        .await;
        assert_eq!(status, 502);
        assert_eq!(reason, Some(FailureReason::EmptyResponse));
        assert!(body.contains("\"empty_response\""));

        let (status, _, _) =
            check("data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n").await;
        assert_eq!(status, 502);
    }

    #[tokio::test]
    async fn output_commits_and_replays_every_byte() {
        const BODY: &str = concat!(
            "data: {\"type\":\"response.created\",\"response\":{}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n",
            "data: {\"type\":\"error\",\"message\":\"later\"}\n\n",
        );
        let (status, reason, body) = check(BODY).await;
        assert_eq!(status, 200);
        assert_eq!(reason, None);
        assert_eq!(body, BODY);
    }

    #[tokio::test]
    async fn tool_call_counts_as_output() {
        let (status, _, _) = check(
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"c\"}}\n\n",
        )
        .await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn non_sse_is_untouched() {
        let r = reqwest::Response::from(axum::http::Response::new(Bytes::from_static(b"{}")));
        let checked = check_first_output(r, "acct", 1024, Duration::from_secs(1)).await;
        assert_eq!(checked.reason, None);
        assert_eq!(checked.response.text().await.unwrap(), "{}");
    }

    #[tokio::test]
    async fn past_the_limit_the_stream_is_committed_unchecked() {
        const BODY: &str = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"padding\":\"xxxxxxxxxxxxxxxx\"}}\n\n",
            "data: {\"type\":\"error\",\"message\":\"overloaded\"}\n\n",
        );
        let (first, second) = BODY.split_at(BODY.find("data: {\"type\":\"error").unwrap());
        let chunks = futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(first.as_bytes())),
            Ok(Bytes::from_static(second.as_bytes())),
        ]);
        let mut r = axum::http::Response::new(reqwest::Body::wrap_stream(chunks));
        r.headers_mut().insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("text/event-stream"),
        );
        let checked = check_first_output(
            reqwest::Response::from(r),
            "acct",
            first.len() - 1,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(checked.response.status().as_u16(), 200);
        assert_eq!(checked.response.text().await.unwrap(), BODY);
    }
}
