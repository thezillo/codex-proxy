//! `/v1/responses` over WebSocket — the transport the Codex CLI uses for a
//! provider with `supports_websockets = true` (the built-in `openai` one
//! included, i.e. whenever `openai_base_url` points here).
//!
//! The socket is only the client side. Each `response.create` frame becomes
//! an ordinary `/v1/responses` HTTP request (`server::dispatch_responses`:
//! the same alias/model gates, account pool, downgrades and fallback chain),
//! and each SSE event of the answer goes back as one text frame. Nothing new
//! talks to the upstream, so its TLS fingerprint is unchanged.
//!
//! What the upstream socket would keep for us, this keeps itself: Codex sends
//! a follow-up turn as `previous_response_id` plus only the NEW input items,
//! relying on the server remembering the previous request's input and the
//! items it produced. We hold exactly that for the connection's last
//! completed response and rebuild the full input before forwarding. Any other
//! `previous_response_id` gets `previous_response_not_found`, on which Codex
//! resends the full request — slower, never wrong.

use std::collections::BTreeMap;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Map, Value};

use crate::observe::AccessCtx;
use crate::server::{dispatch_responses, AppState, Dispatched};
use crate::translate::sse_events;

/// `endpoint` label/field for requests that arrive over the socket, so they
/// stay distinguishable from the HTTP route in logs and metrics.
pub const WS_ENDPOINT: &str = "/v1/responses (ws)";

/// Upstream response headers worth handing to Codex inside an error frame:
/// it reads rate-limit state and `retry-after` from them, as it would from
/// an HTTP error.
fn is_relayed_error_header(name: &str) -> bool {
    name.starts_with("x-codex-") || name == "retry-after"
}

/// What the upstream socket would remember about the last completed response.
/// Kept as JSON text, never as a parsed tree: a long session's history is
/// most of `max_body_bytes`, and a `Value` tree of it costs several times its
/// size — the same reason `translate::rewrite_model` splices bytes.
struct Continuation {
    response_id: String,
    /// The full input that request was sent with, as the text between the
    /// array's brackets.
    input: String,
    /// Its `response.output_item.done` items, in order.
    output: Vec<Box<RawValue>>,
}

/// A client frame, split into its top-level members only. Every value stays
/// raw text, so the (possibly multi-MB) `input` is never parsed into a tree.
type Frame = BTreeMap<String, Box<RawValue>>;

/// The events the continuation is built from and those that end a turn, and
/// only the fields needed; serde skips the rest without building anything.
#[derive(Deserialize)]
struct TrackedEvent {
    #[serde(rename = "type")]
    kind: String,
    item: Option<Box<RawValue>>,
    response: Option<ResponseId>,
}

#[derive(Deserialize)]
struct ResponseId {
    id: Option<String>,
}

pub async fn upgrade(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let max_frame = state.config.server.max_body_bytes;
    ws.max_message_size(max_frame)
        .max_frame_size(max_frame)
        .on_upgrade(move |socket| serve(state, ctx, headers, socket))
        .into_response()
}

async fn serve(state: AppState, ctx: AccessCtx, headers: HeaderMap, mut socket: WebSocket) {
    let mut last: Option<Continuation> = None;
    // Frames are handled strictly one at a time, like the upstream socket:
    // Codex never sends a second `response.create` before the first one's
    // `response.completed` (or error).
    loop {
        let message = match socket.recv().await {
            Some(Ok(message)) => message,
            Some(Err(e)) => {
                // Over HTTP an oversized body is a 413; say the same here
                // rather than drop the socket, which Codex would retry as a
                // transport failure with the same frame.
                if is_oversized(&e) {
                    let max = state.config.server.max_body_bytes;
                    let message = format!("request exceeds {max} bytes");
                    let _ =
                        send_error(&mut socket, 413, "invalid_request_error", None, &message).await;
                }
                break;
            }
            None => break,
        };
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            Message::Binary(_) => {
                if send_error(
                    &mut socket,
                    400,
                    "invalid_request_error",
                    None,
                    "binary frames are not supported",
                )
                .await
                .is_err()
                {
                    break;
                }
                continue;
            }
            // Pings are answered by the socket itself.
            Message::Ping(_) | Message::Pong(_) => continue,
        };
        if handle_create(&state, &ctx, &headers, &mut socket, &mut last, text)
            .await
            .is_err()
        {
            break;
        }
    }
}

fn is_oversized(error: &axum::Error) -> bool {
    std::error::Error::source(error)
        .and_then(|source| source.downcast_ref::<tungstenite::Error>())
        .is_some_and(|e| matches!(e, tungstenite::Error::Capacity(_)))
}

/// One `response.create`. `Err` only when the client socket is gone.
async fn handle_create(
    state: &AppState,
    ctx: &AccessCtx,
    headers: &HeaderMap,
    socket: &mut WebSocket,
    last: &mut Option<Continuation>,
    text: String,
) -> Result<(), axum::Error> {
    let Ok(mut request) = serde_json::from_str::<Frame>(&text) else {
        return send_error(
            socket,
            400,
            "invalid_request_error",
            None,
            "expected a JSON object frame",
        )
        .await;
    };
    drop(text);
    if take_as::<String>(&mut request, "type").as_deref() != Some("response.create") {
        return send_error(
            socket,
            400,
            "invalid_request_error",
            None,
            "expected a `response.create` frame",
        )
        .await;
    }

    // Whatever happens next, the previous continuation is spent: a failed or
    // abandoned turn must not be continued from.
    let previous = last.take();
    let Some(input) = rebuild_input(&mut request, previous) else {
        return send_error(
            socket,
            400,
            "invalid_request_error",
            Some("previous_response_not_found"),
            "Previous response was not found on this connection.",
        )
        .await;
    };

    // `generate: false` is Codex's connection warm-up: nothing to generate,
    // only a response id to continue from. Answered here — the HTTP endpoint
    // has no such mode, and a real round-trip would cost a model call.
    if take_as::<bool>(&mut request, "generate") == Some(false) {
        let response_id = synthetic_response_id();
        let response = json!({ "id": response_id, "status": "completed", "output": [] });
        send_json(
            socket,
            &json!({ "type": "response.created", "response": response }),
        )
        .await?;
        send_json(
            socket,
            &json!({ "type": "response.completed", "response": response }),
        )
        .await?;
        if let Some(input) = input {
            *last = Some(Continuation {
                response_id,
                input,
                output: Vec::new(),
            });
        }
        return Ok(());
    }

    let body = bytes::Bytes::from(encode_request(&request, input.as_deref()));
    drop(request);
    let max_body_bytes = state.config.server.max_body_bytes;
    if body.len() > max_body_bytes {
        return send_error(
            socket,
            413,
            "invalid_request_error",
            None,
            &format!("request exceeds {max_body_bytes} bytes"),
        )
        .await;
    }

    let (fwd, log) = match dispatch_responses(state, ctx.clone(), headers, body, WS_ENDPOINT).await
    {
        Ok(Dispatched::Forwarded(fwd, log)) => (fwd, log),
        Ok(Dispatched::Rejected(response)) => {
            return send_response_as_error(socket, response).await
        }
        Err(e) => return send_response_as_error(socket, e.into_response()).await,
    };

    let upstream = fwd.response;
    if !upstream.status().is_success() {
        let status = upstream.status().as_u16();
        log.emit(status, None);
        let relayed: Map<String, Value> = upstream
            .headers()
            .iter()
            .filter(|(name, _)| is_relayed_error_header(name.as_str()))
            .filter_map(|(name, value)| {
                Some((
                    name.to_string(),
                    Value::String(value.to_str().ok()?.to_string()),
                ))
            })
            .collect();
        let body = read_capped(upstream, max_body_bytes).await;
        let mut frame = error_frame(status, &body);
        if !relayed.is_empty() {
            frame["headers"] = Value::Object(relayed);
        }
        return send_json(socket, &frame).await;
    }

    let mut events = std::pin::pin!(sse_events(upstream, log, max_body_bytes));
    let mut output = Vec::new();
    let mut completed_id = None;
    let mut terminal = false;
    loop {
        // The socket is read while the answer streams, so a client that
        // leaves is noticed at once: returning drops `events`, and with it
        // the upstream request, instead of letting it run on a pool account.
        // Reading is also what gets pings their pongs.
        let event = tokio::select! {
            event = events.next() => event,
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Text(_) | Message::Binary(_))) => {
                    send_error(
                        socket,
                        400,
                        "invalid_request_error",
                        None,
                        "a response is already in progress on this connection",
                    )
                    .await?;
                    continue;
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                    tracing::debug!("websocket client left mid-response; upstream request dropped");
                    return Err(axum::Error::new("client closed the socket mid-response"));
                }
            },
        };
        let Some(event) = event else { break };
        let data = match event {
            Ok(data) => data,
            Err(e) => {
                // Mid-stream failure: Codex treats a 5xx as retryable and
                // resends the turn, which is the right recovery here too.
                return send_error(socket, 502, "server_error", None, &e.to_string()).await;
            }
        };
        // Only the event kinds the continuation and the turn's end need are
        // parsed; the rest (mostly small deltas) go through untouched.
        if TRACKED_KINDS.iter().any(|kind| data.contains(kind)) {
            if let Ok(event) = serde_json::from_str::<TrackedEvent>(&data) {
                match event.kind.as_str() {
                    "response.output_item.done" => output.extend(event.item),
                    "response.completed" => {
                        terminal = true;
                        completed_id = event.response.and_then(|r| r.id);
                    }
                    "response.failed" | "response.incomplete" | "error" => terminal = true,
                    _ => {}
                }
            }
        }
        socket.send(Message::Text(data)).await?;
    }

    if !terminal {
        // The upstream body ended cleanly but early. Over HTTP Codex sees the
        // EOF and retries; on the socket nothing would ever end the turn.
        return send_error(
            socket,
            502,
            "server_error",
            None,
            "upstream stream ended before the response completed",
        )
        .await;
    }
    if let (Some(response_id), Some(input)) = (completed_id, input) {
        *last = Some(Continuation {
            response_id,
            input,
            output,
        });
    }
    Ok(())
}

/// Substrings of the events `handle_create` parses (see `TrackedEvent`).
const TRACKED_KINDS: &[&str] = &[
    "\"response.output_item.done\"",
    "\"response.completed\"",
    "\"response.failed\"",
    "\"response.incomplete\"",
    "\"error\"",
];

/// Remove `key` from the frame and decode it as `T`; `None` when absent or
/// of another type.
fn take_as<T: DeserializeOwned>(frame: &mut Frame, key: &str) -> Option<T> {
    serde_json::from_str(frame.remove(key)?.get()).ok()
}

/// Take `previous_response_id` and `input` out of `request` and return the
/// full input to send, as array items text: the frame's own input, or —
/// continuing the last response — that response's input and output followed
/// by the frame's.
///
/// `None` = the frame continues a response this connection can't rebuild.
/// `Some(None)` = an input that isn't an item list (e.g. a bare string); it
/// stays in `request` as sent, and can't be continued from later.
#[allow(clippy::option_option)]
fn rebuild_input(request: &mut Frame, previous: Option<Continuation>) -> Option<Option<String>> {
    let previous_id: Option<String> = take_as(request, "previous_response_id");
    let items = match request.get("input").map(|raw| array_items(raw.get())) {
        None => String::new(),
        Some(Some(items)) => {
            let items = items.to_string();
            request.remove("input");
            items
        }
        Some(None) if previous_id.is_some() => return None,
        Some(None) => return Some(None),
    };
    let Some(previous_id) = previous_id else {
        return Some(Some(items));
    };
    match previous {
        Some(prev) if prev.response_id == previous_id => {
            tracing::debug!(
                output_items = prev.output.len(),
                "websocket frame continues the previous response; rebuilt its full input"
            );
            let mut input = prev.input;
            let appended = prev.output.iter().map(|item| item.get());
            for item in appended.chain(Some(items.as_str()).filter(|s| !s.is_empty())) {
                if !input.is_empty() {
                    input.push(',');
                }
                input.push_str(item);
            }
            Some(Some(input))
        }
        _ => None,
    }
}

/// The text between the brackets of a JSON array, trimmed; `None` when
/// `json` (valid JSON, straight from a `RawValue`) isn't an array.
fn array_items(json: &str) -> Option<&str> {
    let inner = json.trim().strip_prefix('[')?.strip_suffix(']')?;
    Some(inner.trim())
}

/// The Responses body for a frame: its members as sent, `stream` forced on,
/// and `input` (when given) as the rebuilt item list — written last, so the
/// alias splice in `dispatch_responses` finds the top-level `model` without
/// scanning (or being fooled by) a multi-MB history first.
fn encode_request(request: &Frame, input: Option<&str>) -> Vec<u8> {
    let size: usize = request
        .iter()
        .map(|(k, v)| k.len() + v.get().len() + 4)
        .sum();
    let mut out = String::with_capacity(size + input.map_or(0, str::len) + 32);
    out.push_str("{\"stream\":true");
    let (history, rest): (Vec<_>, Vec<_>) = request
        .iter()
        .filter(|(key, _)| *key != "stream")
        .partition(|(key, _)| *key == "input");
    for (key, value) in rest.into_iter().chain(history) {
        out.push(',');
        out.push_str(&Value::String(key.clone()).to_string());
        out.push(':');
        out.push_str(value.get());
    }
    if let Some(items) = input {
        out.push_str(",\"input\":[");
        out.push_str(items);
        out.push(']');
    }
    out.push('}');
    out.into_bytes()
}

fn synthetic_response_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!(
        "resp_ws_warmup_{nanos:x}_{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// An error frame in the shape Codex parses on this transport:
/// `{"type":"error","status":N,"error":{...}}`. `body` is the upstream's (or
/// our own) error body; its `error` object is kept as is so Codex classifies
/// it exactly as it would over HTTP (usage limits, context window, ...).
fn error_frame(status: u16, body: &[u8]) -> Value {
    let error = match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(mut map)) => match map.remove("error") {
            Some(error @ Value::Object(_)) => error,
            _ => json!({ "message": Value::Object(map).to_string() }),
        },
        _ => json!({ "message": String::from_utf8_lossy(body) }),
    };
    json!({ "type": "error", "status": status, "error": error })
}

async fn send_error(
    socket: &mut WebSocket,
    status: u16,
    kind: &str,
    code: Option<&str>,
    message: &str,
) -> Result<(), axum::Error> {
    let mut error = json!({ "type": kind, "message": message });
    if let Some(code) = code {
        error["code"] = Value::String(code.to_string());
    }
    send_json(
        socket,
        &json!({ "type": "error", "status": status, "error": error }),
    )
    .await
}

async fn send_response_as_error(
    socket: &mut WebSocket,
    response: Response,
) -> Result<(), axum::Error> {
    let status = response.status().as_u16();
    // Our own error bodies (unknown model, ProxyError) are small JSON.
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap_or_default();
    send_json(socket, &error_frame(status, &body)).await
}

async fn send_json(socket: &mut WebSocket, value: &Value) -> Result<(), axum::Error> {
    socket.send(Message::Text(value.to_string())).await
}

async fn read_capped(upstream: reqwest::Response, cap: usize) -> Vec<u8> {
    let mut body = Vec::new();
    let mut stream = upstream.bytes_stream();
    while let Some(Ok(chunk)) = stream.next().await {
        body.extend_from_slice(&chunk);
        if body.len() > cap {
            body.truncate(cap);
            break;
        }
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(json: &str) -> Frame {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn a_string_input_is_forwarded_as_sent_but_never_continued() {
        let mut request = frame(r#"{"model":"m","input":"hi"}"#);
        assert_eq!(rebuild_input(&mut request, None), Some(None));
        let body: Value = serde_json::from_slice(&encode_request(&request, None)).unwrap();
        assert_eq!(body, json!({ "model": "m", "input": "hi", "stream": true }));

        let mut request = frame(r#"{"previous_response_id":"r","input":"hi"}"#);
        assert_eq!(rebuild_input(&mut request, None), None);
    }

    #[test]
    fn continuation_appends_output_then_new_items_to_the_previous_input() {
        let previous = Continuation {
            response_id: "r1".into(),
            input: r#"{"a":1}"#.into(),
            output: vec![RawValue::from_string(r#"{"b":2}"#.into()).unwrap()],
        };
        let mut request =
            frame(r#"{"previous_response_id":"r1","input":[ {"c":3} ],"stream":false}"#);
        let input = rebuild_input(&mut request, Some(previous))
            .unwrap()
            .unwrap();
        let encoded = encode_request(&request, Some(&input));
        let body: Value = serde_json::from_slice(&encoded).unwrap();
        assert!(encoded.ends_with(br#"{"c":3}]}"#), "input is written last");
        assert_eq!(
            body,
            json!({ "stream": true, "input": [{ "a": 1 }, { "b": 2 }, { "c": 3 }] })
        );
    }
}
