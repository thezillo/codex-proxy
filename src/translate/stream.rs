//! Codex Responses SSE -> OpenAI chat.completions.
//!
//! Two consumers:
//!   * `stream_chat` — re-streams as `chat.completion.chunk` SSE.
//!   * `collect_chat` — buffers into a single `chat.completion` object.
//!
//! Codex events handled (keyed off each data payload's `type`):
//!   response.output_text.delta              -> content delta
//!   response.reasoning_summary_text.delta   -> reasoning_content delta (opt-in)
//!   response.output_item.added (func call)  -> tool-call start
//!   response.function_call_arguments.delta  -> tool-call args delta
//!   response.function_call_arguments.done   -> tool-call args (if no deltas)
//!   response.completed                      -> finish + usage
//!   response.failed / error                 -> error

use std::collections::{HashMap, HashSet};

use async_stream::try_stream;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use serde_json::{json, Value};

use crate::config::DefaultsConfig;
use crate::error::ProxyError;
use crate::observe::CompletionLog;

fn now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn chunk_id() -> String {
    // Second-granularity time alone collides when two responses start in the
    // same second; a process-lifetime counter makes the id unique so clients
    // that correlate chunks by `id` don't conflate separate completions.
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("chatcmpl-{}-{n:x}", now_secs())
}

#[derive(Default)]
struct StreamState {
    tool_index: HashMap<String, usize>,
    next_index: usize,
    /// Tool-call indices that already streamed incremental argument deltas.
    /// Keyed by canonical index (not raw event id) so a delta labelled with
    /// `item_id` and a `done` labelled with `call_id` still reconcile.
    indices_with_deltas: HashSet<usize>,
    has_tool_calls: bool,
    has_content: bool,
}

fn sse(value: Value) -> Bytes {
    Bytes::from(format!("data: {value}\n\n"))
}

/// Translate one Codex event into zero or more chat.completion.chunk values.
fn translate_event(
    evt: &Value,
    id: &str,
    created: i64,
    model: &str,
    include_reasoning: bool,
    st: &mut StreamState,
) -> Vec<Value> {
    let etype = evt.get("type").and_then(Value::as_str).unwrap_or("");
    let mut out = Vec::new();

    let base = |delta: Value, finish: Option<&str>| {
        json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
        })
    };

    match etype {
        "response.output_text.delta" => {
            if let Some(d) = evt.get("delta").and_then(Value::as_str) {
                st.has_content = true;
                out.push(base(json!({ "content": d }), None));
            }
        }

        "response.reasoning_summary_text.delta" if include_reasoning => {
            if let Some(d) = evt.get("delta").and_then(Value::as_str) {
                out.push(base(json!({ "reasoning_content": d }), None));
            }
        }

        "response.output_item.added" => {
            let item = evt.get("item").cloned().unwrap_or(Value::Null);
            if item.get("type").and_then(Value::as_str) == Some("function_call") {
                let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
                let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                let item_id = item.get("id").and_then(Value::as_str).unwrap_or(call_id);
                let idx = st.next_index;
                st.next_index += 1;
                st.tool_index.insert(item_id.to_string(), idx);
                st.tool_index.insert(call_id.to_string(), idx);
                st.has_tool_calls = true;
                st.has_content = true;
                out.push(base(
                    json!({ "tool_calls": [{
                        "index": idx,
                        "id": call_id,
                        "type": "function",
                        "function": { "name": name, "arguments": "" }
                    }]}),
                    None,
                ));
            }
        }

        "response.function_call_arguments.delta" => {
            let key = event_call_key(evt);
            // Never default an unknown key to index 0 — that splices arguments
            // into the wrong tool call. Drop the fragment loudly instead.
            let Some(idx) = st.tool_index.get(&key).copied() else {
                tracing::warn!(%key, "tool-call args delta for unknown call id; dropping fragment");
                return out;
            };
            if let Some(d) = evt.get("delta").and_then(Value::as_str) {
                st.indices_with_deltas.insert(idx);
                out.push(base(
                    json!({ "tool_calls": [{ "index": idx, "function": { "arguments": d } }] }),
                    None,
                ));
            }
        }

        "response.function_call_arguments.done" => {
            let key = event_call_key(evt);
            // Resolve to the canonical index first; both id labels map here, so a
            // delta tagged with one label and this `done` tagged with the other
            // still reconcile. Without this, the full arguments would be re-emitted
            // on top of the streamed deltas, corrupting the client-side JSON.
            let Some(idx) = st.tool_index.get(&key).copied() else {
                tracing::warn!(%key, "tool-call args done for unknown call id; dropping");
                return out;
            };
            if !st.indices_with_deltas.contains(&idx) {
                let args = evt.get("arguments").and_then(Value::as_str).unwrap_or("");
                out.push(base(
                    json!({ "tool_calls": [{ "index": idx, "function": { "arguments": args } }] }),
                    None,
                ));
            }
        }

        "response.completed" => {
            let finish = if st.has_tool_calls {
                "tool_calls"
            } else {
                "stop"
            };
            let usage = usage_from_completed(evt);
            let mut chunk = base(json!({}), Some(finish));
            chunk.as_object_mut().unwrap().insert("usage".into(), usage);
            out.push(chunk);
        }

        "response.failed" | "error" => {
            let msg = evt
                .pointer("/error/message")
                .or_else(|| evt.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("upstream error");
            out.push(base(
                json!({ "content": format!("[error] {msg}") }),
                Some("stop"),
            ));
        }

        _ => {}
    }

    out
}

/// The Responses API labels argument events with `item_id` (sometimes `call_id`).
fn event_call_key(evt: &Value) -> String {
    evt.get("item_id")
        .and_then(Value::as_str)
        .or_else(|| evt.get("call_id").and_then(Value::as_str))
        .unwrap_or("")
        .to_string()
}

/// Append a tool call unless one with the same `call_id` is already present
/// (an empty id is treated as always-new to avoid collapsing distinct calls).
fn push_tool_call(tool_calls: &mut Vec<Value>, call_id: &str, name: &str, args: &str) {
    if !call_id.is_empty()
        && tool_calls
            .iter()
            .any(|t| t.get("id").and_then(Value::as_str) == Some(call_id))
    {
        return;
    }
    tool_calls.push(json!({
        "id": call_id,
        "type": "function",
        "function": { "name": name, "arguments": args },
    }));
}

/// Pull `(input_tokens, output_tokens)` out of a `response.completed` event,
/// or `None` when the upstream omitted the usage block. Shared by the wire
/// translation (`usage_from_completed`) and the access-log token attribution so
/// both read the same numbers.
pub fn usage_tokens(evt: &Value) -> Option<(i64, i64)> {
    let u = evt.pointer("/response/usage")?;
    let input = u.get("input_tokens").and_then(Value::as_i64).unwrap_or(0);
    let output = u.get("output_tokens").and_then(Value::as_i64).unwrap_or(0);
    Some((input, output))
}

fn usage_from_completed(evt: &Value) -> Value {
    let (input, output) = usage_tokens(evt).unwrap_or_else(|| {
        tracing::warn!("response.completed without usage block; reporting zero tokens");
        (0, 0)
    });
    json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input + output,
    })
}

/// Re-stream an upstream Responses SSE body as chat.completion.chunk SSE.
///
/// `log` is consumed when the stream finishes: token usage seen in the
/// `response.completed` event is emitted on the access line. If the client
/// disconnects mid-stream the future is dropped and no completion line fires —
/// the middleware's start line already recorded *who* made the request.
pub fn stream_chat(
    resp: reqwest::Response,
    model: String,
    defaults: DefaultsConfig,
    log: CompletionLog,
) -> impl Stream<Item = Result<Bytes, ProxyError>> {
    let id = chunk_id();
    let created = now_secs();
    let include_reasoning = defaults.include_reasoning;

    try_stream! {
        let mut final_usage: Option<(i64, i64)> = None;
        // Opening role chunk.
        yield sse(json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{ "index": 0, "delta": { "role": "assistant" }, "finish_reason": null }],
        }));

        let mut st = StreamState::default();
        let mut buf: Vec<u8> = Vec::new();
        let mut body = resp.bytes_stream();

        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|e| ProxyError::Upstream(format!("stream read: {e}")))?;
            buf.extend_from_slice(&chunk);

            while let Some((pos, dlen)) = find_event_delimiter(&buf) {
                let block: Vec<u8> = buf.drain(..pos + dlen).collect();
                let block = String::from_utf8_lossy(&block);
                let Some(data) = extract_data(&block) else { continue };
                if data == "[DONE]" {
                    continue;
                }
                let evt = match serde_json::from_str::<Value>(&data) {
                    Ok(v) => v,
                    Err(e) => {
                        // Dropping an upstream event here is silent data loss
                        // (the client still gets a clean `stop`). Make it visible.
                        tracing::warn!(error = %e, bytes = data.len(), "dropping unparseable upstream SSE event");
                        continue;
                    }
                };
                if evt.get("type").and_then(Value::as_str) == Some("response.completed") {
                    final_usage = usage_tokens(&evt);
                }
                for c in translate_event(&evt, &id, created, &model, include_reasoning, &mut st) {
                    yield sse(c);
                }
            }
        }

        // Stream drained cleanly: attribute the tokens this request spent.
        log.emit(200, final_usage);

        // If upstream produced nothing, surface a visible note so the client
        // doesn't hang on an empty assistant message. Headers are already sent,
        // so we can't change the HTTP status — at least log it for the operator.
        if !st.has_content {
            tracing::warn!("upstream stream produced no content; emitting empty-response note");
            yield sse(json!({
                "id": id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{ "index": 0, "delta": { "content": "[error] empty response from upstream" }, "finish_reason": "stop" }],
            }));
        }

        yield Bytes::from("data: [DONE]\n\n");
    }
}

/// Collect an upstream Responses SSE body into one chat.completion object.
pub async fn collect_chat(
    resp: reqwest::Response,
    model: String,
    defaults: DefaultsConfig,
) -> Result<Value, ProxyError> {
    let id = chunk_id();
    let created = now_secs();

    let mut full_text = String::new();
    let mut full_reasoning = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    // Resolve argument events back to their stable call_id + name, registered
    // when the function_call item is first announced. Keyed by both item_id and
    // call_id so either label on a later event resolves correctly.
    let mut call_info: HashMap<String, (String, String)> = HashMap::new();
    let mut usage = json!({ "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0 });

    let mut buf: Vec<u8> = Vec::new();
    let mut body = resp.bytes_stream();

    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| ProxyError::Upstream(format!("stream read: {e}")))?;
        buf.extend_from_slice(&chunk);

        while let Some((pos, dlen)) = find_event_delimiter(&buf) {
            let block: Vec<u8> = buf.drain(..pos + dlen).collect();
            let block = String::from_utf8_lossy(&block);
            let Some(data) = extract_data(&block) else {
                continue;
            };
            if data == "[DONE]" {
                continue;
            }
            let evt = match serde_json::from_str::<Value>(&data) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, bytes = data.len(), "dropping unparseable upstream SSE event");
                    continue;
                }
            };
            match evt.get("type").and_then(Value::as_str).unwrap_or("") {
                "response.output_text.delta" => {
                    if let Some(d) = evt.get("delta").and_then(Value::as_str) {
                        full_text.push_str(d);
                    }
                }
                "response.reasoning_summary_text.delta" => {
                    if let Some(d) = evt.get("delta").and_then(Value::as_str) {
                        full_reasoning.push_str(d);
                    }
                }
                "response.output_item.added" => {
                    let item = evt.get("item").cloned().unwrap_or(Value::Null);
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let call_id = item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let name = item
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string();
                        let item_id = item.get("id").and_then(Value::as_str).unwrap_or(&call_id);
                        call_info.insert(item_id.to_string(), (call_id.clone(), name.clone()));
                        call_info.insert(call_id.clone(), (call_id, name));
                    }
                }
                "response.function_call_arguments.done" => {
                    let key = event_call_key(&evt);
                    // Resolve to the stable call_id/name registered at `added`;
                    // never use the raw item_id as the emitted tool-call id.
                    let (call_id, name) = call_info.get(&key).cloned().unwrap_or_else(|| {
                        (
                            key.clone(),
                            evt.get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_string(),
                        )
                    });
                    let args = evt.get("arguments").and_then(Value::as_str).unwrap_or("");
                    push_tool_call(&mut tool_calls, &call_id, &name, args);
                }
                "response.output_item.done" => {
                    // Final fallback: a function_call item whose args never came
                    // through `arguments.done`. Deduped by call_id.
                    let item = evt.get("item").cloned().unwrap_or(Value::Null);
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
                        let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                        let args = item.get("arguments").and_then(Value::as_str).unwrap_or("");
                        push_tool_call(&mut tool_calls, call_id, name, args);
                    }
                }
                "response.completed" => {
                    usage = usage_from_completed(&evt);
                }
                "response.failed" | "error" => {
                    let msg = evt
                        .pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("upstream error");
                    return Err(ProxyError::Upstream(msg.to_string()));
                }
                _ => {}
            }
        }
    }

    let has_tools = !tool_calls.is_empty();
    if full_text.is_empty() && !has_tools && full_reasoning.is_empty() {
        tracing::warn!("upstream produced no content, tool calls, or reasoning (empty completion)");
    }
    let mut message = json!({
        "role": "assistant",
        "content": if full_text.is_empty() { Value::Null } else { Value::String(full_text) },
    });
    if defaults.include_reasoning && !full_reasoning.is_empty() {
        message
            .as_object_mut()
            .unwrap()
            .insert("reasoning_content".into(), Value::String(full_reasoning));
    }
    if has_tools {
        message
            .as_object_mut()
            .unwrap()
            .insert("tool_calls".into(), Value::Array(tool_calls));
    }

    Ok(json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": if has_tools { "tool_calls" } else { "stop" },
        }],
        "usage": usage,
    }))
}

/// Forward an upstream Responses SSE body **verbatim** while tee-ing it for
/// token usage. Used by the `/v1/responses` passthrough — the path the real
/// Codex CLI hits — so its token spend is attributed too, without altering the
/// bytes the client receives. The scan buffer only ever holds the current
/// partial event (completed events are drained), so it adds negligible memory.
pub fn tee_responses(
    resp: reqwest::Response,
    log: CompletionLog,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
    let status = resp.status().as_u16();
    try_stream! {
        let mut scanner = SseUsageScanner::default();
        let mut body = resp.bytes_stream();

        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(std::io::Error::other)?;
            // Scan a copy for usage; the forwarded `chunk` is never modified.
            scanner.push(&chunk);
            yield chunk;
        }

        log.emit(status, scanner.usage);
    }
}

/// Incremental SSE scanner that watches a byte stream for the
/// `response.completed` event and remembers its token usage — without altering
/// the bytes. Chunk boundaries are arbitrary (an event may be split across
/// reads), so it buffers across pushes and drains whole events as they
/// complete; `buf` only ever holds the current partial event.
#[derive(Default)]
struct SseUsageScanner {
    buf: Vec<u8>,
    usage: Option<(i64, i64)>,
}

impl SseUsageScanner {
    fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
        while let Some((pos, dlen)) = find_event_delimiter(&self.buf) {
            let block: Vec<u8> = self.buf.drain(..pos + dlen).collect();
            let block = String::from_utf8_lossy(&block);
            let Some(data) = extract_data(&block) else {
                continue;
            };
            if data == "[DONE]" {
                continue;
            }
            if let Ok(evt) = serde_json::from_str::<Value>(&data) {
                if evt.get("type").and_then(Value::as_str) == Some("response.completed") {
                    self.usage = usage_tokens(&evt);
                }
            }
        }
    }
}

/// First SSE event delimiter in `buf`, returning `(offset, delimiter_len)`.
/// Handles both `\n\n` (LF) and `\r\n\r\n` (CRLF) line endings.
fn find_event_delimiter(buf: &[u8]) -> Option<(usize, usize)> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|p| (p, 2));
    let crlf = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| (p, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Concatenate the `data:` lines of one SSE block into a single payload string.
fn extract_data(block: &str) -> Option<String> {
    let mut data = String::new();
    let mut found = false;
    for line in block.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            found = true;
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    if found {
        Some(data)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn st() -> StreamState {
        StreamState::default()
    }

    #[test]
    fn scanner_captures_usage_across_chunk_boundary() {
        // The `response.completed` event is split mid-JSON across two pushes —
        // the scanner must buffer and still recover the token counts.
        let full = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":3}}}\n\n",
            "data: [DONE]\n\n"
        );
        let split = full.len() / 2;
        let mut s = SseUsageScanner::default();
        s.push(&full.as_bytes()[..split]);
        assert_eq!(s.usage, None, "usage should not resolve from a partial event");
        s.push(&full.as_bytes()[split..]);
        assert_eq!(s.usage, Some((7, 3)));
    }

    #[test]
    fn scanner_reports_none_without_usage_block() {
        let mut s = SseUsageScanner::default();
        s.push(b"data: {\"type\":\"response.completed\",\"response\":{}}\n\n");
        assert_eq!(s.usage, None);
    }

    #[test]
    fn extracts_single_and_multiline_data() {
        assert_eq!(extract_data("data: hello\n"), Some("hello".to_string()));
        assert_eq!(
            extract_data("event: x\ndata: a\ndata: b\n"),
            Some("a\nb".to_string())
        );
        assert_eq!(extract_data("event: ping\n"), None);
    }

    #[test]
    fn finds_event_delimiter_lf_and_crlf() {
        assert_eq!(find_event_delimiter(b"abc\n\ndef"), Some((3, 2)));
        assert_eq!(find_event_delimiter(b"abc\r\n\r\ndef"), Some((3, 4)));
        assert_eq!(find_event_delimiter(b"abc\ndef"), None);
        // earliest delimiter wins
        assert_eq!(find_event_delimiter(b"a\n\nb\r\n\r\n"), Some((1, 2)));
    }

    #[test]
    fn unknown_tool_key_does_not_corrupt_index_zero() {
        let mut s = st();
        // register call at index 0
        let _ = translate_event(
            &json!({"type":"response.output_item.added","item":{"type":"function_call","id":"i0","call_id":"c0","name":"a"}}),
            "id",
            1,
            "m",
            false,
            &mut s,
        );
        // a delta for an UNKNOWN id must be dropped, not appended to index 0
        let out = translate_event(
            &json!({"type":"response.function_call_arguments.delta","item_id":"ghost","delta":"x"}),
            "id",
            1,
            "m",
            false,
            &mut s,
        );
        assert!(out.is_empty());
    }

    #[test]
    fn mixed_item_id_and_call_id_does_not_duplicate_tool_args() {
        let mut s = st();
        // Announce the call; tool_index records BOTH i0 and c0 -> index 0.
        let _ = translate_event(
            &json!({"type":"response.output_item.added","item":{"type":"function_call","id":"i0","call_id":"c0","name":"a"}}),
            "id",
            1,
            "m",
            false,
            &mut s,
        );
        // Deltas arrive labelled with item_id.
        let deltas = translate_event(
            &json!({"type":"response.function_call_arguments.delta","item_id":"i0","delta":"{\"x\":1}"}),
            "id",
            1,
            "m",
            false,
            &mut s,
        );
        assert_eq!(deltas.len(), 1);
        // `done` arrives labelled with the OTHER id (call_id). It must not
        // re-emit the full arguments on top of the streamed deltas.
        let done = translate_event(
            &json!({"type":"response.function_call_arguments.done","call_id":"c0","arguments":"{\"x\":1}"}),
            "id",
            1,
            "m",
            false,
            &mut s,
        );
        assert!(
            done.is_empty(),
            "done with mismatched id label re-emitted args: {done:?}"
        );
    }

    #[test]
    fn text_delta_becomes_content_chunk() {
        let evt = json!({ "type": "response.output_text.delta", "delta": "Hi" });
        let mut s = st();
        let out = translate_event(&evt, "id1", 1, "m", false, &mut s);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["choices"][0]["delta"]["content"], "Hi");
        assert!(s.has_content);
    }

    #[test]
    fn tool_call_lifecycle_maps_indices() {
        let mut s = st();
        let added = json!({
            "type": "response.output_item.added",
            "item": { "type": "function_call", "id": "item_1", "call_id": "call_1", "name": "get_weather" }
        });
        let out = translate_event(&added, "id", 1, "m", false, &mut s);
        assert_eq!(
            out[0]["choices"][0]["delta"]["tool_calls"][0]["id"],
            "call_1"
        );
        assert_eq!(
            out[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert!(s.has_tool_calls);

        let delta = json!({
            "type": "response.function_call_arguments.delta",
            "item_id": "item_1", "delta": "{\"city\":"
        });
        let out = translate_event(&delta, "id", 1, "m", false, &mut s);
        assert_eq!(out[0]["choices"][0]["delta"]["tool_calls"][0]["index"], 0);
        assert_eq!(
            out[0]["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
            "{\"city\":"
        );

        // done after deltas → suppressed (no duplicate args)
        let done = json!({
            "type": "response.function_call_arguments.done",
            "item_id": "item_1", "arguments": "{\"city\":\"LA\"}"
        });
        let out = translate_event(&done, "id", 1, "m", false, &mut s);
        assert!(
            out.is_empty(),
            "done should be suppressed when deltas streamed"
        );
    }

    #[test]
    fn completed_emits_finish_and_usage() {
        let mut s = st();
        s.has_tool_calls = true;
        let evt = json!({
            "type": "response.completed",
            "response": { "usage": { "input_tokens": 10, "output_tokens": 5 } }
        });
        let out = translate_event(&evt, "id", 1, "m", false, &mut s);
        assert_eq!(out[0]["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(out[0]["usage"]["prompt_tokens"], 10);
        assert_eq!(out[0]["usage"]["total_tokens"], 15);
    }

    #[test]
    fn reasoning_gated_by_flag() {
        let evt = json!({ "type": "response.reasoning_summary_text.delta", "delta": "thinking" });
        let mut s = st();
        assert!(translate_event(&evt, "id", 1, "m", false, &mut s).is_empty());
        assert_eq!(
            translate_event(&evt, "id", 1, "m", true, &mut s)[0]["choices"][0]["delta"]
                ["reasoning_content"],
            "thinking"
        );
    }
}
