//! Recovery from replayed encrypted state the serving upstream can't decrypt.
//!
//! Codex keeps the whole conversation client-side and resends it every turn,
//! including `reasoning` items (and `compaction` items, after a compaction)
//! whose `encrypted_content` was minted by whichever upstream served that
//! turn. The blob is bound to that upstream: another ChatGPT account, or a
//! fallback provider, rejects the whole request with
//! `400 invalid_encrypted_content` — so the first failover of a live session
//! (quota exhausted, account rotated, pool -> fallback and back) kills it.
//!
//! The recovery is reactive, not preventive: the request goes out untouched,
//! and only when the upstream answers with that specific error is it retried
//! once, on the SAME upstream, without the items carrying encrypted state.
//! Visible messages and tool calls/outputs are kept, so the conversation
//! continues; what is lost is the hidden reasoning the new upstream could
//! never have read anyway, and — for a `compaction` item — the summarized
//! history before it.

use bytes::Bytes;
use serde_json::Value;

/// `error.code` values meaning "a replayed encrypted item was rejected".
/// `thinking_signature_invalid` is the same condition as some gateways
/// report it.
const UNDECRYPTABLE_CODES: &[&str] = &["invalid_encrypted_content", "thinking_signature_invalid"];

/// Input item types that carry upstream-bound `encrypted_content`.
/// `compaction_summary` is Codex's older name for `compaction`.
const ENCRYPTED_ITEM_TYPES: &[&str] = &["reasoning", "compaction", "compaction_summary"];

/// Whether a response with this status is worth reading for the error: a
/// 4xx that isn't one of the account failures the pool already fails over
/// on. OpenAI answers 400, but a gateway in front of a provider may remap it.
pub fn may_be_undecryptable(status: reqwest::StatusCode) -> bool {
    status.is_client_error() && !crate::upstream::is_account_failure(status)
}

/// Whether an error body says a replayed encrypted item couldn't be
/// decrypted. Matches the `code` first; the message check covers gateways
/// that keep OpenAI's text but drop or rename the code.
pub fn is_undecryptable(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let error = value.get("error").unwrap_or(&value);
    if error
        .get("code")
        .and_then(Value::as_str)
        .is_some_and(|code| UNDECRYPTABLE_CODES.contains(&code))
    {
        return true;
    }
    error
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .is_some_and(|m| {
            m.contains("encrypted content")
                && (m.contains("could not be decrypted") || m.contains("could not be verified"))
        })
}

/// `parsed` without the input items carrying encrypted state, plus how many
/// were dropped — or `None` when there is nothing to drop (no `input` array,
/// or no such item), in which case a retry would only fail the same way.
///
/// Drops whole items rather than just the `encrypted_content` field: a
/// reasoning shell left with only its summary is not what the upstream
/// produced, and the next turn would replay it again. A reasoning item
/// without `encrypted_content` is plain text any upstream can read, so it
/// stays.
pub fn strip_encrypted_items(parsed: &Value) -> Option<(Value, usize)> {
    let input = parsed.get("input")?.as_array()?;
    let kept: Vec<Value> = input
        .iter()
        .filter(|item| !carries_encrypted_state(item))
        .cloned()
        .collect();
    let dropped = input.len() - kept.len();
    if dropped == 0 {
        return None;
    }
    let mut stripped = parsed.clone();
    stripped["input"] = Value::Array(kept);
    Some((stripped, dropped))
}

/// `strip_encrypted_items` for a raw request body. `None` also for a body
/// that isn't JSON.
pub fn strip_encrypted_body(body: &[u8]) -> Option<(Bytes, usize)> {
    let parsed = serde_json::from_slice::<Value>(body).ok()?;
    let (stripped, dropped) = strip_encrypted_items(&parsed)?;
    Some((Bytes::from(serde_json::to_vec(&stripped).ok()?), dropped))
}

fn carries_encrypted_state(item: &Value) -> bool {
    let is_encrypted_type = item
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|t| ENCRYPTED_ITEM_TYPES.contains(&t));
    is_encrypted_type
        && item
            .get("encrypted_content")
            .and_then(Value::as_str)
            .is_some_and(|c| !c.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn may_be_undecryptable_only_for_non_account_4xx() {
        use reqwest::StatusCode;
        assert!(may_be_undecryptable(StatusCode::BAD_REQUEST));
        assert!(may_be_undecryptable(StatusCode::UNPROCESSABLE_ENTITY));
        assert!(!may_be_undecryptable(StatusCode::UNAUTHORIZED));
        assert!(!may_be_undecryptable(StatusCode::FORBIDDEN));
        assert!(!may_be_undecryptable(StatusCode::TOO_MANY_REQUESTS));
        assert!(!may_be_undecryptable(StatusCode::OK));
        assert!(!may_be_undecryptable(StatusCode::BAD_GATEWAY));
    }

    #[test]
    fn is_undecryptable_recognizes_the_error() {
        // The exact shape OpenAI/Azure return (openai/codex#17541).
        assert!(is_undecryptable(
            br#"{"error":{"message":"The encrypted content gAAA...S6Q= could not be verified. Reason: Encrypted content could not be decrypted or parsed.","type":"invalid_request_error","param":null,"code":"invalid_encrypted_content"}}"#
        ));
        assert!(is_undecryptable(
            br#"{"error":{"code":"thinking_signature_invalid"}}"#
        ));
        // Code dropped by a gateway, OpenAI's message kept.
        assert!(is_undecryptable(
            br#"{"error":{"message":"Reason: Encrypted content could not be decrypted or parsed."}}"#
        ));
        // Top-level error object, no `error` wrapper.
        assert!(is_undecryptable(
            br#"{"code":"invalid_encrypted_content","message":"x"}"#
        ));
    }

    #[test]
    fn is_undecryptable_ignores_other_errors() {
        assert!(!is_undecryptable(
            br#"{"error":{"message":"The 'x' model is not supported","code":"model_not_supported"}}"#
        ));
        assert!(!is_undecryptable(
            br#"{"error":{"message":"encrypted content is required"}}"#
        ));
        assert!(!is_undecryptable(b"Request blocked."));
        assert!(!is_undecryptable(b""));
    }

    #[test]
    fn strip_drops_encrypted_items_and_keeps_the_transcript() {
        let body = json!({
            "model": "gpt-5.5",
            "input": [
                {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "gAAA-foreign"},
                {"type": "function_call", "call_id": "c1", "name": "sh", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "c1", "output": "ok"},
                {"type": "compaction", "encrypted_content": "gAAA-summary"},
                {"type": "reasoning", "id": "rs_2", "summary": [{"type": "summary_text", "text": "plain"}]},
                {"type": "reasoning", "id": "rs_3", "summary": [], "encrypted_content": ""},
                {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "done"}]},
            ],
        });
        let (stripped, dropped) = strip_encrypted_items(&body).unwrap();
        assert_eq!(dropped, 2);
        assert_eq!(stripped["model"], "gpt-5.5");
        let types: Vec<&str> = stripped["input"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            [
                "message",
                "function_call",
                "function_call_output",
                "reasoning",
                "reasoning",
                "message"
            ]
        );
        assert_eq!(stripped["input"][3]["id"], "rs_2");
        assert_eq!(stripped["input"][4]["id"], "rs_3");
    }

    #[test]
    fn strip_returns_none_when_there_is_nothing_to_drop() {
        assert!(strip_encrypted_items(&json!({"input": "hi"})).is_none());
        assert!(strip_encrypted_items(&json!({"model": "gpt-5.5"})).is_none());
        assert!(strip_encrypted_items(&json!({
            "input": [{"type": "message", "role": "user", "content": "hi"}]
        }))
        .is_none());
        assert!(strip_encrypted_body(b"not json").is_none());
    }

    #[test]
    fn strip_body_round_trips_json() {
        let body = br#"{"model":"m","input":[{"type":"compaction_summary","encrypted_content":"x"},{"type":"message","role":"user","content":"hi"}]}"#;
        let (stripped, dropped) = strip_encrypted_body(body).unwrap();
        assert_eq!(dropped, 1);
        let value: Value = serde_json::from_slice(&stripped).unwrap();
        assert_eq!(
            value["input"],
            json!([{"type":"message","role":"user","content":"hi"}])
        );
    }
}
