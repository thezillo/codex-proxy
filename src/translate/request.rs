//! chat/completions request -> Codex Responses API request.
//!
//! Mapping (mirrors openai/codex-proxy's openai-to-codex):
//!   system/developer messages   -> `instructions`
//!   user/assistant/tool messages -> `input[]`
//!   assistant.tool_calls         -> `{type:"function_call", call_id, name, arguments}`
//!   tool message                 -> `{type:"function_call_output", call_id, output}`
//!   reasoning_effort             -> `reasoning: {effort, summary}`
//!   tools                        -> flattened `{type:"function", name, ...}`

use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::DefaultsConfig;
use crate::translate::openai::{ChatCompletionRequest, ImageUrl, MessageContent};

/// Read just the `model` field of a Responses body. Deserializing into a
/// one-field struct walks the JSON but builds no tree — serde skips every
/// other field as `IgnoredAny` — so reading the model out of a multi-megabyte
/// body costs a scan, not a `Value` allocation of it.
///
/// `None` covers "not JSON", "no model", "model isn't a string" and the one
/// odd case of a duplicate top-level `model` key, which serde rejects where
/// `Value` would take the last. No real client emits duplicates, and every one
/// of those bodies is forwarded untouched, exactly as before this existed.
fn model_of(body: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct ModelOnly {
        model: Option<String>,
    }
    serde_json::from_slice::<ModelOnly>(body).ok()?.model
}

/// Apply `defaults.model_aliases` to a raw `/v1/responses` body.
///
/// Codex itself speaks `wire_api = "responses"`, so the alias map — which
/// exists precisely to stop a bare `gpt-6`/`gpt-5.6` from 400ing upstream and
/// silently diverting the request to a paid fallback — has to reach this path
/// too, not just the chat/completions translation. `/v1/models` advertises the
/// bare names, so a client picking one from that list must land on the
/// subscription, whichever endpoint it uses.
///
/// Returns `Some(rewritten)` ONLY when an alias actually fired; callers
/// forward the original bytes otherwise, so the passthrough stays byte-exact
/// for every request that needs no rewrite. Unlike `resolve_model` this never
/// substitutes `defaults.model` for an unrecognized id: on this path the id is
/// the client's own, and an unknown one is the upstream's call to reject.
///
/// A body that isn't JSON, or carries no string `model`, is left alone — the
/// proxy has no business erroring on a shape only the upstream defines.
pub fn alias_responses_model(body: &[u8], defaults: &DefaultsConfig) -> Option<Vec<u8>> {
    if defaults.model_aliases.is_empty() {
        return None;
    }
    // Cheap read first: everything below only happens for a request that is
    // actually going to be rewritten. Note that for the client this feature
    // exists for — Codex pinned to a bare `gpt-6`/`gpt-5.6` — that is EVERY
    // request of the session, each carrying a conversation history that grows
    // toward `server.max_body_bytes`, so the rewrite has to stay cheap in
    // memory, not just in wall time.
    let requested = model_of(body)?;
    let mapped = defaults.model_aliases.get(&requested)?;
    // An identity entry (the shipped config.toml has `"gpt-5" = "gpt-5"`)
    // maps to itself, so rewriting would only reserialize the body into
    // something equivalent. Bail before paying for that.
    if mapped == &requested {
        return None;
    }

    // Fast path: splice the new name into the raw bytes. The obvious
    // implementation — parse to `Value`, set the field, reserialize — costs
    // about 4x the body in peak RSS and 4x the time (measured on a 16 MiB
    // body: +64 MB and 75 ms, against +17 MB and 17 ms for the splice), which
    // would break the sizing this proxy is deployed under: buffers there are
    // budgeted as "baseline + the bodies currently in flight". Splicing keeps
    // that true — its only allocation is the outgoing copy.
    //
    // The splice is then VERIFIED by re-reading the model, so a body that
    // defeats the byte scan (a nested `"model"` member appearing first) falls
    // through to the tree rewrite rather than being forwarded wrong. That
    // makes correctness depend on serde, not on the scanner.
    if let Some(spliced) = splice_top_level_model(body, &requested, mapped) {
        if model_of(&spliced).as_deref() == Some(mapped.as_str()) {
            return Some(spliced);
        }
    }

    // Getting here means the scan couldn't be trusted and the tree round-trip
    // runs instead. The outcome is still correct, but it costs the ~4x RSS the
    // splice exists to avoid — and it is sticky: whatever defeated the scan (a
    // nested `"model"` member, an escaped model name) usually rides in the
    // conversation history for the rest of the session, so every later request
    // pays it too. One line, so a step-change in memory has an explanation.
    tracing::debug!(model = %requested, "responses model alias fell back to a full JSON rewrite");
    let mut parsed: Value = serde_json::from_slice(body).ok()?;
    *parsed.get_mut("model")? = Value::String(mapped.clone());
    serde_json::to_vec(&parsed).ok()
}

/// Replace the value of a `"model"` member in `body`, by bytes.
///
/// Finds an occurrence of the JSON-encoded `requested` string that is preceded
/// by `"model"` and a colon (whitespace allowed around it), and swaps in
/// `mapped`. Deliberately does NOT try to prove the member is top-level — the
/// caller verifies the result instead, which is both simpler and stricter than
/// a hand-rolled depth tracker would be.
fn splice_top_level_model(body: &[u8], requested: &str, mapped: &str) -> Option<Vec<u8>> {
    let needle = format!("\"{requested}\"");
    let replacement = format!("\"{mapped}\"");
    let start = (0..body.len())
        .find(|&i| body[i..].starts_with(needle.as_bytes()) && key_precedes(body, i))?;

    let mut out = Vec::with_capacity(body.len() - needle.len() + replacement.len());
    out.extend_from_slice(&body[..start]);
    out.extend_from_slice(replacement.as_bytes());
    out.extend_from_slice(&body[start + needle.len()..]);
    Some(out)
}

/// True when the bytes before `at` are `"model"` and a colon, ignoring the
/// whitespace JSON allows in between.
fn key_precedes(body: &[u8], at: usize) -> bool {
    const KEY: &[u8] = b"\"model\"";
    let Some(before) = body[..at].trim_ascii_end().strip_suffix(b":") else {
        return false;
    };
    before.trim_ascii_end().ends_with(KEY)
}

/// Build the Codex Responses request body. We always request `stream: true`
/// upstream and adapt to the client's streaming preference ourselves.
pub fn build_codex_request(req: &ChatCompletionRequest, defaults: &DefaultsConfig) -> Value {
    let instructions = collect_instructions(req, defaults);
    let input = build_input(req);
    let model = resolve_model(&req.model, defaults);

    let mut body = json!({
        "model": model,
        "instructions": instructions,
        "input": input,
        "stream": true,
        "store": false,
    });
    let obj = body.as_object_mut().expect("json object");

    // Tools: pass everything through. Only `type:"function"` needs reshaping
    // (OpenAI nests it under `function`; Responses wants it flat). Hosted tools
    // — web_search, image_generation, anything else — go through verbatim.
    if let Some(tools) = &req.tools {
        let converted: Vec<Value> = tools.iter().map(convert_tool).collect();
        if !converted.is_empty() {
            obj.insert("tools".into(), Value::Array(converted));
            if let Some(tc) = &req.tool_choice {
                obj.insert("tool_choice".into(), convert_tool_choice(tc));
            }
        }
    }

    // Reasoning: request field overrides the configured default.
    let effort = req
        .reasoning_effort
        .clone()
        .unwrap_or_else(|| defaults.reasoning_effort.clone());
    if !effort.is_empty() {
        obj.insert(
            "reasoning".into(),
            json!({ "effort": effort, "summary": defaults.reasoning_summary }),
        );
    }

    body
}

fn resolve_model(requested: &str, defaults: &DefaultsConfig) -> String {
    if let Some(mapped) = defaults.model_aliases.get(requested) {
        return mapped.clone();
    }
    // Pass through real-looking ids; otherwise fall back to the configured model.
    if requested.starts_with("gpt-") || requested.starts_with("o") {
        requested.to_string()
    } else {
        defaults.model.clone()
    }
}

fn collect_instructions(req: &ChatCompletionRequest, defaults: &DefaultsConfig) -> String {
    let system: Vec<String> = req
        .messages
        .iter()
        .filter(|m| m.role == "system" || m.role == "developer")
        .filter_map(|m| m.content.as_ref().map(|c| c.as_text()))
        .filter(|s| !s.is_empty())
        .collect();

    if system.is_empty() {
        defaults.instructions.clone()
    } else {
        system.join("\n\n")
    }
}

fn build_input(req: &ChatCompletionRequest) -> Value {
    let mut input: Vec<Value> = Vec::new();

    for msg in &req.messages {
        match msg.role.as_str() {
            "system" | "developer" => continue,

            "assistant" => {
                let text = msg
                    .content
                    .as_ref()
                    .map(|c| c.as_text())
                    .unwrap_or_default();
                let has_tool_calls = msg.tool_calls.as_ref().is_some_and(|t| !t.is_empty());
                if !text.is_empty() || !has_tool_calls {
                    input.push(json!({ "role": "assistant", "content": text }));
                }
                if let Some(tool_calls) = &msg.tool_calls {
                    for tc in tool_calls {
                        input.push(json!({
                            "type": "function_call",
                            "call_id": tc.id,
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        }));
                    }
                }
            }

            "tool" => {
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": msg.tool_call_id.clone().unwrap_or_else(|| "unknown".into()),
                    "output": msg.content.as_ref().map(|c| c.as_text()).unwrap_or_default(),
                }));
            }

            "function" => {
                // Legacy OpenAI function-result format.
                let name = msg.name.clone().unwrap_or_else(|| "unknown".into());
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": format!("fc_{name}"),
                    "output": msg.content.as_ref().map(|c| c.as_text()).unwrap_or_default(),
                }));
            }

            _ => {
                // user (and any other) message
                input
                    .push(json!({ "role": "user", "content": user_content(msg.content.as_ref()) }));
            }
        }
    }

    if input.is_empty() {
        input.push(json!({ "role": "user", "content": "" }));
    }
    Value::Array(input)
}

/// User content: plain string when text-only, structured parts when images are
/// present (Responses uses `input_text` / `input_image`).
fn user_content(content: Option<&MessageContent>) -> Value {
    match content {
        None => Value::String(String::new()),
        Some(MessageContent::Text(s)) => Value::String(s.clone()),
        Some(MessageContent::Parts(parts)) => {
            let has_image = parts.iter().any(|p| p.kind == "image_url");
            if !has_image {
                return Value::String(
                    parts
                        .iter()
                        .filter(|p| p.kind == "text")
                        .filter_map(|p| p.text.clone())
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
            let mut out: Vec<Value> = Vec::new();
            for p in parts {
                match p.kind.as_str() {
                    "text" => {
                        if let Some(t) = &p.text {
                            out.push(json!({ "type": "input_text", "text": t }));
                        }
                    }
                    "image_url" => {
                        let url = match &p.image_url {
                            Some(ImageUrl::Str(s)) => Some(s.clone()),
                            Some(ImageUrl::Obj { url }) => Some(url.clone()),
                            None => None,
                        };
                        if let Some(url) = url {
                            out.push(json!({ "type": "input_image", "image_url": url }));
                        }
                    }
                    _ => {}
                }
            }
            Value::Array(out)
        }
    }
}

/// Chat Completions `tool_choice` -> Responses. String modes ("auto", "none",
/// "required") pass through unchanged. The specific-function object
/// `{"type":"function","function":{"name":...}}` is flattened to
/// `{"type":"function","name":...}`.
fn convert_tool_choice(tc: &Value) -> Value {
    if tc.get("type").and_then(Value::as_str) == Some("function") {
        if let Some(name) = tc.pointer("/function/name") {
            return json!({ "type": "function", "name": name.clone() });
        }
    }
    tc.clone()
}

/// Reshape one tool for the Responses API. `type:"function"` is flattened from
/// OpenAI's nested `{type, function:{name,...}}` to `{type:"function", name,...}`.
/// Every other tool type is passed through unchanged.
fn convert_tool(tool: &Value) -> Value {
    if tool.get("type").and_then(Value::as_str) != Some("function") {
        return tool.clone();
    }
    let Some(f) = tool.get("function") else {
        return tool.clone();
    };
    json!({
        "type": "function",
        "name": f.get("name").cloned().unwrap_or(Value::Null),
        "description": f.get("description").cloned().unwrap_or(Value::Null),
        "parameters": normalize_schema(f.get("parameters").cloned()),
        "strict": f.get("strict").cloned().unwrap_or(Value::Null),
    })
}

/// OpenAI requires `properties` on object schemas; the Responses backend is
/// equally strict.
fn normalize_schema(schema: Option<Value>) -> Value {
    match schema {
        None => json!({ "type": "object", "properties": {} }),
        Some(Value::Object(mut map)) => {
            if map.get("type").and_then(Value::as_str) == Some("object")
                && !map.contains_key("properties")
            {
                map.insert("properties".into(), json!({}));
            }
            Value::Object(map)
        }
        Some(other) => other,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn parse(body: serde_json::Value) -> ChatCompletionRequest {
        serde_json::from_value(body).unwrap()
    }

    #[test]
    fn maps_system_user_and_reasoning() {
        let req = parse(json!({
            "model": "gpt-4o",
            "messages": [
                { "role": "system", "content": "be terse" },
                { "role": "user", "content": "hello" }
            ]
        }));
        let mut defaults = DefaultsConfig::default();
        defaults
            .model_aliases
            .insert("gpt-4o".into(), "gpt-5-codex".into());

        let body = build_codex_request(&req, &defaults);
        assert_eq!(body["model"], "gpt-5-codex"); // alias applied
        assert_eq!(body["instructions"], "be terse");
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(body["input"][0]["content"], "hello");
        assert_eq!(body["stream"], true);
        assert_eq!(body["reasoning"]["effort"], "medium"); // default
    }

    #[test]
    fn responses_alias_rewrites_a_bare_slug_and_keeps_everything_else() {
        // Codex speaks the Responses wire API, so this is the path that
        // actually matters for a bare name the upstream 400s.
        let defaults = DefaultsConfig::default();
        let body = br#"{"model":"gpt-5.6","store":false,"stream":true,"input":[{"type":"message","role":"user","content":"hi"}]}"#;

        let rewritten = alias_responses_model(body, &defaults).expect("alias applies");
        let parsed: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(parsed["model"], "gpt-5.6-sol");
        // Everything the client sent must survive the round-trip untouched —
        // this endpoint is a passthrough, the model is the only edit.
        assert_eq!(parsed["store"], false);
        assert_eq!(parsed["stream"], true);
        assert_eq!(parsed["input"][0]["content"], "hi");

        let bare_six = br#"{"model":"gpt-6","input":[]}"#;
        let rewritten = alias_responses_model(bare_six, &defaults).expect("alias applies");
        let parsed: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(parsed["model"], "gpt-6-astra");
    }

    #[test]
    fn responses_alias_splices_bytes_and_leaves_the_rest_of_the_body_alone() {
        // The splice must edit exactly the model value: byte-identical prefix
        // and suffix, odd whitespace around the colon included.
        let defaults = DefaultsConfig::default();
        let body = br#"{"store":false, "model"  :   "gpt-5.6" ,"input":[{"text":"gpt-5.6 mentioned in prose"}]}"#;

        let out = alias_responses_model(body, &defaults).expect("alias applies");
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            r#"{"store":false, "model"  :   "gpt-5.6-sol" ,"input":[{"text":"gpt-5.6 mentioned in prose"}]}"#,
            "only the model member may change — a matching string elsewhere must survive"
        );
    }

    #[test]
    fn responses_alias_falls_back_to_the_tree_when_the_scan_cannot_be_trusted() {
        // A nested `"model"` member carrying the same value appears BEFORE the
        // real one, so the byte scan splices the wrong occurrence. The
        // verification re-read catches that and the tree rewrite takes over —
        // the forwarded body must still name the aliased model at top level.
        let defaults = DefaultsConfig::default();
        let body = br#"{"input":[{"type":"mcp_call","model":"gpt-5.6"}],"model":"gpt-5.6"}"#;

        let out = alias_responses_model(body, &defaults).expect("alias applies");
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["model"], "gpt-5.6-sol");
        // The tree path reserializes, so the nested member is preserved by
        // value rather than by byte offset — what matters is that it is intact
        // and was NOT the thing rewritten.
        assert_eq!(parsed["input"][0]["model"], "gpt-5.6");
    }

    #[test]
    fn responses_alias_survives_a_key_ending_in_an_escaped_quote() {
        // `key_precedes` matches on the raw bytes `"model"`, and a member name
        // ending in an escaped quote — `"a\"model"` — ends with exactly those
        // bytes. So the scan happily splices that member instead of the real
        // one. Nothing upstream of the verification notices; the verification
        // is what makes it safe, which is precisely why it is not optional.
        let defaults = DefaultsConfig::default();
        let body = br#"{"a\"model":"gpt-5.6","model":"gpt-5.6"}"#;

        let out = alias_responses_model(body, &defaults).expect("alias applies");
        let parsed: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["model"], "gpt-5.6-sol");
        assert_eq!(
            parsed["a\"model"], "gpt-5.6",
            "the decoy member must be untouched"
        );
    }

    #[test]
    fn responses_alias_leaves_anything_it_cannot_improve_alone() {
        // `None` means "forward the original bytes", so each of these keeps
        // the passthrough byte-exact rather than paying a JSON round-trip.
        let defaults = DefaultsConfig::default();

        // Already a flavored slug — no alias entry, nothing to do.
        assert!(alias_responses_model(br#"{"model":"gpt-6-astra"}"#, &defaults).is_none());
        // An id the map doesn't know. Unlike `resolve_model` on the
        // chat/completions path, this must NOT substitute `defaults.model`:
        // rejecting an unknown id is the upstream's call, not the proxy's.
        assert!(alias_responses_model(br#"{"model":"whatever-1"}"#, &defaults).is_none());
        // No model field at all, and a body that isn't JSON — the proxy has no
        // business erroring on a shape only the upstream defines.
        assert!(alias_responses_model(br#"{"input":[]}"#, &defaults).is_none());
        assert!(alias_responses_model(b"not json at all", &defaults).is_none());
        assert!(alias_responses_model(br#"{"model":42}"#, &defaults).is_none());
        // An identity entry maps a name to itself — rewriting would only
        // reserialize the body into an equivalent one, so it must not fire.
        let identity = DefaultsConfig {
            model_aliases: HashMap::from([("gpt-5".to_string(), "gpt-5".to_string())]),
            ..DefaultsConfig::default()
        };
        assert!(alias_responses_model(br#"{"model":"gpt-5"}"#, &identity).is_none());
        // An empty alias map short-circuits before any parsing.
        let no_aliases = DefaultsConfig {
            model_aliases: HashMap::new(),
            ..DefaultsConfig::default()
        };
        assert!(alias_responses_model(br#"{"model":"gpt-5.6"}"#, &no_aliases).is_none());
    }

    #[test]
    fn built_in_aliases_resolve_bare_slugs_to_their_flavor() {
        // The upstream 400s a bare "gpt-6"/"gpt-5.6" (only the flavored slugs
        // work over a ChatGPT account), which would silently route the request
        // to the paid fallback — the default alias map must keep catching both
        // generations. Verified against the live upstream, which answers
        // `{"detail":"The 'gpt-6' model is not supported when using Codex with
        // a ChatGPT account."}`.
        let defaults = DefaultsConfig::default();
        assert_eq!(resolve_model("gpt-6", &defaults), "gpt-6-astra");
        assert_eq!(resolve_model("gpt-5.6", &defaults), "gpt-5.6-sol");
        assert_eq!(defaults.model, "gpt-6-astra");
    }

    #[test]
    fn falls_back_to_default_instructions() {
        let req = parse(json!({
            "model": "gpt-5-codex",
            "messages": [{ "role": "user", "content": "hi" }]
        }));
        let body = build_codex_request(&req, &DefaultsConfig::default());
        assert_eq!(body["instructions"], "You are a helpful coding assistant.");
    }

    #[test]
    fn maps_assistant_tool_calls_and_results() {
        let req = parse(json!({
            "model": "gpt-5-codex",
            "messages": [
                { "role": "user", "content": "weather?" },
                { "role": "assistant", "content": "",
                  "tool_calls": [{ "id": "call_1", "type": "function",
                    "function": { "name": "get_weather", "arguments": "{}" } }] },
                { "role": "tool", "tool_call_id": "call_1", "content": "sunny" }
            ]
        }));
        let body = build_codex_request(&req, &DefaultsConfig::default());
        let input = body["input"].as_array().unwrap();
        // user, function_call, function_call_output
        assert!(input
            .iter()
            .any(|i| i["type"] == "function_call" && i["call_id"] == "call_1"));
        assert!(input
            .iter()
            .any(|i| i["type"] == "function_call_output" && i["output"] == "sunny"));
    }

    #[test]
    fn flattens_tools_and_defaults_object_properties() {
        let req = parse(json!({
            "model": "gpt-5-codex",
            "messages": [{ "role": "user", "content": "x" }],
            "tools": [{ "type": "function", "function": {
                "name": "f", "description": "d", "parameters": { "type": "object" }
            }}]
        }));
        let body = build_codex_request(&req, &DefaultsConfig::default());
        let tool = &body["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["name"], "f"); // flattened, not nested under "function"
        assert!(tool["parameters"]["properties"].is_object());
    }

    #[test]
    fn passes_hosted_tools_through_verbatim() {
        let req = parse(json!({
            "model": "gpt-5-codex",
            "messages": [{ "role": "user", "content": "search the web" }],
            "tools": [
                { "type": "web_search", "external_web_access": true },
                { "type": "function", "function": { "name": "f", "parameters": { "type": "object", "properties": {} } } }
            ]
        }));
        let body = build_codex_request(&req, &DefaultsConfig::default());
        let tools = body["tools"].as_array().unwrap();
        // web_search passed through untouched
        assert_eq!(tools[0]["type"], "web_search");
        assert_eq!(tools[0]["external_web_access"], true);
        // function still flattened
        assert_eq!(tools[1]["type"], "function");
        assert_eq!(tools[1]["name"], "f");
    }

    #[test]
    fn tool_choice_flattened_and_modes_pass_through() {
        assert_eq!(convert_tool_choice(&json!("auto")), json!("auto"));
        assert_eq!(convert_tool_choice(&json!("required")), json!("required"));
        assert_eq!(
            convert_tool_choice(&json!({ "type": "function", "function": { "name": "f" } })),
            json!({ "type": "function", "name": "f" })
        );
    }
}
