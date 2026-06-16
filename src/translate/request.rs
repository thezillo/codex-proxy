//! chat/completions request -> Codex Responses API request.
//!
//! Mapping (mirrors openai/codex-proxy's openai-to-codex):
//!   system/developer messages   -> `instructions`
//!   user/assistant/tool messages -> `input[]`
//!   assistant.tool_calls         -> `{type:"function_call", call_id, name, arguments}`
//!   tool message                 -> `{type:"function_call_output", call_id, output}`
//!   reasoning_effort             -> `reasoning: {effort, summary}`
//!   tools                        -> flattened `{type:"function", name, ...}`

use serde_json::{json, Value};

use crate::config::DefaultsConfig;
use crate::translate::openai::{ChatCompletionRequest, ImageUrl, MessageContent};

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
