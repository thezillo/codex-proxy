//! Shared test-only fixtures for building fake `auth.json` files and unsigned
//! JWTs — used by both `server.rs` and `upstream.rs` tests, which each need
//! independent `codex_home` directories to exercise auth against a fake
//! upstream.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine;
use serde_json::json;

/// A `usage_limit_reached` 429 body shaped like the Codex backend's, shared
/// by the `upstream` and `server` quota tests. `resets_at` is far in the
/// future (2100) so the parsed hold is the clamp ceiling rather than
/// something that could lapse mid-test.
pub(crate) const USAGE_LIMIT_429_BODY: &str = r#"{"error":{"type":"usage_limit_reached","message":"You've hit your usage limit.","plan_type":"plus","resets_at":4102444800}}"#;

/// Write a fake `auth.json` (with a far-future `exp` so it never needs a
/// refresh mid-test) into a fresh temp `codex_home`, carrying `account_id` as
/// the id_token's `chatgpt_account_id` claim, and return that directory.
pub(crate) fn write_test_auth_json(account_id: &str) -> PathBuf {
    let codex_home = unique_temp_dir();
    std::fs::create_dir_all(&codex_home).unwrap();

    let access_token = unsigned_jwt(json!({ "exp": 4_102_444_800_i64 }));
    let id_token = unsigned_jwt(json!({
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id
        }
    }));
    let auth_json = json!({
        "tokens": {
            "id_token": id_token,
            "access_token": access_token,
            "refresh_token": "refresh_test"
        }
    });
    std::fs::write(
        codex_home.join("auth.json"),
        serde_json::to_vec(&auth_json).unwrap(),
    )
    .unwrap();

    codex_home
}

/// A JWT with an unsigned ("none") header — enough for `auth::jwt`'s
/// unverified payload decoding, which is all this proxy ever does with it.
pub(crate) fn unsigned_jwt(payload: serde_json::Value) -> String {
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header = engine.encode(br#"{"alg":"none"}"#);
    let payload = engine.encode(serde_json::to_vec(&payload).unwrap());
    format!("{header}.{payload}.signature")
}

/// A fresh, never-reused temp directory path (not created — callers decide
/// when), so concurrent tests never collide on the same `codex_home`.
pub(crate) fn unique_temp_dir() -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "codex-proxy-test-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

/// OpenAI's `invalid_encrypted_content` 400 body (openai/codex#17541).
pub(crate) const INVALID_ENCRYPTED_400_BODY: &str = r#"{"error":{"message":"The encrypted content gAAA...= could not be verified. Reason: Encrypted content could not be decrypted or parsed.","type":"invalid_request_error","param":null,"code":"invalid_encrypted_content"}}"#;

/// A Responses body replaying a reasoning item minted by some other
/// upstream, between a user message and a tool call/output pair.
pub(crate) const FOREIGN_REPLAY_BODY: &str = r#"{"model":"gpt-5.5","input":[{"type":"message","role":"user","content":"hi"},{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"gAAA-foreign"},{"type":"function_call","call_id":"c1","name":"sh","arguments":"{}"},{"type":"function_call_output","call_id":"c1","output":"ok"}]}"#;

/// One request a `ReplayRejectingUpstream` received.
pub(crate) struct ReplayRequest {
    pub headers: axum::http::HeaderMap,
    pub body: serde_json::Value,
}

/// Fake upstream that, like a real one handed another upstream's encrypted
/// state, answers `INVALID_ENCRYPTED_400_BODY` to any body replaying an
/// `encrypted_content` — or to every body, with `always_reject` — and 200
/// otherwise. Serves `path`, and records every request in order.
pub(crate) struct ReplayRejectingUpstream {
    pub base_url: String,
    pub rx: tokio::sync::mpsc::Receiver<ReplayRequest>,
}

pub(crate) async fn start_replay_rejecting_upstream(
    path: &str,
    always_reject: bool,
) -> ReplayRejectingUpstream {
    use axum::extract::State;
    use axum::response::Response;

    type Tx = tokio::sync::mpsc::Sender<ReplayRequest>;
    async fn handle(
        State((tx, always_reject)): State<(Tx, bool)>,
        headers: axum::http::HeaderMap,
        body: bytes::Bytes,
    ) -> Response {
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let replays_encrypted = body["input"]
            .as_array()
            .is_some_and(|items| items.iter().any(|i| i.get("encrypted_content").is_some()));
        tx.send(ReplayRequest { headers, body }).await.unwrap();
        let (status, reply) = if always_reject || replays_encrypted {
            (400, INVALID_ENCRYPTED_400_BODY)
        } else {
            (200, r#"{"ok":true}"#)
        };
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(axum::body::Body::from(reply))
            .unwrap()
    }

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let app = axum::Router::new()
        .route(path, axum::routing::post(handle))
        .with_state((tx, always_reject));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    ReplayRejectingUpstream {
        base_url: format!("http://{addr}"),
        rx,
    }
}

/// The `type` of every input item, in order.
pub(crate) fn input_types(body: &serde_json::Value) -> Vec<&str> {
    body["input"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["type"].as_str().unwrap())
        .collect()
}
