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
