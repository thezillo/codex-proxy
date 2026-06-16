//! Read/write `~/.codex/auth.json` — the file `codex login` produces.
//!
//! Shape mirrors openai/codex so the same file works for both tools:
//! {
//!   "OPENAI_API_KEY": "sk-..."        // optional, API-key auth (unused here)
//!   "tokens": {
//!     "id_token": "<jwt>",
//!     "access_token": "<jwt>",
//!     "refresh_token": "...",
//!     "account_id": "..."             // optional
//!   },
//!   "last_refresh": "2026-01-01T00:00:00Z"
//! }

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AuthDotJson {
    #[serde(rename = "OPENAI_API_KEY", skip_serializing_if = "Option::is_none")]
    pub openai_api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens: Option<Tokens>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refresh: Option<DateTime<Utc>>,
    /// Any other fields present in the file (e.g. ones written by the codex CLI
    /// we don't model). Captured and re-serialized so a refresh never drops them.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Tokens {
    pub id_token: String,
    pub access_token: String,
    pub refresh_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

pub fn auth_file(codex_home: &Path) -> PathBuf {
    codex_home.join("auth.json")
}

/// Seed `auth.json` from the `CODEXPROXY_AUTH_JSON` env var when the file is
/// absent — for cloud deploys where credentials arrive as a secret rather than
/// via `codex login`. Written once; afterwards the on-disk file (with rotated
/// tokens) wins, so token rotation survives restarts when `codex_home` is a
/// persistent volume.
pub fn seed_from_env_if_absent(codex_home: &Path) -> anyhow::Result<()> {
    let path = auth_file(codex_home);
    if path.exists() {
        return Ok(());
    }
    match std::env::var("CODEXPROXY_AUTH_JSON") {
        Ok(content) if !content.trim().is_empty() => {
            std::fs::create_dir_all(codex_home)?;
            write_secret(&path, content.as_bytes())?;
            tracing::info!("seeded auth.json from CODEXPROXY_AUTH_JSON");
        }
        _ => {}
    }
    Ok(())
}

pub fn load(codex_home: &Path) -> anyhow::Result<AuthDotJson> {
    let path = auth_file(codex_home);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {e}", path.display()))?;
    Ok(serde_json::from_str(&text)?)
}

pub fn save(codex_home: &Path, auth: &AuthDotJson) -> anyhow::Result<()> {
    let path = auth_file(codex_home);
    let text = serde_json::to_string_pretty(auth)?;
    // Write to a temp file then rename, to avoid corrupting the file if the
    // process dies mid-write (codex reads the same file). The temp file is
    // created 0600 — it holds access/refresh tokens, so it must never be
    // world-readable regardless of the process umask.
    let tmp = path.with_extension("json.tmp");
    write_secret(&tmp, text.as_bytes())?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Create/overwrite `path` with owner-only (0600) permissions on Unix, write the
/// bytes, and fsync before returning so the rename can't expose a partial file.
fn write_secret(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    // `mode` only applies on creation; tighten an existing temp file too.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_api_key_and_unknown_fields_on_roundtrip() {
        // Simulate what apply_refresh does: load a file with extra fields, then
        // overwrite only the tokens, and confirm nothing else is lost.
        let on_disk = r#"{
            "OPENAI_API_KEY": "sk-keep",
            "tokens": {"id_token":"a","access_token":"b","refresh_token":"c"},
            "last_refresh": "2026-01-01T00:00:00Z",
            "some_codex_field": 123
        }"#;
        let mut auth: AuthDotJson = serde_json::from_str(on_disk).unwrap();
        auth.tokens = Some(Tokens {
            id_token: "new_id".into(),
            access_token: "new_access".into(),
            refresh_token: "new_refresh".into(),
            account_id: Some("acct".into()),
        });

        let out = serde_json::to_value(&auth).unwrap();
        assert_eq!(out["OPENAI_API_KEY"], "sk-keep"); // preserved
        assert_eq!(out["some_codex_field"], 123); // unknown field preserved
        assert_eq!(out["tokens"]["access_token"], "new_access"); // updated
    }
}
