//! Minimal JWT payload decoding for Codex auth tokens.
//!
//! Ported and trimmed from openai/codex (codex-rs/login/src/token_data.rs),
//! licensed Apache-2.0. See the NOTICE file. We only need two things from the
//! tokens: the access-token expiry, and the ChatGPT account id claim.

use base64::Engine;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("invalid JWT format")]
    InvalidFormat,
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Deserialize)]
struct StandardClaims {
    #[serde(default)]
    exp: Option<i64>,
}

#[derive(Deserialize)]
struct IdClaims {
    #[serde(rename = "https://api.openai.com/auth", default)]
    auth: Option<AuthClaims>,
}

#[derive(Deserialize)]
struct AuthClaims {
    #[serde(default)]
    chatgpt_account_id: Option<String>,
}

/// Decode the (unverified) payload section of a JWT into `T`.
///
/// We never verify the signature — we are not the token's audience, we just
/// relay it upstream where it *is* verified. We only read claims to know when
/// to refresh and which account header to send.
fn decode_payload<T: DeserializeOwned>(jwt: &str) -> Result<T, JwtError> {
    let mut parts = jwt.split('.');
    let payload_b64 = match (parts.next(), parts.next(), parts.next()) {
        (Some(h), Some(p), Some(s)) if !h.is_empty() && !p.is_empty() && !s.is_empty() => p,
        _ => return Err(JwtError::InvalidFormat),
    };
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload_b64)?;
    Ok(serde_json::from_slice(&bytes)?)
}

/// Expiry timestamp from the `exp` claim, if present.
pub fn expiration(jwt: &str) -> Result<Option<DateTime<Utc>>, JwtError> {
    let claims: StandardClaims = decode_payload(jwt)?;
    Ok(claims
        .exp
        .and_then(|exp| DateTime::<Utc>::from_timestamp(exp, 0)))
}

/// `chatgpt_account_id` from the id_token claims, if present.
///
/// Distinguishes "decode failed" (logged — a real problem) from "claim simply
/// absent" (silent — fine, e.g. a personal account with no workspace).
pub fn account_id(id_token_jwt: &str) -> Option<String> {
    match decode_payload::<IdClaims>(id_token_jwt) {
        Ok(claims) => claims.auth.and_then(|a| a.chatgpt_account_id),
        Err(e) => {
            tracing::warn!("could not decode id_token for account_id ({e}); omitting ChatGPT-Account-ID header");
            None
        }
    }
}
