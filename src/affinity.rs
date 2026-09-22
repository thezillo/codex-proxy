//! Conversation keys: which conversation a request belongs to, so every turn
//! of it lands where its prompt prefix is already cached.
//!
//! Prompt caching (OpenAI's, and OpenRouter's, which is just the upstream
//! provider's) only hits on an exact prefix. Every turn of a conversation
//! resends the whole history and appends to it, so the *start* of the request
//! — instructions, tools, first input item — is identical across its turns and
//! differs between conversations. A hash of that start is therefore a
//! conversation id on its own: no similarity scoring needed, and none would
//! help, since two near-identical prompts share zero cached tokens.
//!
//! The key is used twice: `Upstream` picks a pool account from its hash, and
//! the fallback chain forwards its value as `session_id`/`prompt_cache_key`,
//! the fields OpenRouter keys its provider stickiness on.

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// Client headers that identify one conversation, in order of preference.
/// The real Codex CLI sends both on every turn; `session-id` spans the whole
/// session, so it wins; `thread-id` alone still keeps one thread together.
const SESSION_HEADERS: &[&str] = &["session-id", "thread-id"];

/// OpenRouter's documented cap on `session_id`. Longer client values are
/// replaced by a hash-derived id rather than truncated, so two long ids that
/// share a prefix can't collide.
const MAX_SESSION_VALUE_LEN: usize = 256;

/// Where a conversation key came from — logged, so an operator can tell
/// "the client told us" from "we inferred it".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    /// `session-id`/`thread-id` request header.
    Header,
    /// `prompt_cache_key` in the request body.
    Body,
    /// Hash of the request's prefix (instructions, tools, first input item).
    Derived,
}

impl KeySource {
    pub fn as_str(self) -> &'static str {
        match self {
            KeySource::Header => "header",
            KeySource::Body => "body",
            KeySource::Derived => "derived",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationKey {
    /// Picks the pool account. Stable across restarts (FNV-1a).
    pub hash: u64,
    /// Sent to fallback providers as `session_id`/`prompt_cache_key`, and to
    /// the pool as `prompt_cache_key` where the proxy builds the body itself.
    pub value: String,
    pub source: KeySource,
}

impl ConversationKey {
    fn from_value(value: &str, source: KeySource) -> Self {
        let hash = fnv1a(value.as_bytes());
        let value = if value.len() <= MAX_SESSION_VALUE_LEN {
            value.to_string()
        } else {
            derived_value(hash)
        };
        Self {
            hash,
            value,
            source,
        }
    }
}

/// What the proxy reads out of a Responses body: its model, and the fields a
/// conversation key is made of. Deserialized in one pass that borrows the
/// large fields as raw JSON (`RawValue`) — the input array of a long
/// conversation can be many MB, and none of it is copied or built into a tree.
#[derive(Deserialize)]
struct Prefix<'a> {
    model: Option<String>,
    prompt_cache_key: Option<String>,
    #[serde(borrow)]
    instructions: Option<&'a RawValue>,
    #[serde(borrow)]
    tools: Option<&'a RawValue>,
    #[serde(borrow)]
    input: Option<&'a RawValue>,
}

/// A request body, as far as model routing and conversation affinity care.
#[derive(Debug, Default)]
pub struct BodyInfo {
    pub model: Option<String>,
    /// Key from the body alone: `prompt_cache_key` if the client set one,
    /// else the derived prefix hash. `None` when the body isn't a JSON
    /// object or has neither instructions nor input to hash.
    pub key: Option<ConversationKey>,
}

/// Read `body` once for its model and conversation key. A body that isn't
/// JSON (or has duplicate top-level keys, which serde rejects) yields the
/// empty default: nothing is inferred, and the request is forwarded as-is.
pub fn inspect(body: &[u8]) -> BodyInfo {
    let Ok(prefix) = serde_json::from_slice::<Prefix>(body) else {
        return BodyInfo::default();
    };
    let key = match prefix.prompt_cache_key.as_deref() {
        Some(k) if !k.is_empty() => Some(ConversationKey::from_value(k, KeySource::Body)),
        _ => derive(&prefix),
    };
    BodyInfo {
        model: prefix.model,
        key,
    }
}

/// The conversation key for a request: a session header wins, then whatever
/// the body yields (see `BodyInfo::key`).
pub fn resolve(
    headers: &reqwest::header::HeaderMap,
    body_key: Option<ConversationKey>,
) -> Option<ConversationKey> {
    header_key(headers).or(body_key)
}

fn header_key(headers: &reqwest::header::HeaderMap) -> Option<ConversationKey> {
    let value = SESSION_HEADERS
        .iter()
        .find_map(|name| headers.get(*name))
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())?;
    Some(ConversationKey::from_value(value, KeySource::Header))
}

fn derive(prefix: &Prefix<'_>) -> Option<ConversationKey> {
    let first_input = prefix.input.and_then(first_item);
    if prefix.instructions.is_none() && first_input.is_none() {
        return None;
    }
    let mut hash = FNV_OFFSET;
    // A separator between parts, so moving bytes from one part into the
    // next can't produce the same hash.
    for part in [prefix.instructions, prefix.tools, first_input] {
        hash = fnv1a_extend(hash, part.map_or(b"-".as_slice(), |r| r.get().as_bytes()));
        hash = fnv1a_extend(hash, &[0x1f]);
    }
    Some(ConversationKey {
        hash,
        value: derived_value(hash),
        source: KeySource::Derived,
    })
}

fn derived_value(hash: u64) -> String {
    format!("cp-{hash:016x}")
}

/// The first element of `input` when it's an array, or the whole value when
/// it's a string (the Responses API accepts both). The rest of the array is
/// skipped without being parsed into anything.
fn first_item(input: &RawValue) -> Option<&RawValue> {
    if input.get().trim_start().starts_with('[') {
        serde_json::from_str::<FirstElement>(input.get()).ok()?.0
    } else {
        Some(input)
    }
}

struct FirstElement<'a>(Option<&'a RawValue>);

impl<'de: 'a, 'a> Deserialize<'de> for FirstElement<'a> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FirstVisitor;
        impl<'de> Visitor<'de> for FirstVisitor {
            type Value = FirstElement<'de>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let first = seq.next_element::<&'de RawValue>()?;
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(FirstElement(first))
            }
        }
        deserializer.deserialize_seq(FirstVisitor)
    }
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a rather than `std`'s hasher: the value picks a pool account, so it
/// must not change across restarts or Rust versions, or every live
/// conversation would move accounts (and lose its prompt cache) on each deploy.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_extend(FNV_OFFSET, bytes)
}

fn fnv1a_extend(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderMap;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (name, value) in pairs {
            h.insert(*name, value.parse().unwrap());
        }
        h
    }

    #[test]
    fn fnv1a_is_pinned_so_conversations_keep_their_account_across_deploys() {
        assert_eq!(fnv1a(b"sess-abc"), 0x7dcf_79c7_f6fa_627e);
    }

    #[test]
    fn a_session_header_wins_over_anything_in_the_body() {
        let body = inspect(br#"{"prompt_cache_key":"pck","instructions":"x","input":"hi"}"#);
        let key = resolve(&headers(&[("session-id", "sess-abc")]), body.key).unwrap();
        assert_eq!(key.source, KeySource::Header);
        assert_eq!(key.value, "sess-abc");
        assert_eq!(key.hash, fnv1a(b"sess-abc"));
        // session-id over thread-id; thread-id alone still counts.
        let both = headers(&[("session-id", "sess-abc"), ("thread-id", "t")]);
        assert_eq!(resolve(&both, None).unwrap().value, "sess-abc");
        assert_eq!(
            resolve(&headers(&[("thread-id", "t")]), None)
                .unwrap()
                .value,
            "t"
        );
    }

    #[test]
    fn a_client_prompt_cache_key_wins_over_the_derived_one() {
        let info = inspect(br#"{"model":"gpt-6-astra","prompt_cache_key":"pck","input":"hi"}"#);
        assert_eq!(info.model.as_deref(), Some("gpt-6-astra"));
        let key = info.key.unwrap();
        assert_eq!((key.source, key.value.as_str()), (KeySource::Body, "pck"));
    }

    #[test]
    fn every_turn_of_a_conversation_derives_the_same_key() {
        let turn1 = br#"{"instructions":"be terse","tools":[{"type":"function","name":"ls"}],
            "input":[{"role":"user","content":"fix the bug"}]}"#;
        let turn2 = br#"{"instructions":"be terse","tools":[{"type":"function","name":"ls"}],
            "input":[{"role":"user","content":"fix the bug"},
                     {"role":"assistant","content":"done"},
                     {"role":"user","content":"now test it"}]}"#;
        let k1 = inspect(turn1).key.unwrap();
        let k2 = inspect(turn2).key.unwrap();
        assert_eq!(k1, k2);
        assert_eq!(k1.source, KeySource::Derived);
        assert!(k1.value.starts_with("cp-") && k1.value.len() == 19);
    }

    #[test]
    fn different_conversations_derive_different_keys() {
        let a = inspect(br#"{"instructions":"x","input":[{"role":"user","content":"task A"}]}"#);
        let b = inspect(br#"{"instructions":"x","input":[{"role":"user","content":"task B"}]}"#);
        assert_ne!(a.key.unwrap().hash, b.key.unwrap().hash);
        // Same text moved between parts must not collide.
        let c = inspect(br#"{"instructions":"ab","input":"c"}"#);
        let d = inspect(br#"{"instructions":"a","input":"bc"}"#);
        assert_ne!(c.key.unwrap().hash, d.key.unwrap().hash);
    }

    #[test]
    fn nothing_to_hash_or_not_json_yields_no_key() {
        assert!(inspect(br#"{"model":"gpt-6-astra"}"#).key.is_none());
        assert!(inspect(br#"{"input":[]}"#).key.is_none());
        let junk = inspect(b"not json");
        assert!(junk.key.is_none() && junk.model.is_none());
    }

    #[test]
    fn an_overlong_client_id_is_replaced_by_a_hash_not_truncated() {
        let long = "s".repeat(300);
        let key = resolve(&headers(&[("session-id", long.as_str())]), None).unwrap();
        assert_eq!(key.value, format!("cp-{:016x}", fnv1a(long.as_bytes())));
    }
}
