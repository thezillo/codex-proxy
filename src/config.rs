//! Configuration: TOML file + env overrides, with coding-friendly defaults.
//!
//! Resolution order (lowest to highest priority):
//!   built-in defaults  ->  config.toml  ->  CODEXPROXY_* env vars

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub client_auth: ClientAuthConfig,
    pub upstream: UpstreamConfig,
    pub defaults: DefaultsConfig,
    pub models: ModelsConfig,
    pub logging: LoggingConfig,
    /// Secondary Responses-API-compatible providers (Azure OpenAI, OpenRouter,
    /// ...), tried in order — only once the whole ChatGPT account pool has
    /// failed. Empty by default: existing deployments are unaffected.
    pub fallback: Vec<FallbackProviderConfig>,
    /// `POST /v1/embeddings`, routed directly to one of the `fallback` entries
    /// above (by name). `None` (the default) leaves the endpoint answering 404.
    pub embeddings: Option<EmbeddingsConfig>,
}

/// One fallback provider. Unlike most other config structs, this is NOT
/// `#[serde(default)]`'d field-by-field: a `[[fallback]]` entry missing
/// `name`/`base_url`/`auth_style`/`api_key`/`model_map` is a misconfiguration
/// that should fail config load, not silently produce a dead fallback entry.
#[derive(Debug, Clone, Deserialize)]
pub struct FallbackProviderConfig {
    /// Access-log label (e.g. "azure", "openrouter") — also used to build
    /// this provider's `CODEXPROXY_FALLBACK_{NAME}_API_KEY` env override name.
    pub name: String,
    pub base_url: String,
    #[serde(default = "default_fallback_responses_path")]
    pub responses_path: String,
    /// "api-key" (sends `api-key: <key>`, Azure-style) or "bearer" (sends
    /// `Authorization: Bearer <key>`, OpenRouter-style). Kept a plain string,
    /// validated at `FallbackChain` construction — matches this project's
    /// existing lightweight config style (no enum + custom Deserialize).
    pub auth_style: String,
    pub api_key: String,
    /// Maps the model id actually present in the outbound request body (the
    /// client-facing id for `/v1/chat/completions`'s translated body, or
    /// whatever the client itself sent for `/v1/responses`) to this
    /// provider's own model/deployment string. A model absent from this map
    /// means this provider is skipped for that request — never guessed.
    pub model_map: HashMap<String, String>,
    /// Send the request's conversation key as `session_id` and
    /// `prompt_cache_key` (each only when the client didn't set it) — the
    /// fields OpenRouter keeps provider stickiness on, so every turn of a
    /// conversation reaches the provider already holding its prompt cache.
    /// Off by default: a provider that validates its request schema strictly
    /// (Azure OpenAI) may reject the unknown `session_id` field. Turn it on
    /// for OpenRouter.
    #[serde(default)]
    pub sticky_session: bool,
}

fn default_fallback_responses_path() -> String {
    "/responses".to_string()
}

/// `[embeddings]`: which declared `[[fallback]]` provider serves
/// `POST /v1/embeddings`, and how client-facing model ids map to its own.
/// Like `FallbackProviderConfig`, not `#[serde(default)]`'d field-by-field: a
/// section missing `provider`/`model_map` is a misconfiguration, not a
/// silently dead endpoint. That the provider exists is checked in
/// `embeddings::EmbeddingsUpstream::from_config`, at startup.
#[derive(Debug, Clone, Deserialize)]
pub struct EmbeddingsConfig {
    pub provider: String,
    /// Appended to the provider's `base_url`, the way `responses_path` is.
    #[serde(default = "default_embeddings_path")]
    pub path: String,
    pub model_map: HashMap<String, String>,
}

fn default_embeddings_path() -> String {
    "/embeddings".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    /// Max request body the proxy will buffer before forwarding, in bytes.
    /// Overrides Axum's 2 MB default for the `Bytes` extractor, which would
    /// otherwise 413 large contexts or base64 image payloads before proxying.
    pub max_body_bytes: usize,
    /// Port for `/metrics` (Prometheus scrape) — a SEPARATE HTTP server from
    /// the client-facing API above, so it can be network-isolated (Fly
    /// private networking, k8s NetworkPolicy) independently of whether the
    /// main API is publicly exposed. `0` disables the metrics server
    /// entirely (metrics are still recorded in memory, just never served).
    pub metrics_port: u16,
    /// Bind host for the metrics server. Deliberately does NOT default to
    /// (or inherit) `host` above — that may be `0.0.0.0`, and metrics
    /// shouldn't become publicly reachable just because the main API is.
    pub metrics_host: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClientAuthConfig {
    pub keys: Vec<ClientKey>,
    pub require: bool,
}

/// One accepted client credential.
#[derive(Debug, Clone, Deserialize)]
pub struct ClientKey {
    /// Either a raw secret ("sk-...", compared directly) or `sha256:<hex>` —
    /// a digest, so the config never holds the actual bearer value. Generate
    /// with e.g. `printf '%s' 'the-real-key' | shasum -a 256`.
    pub key: String,
    /// Friendly label for access logs, so token spend is attributed to a
    /// name instead of an opaque fingerprint (`key-XXXXXXXX`).
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    /// Directory holding Codex credentials. Auto-discovered as a round-robin
    /// pool: if `data_dir/auth.json` exists, `data_dir` itself is an account;
    /// every immediate subdirectory that has its own `auth.json` is an
    /// additional account. No accounts on disk yet (first boot) still yields
    /// `data_dir` as the lone account, so `codex login`/`CODEXPROXY_AUTH_JSON`
    /// have somewhere to write. Add an account by dropping a new
    /// subdirectory's `auth.json` in and restarting — nothing to list here.
    pub data_dir: String,
    /// Friendly label per account in access logs, keyed by directory basename
    /// (`data_dir`'s own basename for the top-level account, or the
    /// subdirectory name for each additional one). Entries absent here fall
    /// back to that basename.
    pub account_names: HashMap<String, String>,
    pub base_url: String,
    pub responses_path: String,
    pub issuer: String,
    pub client_id: String,
    pub originator: String,
    /// Codex CLI version we impersonate in the upstream User-Agent. Only the
    /// version is configured here; the `(OsType os_version; arch)` part is
    /// generated from `os_info` at runtime, and there is no `codex-proxy`
    /// suffix — both so the UA can't fingerprint the proxy to ChatGPT. Bump
    /// this (or set `CODEXPROXY_CLI_VERSION`) when the real Codex CLI bumps.
    pub cli_version: String,
    pub refresh_skew_secs: i64,
    pub request_timeout_secs: u64,
    pub connect_timeout_secs: u64,
    /// Optional outbound proxy for ALL upstream traffic (forward + token
    /// refresh). Supports `http://`, `https://`, and `socks5://`, with optional
    /// `user:pass@` credentials. Useful when OpenAI blocks the deploy region/IP.
    /// Empty/absent = direct connection (system proxy env vars still honored).
    pub proxy: Option<String>,
    /// How long a pool account is skipped by round-robin after it fails with
    /// 401 (post-retry)/403/429 — avoids wasting a request on an account
    /// that's still probably banned/rate-limited/broken, without any active
    /// health-checking. Only affects account *selection*; a request already
    /// mid-failover still tries every account regardless of cooldown state.
    ///
    /// When every account is cooling down (or quota-exhausted, see
    /// `quota_check_interval_secs`), what happens depends on whether a
    /// `[[fallback]]` chain exists: without one the pool still tries an
    /// account — a shaky account beats refusing the request outright; with
    /// one the request goes straight to the fallback chain without an
    /// upstream round-trip, since a working fallback beats a request we just
    /// saw fail.
    pub account_cooldown_secs: u64,
    /// Path (appended to `base_url`) of the ChatGPT usage endpoint the
    /// quota poller queries. Internal/undocumented, but the real Codex CLI
    /// calls it too (for `/status`), so hitting it is not a fingerprint.
    pub usage_path: String,
    /// Quota-aware account state. A 429 whose body says
    /// `usage_limit_reached` is not a transient throttle: it's a hard limit
    /// with a known reset time (hours for the 5h window, days for the
    /// weekly one). Such an account is marked quota-exhausted until that
    /// reset — separately from the short `account_cooldown_secs` — and
    /// skipped by selection meanwhile. While any account is in that state,
    /// the poller re-reads `usage_path` for it every this many seconds and
    /// clears the state as soon as the usage report says the quota is back,
    /// which can happen before the originally reported reset (manual reset,
    /// plan change). `0` disables polling: the state then only clears when
    /// the reset time reported on the 429 passes.
    pub quota_check_interval_secs: u64,
}

/// Coding-friendly request defaults, applied when the client omits a field.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DefaultsConfig {
    /// Upstream model used when the request's model isn't a known alias/id.
    pub model: String,
    /// low | medium | high | xhigh — injected as reasoning.effort when the
    /// client doesn't send reasoning_effort.
    pub reasoning_effort: String,
    /// reasoning.summary value ("auto" | "concise" | "detailed" | "none").
    pub reasoning_summary: String,
    /// Base instructions when no system/developer message is present.
    pub instructions: String,
    /// Emit Codex reasoning as `reasoning_content` deltas to the client.
    pub include_reasoning: bool,
    /// Map incoming model names to upstream model ids (e.g. "gpt-4o" -> "gpt-5-codex").
    pub model_aliases: HashMap<String, String>,
}

/// `[models]`: which model ids the proxy accepts, and what it does when the
/// subscription pool can't serve one — the cost guardrails in front of the
/// paid `[[fallback]]` chain.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ModelsConfig {
    /// Reject a request whose model (after `defaults.model_aliases`) is not a
    /// model this proxy knows — the advertised `/v1/models` list plus
    /// `extra` below — with a 400 `model_not_found`, before it reaches the
    /// pool or the paid fallback. Off = forward anything, as before.
    pub reject_unknown: bool,
    /// Additional accepted model ids beyond the built-in advertised list
    /// (e.g. a new upstream slug this build doesn't know yet). Accepted, not
    /// advertised.
    pub extra: Vec<String>,
    /// Model downgrade chain inside the subscription pool: when the pool
    /// throttles a model with a plain 429 (NOT a `usage_limit_reached` quota
    /// error, which is account-wide and would fail the same way for every
    /// model), the request is retried on the pool with the model named here,
    /// following the chain (`a -> b -> c`) until one is served or the chain
    /// ends — and only then goes to the paid `[[fallback]]` chain, with the
    /// client's ORIGINAL model. Empty map = no downgrades.
    pub downgrades: HashMap<String, String>,
    /// When the pool rejects a request with a 4xx other than 401/403/429
    /// (typically `400 "model is not supported when using Codex with a
    /// ChatGPT account"`), send it to the paid `[[fallback]]` chain anyway.
    /// Off by default: such a rejection is the same for every account, so
    /// retrying it on a paid provider only hides a client misconfiguration
    /// behind a bill. The client gets the pool's own error instead.
    pub fallback_on_bad_request: bool,
}

impl Default for ModelsConfig {
    fn default() -> Self {
        Self {
            reject_unknown: true,
            extra: Vec::new(),
            downgrades: HashMap::from([("gpt-6-astra".to_string(), "gpt-5.6-sol".to_string())]),
            fallback_on_bad_request: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
    /// "text" (human-readable, default) or "json" (one structured object per
    /// line). Use "json" on hosted deploys (e.g. Fly) so access lines can be
    /// queried/aggregated — e.g. summing `total_tokens` grouped by `client`.
    pub format: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8787,
            // 16 MiB: covers long conversations and a few base64 images while
            // bounding how much a single request can buffer on a small (256 MB)
            // VM — at hard_limit=60 connections even this is generous. Raise via
            // CODEXPROXY_MAX_BODY_BYTES when the host has memory headroom.
            max_body_bytes: 16 * 1024 * 1024,
            metrics_port: 9090,
            metrics_host: "127.0.0.1".to_string(),
        }
    }
}

/// Built-in placeholder client key used when no config/env supplies one. Known
/// publicly, so it must never guard a non-loopback deployment — startup refuses
/// to bind a public interface while this key is in the accepted set.
pub const DEFAULT_CLIENT_KEY: &str = "sk-local-changeme";

/// Default Codex CLI version impersonated in the upstream User-Agent. Bump when
/// the real Codex CLI bumps, or override via config/`CODEXPROXY_CLI_VERSION`.
///
/// Keep this at or above the highest `minimal_client_version` in the Codex
/// model catalog for the models we advertise: the catalog gates gpt-6-astra on
/// 0.153.0, and while the upstream does not currently enforce it server-side
/// (gpt-6-astra answers fine under an older UA), a request for a model no
/// build of that CLI version could offer is a needless fingerprint.
pub const DEFAULT_CLI_VERSION: &str = "0.153.4";

impl Default for ClientAuthConfig {
    fn default() -> Self {
        Self {
            keys: vec![ClientKey {
                key: DEFAULT_CLIENT_KEY.to_string(),
                name: None,
            }],
            require: true,
        }
    }
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            data_dir: "~/.codex".to_string(),
            account_names: HashMap::new(),
            base_url: "https://chatgpt.com/backend-api".to_string(),
            responses_path: "/codex/responses".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            // Public OAuth client id used by the Codex CLI for token refresh.
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann".to_string(),
            originator: "codex_cli_rs".to_string(),
            cli_version: DEFAULT_CLI_VERSION.to_string(),
            refresh_skew_secs: 300,
            request_timeout_secs: 600,
            connect_timeout_secs: 30,
            proxy: None,
            account_cooldown_secs: 30,
            usage_path: "/wham/usage".to_string(),
            quota_check_interval_secs: 600,
        }
    }
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            model: "gpt-6-astra".to_string(),
            reasoning_effort: "medium".to_string(),
            reasoning_summary: "auto".to_string(),
            instructions: "You are a helpful coding assistant.".to_string(),
            include_reasoning: false,
            // The Codex upstream only accepts the *flavored* slugs over a
            // ChatGPT account — a bare "gpt-6"/"gpt-5.6" gets 400 "model is not
            // supported" and would silently burn the paid fallback instead of
            // the subscription pool. Both generations name their flavor the
            // same way, so both need an alias onto the CLI's own default
            // flavor (gpt-6-astra, gpt-5.6-sol). Applied on /v1/responses as
            // well as /v1/chat/completions: Codex itself speaks the Responses
            // wire API, so an alias that only covered the translation path
            // would miss the client that needs it most.
            model_aliases: HashMap::from([
                ("gpt-6".to_string(), "gpt-6-astra".to_string()),
                ("gpt-5.6".to_string(), "gpt-5.6-sol".to_string()),
            ]),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "text".to_string(),
        }
    }
}

impl Config {
    /// Load config from `path` (if it exists) and then apply env overrides.
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let mut cfg = match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str::<Config>(&text)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("config file {path} not found, using built-in defaults");
                Config::default()
            }
            Err(e) => return Err(e.into()),
        };
        cfg.apply_env_overrides();
        Ok(cfg)
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("CODEXPROXY_HOST") {
            self.server.host = v;
        }
        if let Ok(v) = std::env::var("CODEXPROXY_PORT") {
            match v.parse() {
                Ok(p) => self.server.port = p,
                Err(_) => tracing::warn!(
                    "ignoring invalid CODEXPROXY_PORT={v:?}; using {}",
                    self.server.port
                ),
            }
        }
        if let Ok(v) = std::env::var("CODEXPROXY_MAX_BODY_BYTES") {
            match v.parse() {
                Ok(n) => self.server.max_body_bytes = n,
                Err(_) => tracing::warn!(
                    "ignoring invalid CODEXPROXY_MAX_BODY_BYTES={v:?}; using {}",
                    self.server.max_body_bytes
                ),
            }
        }
        if let Ok(v) = std::env::var("CODEXPROXY_METRICS_HOST") {
            self.server.metrics_host = v;
        }
        if let Ok(v) = std::env::var("CODEXPROXY_METRICS_PORT") {
            match v.parse() {
                Ok(p) => self.server.metrics_port = p,
                Err(_) => tracing::warn!(
                    "ignoring invalid CODEXPROXY_METRICS_PORT={v:?}; using {}",
                    self.server.metrics_port
                ),
            }
        }
        // Comma-separated list of client keys (unnamed — names are config.toml-only).
        if let Ok(v) = std::env::var("CODEXPROXY_API_KEYS") {
            self.client_auth.keys = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(|key| ClientKey { key, name: None })
                .collect();
        }
        if let Ok(v) = std::env::var("CODEXPROXY_DATA_DIR") {
            self.upstream.data_dir = v;
        }
        if let Ok(v) = std::env::var("CODEXPROXY_CLI_VERSION") {
            self.upstream.cli_version = v;
        }
        if let Ok(v) = std::env::var("CODEXPROXY_PROXY") {
            self.upstream.proxy = if v.trim().is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("CODEXPROXY_LOG") {
            self.logging.level = v;
        }
        if let Ok(v) = std::env::var("CODEXPROXY_LOG_FORMAT") {
            self.logging.format = v;
        }
        for provider in &mut self.fallback {
            let env_name = format!(
                "CODEXPROXY_FALLBACK_{}_API_KEY",
                normalize_env_suffix(&provider.name)
            );
            if let Ok(v) = std::env::var(&env_name) {
                provider.api_key = v;
            }
        }
    }

    /// The ChatGPT-account pool in round-robin order: `(absolute directory,
    /// access-log label)`. Always non-empty. Auto-discovered from
    /// `upstream.data_dir` — see that field's docs for the discovery rule.
    pub fn account_pool(&self) -> Vec<(PathBuf, String)> {
        discover_accounts(&self.primary_data_dir(), &self.upstream.account_names)
    }

    /// `upstream.data_dir`, tilde-expanded — the directory `CODEXPROXY_AUTH_JSON`
    /// seeding targets. Callers must seed here (and do it *before* calling
    /// `account_pool()`, not after): `account_pool()`'s discovery excludes
    /// this directory whenever any subdirectory account already exists and
    /// this one has no `auth.json` of its own yet (see `discover_accounts`),
    /// so seeding after discovery — or seeding whatever `account_pool()[0]`
    /// happened to be — can silently target the wrong directory, or a
    /// directory that's already provisioned and skip the actual seed.
    pub fn primary_data_dir(&self) -> PathBuf {
        expand_tilde(&self.upstream.data_dir)
    }
}

/// Turn a fallback provider's `name` into the suffix of its env-override
/// var: uppercased, with any byte that isn't `[A-Z0-9_]` replaced by `_` —
/// e.g. "azure-eastus" -> `CODEXPROXY_FALLBACK_AZURE_EASTUS_API_KEY`.
pub(crate) fn normalize_env_suffix(name: &str) -> String {
    name.to_uppercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Label an account directory: the configured `account_names` entry for its
/// basename, else the basename itself.
fn account_label(dir: &Path, names: &HashMap<String, String>) -> String {
    let basename = dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| dir.to_string_lossy().into_owned());
    names.get(&basename).cloned().unwrap_or(basename)
}

/// Discover the account pool under `data_dir`: every immediate subdirectory
/// holding its own `auth.json` is an account, plus `data_dir` itself if it
/// directly holds `auth.json` — or, when no accounts exist on disk at all
/// (first boot), `data_dir` alone so credentials have somewhere to land.
/// Subdirectories are sorted by name for a stable round-robin order across
/// restarts.
fn discover_accounts(data_dir: &Path, names: &HashMap<String, String>) -> Vec<(PathBuf, String)> {
    let mut subdirs: Vec<PathBuf> = std::fs::read_dir(data_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.is_dir() && p.join("auth.json").is_file())
                .collect()
        })
        .unwrap_or_default();
    subdirs.sort();

    let mut pool: Vec<(PathBuf, String)> = Vec::with_capacity(subdirs.len() + 1);
    if data_dir.join("auth.json").is_file() || subdirs.is_empty() {
        pool.push((data_dir.to_path_buf(), account_label(data_dir, names)));
    }
    pool.extend(subdirs.into_iter().map(|dir| {
        let label = account_label(&dir, names);
        (dir, label)
    }));
    pool
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::unique_temp_dir;

    fn cfg_with_data_dir(data_dir: &Path) -> Config {
        let mut cfg = Config::default();
        cfg.upstream.data_dir = data_dir.to_string_lossy().into_owned();
        cfg
    }

    fn touch_auth_json(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("auth.json"), b"{}").unwrap();
    }

    #[test]
    fn shipped_config_toml_parses_and_its_aliases_pass_the_model_gate() {
        let cfg: Config =
            toml::from_str(include_str!("../config.toml")).expect("config.toml parses");
        assert!(cfg.models.reject_unknown);
        assert_eq!(
            cfg.models.downgrades.get("gpt-6-astra").map(String::as_str),
            Some("gpt-5.6-sol")
        );
        // Every alias target must be a model the unknown-model gate accepts,
        // or the alias would turn a working request into a 400.
        for target in cfg.defaults.model_aliases.values() {
            assert!(
                target.starts_with("gpt-6") || target.starts_with("gpt-5.6"),
                "alias target {target} would be rejected"
            );
        }
    }

    #[test]
    fn account_pool_falls_back_to_data_dir_when_nothing_on_disk_yet() {
        // First boot: data_dir doesn't even exist yet. Still yields one
        // account (itself) so credentials have somewhere to land.
        let data_dir = unique_temp_dir();
        let pool = cfg_with_data_dir(&data_dir).account_pool();
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].0, data_dir);
    }

    #[test]
    fn account_pool_uses_data_dir_directly_when_it_has_auth_json() {
        let data_dir = unique_temp_dir();
        touch_auth_json(&data_dir);

        let pool = cfg_with_data_dir(&data_dir).account_pool();
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].0, data_dir);
    }

    #[test]
    fn account_pool_discovers_subdirectories_with_auth_json() {
        let data_dir = unique_temp_dir();
        std::fs::create_dir_all(&data_dir).unwrap();
        touch_auth_json(&data_dir.join("alice"));
        touch_auth_json(&data_dir.join("bob"));

        let pool = cfg_with_data_dir(&data_dir).account_pool();
        // No auth.json directly in data_dir -> only the two subdirectories,
        // sorted by name for a stable round-robin order.
        assert_eq!(pool.len(), 2);
        assert_eq!(pool[0].0, data_dir.join("alice"));
        assert_eq!(pool[0].1, "alice");
        assert_eq!(pool[1].0, data_dir.join("bob"));
        assert_eq!(pool[1].1, "bob");
    }

    #[test]
    fn primary_data_dir_is_data_dir_even_when_pool_zero_would_be_a_subdirectory() {
        // Exact precondition of `account_pool_discovers_subdirectories_with_auth_json`
        // above: data_dir has subdirectory accounts but no auth.json of its
        // own, so account_pool()[0] is "alice", NOT data_dir. Seeding must
        // still target data_dir itself here, not whatever pool()[0] is —
        // this is what CODEXPROXY_AUTH_JSON seeding in main.rs relies on.
        let data_dir = unique_temp_dir();
        std::fs::create_dir_all(&data_dir).unwrap();
        touch_auth_json(&data_dir.join("alice"));
        let cfg = cfg_with_data_dir(&data_dir);

        assert_eq!(cfg.account_pool()[0].0, data_dir.join("alice"));
        assert_eq!(cfg.primary_data_dir(), data_dir);

        // Simulating what `seed_from_env_if_absent(&cfg.primary_data_dir())`
        // does: once data_dir actually has an auth.json, it joins the pool
        // (and sorts first, ahead of "alice") — proving the fix's ordering
        // (seed data_dir, then call account_pool()) actually includes the
        // seeded account, unlike seeding after discovery would have.
        touch_auth_json(&data_dir);
        let pool = cfg.account_pool();
        assert_eq!(pool.len(), 2);
        assert_eq!(pool[0].0, data_dir);
    }

    #[test]
    fn account_pool_ignores_subdirectories_without_auth_json() {
        let data_dir = unique_temp_dir();
        touch_auth_json(&data_dir);
        std::fs::create_dir_all(data_dir.join("not-an-account")).unwrap();

        let pool = cfg_with_data_dir(&data_dir).account_pool();
        assert_eq!(pool.len(), 1);
        assert_eq!(pool[0].0, data_dir);
    }

    #[test]
    fn account_pool_includes_top_level_plus_subdirectories() {
        let data_dir = unique_temp_dir();
        touch_auth_json(&data_dir); // top-level is itself an account too
        touch_auth_json(&data_dir.join("alice"));

        let pool = cfg_with_data_dir(&data_dir).account_pool();
        assert_eq!(pool.len(), 2);
        assert_eq!(pool[0].0, data_dir); // top-level always first
        assert_eq!(pool[1].0, data_dir.join("alice"));
    }

    #[test]
    fn account_pool_label_falls_back_to_basename_then_configured_name() {
        let data_dir = unique_temp_dir();
        std::fs::create_dir_all(&data_dir).unwrap();
        touch_auth_json(&data_dir.join("alice"));
        touch_auth_json(&data_dir.join("bob"));

        let mut cfg = cfg_with_data_dir(&data_dir);
        cfg.upstream
            .account_names
            .insert("bob".to_string(), "backup-bob".to_string());

        let pool = cfg.account_pool();
        assert_eq!(pool[0].1, "alice"); // no configured name -> basename
        assert_eq!(pool[1].1, "backup-bob"); // configured name wins
    }
}
