//! Configuration: TOML file + env overrides, with coding-friendly defaults.
//!
//! Resolution order (lowest to highest priority):
//!   built-in defaults  ->  config.toml  ->  CODEXPROXY_* env vars

use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub client_auth: ClientAuthConfig,
    pub upstream: UpstreamConfig,
    pub defaults: DefaultsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ClientAuthConfig {
    pub keys: Vec<String>,
    pub require: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    pub codex_home: String,
    pub base_url: String,
    pub responses_path: String,
    pub issuer: String,
    pub client_id: String,
    pub originator: String,
    pub refresh_skew_secs: i64,
    pub request_timeout_secs: u64,
    pub connect_timeout_secs: u64,
    /// Optional outbound proxy for ALL upstream traffic (forward + token
    /// refresh). Supports `http://`, `https://`, and `socks5://`, with optional
    /// `user:pass@` credentials. Useful when OpenAI blocks the deploy region/IP.
    /// Empty/absent = direct connection (system proxy env vars still honored).
    pub proxy: Option<String>,
}

/// Coding-friendly request defaults, applied when the client omits a field.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DefaultsConfig {
    /// Upstream model used when the request's model isn't a known alias/id.
    pub model: String,
    /// low | medium | high — injected as reasoning.effort when the client
    /// doesn't send reasoning_effort.
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    pub level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8787,
        }
    }
}

impl Default for ClientAuthConfig {
    fn default() -> Self {
        Self {
            keys: vec!["sk-local-changeme".to_string()],
            require: true,
        }
    }
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            codex_home: "~/.codex".to_string(),
            base_url: "https://chatgpt.com/backend-api".to_string(),
            responses_path: "/codex/responses".to_string(),
            issuer: "https://auth.openai.com".to_string(),
            // Public OAuth client id used by the Codex CLI for token refresh.
            client_id: "app_EMoamEEZ73f0CkXaXp7hrann".to_string(),
            originator: "codex_cli_rs".to_string(),
            refresh_skew_secs: 300,
            request_timeout_secs: 600,
            connect_timeout_secs: 30,
            proxy: None,
        }
    }
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            model: "gpt-5-codex".to_string(),
            reasoning_effort: "medium".to_string(),
            reasoning_summary: "auto".to_string(),
            instructions: "You are a helpful coding assistant.".to_string(),
            include_reasoning: false,
            model_aliases: HashMap::new(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
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
        // Comma-separated list of client keys.
        if let Ok(v) = std::env::var("CODEXPROXY_API_KEYS") {
            self.client_auth.keys = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Ok(v) = std::env::var("CODEXPROXY_CODEX_HOME") {
            self.upstream.codex_home = v;
        }
        if let Ok(v) = std::env::var("CODEXPROXY_PROXY") {
            self.upstream.proxy = if v.trim().is_empty() { None } else { Some(v) };
        }
        if let Ok(v) = std::env::var("CODEXPROXY_LOG") {
            self.logging.level = v;
        }
    }

    /// Absolute path to the codex_home directory, expanding a leading `~`.
    pub fn codex_home_path(&self) -> PathBuf {
        expand_tilde(&self.upstream.codex_home)
    }
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}
