//! Direct route for `POST /v1/embeddings`.
//!
//! The ChatGPT subscription backend behind the account pool has no embeddings
//! API at all, so this endpoint never touches the pool and never fails over:
//! every request goes straight to ONE already-declared `[[fallback]]` provider
//! (picked by name in `[embeddings]`), reusing its base URL, auth style and
//! key. That makes `account=<provider>` on this endpoint the *normal* state,
//! not a symptom — see `server::EMBEDDINGS_ENDPOINT` for how metrics and the
//! infra alert tell it apart from a real pool failover.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::config::Config;
use crate::error::ProxyError;

enum Auth {
    /// Azure-style: `api-key: <key>`.
    ApiKeyHeader(String),
    /// OpenRouter-style: `Authorization: Bearer <key>`.
    BearerHeader(String),
}

pub struct EmbeddingsUpstream {
    name: Arc<str>,
    url: String,
    auth: Auth,
    model_map: HashMap<String, String>,
    http: reqwest::Client,
}

impl EmbeddingsUpstream {
    /// `Ok(None)` when `[embeddings]` is absent (the endpoint then answers
    /// 404). Errors are startup errors: a section naming an undeclared
    /// provider, or with nothing in `model_map`, must not boot into a route
    /// that can only ever fail.
    pub fn from_config(config: &Config, http: reqwest::Client) -> anyhow::Result<Option<Self>> {
        let Some(cfg) = &config.embeddings else {
            return Ok(None);
        };
        let provider = config
            .fallback
            .iter()
            .find(|p| p.name == cfg.provider)
            .ok_or_else(|| {
                let declared: Vec<&str> =
                    config.fallback.iter().map(|p| p.name.as_str()).collect();
                anyhow::anyhow!(
                    "[embeddings]: provider {:?} is not a declared [[fallback]] entry (declared: {})",
                    cfg.provider,
                    if declared.is_empty() {
                        "none".to_string()
                    } else {
                        declared.join(", ")
                    }
                )
            })?;
        // `FallbackChain::new` already refuses an empty key for every declared
        // provider and runs first in main — this only matters for callers that
        // build the two independently (tests).
        if provider.api_key.trim().is_empty() {
            anyhow::bail!(
                "[embeddings]: provider {:?} has an empty api_key",
                provider.name
            );
        }
        if cfg.model_map.is_empty() {
            anyhow::bail!("[embeddings]: model_map is empty — no model could ever be served");
        }
        let auth = match provider.auth_style.as_str() {
            "api-key" => Auth::ApiKeyHeader(provider.api_key.clone()),
            "bearer" => Auth::BearerHeader(provider.api_key.clone()),
            other => anyhow::bail!(
                "[embeddings]: provider {:?} has unknown auth_style {other:?} (expected \"api-key\" or \"bearer\")",
                provider.name
            ),
        };
        tracing::info!(
            provider = %provider.name,
            models = cfg.model_map.len(),
            "embeddings configured: direct to provider, no pool, no failover"
        );
        Ok(Some(Self {
            name: provider.name.clone().into(),
            url: format!("{}{}", provider.base_url.trim_end_matches('/'), cfg.path),
            auth,
            model_map: cfg.model_map.clone(),
            http,
        }))
    }

    /// Provider name for the `account` field/label — the same string a real
    /// failover to this provider would carry, because it IS the same provider.
    pub fn name(&self) -> Arc<str> {
        self.name.clone()
    }

    /// Provider-side id for a client-facing `model`, or `None` if unmapped.
    pub fn map_model(&self, requested: &str) -> Option<&str> {
        self.model_map.get(requested).map(String::as_str)
    }

    /// Client-facing ids this route accepts, sorted — for error messages.
    pub fn models(&self) -> Vec<&str> {
        let mut v: Vec<&str> = self.model_map.keys().map(String::as_str).collect();
        v.sort_unstable();
        v
    }

    /// Bounded `model` label for Prometheus: the requested id if the config
    /// knows it, else `"other"`. Same reasoning as `server::metric_model_label`,
    /// but bounded by `model_map` rather than the chat model list — the two
    /// sets don't overlap.
    pub fn metric_model_label<'a>(&self, requested: &'a str) -> &'a str {
        if self.model_map.contains_key(requested) {
            requested
        } else {
            "other"
        }
    }

    /// One JSON request, one JSON response. Deliberately not
    /// `FallbackProvider::send`: that one asks for `text/event-stream`. Like
    /// it, this sends no Codex/ChatGPT headers to a third party.
    pub async fn send(&self, body: Bytes) -> Result<reqwest::Response, ProxyError> {
        let mut req = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .body(body);
        req = match &self.auth {
            Auth::ApiKeyHeader(key) => req.header("api-key", key),
            Auth::BearerHeader(key) => req.header("Authorization", format!("Bearer {key}")),
        };
        req.send().await.map_err(|e| {
            tracing::warn!(provider = %self.name, error = %e, "embeddings provider request failed");
            ProxyError::Upstream(format!(
                "embeddings provider '{}' request failed: {e}",
                self.name
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EmbeddingsConfig, FallbackProviderConfig};

    fn openrouter() -> FallbackProviderConfig {
        FallbackProviderConfig {
            name: "openrouter".to_string(),
            base_url: "https://openrouter.ai/api/v1/".to_string(),
            responses_path: "/responses".to_string(),
            auth_style: "bearer".to_string(),
            api_key: "sk-or-test".to_string(),
            model_map: HashMap::new(),
            sticky_session: false,
        }
    }

    fn embeddings(provider: &str) -> EmbeddingsConfig {
        EmbeddingsConfig {
            provider: provider.to_string(),
            path: "/embeddings".to_string(),
            model_map: [(
                "text-embedding-3-small".to_string(),
                "openai/text-embedding-3-small".to_string(),
            )]
            .into_iter()
            .collect(),
        }
    }

    #[test]
    fn absent_section_means_no_upstream() {
        let config = Config::default();
        assert!(
            EmbeddingsUpstream::from_config(&config, reqwest::Client::new())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn builds_from_the_named_fallback_provider() {
        let config = Config {
            fallback: vec![openrouter()],
            embeddings: Some(embeddings("openrouter")),
            ..Default::default()
        };
        let up = EmbeddingsUpstream::from_config(&config, reqwest::Client::new())
            .unwrap()
            .expect("configured");
        assert_eq!(&*up.name(), "openrouter");
        // Trailing slash on base_url must not double up.
        assert_eq!(up.url, "https://openrouter.ai/api/v1/embeddings");
        assert_eq!(
            up.map_model("text-embedding-3-small"),
            Some("openai/text-embedding-3-small")
        );
        assert_eq!(up.map_model("text-embedding-3-large"), None);
        assert_eq!(
            up.metric_model_label("text-embedding-3-small"),
            "text-embedding-3-small"
        );
        assert_eq!(up.metric_model_label("anything-else"), "other");
        assert_eq!(up.models(), vec!["text-embedding-3-small"]);
    }

    #[test]
    fn unknown_provider_is_a_startup_error() {
        let config = Config {
            fallback: vec![openrouter()],
            embeddings: Some(embeddings("azure")),
            ..Default::default()
        };
        let err = EmbeddingsUpstream::from_config(&config, reqwest::Client::new())
            .err()
            .expect("must fail")
            .to_string();
        assert!(err.contains("\"azure\""), "{err}");
        assert!(err.contains("openrouter"), "{err}");
    }

    #[test]
    fn empty_model_map_is_a_startup_error() {
        let mut cfg = embeddings("openrouter");
        cfg.model_map.clear();
        let config = Config {
            fallback: vec![openrouter()],
            embeddings: Some(cfg),
            ..Default::default()
        };
        assert!(EmbeddingsUpstream::from_config(&config, reqwest::Client::new()).is_err());
    }

    #[test]
    fn section_parses_from_toml_with_default_path() {
        let text = r#"
            [[fallback]]
            name = "openrouter"
            base_url = "https://openrouter.ai/api/v1"
            auth_style = "bearer"
            api_key = "sk-or-test"
            [fallback.model_map]
            "gpt-5.5" = "openai/gpt-5.5"

            [embeddings]
            provider = "openrouter"
            [embeddings.model_map]
            "text-embedding-3-small" = "openai/text-embedding-3-small"
        "#;
        let config: Config = toml::from_str(text).unwrap();
        let cfg = config.embeddings.as_ref().expect("parsed");
        assert_eq!(cfg.provider, "openrouter");
        assert_eq!(cfg.path, "/embeddings");
        assert_eq!(cfg.model_map.len(), 1);
        assert!(
            EmbeddingsUpstream::from_config(&config, reqwest::Client::new())
                .unwrap()
                .is_some()
        );
    }
}
