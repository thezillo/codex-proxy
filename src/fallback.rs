//! Fallback to secondary Responses-API-compatible providers (Azure OpenAI,
//! OpenRouter, ...), tried in configured order only once the whole ChatGPT
//! account pool (`upstream::Upstream`) has failed. No new wire-format
//! translation is needed here — both candidate providers speak the same
//! Responses API JSON/SSE shape this proxy already produces/consumes
//! elsewhere; only the base URL, auth header, and the outbound `model` field
//! differ per provider.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::config::FallbackProviderConfig;
use crate::error::ProxyError;
use crate::upstream::ForwardedResponse;

enum FallbackAuth {
    /// Azure-style: `api-key: <key>`.
    ApiKeyHeader(String),
    /// OpenRouter-style: `Authorization: Bearer <key>`.
    BearerHeader(String),
}

/// One configured fallback provider. Deliberately has no knowledge of
/// "Azure" or "OpenRouter" as concepts — both speak the same wire protocol,
/// so a single generic shape (base URL + auth header + model map) covers
/// any future Responses-API-compatible provider with zero new code.
struct FallbackProvider {
    name: Arc<str>,
    responses_url: String,
    auth: FallbackAuth,
    model_map: HashMap<String, String>,
}

impl FallbackProvider {
    fn new(cfg: &FallbackProviderConfig) -> anyhow::Result<Self> {
        // Runs after `Config::apply_env_overrides`, so this catches BOTH "no
        // api_key in config.toml and the env override was never set" and "an
        // empty placeholder was left in config.toml on purpose but nobody
        // set the env var either" — a fallback provider silently sending
        // every request with a blank auth header would just look like it's
        // permanently down, not misconfigured.
        if cfg.api_key.trim().is_empty() {
            anyhow::bail!(
                "fallback provider '{}': api_key is empty — set it in config.toml or via \
                 CODEXPROXY_FALLBACK_{}_API_KEY",
                cfg.name,
                crate::config::normalize_env_suffix(&cfg.name)
            );
        }
        let auth = match cfg.auth_style.as_str() {
            "api-key" => FallbackAuth::ApiKeyHeader(cfg.api_key.clone()),
            "bearer" => FallbackAuth::BearerHeader(cfg.api_key.clone()),
            other => anyhow::bail!(
                "fallback provider '{}': unknown auth_style {other:?} (expected \"api-key\" or \"bearer\")",
                cfg.name
            ),
        };
        Ok(Self {
            name: cfg.name.clone().into(),
            responses_url: format!(
                "{}{}",
                cfg.base_url.trim_end_matches('/'),
                cfg.responses_path
            ),
            auth,
            model_map: cfg.model_map.clone(),
        })
    }

    /// `parsed` (the original request body, already parsed once by
    /// `FallbackChain::run` — a multi-provider cascade would otherwise
    /// re-parse the same, possibly large, body once per provider tried) with
    /// its `"model"` field rewritten to this provider's own model/deployment
    /// string, or `None` if `model_map` has no entry for `requested_model` —
    /// the caller skips this provider entirely rather than guessing a name
    /// that likely doesn't exist on this provider (e.g. an Azure deployment
    /// name).
    fn patch_model(&self, parsed: &serde_json::Value, requested_model: &str) -> Option<Bytes> {
        let mapped = self.model_map.get(requested_model)?.clone();
        let mut patched = parsed.clone();
        patched["model"] = serde_json::Value::String(mapped);
        Some(Bytes::from(serde_json::to_vec(&patched).ok()?))
    }

    /// Minimal request: Content-Type, Accept, and the one auth header —
    /// deliberately no `client_headers` parameter anywhere in this module, so
    /// Codex/ChatGPT-specific headers (`x-codex-*`, `ChatGPT-Account-ID`, the
    /// impersonated User-Agent) can never leak to a third-party provider that
    /// wouldn't understand them anyway.
    async fn send(
        &self,
        http: &reqwest::Client,
        body: Bytes,
    ) -> Result<reqwest::Response, ProxyError> {
        let mut req = http
            .post(&self.responses_url)
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .body(body);
        req = match &self.auth {
            FallbackAuth::ApiKeyHeader(key) => req.header("api-key", key),
            FallbackAuth::BearerHeader(key) => req.header("Authorization", format!("Bearer {key}")),
        };
        req.send().await.map_err(|e| {
            tracing::warn!(provider = %self.name, error = %e, "fallback provider request failed");
            ProxyError::Upstream(format!(
                "fallback provider '{}' request failed: {e}",
                self.name
            ))
        })
    }
}

/// Ordered chain of fallback providers, tried after the ChatGPT account pool
/// is exhausted. Empty by default — every existing deployment (no
/// `[[fallback]]` configured) pays zero cost beyond one `Vec::is_empty` check.
pub struct FallbackChain {
    http: reqwest::Client,
    providers: Vec<FallbackProvider>,
}

impl FallbackChain {
    pub fn new(http: reqwest::Client, cfgs: &[FallbackProviderConfig]) -> anyhow::Result<Self> {
        let providers = cfgs
            .iter()
            .map(FallbackProvider::new)
            .collect::<anyhow::Result<Vec<_>>>()?;
        if !providers.is_empty() {
            tracing::info!(count = providers.len(), "fallback providers configured");
        }
        Ok(Self { http, providers })
    }

    /// Try each configured provider in order against `body` — the same body
    /// the ChatGPT pool just failed on; each provider rewrites `"model"` for
    /// itself. Returns `None` when: the chain is empty, no provider has a
    /// mapping for the requested model, or every attempted provider
    /// transport-errored with no response at all — in every `None` case the
    /// caller should return the ORIGINAL pool failure unchanged. Otherwise
    /// returns the first success, or — mirroring
    /// `Upstream::forward_responses`'s own philosophy — the last attempted
    /// provider's failing response, so the client sees a real error rather
    /// than a synthetic one.
    ///
    /// Any non-2xx from a fallback provider moves on to the next one — unlike
    /// the ChatGPT pool (`is_account_failure`, 401/403/429 only), which is a
    /// homogeneous set of accounts where a plain 400 means the request itself
    /// is bad for every account alike. A fallback chain is heterogeneous:
    /// different providers can have different body/param requirements, so a
    /// 400/422 from one doesn't imply the same from the next, and a
    /// provider's own 5xx (down/overloaded) is the canonical reason to try
    /// somewhere else.
    pub async fn run(&self, body: Bytes) -> Option<ForwardedResponse> {
        if self.providers.is_empty() {
            return None;
        }
        // Parsed once here, not once per provider: a cascade past the first
        // failing provider would otherwise re-parse the same (possibly
        // large) body again for every subsequent attempt.
        let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&body) else {
            return None;
        };
        let requested_model = parsed.get("model").and_then(serde_json::Value::as_str)?;

        let mut last = None;
        for provider in &self.providers {
            let Some(patched) = provider.patch_model(&parsed, requested_model) else {
                continue;
            };
            match provider.send(&self.http, patched).await {
                Ok(response) => {
                    if !response.status().is_success() {
                        tracing::warn!(
                            provider = %provider.name,
                            status = %response.status(),
                            "fallback provider failed, trying next"
                        );
                        last = Some(ForwardedResponse {
                            response,
                            account: provider.name.clone(),
                        });
                        continue;
                    }
                    return Some(ForwardedResponse {
                        response,
                        account: provider.name.clone(),
                    });
                }
                Err(_) => continue,
            }
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::response::Response;
    use axum::routing::post;
    use axum::Router;
    use tokio::sync::mpsc;

    use super::*;

    fn provider_cfg(
        name: &str,
        base_url: &str,
        auth_style: &str,
        model_map: &[(&str, &str)],
    ) -> FallbackProviderConfig {
        FallbackProviderConfig {
            name: name.to_string(),
            base_url: base_url.to_string(),
            responses_path: "/responses".to_string(),
            auth_style: auth_style.to_string(),
            api_key: "test-key".to_string(),
            model_map: model_map
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn patch_model_rewrites_mapped_model_and_preserves_rest() {
        let provider = FallbackProvider::new(&provider_cfg(
            "azure",
            "http://x",
            "api-key",
            &[("gpt-5.5", "my-deployment")],
        ))
        .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(r#"{"model":"gpt-5.5","input":"hi"}"#).unwrap();
        let patched = provider.patch_model(&parsed, "gpt-5.5").unwrap();
        let value: serde_json::Value = serde_json::from_slice(&patched).unwrap();
        assert_eq!(value["model"], "my-deployment");
        assert_eq!(value["input"], "hi");
    }

    #[test]
    fn patch_model_returns_none_without_a_mapping() {
        let provider =
            FallbackProvider::new(&provider_cfg("azure", "http://x", "api-key", &[])).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(r#"{"model":"gpt-5.5"}"#).unwrap();
        assert!(provider.patch_model(&parsed, "gpt-5.5").is_none());
    }

    #[test]
    fn unknown_auth_style_fails_fast() {
        match FallbackProvider::new(&provider_cfg("x", "http://x", "oauth2", &[])) {
            Ok(_) => panic!("expected an error for an unknown auth_style"),
            Err(e) => assert!(e.to_string().contains("auth_style")),
        }
    }

    #[test]
    fn empty_api_key_fails_fast_with_the_env_var_name_to_set() {
        // Catches the gap between "declared in config.toml" and "actually
        // has a secret" — e.g. a placeholder left in place with the intended
        // CODEXPROXY_FALLBACK_{NAME}_API_KEY override never actually set.
        let mut cfg = provider_cfg("azure-eu", "http://x", "api-key", &[]);
        cfg.api_key = "  ".to_string();
        match FallbackProvider::new(&cfg) {
            Ok(_) => panic!("expected an error for an empty api_key"),
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("api_key is empty"));
                assert!(msg.contains("CODEXPROXY_FALLBACK_AZURE_EU_API_KEY"));
            }
        }
    }

    struct CapturedRequest {
        headers: HeaderMap,
        body: Bytes,
    }

    async fn capture_request(
        State((tx, status)): State<(mpsc::Sender<CapturedRequest>, u16)>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let _ = tx.send(CapturedRequest { headers, body }).await;
        Response::builder()
            .status(status)
            .body(Body::from(r#"{"ok":true}"#))
            .unwrap()
    }

    /// Fake fallback provider: replies with a fixed scripted status and
    /// records every request it receives (headers + body).
    async fn start_fake_provider(status: u16) -> (String, mpsc::Receiver<CapturedRequest>) {
        let (tx, rx) = mpsc::channel(4);
        let app = Router::new()
            .route("/responses", post(capture_request))
            .with_state((tx, status));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), rx)
    }

    #[tokio::test]
    async fn send_uses_api_key_header_for_api_key_style() {
        let (base_url, mut rx) = start_fake_provider(200).await;
        let provider = FallbackProvider::new(&provider_cfg(
            "azure",
            &base_url,
            "api-key",
            &[("gpt-5.5", "deployment-x")],
        ))
        .unwrap();
        let http = reqwest::Client::new();
        let parsed: serde_json::Value = serde_json::from_str(r#"{"model":"gpt-5.5"}"#).unwrap();
        let patched = provider.patch_model(&parsed, "gpt-5.5").unwrap();
        provider.send(&http, patched).await.unwrap();

        let captured = rx.recv().await.unwrap();
        assert_eq!(captured.headers.get("api-key").unwrap(), "test-key");
        assert!(captured.headers.get("authorization").is_none());
    }

    #[tokio::test]
    async fn send_uses_bearer_header_for_bearer_style() {
        let (base_url, mut rx) = start_fake_provider(200).await;
        let provider = FallbackProvider::new(&provider_cfg(
            "openrouter",
            &base_url,
            "bearer",
            &[("gpt-5.5", "openai/gpt-4.1")],
        ))
        .unwrap();
        let http = reqwest::Client::new();
        let parsed: serde_json::Value = serde_json::from_str(r#"{"model":"gpt-5.5"}"#).unwrap();
        let patched = provider.patch_model(&parsed, "gpt-5.5").unwrap();
        provider.send(&http, patched).await.unwrap();

        let captured = rx.recv().await.unwrap();
        assert_eq!(
            captured.headers.get("authorization").unwrap(),
            "Bearer test-key"
        );
        assert!(captured.headers.get("api-key").is_none());
    }

    #[tokio::test]
    async fn run_returns_none_for_empty_chain_without_any_http_call() {
        let chain = FallbackChain::new(reqwest::Client::new(), &[]).unwrap();
        let result = chain
            .run(Bytes::from_static(br#"{"model":"gpt-5.5"}"#))
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn run_returns_none_for_a_body_with_no_parseable_model_without_any_http_call() {
        // The body is parsed once up front in `run()` now (not per provider,
        // for the multi-provider-cascade cost this avoids) — this is the
        // regression guard that hoisting didn't change the graceful-skip
        // behavior for malformed/model-less bodies.
        let (base_url, mut rx) = start_fake_provider(200).await;
        let chain = FallbackChain::new(
            reqwest::Client::new(),
            &[provider_cfg(
                "azure",
                &base_url,
                "api-key",
                &[("gpt-5.5", "deployment-x")],
            )],
        )
        .unwrap();

        assert!(chain.run(Bytes::from_static(b"not json")).await.is_none());
        assert!(chain
            .run(Bytes::from_static(br#"{"no_model_field":true}"#))
            .await
            .is_none());
        assert!(
            rx.try_recv().is_err(),
            "a body the chain can't even find a model in must never reach a provider"
        );
    }

    #[tokio::test]
    async fn run_tries_providers_in_order_and_returns_first_success() {
        let (url_a, mut rx_a) = start_fake_provider(429).await;
        let (url_b, mut rx_b) = start_fake_provider(200).await;
        let cfgs = vec![
            provider_cfg("first", &url_a, "bearer", &[("gpt-5.5", "model-a")]),
            provider_cfg("second", &url_b, "bearer", &[("gpt-5.5", "model-b")]),
        ];
        let chain = FallbackChain::new(reqwest::Client::new(), &cfgs).unwrap();

        let result = chain
            .run(Bytes::from_static(br#"{"model":"gpt-5.5"}"#))
            .await
            .unwrap();
        assert_eq!(result.response.status(), reqwest::StatusCode::OK);
        assert_eq!(&*result.account, "second");

        let first_req = rx_a.recv().await.unwrap();
        let first_body: serde_json::Value = serde_json::from_slice(&first_req.body).unwrap();
        assert_eq!(first_body["model"], "model-a");

        let second_req = rx_b.recv().await.unwrap();
        let second_body: serde_json::Value = serde_json::from_slice(&second_req.body).unwrap();
        assert_eq!(second_body["model"], "model-b");
    }

    #[tokio::test]
    async fn run_returns_last_providers_failure_when_all_fail() {
        let (url_a, mut rx_a) = start_fake_provider(403).await;
        let (url_b, mut rx_b) = start_fake_provider(429).await;
        let cfgs = vec![
            provider_cfg("first", &url_a, "bearer", &[("gpt-5.5", "model-a")]),
            provider_cfg("second", &url_b, "bearer", &[("gpt-5.5", "model-b")]),
        ];
        let chain = FallbackChain::new(reqwest::Client::new(), &cfgs).unwrap();

        let result = chain
            .run(Bytes::from_static(br#"{"model":"gpt-5.5"}"#))
            .await
            .unwrap();
        assert_eq!(
            result.response.status(),
            reqwest::StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(&*result.account, "second");
        assert!(rx_a.recv().await.is_some());
        assert!(rx_b.recv().await.is_some());
    }

    #[tokio::test]
    async fn run_advances_past_a_5xx_and_a_400_to_the_next_provider() {
        // Unlike the ChatGPT pool (homogeneous accounts, where a plain 400
        // means the request is bad for all of them alike), a fallback chain
        // is heterogeneous — a provider being down (5xx) or rejecting this
        // particular body (400, e.g. wrong param shape) doesn't mean the
        // next provider would too.
        let (url_a, mut rx_a) = start_fake_provider(503).await;
        let (url_b, mut rx_b) = start_fake_provider(400).await;
        let (url_c, mut rx_c) = start_fake_provider(200).await;
        let cfgs = vec![
            provider_cfg("down", &url_a, "bearer", &[("gpt-5.5", "model-a")]),
            provider_cfg("rejects-body", &url_b, "bearer", &[("gpt-5.5", "model-b")]),
            provider_cfg("healthy", &url_c, "bearer", &[("gpt-5.5", "model-c")]),
        ];
        let chain = FallbackChain::new(reqwest::Client::new(), &cfgs).unwrap();

        let result = chain
            .run(Bytes::from_static(br#"{"model":"gpt-5.5"}"#))
            .await
            .unwrap();
        assert_eq!(result.response.status(), reqwest::StatusCode::OK);
        assert_eq!(&*result.account, "healthy");
        assert!(rx_a.recv().await.is_some());
        assert!(rx_b.recv().await.is_some());
        assert!(rx_c.recv().await.is_some());
    }

    #[tokio::test]
    async fn run_skips_a_provider_with_no_mapping_for_the_requested_model() {
        let (url_a, mut rx_a) = start_fake_provider(200).await;
        let (url_b, mut rx_b) = start_fake_provider(200).await;
        let cfgs = vec![
            provider_cfg("no-mapping", &url_a, "bearer", &[]),
            provider_cfg("has-mapping", &url_b, "bearer", &[("gpt-5.5", "model-b")]),
        ];
        let chain = FallbackChain::new(reqwest::Client::new(), &cfgs).unwrap();

        let result = chain
            .run(Bytes::from_static(br#"{"model":"gpt-5.5"}"#))
            .await
            .unwrap();
        assert_eq!(&*result.account, "has-mapping");
        assert!(
            rx_a.try_recv().is_err(),
            "provider without a mapping for this model must never receive a request"
        );
        assert!(rx_b.recv().await.is_some());
    }
}
