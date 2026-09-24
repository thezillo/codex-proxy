//! HTTP server: client API-key auth + OpenAI-compatible endpoints.
//!
//! `/v1/chat/completions` translates OpenAI Chat Completions to/from the Codex
//! Responses API (streaming and buffered). `/v1/responses` is a raw passthrough
//! for clients that already speak the `responses` wire format (e.g. codex
//! itself). Upstream errors are relayed with their original status and body.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures_util::StreamExt;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::affinity::{self, ConversationKey};
use crate::config::{ClientKey, Config};
use crate::embeddings::EmbeddingsUpstream;
use crate::error::ProxyError;
use crate::fallback::FallbackChain;
use crate::metrics::{Metrics, TokenUsage};
use crate::observe::{self, AccessCtx, CompletionLog};
use crate::translate::{
    alias_responses_model, build_codex_request, chat_response_format_error, collect_chat, model_of,
    rewrite_model, stream_chat, tee_responses, ChatCompletionRequest,
};
use crate::upstream::{Endpoint, FailureReason, ForwardedResponse, Upstream};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub upstream: Arc<Upstream>,
    pub fallback: Arc<FallbackChain>,
    pub metrics: Arc<Metrics>,
    /// `None` until `[embeddings]` is configured; the route then answers 404.
    pub embeddings: Option<Arc<EmbeddingsUpstream>>,
}

/// `endpoint` label/field for the embeddings route. Requests here always carry
/// `account=<provider>` because they are routed *directly* to that provider
/// (the ChatGPT pool has no embeddings API) — this is not a failover, and no
/// failover log line is ever emitted for it. The infra alert "served by
/// fallback" excludes this exact string (`endpoint!="/v1/embeddings"`); keep
/// the two in sync.
pub const EMBEDDINGS_ENDPOINT: &str = "/v1/embeddings";

impl AppState {
    /// Try the ChatGPT account pool first; if its final response is any
    /// non-2xx — not just the 401/403/429 that trigger intra-pool failover,
    /// but also e.g. a 5xx from Codex itself being down — try the configured
    /// fallback chain and use its result instead. A narrower gate here (say,
    /// mirroring `is_account_failure`) would silently skip fallback on the
    /// single most likely real "everything's down" case: a whole-service
    /// Codex outage returns 5xx to every pool account alike, which isn't an
    /// *account* failure but is exactly when a working fallback matters most.
    /// If the fallback chain has nothing usable for this request (empty, no
    /// provider mapped for the model, or every attempted provider
    /// transport-errored), the pool's own response is returned unchanged. A
    /// pool `Err` (every account transport-errored, no response obtained at
    /// all) is treated the same way — tried against the fallback chain, with
    /// the `Err` only propagating if the chain has nothing for it either.
    ///
    /// Every actual pool -> fallback switch emits one `access` line naming the
    /// normalized `reason` (see `observe::log_failover`), because otherwise a
    /// request served by a fallback provider is indistinguishable in the logs
    /// from one the pool never even attempted.
    async fn forward_with_fallback(
        &self,
        body: bytes::Bytes,
        client_headers: &HeaderMap,
        ctx: &AccessCtx,
        // `None` when the body carried no string model.
        model: Option<&str>,
        // Which conversation this is (see `crate::affinity`): picks the pool
        // account, and rides along to fallback providers that want it.
        key: Option<&ConversationKey>,
    ) -> Result<ForwardedResponse, ProxyError> {
        let affinity = key.map(|k| k.hash);
        // Quota-aware short-circuit: when every pool account is known to be
        // unusable right now (quota-exhausted, or cooling down after a
        // failure) and there is somewhere else to send the request, don't
        // pay for an upstream round-trip we just watched fail — go straight
        // to the fallback chain. Only with a chain configured: without one,
        // a shaky account is still the best (only) option, and the pool's
        // own "try one anyway" behavior stands. The chain may still decline
        // (no model_map for this model, every provider down) — then the pool
        // gets its normal shot below, so a partial chain never turns into a
        // synthetic error with zero upstream attempts.
        let skip_reason = if self.fallback.is_empty() {
            None
        } else {
            self.upstream.unavailable()
        };
        let pool_skipped = skip_reason.is_some();
        let pool_result = match skip_reason {
            Some(failure) => Err(failure),
            None => match self
                .upstream
                .forward_responses(body.clone(), client_headers, affinity)
                .await
            {
                Ok(fwd) if fwd.response.status().is_success() => return Ok(fwd),
                other => other,
            },
        };

        // A 4xx other than 401/403/429 is the pool refusing the REQUEST, not
        // an account: typically `400 "The 'x' model is not supported when
        // using Codex with a ChatGPT account"`. Every account would say the
        // same, and a paid provider that happens to carry the model would
        // turn the client's misconfiguration into a silent bill (gpt-5.5 ran
        // on OpenRouter for weeks this way). Relay the pool's own error.
        if !self.config.models.fallback_on_bad_request {
            if let Ok(fwd) = &pool_result {
                let status = fwd.response.status();
                if fwd.reason.is_none()
                    && FailureReason::from_status(status) == FailureReason::BadRequest
                {
                    self.metrics
                        .record_rejected(&ctx.client, "pool_bad_request");
                    tracing::warn!(
                        target: "access",
                        client = %ctx.client,
                        model = %observe::truncate(model.unwrap_or("-")),
                        account = %fwd.account,
                        status = status.as_u16(),
                        "pool rejected the request itself; relayed to the client, not sent to a paid fallback"
                    );
                    return pool_result.map_err(ProxyError::from);
                }
            }
        }

        // Throttled on this model? Try a lower one on the SAME pool before
        // paying anyone.
        if downgrade_eligible(&pool_result) {
            if let Some(fwd) = self
                .try_downgrades(&body, client_headers, ctx, affinity)
                .await
            {
                return Ok(fwd);
            }
        }

        match self.fallback.run(body.clone(), key).await {
            Some((fallback_fwd, dispatched_model)) => {
                // Status and error are mutually exclusive: a status exists iff
                // the pool got a response back, an error iff it didn't.
                let (reason, pool_account, pool_status, error) = match &pool_result {
                    Ok(fwd) => {
                        let status = fwd.response.status();
                        // The pool's own classification (a quota 429) wins
                        // over the bare status, so the request that
                        // discovers an exhaustion groups with the ones
                        // diverted after it, not with transient throttles.
                        let reason = fwd
                            .reason
                            .unwrap_or_else(|| FailureReason::from_status(status));
                        (reason, Some(&*fwd.account), Some(status.as_u16()), None)
                    }
                    Err(f) => (f.reason, f.account.as_deref(), None, Some(&f.error)),
                };
                let failover_model = model.unwrap_or(&dispatched_model);
                self.metrics.record_failover(
                    &ctx.client,
                    metric_model_label(failover_model),
                    reason.as_str(),
                    &fallback_fwd.account,
                );
                observe::log_failover(observe::Failover {
                    ctx,
                    // The chain parsed the body to route it, so it knows the
                    // model even where the passthrough handler doesn't.
                    model: failover_model,
                    request_id: observe::request_id(client_headers),
                    reason,
                    pool_account,
                    pool_status,
                    fallback_account: &fallback_fwd.account,
                    error,
                });
                Ok(fallback_fwd)
            }
            // Chain empty, no provider mapped for this model, every provider
            // transport-errored, or a body the chain couldn't parse a model
            // out of: no switch happened, so no failover line. Note this is
            // NOT fully covered by the request's own access line — on the
            // `Err` path the handler bails before `CompletionLog::emit`, so
            // the only artifact is `ProxyError`'s own warn, off the `access`
            // target and without client/account/reason.
            None if pool_skipped => {
                // The chain declined and the pool was never tried: give the
                // pool its normal chance, unavailable or not.
                self.upstream
                    .forward_responses(body, client_headers, affinity)
                    .await
                    .map_err(ProxyError::from)
            }
            None => pool_result.map_err(ProxyError::from),
        }
    }

    /// Walk `[models.downgrades]` from the body's model, retrying the pool
    /// with each lower model until one is served. `None` = nothing served
    /// (no chain for this model, or every step failed) — the caller then
    /// goes on to the paid fallback with the ORIGINAL body, so a paid
    /// provider still serves the model the client asked for.
    ///
    /// Calls `forward_responses` directly, never `forward_with_fallback`:
    /// the 429 that got us here just put the account into its cooldown, so
    /// `Upstream::unavailable` would skip the pool on a second pass — the
    /// sweep itself still tries a cooling account.
    ///
    /// Stops early on anything but a plain 429: a quota 429 is account-wide
    /// (every model would get it), and any other failure isn't about the
    /// model at all. Bounded by the chain's length, with a cycle guard.
    async fn try_downgrades(
        &self,
        body: &bytes::Bytes,
        client_headers: &HeaderMap,
        ctx: &AccessCtx,
        affinity: Option<u64>,
    ) -> Option<ForwardedResponse> {
        let downgrades = &self.config.models.downgrades;
        if downgrades.is_empty() {
            return None;
        }
        let original = model_of(body)?;
        let mut seen = vec![original.clone()];
        let mut current = original.clone();
        while let Some(next) = downgrades.get(&current) {
            if seen.contains(next) {
                tracing::warn!(model = %next, "cycle in [models.downgrades]; stopping");
                return None;
            }
            seen.push(next.clone());
            let lowered = rewrite_model(body, &original, next)?;
            let result = self
                .upstream
                .forward_responses(lowered.into(), client_headers, affinity)
                .await;
            let served = matches!(&result, Ok(fwd) if fwd.response.status().is_success());
            self.metrics.record_downgrade(
                &ctx.client,
                metric_model_label(&current),
                metric_model_label(next),
                served,
            );
            tracing::warn!(
                target: "access",
                client = %ctx.client,
                from = %observe::truncate(&current),
                to = %observe::truncate(next),
                requested = %observe::truncate(&original),
                outcome = if served { "served" } else { "failed" },
                "pool throttled the model; retried the pool with a lower one"
            );
            match result {
                Ok(fwd) if served => return Some(fwd),
                Ok(fwd) if is_plain_throttle(&fwd) => current = next.clone(),
                _ => return None,
            }
        }
        None
    }
}

/// A pool response that is a model-level throttle: 429 and NOT classified as
/// an account-wide `usage_limit_reached` quota.
fn is_plain_throttle(fwd: &ForwardedResponse) -> bool {
    fwd.response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        && fwd.reason != Some(FailureReason::QuotaExhausted)
}

/// Whether a failed pool attempt is worth retrying with a lower model: a
/// plain 429, or a pool skipped because its accounts are cooling down after
/// one (with a single account, every request right after a throttle lands
/// here). A quota hold never is — it applies to every model on the account.
/// A cooldown caused by a 401/403/transport failure qualifies too; that
/// costs one wasted pool round-trip before the paid fallback, bounded by
/// the chain length.
fn downgrade_eligible(
    pool_result: &Result<ForwardedResponse, crate::upstream::PoolFailure>,
) -> bool {
    match pool_result {
        Ok(fwd) => is_plain_throttle(fwd),
        Err(f) => f.reason == FailureReason::CoolingDown,
    }
}

/// Every model id this proxy accepts: the advertised list plus
/// `[models] extra`.
fn is_known_model(config: &Config, model: &str) -> bool {
    SUPPORTED_MODELS.contains(&model) || config.models.extra.iter().any(|m| m == model)
}

/// `Some(response)` when `[models] reject_unknown` refuses `model` — a 400 in
/// the same `model_not_found` shape as `GET /v1/models/{id}`'s 404, listing
/// what IS available, so the client's author sees the fix in the error
/// itself. Counted and logged: a client sending a dead model id is exactly
/// what used to end up billed on the paid fallback.
fn reject_unknown_model(state: &AppState, ctx: &AccessCtx, model: &str) -> Option<Response> {
    if !state.config.models.reject_unknown || is_known_model(&state.config, model) {
        return None;
    }
    state.metrics.record_rejected(&ctx.client, "unknown_model");
    tracing::warn!(
        target: "access",
        client = %ctx.client,
        model = %observe::truncate(model),
        "unknown model rejected"
    );
    let mut available: Vec<&str> = SUPPORTED_MODELS.to_vec();
    available.extend(state.config.models.extra.iter().map(String::as_str));
    Some(
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "message": format!(
                        "The model '{model}' is not available on this proxy. Available: {}",
                        available.join(", ")
                    ),
                    "type": "invalid_request_error",
                    "param": "model",
                    "code": "model_not_found",
                }
            })),
        )
            .into_response(),
    )
}

pub fn router(state: AppState) -> Router {
    // Lift Axum's 2 MB default body cap to the configured limit so large
    // contexts / base64 images aren't 413'd before we can proxy them.
    let body_limit = state.config.server.max_body_bytes;
    let auth_layer = middleware::from_fn_with_state(state.config.clone(), require_client_auth);

    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(models))
        .route("/v1/models/:model", get(model_by_id))
        .route(
            "/v1/responses",
            post(responses).route_layer(auth_layer.clone()),
        )
        .route(
            "/v1/responses/compact",
            post(responses_compact).route_layer(auth_layer.clone()),
        )
        .route(
            "/v1/alpha/search",
            post(alpha_search).route_layer(auth_layer.clone()),
        )
        .route(
            EMBEDDINGS_ENDPOINT,
            post(embeddings).route_layer(auth_layer.clone()),
        )
        .route(
            "/v1/chat/completions",
            post(chat_completions).route_layer(auth_layer),
        )
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state)
}

/// A separate, minimal router for `/metrics` — deliberately NOT part of
/// `router()` above. It's meant to run on its own port (see
/// `config.server.metrics_port`/`metrics_host`) so it can be network-isolated
/// independently of the client-facing API; no client-key auth, no body limit
/// (a GET with no body), nothing beyond the one route.
pub fn metrics_router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(metrics)
}

async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> Response {
    let (content_type, body) = metrics.encode();
    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build metrics response: {e}")).into_response()
        })
}

/// Authenticate before any handler body extractor runs. The POST endpoints
/// accept potentially-large JSON bodies, so doing this inside the handler would
/// let unauthenticated clients force buffering up to `max_body_bytes`.
async fn require_client_auth(
    State(config): State<Arc<Config>>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, ProxyError> {
    let client = check_client_auth(&config, &headers)?;
    let ip = observe::client_ip(&headers);

    // Start line: answers "who is calling, from where" for *every* protected
    // endpoint — including the `/v1/responses` passthrough and requests the
    // client aborts before any completion line can be emitted.
    tracing::info!(
        target: "access",
        client = %client,
        ip = %ip,
        ua = %observe::user_agent(&headers),
        method = %request.method(),
        path = %request.uri().path(),
        "request accepted"
    );

    // Hand the attribution to the handler so its completion line (model, tokens,
    // status, duration) carries the same client/ip.
    request.extensions_mut().insert(AccessCtx { client, ip });
    Ok(next.run(request).await)
}

async fn health() -> impl IntoResponse {
    Json(json!({ "status": "ok" }))
}

/// Models this proxy advertises AND accepts (see `[models] reject_unknown`),
/// most capable first (the order is client-visible — pickers render it as
/// given). Exactly the `visibility: list` entries of the live Codex catalog
/// (`/backend-api/codex/models`), in its order: the gpt-6 astra/sol/luna trio,
/// the 5.6 sol/terra/luna trio and gpt-5.5 (verified answering over a ChatGPT
/// account on 2026-09-23). Older generations the catalog no longer lists
/// (gpt-5.4, the *-codex models) are refused; so are hidden catalog entries
/// (`gpt-reserve`, `codex-auto-review`), which aren't advertised. The bare "gpt-6"/"gpt-5.6" names are listed for
/// OpenAI-style clients and resolved to their flavored form by the default
/// model aliases — on both POST endpoints, so a client that picks a bare name
/// out of this very list reaches the subscription pool whichever wire API it
/// speaks. Both the list and retrieve endpoints derive their output from this
/// slice.
const SUPPORTED_MODELS: &[&str] = &[
    "gpt-6-astra",
    "gpt-6-sol",
    "gpt-6-luna",
    "gpt-6",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna",
    "gpt-5.6",
    "gpt-5.5",
];

/// One OpenAI-style model object. Mirrors what `/v1/models` returns per entry,
/// so a `GET /v1/models/{id}` retrieve and a list entry stay byte-identical.
fn model_object(id: &str) -> serde_json::Value {
    json!({ "id": id, "object": "model", "owned_by": "openai" })
}

/// Clamp a client-supplied model string to a bounded metric-label value.
/// `req.model` in `/v1/chat/completions` is arbitrary client input (echoed
/// back verbatim in the response, which is fine for a JSON field but NOT for
/// a Prometheus label — an unrecognized value would mint a new time series
/// per distinct string, e.g. an attacker or a misconfigured client hammering
/// the registry/Mimir with unbounded cardinality). Anything outside
/// `SUPPORTED_MODELS` collapses to `"other"`; the access *log* still records
/// the real requested model separately (that's fine, log lines aren't
/// aggregated into label-indexed series).
fn metric_model_label(model: &str) -> &str {
    if SUPPORTED_MODELS.contains(&model) {
        model
    } else {
        "other"
    }
}

/// Static models list so OpenAI-style clients can populate a picker.
async fn models() -> impl IntoResponse {
    let data: Vec<serde_json::Value> = SUPPORTED_MODELS.iter().map(|id| model_object(id)).collect();
    Json(json!({ "object": "list", "data": data }))
}

/// OpenAI-compatible "retrieve model" (`GET /v1/models/{id}`). Codex clients
/// (e.g. cyrus's `CodexRunner`) probe this to validate a configured model before
/// a run; without it the 404 makes them fall back to an unsupported default
/// (`gpt-5.2-codex`), which the ChatGPT-account upstream then rejects. Return
/// the same object as the list endpoint for known ids, else a 404 with the
/// canonical `model_not_found` error shape.
async fn model_by_id(Path(model): Path<String>) -> Response {
    if SUPPORTED_MODELS.contains(&model.as_str()) {
        return Json(model_object(&model)).into_response();
    }

    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": {
                "message": format!("Model '{model}' not found"),
                "type": "invalid_request_error",
                "param": "model",
                "code": "model_not_found",
            }
        })),
    )
        .into_response()
}

/// Passthrough to the Codex Responses API. The request body is forwarded
/// byte-for-byte unless `[defaults.model_aliases]` renames its model (see
/// below); the upstream response streams back as-is (preserving SSE).
async fn responses(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    // Codex uses this endpoint, so the model alias map has to be applied here
    // too — otherwise a bare `gpt-6`/`gpt-5.6` (both advertised by /v1/models)
    // 400s upstream and the request silently lands on the paid fallback. The
    // body is only rebuilt when an alias actually fires; every other request
    // is still forwarded byte-for-byte.
    let body = match alias_responses_model(&body, &state.config.defaults) {
        Some(rewritten) => bytes::Bytes::from(rewritten),
        None => body,
    };
    // A second scan of the body (the alias check did one), borrowing the big
    // fields as raw JSON rather than building a tree: the model, plus what
    // the conversation key is made of. A model of `None` (not JSON, no string
    // model) is left for the upstream to judge, as before. The body itself
    // is still forwarded byte-for-byte to the pool.
    let info = affinity::inspect(&body);
    if let Some(m) = &info.model {
        if let Some(rejection) = reject_unknown_model(&state, &ctx, m) {
            return Ok(rejection);
        }
    }
    let key = affinity::resolve(&headers, info.key);
    let fwd = state
        .forward_with_fallback(body, &headers, &ctx, info.model.as_deref(), key.as_ref())
        .await?;
    let upstream = fwd.response;

    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    // Captured before `tee_responses` consumes `upstream` below.
    let upstream_headers = upstream.headers().clone();

    // Forward verbatim, but tee the SSE for token usage so this passthrough —
    // the path the real Codex CLI uses — is attributed too. The model comes
    // from the `inspect` scan above (post-alias); `-` = the body had none.
    let model = observe::truncate(info.model.as_deref().unwrap_or("-"));
    let metric_model = metric_model_label(&model).to_string();
    let mut log = CompletionLog::new(
        ctx,
        "/v1/responses",
        model,
        metric_model,
        state.metrics.clone(),
    );
    log.set_account(fwd.account);
    if let Some(key) = &key {
        log.set_affinity(key.source.as_str());
    }
    let stream = tee_responses(upstream, log, state.config.server.max_body_bytes);

    let mut response = Response::builder()
        .status(status)
        .body(Body::from_stream(stream))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build response: {e}")).into_response()
        });

    // Preserve Content-Type (SSE vs JSON) plus the verified Codex
    // session-continuity headers (see `CODEX_SESSION_RESPONSE_HEADERS`) so
    // the real Codex CLI can replay them on its next request. An explicit
    // allowlist rather than "relay everything except a few" — reqwest's
    // gzip/br/zstd decoding may or may not strip `Content-Encoding` from an
    // encoded upstream response, and blindly relaying it over an
    // already-decoded body would corrupt it for the client either way.
    copy_relayed_headers(&upstream_headers, response.headers_mut());

    Ok(response)
}

/// `/v1/responses/compact`: OpenAI's public, stateless history compaction
/// (JSON in, JSON out; the `compaction` items it returns go into the next
/// `/v1/responses` call as is). Older Codex CLIs also call it for an
/// `openai`-named provider; 0.155+ compact through `/v1/responses` instead.
async fn responses_compact(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    forward_aux_json(
        &state,
        ctx,
        &headers,
        body,
        Endpoint::Compact,
        "/v1/responses/compact",
    )
    .await
}

/// `/v1/alpha/search`: the Codex CLI's standalone web search tool, sent for
/// a provider with `supports_standalone_web_search = true` (or `openai`).
async fn alpha_search(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    forward_aux_json(
        &state,
        ctx,
        &headers,
        body,
        Endpoint::Search,
        "/v1/alpha/search",
    )
    .await
}

/// Shared passthrough for the auxiliary JSON endpoints: the same alias and
/// unknown-model gates as `/v1/responses`, then the pool — and ONLY the pool.
/// `[[fallback]]` providers speak plain Responses; neither OpenRouter nor
/// Azure has these paths, so a fallback attempt would just trade the pool's
/// real error for a 404. The body streams back verbatim (no size cap needed:
/// nothing is buffered).
async fn forward_aux_json(
    state: &AppState,
    ctx: AccessCtx,
    headers: &HeaderMap,
    body: bytes::Bytes,
    endpoint: Endpoint,
    path: &'static str,
) -> Result<Response, ProxyError> {
    let body = match alias_responses_model(&body, &state.config.defaults) {
        Some(rewritten) => bytes::Bytes::from(rewritten),
        None => body,
    };
    let info = affinity::inspect(&body);
    if let Some(m) = &info.model {
        if let Some(rejection) = reject_unknown_model(state, &ctx, m) {
            return Ok(rejection);
        }
    }
    let model = observe::truncate(info.model.as_deref().unwrap_or("-"));
    let metric_model = metric_model_label(&model).to_string();
    let key = affinity::resolve(headers, info.key);
    let mut log = CompletionLog::new(ctx, path, model, metric_model, state.metrics.clone());
    let fwd = match state
        .upstream
        .forward(endpoint, body, headers, key.as_ref().map(|k| k.hash))
        .await
    {
        Ok(fwd) => fwd,
        Err(failure) => {
            if let Some(account) = failure.account.clone() {
                log.set_account(account);
            }
            log.emit(failure.error.status().as_u16(), None);
            return Err(failure.into());
        }
    };
    log.set_account(fwd.account);
    if let Some(key) = &key {
        log.set_affinity(key.source.as_str());
    }
    let upstream = fwd.response;
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    // Neither endpoint reports token usage in a shape worth parsing; the
    // line still records who called what, on which account, with what result.
    log.emit(status.as_u16(), None);
    let upstream_headers = upstream.headers().clone();
    let mut response = Response::builder()
        .status(status)
        .body(Body::from_stream(upstream.bytes_stream()))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build response: {e}")).into_response()
        });
    copy_relayed_headers(&upstream_headers, response.headers_mut());
    Ok(response)
}

/// Content-Type plus the Codex session-continuity headers, the only upstream
/// response headers relayed (see `responses` for why it's an allowlist).
fn copy_relayed_headers(upstream: &reqwest::header::HeaderMap, out: &mut HeaderMap) {
    let content_type = upstream
        .get(axum::http::header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| axum::http::HeaderValue::from_static("application/json"));
    out.insert(axum::http::header::CONTENT_TYPE, content_type);
    for name in crate::upstream::CODEX_SESSION_RESPONSE_HEADERS {
        if let Some(value) = upstream.get(*name) {
            out.insert(axum::http::HeaderName::from_static(name), value.clone());
        }
    }
}

/// OpenAI-compatible `/v1/chat/completions`: translate to the Responses API,
/// forward, then translate the response back (streaming or buffered).
async fn chat_completions(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    let req: ChatCompletionRequest = serde_json::from_slice(&body)
        .map_err(|e| ProxyError::BadRequest(format!("invalid chat request: {e}")))?;
    let client_wants_stream = req.stream.unwrap_or(false);
    let echo_model = req.model.clone();
    let defaults = state.config.defaults.clone();
    let mut log = CompletionLog::new(
        ctx,
        "/v1/chat/completions",
        echo_model.clone(),
        metric_model_label(&echo_model),
        state.metrics.clone(),
    );

    if let Some((message, param, code)) = chat_response_format_error(&req) {
        log.emit(StatusCode::BAD_REQUEST.as_u16(), None);
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "message": message,
                    "type": "invalid_request_error",
                    "param": param,
                    "code": code,
                }
            })),
        )
            .into_response());
    }
    let codex_body = build_codex_request(&req, &defaults);
    // Checked after alias resolution: `gpt-6` is fine because it becomes
    // `gpt-6-astra`; `gpt-5.2-codex` is not, whatever it's called.
    if let Some(resolved) = codex_body.get("model").and_then(serde_json::Value::as_str) {
        if let Some(rejection) = reject_unknown_model(&state, log.ctx(), resolved) {
            log.emit(StatusCode::BAD_REQUEST.as_u16(), None);
            return Ok(rejection);
        }
    }
    // The proxy builds this body itself, so it can also carry the
    // conversation key upstream as `prompt_cache_key` — the field OpenAI
    // (and, on the fallback, OpenRouter) route cache lookups by. The client's
    // own `prompt_cache_key` was already copied in by `build_codex_request`
    // and wins; a session header comes next; otherwise the key is derived
    // from the body's prefix. Chat bodies are small, so serializing twice
    // when a key has to be added is fine.
    let mut codex_body = codex_body;
    let mut bytes = serde_json::to_vec(&codex_body)
        .map_err(|e| ProxyError::Internal(format!("serialize codex request: {e}")))?;
    let key = affinity::resolve(&headers, affinity::inspect(&bytes).key);
    if let Some(key) = &key {
        log.set_affinity(key.source.as_str());
        if codex_body.get("prompt_cache_key").is_none() {
            codex_body["prompt_cache_key"] = serde_json::Value::String(key.value.clone());
            bytes = serde_json::to_vec(&codex_body)
                .map_err(|e| ProxyError::Internal(format!("serialize codex request: {e}")))?;
        }
    }

    let fwd = state
        .forward_with_fallback(
            bytes.into(),
            &headers,
            log.ctx(),
            Some(&echo_model),
            key.as_ref(),
        )
        .await?;
    log.set_account(fwd.account);
    let upstream = fwd.response;

    // Pass upstream failures straight through with their real status, body, and
    // content-type — so a 401/429/400 from Codex reaches the client unchanged
    // instead of being flattened to a generic 502.
    if !upstream.status().is_success() {
        log.emit(upstream.status().as_u16(), None);
        return Ok(passthrough_response(upstream, state.config.server.max_body_bytes).await);
    }

    let max_event_bytes = state.config.server.max_body_bytes;
    if client_wants_stream {
        // The stream owns `log` and emits the completion line (with usage) when
        // it finishes draining.
        let stream = stream_chat(upstream, echo_model, defaults, log, max_event_bytes);
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache")
            .body(Body::from_stream(stream))
            .unwrap_or_else(|e| {
                ProxyError::Internal(format!("failed to build stream response: {e}"))
                    .into_response()
            }))
    } else {
        let json = collect_chat(upstream, echo_model, defaults, max_event_bytes).await?;
        log.emit(StatusCode::OK.as_u16(), usage_pair(&json));
        Ok(Json(json).into_response())
    }
}

/// Token usage from a buffered chat.completion body for the access log, or
/// `None` if the usage block is missing. `collect_chat` builds that block
/// from the upstream's own counts (see `usage_tokens`), cache reads included;
/// cache writes aren't part of the chat shape and are only counted on the
/// streaming paths.
fn usage_pair(chat: &serde_json::Value) -> Option<TokenUsage> {
    let usage = chat.get("usage")?;
    Some(TokenUsage {
        prompt: usage.get("prompt_tokens").and_then(|v| v.as_i64())?,
        completion: usage.get("completion_tokens").and_then(|v| v.as_i64())?,
        cached: usage
            .pointer("/prompt_tokens_details/cached_tokens")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        cache_write: 0,
    })
}

/// `POST /v1/embeddings` — direct to the configured provider, no pool, no
/// failover; see `EMBEDDINGS_ENDPOINT`.
async fn embeddings(
    State(state): State<AppState>,
    Extension(ctx): Extension<AccessCtx>,
    body: bytes::Bytes,
) -> Result<Response, ProxyError> {
    let Some(upstream) = state.embeddings.as_ref() else {
        return Err(ProxyError::NotFound(
            "embeddings are not configured on this proxy: add an [embeddings] section \
             naming a [[fallback]] provider (see config.toml)"
                .into(),
        ));
    };
    let mut parsed: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| ProxyError::BadRequest(format!("invalid embeddings request: {e}")))?;
    let requested = parsed
        .get("model")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ProxyError::BadRequest("embeddings request must include \"model\"".into()))?
        .to_string();

    let mut log = CompletionLog::new(
        ctx,
        EMBEDDINGS_ENDPOINT,
        requested.clone(),
        upstream.metric_model_label(&requested).to_string(),
        state.metrics.clone(),
    );
    // Attributed to the provider up front: there is no pool attempt whose
    // account could be recorded first, and a rejected model should still show
    // where it *would* have gone.
    log.set_account(upstream.name());

    let Some(mapped) = upstream.map_model(&requested) else {
        log.emit(StatusCode::BAD_REQUEST.as_u16(), None);
        return Err(ProxyError::BadRequest(format!(
            "embeddings model {requested:?} is not available here; configured: {}",
            upstream.models().join(", ")
        )));
    };
    parsed["model"] = serde_json::Value::String(mapped.to_string());
    let outbound = serde_json::to_vec(&parsed)
        .map_err(|e| ProxyError::Internal(format!("serialize embeddings request: {e}")))?;

    let response = match upstream.send(outbound.into()).await {
        Ok(r) => r,
        Err(e) => {
            log.emit(StatusCode::BAD_GATEWAY.as_u16(), None);
            return Err(e);
        }
    };
    let max_body_bytes = state.config.server.max_body_bytes;
    if !response.status().is_success() {
        log.emit(response.status().as_u16(), None);
        return Ok(passthrough_response(response, max_body_bytes).await);
    }

    let status = response.status().as_u16();
    let mut json = match collect_json_capped(response, max_body_bytes).await {
        Ok(v) => v,
        Err(e) => {
            log.emit(StatusCode::BAD_GATEWAY.as_u16(), None);
            return Err(e);
        }
    };
    let prompt_tokens = json
        .get("usage")
        .and_then(|u| u.get("prompt_tokens"))
        .and_then(serde_json::Value::as_i64);
    // Echo the id the client asked for, as chat does with `echo_model` — the
    // provider's namespaced id (`openai/...`) is an implementation detail.
    json["model"] = serde_json::Value::String(requested);
    log.emit(status, prompt_tokens.map(|p| TokenUsage::new(p, 0)));
    Ok(Json(json).into_response())
}

/// Buffer a 2xx JSON body, refusing (rather than truncating, unlike the
/// error-path `passthrough_response`) anything over `max_body_bytes`: a
/// truncated success body would reach the client as JSON that isn't.
async fn collect_json_capped(
    upstream: reqwest::Response,
    max_body_bytes: usize,
) -> Result<serde_json::Value, ProxyError> {
    let mut body = Vec::new();
    let mut stream = upstream.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| ProxyError::Upstream(format!("reading embeddings response: {e}")))?;
        body.extend_from_slice(&chunk);
        if body.len() > max_body_bytes {
            return Err(ProxyError::Upstream(format!(
                "embeddings response exceeded {max_body_bytes} bytes"
            )));
        }
    }
    serde_json::from_slice(&body).map_err(|e| {
        ProxyError::Upstream(format!("embeddings provider returned invalid JSON: {e}"))
    })
}

/// Relay an upstream `reqwest::Response` to the client verbatim: same status
/// code, same content-type, same body. Used for error responses and the raw
/// `/v1/responses` passthrough so client-visible semantics aren't altered.
/// `max_body_bytes` bounds how much of an upstream error body this reads —
/// `reqwest::Response::bytes()` has no cap of its own, and this is a
/// non-success status from an account or fallback provider (external, only
/// somewhat trusted), so an enormous error body shouldn't buffer without
/// limit. A truncated read still gets the client something close to the real
/// error rather than nothing.
async fn passthrough_response(upstream: reqwest::Response, max_body_bytes: usize) -> Response {
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let mut body = Vec::new();
    let mut stream = upstream.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        body.extend_from_slice(&chunk);
        if body.len() > max_body_bytes {
            // Truncate exactly to the cap rather than however much this one
            // chunk happened to add past it — a single large chunk (chunking
            // is transport-dependent, not something to rely on for the bound
            // itself) must not be able to exceed the limit either.
            tracing::warn!(
                bytes = body.len(),
                limit = max_body_bytes,
                "upstream error body exceeded limit; truncating passthrough"
            );
            body.truncate(max_body_bytes);
            break;
        }
    }

    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .body(Body::from(body))
        .unwrap_or_else(|e| {
            ProxyError::Internal(format!("failed to build passthrough response: {e}"))
                .into_response()
        })
}

/// Validate the client's `Authorization: Bearer <key>` against the configured
/// key list and return a non-secret label for the matched key (configured name
/// or fingerprint), used to attribute token spend in the access log. When
/// `client_auth.require = false`, auth is skipped and everyone is `anonymous`.
fn check_client_auth(config: &Config, headers: &HeaderMap) -> Result<String, ProxyError> {
    if !config.client_auth.require {
        return Ok("anonymous".to_string());
    }

    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);

    match presented {
        Some(key) => match key_matches(&config.client_auth.keys, key) {
            Some(matched) => Ok(observe::key_label(key, matched.name.as_deref())),
            None => Err(ProxyError::Unauthorized("invalid API key".into())),
        },
        None => Err(ProxyError::Unauthorized(
            "missing Authorization: Bearer <key> header".into(),
        )),
    }
}

/// Compare `presented` against every configured key in constant time — every
/// key is checked unconditionally (no early return) so neither the byte
/// comparison nor the list iteration leaks timing about how much of a key
/// matched. Returns the last matching entry (there should only ever be one).
fn key_matches<'a>(keys: &'a [ClientKey], presented: &str) -> Option<&'a ClientKey> {
    let mut matched = None;
    for k in keys {
        if key_secret_matches(&k.key, presented) {
            matched = Some(k);
        }
    }
    matched
}

/// Whether `presented` satisfies a configured secret, which is either a raw
/// key (direct constant-time compare) or a `sha256:<hex>` digest (hash
/// `presented` and compare digests) — see `ClientKey::key`.
fn key_secret_matches(secret: &str, presented: &str) -> bool {
    match secret.strip_prefix("sha256:") {
        Some(hex_digest) => {
            let Some(expected) = decode_hex(hex_digest) else {
                return false;
            };
            let computed = Sha256::digest(presented.as_bytes());
            constant_time_eq(&computed, &expected)
        }
        None => constant_time_eq(secret.as_bytes(), presented.as_bytes()),
    }
}

/// Decode a hex string into bytes, or `None` if it's malformed (odd length or
/// non-hex characters) — a misconfigured digest should fail to match, not panic.
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Length-independent constant-time byte equality.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Fold the length difference into the accumulator so unequal lengths never
    // short-circuit; the loop runs over max(len) with wrap-around indexing.
    let mut diff = (a.len() ^ b.len()) as u8;
    let n = a.len().max(b.len()).max(1);
    for i in 0..n {
        let x = a.get(i % a.len().max(1)).copied().unwrap_or(0);
        let y = b.get(i % b.len().max(1)).copied().unwrap_or(0);
        diff |= x ^ y;
    }
    diff == 0 && a.len() == b.len()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{HeaderMap, Request as HttpRequest, StatusCode};
    use axum::response::Response;
    use axum::routing::post;
    use axum::Router;
    use bytes::Bytes;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    use super::{
        constant_time_eq, key_matches, key_secret_matches, metrics_router, router, AppState,
        EMBEDDINGS_ENDPOINT,
    };
    use crate::auth::AuthManager;
    use crate::config::{ClientKey, Config};
    use crate::embeddings::EmbeddingsUpstream;
    use crate::fallback::FallbackChain;
    use crate::metrics::Metrics;
    use crate::test_support::{write_test_auth_json, USAGE_LIMIT_429_BODY};
    use crate::upstream::Upstream;

    fn bare_key(key: &str) -> ClientKey {
        ClientKey {
            key: key.to_string(),
            name: None,
        }
    }

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"secret", b"Secret"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn key_matches_any_configured_key() {
        let keys = vec![bare_key("k1"), bare_key("k2")];
        assert!(key_matches(&keys, "k2").is_some());
        assert!(key_matches(&keys, "k3").is_none());
        assert!(key_matches(&[], "anything").is_none());
    }

    #[test]
    fn key_secret_matches_sha256_digest() {
        let digest = format!("sha256:{}", hex_encode(&Sha256::digest(b"the-real-key")));
        assert!(key_secret_matches(&digest, "the-real-key"));
        assert!(!key_secret_matches(&digest, "wrong-key"));
        assert!(!key_secret_matches("sha256:not-hex!!", "anything")); // malformed hex doesn't panic
        assert!(!key_secret_matches("sha256:ab", "anything")); // too-short digest doesn't match
    }

    fn hex_encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[tokio::test]
    async fn unauthenticated_large_body_is_rejected_before_body_limit() {
        let app = test_router(8);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .body(Body::from(vec![b'a'; 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();

        // This must be 401, not 413: auth should run before `Bytes` buffers and
        // applies the configured request-body limit.
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn authenticated_large_body_still_hits_body_limit() {
        let app = test_router(8);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .body(Body::from(vec![b'a'; 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn models_list_and_retrieve_agree_for_known_model() {
        let app = test_router(8);

        let list = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let list: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(list.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let listed = &list["data"][0];
        assert_eq!(listed["id"], "gpt-6-astra");

        let retrieved = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models/gpt-6-astra")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retrieved.status(), StatusCode::OK);
        let retrieved: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(retrieved.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        // Retrieve must mirror the list entry, so model-validating clients accept it.
        assert_eq!(&retrieved, listed);
    }

    #[tokio::test]
    async fn retrieve_unknown_model_is_404() {
        let app = test_router(8);
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models/gpt-5.2-codex")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body["error"]["code"], "model_not_found");
    }

    #[test]
    fn metric_model_label_clamps_unrecognized_client_input() {
        // A metric label must be bounded regardless of what a client sends —
        // otherwise arbitrary `model` strings would mint unbounded Prometheus
        // time series (see codexproxy_requests_total{model=...}).
        assert_eq!(super::metric_model_label("gpt-6-astra"), "gpt-6-astra");
        assert_eq!(super::metric_model_label("gpt-5.6-luna"), "gpt-5.6-luna");
        assert_eq!(
            super::metric_model_label("literally-anything-a-client-sends"),
            "other"
        );
        assert_eq!(super::metric_model_label(""), "other");
    }

    #[tokio::test]
    async fn chat_completions_forwards_to_upstream_and_collects_response() {
        let upstream_body = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":2}}}\n\n",
            "data: [DONE]\n\n"
        );
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", upstream_body).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-6-astra",
                            "messages": [
                                { "role": "system", "content": "be concise" },
                                { "role": "user", "content": "hi" }
                            ],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["object"], "chat.completion");
        assert_eq!(body["choices"][0]["message"]["content"], "Hello");
        assert_eq!(body["usage"]["prompt_tokens"], 3);
        assert_eq!(body["usage"]["completion_tokens"], 2);
        assert_eq!(body["usage"]["total_tokens"], 5);

        let captured = fake.recv().await;
        assert!(captured
            .authorization
            .as_deref()
            .is_some_and(|v| v.starts_with("Bearer ")));
        assert_eq!(captured.account_id.as_deref(), Some("acct_test"));
        assert_eq!(captured.originator.as_deref(), Some("codex_cli_rs"));
        assert_eq!(captured.accept.as_deref(), Some("text/event-stream"));

        let upstream_request: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(upstream_request["model"], "gpt-6-astra");
        assert_eq!(upstream_request["instructions"], "be concise");
        assert_eq!(upstream_request["input"][0]["role"], "user");
        assert_eq!(upstream_request["input"][0]["content"], "hi");
        assert_eq!(upstream_request["stream"], true);
        assert_eq!(upstream_request["store"], false);
        assert_eq!(upstream_request["reasoning"]["effort"], "medium");
    }

    #[tokio::test]
    async fn chat_completions_streaming_passes_through_and_finishes() {
        // Exercises the streaming path that carries the CompletionLog into
        // stream_chat: the SSE must reach the client with content + [DONE].
        let upstream_body = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hi\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":4,\"output_tokens\":1}}}\n\n",
            "data: [DONE]\n\n"
        );
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", upstream_body).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-6-astra",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("chat.completion.chunk"));
        assert!(body.contains("\"content\":\"Hi\""));
        assert!(body.contains("data: [DONE]"));
    }

    #[tokio::test]
    async fn responses_endpoint_passthrough_preserves_status_content_type_and_body() {
        let fake =
            start_fake_upstream(StatusCode::ACCEPTED, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"input":"raw"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":true}"#);

        let captured = fake.recv().await;
        assert_eq!(captured.account_id.as_deref(), Some("acct_test"));
        assert_eq!(captured.content_type.as_deref(), Some("application/json"));
        assert_eq!(&captured.body[..], br#"{"input":"raw"}"#);
    }

    #[tokio::test]
    async fn responses_endpoint_relays_codex_session_headers_both_ways() {
        let fake = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .header("x-codex-turn-state", "client-turn-token")
                    .body(Body::from(r#"{"input":"raw"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The real Codex CLI's own continuity header (request side) is
        // relayed to the upstream, and the upstream's fresh token (response
        // side) is relayed back down to the client — so a real Codex CLI's
        // sticky-routing state survives being routed through this proxy.
        assert_eq!(
            response
                .headers()
                .get("x-codex-turn-state")
                .and_then(|v| v.to_str().ok()),
            Some("server-issued-token")
        );

        let captured = fake.recv().await;
        assert_eq!(captured.turn_state.as_deref(), Some("client-turn-token"));
    }

    fn test_router(max_body_bytes: usize) -> axum::Router {
        test_router_with_upstream(max_body_bytes, Config::default().upstream.base_url)
    }

    fn test_router_with_upstream(max_body_bytes: usize, upstream_base_url: String) -> axum::Router {
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.server.max_body_bytes = max_body_bytes;
        config.upstream.base_url = upstream_base_url;

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback = Arc::new(
            FallbackChain::new(http, &config.fallback).expect("build empty fallback chain"),
        );
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics,
            embeddings: None,
        })
    }

    /// Pool pointed at the same fake (never used by the embeddings route), one
    /// `[[fallback]]` provider `fb` at `provider_base_url`, and `[embeddings]`
    /// routed to it. Returns the metrics too, so tests can read the labels.
    fn test_router_with_embeddings(provider_base_url: String) -> (axum::Router, Arc<Metrics>) {
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = provider_base_url.clone();
        config.fallback = vec![fallback_provider_cfg("fb", &provider_base_url)];
        config.embeddings = Some(crate::config::EmbeddingsConfig {
            provider: "fb".to_string(),
            path: "/embeddings".to_string(),
            model_map: [(
                "text-embedding-3-small".to_string(),
                "openai/text-embedding-3-small".to_string(),
            )]
            .into_iter()
            .collect(),
        });

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback = Arc::new(
            FallbackChain::new(http.clone(), &config.fallback).expect("build fallback chain"),
        );
        let embeddings = EmbeddingsUpstream::from_config(&config, http)
            .expect("build embeddings upstream")
            .map(Arc::new);
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics: metrics.clone(),
            embeddings,
        });
        (app, metrics)
    }

    fn embeddings_request(body: &'static str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri(EMBEDDINGS_ENDPOINT)
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    const EMBEDDINGS_OK: &str = r#"{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"model":"openai/text-embedding-3-small","usage":{"prompt_tokens":5,"total_tokens":5}}"#;

    async fn json_body(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn embeddings_go_direct_to_the_provider_and_echo_the_requested_model() {
        let fake = start_fake_upstream(StatusCode::OK, "application/json", EMBEDDINGS_OK).await;
        let (app, metrics) = test_router_with_embeddings(fake.base_url.clone());

        let response = app
            .oneshot(embeddings_request(
                r#"{"model":"text-embedding-3-small","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["model"], "text-embedding-3-small");
        assert_eq!(body["usage"]["prompt_tokens"], 5);
        assert_eq!(body["data"][0]["embedding"][1], 0.2);

        let captured = fake.recv().await;
        assert_eq!(
            captured.authorization.as_deref(),
            Some("Bearer fallback-key"),
            "provider auth_style=bearer must be honoured"
        );
        assert_eq!(captured.accept.as_deref(), Some("application/json"));
        assert!(
            captured.account_id.is_none() && captured.originator.is_none(),
            "no ChatGPT/Codex headers may reach a third-party provider"
        );
        let sent: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(sent["model"], "openai/text-embedding-3-small");
        assert_eq!(sent["input"], "hi");

        // The mark the infra alert keys on: endpoint, with the provider as
        // account — and tokens booked on the prompt side only.
        let text = String::from_utf8(metrics.encode().1).unwrap();
        let requests_line = text
            .lines()
            .find(|l| l.starts_with("codexproxy_requests_total{"))
            .expect("requests series");
        for needle in [
            r#"endpoint="/v1/embeddings""#,
            r#"account="fb""#,
            r#"model="text-embedding-3-small""#,
            r#"status="200""#,
        ] {
            assert!(
                requests_line.contains(needle),
                "{needle} in {requests_line}"
            );
        }
        let prompt_line = text
            .lines()
            .find(|l| l.starts_with("codexproxy_tokens_total{") && l.contains(r#"kind="prompt""#))
            .expect("prompt tokens series");
        assert!(prompt_line.ends_with(" 5"), "{prompt_line}");
        let completion_line = text
            .lines()
            .find(|l| {
                l.starts_with("codexproxy_tokens_total{") && l.contains(r#"kind="completion""#)
            })
            .expect("completion tokens series");
        assert!(completion_line.ends_with(" 0"), "{completion_line}");
    }

    #[tokio::test]
    async fn embeddings_unmapped_model_is_rejected_before_reaching_the_provider() {
        let mut fake = start_fake_upstream(StatusCode::OK, "application/json", EMBEDDINGS_OK).await;
        let (app, _metrics) = test_router_with_embeddings(fake.base_url.clone());

        let response = app
            .oneshot(embeddings_request(
                r#"{"model":"text-embedding-3-large","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = json_body(response).await;
        let message = body["error"]["message"].as_str().unwrap();
        assert!(message.contains("text-embedding-3-large"), "{message}");
        assert!(message.contains("text-embedding-3-small"), "{message}");
        assert!(
            fake.rx.try_recv().is_err(),
            "an unmapped model must never be sent to the provider"
        );
    }

    #[tokio::test]
    async fn embeddings_unconfigured_is_a_json_404() {
        let fake = start_fake_upstream(StatusCode::OK, "application/json", "{}").await;
        let app = test_router_with_upstream(1024 * 1024, fake.base_url.clone());

        let response = app
            .oneshot(embeddings_request(
                r#"{"model":"text-embedding-3-small","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = json_body(response).await;
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("[embeddings]"));
    }

    #[tokio::test]
    async fn embeddings_provider_error_status_is_relayed_unchanged() {
        let fake = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            r#"{"error":{"message":"slow down"}}"#,
        )
        .await;
        let (app, metrics) = test_router_with_embeddings(fake.base_url.clone());

        let response = app
            .oneshot(embeddings_request(
                r#"{"model":"text-embedding-3-small","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let body = json_body(response).await;
        assert_eq!(body["error"]["message"], "slow down");

        let text = String::from_utf8(metrics.encode().1).unwrap();
        assert!(text.contains(r#"status="429""#), "{text}");
    }

    /// Like `test_router_with_upstream`, but with a configurable fallback
    /// chain instead of an always-empty one — for testing the
    /// pool-exhausted-falls-over-to-fallback composition in `AppState`.
    fn test_router_with_fallback(
        upstream_base_url: String,
        fallback_cfgs: Vec<crate::config::FallbackProviderConfig>,
    ) -> axum::Router {
        test_router_with_fallback_and_cooldown(upstream_base_url, fallback_cfgs, 30)
    }

    /// `account_cooldown_secs` is a parameter so the quota tests can set it
    /// to 0: with the default 30s, any 429 makes the lone account
    /// "unavailable" for the next request whether or not it was classified
    /// as a quota hold, and a test can't tell the two apart.
    fn test_router_with_fallback_and_cooldown(
        upstream_base_url: String,
        fallback_cfgs: Vec<crate::config::FallbackProviderConfig>,
        account_cooldown_secs: u64,
    ) -> axum::Router {
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = upstream_base_url;
        config.upstream.account_cooldown_secs = account_cooldown_secs;
        config.fallback = fallback_cfgs;

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics,
            embeddings: None,
        })
    }

    fn fallback_provider_cfg(name: &str, base_url: &str) -> crate::config::FallbackProviderConfig {
        crate::config::FallbackProviderConfig {
            name: name.to_string(),
            base_url: base_url.to_string(),
            responses_path: "/responses".to_string(),
            auth_style: "bearer".to_string(),
            api_key: "fallback-key".to_string(),
            model_map: [(
                "gpt-5.6-luna".to_string(),
                "gpt-5.6-luna-on-fallback".to_string(),
            )]
            .into_iter()
            .collect(),
            sticky_session: false,
        }
    }

    struct FakeUpstream {
        base_url: String,
        rx: mpsc::Receiver<CapturedUpstreamRequest>,
    }

    impl FakeUpstream {
        async fn recv(mut self) -> CapturedUpstreamRequest {
            self.rx.recv().await.expect("fake upstream request")
        }
    }

    #[derive(Clone)]
    struct FakeUpstreamState {
        tx: mpsc::Sender<CapturedUpstreamRequest>,
        status: StatusCode,
        content_type: &'static str,
        body: &'static str,
    }

    struct CapturedUpstreamRequest {
        path: String,
        authorization: Option<String>,
        account_id: Option<String>,
        originator: Option<String>,
        accept: Option<String>,
        content_type: Option<String>,
        turn_state: Option<String>,
        body: Bytes,
    }

    async fn start_fake_upstream(
        status: StatusCode,
        content_type: &'static str,
        body: &'static str,
    ) -> FakeUpstream {
        let (tx, rx) = mpsc::channel(1);
        let state = FakeUpstreamState {
            tx,
            status,
            content_type,
            body,
        };
        // Registers both the ChatGPT-pool path and the fallback-provider
        // default path (`FallbackProviderConfig::responses_path`'s default)
        // on the same handler, so this one fixture doubles as either a pool
        // upstream or a fallback provider in tests.
        let app = Router::new()
            .route("/codex/responses", post(fake_responses))
            .route("/responses", post(fake_responses))
            // The pool's auxiliary JSON endpoints.
            .route("/codex/responses/compact", post(fake_responses))
            .route("/codex/alpha/search", post(fake_responses))
            // ...and the `[embeddings]` default path, so it can also stand in
            // for the direct embeddings provider.
            .route("/embeddings", post(fake_responses))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        FakeUpstream {
            base_url: format!("http://{addr}"),
            rx,
        }
    }

    async fn fake_responses(
        axum::extract::State(state): axum::extract::State<FakeUpstreamState>,
        uri: axum::http::Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let header = |name| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let captured = CapturedUpstreamRequest {
            path: uri.path().to_string(),
            authorization: header("authorization"),
            account_id: header("chatgpt-account-id"),
            originator: header("originator"),
            accept: header("accept"),
            content_type: header("content-type"),
            turn_state: header("x-codex-turn-state"),
            body,
        };
        state.tx.send(captured).await.unwrap();

        // A fixed reply header, unrelated to what the request carried — lets
        // tests assert the *response* side of header relaying independently
        // of the request side.
        Response::builder()
            .status(state.status)
            .header("Content-Type", state.content_type)
            .header("x-codex-turn-state", "server-issued-token")
            .body(Body::from(state.body))
            .unwrap()
    }

    fn responses_request(body: &'static str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    fn post_json(uri: &'static str, body: &'static str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri(uri)
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn compact_and_search_reach_their_own_pool_paths() {
        for (uri, upstream_path) in [
            ("/v1/responses/compact", "/codex/responses/compact"),
            ("/v1/alpha/search", "/codex/alpha/search"),
        ] {
            let pool =
                start_fake_upstream(StatusCode::OK, "application/json", r#"{"output":[]}"#).await;
            let app = test_router_with_upstream(1024 * 1024, pool.base_url.clone());

            let response = app
                .oneshot(post_json(uri, r#"{"model":"gpt-5.6","input":[]}"#))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(
                response.headers()["x-codex-turn-state"],
                "server-issued-token",
                "{uri}"
            );
            assert_eq!(body_string(response).await, r#"{"output":[]}"#, "{uri}");

            let captured = pool.recv().await;
            assert_eq!(captured.path, upstream_path);
            // JSON in, JSON out: not the SSE `Accept` of /responses.
            assert_eq!(captured.accept.as_deref(), Some("application/json"));
            assert!(captured
                .authorization
                .is_some_and(|a| a.starts_with("Bearer ") && a != "Bearer test-key"));
            let forwarded: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
            assert_eq!(forwarded["model"], "gpt-5.6-sol", "alias applied on {uri}");
        }
    }

    #[tokio::test]
    async fn aux_endpoints_gate_unknown_models() {
        let pool = start_fake_upstream(StatusCode::OK, "application/json", "{}").await;
        let app = test_router_with_upstream(1024 * 1024, pool.base_url.clone());
        let response = app
            .oneshot(post_json("/v1/alpha/search", r#"{"model":"gpt-4o"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn aux_endpoints_relay_the_pool_error_instead_of_trying_the_fallback() {
        let mut pool = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            USAGE_LIMIT_429_BODY,
        )
        .await;
        let mut fallback_fake = start_fake_upstream(StatusCode::OK, "application/json", "{}").await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );
        let response = app
            .clone()
            .oneshot(post_json(
                "/v1/responses/compact",
                r#"{"model":"gpt-5.6-luna","input":[]}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let _ = pool.rx.recv().await.unwrap();
        assert!(fallback_fake.rx.try_recv().is_err());

        // Not even a quota-shaped 429 there marks the account: the next
        // /v1/responses still tries the pool before any paid fallback.
        let _ = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        let tried = pool.rx.try_recv().expect("the pool is tried first");
        assert_eq!(tried.path, "/codex/responses");
    }

    #[tokio::test]
    async fn pool_success_never_touches_fallback() {
        let pool = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let mut fallback_fake =
            start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let _ = pool.recv().await;
        assert!(
            fallback_fake.rx.try_recv().is_err(),
            "fallback must not be touched when the pool succeeds"
        );
    }

    #[tokio::test]
    async fn responses_passthrough_applies_the_model_alias_before_forwarding() {
        // The endpoint Codex actually uses. Without this the bare name goes
        // upstream verbatim, 400s over a ChatGPT account, and the request
        // silently ends up on a paid fallback provider instead of the pool.
        let pool = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_upstream(1024 * 1024, pool.base_url.clone());

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6","store":false,"input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let captured = pool.recv().await;
        let forwarded: serde_json::Value = serde_json::from_slice(&captured.body).unwrap();
        assert_eq!(forwarded["model"], "gpt-5.6-sol");
        // The rest of the body is the client's and must arrive unchanged.
        assert_eq!(forwarded["store"], false);
        assert_eq!(forwarded["input"], "hi");
    }

    #[tokio::test]
    async fn responses_passthrough_forwards_an_unaliased_body_byte_for_byte() {
        // No alias entry means no JSON round-trip at all: key order and
        // formatting reach the upstream exactly as the client wrote them.
        let pool = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let app = test_router_with_upstream(1024 * 1024, pool.base_url.clone());

        const BODY: &str = r#"{"store":false,   "model":"gpt-6-astra","input":"hi"}"#;
        let response = app.oneshot(responses_request(BODY)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let captured = pool.recv().await;
        assert_eq!(captured.body, BODY.as_bytes());
    }

    #[tokio::test]
    async fn pool_exhausted_falls_over_to_configured_provider() {
        let pool = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            r#"{"error":"rate limited"}"#,
        )
        .await;
        let fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);

        let pool_req = pool.recv().await;
        let pool_body: serde_json::Value = serde_json::from_slice(&pool_req.body).unwrap();
        assert_eq!(pool_body["model"], "gpt-5.6-luna");

        let fallback_req = fallback_fake.recv().await;
        let fallback_body: serde_json::Value = serde_json::from_slice(&fallback_req.body).unwrap();
        assert_eq!(fallback_body["model"], "gpt-5.6-luna-on-fallback");
    }

    #[tokio::test]
    async fn quota_exhausted_pool_is_skipped_and_fallback_serves_directly() {
        let mut pool = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            USAGE_LIMIT_429_BODY,
        )
        .await;
        let mut fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        // Cooldown off: only the quota hold can make the pool unavailable.
        let app = test_router_with_fallback_and_cooldown(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
            0,
        );

        // First request: the pool is tried, says quota exhausted, fallback
        // serves. This is the pre-existing failover path.
        let response = app
            .clone()
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            pool.rx.recv().await.is_some(),
            "first request must reach the pool"
        );
        assert!(fallback_fake.rx.recv().await.is_some());

        // Second request: the pool is known-exhausted, so it must not be
        // touched at all — straight to the fallback provider.
        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"again"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);
        let fallback_req = fallback_fake.rx.recv().await.unwrap();
        let fallback_body: serde_json::Value = serde_json::from_slice(&fallback_req.body).unwrap();
        assert_eq!(fallback_body["input"], "again");
        assert!(
            pool.rx.try_recv().is_err(),
            "quota-exhausted pool must not be hit while a fallback exists"
        );
    }

    #[tokio::test]
    async fn plain_429_does_not_short_circuit_the_pool() {
        // Same shape as the quota test, but the 429 body is a plain throttle:
        // no quota hold, cooldown off, so request 2 must reach the pool again.
        // This is the discriminator that the quota test alone can't provide.
        let mut pool = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            r#"{"error":"rate limited"}"#,
        )
        .await;
        let mut fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        let app = test_router_with_fallback_and_cooldown(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
            0,
        );

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(responses_request(
                    r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert!(
                pool.rx.recv().await.is_some(),
                "a throttled pool is retried every request"
            );
            assert!(fallback_fake.rx.recv().await.is_some());
        }
    }

    #[tokio::test]
    async fn unavailable_pool_is_still_tried_when_no_chain_is_configured() {
        // The config.rs promise: without a [[fallback]] chain the pool tries
        // an account even when every one is cooling down — a shaky account
        // beats refusing the request. Two requests inside one cooldown.
        let mut pool = start_fake_upstream(
            StatusCode::FORBIDDEN,
            "application/json",
            r#"{"error":"banned"}"#,
        )
        .await;
        let app = test_router_with_fallback(pool.base_url.clone(), vec![]);

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(responses_request(
                    r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
                ))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], br#"{"error":"banned"}"#);
            assert!(
                pool.rx.recv().await.is_some(),
                "with no chain the cooling account must still be tried"
            );
        }
    }

    #[tokio::test]
    async fn quota_exhausted_pool_is_still_tried_when_the_chain_declines_the_model() {
        // The chain only maps gpt-5.6-luna. A request for another model finds no
        // provider, so the skipped pool must get its normal shot instead of
        // the client seeing a synthetic error with zero upstream attempts.
        let mut pool = start_fake_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            USAGE_LIMIT_429_BODY,
        )
        .await;
        let mut fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        let app = test_router_with_fallback_and_cooldown(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
            0,
        );

        // Put the pool into quota-exhausted state.
        let response = app
            .clone()
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = pool.rx.recv().await;
        let _ = fallback_fake.rx.recv().await;

        // Unmapped model: pool tried anyway, client sees the real 429.
        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let pool_req = pool
            .rx
            .recv()
            .await
            .expect("pool must be tried when the chain declines");
        let pool_body: serde_json::Value = serde_json::from_slice(&pool_req.body).unwrap();
        assert_eq!(pool_body["model"], "gpt-6-astra");
        assert!(fallback_fake.rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn pool_5xx_also_falls_over_to_configured_provider() {
        // A whole-service Codex outage returns 5xx to every pool account
        // alike — not an *account* failure (`is_account_failure` is
        // 401/403/429 only), but exactly the case a fallback matters most
        // for. The outer gate must not be narrower than that.
        let pool = start_fake_upstream(
            StatusCode::SERVICE_UNAVAILABLE,
            "application/json",
            r#"{"error":"upstream down"}"#,
        )
        .await;
        let fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        let app = test_router_with_fallback(
            pool.base_url.clone(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);

        let _ = pool.recv().await;
        let _ = fallback_fake.recv().await;
    }

    #[tokio::test]
    async fn unmapped_fallback_provider_is_skipped_client_sees_original_failure() {
        let pool = start_fake_upstream(
            StatusCode::FORBIDDEN,
            "application/json",
            r#"{"error":"banned"}"#,
        )
        .await;
        let mut fallback_fake =
            start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let mut cfg = fallback_provider_cfg("fb", &fallback_fake.base_url);
        cfg.model_map.clear();
        let app = test_router_with_fallback(pool.base_url.clone(), vec![cfg]);

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"banned"}"#);

        let _ = pool.recv().await;
        assert!(
            fallback_fake.rx.try_recv().is_err(),
            "a provider without a mapping for the requested model must never be called"
        );
    }

    #[tokio::test]
    async fn empty_fallback_chain_matches_pre_fallback_behavior() {
        let pool = start_fake_upstream(
            StatusCode::FORBIDDEN,
            "application/json",
            r#"{"error":"banned"}"#,
        )
        .await;
        let app = test_router_with_fallback(pool.base_url.clone(), vec![]);

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"banned"}"#);
    }

    #[tokio::test]
    async fn pool_transport_failure_falls_over_to_configured_provider() {
        let fallback_fake = start_fake_upstream(
            StatusCode::OK,
            "application/json",
            r#"{"ok":"from-fallback"}"#,
        )
        .await;
        // Nothing listens on port 1 on loopback — a connection attempt here
        // fails fast (refused), so the pool's only account errors out with a
        // transport error, not just a bad HTTP status.
        let app = test_router_with_fallback(
            "http://127.0.0.1:1".to_string(),
            vec![fallback_provider_cfg("fb", &fallback_fake.base_url)],
        );

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"ok":"from-fallback"}"#);

        let _ = fallback_fake.recv().await;
    }

    #[tokio::test]
    async fn metrics_endpoint_reports_a_request_made_through_the_main_router() {
        let fake = start_fake_upstream(StatusCode::OK, "application/json", r#"{"ok":true}"#).await;
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = fake.base_url.clone();

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        // The same Arc<Metrics> backs both routers, mirroring how main.rs
        // shares one Metrics instance between the client-facing API and the
        // separate metrics-port router.
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics: metrics.clone(),
            embeddings: None,
        });
        let metrics_app = metrics_router(metrics);

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // `/v1/responses` streams the body; `emit()` (and so the metrics
        // recording) only fires once the SSE stream fully drains, so the
        // response must actually be read here, not just status-checked.
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let _ = fake.recv().await;

        let metrics_response = metrics_app
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("codexproxy_requests_total"));
        assert!(text.contains(r#"endpoint="/v1/responses""#));
        assert!(text.contains(r#"status="200""#));
    }

    #[tokio::test]
    async fn metrics_endpoint_clamps_an_unrecognized_client_supplied_model() {
        let upstream_body = concat!(
            "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
            "data: [DONE]\n\n"
        );
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", upstream_body).await;
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = fake.base_url.clone();

        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        let metrics = Arc::new(Metrics::new().expect("build metrics"));

        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics: metrics.clone(),
            embeddings: None,
        });
        let metrics_app = metrics_router(metrics);

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "totally-unrecognized-client-supplied-garbage",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let _ = fake.recv().await;

        let metrics_response = metrics_app
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains(r#"model="other""#));
        assert!(!text.contains("totally-unrecognized-client-supplied-garbage"));
    }

    #[tokio::test]
    async fn buffered_chat_completions_errors_when_a_single_sse_event_exceeds_max_body_bytes() {
        // A single "data: ..." line with no "\n\n" anywhere — an event
        // boundary never arrives, the pathological case the byte cap guards
        // against. `Box::leak` just to get a genuine `&'static str` at
        // runtime for the fixture, which only accepts a literal-shaped body.
        let body: &'static str = Box::leak(format!("data: {}", "x".repeat(300)).into_boxed_str());
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", body).await;
        let app = test_router_with_upstream(200, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-5.6-luna",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn responses_passthrough_still_forwards_full_body_when_usage_scanner_gives_up() {
        // Same pathological body as above, but through the raw `/v1/responses`
        // passthrough: the usage scanner is a side channel (see
        // `tee_responses`'s docs) — giving up on token attribution must NOT
        // truncate or otherwise affect what the client actually receives.
        let body: &'static str = Box::leak(format!("data: {}", "x".repeat(300)).into_boxed_str());
        let fake = start_fake_upstream(StatusCode::OK, "text/event-stream", body).await;
        let app = test_router_with_upstream(200, fake.base_url.clone());

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let received = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(&received[..], body.as_bytes());
    }

    #[tokio::test]
    async fn chat_completions_error_passthrough_truncates_an_oversized_upstream_error_body() {
        // A non-success status with a huge error body — `reqwest::Response::
        // bytes()` (the old implementation) has no cap of its own, so this
        // must be bounded the same way the success-path SSE buffers are.
        let body: &'static str = Box::leak("x".repeat(500).into_boxed_str());
        let fake = start_fake_upstream(StatusCode::BAD_REQUEST, "application/json", body).await;
        let app = test_router_with_upstream(200, fake.base_url.clone());

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({
                            "model": "gpt-5.6-luna",
                            "messages": [{ "role": "user", "content": "hi" }],
                            "stream": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // The real upstream status/content-type still pass through unchanged
        // — only the body length is bounded, not the error's meaning.
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let received = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(received.len(), 200, "must truncate exactly to the cap");
        assert!(received.len() < body.len());
    }

    // ---- cost guardrails: model downgrades, pool 400 relay, unknown models ----

    /// Build a router (and keep its metrics) from a fully custom config — the
    /// guardrail tests flip `[models]` switches the other helpers don't expose.
    fn guarded_router(config: Config) -> (axum::Router, Arc<Metrics>) {
        let http = reqwest::Client::new();
        let auth = AuthManager::load(
            &config.upstream,
            write_test_auth_json("acct_test"),
            http.clone(),
        )
        .expect("load test auth");
        let upstream = Arc::new(Upstream::new(
            &config.upstream,
            http.clone(),
            vec![(auth, "test-account".to_string())],
        ));
        let fallback =
            Arc::new(FallbackChain::new(http, &config.fallback).expect("build fallback chain"));
        let metrics = Arc::new(Metrics::new().expect("build metrics"));
        let app = router(AppState {
            config: Arc::new(config),
            upstream,
            fallback,
            metrics: metrics.clone(),
            embeddings: None,
        });
        (app, metrics)
    }

    fn guarded_config(pool_url: &str, fallback_url: &str, cooldown_secs: u64) -> Config {
        let mut config = Config::default();
        config.client_auth.keys = vec![bare_key("test-key")];
        config.upstream.base_url = pool_url.to_string();
        config.upstream.account_cooldown_secs = cooldown_secs;
        let mut provider = fallback_provider_cfg("fb", fallback_url);
        provider
            .model_map
            .insert("gpt-6-astra".to_string(), "astra-on-fallback".to_string());
        config.fallback = vec![provider];
        config
    }

    /// A pool that answers per requested model — the other fakes answer the
    /// same thing whatever they're sent, which can't show a downgrade working.
    /// Records every model it was asked for, in order.
    struct ModelAwareUpstream {
        base_url: String,
        seen: Arc<std::sync::Mutex<Vec<String>>>,
        bodies: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl ModelAwareUpstream {
        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }

        fn bodies(&self) -> Vec<serde_json::Value> {
            self.bodies.lock().unwrap().clone()
        }
    }

    async fn start_model_aware_upstream(
        replies: Vec<(&'static str, StatusCode, &'static str)>,
    ) -> ModelAwareUpstream {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let bodies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let replies = Arc::new(replies);
        let handler = {
            let seen = seen.clone();
            let bodies = bodies.clone();
            move |body: Bytes| {
                let seen = seen.clone();
                let bodies = bodies.clone();
                let replies = replies.clone();
                async move {
                    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let model = parsed["model"].as_str().unwrap_or("").to_string();
                    seen.lock().unwrap().push(model.clone());
                    bodies.lock().unwrap().push(parsed);
                    let (status, reply) = replies
                        .iter()
                        .find(|(m, _, _)| *m == model)
                        .map(|(_, s, b)| (*s, *b))
                        .unwrap_or((StatusCode::IM_A_TEAPOT, "unexpected model"));
                    Response::builder()
                        .status(status)
                        .header("Content-Type", "application/json")
                        .body(Body::from(reply))
                        .unwrap()
                }
            }
        };
        let app = Router::new()
            .route("/codex/responses", post(handler.clone()))
            .route("/responses", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        ModelAwareUpstream {
            base_url: format!("http://{addr}"),
            seen,
            bodies,
        }
    }

    async fn body_string(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn scrape(metrics: Arc<Metrics>) -> String {
        let response = metrics_router(metrics)
            .oneshot(
                HttpRequest::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        body_string(response).await
    }

    const THROTTLE_429: &str = r#"{"error":{"type":"rate_limit_exceeded","message":"slow down"}}"#;

    #[tokio::test]
    async fn throttled_model_is_served_by_a_lower_model_on_the_pool() {
        let pool = start_model_aware_upstream(vec![
            ("gpt-6-astra", StatusCode::TOO_MANY_REQUESTS, THROTTLE_429),
            ("gpt-5.6-sol", StatusCode::OK, r#"{"ok":"sol"}"#),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, metrics) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_string(response).await, r#"{"ok":"sol"}"#);
        assert_eq!(pool.seen(), ["gpt-6-astra", "gpt-5.6-sol"]);
        assert!(
            fallback.seen().is_empty(),
            "no paid fallback when the downgrade served"
        );
        assert!(scrape(metrics)
            .await
            .contains(r#"codexproxy_model_downgrades_total{client="key-"#));
    }

    #[tokio::test]
    async fn cooling_pool_still_gets_the_downgrade_before_the_paid_fallback() {
        // One account, 30s cooldown: after the first 429 every request finds
        // the pool "unavailable". The downgrade must still go to the pool.
        let pool = start_model_aware_upstream(vec![
            ("gpt-6-astra", StatusCode::TOO_MANY_REQUESTS, THROTTLE_429),
            ("gpt-5.6-sol", StatusCode::OK, r#"{"ok":"sol"}"#),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        // Request 2 skipped the cooling astra attempt and went straight to sol.
        assert_eq!(pool.seen(), ["gpt-6-astra", "gpt-5.6-sol", "gpt-5.6-sol"]);
        assert!(fallback.seen().is_empty());
    }

    #[tokio::test]
    async fn failed_downgrade_falls_back_with_the_original_model() {
        let pool = start_model_aware_upstream(vec![
            ("gpt-6-astra", StatusCode::TOO_MANY_REQUESTS, THROTTLE_429),
            ("gpt-5.6-sol", StatusCode::TOO_MANY_REQUESTS, THROTTLE_429),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![(
            "astra-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let (app, metrics) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_string(response).await, r#"{"ok":"fb"}"#);
        assert_eq!(pool.seen(), ["gpt-6-astra", "gpt-5.6-sol"]);
        assert_eq!(fallback.seen(), ["astra-on-fallback"]);
        let scraped = scrape(metrics).await;
        assert!(scraped.contains(r#"outcome="failed""#), "{scraped}");
        assert!(
            scraped.contains(r#"codexproxy_failovers_total{client="key-"#)
                && scraped.contains(r#"model="gpt-6-astra",reason="rate_limit""#),
            "{scraped}"
        );
    }

    #[tokio::test]
    async fn quota_429_is_account_wide_so_no_downgrade_is_tried() {
        let pool = start_model_aware_upstream(vec![
            (
                "gpt-6-astra",
                StatusCode::TOO_MANY_REQUESTS,
                USAGE_LIMIT_429_BODY,
            ),
            ("gpt-5.6-sol", StatusCode::OK, r#"{"ok":"sol"}"#),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![(
            "astra-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 0));

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(pool.seen(), ["gpt-6-astra"]);
        assert_eq!(fallback.seen(), ["astra-on-fallback"]);
    }

    #[tokio::test]
    async fn empty_downgrade_map_goes_straight_to_the_fallback() {
        let pool = start_model_aware_upstream(vec![
            ("gpt-6-astra", StatusCode::TOO_MANY_REQUESTS, THROTTLE_429),
            ("gpt-5.6-sol", StatusCode::OK, r#"{"ok":"sol"}"#),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![(
            "astra-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let mut config = guarded_config(&pool.base_url, &fallback.base_url, 30);
        config.models.downgrades.clear();
        let (app, _) = guarded_router(config);

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(pool.seen(), ["gpt-6-astra"]);
        assert_eq!(fallback.seen(), ["astra-on-fallback"]);
    }

    #[tokio::test]
    async fn pool_400_is_relayed_instead_of_paying_the_fallback() {
        const REFUSED: &str = r#"{"detail":"The 'gpt-5.6-luna' model is not supported when using Codex with a ChatGPT account."}"#;
        let pool =
            start_model_aware_upstream(vec![("gpt-5.6-luna", StatusCode::BAD_REQUEST, REFUSED)])
                .await;
        let fallback = start_model_aware_upstream(vec![(
            "gpt-5.6-luna-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let (app, metrics) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_string(response).await, REFUSED);
        assert!(fallback.seen().is_empty());
        assert!(scrape(metrics)
            .await
            .contains(r#"reason="pool_bad_request""#));
    }

    #[tokio::test]
    async fn pool_400_still_falls_back_when_explicitly_allowed() {
        let pool =
            start_model_aware_upstream(vec![("gpt-5.6-luna", StatusCode::BAD_REQUEST, "{}")]).await;
        let fallback = start_model_aware_upstream(vec![(
            "gpt-5.6-luna-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let mut config = guarded_config(&pool.base_url, &fallback.base_url, 30);
        config.models.fallback_on_bad_request = true;
        let (app, _) = guarded_router(config);

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(fallback.seen(), ["gpt-5.6-luna-on-fallback"]);
    }

    #[tokio::test]
    async fn unknown_model_is_rejected_before_any_upstream_on_both_endpoints() {
        let pool = start_model_aware_upstream(vec![]).await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, metrics) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .clone()
            .oneshot(responses_request(
                r#"{"model":"gpt-5.2-codex","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_str(&body_string(response).await).unwrap();
        assert_eq!(body["error"]["code"], "model_not_found");
        assert!(body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("gpt-6-astra"));

        let response = app
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({"model": "gpt-4.1", "messages": [{"role": "user", "content": "hi"}]})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        assert!(pool.seen().is_empty() && fallback.seen().is_empty());
        let scraped = scrape(metrics).await;
        assert!(
            scraped.contains(r#"reason="unknown_model"} 2"#),
            "{scraped}"
        );
    }

    #[tokio::test]
    async fn gpt_6_sol_and_luna_are_accepted_and_advertised() {
        let pool = start_model_aware_upstream(vec![
            ("gpt-6-sol", StatusCode::OK, "{}"),
            ("gpt-6-luna", StatusCode::OK, "{}"),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));
        for model in ["gpt-6-sol", "gpt-6-luna"] {
            let body = format!(r#"{{"model":"{model}","input":"hi"}}"#);
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri("/v1/responses")
                        .header("Authorization", "Bearer test-key")
                        .header("Content-Type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{model}");
        }
        assert_eq!(pool.seen(), ["gpt-6-sol", "gpt-6-luna"]);
        let listed = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let listed = body_string(listed).await;
        assert!(listed.contains("gpt-6-sol") && listed.contains("gpt-6-luna"));
        assert!(
            !listed.contains("gpt-reserve"),
            "hidden catalog models aren't advertised"
        );
    }

    #[tokio::test]
    async fn retired_generations_are_refused_not_billed() {
        // Generations the Codex catalog no longer lists: the pool 400s them,
        // and before the gate a paid provider carrying them served them.
        let pool = start_model_aware_upstream(vec![]).await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));
        for model in ["gpt-5.4", "gpt-5.2-codex", "gpt-4o"] {
            let body = format!(r#"{{"model":"{model}","input":"hi"}}"#);
            let response = app
                .clone()
                .oneshot(
                    HttpRequest::builder()
                        .method("POST")
                        .uri("/v1/responses")
                        .header("Authorization", "Bearer test-key")
                        .header("Content-Type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{model}");
        }
        assert!(pool.seen().is_empty() && fallback.seen().is_empty());
    }

    #[tokio::test]
    async fn aliases_and_extra_models_pass_the_unknown_model_gate() {
        let pool = start_model_aware_upstream(vec![
            ("gpt-6-astra", StatusCode::OK, "{}"),
            ("gpt-6-astra-pro", StatusCode::OK, "{}"),
        ])
        .await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let mut config = guarded_config(&pool.base_url, &fallback.base_url, 30);
        config.models.extra = vec!["gpt-6-astra-pro".to_string()];
        let (app, _) = guarded_router(config);

        for body in [
            r#"{"model":"gpt-6","input":"hi"}"#,
            r#"{"model":"gpt-6-astra-pro","input":"hi"}"#,
        ] {
            let response = app.clone().oneshot(responses_request(body)).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{body}");
        }
        assert_eq!(pool.seen(), ["gpt-6-astra", "gpt-6-astra-pro"]);
    }

    #[tokio::test]
    async fn unknown_models_pass_through_when_the_gate_is_off() {
        let pool = start_model_aware_upstream(vec![("gpt-5.2-codex", StatusCode::OK, "{}")]).await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let mut config = guarded_config(&pool.base_url, &fallback.base_url, 30);
        config.models.reject_unknown = false;
        let (app, _) = guarded_router(config);

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.2-codex","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(pool.seen(), ["gpt-5.2-codex"]);
    }

    // ---- conversation affinity: prompt_cache_key, sticky fallback, cache metrics ----

    const COMPLETED_SSE: &str = "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1000,\"output_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":800,\"cache_write_tokens\":150}}}}\n\n";

    fn chat_request(messages: serde_json::Value, extra: serde_json::Value) -> HttpRequest<Body> {
        let mut body = json!({"model": "gpt-6-astra", "messages": messages, "stream": true});
        if let (Some(obj), Some(more)) = (body.as_object_mut(), extra.as_object()) {
            obj.extend(more.clone());
        }
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    #[tokio::test]
    async fn chat_turns_of_one_conversation_carry_one_derived_prompt_cache_key() {
        let pool =
            start_model_aware_upstream(vec![("gpt-6-astra", StatusCode::OK, COMPLETED_SSE)]).await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let turn1 =
            json!([{"role": "system", "content": "sys"}, {"role": "user", "content": "task A"}]);
        let turn2 = json!([{"role": "system", "content": "sys"}, {"role": "user", "content": "task A"},
                           {"role": "assistant", "content": "ok"}, {"role": "user", "content": "more"}]);
        let other =
            json!([{"role": "system", "content": "sys"}, {"role": "user", "content": "task B"}]);
        for messages in [turn1, turn2, other] {
            let response = app
                .clone()
                .oneshot(chat_request(messages, json!({})))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let _ = body_string(response).await;
        }
        let keys: Vec<String> = pool
            .bodies()
            .iter()
            .map(|b| b["prompt_cache_key"].as_str().unwrap().to_string())
            .collect();
        assert!(keys[0].starts_with("cp-"), "{keys:?}");
        assert_eq!(keys[0], keys[1], "turns of one conversation share a key");
        assert_ne!(keys[0], keys[2], "different conversations don't");
    }

    #[tokio::test]
    async fn a_client_prompt_cache_key_reaches_the_pool_unchanged() {
        let pool =
            start_model_aware_upstream(vec![("gpt-6-astra", StatusCode::OK, COMPLETED_SSE)]).await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .oneshot(chat_request(
                json!([{"role": "user", "content": "hi"}]),
                json!({"prompt_cache_key": "client-key"}),
            ))
            .await
            .unwrap();
        let _ = body_string(response).await;
        assert_eq!(pool.bodies()[0]["prompt_cache_key"], "client-key");
    }

    #[tokio::test]
    async fn a_sticky_fallback_gets_the_conversation_key_as_session_id() {
        let pool = start_model_aware_upstream(vec![(
            "gpt-5.6-luna",
            StatusCode::TOO_MANY_REQUESTS,
            THROTTLE_429,
        )])
        .await;
        let fallback = start_model_aware_upstream(vec![(
            "gpt-5.6-luna-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let mut config = guarded_config(&pool.base_url, &fallback.base_url, 30);
        config.fallback[0].sticky_session = true;
        let (app, _) = guarded_router(config);

        // Header key: forwarded verbatim as both fields.
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/v1/responses")
            .header("Authorization", "Bearer test-key")
            .header("Content-Type", "application/json")
            .header("session-id", "sess-abc")
            .body(Body::from(r#"{"model":"gpt-5.6-luna","input":"hi"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        // A client's own session_id is never overwritten.
        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","session_id":"mine","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let bodies = fallback.bodies();
        assert_eq!(bodies[0]["session_id"], "sess-abc");
        assert_eq!(bodies[0]["prompt_cache_key"], "sess-abc");
        assert_eq!(bodies[1]["session_id"], "mine");
        assert!(bodies[1]["prompt_cache_key"]
            .as_str()
            .unwrap()
            .starts_with("cp-"));
    }

    #[tokio::test]
    async fn a_non_sticky_fallback_body_gets_no_session_fields() {
        let pool = start_model_aware_upstream(vec![(
            "gpt-5.6-luna",
            StatusCode::TOO_MANY_REQUESTS,
            THROTTLE_429,
        )])
        .await;
        let fallback = start_model_aware_upstream(vec![(
            "gpt-5.6-luna-on-fallback",
            StatusCode::OK,
            r#"{"ok":"fb"}"#,
        )])
        .await;
        let (app, _) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .oneshot(responses_request(
                r#"{"model":"gpt-5.6-luna","input":"hi"}"#,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = &fallback.bodies()[0];
        assert!(body.get("session_id").is_none() && body.get("prompt_cache_key").is_none());
    }

    #[tokio::test]
    async fn cached_and_cache_write_tokens_are_counted() {
        let pool =
            start_model_aware_upstream(vec![("gpt-6-astra", StatusCode::OK, COMPLETED_SSE)]).await;
        let fallback = start_model_aware_upstream(vec![]).await;
        let (app, metrics) = guarded_router(guarded_config(&pool.base_url, &fallback.base_url, 30));

        let response = app
            .oneshot(responses_request(r#"{"model":"gpt-6-astra","input":"hi"}"#))
            .await
            .unwrap();
        let _ = body_string(response).await;
        let scraped = scrape(metrics).await;
        for (kind, n) in [("prompt", 1000), ("cached", 800), ("cache_write", 150)] {
            let needle = format!(r#"kind="{kind}",model="gpt-6-astra"}} {n}"#);
            assert!(scraped.contains(&needle), "missing {needle} in {scraped}");
        }
    }
}
