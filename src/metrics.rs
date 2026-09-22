//! Prometheus metrics, recorded from the same single choke point the access
//! log uses (`observe::CompletionLog::emit`) so instrumentation doesn't get
//! scattered across the four request-completion paths (buffered/streaming ×
//! `/v1/chat/completions`/`/v1/responses`).

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder,
};

/// One completed request, as `Metrics::record` needs it — bundled into a
/// struct rather than passed as separate arguments (clippy's
/// `too_many_arguments`), and it mirrors what `CompletionLog::emit` already
/// logs, so the two stay easy to compare at a glance.
pub struct RequestOutcome<'a> {
    pub endpoint: &'a str,
    pub client: &'a str,
    pub account: &'a str,
    pub model: &'a str,
    pub status: u16,
    pub usage: Option<(i64, i64)>,
    pub duration_secs: f64,
}

pub struct Metrics {
    registry: Registry,
    requests_total: IntCounterVec,
    tokens_total: IntCounterVec,
    request_duration_seconds: HistogramVec,
    failovers_total: IntCounterVec,
    model_downgrades_total: IntCounterVec,
    rejected_requests_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            Opts::new(
                "codexproxy_requests_total",
                "Total requests handled, by outcome",
            ),
            &["endpoint", "client", "account", "model", "status"],
        )?;
        registry.register(Box::new(requests_total.clone()))?;

        let tokens_total = IntCounterVec::new(
            Opts::new(
                "codexproxy_tokens_total",
                "Total upstream tokens spent, by client key",
            ),
            &["client", "account", "model", "kind"],
        )?;
        registry.register(Box::new(tokens_total.clone()))?;

        // LLM requests are frequently long-lived streams (many seconds to a
        // few minutes), not the sub-second REST calls Prometheus client
        // libraries' default histogram buckets are tuned for.
        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "codexproxy_request_duration_seconds",
                "Request duration in seconds, start to completion (or client disconnect)",
            )
            .buckets(vec![
                0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
            ]),
            &["endpoint", "client", "account", "model"],
        )?;
        registry.register(Box::new(request_duration_seconds.clone()))?;

        // The cost-alerting counters. `codexproxy_requests_total` only says
        // which account served a request; these say WHY the pool didn't —
        // quota vs throttle vs a model the pool refuses — which is what an
        // alert has to tell apart (a quota hit waits for a reset, a refused
        // model is a client to fix). Every label is a closed set: `model`
        // is clamped by the caller, `reason` is `FailureReason::as_str`.
        let failovers_total = IntCounterVec::new(
            Opts::new(
                "codexproxy_failovers_total",
                "Requests the subscription pool could not serve that a paid fallback provider served instead",
            ),
            &["client", "model", "reason", "fallback"],
        )?;
        registry.register(Box::new(failovers_total.clone()))?;

        let model_downgrades_total = IntCounterVec::new(
            Opts::new(
                "codexproxy_model_downgrades_total",
                "Pool retries with a lower model after the requested one was throttled, by outcome (served|failed)",
            ),
            &["client", "from", "to", "outcome"],
        )?;
        registry.register(Box::new(model_downgrades_total.clone()))?;

        let rejected_requests_total = IntCounterVec::new(
            Opts::new(
                "codexproxy_rejected_requests_total",
                "Requests refused instead of forwarded to a paid fallback: unknown_model (proxy-side) or pool_bad_request (the pool's own 4xx, relayed)",
            ),
            &["client", "reason"],
        )?;
        registry.register(Box::new(rejected_requests_total.clone()))?;

        Ok(Self {
            registry,
            requests_total,
            tokens_total,
            request_duration_seconds,
            failovers_total,
            model_downgrades_total,
            rejected_requests_total,
        })
    }

    /// Record one completed request. Called once from
    /// `CompletionLog::emit`, mirroring exactly what that call already logs.
    pub fn record(&self, outcome: RequestOutcome) {
        let status = outcome.status.to_string();
        self.requests_total
            .with_label_values(&[
                outcome.endpoint,
                outcome.client,
                outcome.account,
                outcome.model,
                &status,
            ])
            .inc();
        self.request_duration_seconds
            .with_label_values(&[
                outcome.endpoint,
                outcome.client,
                outcome.account,
                outcome.model,
            ])
            .observe(outcome.duration_secs);
        if let Some((prompt, completion)) = outcome.usage {
            self.tokens_total
                .with_label_values(&[outcome.client, outcome.account, outcome.model, "prompt"])
                .inc_by(prompt.max(0) as u64);
            self.tokens_total
                .with_label_values(&[outcome.client, outcome.account, outcome.model, "completion"])
                .inc_by(completion.max(0) as u64);
        }
    }

    /// One pool -> paid-fallback switch. Called next to `log_failover`.
    pub fn record_failover(&self, client: &str, model: &str, reason: &str, fallback: &str) {
        self.failovers_total
            .with_label_values(&[client, model, reason, fallback])
            .inc();
    }

    /// One in-pool model downgrade attempt; `served` = the lower model
    /// answered, so no paid fallback was needed for this request.
    pub fn record_downgrade(&self, client: &str, from: &str, to: &str, served: bool) {
        let outcome = if served { "served" } else { "failed" };
        self.model_downgrades_total
            .with_label_values(&[client, from, to, outcome])
            .inc();
    }

    /// One request refused rather than paid for; `reason` is
    /// `unknown_model` or `pool_bad_request`.
    pub fn record_rejected(&self, client: &str, reason: &str) {
        self.rejected_requests_total
            .with_label_values(&[client, reason])
            .inc();
    }

    /// Prometheus text-exposition encoding of the current metric values, for
    /// the `/metrics` handler: `(Content-Type, body)`.
    pub fn encode(&self) -> (String, Vec<u8>) {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .expect("encoding an in-process Prometheus registry cannot fail");
        (encoder.format_type().to_string(), buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(m: &Metrics, name: &str) -> prometheus::proto::MetricFamily {
        m.registry
            .gather()
            .into_iter()
            .find(|f| f.get_name() == name)
            .unwrap_or_else(|| panic!("metric family {name} not registered"))
    }

    #[test]
    fn record_increments_requests_and_tokens_with_expected_labels() {
        let metrics = Metrics::new().unwrap();
        metrics.record(RequestOutcome {
            endpoint: "/v1/chat/completions",
            client: "alice",
            account: "primary",
            model: "gpt-5.5",
            status: 200,
            usage: Some((10, 20)),
            duration_secs: 1.5,
        });

        let requests = metrics.registry.gather();
        let requests_family = requests
            .iter()
            .find(|f| f.get_name() == "codexproxy_requests_total")
            .unwrap();
        let metric = &requests_family.get_metric()[0];
        assert_eq!(metric.get_counter().get_value(), 1.0);
        let labels: std::collections::HashMap<_, _> = metric
            .get_label()
            .iter()
            .map(|l| (l.get_name(), l.get_value()))
            .collect();
        assert_eq!(labels["endpoint"], "/v1/chat/completions");
        assert_eq!(labels["client"], "alice");
        assert_eq!(labels["account"], "primary");
        assert_eq!(labels["model"], "gpt-5.5");
        assert_eq!(labels["status"], "200");

        let tokens_family = family(&metrics, "codexproxy_tokens_total");
        let mut by_kind = std::collections::HashMap::new();
        for m in tokens_family.get_metric() {
            let kind = m
                .get_label()
                .iter()
                .find(|l| l.get_name() == "kind")
                .unwrap()
                .get_value()
                .to_string();
            by_kind.insert(kind, m.get_counter().get_value());
        }
        assert_eq!(by_kind["prompt"], 10.0);
        assert_eq!(by_kind["completion"], 20.0);
    }

    #[test]
    fn record_without_usage_does_not_touch_tokens_total() {
        let metrics = Metrics::new().unwrap();
        metrics.record(RequestOutcome {
            endpoint: "/v1/responses",
            client: "bob",
            account: "-",
            model: "-",
            status: 429,
            usage: None,
            duration_secs: 0.05,
        });

        // No sample was ever recorded for this metric, so a never-observed
        // `IntCounterVec` doesn't appear in `gather()` at all (not present
        // with zero series — genuinely absent).
        let has_tokens_family = metrics
            .registry
            .gather()
            .into_iter()
            .any(|f| f.get_name() == "codexproxy_tokens_total");
        assert!(!has_tokens_family);
    }

    #[test]
    fn encode_produces_prometheus_text_exposition_format() {
        let metrics = Metrics::new().unwrap();
        metrics.record(RequestOutcome {
            endpoint: "/v1/chat/completions",
            client: "alice",
            account: "primary",
            model: "gpt-5.5",
            status: 200,
            usage: Some((1, 2)),
            duration_secs: 0.1,
        });

        let (content_type, body) = metrics.encode();
        assert!(content_type.starts_with("text/plain"));
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains("# HELP codexproxy_requests_total"));
        assert!(text.contains("# TYPE codexproxy_requests_total counter"));
        assert!(text.contains("codexproxy_tokens_total"));
        assert!(text.contains("codexproxy_request_duration_seconds"));
    }
}
