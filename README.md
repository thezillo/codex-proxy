# codex-proxy

OpenAI-compatible proxy for the Codex (ChatGPT subscription) Responses API.
Point any OpenAI client at it; it forwards requests to `chatgpt.com` using the
credentials from `codex login`, refreshing the token as needed. Rust, single
binary, no database. Request format and TLS fingerprint match the official
Codex client.

What a running instance gives you:

- `http://<host>:8787/v1` — an OpenAI-compatible endpoint (chat completions +
  raw Responses passthrough), guarded by your own client API keys.
- `http://127.0.0.1:9090/metrics` — Prometheus counters, on a separate port so
  it stays private even when the API is public.
- A data directory holding `auth.json` — the ChatGPT credentials and the
  rotated refresh token. **Keep it**; it's the only stateful thing here.
- A **~3 MiB** resident footprint. Measured on the container over a week of
  production traffic: 2.9 MiB min, 3.2 MiB mean, 6.1 MiB peak. It's a static
  musl binary — no interpreter, no GC, no database behind it — so it fits on
  the smallest instance any host will sell you.

## Quick start (Docker)

Needs `~/.codex/auth.json` from `codex login` (the official CLI), once.

```sh
KEY=$(openssl rand -hex 24); echo "client key: $KEY"

docker run -d --name codex-proxy -p 8787:8787 \
  -v codex_data:/data \
  -e CODEXPROXY_DATA_DIR=/data \
  -e CODEXPROXY_API_KEYS="$KEY" \
  -e CODEXPROXY_AUTH_JSON="$(cat ~/.codex/auth.json)" \
  ghcr.io/thezillo/codex-proxy:latest
```

Then send requests to `http://localhost:8787/v1` with that value as the bearer
token:

```sh
curl http://localhost:8787/v1/chat/completions \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"model":"gpt-6-astra","stream":true,
       "messages":[{"role":"user","content":"hi"}]}'
```

`model` is required. One that doesn't look like a real id (anything not
starting with `gpt-`/`o`) is replaced by `defaults.model` (`gpt-6-astra`);
`[defaults.model_aliases]` is applied first. Check the proxy is alive with
`curl localhost:8787/health` — that endpoint needs no auth.

`CODEXPROXY_AUTH_JSON` seeds `auth.json` only when the data directory is
empty; after that the rotated token on the volume wins, so the env var is
harmless on later restarts but also can't be used to *replace* credentials.

Image tags: `latest`, `v0.3.3`, `sha-<commit>` (GHCR, built on push to `main`
and on `v*` tags). Pin a digest for anything you care about.

## Full run (named keys, metrics, fallback)

`CODEXPROXY_API_KEYS` is the quick path, but it **replaces the entire key list
with unnamed keys** — access logs then attribute spend to a fingerprint
(`client=key-1a2b3c4d`) instead of a name. To see *who* is spending tokens,
declare keys in a config file instead and mount it:

```sh
cat > config.toml <<'EOF'
[client_auth]
require = true

[[client_auth.keys]]
key = "sk-alice-..."
name = "alice"

[[client_auth.keys]]
# sha256 digest instead of the raw secret, so this file can be committed:
#   printf '%s' 'sk-bob-...' | shasum -a 256
key = "sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
name = "bob"

[logging]
level = "info"
format = "json"
EOF

docker run -d --name codex-proxy \
  --restart unless-stopped \
  -p 8787:8787 \
  -p 127.0.0.1:9090:9090 \
  --memory=256m \
  -v codex_data:/data \
  -v "$PWD/config.toml:/config/config.toml:ro" \
  -e CODEXPROXY_CONFIG=/config/config.toml \
  -e CODEXPROXY_DATA_DIR=/data \
  -e CODEXPROXY_METRICS_HOST=0.0.0.0 \
  -e CODEXPROXY_AUTH_JSON="$(cat ~/.codex/auth.json)" \
  ghcr.io/thezillo/codex-proxy:latest
```

`GET /health` is unauthenticated and returns 200 whenever the proxy is up —
wire it to whatever liveness/readiness probe your orchestrator uses.

Why each flag is there:

| Flag | Why |
|---|---|
| `-p 8787:8787` | the API. The image sets `CODEXPROXY_HOST=0.0.0.0` so it's reachable outside the container |
| `-p 127.0.0.1:9090:9090` | metrics, published to loopback only — they're unauthenticated |
| `-e CODEXPROXY_METRICS_HOST=0.0.0.0` | needed *as well*: `metrics_host` defaults to `127.0.0.1` and deliberately does **not** inherit `host`, so without this the port publish reaches nothing |
| `-v codex_data:/data` | `auth.json` + the rotated refresh token. Lose it and you re-`codex login` |
| `-v .../config.toml:ro` + `CODEXPROXY_CONFIG` | the config file. Without the env var the binary looks for `./config.toml` in its working directory; if that's missing it warns once and runs on built-in defaults — i.e. a mount at the wrong path costs you the whole file, not an error |
| `--memory=256m` | Headroom, not the baseline — steady state is the ~3 MiB above. RSS ≈ baseline + the request bodies in flight, each capped at `max_body_bytes` (16 MiB), so the limit covers a burst of large bodies. Don't go lower unless you also lower that cap |
| `--restart unless-stopped` | see the single-instance rule below — restart, never a second instance |

**Run exactly one instance per data directory.** Two processes sharing an
`auth.json` will each refresh the token and invalidate the other's — the
result is intermittent 401s that look like an upstream problem. Scale by
adding accounts to the pool (below), not replicas.

## Configuration reference

Resolution order, lowest priority first:
**built-in defaults → `config.toml` → `CODEXPROXY_*` env vars.**
Anything settable by env is also settable in the file; the reverse isn't true.

| Env var | `config.toml` | Default |
|---|---|---|
| `CODEXPROXY_CONFIG` | — (path *to* the file) | `config.toml` |
| `CODEXPROXY_HOST` | `server.host` | `127.0.0.1` (`0.0.0.0` in the image) |
| `CODEXPROXY_PORT` | `server.port` | `8787` |
| `CODEXPROXY_MAX_BODY_BYTES` | `server.max_body_bytes` | `16777216` (16 MiB) |
| `CODEXPROXY_METRICS_HOST` | `server.metrics_host` | `127.0.0.1` |
| `CODEXPROXY_METRICS_PORT` | `server.metrics_port` | `9090` (`0` disables serving) |
| `CODEXPROXY_API_KEYS` | `client_auth.keys` | `sk-local-changeme` placeholder |
| `CODEXPROXY_DATA_DIR` | `upstream.data_dir` | `~/.codex` |
| `CODEXPROXY_AUTH_JSON` | — (seed, not config) | unset |
| `CODEXPROXY_CLI_VERSION` | `upstream.cli_version` | `0.155.1` |
| `CODEXPROXY_PROXY` | `upstream.proxy` | unset (direct) |
| `CODEXPROXY_LOG` | `logging.level` | `info` |
| `CODEXPROXY_LOG_FORMAT` | `logging.format` | `text` |
| `CODEXPROXY_FALLBACK_{NAME}_API_KEY` | `fallback[].api_key` | — |

`CODEXPROXY_API_KEYS` is comma-separated and replaces the whole list; names
are file-only. `{NAME}` in the fallback var is the provider's `name`,
uppercased with every character outside `[A-Z0-9_]` turned into `_` —
`azure-eastus` → `CODEXPROXY_FALLBACK_AZURE_EASTUS_API_KEY`.

File-only settings, with their defaults:

```toml
[client_auth]
require = true                  # false disables auth — loopback binds only

[upstream]
base_url = "https://chatgpt.com/backend-api"
responses_path = "/codex/responses"
issuer = "https://auth.openai.com"
client_id = "app_EMoamEEZ73f0CkXaXp7hrann"   # the Codex CLI's public OAuth id
originator = "codex_cli_rs"
refresh_skew_secs = 300         # refresh this long before the JWT `exp`
request_timeout_secs = 600
connect_timeout_secs = 30
account_cooldown_secs = 30      # skip a failed pool account this long
usage_path = "/wham/usage"      # ChatGPT usage endpoint the quota poller reads
compact_path = "/codex/responses/compact"  # upstream of /v1/responses/compact
search_path = "/codex/alpha/search"        # upstream of /v1/alpha/search
quota_check_interval_secs = 600 # re-check quota-exhausted accounts; 0 = off
# [upstream.account_names]      # label pool accounts in logs, by dir basename

[defaults]                      # applied when the client omits the field
model = "gpt-6-astra"
reasoning_effort = "medium"     # none | minimal | low | medium | high | xhigh | max
reasoning_summary = "auto"
instructions = "You are a helpful coding assistant."
include_reasoning = false       # emit reasoning as `reasoning_content` deltas

[defaults.model_aliases]        # NOTE: defining this REPLACES the built-in map
"gpt-6" = "gpt-6-astra"         # ...which is exactly these two entries
"gpt-5.6" = "gpt-5.6-sol"
```

Two traps worth repeating, because both fail quietly:

- `metrics_host` does not inherit `host`. Binding the API to `0.0.0.0` leaves
  metrics on loopback — intentional, but it means a `/metrics` scrape from
  another host just hangs until you set it.
- `[defaults.model_aliases]` replaces the built-in map rather than merging
  into it. Keep the `gpt-6` and `gpt-5.6` entries: the upstream only accepts
  the flavored slugs and 400s the bare names (`The 'gpt-6' model is not
  supported when using Codex with a ChatGPT account.`), which would silently
  divert traffic to a paid fallback. The map applies on both POST endpoints —
  including the `/v1/responses` passthrough, which is the one Codex uses.

## Startup guards

The proxy refuses to start — rather than serve something unsafe — when it
would bind a **non-loopback** host with any of:

- `client_auth.require = false` (no auth at all),
- the built-in `sk-local-changeme` key still in the accepted set (it's public;
  it exists so a loopback dev run works with no setup),
- an empty key set (e.g. `CODEXPROXY_API_KEYS=""` wiping the list).

On a loopback bind these are warnings instead. If you see `refusing to start:`
in the logs, it's one of these three — the message names which.

## Endpoints

- `POST /v1/chat/completions` — Chat Completions, translated to/from Codex Responses (stream or buffered).
  `response_format` (`json_object` / `json_schema`) becomes the Responses
  `text.format`, so structured output works through the translation too.
  JSON mode follows OpenAI Chat, not the Responses backend: "json" in any
  message, the system prompt included, is enough, and a request that never
  says it gets OpenAI's own 400 (`'messages' must contain the word 'json'…`).
  The backend only looks at `input`, so when only the system prompt says it,
  those system messages are also sent as `developer` input items (they stay
  `instructions` too, so the configured default never replaces them). A
  `response_format` OpenAI would refuse (unknown `type`, `json_schema`
  without its object or `name`) gets OpenAI's 400 too.
- `POST /v1/responses` — passthrough to the Codex Responses API. Forwarded
  byte-for-byte, with one exception: a `model` that `[defaults.model_aliases]`
  maps is rewritten, since Codex speaks this wire API and a bare `gpt-6` /
  `gpt-5.6` would otherwise 400 upstream and fall through to a paid provider.
  A body that needs no rewrite is never even parsed into a JSON tree.
- `GET /v1/responses` (WebSocket upgrade) — the same endpoint over the
  transport Codex uses when its provider has `supports_websockets`. See
  [Codex over WebSocket](#codex-over-websocket).
- `POST /v1/responses/compact` — OpenAI's stateless history compaction (JSON
  in, JSON out; the returned `compaction` items go into the next
  `/v1/responses` call as is) — and `POST /v1/alpha/search`, Codex's
  standalone web search. Both are forwarded to the account pool
  (`upstream.compact_path` / `search_path`) with the same model alias and
  unknown-model gate as `/v1/responses`. Pool only: no `[[fallback]]`
  provider has these endpoints, so a pool error is relayed as is.
- `POST /v1/embeddings` — direct to the `[embeddings]` provider (the ChatGPT
  pool has no embeddings API); `model` is mapped through its `model_map` on
  the way out and echoed back as requested on the way in. Answers 404 until
  `[embeddings]` is configured. See [Embeddings](#embeddings).
- `GET /v1/models`, `GET /v1/models/{id}`, `GET /health`.

`/health` and the model endpoints need no auth (so they work as container
probes); all `/v1` POST endpoints do. Advertised (and, by default, the only
accepted) models: `gpt-6-astra`, `gpt-6-sol`, `gpt-6-luna`, `gpt-6`,
`gpt-5.6-sol`, `gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-5.6`, `gpt-5.5` — the
listed entries of the live Codex model catalog. Older generations the catalog
no longer lists (e.g. `gpt-5.4`) are refused by the ChatGPT upstream and get
a 400 here; see Cost guardrails.

Function tools are reshaped to the Responses form; hosted tools (`web_search`,
`image_generation`) pass through. Upstream errors are relayed with their
original status and body.

`/v1/responses` also relays Codex CLI's own turn/session headers both ways
(`session-id`, `thread-id`, `x-client-request-id`, and the sticky-routing
`x-codex-turn-state`), so pointing a real `codex` CLI at this proxy doesn't
lose session continuity. `x-codex-turn-state` only gets relayed when there's
exactly one pool account — with multiple accounts it's tied to whichever one
issued it, so it's dropped instead of replayed against the wrong account.

## Codex over WebSocket

The Codex CLI only calls two of the endpoints above when its provider
declares it can serve them. For a custom provider:

```toml
[model_providers.proxy]
name = "codex-proxy"
base_url = "https://proxy.example.com/v1"
wire_api = "responses"
env_key = "CODEX_PROXY_KEY"
supports_websockets = true              # GET /v1/responses (WebSocket)
supports_standalone_web_search = true   # POST /v1/alpha/search
```

The built-in `openai` provider, pointed here with `openai_base_url`, turns
on both flags by itself (and, in CLIs up to 0.14x, also compacts through
`/v1/responses/compact`; newer ones compact through `/v1/responses`).

On the socket, each `response.create` frame is forwarded as an ordinary
`/v1/responses` request. It goes through the same pool, downgrades and fallback
chain, and each upstream SSE event comes back as one text frame. The upstream
side is plain HTTP, as before, so the TLS fingerprint doesn't change. Codex
sends a follow-up turn as `previous_response_id` plus only the new input items.
The proxy keeps the previous request's input and output items for the
connection and rebuilds the full input before forwarding. A
`previous_response_id` from anywhere else gets `previous_response_not_found`,
and Codex answers that by resending the whole request. Warm-up frames
(`generate: false`) are answered locally, without a model call. Requests on
the socket are logged and counted under `endpoint="/v1/responses (ws)"`.

Memory cost: each open socket keeps the text of its last request's input, up
to `max_body_bytes` for a long session. Count one such body per concurrent
Codex session on top of the bodies in flight when you size `--memory`.

## Logging & token usage

Every authenticated request emits two lines under the `access` log target, so
you can see **who** is spending tokens (plus a third on failover, below):

```
request accepted   client=alice ip=1.2.3.4 ua=... method=POST path=/v1/chat/completions
request completed  client=alice account=primary endpoint=/v1/chat/completions model=gpt-6-astra \
                   status=200 prompt_tokens=18 completion_tokens=5 total_tokens=23 duration_ms=1392
```

- `client` is the `name` on the matched `[[client_auth.keys]]` entry, or a
  non-reversible fingerprint (`key-XXXXXXXX`) for unnamed keys — the raw key
  is never logged.
- `account` is which ChatGPT account served the request, `-` if it failed
  before one was picked.
- `ip` comes from `Fly-Client-IP` / `X-Forwarded-For`.
- Both `/v1/chat/completions` and the `/v1/responses` passthrough report
  token usage.
- Prompts and response bodies are never logged, only metadata.

A third `access` line appears only when the ChatGPT pool failed and a
fallback provider served the request instead — it's the one place that says
*why* the pool didn't serve it:

```
pool failed, served by fallback provider  client=alice ip=1.2.3.4 request_id=... \
                   model=gpt-5.6-sol reason=rate_limit account=primary status=429 \
                   fallback_account=openrouter error=
```

- `reason` is a normalized category, so it can be grouped on: `rate_limit`,
  `auth`, `timeout`, `capacity`, `upstream_5xx`, `bad_request`, `transport`,
  `quota_exhausted`, `cooling_down`, `unknown`. `quota_exhausted` is a
  `usage_limit_reached` 429 — either the request that discovered it
  (`status=429`) or one diverted straight to the chain afterwards because
  every account is under such a hold (`status=0`); `cooling_down` is always
  the diverted case (`status=0`). See Quota-aware fallback below.
- `account`/`status` are the LAST pool account tried and the status it
  returned. `status=0` means it never returned one — `transport`/`timeout`,
  but also `auth`, which is the revoked-session case (our own token refresh
  failed, as opposed to a 401/403 coming back from upstream). `error` carries
  the bounded message on those paths.
- `reason` describes the pool's *final* attempt, not a verdict on the whole
  pool: a sweep that hits 429 on one account and a dead token on another
  reports only what the pool ended up returning. Each account's own failure
  gets its own `account failed…` line, so grep those to see the rest.
- `request_id` comes from the client's own `x-client-request-id` (or
  `session-id`), `-` when it sent neither — this proxy mints no id of its own.
- No switch, no line: if the fallback chain has nothing for the request, the
  pool's own response is returned unchanged. Careful — when the pool got no
  response at all, the request fails before any completion line is emitted, so
  it produces no `access` line and no Prometheus sample either; the only trace
  is a `request failed` warn off the `access` target.

Under `format = "json"` the subscriber nests event fields one level down, so
in Loki the labels are `fields_*`:

```logql
{app="codex-proxy"} | json | fields_reason != "" | line_format "{{.fields_client}} {{.fields_reason}} {{.fields_fallback_account}}"
```

Set `CODEXPROXY_LOG_FORMAT=json` for one structured object per line if you
want to aggregate it. The `access` target stays at `info` regardless of the
app log level.

## Metrics

Prometheus metrics are served on a separate port from the API
(`CODEXPROXY_METRICS_PORT`, default `9090`), bound to `127.0.0.1` by default
even if the API itself is public. Set `CODEXPROXY_METRICS_HOST` to expose it
elsewhere (and firewall it — it's unauthenticated). `metrics_port = 0`
disables the metrics server without disabling collection.

- `codexproxy_requests_total{endpoint, client, account, model, status}`
- `codexproxy_tokens_total{client, account, model, kind}` — `kind` is `prompt`,
  `completion`, `cached` (prompt tokens read from the provider's prompt cache,
  a subset of `prompt`) or `cache_write` (written to it; OpenRouter reports
  this and bills it at a premium for GPT-5.6+, so a high `cache_write` with a
  low `cached` means paying extra for a cache that never gets read)
- `codexproxy_request_duration_seconds{endpoint, client, account, model}`
- `codexproxy_failovers_total{client, model, reason, fallback}` — one per
  request a paid fallback served; `reason` is the same closed set as the
  failover log line (`rate_limit`, `quota_exhausted`, `cooling_down`, ...)
- `codexproxy_model_downgrades_total{client, from, to, outcome}` — in-pool
  retries with a lower model, `outcome` is `served` or `failed`
- `codexproxy_rejected_requests_total{client, reason}` — requests refused
  instead of paid for: `unknown_model` or `pool_bad_request`

`model` is clamped to the models this proxy actually serves — anything else
shows up as `other`, so a client sending garbage can't create unbounded
Prometheus series. The access log still shows the real value. On
`/v1/embeddings` the bound is the `[embeddings]` `model_map` instead.

`endpoint="/v1/embeddings"` always carries `account=<provider>`: that route
is *direct* to the configured provider, never a pool failover, and never
emits a failover log line. An alert on "served by fallback" should exclude
it by endpoint (`endpoint!="/v1/embeddings"`), not by provider name — the
provider is the same one a real failover would use. Or alert on
`codexproxy_failovers_total` directly, which only ever counts real failovers.

Scraped through a Prometheus Operator `ServiceMonitor`, the app's `endpoint`
label collides with the target label of the same name (the scrape port) and
is stored as `exported_endpoint` unless the ServiceMonitor sets
`honorLabels: true`. Filter on whichever one your setup actually keeps.

## Multiple ChatGPT accounts

No list to maintain — the pool is auto-discovered from `data_dir`. Drop each
extra account's `auth.json` into its own subdirectory (its own
`codex login --codex-home <subdir>`, or its own mounted secret) and restart;
requests are spread across whatever's found. Useful once one account's rate
limit isn't enough.

Every request is pinned to one "home" account per conversation, so all
turns of a conversation land on the same account while it's healthy and that
account's prompt cache keeps serving the growing history, instead of it being
re-read from scratch on another account every other turn. The conversation
key is, in order of preference:

1. the `session-id` (else `thread-id`) header — the real Codex CLI sends both
   on every turn;
2. `prompt_cache_key` in the request body;
3. a hash of the request's prefix: `instructions`, `tools` and the first
   `input` item. Every turn resends the history and appends to it, so this
   prefix is the same for all turns of a conversation and differs between
   conversations. No similarity matching is involved or needed: prompt
   caching only hits on an exact prefix anyway.

Only requests with none of these (e.g. an empty body) round-robin. If the
home account fails, the conversation moves to the next account in order and
stays there until home is usable again. The hash is fixed (FNV-1a), so
conversations keep their account across restarts; adding or removing an
account does reshuffle them. A Codex history compaction rewrites the prefix,
so a derived key changes once at that point. The access log's `affinity`
field says which of the three the key came from.

On `/v1/chat/completions`, where the proxy builds the upstream body itself,
the key is also sent upstream as `prompt_cache_key` (a client's own value is
kept). `/v1/responses` bodies are forwarded unchanged.

A 401 triggers one forced token refresh and retry on the same account. If
that still fails, or the account gets a 403 or 429, the request fails over to
the next account in the pool. A failing account also cools down for
`upstream.account_cooldown_secs` (default 30s) and gets skipped by
round-robin until then. Check the `account` field in the access log to see
which one served (or failed) a request.

### Quota-aware fallback

A 429 whose body says `usage_limit_reached` is not a throttle — it's the
5-hour or weekly Codex quota, with a reset hours or days away. Such an
account is marked *quota-exhausted* until the reset the 429 reports
(separately from the 30s cooldown) and skipped by round-robin meanwhile.
While any account is in that state the proxy polls the ChatGPT usage
endpoint for it every `upstream.quota_check_interval_secs` (default 600) and
puts it back in rotation as soon as the report shows headroom again — so a
reset that lands early (manual reset, plan change) is picked up without
waiting for the originally reported time. Both the 5h and the weekly window
count; healthy accounts are never polled; `0` disables polling (the state
then clears only when the reported reset passes). A poll that fails leaves
the state as it is: the account comes back when its 429 said it would.

When every account is quota-exhausted (or cooling down) and a `[[fallback]]`
chain is configured, requests go straight to the chain without an upstream
round-trip — the failover line then says `reason=quota_exhausted` (or
`cooling_down`) with `status=0`; the request that discovered the exhaustion
logs `reason=quota_exhausted status=429`. Without a chain the pool still tries an
account, as before: a shaky account beats refusing the request. If the chain
declines a request (no `model_map` entry for that model), the pool is tried
anyway.

### Sessions surviving a switch

Codex resends its whole history every turn, including `reasoning` (and,
after a compaction, `compaction`) items whose `encrypted_content` only the
upstream that minted them can decrypt. After a failover — account 1 to
account 2, the pool to a `[[fallback]]` provider, or back once the quota
resets — the new upstream rejects that replay with `400
invalid_encrypted_content`, which used to end the session.

The proxy now retries such a request once, on the same account or provider,
without the input items that carry `encrypted_content`; messages and tool
calls/outputs are kept, so the session goes on. Requests are never modified
up front — only after that specific rejection — and each recovery logs a
`could not decrypt replayed state` warning with the number of items dropped.
The cost: hidden reasoning from earlier turns is gone for the new upstream
(it couldn't read it anyway), and a dropped `compaction` item takes the
history it summarized with it. Codex keeps the foreign items in its own
history, so until the session is restarted every later turn pays one rejected
round-trip (no tokens billed) and logs the warning again — expected, not a
new failure.

## Cost guardrails (`[models]`)

The paid fallback is only for the pool being *down*. Three things keep it
from quietly serving traffic the pool refused for other reasons:

- **Unknown models are refused.** With `reject_unknown = true` (default), a
  model that isn't in `/v1/models` or `[models] extra` gets a 400
  `model_not_found` listing what's available — before the pool or any paid
  provider sees it. Checked after `[defaults.model_aliases]`, so `gpt-6`
  still works as `gpt-6-astra`.
- **The pool's own 4xx is relayed.** A 400/404/422 from the pool (e.g. `The
  'gpt-5.4' model is not supported when using Codex with a ChatGPT account`)
  is the same for every account, so it goes back to the client instead of to
  the paid chain. `fallback_on_bad_request = true` restores the old behavior.
- **Throttled models downgrade inside the pool first.** On a plain 429 (or a
  pool skipped because it's cooling down after one), the request is retried
  on the pool with the model from `[models.downgrades]` (default
  `gpt-6-astra -> gpt-5.6-sol`), following the chain. Only if that fails too
  does the paid chain run — with the client's original model. A
  `usage_limit_reached` quota 429 skips the downgrade: it's account-wide, so
  every model would fail the same way.

Each of the three is counted (see Metrics) so an alert can tell a quota
outage from a client sending a dead model id.

## Fallback providers

None configured by default. `[[fallback]]` in `config.toml` adds secondary
Responses-API providers (Azure OpenAI, OpenRouter) tried after the whole
ChatGPT pool has failed. Any pool failure except a request-level 4xx (see
Cost guardrails), and any fallback failure, moves on to the next option; if everything fails, the client sees
the last provider's real error.

Each provider needs a `model_map`, since the model id has to become whatever
that provider expects — an Azure deployment name, or OpenRouter's namespaced
id (`openai/gpt-4.1`). A model missing from the map skips that provider
rather than guessing. Key it by the name sent upstream — i.e. after
`[defaults.model_aliases]` resolution, on both endpoints. See the commented
example in `config.toml`.

A provider must be declared in `config.toml` — there's no env var that
creates one from nothing. `CODEXPROXY_FALLBACK_{NAME}_API_KEY` only overrides
the key of a provider already declared there. The proxy refuses to start if a
declared provider ends up with an empty key.

Fallback requests never carry Codex/ChatGPT-specific headers, and reuse the
`upstream.proxy` setting if one's configured.

`sticky_session = true` on a provider adds the request's conversation key
(see Multiple ChatGPT accounts) to the body as `session_id` and
`prompt_cache_key`, unless the client already set them. OpenRouter keeps
provider stickiness on exactly these fields, so every turn of a conversation
reaches the provider that already holds its prompt cache. Enable it for
OpenRouter; leave it off for Azure OpenAI, which may reject the unknown
`session_id` field.

Every request that actually switches to a fallback provider logs why the
pool refused it (`reason=`, see Logging above) — otherwise "served by
fallback" is all you'd ever see.

Fallback only fires once the *whole* pool is down, so a misconfigured
provider stays invisible until an outage. Confirm it loaded at startup:

```sh
docker logs codex-proxy 2>&1 | grep -i 'fallback'
```

## Embeddings

The ChatGPT subscription backend has no embeddings API, so
`POST /v1/embeddings` never touches the account pool. `[embeddings]` in
`config.toml` points it at ONE already-declared `[[fallback]]` provider by
name and reuses that provider's `base_url`, `auth_style` and `api_key`
(including the `CODEXPROXY_FALLBACK_{NAME}_API_KEY` override), so there is
no second secret to manage:

```toml
[embeddings]
provider = "openrouter"          # must be a [[fallback]] name
# path = "/embeddings"           # appended to the provider's base_url
[embeddings.model_map]
"text-embedding-3-small" = "openai/text-embedding-3-small"
"text-embedding-3-large" = "openai/text-embedding-3-large"
```

Only mapped models are accepted; anything else is a 400 listing what is.
A provider non-2xx is relayed unchanged. The response's `model` is rewritten
back to the id the client sent. Usage is logged as prompt tokens (embeddings
have no completion side). The proxy refuses to start if `provider` names no
declared fallback entry or `model_map` is empty, and logs
`embeddings configured: direct to provider` once it has loaded.

This is not a failover and is not logged or alerted as one — see
[Metrics](#metrics).

## Run from source

```sh
codex login
cargo run --release   # reads ./config.toml; override with CODEXPROXY_CONFIG
```

Rust 1.95+. Binary at `target/release/codex-proxy`. The repo's `config.toml`
is a commented reference listing every setting at its default — safe to run
as-is on loopback, and the place to look when this README is too short.

## License

Apache-2.0. Portions adapted from openai/codex (see [NOTICE](./NOTICE)).
