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
  -d '{"model":"gpt-5.6-sol","stream":true,
       "messages":[{"role":"user","content":"hi"}]}'
```

Omit `model` and you get `defaults.model` (`gpt-5.6-sol`). Check it's alive
with `curl localhost:8787/health` — that endpoint needs no auth.

`CODEXPROXY_AUTH_JSON` seeds `auth.json` only when the data directory is
empty; after that the rotated token on the volume wins, so the env var is
harmless on later restarts but also can't be used to *replace* credentials.

Image tags: `latest`, `v0.2.4`, `sha-<commit>` (GHCR, built on push to `main`
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
| `CODEXPROXY_CLI_VERSION` | `upstream.cli_version` | `0.144.3` |
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
# [upstream.account_names]      # label pool accounts in logs, by dir basename

[defaults]                      # applied when the client omits the field
model = "gpt-5.6-sol"
reasoning_effort = "medium"     # low | medium | high | xhigh
reasoning_summary = "auto"
instructions = "You are a helpful coding assistant."
include_reasoning = false       # emit reasoning as `reasoning_content` deltas

[defaults.model_aliases]        # NOTE: defining this REPLACES the built-in map
"gpt-5.6" = "gpt-5.6-sol"       # ...which is exactly this one entry
```

Two traps worth repeating, because both fail quietly:

- `metrics_host` does not inherit `host`. Binding the API to `0.0.0.0` leaves
  metrics on loopback — intentional, but it means a `/metrics` scrape from
  another host just hangs until you set it.
- `[defaults.model_aliases]` replaces the built-in map rather than merging
  into it. Keep the `gpt-5.6` entry: the upstream only accepts the flavored
  slugs and 400s a bare `gpt-5.6`, which would silently divert traffic to a
  paid fallback.

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
- `POST /v1/responses` — raw passthrough to the Codex Responses API.
- `GET /v1/models`, `GET /v1/models/{id}`, `GET /health`.

`/health` and the model endpoints need no auth (so they work as container
probes); both `/v1` POST endpoints do. Advertised models: `gpt-5.6-sol`,
`gpt-5.6-terra`, `gpt-5.6-luna`, `gpt-5.6`, `gpt-5.5`.

Function tools are reshaped to the Responses form; hosted tools (`web_search`,
`image_generation`) pass through. Upstream errors are relayed with their
original status and body.

`/v1/responses` also relays Codex CLI's own turn/session headers both ways
(`session-id`, `thread-id`, `x-client-request-id`, and the sticky-routing
`x-codex-turn-state`), so pointing a real `codex` CLI at this proxy doesn't
lose session continuity. `x-codex-turn-state` only gets relayed when there's
exactly one pool account — with multiple accounts it's tied to whichever one
issued it, so it's dropped instead of replayed against the wrong account.

## Logging & token usage

Every authenticated request emits two lines under the `access` log target, so
you can see **who** is spending tokens (plus a third on failover, below):

```
request accepted   client=alice ip=1.2.3.4 ua=... method=POST path=/v1/chat/completions
request completed  client=alice account=primary endpoint=/v1/chat/completions model=gpt-5.5 \
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
  `unknown`.
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
- `codexproxy_tokens_total{client, account, model, kind}` — `kind` is `prompt` or `completion`
- `codexproxy_request_duration_seconds{endpoint, client, account, model}`

`model` is clamped to the models this proxy actually serves — anything else
shows up as `other`, so a client sending garbage can't create unbounded
Prometheus series. The access log still shows the real value.

## Multiple ChatGPT accounts

No list to maintain — the pool is auto-discovered from `data_dir`. Drop each
extra account's `auth.json` into its own subdirectory (its own
`codex login --codex-home <subdir>`, or its own mounted secret) and restart;
requests round-robin across whatever's found. Useful once one account's rate
limit isn't enough.

A 401 triggers one forced token refresh and retry on the same account. If
that still fails, or the account gets a 403 or 429, the request fails over to
the next account in the pool. A failing account also cools down for
`upstream.account_cooldown_secs` (default 30s) and gets skipped by
round-robin until then. Check the `account` field in the access log to see
which one served (or failed) a request.

## Fallback providers

None configured by default. `[[fallback]]` in `config.toml` adds secondary
Responses-API providers (Azure OpenAI, OpenRouter) tried after the whole
ChatGPT pool has failed. Any failure anywhere in the chain — pool or
fallback — moves on to the next option; if everything fails, the client sees
the last provider's real error.

Each provider needs a `model_map`, since the model id has to become whatever
that provider expects — an Azure deployment name, or OpenRouter's namespaced
id (`openai/gpt-4.1`). A model missing from the map skips that provider
rather than guessing. See the commented example in `config.toml`.

A provider must be declared in `config.toml` — there's no env var that
creates one from nothing. `CODEXPROXY_FALLBACK_{NAME}_API_KEY` only overrides
the key of a provider already declared there. The proxy refuses to start if a
declared provider ends up with an empty key.

Fallback requests never carry Codex/ChatGPT-specific headers, and reuse the
`upstream.proxy` setting if one's configured.

Every request that actually switches to a fallback provider logs why the
pool refused it (`reason=`, see Logging above) — otherwise "served by
fallback" is all you'd ever see.

Fallback only fires once the *whole* pool is down, so a misconfigured
provider stays invisible until an outage. Confirm it loaded at startup:

```sh
docker logs codex-proxy 2>&1 | grep -i 'fallback'
```

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
