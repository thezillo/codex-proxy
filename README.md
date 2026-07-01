# codex-proxy

OpenAI-compatible proxy for the Codex (ChatGPT subscription) Responses API.
Point any OpenAI client at it; it forwards requests to `chatgpt.com` using the
credentials from `codex login`, refreshing the token as needed. Rust, single
binary, no database. Request format and TLS fingerprint match the official
Codex client.

## Run with Docker

Needs `~/.codex/auth.json` from `codex login` (the official CLI), once.

```sh
docker run -d --name codex-proxy -p 8787:8787 \
  -v codex_data:/data \
  -e CODEXPROXY_DATA_DIR=/data \
  -e CODEXPROXY_API_KEYS="$(openssl rand -hex 24)" \
  -e CODEXPROXY_AUTH_JSON="$(cat ~/.codex/auth.json)" \
  ghcr.io/thezillo/codex-proxy:latest
```

Then send requests to `http://localhost:8787/v1` with the `CODEXPROXY_API_KEYS`
value as the bearer token:

```sh
curl http://localhost:8787/v1/chat/completions \
  -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"model":"gpt-5-codex","stream":true,
       "messages":[{"role":"user","content":"hi"}]}'
```

`CODEXPROXY_AUTH_JSON` seeds `auth.json` only when the volume is empty; after
that the rotated token on the volume is used, so keep the volume.

Image tags: `latest`, `v0.1.0`, `sha-<commit>` (GHCR, built on push to `main`
and on `v*` tags).

## Config

Env vars (override `config.toml`):

- `CODEXPROXY_API_KEYS` — comma-separated client keys; required unless bound to loopback.
- `CODEXPROXY_AUTH_JSON` — seed `auth.json` on first boot (primary account only).
- `CODEXPROXY_DATA_DIR` — directory holding `auth.json` (e.g. `/data`); extra
  ChatGPT accounts are auto-discovered as subdirectories (see Multiple ChatGPT
  accounts below).
- `CODEXPROXY_PROXY` — outbound proxy, `socks5://` / `http://` / `https://`.
- `CODEXPROXY_HOST`, `CODEXPROXY_PORT` — bind address (image defaults to `0.0.0.0:8787`).
- `CODEXPROXY_MAX_BODY_BYTES` — request body cap in bytes (default 16 MiB).
- `CODEXPROXY_METRICS_HOST`, `CODEXPROXY_METRICS_PORT` — metrics listener (see Metrics).
- `CODEXPROXY_CLI_VERSION` — Codex CLI version impersonated in the upstream User-Agent (OS/arch auto-detected).
- `CODEXPROXY_LOG`, `CODEXPROXY_LOG_FORMAT` — log level and `text`/`json` output (see Logging).

Or in `config.toml`:

```toml
[[client_auth.keys]]
key = "replace-me"
name = "alice" # optional; shown in access logs instead of a fingerprint

[defaults]
model = "gpt-5-codex"
```

## Endpoints

- `POST /v1/chat/completions` — Chat Completions, translated to/from Codex Responses (stream or buffered).
- `POST /v1/responses` — raw passthrough to the Codex Responses API.
- `GET /v1/models`, `GET /health`.

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
you can see **who** is spending tokens:

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

## Run from source

```sh
codex login
cargo run --release   # reads ./config.toml; override with CODEXPROXY_CONFIG
```

Rust 1.95+. Binary at `target/release/codex-proxy`.

## License

Apache-2.0. Portions adapted from openai/codex (see [NOTICE](./NOTICE)).
