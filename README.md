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
  -e CODEXPROXY_CODEX_HOME=/data \
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
- `CODEXPROXY_AUTH_JSON` — seed `auth.json` on first boot.
- `CODEXPROXY_CODEX_HOME` — directory holding `auth.json` (e.g. `/data`).
- `CODEXPROXY_PROXY` — outbound proxy, `socks5://` / `http://` / `https://`.
- `CODEXPROXY_HOST`, `CODEXPROXY_PORT` — bind address (image defaults to `0.0.0.0:8787`).
- `CODEXPROXY_MAX_BODY_BYTES` — max request body, bytes (default 16 MiB).
- `CODEXPROXY_CLI_VERSION` — Codex CLI version impersonated in the upstream User-Agent (OS/arch auto-detected).
- `CODEXPROXY_LOG`, `CODEXPROXY_LOG_FORMAT` — log level and `text`/`json` output (see Logging).

Or in `config.toml`:

```toml
[client_auth]
keys = ["replace-me"]

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

## Logging & token usage

Every authenticated request emits two lines under the `access` log target, so
you can see **who** is spending tokens — useful when a shared subscription is
being drained by an unknown caller:

```
request accepted   client=alice ip=1.2.3.4 ua=... method=POST path=/v1/chat/completions
request completed  client=alice endpoint=/v1/chat/completions model=gpt-5.5 \
                   status=200 prompt_tokens=18 completion_tokens=5 total_tokens=23 duration_ms=1392
```

- `client` is the friendly name from `[client_auth.key_names]`, or a
  non-reversible fingerprint (`key-XXXXXXXX`) for unnamed keys — the raw key is
  never logged.
- `ip` is taken from `Fly-Client-IP` / `X-Forwarded-For` (the real caller behind
  a proxy/edge).
- Token counts cover **both** `/v1/chat/completions` and the `/v1/responses`
  passthrough (the path the real Codex CLI uses).
- Request/response bodies (your prompts) are **never** logged — only metadata.

Set `CODEXPROXY_LOG_FORMAT=json` (or `[logging] format = "json"`) for one
structured object per line, then aggregate — e.g. sum `total_tokens` grouped by
`client`. The `access` target stays at `info` even if you lower the app level.

## Run from source

```sh
codex login
cargo run --release   # reads ./config.toml; override with CODEXPROXY_CONFIG
```

Rust 1.95+. Binary at `target/release/codex-proxy`.

## License

Apache-2.0. Portions adapted from openai/codex (see [NOTICE](./NOTICE)).
