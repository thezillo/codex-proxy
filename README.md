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

## Run from source

```sh
codex login
cargo run --release   # reads ./config.toml; override with CODEXPROXY_CONFIG
```

Rust 1.95+. Binary at `target/release/codex-proxy`.

## License

Apache-2.0. Portions adapted from openai/codex (see [NOTICE](./NOTICE)).
