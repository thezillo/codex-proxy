# codex-proxy

Use your **Codex (ChatGPT subscription)** from any OpenAI-compatible client. A
tiny Rust reverse proxy that turns the Codex Responses API into a standard
`/v1/chat/completions` (and `/v1/responses`) endpoint, guarded by your own key.

- **Single container / static binary** — no Electron, no database.
- **Streaming (SSE)**, tool-calls, and hosted tools (`web_search`, `image_generation`).
- **Looks like a real Codex client**: same request format, and a TLS fingerprint
  pinned to match the official Codex client (reqwest + rustls 0.23.36).
- Optional outbound **SOCKS5/HTTP proxy** for egress.

## Quick start (Docker)

**Prerequisite —** sign in to the official
[`codex`](https://github.com/openai/codex) CLI once. It writes
`~/.codex/auth.json`, which this proxy reads and auto-refreshes.

```sh
codex login   # once, with the official CLI

docker run -d --name codex-proxy -p 8787:8787 \
  -v codex_data:/data \
  -e CODEXPROXY_CODEX_HOME=/data \
  -e CODEXPROXY_API_KEYS="$(openssl rand -hex 24)" \
  -e CODEXPROXY_AUTH_JSON="$(cat ~/.codex/auth.json)" \
  ghcr.io/thezillo/codex-proxy:latest
```

That's it. Point any OpenAI client at `http://localhost:8787/v1` and use the
`CODEXPROXY_API_KEYS` value as the Bearer token:

```sh
curl http://localhost:8787/v1/chat/completions \
  -H "Authorization: Bearer <your CODEXPROXY_API_KEYS value>" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-5-codex","stream":true,
       "messages":[{"role":"user","content":"Write a haiku about borrow checking."}]}'
```

Image tags: `:latest`, `:v0.1.0` (releases), `:sha-<commit>`. Published to
GHCR on every push to `main` and every `v*` tag.

**docker compose:**

```yaml
services:
  codex-proxy:
    image: ghcr.io/thezillo/codex-proxy:latest
    ports: ["8787:8787"]
    volumes: ["codex_data:/data"]
    environment:
      CODEXPROXY_CODEX_HOME: /data
      CODEXPROXY_API_KEYS: "replace-with-a-strong-key"
      CODEXPROXY_AUTH_JSON: ${CODEXPROXY_AUTH_JSON}   # contents of ~/.codex/auth.json
    restart: unless-stopped
volumes:
  codex_data:
```

> **Keep it private** and always set `CODEXPROXY_API_KEYS` — the proxy spends
> your ChatGPT subscription. It refuses to bind a public address with the
> built-in default key.

Good to know:
- `CODEXPROXY_AUTH_JSON` **seeds** `auth.json` only on first boot (while the
  `/data` volume is empty). Afterwards the rotated on-disk token wins, so the
  single-use refresh token survives restarts — keep the volume.
- Add `-e CODEXPROXY_PROXY="socks5://user:pass@host:1080"` to send all upstream
  traffic through an egress proxy (e.g. if OpenAI blocks your region/IP).

## How it works

```
client ──Bearer <your-key>──▶ codex-proxy ──Bearer <chatgpt-token>──▶ chatgpt.com/backend-api/codex/responses
```

Your client authenticates to the proxy with your key. The proxy swaps in the
ChatGPT subscription token from `auth.json` (refreshing it before it expires),
adds the headers the official client sends, and streams the response back —
translating to/from the OpenAI Chat Completions format.

## Endpoints

| Method | Path                   | Purpose                                                      |
|--------|------------------------|-------------------------------------------------------------|
| POST   | `/v1/chat/completions` | OpenAI Chat Completions → Codex Responses (stream + buffered)|
| POST   | `/v1/responses`        | Raw passthrough to the Codex Responses API (streamed)       |
| GET    | `/v1/models`           | Static model list                                           |
| GET    | `/health`              | Liveness check                                              |

Upstream errors (`401`, `429`, `400` …) are relayed with their original status
and body, so client retry/auth logic keeps working.

## Configuration

Docker is configured entirely by the env vars above. The full set (env overrides
take precedence over `config.toml`):

| Env var | Purpose |
|---------|---------|
| `CODEXPROXY_API_KEYS` | comma-separated client keys (**required** when exposed) |
| `CODEXPROXY_AUTH_JSON` | seed `auth.json` on first boot |
| `CODEXPROXY_CODEX_HOME` | dir holding `auth.json` (e.g. `/data`) |
| `CODEXPROXY_PROXY` | outbound `socks5://` / `http://` / `https://` proxy |
| `CODEXPROXY_HOST` / `CODEXPROXY_PORT` | bind address (default `0.0.0.0:8787` in the image) |
| `CODEXPROXY_MAX_BODY_BYTES` | max request body, bytes (default 16 MiB) |
| `CODEXPROXY_CONFIG` / `CODEXPROXY_LOG` | config path / log level |

For source runs or fine-tuning, everything lives in
[`config.toml`](./config.toml). Minimal example:

```toml
[server]
host = "0.0.0.0"
port = 8787

[client_auth]
keys = ["replace-with-a-strong-key"]

[defaults]
model = "gpt-5-codex"   # default when the client doesn't pick a model
```

Other `[defaults]` knobs: `reasoning_effort`, `instructions`,
`include_reasoning`, and `[defaults.model_aliases]` to map client model names
(e.g. `gpt-4o`) onto upstream ids.

## Tools

Tool definitions pass through transparently:

- `type: "function"` tools are reshaped into the flat Responses form; the model
  emits `tool_calls` your client executes as usual.
- **Hosted tools** the backend runs itself (`web_search`, `image_generation`, …)
  go through untouched — send `{ "type": "web_search" }` in `tools` and the
  backend does the search and folds results into the answer.

## Run from source

```sh
codex login                 # writes ~/.codex/auth.json
$EDITOR config.toml         # set client_auth.keys
cargo run --release         # reads ./config.toml (override: CODEXPROXY_CONFIG)
```

Requires Rust 1.95+. The binary is `target/release/codex-proxy`; it listens on
`http://127.0.0.1:8787` by default.

## License

Apache-2.0. Portions adapted from openai/codex — see [`NOTICE`](./NOTICE).
