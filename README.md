# codex-proxy

A minimal reverse proxy that exposes the **Codex (ChatGPT subscription) Responses
API** over a local OpenAI-compatible endpoint, guarded by client API keys.

No TLS fingerprinting, no Electron, no database — a single Rust binary. It uses
the same credentials and request format as the official
[openai/codex](https://github.com/openai/codex) CLI (Apache-2.0), so it presents
itself as a legitimate Codex client.

## How it works

```
client ──Bearer <your-key>──▶ codex-proxy ──Bearer <chatgpt-token>──▶ chatgpt.com/backend-api/codex/responses
                                     │
                                     └─ reads ~/.codex/auth.json, auto-refreshes the token before it expires
```

1. You authenticate to the proxy with a key from `config.toml`.
2. The proxy reads your Codex credentials from `~/.codex/auth.json`
   (produced by `codex login`), refreshing the access token automatically.
3. It forwards the request to the Codex Responses API with the proper
   `Authorization`, `ChatGPT-Account-ID`, `originator`, and `User-Agent` headers,
   and streams the response back (translating to/from the OpenAI Chat
   Completions format).

## Requirements

- Rust 1.95+ (2021 edition)
- The official [`codex`](https://github.com/openai/codex) CLI, signed in once
  (it writes `~/.codex/auth.json`, which this proxy reads and refreshes)

## Quick start

```sh
# 1. Authenticate the official codex CLI once (creates ~/.codex/auth.json)
codex login

# 2. Set your own client key
$EDITOR config.toml          # change client_auth.keys

# 3. Build and run (config.toml is read from the working directory;
#    override the path with CODEXPROXY_CONFIG=/path/to/config.toml)
cargo run --release
```

The proxy listens on `http://127.0.0.1:8787` by default. The binary is
`codex-proxy`; after `cargo build --release` it lives at
`target/release/codex-proxy`.

### Try it

```sh
# OpenAI-compatible chat/completions (works with most OpenAI clients)
curl http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-changeme" \
  -H "Content-Type: application/json" \
  -d '{
        "model": "gpt-5-codex",
        "messages": [{ "role": "user", "content": "Write a haiku about borrow checking." }],
        "stream": true
      }'
```

## Endpoints

| Method | Path                    | Purpose                                                     |
|--------|-------------------------|-------------------------------------------------------------|
| POST   | `/v1/chat/completions`  | OpenAI Chat Completions → Codex Responses (stream + buffered)|
| POST   | `/v1/responses`         | Raw passthrough to the Codex Responses API (streamed)       |
| GET    | `/v1/models`            | Static model list                                           |
| GET    | `/health`               | Liveness check                                              |

`/v1/chat/completions` translates the request to the Responses wire format and
the SSE response back to chat chunks — supporting text, tool/function calls,
usage, and (optionally) reasoning. Point any OpenAI-compatible client at it.

Upstream errors (e.g. `401`, `429`, `400` from Codex) are relayed with their
original status code and body, not flattened — so client retry/auth logic keeps
working.

## Tools

Tool definitions are forwarded transparently:

- `type: "function"` tools are reshaped into the flat Responses form the backend
  expects; the model emits `tool_calls` your client executes as usual.
- **Hosted tools** the Codex backend runs itself — `web_search`,
  `image_generation`, and any future ones — are passed through **untouched**. To
  enable web search, just send `{ "type": "web_search" }` in the request's
  `tools` array; the backend performs the search and folds results into the
  answer (no extra setup here).

## Configuration

All settings live in [`config.toml`](./config.toml) with coding-friendly
defaults. Highlights: `[server] max_body_bytes` for large contexts/images,
`[defaults] model`, `reasoning_effort`, `instructions`, `include_reasoning`,
and `[defaults.model_aliases]` for mapping client model names (e.g. `gpt-4o`)
onto upstream ids.

Selected env overrides: `CODEXPROXY_CONFIG`, `CODEXPROXY_PORT`,
`CODEXPROXY_MAX_BODY_BYTES`, `CODEXPROXY_API_KEYS` (comma-separated),
`CODEXPROXY_CODEX_HOME`, `CODEXPROXY_PROXY`, `CODEXPROXY_LOG`.

### Outbound proxy

To route all upstream traffic (request forwarding **and** token refresh) through
a proxy — e.g. when OpenAI blocks your deploy region/IP — set `[upstream] proxy`
or `CODEXPROXY_PROXY`:

```toml
[upstream]
proxy = "socks5://user:pass@host:1080"   # or http://… / https://…
```

## Deploy (Fly.io)

A `Dockerfile` and `fly.toml` are included. Credentials are seeded from a secret
and then persisted (with rotated tokens) on a volume, so the single-use refresh
token survives restarts.

```sh
fly auth login

# Edit `app` in fly.toml to a globally-unique name, then:
fly launch --no-deploy --copy-config --name <your-app>

# Persistent volume for ~/.codex (region must match primary_region in fly.toml)
fly volumes create codex_data --size 1 --region iad

# Secrets: your client key, your Codex credentials, optional egress proxy
fly secrets set CODEXPROXY_API_KEYS="$(openssl rand -hex 24)"
fly secrets set CODEXPROXY_AUTH_JSON="$(cat ~/.codex/auth.json)"
fly secrets set CODEXPROXY_PROXY="socks5://user:pass@host:1080"   # optional

fly deploy
```

Point a client at `https://<your-app>.fly.dev/v1` with
`Authorization: Bearer <the CODEXPROXY_API_KEYS value>`.

> The proxy uses your ChatGPT subscription tokens — keep the app **private**,
> always set `CODEXPROXY_API_KEYS`, and consider `CODEXPROXY_PROXY` if Fly's
> egress region is blocked by OpenAI.

## License

Apache-2.0. Portions adapted from openai/codex — see [`NOTICE`](./NOTICE).
