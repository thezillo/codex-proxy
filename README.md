# codex-proxy-mini

A minimal reverse proxy that exposes the **Codex (ChatGPT subscription) Responses
API** over a local OpenAI-compatible endpoint, guarded by client API keys.

No TLS fingerprinting, no Electron, no database — a single Rust binary. It uses
the same credentials and request format as the official
[openai/codex](https://github.com/openai/codex) CLI (Apache-2.0), so it presents
itself as a legitimate Codex client.

## How it works

```
client ──Bearer <your-key>──▶ codex-proxy-mini ──Bearer <chatgpt-token>──▶ chatgpt.com/backend-api/codex/responses
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

## Quick start

```sh
# 1. Authenticate the official codex CLI once (creates ~/.codex/auth.json)
codex login

# 2. Set your own client key
$EDITOR config.toml          # change client_auth.keys

# 3. Run
cargo run --release
```

The proxy listens on `http://127.0.0.1:8787` by default.

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

## Configuration

All settings live in [`config.toml`](./config.toml) with coding-friendly
defaults. Highlights under `[defaults]`: `model`, `reasoning_effort`,
`instructions`, `include_reasoning`, and `[defaults.model_aliases]` for mapping
client model names (e.g. `gpt-4o`) onto upstream ids.

Selected env overrides: `CODEXPROXY_CONFIG`, `CODEXPROXY_PORT`,
`CODEXPROXY_API_KEYS` (comma-separated), `CODEXPROXY_CODEX_HOME`,
`CODEXPROXY_LOG`.

## License

Apache-2.0. Portions adapted from openai/codex — see [`NOTICE`](./NOTICE).
