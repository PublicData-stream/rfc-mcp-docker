# 0001 — MCP 2026-07-28 over Streamable HTTP

Use the official `rmcp` SDK with an exact dependency pin and an explicit one-version server configuration. Configure JSON responses, per-request metadata, and a session manager that never creates sessions. Allow only the loopback probe listener in Phase 0. A request boundary rejects legacy initialization before the SDK service. Use actual discovery, list and direct call requests to qualify each future SDK pin.

The final public endpoint will be anonymous and read-only, with Host and Origin validation, TLS at the public proxy, and admission limits.

[Protocol specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
