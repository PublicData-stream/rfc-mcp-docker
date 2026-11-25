# RFC MCP API

Send one JSON-RPC message per request to `POST /mcp`. The endpoint uses anonymous, read-only Streamable HTTP at revision `2026-07-28`. Use `server/discover`, `tools/list`, and `tools/call` on independent requests. Responses are JSON. GET and DELETE return HTTP 405. [Streamable HTTP specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)

```http
POST /mcp HTTP/1.1
Host: rfc.example
Content-Type: application/json
Accept: application/json, text/event-stream
MCP-Protocol-Version: 2026-07-28
Mcp-Method: tools/call
Mcp-Name: get

{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get","arguments":{"id":2119},"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"example","version":"1"}}}}
```

## Tools

| Tool | Arguments | Behavior |
| --- | --- | --- |
| `list` | `limit`, `cursor`, `sort` | Lists published members with original TXT. Default order is `id_asc`. Use `id_desc` for descending order. Coverage reports the catalog-only count when known. |
| `search` | `query`, `limit`, `cursor`, `include_snippets` | Uses the [RFC search DSL](search.md). Optional snippets carry original match offsets and complete notice bundles. Coverage describes incomplete or budgeted evaluation. |
| `get` | `id`, `section_id`, `filter_section_text`, `range`, `max_bytes`, `cursor`, `format` | Retrieves whole TXT by default. Supply at most one selector. Heading text matches the parser's heading exactly, including case. Duplicate headings return candidate IDs. Section slices include descendants. |
| `table` | `id`, `max_depth`, `limit`, `cursor` | Lists indexed sections in original order. Includes section IDs, depth, parent, parser coverage, and original bounds. Exact heading bytes and the parsed heading label appear when the source policy and complete notices permit excerpts. |
| `rg` | `pattern`, `mode`, `case_sensitive`, `ids`, `context_lines`, `limit`, `cursor` | Finds occurrences in approved original TXT, in RFC and byte order. Each result includes its original range and references its notice bundle. |

Arguments are JSON objects. The [exported schemas](../apps/schemas/tools.json) describe input fields and structured success and error output. A tool error uses `isError: true` with a stable code, message, and relevant recovery details. Protocol errors use the JSON-RPC contract for the pinned revision. Successful outputs report the generation and publication sequence as decimal strings. The MCP text block contains serialized JSON matching `structuredContent`.

`list`, `search`, and `table` default to 20 entries and accept 1–100. Send the same normalized arguments when resuming those tools. For `get`, send the same `id` and the returned cursor. Repeated optional arguments must agree with the cursor. Cursors expire within 30 minutes and refer to the original generation. Start a fresh request after `cursor_expired` or `generation_unavailable`.

`rg` defaults to a case-sensitive single-line regex, zero context lines, and 20 matches. Use `mode: "literal"` for a fixed string. Patterns accept up to 2,048 UTF-8 bytes, `context_lines` accepts 0–5, `limit` accepts 1–100, and `ids` accepts up to 100 positive RFC numbers. Continue with the same normalized arguments and the returned cursor. A resumed document is evaluated from byte zero. Each occurrence has a zero-based half-open `match_range`. Context may be shortened to fit the response. An excerpt uses base64 when its bytes cannot be represented as UTF-8.

`scan_complete` means the selected scope was fully covered through the current page. An empty partial page leaves the global match count unknown. `omitted` reports documents excluded by policy, unavailable source bytes, or per-original size limits. `source_bytes_charged` includes each original handed to the worker, including an interrupted scan. Follow a returned `next_cursor` after a budget stop. Narrow the scope or pattern if no progress is possible. A failed sandbox check yields `sandbox_unavailable`.

## Original bytes and notices

Source records can include `source_verification` with the proof `method`
recorded for that generation. `rsync_checksum_match` can also include its
negotiated `algorithm`. `dual_source_byte_match` records an exact comparison
of two independently captured originals. Historic records report their
recorded `dual_source`, `legacy_https_verified`, or `rsync_integrity`
method. Each record retains its original `source_url`.

`range` uses zero-based `start_byte` and exclusive `end_byte`. When text is returned, JSON decoding followed by UTF-8 encoding reproduces the original range, including CRLFs and form feeds. Use `format: "base64"` for exact byte slices that split a UTF-8 character. Originals with other encodings default to base64. An explicit text request for those originals returns `unsupported_source_encoding`.

Each `get` page contains up to 64 KiB of original bytes. The complete serialized JSON-RPC response has a 256 KiB limit, including both representations and all notices. A page can contain fewer bytes to fit. Continue until `next_cursor` is null. Compare the reassembled bytes with the returned full BLAKE3 digest.

RFC 2119 and RFC 8174 have single-response, whole-file approvals. Approved excerpts include complete original notices outside the body. A separately reviewed `whole_paginated` decision allows only sequential whole-file retrieval and includes the approved notices on every page. Add such decisions to the sync registry with the RFC ID, exact TXT BLAKE3 digest, review reference, and an ordered, non-overlapping list of complete notice spans. Bump the registry version and reconcile and publish a new generation to apply the receipt. A URL is available when a request cannot return the original.

## Serving in Docker Compose

For the production profile, see [Docker Compose deployment](deployment.md).

Start `rfc-mcp-docker-serve` with its default entry point. Configure its existing database TLS credentials for the SELECT-only role and mount the mirror read-only. Mount a dedicated secret file containing exactly 48 random bytes for cursor authentication. Use a shared stable key for restarts that should preserve outstanding cursors.

| Setting | Value |
| --- | --- |
| `RFC_MCP_BIND` | HTTP bind address. Default `127.0.0.1:8780`. For a separate TLS proxy container, bind on the private ingress interface. |
| `RFC_MCP_ADMIN_BIND` | Loopback bind for GET `/live`, `/ready`, and `/metrics`. Default `127.0.0.1:8781`. |
| `RFC_MCP_HOSTS` | Required comma-separated allowed Host authorities. A listed port must match. |
| `RFC_MCP_ORIGINS` | Comma-separated canonical allowed origins, such as `https://rfc.example`. Requests without Origin are accepted. |
| `RFC_MCP_TRUSTED_PROXIES` | Comma-separated trusted socket peer IPs. Each trusted peer must overwrite `X-Real-IP` with one IP. Direct callers are limited by their socket peer. |
| `RFC_MCP_CURSOR_KEY_FILE` | Cursor key mount path. Default `/run/secrets/rfc_cursor_key`. |

Terminate HTTPS at the ingress proxy and preserve the validated Host and MCP headers. Keep the database and admin listener on private routes. The server bounds request bodies to 64 KiB, reads headers within five seconds, and allows 15 seconds for a request after its headers arrive. Initial admission limits are 16 simultaneous requests and five requests per second per source with a burst of ten. Shared network egress shares a bucket. Liveness indicates the process is running. Readiness returns HTTP 200 only when an active generation is readable and the ripgrep sandbox is available. The loopback metrics report `rfc_mcp_rg_available`.

Propagate downstream disconnects from the proxy. Serve closes the dedicated database socket when a read is cancelled. PostgreSQL checks that socket every 100 ms during queries so it can release a blocked read promptly. [PostgreSQL TCP settings](https://www.postgresql.org/docs/19/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)

Run the [private Phase 5 gate](../scripts/phase5/integration.py) after preparing the images with the Phase 0 Compose probe. It uses Python 3, the Python `jsonschema` package, and OpenSSL. The gate mounts disposable keys and test data from `/tmp`.

## Client compatibility

The [Phase 8 restore probe](../scripts/phase8/integration.py) exercised distinct clients against the restored service behind a disposable HTTPS proxy, with certificate verification and per-request MCP metadata enabled.

| Client | Discovery, listing, and direct calls to all five tools | Revision `2025-11-25` | GET stream and DELETE session |
| --- | --- | --- | --- |
| Python 3.14.4 `http.client` | HTTP 200 at `2026-07-28` | HTTP 400, unsupported | HTTP 405, unsupported |
| curl 8.18.0 over HTTP/1.1 | HTTP 200 at `2026-07-28` | HTTP 400, unsupported | HTTP 405, unsupported |

Other clients need to support the pinned request metadata and headers, including direct calls without initialization. Legacy initialization, stdio, and standalone HTTP+SSE are unsupported. Hosted clients require their own integration test. [Observed Phase 8 results](implementation-status.md#phase-8)
