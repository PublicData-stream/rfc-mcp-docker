# Repository guidance

Read [CONTRIBUTING.md](CONTRIBUTING.md) before editing. Preserve official RFC TXT as original bytes and use zero-based half-open byte offsets. Keep the public MCP endpoint read-only and limited to revision `2026-07-28` over Streamable HTTP. Use Docker Compose for deployment. Keep runtime permissions scoped to each service.

Run the offline checks and relevant Phase 0 probes before marking work verified. Keep temporary test data under `/tmp` and use `/tmp/rust/target` for Cargo artifacts. Record observed results and blockers in [the implementation status](docs/implementation-status.md).
