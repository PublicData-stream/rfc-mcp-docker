# RFC MCP Docker

A read-only RFC research MCP service under development. Sync builds and publishes indexed snapshots of original RFC Editor TXT and metadata with recorded source evidence. Feed and publication hints use private authenticated HTTP. Operators can roll back retained generations, and readers hold a generation guard while accessing corpus data. The MCP endpoint exposes generation-scoped list, search, retrieval, section, and sandboxed ripgrep tools.

The HTTPS-first Phase 1 build runs discovery and gates new publication. See [Phase 1 discovery](docs/ingestion.md#run-discovery).

See [Docker Compose deployment](docs/deployment.md), the [MCP API](docs/mcp.md), [implementation status](docs/implementation-status.md), [candidate ingestion](docs/ingestion.md), [control plane and key rotation](docs/control.md), [publication and recovery](docs/publication.md), [storage and database provisioning](docs/storage.md), and [contributing](CONTRIBUTING.md).

See [Phase 8 release readiness](docs/release-readiness.md) for qualification commands, the acceptance matrix, and public-release gates.

See [dual-queue bootstrap qualification](docs/ingestion.md#bootstrap-qualification) for first-publication benchmarks and the production HTTPS pacing calculation.
