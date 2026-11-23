# RFC MCP Docker

A read-only RFC research MCP service under development. Sync builds and publishes indexed RFC snapshots from verified RFC Editor TXT and metadata. Feed and publication hints use private authenticated HTTP. Operators can roll back retained generations, and readers hold a generation guard while accessing corpus data. The MCP endpoint exposes generation-scoped list, search, retrieval, section, and sandboxed ripgrep tools.

See [Docker Compose deployment](docs/deployment.md), the [MCP API](docs/mcp.md), [implementation status](docs/implementation-status.md), [candidate ingestion](docs/ingestion.md), [control plane and key rotation](docs/control.md), [publication and recovery](docs/publication.md), [storage and database provisioning](docs/storage.md), and [contributing](CONTRIBUTING.md).
