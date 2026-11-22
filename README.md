# RFC MCP Docker

A read-only RFC research MCP service under development. Sync builds indexed RFC candidates from verified RFC Editor TXT and metadata. Private operator commands publish and roll back complete candidates, and readers hold a generation guard while accessing corpus data. The MCP entrypoint provides the Phase 0 protocol probe.

See [implementation status](docs/implementation-status.md), [candidate ingestion](docs/ingestion.md), [publication and recovery](docs/publication.md), [storage and database provisioning](docs/storage.md), [deployment prerequisites](docs/deployment-prerequisites.md), and [contributing](CONTRIBUTING.md).
