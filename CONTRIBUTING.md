# Contributing to RFC MCP Docker

RFC MCP Docker is a Rust service for researching official RFCs. The serve
service answers read-only MCP requests against an indexed generation. Sync
collects and verifies source material and publishes generations. Feed sends
authenticated hints to sync. Changes to public request handling, source
verification, generation visibility, and runtime isolation need focused review.

Use repository-relative paths from the root in documentation, commands, and
review notes. If a command must run from a subdirectory, state that explicitly.

## Repository Layout

| Path | Purpose | Change here when |
| --- | --- | --- |
| `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml` | Rust workspace, locked dependencies, and pinned toolchain. | Workspace membership, shared dependencies, or toolchain requirements change. |
| `apps/` | Serve and feed entrypoints, public MCP transport and schemas, and private control HTTP and authentication. | MCP behavior, feed behavior, service wiring, or control messages change. |
| `crates/core/`, `crates/storage/` | Shared corpus types, mirror and blob storage, and PostgreSQL access. | Byte coordinates, storage layout, database access, or generation guards change. |
| `crates/sync/`, `crates/search/` | Ingestion, indexing, publication, search, and the ripgrep worker. | Source handling, reconciliation, query semantics, or worker confinement changes. |
| `apps/tests/`, `crates/test-support/`, `fixtures/rfc/` | Rust integration tests, shared fixture helpers, and original RFC fixtures with provenance. | Behavior needs regression coverage or an official fixture changes. |
| `migrations/` | PostgreSQL provisioning, schema migrations, and runtime grants. | Persisted data, role privileges, or upgrade behavior changes. |
| `Dockerfile`, `compose.yml`, `deploy/` | Service images, production Compose topology, and configuration examples. | Image contents, deployment settings, or runtime permissions change. |
| `scripts/`, `.github/workflows/` | Local probes, qualification runners, and GitHub Actions. | Verification, CI, or publication automation changes. |
| `docs/` | Protocol and operator guides, decisions, and observed implementation results. | Public contracts, operations, or verification evidence changes. |

## Contribution Workflow

1. Identify the affected area and read its technical guide before editing.
2. Make the smallest change that addresses the requested behavior. Update
   relevant tests when changing runtime, protocol, storage, permissions,
   deployment, or CI behavior.
3. Update the technical guide and applicable configuration examples when
   behavior, syntax, defaults, or operator commands change. Use the README
   for setup, usage, and discoverability.
4. Run the offline workspace checks and the Phase 0 or later probes relevant
   to the change. Record what ran, what passed, and any blockers in the
   [implementation status](docs/implementation-status.md).
5. Clean up disposable test resources and review the diff before committing.

Use the Rust toolchain selected by `rust-toolchain.toml`. Prefetch the locked
dependencies once when the cache needs population, then run the offline gate
from the repository root.

```sh
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp
cargo fetch --locked
scripts/phase0/check-rust.sh
```

The gate checks formatting, the frozen build, Clippy, workspace and
documentation tests, documentation warnings, and the local MCP wire protocol.
For changes that affect Rust features, run the all-feature checks used by
[workspace CI](.github/workflows/ci.yml) with the same environment.

```sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
```

Run `python3 -B scripts/phase8/http-smoke.py` for public HTTP boundary changes.
Run `python3 -B scripts/ci/check-doc-links.py` when editing Markdown links or
headings. It checks repository-relative targets and Markdown anchors offline.
Run `actionlint` when editing GitHub Actions workflows.

## Release and Upgrade Guidance

Document changes to public MCP behavior, configuration, schema, and persisted
state in the affected technical and operator guides. Include the upgrade and
recovery procedure when a change affects an existing deployment. Where a
migration requires restoring older state, identify the matching backup and
image requirements. [Docker Compose deployment](docs/deployment.md#upgrade-existing-volumes) ·
[Backup and restore](docs/publication.md#backup-and-restore)

Treat source changes, registry publication, and deployment as separate
operations. The release runbook defines the exact-commit CI requirement, signed
tag and channel rules, and digest verification. Use the Compose qualification
guide for the local release evidence and public activation gates.
[GHCR image publication](docs/ghcr-release.md) ·
[Phase 8 release qualification](docs/release-readiness.md)

## Commit Messages

Use Conventional Commits for new commits.

```text
<type>(<scope>): <subject>
```

- Use one of `feat`, `fix`, `chore`, `docs`, `ci`, `refactor`, `security`,
  `tests`, or `perf` for the type.
- Choose a scope that identifies the affected area, such as `mcp`, `sync`,
  `storage`, `search`, `compose`, or `workflows`.
- Start the subject with an imperative verb and keep the summary short.
- Wrap technical identifiers, paths, commands, configuration keys, headers,
  and literal values in backticks in the title and body.
- Keep each commit focused and GPG-sign it. Add
  `Assisted-by: Codex <codex@openai.com>` as a Git trailer to each
  Codex-assisted commit.

```text
fix(mcp): validate the `MCP-Protocol-Version` header
docs(deploy): document mirror ownership
tests(storage): cover restored generation reads
ci(workflows): run the Compose fixture matrix
```

## Rust Module Organization

Place new code in the crate or module that owns the responsibility. Keep
transport validation and response shaping in the application, source
collection and publication in sync, corpus persistence in storage, and query
evaluation and worker supervision in search. Keep shared corpus types and
byte-range rules in core. Add a focused module when a new responsibility
does not fit an existing one.

Keep parsing and validation separate from network, database, and filesystem
effects where practical. Preserve existing module interfaces and lifecycle
ownership when moving code. Explain the safety invariants of any changed
`unsafe` block and exercise the affected path in its intended runtime.

## Area Guidelines

- **Sources and originals.** Preserve every byte of official RFC TXT,
  including CRLFs and form feeds. Use zero-based
  half-open byte offsets. Keep source evidence, redistribution decisions, and
  original notices with the corresponding content. Third-party RFC fixtures
  retain their own notices and provenance.
  [Original byte ranges](docs/decisions/0002-original-byte-ranges.md) ·
  [Ingestion](docs/ingestion.md) · [Fixture provenance](fixtures/rfc/README.md)
- **Public MCP and search.** Keep the endpoint anonymous and read-only at
  revision `2026-07-28` over Streamable HTTP. Preserve generation-scoped
  reads and cursors, and enforce the documented query and response budgets.
  Update the exported schemas and contract tests when tool arguments or
  results change. [MCP API](docs/mcp.md) · [Search DSL](docs/search.md)
- **Publication and storage.** Preserve durable source and manifest
  validation, sequence-checked publication and rollback, and the reader
  guards that protect data from retirement and garbage collection. Test
  failure and recovery paths when changing those boundaries.
  [Publication and recovery](docs/publication.md) · [Storage](docs/storage.md)
- **Configuration and deployment.** Keep Compose and its configuration
  examples aligned with validated defaults and compatibility behavior.
  Keep roles, credentials, mounts, and network access scoped to each service.
  [Compose deployment](docs/deployment.md) · [Private control plane](docs/control.md)

## Tests and Temporary Data

Keep Rust tests beside the affected module or under `apps/tests/` for
application-level behavior. Reuse the shared fixture helpers where
appropriate. Use deterministic inputs and check original-byte equality when
testing source handling or ranges.

- Put temporary test data under `/tmp` and Cargo artifacts under
  `/tmp/rust/target`.
- Generate short-lived certificates, private keys, configuration, and staging
  data in a private directory for the test run. Keep generated credentials,
  build output, and raw probe logs out of Git.
- Clean up the temporary data and resources the run created, including on
  failure where practical. Record sanitized results and blockers in the
  [implementation status](docs/implementation-status.md).

## Docker and Integration Tests

Use disposable Compose projects for integration tests. Run the Phase 0
Compose probe for changes to runtime dependencies, images, or isolation. It
checks the actual service images, MCP wire behavior, PostgreSQL connectivity
and authorization, and runtime confinement.

```sh
python3 -B scripts/phase0/integration.py
```

Use `--context` when needed to select the intended Docker engine. Choose the
later phase gates for the behavior under change and the full qualification
for release candidates. The separate `scripts/phase0/upstream.py` probe
contacts live sources. Use it when upstream assumptions need verification.
[Deployment prerequisites](docs/deployment-prerequisites.md#repeat-phase-0) ·
[Release qualification](docs/release-readiness.md#repeat-the-checks)

Use unique projects and private test mounts. Clean up test containers,
networks, volumes, and test-only images after the run. When deliberately
reusing a qualified image set, identify its tags or digests and preserve
resources owned by other projects.

## Security Requirements

Treat public request metadata and tool arguments, corpus inputs, and private
control messages as untrusted until the relevant checks pass. Keep Host and
Origin validation, trusted-proxy handling, request budgets, and TLS
verification in place. Preserve the private listener boundaries and
authenticated, replay-checked control messages.

Keep serve on read-only mirror access and a read-only database role. Give sync
the required writer access and keep feed off the mirror and database. Keep
the ripgrep worker confined to approved inputs with enforced Landlock,
seccomp, and resource limits. Fail closed when a required security check
cannot run.

Keep credentials, private keys, and sensitive runtime configuration out of
commits, fixtures, and public logs. For a
security-sensitive change, explain the affected trust boundary, controlled
inputs, failure behavior, and compatibility impact. Add focused regression
coverage where practical and report any verification limits. Use the
[security policy](SECURITY.md) to report undisclosed issues privately.

## Pull Request Checklist

- The change and affected area are clear, and the commits are focused,
  GPG-signed, and follow the documented format.
- Relevant tests cover the changed behavior, including failure cases when
  changing trust boundaries or durable state.
- Public contracts, configuration examples, and upgrade or recovery
  instructions reflect the change where applicable.
- Relevant offline and Compose checks ran, or the verification record states
  the blocker and what remains unverified.
- Temporary data and Docker resources created for the checks were cleaned up.
- The diff contains only intended sources, fixtures, documentation, and
  sanitized evidence.
