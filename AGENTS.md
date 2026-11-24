# AGENTS.md

## Project Overview

RFC MCP Docker is a Rust service for researching official RFCs. Sync verifies
source material and publishes indexed generations. Serve exposes anonymous,
read-only MCP only at revision `2026-07-28` over Streamable HTTP. Feed and
publication hints use authenticated private HTTP.

Preserve official RFC TXT as original bytes and use zero-based half-open
byte offsets. Use Docker Compose for deployment and keep runtime permissions
scoped to each service.

## Repository Structure

- `Cargo.toml` and `rust-toolchain.toml` define the workspace and toolchain.
- `apps/` contains the serve and feed binaries, public MCP schemas, private
  control code, and application integration tests.
- `crates/` contains shared corpus types, storage, sync and publication,
  search and the ripgrep worker, and shared test support.
- `migrations/` contains PostgreSQL setup, schema migrations, and roles.
- `Dockerfile`, `compose.yml`, and `deploy/` define images, deployment, and
  configuration examples.
- `fixtures/rfc/` contains official RFC fixtures with notices and provenance.
- `scripts/` contains local integration probes and release qualification.
  `.github/workflows/` contains GitHub Actions workflows.
- `docs/` contains technical specifications, operator guides, and
  implementation evidence.

## Contributor Guidance

Read [CONTRIBUTING.md](CONTRIBUTING.md) before editing. It is the source of
truth for contributor workflow, testing, security requirements, and commit
format.

- [Contribution Workflow](CONTRIBUTING.md#contribution-workflow)
- [Commit Messages](CONTRIBUTING.md#commit-messages)
- [Security Requirements](CONTRIBUTING.md#security-requirements)
- [Pull Request Checklist](CONTRIBUTING.md#pull-request-checklist)

Run the offline checks and relevant Phase 0 probes before marking work
verified. Keep temporary test data under `/tmp` and use `/tmp/rust/target`
for Cargo artifacts. Record observed results and blockers in the
[implementation status](docs/implementation-status.md).

## Agent Commit Message Guidance

Describe the repository change and its verification in terms that another
contributor can use. Use repository-relative paths and stable references.
Summarize a local result in the commit message when the evidence lives in a
temporary directory. Keep session-specific commands, absolute host paths,
private evidence, credentials, and undisclosed finding details out of commit
messages.

## Codex Security Advisory Guidance

Follow the [security policy](SECURITY.md) for validated, reportable findings
from local or Cloud Codex Security reviews. Use one GitHub repository
advisory per distinct finding and keep the report and triage private. Publish
only after a supported fix or actionable mitigation is verified and a
maintainer approves disclosure.

Before each submission or publication, preview the exact payload and obtain
explicit approval for that action. Verify the canonical repository, immutable
source revision and finding locations, duplicate status, authenticated
identity, permissions, and intended visibility. Read the advisory back to
confirm its state. Request a CVE only when explicitly directed. If the
available tools cannot preserve the required visibility, report the blocker
and keep the finding private.
