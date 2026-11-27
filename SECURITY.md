# Security Policy

## System and Scope

This policy covers RFC MCP Docker source, configuration and deployment
guidance, release workflows, and official Serve, Sync, Feed, and Maintenance
images.
Use the exact revision or image digest to identify the affected build.
[Official image repositories and digests](docs/ghcr-release.md)

The public service accepts anonymous, read-only MCP at revision
`2026-07-28` over Streamable HTTP. Its production Compose profile uses an
HTTPS ingress proxy. The admin listener stays on loopback. Feed control,
publication notifications, and PostgreSQL use separate private routes.
[MCP API](docs/mcp.md) · [Compose deployment](docs/deployment.md#proxy-and-network-boundaries)

## Threat Model and Trust Boundaries

Treat public headers, request bodies, query expressions, regex patterns,
and cursors as attacker-controlled. Treat upstream catalog, feed, TXT, XML,
and rsync inputs as untrusted until their applicable validation succeeds.
Treat traffic on private control networks as unauthenticated until the
message checks pass. Compare claims about proxy trust with the actual peer
and configured forwarding behavior. [Ingestion](docs/ingestion.md) ·
[Private control plane](docs/control.md)

Protect source and provenance integrity, active and retained generations,
service credentials, and availability of bounded reads. Consider the
boundary crossed and the prerequisites for crossing it when assessing a
report.

## Security Invariants

- The public endpoint exposes read-only operations with bounded request,
  query, worker, and response processing. Validate authorities and origins
  according to the MCP ingress contract. Honor forwarded client identity
  only from configured trusted proxy peers.
- Keep the serve mirror read-only and its database grants scoped to reads.
  Scope writes to sync and keep feed off the mirror and database. Validate
  database TLS and keep credentials and pairwise keys scoped to their roles.
- Run database maintenance without network access, with a read-only mirror
  and private backup storage. Verify the archive and mirror copy before a
  populated upgrade, and require the role-specific storage and rights
  checks before starting the applications. [Startup and recovery](docs/deployment.md#initialize-or-upgrade)
- Authenticate private requests and acknowledgements. Preserve caller and
  audience binding, replay checks, and the separation of cursor and control
  keys. Fail closed when authentication or the expected state cannot be
  verified.
- Preserve original TXT bytes, source evidence, and redistribution
  restrictions. Verify candidates and fence publication by sequence. Keep
  readers on their guarded generation through rollback, garbage collection,
  and worker cleanup. [Publication and recovery](docs/publication.md)
- Confine ripgrep to approved inputs using enforced Landlock and seccomp.
  Preserve process, network, descriptor, and resource restrictions. Return
  a safe error when the sandbox or a required check is unavailable.
  [Worker sandbox](docs/decisions/0006-worker-sandbox.md)

## Reportable Findings

Report behavior that crosses these boundaries or causes a security impact
under realistic conditions. Examples include public mutation, unauthorized
access to files or credentials, forged control messages that change
publication state, corpus or release artifact substitution, and a bypass
of documented resource controls that makes the service unavailable.

Describe the deployment, configuration, access, and input required to
reach the affected path. Include upstream dependency, base image, or
GitHub Actions findings when this repository's use exposes the issue.
Reports involving forks or local changes should identify any affected
behavior in the official project.

## Supported Versions and Branches

| Branch or release | Security support |
| --- | --- |
| `main` | Supported development branch. |
| Latest stable release | Supported stable release line. |
| Older stable releases, beta releases, build tags, and other branches | Backports are considered case by case. |

Identify released images by immutable digest. Support follows the release
that produced the image, including when the image is reached through a
mutable alias.

## Reporting a Vulnerability

Submit undisclosed vulnerabilities through the
[GitHub private vulnerability report form](https://github.com/PublicData-stream/rfc-mcp-docker/security/advisories/new).
Keep details out of public issues, discussions, pull requests, and commit
messages while the report is private.

Include the affected version, source revision or image digest, deployment
context, prerequisites, security impact, and enough sanitized detail for
maintainers to verify the report. Include a mitigation or relevant upstream
advisory if known. Redact credentials, keys, tokens, personal data, and
private deployment details that are unnecessary for triage. Use systems
you are authorized to test.

## Response and Disclosure

Maintainers acknowledge reports through the private advisory, assess
reachability and impact, and identify affected release lines. They coordinate
with the reporter and affected upstream maintainers as needed and share
status through the private channel.

Keep the report private until a supported fix or actionable mitigation is
verified and a maintainer approves disclosure. Publish the GitHub Security
Advisory with the affected versions, fix or mitigation, and any required
operator action. Agree on attribution with the reporter before publication.
