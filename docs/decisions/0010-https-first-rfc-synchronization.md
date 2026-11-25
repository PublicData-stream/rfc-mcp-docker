# HTTPS-first RFC synchronization

The HTTPS-first collector uses one source policy for daemon startup, scheduled
reconciliation, Feed-triggered work, and manual reconciliation. The schema 9
collector and its two TXT policies are the baseline for this change.
[Historical bootstrap decision](0009-cold-rfc-bootstrap.md) ·
[Phase 0 evidence](../implementation-status.md#https-first-rfc-synchronization-phase-0)

## Source and publication policy

- Select the complete expected RFC set from the official HTTPS catalog and
  preserve the beginning and ending catalog consistency check. Use rsync
  inventory to classify mirror availability and Feed as an authenticated
  wakeup hint. A missing required mirror entry must surface as a deferred or
  failed acquisition. The required selection stays intact.
- Acquire original TXT and RFCXML via authenticated official HTTPS. Use
  bounded rsync checksum dry runs with `--checksum --checksum-choice=auto`
  to audit required TXT before readiness. Bind each successful receipt to
  the selected RFC, the actual HTTPS observation and the local BLAKE3
  object. Record the negotiated checksum match as its own evidence class.
- After a checksum mismatch, obtain fresh HTTPS and rsync captures and
  compare their original bytes. Keep unresolved evidence in bounded
  private quarantine. A required TXT audit failure prevents readiness and
  leaves the incumbent readable. Preserve the existing distinct RFCXML
  outcomes. Record RFCXML audit as inapplicable unless an equivalent
  official rsync artifact is established and qualified.
- Preserve original bytes, including NUL, and zero-based half-open byte
  ranges. Keep bounded staging and durable work, deterministic manifests,
  immutable published generations, reader locks, and the sequence-checked
  pointer and outbox transaction. Manual reconciliation prepares a
  candidate for explicit promotion. The daemon can promote its own
  validated candidate.

## Minimum compatibility work

Ingestion TOML v2 removes `txt_provenance` and the rsync acquisition
mode selector. Old configuration must fail with a migration hint. Keep
resource and scheduling settings separate from the fixed source policy.
Phase 2 brokers Feed polling through Sync. Catalog probes, TXT, RFCXML,
Feed polls, and retries share one HTTPS scheduler. Repair requests use the same scheduler.

A forward schema migration must represent HTTPS captures, checksum audit
receipts, and exact-byte matches for repair as distinct evidence. Extend
the checkpoint context and eligibility checks to bind work to the new
policy and selection. Enforce eligible HTTPS TXT and complete audit at
candidate readiness and promotion. Preserve historical observation IDs,
timestamps, source URLs, verification strength, and manifest digests.
Retained historical generations remain readable under their recorded
provenance. Acquire missing HTTPS evidence and complete the required audits
before the first publication under the new policy.

The upgrade baseline is migrations 0001–0009 and ingestion TOML schema
`1` at revision `376fdc65c37c331ea433ebdb57ef70e7afbfdfbe`. Before the
migration, close ingress, drain Feed and Sync, stop Serve, and retain
a verified matched PostgreSQL and mirror backup, the schema 9 images, and
the v1 configuration. With the upgraded schema, verify the incumbent's
sequence, retained reads, readiness, and a pinned MCP read before reopening
ingress. If the previous binaries reject the upgraded schema, restore the
matched backup into fresh volumes with the saved schema 9 images and v1
config. Returning to strict batched ingestion on schema 9 uses the existing
same-schema configuration procedure.
[Upgrade and strict rollback](../deployment.md#upgrade-existing-volumes) ·
[Backup and restore](../publication.md#backup-and-restore)

## Phase 1 rollout

The Phase 1 binary accepts schema v2 and runs discovery through the daemon
and manual reconciliation commands. Discovery reports a sealed selection
without creating a candidate or a durable catalog observation. It can
read historical evidence on PostgreSQL schema 9. The existing release
continues to supply production ingestion while HTTPS acquisition and the
audit gate are developed. Phase 1 retains sequence-checked rollback to
an already retained generation. [Phase 1 evidence](../implementation-status.md#https-first-rfc-synchronization-phase-1)

## Phase 2 rollout

Use a Sync-only, bounded run spool for originals and observations. Manual
and automatic requests share a single HTTPS owner and preserve the corpus
fence through cleanup. The spool supports acquisition only. Phase 3 adds
CAS, durable checkpoints, and joins. The existing PostgreSQL schema and
publication gate remain in force.
[Phase 2 evidence](../implementation-status.md#https-first-rfc-synchronization-phase-2)

## Phase 3 rollout

Apply schema 10 on the isolated shadow copy. It distinguishes new HTTPS-only
TXT from historical verification methods and rejects capture-only TXT as
generation membership evidence. Checkpoints pin verified originals and sealed
TXT indexes. Both lanes join by selection and RFC identity under the corpus
and publication fence. The existing active generation and retained rollback
remain available. TXT audit, repair and promotion are later phases.
[Phase 3 evidence](../implementation-status.md#https-first-rfc-synchronization-phase-3)

## Phase 4 rollout

Audit and repair run in shadow mode on schema 10. A separate Sync-only
volume holds bounded working files, durable receipts, and quarantined
source pairs. Receipt validation and successful repair feed the existing
coordinator and its ending catalog fence. The database publication proof
contract follows in Phase 5.

## Phase 5 rollout

Apply schema 11 on a matched shadow copy. Audit receipts and terminal RFCXML
outcomes become part of the immutable manifest. Readiness and promotion
enforce the sealed selection, expected sequence, and deadline. The daemon
promotes its own full candidates and completes the associated job and hints
with the pointer and outbox event. Manual reconciliation prepares a candidate
for explicit promotion. Historical manifests and retained rollback preserve
the original evidence. [Phase 5 evidence](../implementation-status.md#https-first-rfc-synchronization-phase-5)

## Phase 6 rollout

Base Compose requires v2 TOML and private disk-backed spool and audit
filesystems. Feed uses the authenticated Sync broker. The runtime uses one
engine for manual and automatic work. Historical collectors are exercised
from pinned checkouts. Schema 11 serves historical manifests using the
evidence originally recorded for them. [Upgrade and operations](../deployment.md#upgrade-existing-volumes)

## Cold-start timing

Keep the [cold-start timing contract](0009-cold-rfc-bootstrap.md#timing-contract)
and include audit staging ownership setup in T0–T1. Qualification of the
replacement belongs to Phase 8. With 9,862 required TXT and one RFCXML
lookup per RFC plus two catalog captures, 19,726 HTTPS starts at the
one-second minimum put 19,725 seconds (5 h 28 min 45 s) between the first
and last starts before retries, audit, and publication. The 900-second
production objective is infeasible at this pacing. A faster profile needs
documented provider permission and separate operator authorization.
