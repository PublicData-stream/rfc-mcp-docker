# Publication and recovery

The PostgreSQL `rfc.active_generation` row identifies the active generation and its publication sequence. Sync finalizes the immutable manifest and verifies the original TXT and provenance files on the mirror before moving the pointer. Promotion, demotion, the new sequence, and the pending outbox event commit together. Serve resolves visibility from PostgreSQL at the start of each guarded read. Phase 6 will deliver the outbox events as authenticated hints.

## Operator commands

Run these commands with the sync binary, sync database credentials, and its writable mirror in the private Compose environment.

| Command | Effect |
|---|---|
| `--phase2-reconcile --full` | Prepare a full candidate and return its generation ID and manifest digest. |
| `--phase3-publish GENERATION --expect-sequence SEQUENCE` | Verify and publish a ready full candidate. The first publication expects sequence `0`. |
| `--phase3-rollback GENERATION --expect-sequence SEQUENCE` | Verify and reactivate a retained generation with a new publication sequence. |
| `--phase3-recover` | Reconcile abandoned candidates, stages, interrupted retirement, and registered or orphaned files awaiting deletion. |
| `--phase3-gc` | Run recovery and retire eligible published generations. |
| `--phase3-discard GENERATION` | Retire an explicitly selected ready candidate and reclaim newly unreachable files. |

An expected-sequence conflict leaves the publication untouched. After a lost connection around commit, inspect `rfc.active_generation` and `rfc.notification_outbox` for the intended sequence and generation before retrying. A missing or corrupted file stops promotion. Restore the verified bytes to the original digest path before retrying. Retain the recorded incident for investigation.

Recovery also runs before each reconciliation. It reports blocked and remaining deletion counts. A queued item with unexpected contents, type, or links remains in place for inspection. Preserve private quarantine. If retiring an eligible generation is busy, repeat GC after the reader has finished.

## Locks and retention

1. Sync takes an exclusive `flock` on the stable writable mirror root, then takes PostgreSQL's session advisory lock `-1` on a dedicated connection. Its filesystem work remains fenced if that connection is lost. The independent migration key is `-2`.
2. A reader resolves the active or pinned generation inside a `READ COMMITTED READ ONLY` transaction. It acquires the shared transaction advisory lock using the nonnegative generation ID and a shared lock on `generation-locks/ID` on the read-only mount. It then rechecks lifecycle state in a fresh statement. Keep all subsequent corpus queries scoped to the returned generation and any future caches keyed by generation and document version.
3. GC holds the sync coordinator and tries the exclusive database lock for the generation. For retained generations it must also take the exclusive mirror lock before making the generation unreadable and pruning its references. A worker supervisor retains a duplicate of the shared mirror lock until its child is reaped. Generation lock files are stable for the lifetime of the mirror and can be recreated when restoring into a fresh mirror before serve starts.

`RFC_RETAIN_GENERATIONS` defaults to three inactive generations. `RFC_RETAIN_HOURS` defaults to 24 hours after demotion. Both protections apply, and the values can be raised. Ready candidates remain protected until discarded or published. Tombstones and outbox metadata remain after reclamation. Bytes shared by any remaining candidate, publication, or retained provenance record stay on the mirror.

## Backup and restore

1. Pause sync and its trigger source. Let in-flight sync work finish and keep one-shot sync commands paused through the snapshot and copy. Confirm sync has released the database coordinator. Serve can continue reading.
2. Take a consistent PostgreSQL dump and save the active generation and sequence. Export the registered `rfc.blobs` digest and length inventory while writers are still paused. It provides a superset of all blobs reachable from the snapshot, including ready candidates and provenance. Copy those canonical paths to independent backup storage and verify each length and full BLAKE3 digest. Save the readable generation lock files with the backup or record their IDs for recreation. Resume sync after verifying the backup.
3. Restore to fresh PostgreSQL and mirror volumes with serve and sync stopped. Provision the same scoped roles, restore the dump and verified bytes, and install the generation lock files for published and retained IDs. Use sync ownership and durable, read-only canonical files. Before reopening traffic, check the pointer, reconstruct the manifest, read original byte ranges from the active and retained snapshots, and verify permissions. Quarantine needs its own backup if operators require those incident records.

Run the [Phase 3 Compose suite](../scripts/phase3/integration.py) for the private fixture restore and failure cases. The restore copies independently of the source mirror, so later source GC has no effect on the backup.
