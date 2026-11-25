# Publication and recovery

The PostgreSQL `rfc.active_generation` row identifies the active generation and its publication sequence. Sync finalizes the immutable manifest and verifies the original TXT and provenance files on the mirror before moving the pointer. Promotion, demotion, the new sequence, and the pending outbox event commit together. Serve resolves visibility from PostgreSQL at the start of each guarded read. Sync delivers the outbox events as authenticated hints.

## Operator commands

Run these commands with the sync binary, sync database credentials, and its writable mirror in the private Compose environment.

The command table describes the published schema 9 collector. With the
HTTPS-first Phase 3 build, `--phase2-reconcile` reports durable processing and
`--phase3-publish` fails at the publication gate. The retained-generation
rollback and maintenance commands still use the rules below.
[Acquisition and rollout](ingestion.md#run-acquisition)

| Command | Effect |
|---|---|
| `--phase2-reconcile --full` | Prepare a full candidate and return its generation ID and manifest digest. |
| `--phase3-publish GENERATION --expect-sequence SEQUENCE` | Verify and publish a ready full candidate. The first publication expects sequence `0`. |
| `--phase3-rollback GENERATION --expect-sequence SEQUENCE` | Verify and reactivate a retained generation with a new publication sequence. |
| `--phase3-recover` | Reconcile abandoned candidates, stages, interrupted retirement, and registered or orphaned files awaiting deletion. |
| `--phase3-gc` | Run recovery and retire eligible published generations. |
| `--phase3-discard GENERATION` | Retire an explicitly selected ready candidate and reclaim newly unreachable files. |

An expected-sequence conflict leaves the publication untouched. After a lost connection around commit, inspect `rfc.active_generation` and `rfc.notification_outbox` for the intended sequence and generation before retrying. A missing or corrupted file stops promotion. Restore the verified bytes to the original digest path before retrying. Retain the recorded incident for investigation.

The schema 9 collector runs recovery before each reconciliation. Phase 3 sweeps its spool and interrupted CAS staging on the next run. Use `--phase3-recover` for an explicit reclamation pass. It reports blocked and remaining deletion counts. An eligible restart checkpoint can pin a canonical file after the interrupted generation is retired. Expiry and eviction release that pin while generation references still protect published or retained bytes. A queued item with unexpected contents, type, or links remains in place for inspection. Preserve private quarantine. If retiring an eligible generation is busy, repeat GC after the reader has finished. [Interrupted reconciliation](ingestion.md#interrupted-reconciliation)

## Locks and retention

1. Sync takes an exclusive `flock` on the stable writable mirror root, then takes PostgreSQL's session advisory lock `-1` on a dedicated connection. Its filesystem work remains fenced if that connection is lost. The independent migration key is `-2`.
2. A reader resolves the active or pinned generation inside a `READ COMMITTED READ ONLY` transaction. It acquires the shared transaction advisory lock using the nonnegative generation ID and a shared lock on `generation-locks/ID` on the read-only mount. It then rechecks lifecycle state in a fresh statement. Keep all subsequent corpus queries scoped to the returned generation and any future caches keyed by generation and document version.
3. GC holds the sync coordinator and tries the exclusive database lock for the generation. For retained generations it must also take the exclusive mirror lock before making the generation unreadable and pruning its references. A worker supervisor retains a duplicate of the shared mirror lock until its child is reaped. Generation lock files are stable for the lifetime of the mirror and can be recreated when restoring into a fresh mirror before serve starts.

`RFC_RETAIN_GENERATIONS` defaults to three inactive generations. `RFC_RETAIN_HOURS` defaults to 24 hours after demotion. Both protections apply, and the values can be raised. Ready candidates remain protected until discarded or published. Tombstones and outbox metadata remain after reclamation. Bytes shared by any remaining candidate, publication, or retained provenance record stay on the mirror.

## Backup and restore

Stop feed and wait for running sync work to finish. Then stop sync and keep all one-shot sync commands, including GC, paused until the dump and copy have been verified. From the PostgreSQL peer socket, check that `SELECT pg_try_advisory_lock(-1)` returns true in a one-shot session. Serve can continue reading.

With a dedicated private `BACKUP_DIR` on separate storage and `RFC_ENV_FILE` set to the stack configuration, run:

```sh
umask 077
docker compose --env-file "$RFC_ENV_FILE" exec -T postgres \
    pg_dump -Fc --schema=rfc -U postgres -d rfc > "$BACKUP_DIR/corpus.dump"
docker compose --env-file "$RFC_ENV_FILE" exec -T postgres psql -XqAt -U postgres -d rfc \
    -c 'SELECT generation_id,publication_sequence FROM rfc.active_generation' > "$BACKUP_DIR/pointer"
docker compose --env-file "$RFC_ENV_FILE" exec -T postgres psql -XqAt -U postgres -d rfc \
    -c "SELECT encode(digest,'hex'),byte_length FROM rfc.blobs ORDER BY digest" > "$BACKUP_DIR/blobs"
docker compose --env-file "$RFC_ENV_FILE" exec -T postgres psql -XqAt -U postgres -d rfc \
    -c "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id" \
    > "$BACKUP_DIR/readable"
```

Copy every file listed in `blobs` from `blobs/blake3/<first-two-hex-digits>/<digest>` while preserving that relative path. The inventory includes originals, ready candidates, provenance and checkpoint-pinned bytes. Use a BLAKE3 tool such as `b3sum` and a byte count to check every file in the independent copy before resuming sync and feed. Save the version and image references used for the snapshot. Back up private quarantine separately when it is needed for incident review.

Restore into fresh volumes with the proxy route closed and all three application processes stopped. Provision PostgreSQL and the scoped roles as in the [deployment guide](deployment.md). Provision fresh secrets for the restored instance, or securely supply the prior cursor key when outstanding cursors must survive.

1. Verify the backup inventory, then restore the schema and data over the peer socket using `pg_restore --clean --if-exists --exit-on-error -U postgres -d rfc` with `corpus.dump` on stdin.
2. Install the verified canonical tree on the fresh mirror and recreate the empty `generation-locks/<id>` files listed in `readable`. Match the original directory ownership and modes on the destination host. Canonical files and generation fences belong to sync UID/GID 10002 inside the container and use mode 0444. Flush the restored file and directory data before bringing up serve. [Storage layout](storage.md)
3. Confirm the saved pointer, sequence, manifests, original ranges, database privileges, and readonly serve mount. Start serve and check loopback readiness and the same pinned MCP reads used before backup. Start sync and feed after the read path passes, then reopen the proxy route.

For a corrupt canonical original, close the proxy route, quiesce the stack, and retain the suspect file for investigation. Restore from a matching verified backup or roll back to a complete retained generation with an expected sequence. A rollback cannot recover shared bytes if the rollback target references the same corrupt digest. Recheck readiness, manifests, and original-byte reads before reopening traffic. [Phase 8 restore rehearsal](release-readiness.md#restore-and-client-results)
