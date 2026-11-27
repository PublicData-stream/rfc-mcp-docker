# RFC discovery and candidate ingestion

Sync runs the same HTTPS-first discovery and durable processing for
manual requests and daemon jobs. It installs original HTTPS TXT and RFCXML
in BLAKE3 CAS, seals TXT indexes in PostgreSQL schema 13, and joins both
outcomes in the Sync coordinator. Each required TXT receives a current rsync
audit or a fresh dual-source byte comparison. Manual reconciliation prepares
a ready candidate. The daemon promotes its own full candidates.
[HTTPS-first design](decisions/0010-https-first-rfc-synchronization.md)

## Sync configuration

Use the [schema v2 TOML example](../deploy/ingestion.toml.example). Compose
requires the host file named by `RFC_SYNC_CONFIG_HOST_FILE` and mounts it
read-only for Sync at `RFC_SYNC_CONFIG_FILE`. The file must be regular UTF-8
and at most 16 KiB. Standalone Sync uses safe defaults when the file path is
unset. A configured path must resolve to a readable file. Validation precedes
database connections, source requests, and queue changes. Feed retains its
own format and cadence. [Control and Feed](control.md)

| Active setting | Default | Range |
| --- | ---: | ---: |
| `ingestion.max_accepted_bytes` | 4294967296 | 1–4294967296 |
| `ingestion.discovery.inventory_timeout_seconds` | 120 | 5–120 |
| `ingestion.discovery.full_reconcile_interval_seconds` | 86400 | 3600–604800 |
| `ingestion.https.max_in_flight_requests` | 2 | 1–2 |
| `ingestion.https.min_request_start_interval_ms` | 1000 | 1000–60000 |
| `ingestion.https.request_timeout_seconds` | 30 | 5–30 |
| `ingestion.https.max_attempts` | 3 | 1–3 |

The accepted-byte limit covers original captures and reused inputs. Discovery
also applies it to canonical TXT rehashes. The ten-minute discovery deadline
applies independently of acquisition and audit. Sync schedules opening and closing
catalog captures, TXT, RFCXML, Feed polls, and retries under one request
budget. Valid `Retry-After` on 429 or 503 applies to all classes and survives a
process handoff. Acquisition makes up to three attempts per artifact. A
delay above 30 seconds ends immediate retries. The 900-second target is
informational.

The pipeline table controls descriptor queue depths, the active artifact
window, parser workers, and raw and parse budgets. The active window
requires a reservation of 16 MiB of raw capacity and 128 MiB of parse
capacity per slot. `ingestion.database.batch_size` caps each checkpoint
transaction at 32 available results by default (range 1–32). The active
window can make a batch smaller. Revalidation defaults to one rsync session,
a 512-file batch, a 120-second session timeout, and three attempts with
jittered backoffs of 5 and 30 seconds. The batch also has a 256 MiB cap.
Periodic full reconciliation uses the shorter of the discovery and
revalidation intervals, both one day by default.

Run `rfc-mcp-docker-sync --check-config` with the standard Compose profile
to report effective configuration, the publication gate, scratch and
cgroup capacity, and free bytes and inodes on the spool and audit mounts.
It performs local checks before connecting to PostgreSQL or upstream. Sync uses its startup
snapshot until restart. [Private staging setup](deployment.md#private-sync-staging)

### Translate v1 settings

Start with the v2 example. Preserve explicit values for `max_accepted_bytes`,
`max_in_flight_requests`, `min_request_start_interval_ms`,
`request_timeout_seconds`, and `parser_workers` within the v2 bounds.
Rename `rfcxml_max_attempts` to `max_attempts` and
`max_pending_documents` to `max_active_documents`. Set
`RFC_SYNC_HTTP_CONCURRENCY` and `RFC_SYNC_PARSE_CONCURRENCY` values in
their TOML equivalents and remove both old environment variables. Remove
`txt_provenance`, `[ingestion.rsync]`, and the legacy bulk overlay.
Choose inventory and audit timeouts from their individual v2 fields.
Keep the v1 file and its matched images for schema rollback.

## Run acquisition

Use a disposable project with a private upstream fixture for qualification.
Set `RFC_ENV_FILE` and `COMPOSE_PROJECT_NAME` for the intended stack.
The existing manual command takes `--full` or `--sample`. The sample
selects RFCs 2119, 8174, and 9110. In the Compose project, an invocation
while the daemon runs uses its private Sync-UID socket. A matching request
joins the active run. One different-scope request may wait for the next
run. With the daemon stopped, the CLI acquires the deployment lease and
runs directly. A ready report includes `generation_id`, `manifest_digest`,
`expected_publication_sequence`, and `promotion_deadline_us` in Unix epoch
microseconds. If a manual invocation attaches to a daemon-owned automatic run,
it receives that run's publication outcome. [Publication commands](publication.md#operator-commands)

~~~sh
docker compose --env-file "$RFC_ENV_FILE" \
  run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --sample
docker compose --env-file "$RFC_ENV_FILE" \
  exec -T rfc-mcp-docker-sync /usr/local/bin/rfc-mcp-docker-sync --phase2-reconcile --full
~~~

The opening HTTPS catalog supplies the expected TXT set. A bounded rsync
inventory classifies mirror availability. New required TXT that has yet to
appear on the mirror defers the run. Loss of incumbent required TXT,
invalid inventory, local canonical corruption, and unequal opening and
ending catalog bytes fail the run.

Inventory discovery selects only canonical numeric RFC TXT regular files.
It ignores known ancillary entries, including the exact listed
`rfc-ref.txt -> rfc-ref.txt.new` symbolic link. The list-only command
reads metadata, and later rsync operations request canonical filenames.
A rejected listing adds `inventory_rejection` to the acquisition report
with the reason, one-based line number, and a bounded, escaped entry preview.
For a listing rejected after a successful rsync exit, the daemon waits five
minutes before the first automatic retry and fifteen minutes before the
second, then fails the job if the listing is still invalid. A waiting retry
takes priority over later automatic jobs. Manual reconciliation remains
available. Transport failures and source convergence use the existing
30- and 120-second delays.

On a cold run, the two queues request required TXT and RFCXML from HTTPS.
An existing TXT observation can be reused if it proves capture from the
official HTTPS URL, the canonical file rehashes correctly, and the mirror
inventory reports compatible size. The observation keeps its original
verification method. Newly captured TXT records `https_capture`. Both
cases produce audit-pending inputs. A successful TXT checkpoint pins
both the original and its sealed index, even while RFCXML is pending.
RFCXML is independently classified as captured, reused, 404, 410,
malformed, retries exhausted, or fetch failed.

<a id="interrupted-reconciliation"></a>

### Restart durable processing

Run the same reconciliation again after an interruption. Sync matches
checkpoints by scope, exact catalog selection, source, processing
versions, and publication sequence. It verifies canonical length and
BLAKE3 before reusing the original observation and indexes. A missing
checkpoint object is reacquired. A corrupt object stops the run for repair.
Restore verified bytes from a matched backup before retrying a canonical
corruption incident. A changed ending catalog invalidates that selection's
checkpoints.

The checkpoint cap is 40,000 records and 4 GiB of original bytes counted
per record. TXT checkpoints expire seven days after first recording.
Positive RFCXML checkpoints and malformed capture evidence expire
24 hours after capture. Reuse keeps the original observation and expiry.
Readiness clears checkpoints in the transaction that seals generation
membership. An interrupted run can reuse its unexpired audit receipts when
the catalog selection, inventory, and source identity still match.
New runs request XML again after 404, 410, malformed content, or fetch
failure. Expired and evicted artifacts become reclaimable through
`--phase3-recover` when the incumbent and retained generations have no
references to them.

<a id="reconciliation-progress"></a>

The terminal report distinguishes downloaded, reused, sealed and
TXT awaiting audit, classified RFCXML, and joined documents. It includes
the processing digest, queue and disk peaks, parser CPU time, CAS time,
and DB transaction counts. `txt_audited` includes checksum matches and repaired originals.
`membership_committed` counts the candidate's sealed membership. A daemon
report records `published` after the pointer commits. A standalone
candidate reports `ready`. The per-run
spool is cleaned after success and cooperative shutdown, or swept on the
next run after a crash. Deferred and failed manual runs return nonzero.
[Control behavior](control.md#reconciliation-and-publication)

## Audit and repair

Sync stages private copies of sealed HTTPS TXT bytes and runs an rsync checksum
dry run with `--checksum-choice=auto`. A complete session must account for
every selected file. Sync verifies the staged bytes again before committing the
receipt. The recorded method identifies the check that succeeded.

- `https_capture` records bytes from the TLS-authenticated official origin.
  TXT still needs a qualifying audit before publication.
- `rsync_checksum_match` records a successful comparison using rsync's
  negotiated pre-transfer checksum. The manifest records the algorithm
  when it can be determined reliably.
- `dual_source_byte_match` records exact equality of independently acquired
  HTTPS and rsync originals. The accepted bytes come from HTTPS.

RFCXML records its HTTPS source and a terminal fetch or parse outcome.
Rsync audit is inapplicable until an equivalent official XML artifact is
verified and qualified.

A required TXT mismatch triggers a fresh capture from each source through
the same HTTPS start gate and the single rsync session budget. Matching
originals are sealed and rejoined using the HTTPS bytes. Diverging pairs are
quarantined with their observation times, source URLs, and digests. The
private 1 GiB mount holds scope journals capped at 64 MiB total and quarantine
capped at 256 MiB, 128 pairs, and seven days. Sync reports required TXT as
unresolved on exhausted retries or full quarantine. Receipts from interrupted
runs are reusable only within the same fresh selection and source identity.
A completed run requires a fresh audit on the next reconciliation.

For a new RFC awaiting mirror convergence, allow the queued retries and
check the catalog and mirror status before invoking another full reconciliation.
For loss of a previously accepted required TXT, repeated checksum disagreement,
or exhausted quarantine capacity, keep the incumbent serving and retain the
private observations for investigation. Retry after the sources converge or
the local capacity or corruption problem has been resolved.

<a id="schema-9-release-pipeline"></a>
<a id="bootstrap-qualification"></a>
<a id="pipeline-and-recovery"></a>
<a id="txt-provenance-and-reuse"></a>

## Monitor Sync

`status` summarizes live progress, pending and retrying jobs, the most
recent terminal job, publication, and the oldest pending notification.
`watch` follows daemon-owned runs, including manual requests handled by
the daemon. It emits a snapshot on startup, when state changes, and every
30 seconds if idle. While its container remains available, it reconnects
after a transient socket failure. Use logs for a standalone reconciliation.

`diagnose` reads the same observations and checks local startup
prerequisites. Findings include a stable code, severity, evidence, and
suggested action. The daemon may be stopped while PostgreSQL supplies
durable records. Local checks inspect the configuration currently mounted
for the command. A running daemon uses the settings it loaded at startup.
The commands make no upstream or Serve requests and leave jobs, mounts,
and publication state untouched.

All three commands default to person-readable output. With `--json`,
`status` and `diagnose` write one JSON object, and `watch` writes one object
per line. Each includes `schema_version: 1`, the sample time in Unix epoch
milliseconds, and separate `live` and `database` states. An unavailable
source has null data. `watch` marks the database source as `not_requested`.
Treat the two sample times independently because live progress can advance
while PostgreSQL is being read.

| Command | Exit status |
|---|---|
| `status` | `0` when it can read either source, `1` when both sources are unavailable |
| `watch` | `0` on clean interruption or pipe closure, `1` on an access or protocol error |
| `diagnose` | `0` with informational findings or warnings, `1` when any finding is an error |

Invalid monitoring arguments exit `2`. A retry deadline is informational.
An older job may lack a stored reason. Look up the job ID in the Sync logs
in that case.

Structured logs record lifecycle transitions, progress, retries, audit,
publication, and notification delivery. Set `RFC_SYNC_LOG_LEVEL` to
`error`, `warn`, `info` (the default), or `debug`. Debug adds per-RFC
HTTPS and repair outcomes and timings, plus database and rsync audit batch
details. Use the run ID to correlate one attempt, the job ID to correlate
retries, and the publication sequence to follow delivery. The daemon's
terminal `acquisition` log includes the typed inventory reason and line
number on a rejection. The explicit reconciliation command continues to
return its operator report, which can include a bounded preview.
[Compose usage](deployment.md#operate-and-recover)

## Historical qualification

See the [pinned schema 9 ingestion guide](https://github.com/PublicData-stream/rfc-mcp-docker/blob/376fdc65c37c331ea433ebdb57ef70e7afbfdfbe/docs/ingestion.md)
for the retired acquisition profiles and their benchmark commands. The
[HTTPS-first timing decision](decisions/0010-https-first-rfc-synchronization.md#cold-start-timing)
describes the current production pacing constraint.

## Source rights

Sync derives operation and range rights from verified originals and RFC Index
metadata during candidate assembly. The evidence is bound to the TXT digest,
generation metadata identity, and the policy revision. Reclassification
reuses local verified bytes. Source verification and the HTTPS-first audit
govern publication. [Source policy](source-policy.md)
