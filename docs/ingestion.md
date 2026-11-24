# RFC discovery and candidate ingestion

The HTTPS-first Phase 1 build performs discovery on demand and through the
Sync daemon. Both paths use the same source policy under the corpus lock.
The existing schema 9 release continues to supply production ingestion
until required HTTPS TXT and audit evidence can gate new publications.
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

The accepted-byte limit covers both catalog captures and local canonical
TXT rehashes. The ten-minute discovery deadline applies independently.
Transient catalog fetch errors use up to three total attempts. A valid
`Retry-After` from a 429 or 503 delays subsequent Sync HTTPS starts. Delays
above 30 seconds end the immediate retry and remain in effect for the
daemon's later catalog work.

The remaining tables in the example reserve and validate capacity for
subsequent phases. Defaults are the initial upper limits for queue sizes,
active documents, workers, payload and parse budgets, sessions, timeouts,
and batch sizes. The queue sizes and budgets must cover the active window
at 16 MiB raw and 128 MiB parse bytes per document. The documented 900-second
target is informational. Set one increasing backoff of 1–120 seconds per
rsync retry. For one total attempt, set `retry_backoff_seconds = []`.
Revalidation cadence accepts 3600–604800 seconds.

Run `rfc-mcp-docker-sync --check-config` for the effective settings, the
publication gate, and the list of settings awaiting runtime consumers. Its
JSON includes the 8 MiB scratch reservation and the memory estimate, which
includes workspace capacity, two catalog bodies, inventory buffers, raw and
parse budgets, and compact queue descriptors. Available scratch and memory
are capped at 512 MiB and 2 GiB and tightened by the observed filesystem,
host, and cgroup limits. Sync uses its startup snapshot until restart.

### Translate v1 settings

Start with the v2 example. Preserve explicit values for `max_accepted_bytes`,
`max_in_flight_requests`, `min_request_start_interval_ms`,
`request_timeout_seconds`, and `parser_workers` within the v2 bounds.
Rename `rfcxml_max_attempts` to `max_attempts` and
`max_pending_documents` to `max_active_documents`. Set
`RFC_SYNC_HTTP_CONCURRENCY` and `RFC_SYNC_PARSE_CONCURRENCY` values in
their TOML equivalents and remove both old environment variables. Remove
`txt_provenance`, `[ingestion.rsync]`, and the legacy bulk overlay.
Choose inventory and future audit timeouts from their individual v2 fields.
Keep the v1 file and its matched images for the existing deployment.

## Run discovery

Use a disposable project with the private upstream fixture for qualification.
The existing CLI names select full or sample discovery.

~~~sh
docker compose run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --full
docker compose run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --sample
~~~

Discovery selects the complete expected TXT set from the official opening
catalog. Sample runs select RFCs 2119, 8174, and 9110. The fixed TXT rsync
module supplies root-level availability through a bounded listing. Source
sizes and times are hints. Sync rehashes selected incumbent originals before
reporting them as potential reuse inputs for later audit. A newly expected
TXT that has yet to appear on the mirror defers discovery. Loss of an
already accepted required RFC, a malformed inventory, a corrupt canonical
blob, and unequal opening and ending catalog bytes fail discovery.

The terminal JSON gives the state, scope, active publication sequence,
catalog observations, inventory digest and counts, and selection identity
when sealed. `discovery_deferred` and `discovery_failed` return nonzero from
the manual command. A successful discovery leaves candidates, the active
pointer, source observations, and the outbox as they were. Explicit
publication is gated. Existing retained rollback remains available.
[Deployment](deployment.md)

## Schema 9 release pipeline

The following sections describe the deployed schema 9 collector and its
historical qualification. Use the corresponding release checkout for its
configuration and runtime commands.

## Pipeline and recovery

Bulk mode runs one rsync session for the selected RFCs. It keeps full and sample working trees on a separate capped filesystem. Sync checks the rsync receipt and the complete inventory before admitting TXT files. Subsequent sessions use the existing files as checksum-verified transfer bases. A failed or interrupted session can leave some files updated. A retry must finish a fresh rsync session and validate the whole selection before using those bytes. [Schema 9 bulk mount provisioning](deployment.md#bulk-rsync-staging)

```mermaid
flowchart LR
    A[Initial catalog and selected RFC IDs] --> B[Private rsync staging]
    B --> C[TXT verification and parsing]
    A --> D[Independent RFCXML HTTPS queue]
    C --> T[Durable TXT and sealed index]
    D --> X[RFCXML parsing and outcome]
    C --> X
    T --> E[Membership, evidence and coverage by RFC ID]
    X --> E
    E --> F[End catalog digest, canonical manifest and candidate validation]
    F --> G[Ready candidate]
    C --> H[Quarantine on byte disagreement]
```

In batched mode, Sync stages at most 32 TXT files of up to 4 MiB each and starts the next batch after removing the previous staging directory. In both modes RFCXML fills the pending-document window immediately after catalog selection, even during rsync. Parsed TXT can be installed and indexed while its XML request is pending. Each completed RFC frees its own slot, so a later RFC can finish and admit more work while an earlier request is slow. The default three slots reserve at most 48 MiB for raw payloads, including one RFCXML body per slot. Each document shares a 128 MiB application parse budget between TXT and RFCXML, with the existing XML node and work limits. Catalog processing runs outside the document window and has a 20,000-entry cap.

- The shared HTTPS start gate spaces catalog, TXT, and RFCXML dispatches by the configured interval even when two responses overlap. Hourly catalog probes and reconciliations sharing the mirror also share the corpus fence. Each new lock holder waits for the configured interval before starting HTTPS.
- RFCXML retries timeouts, interrupted transfers, and HTTP 429, 500, 502, 503, and 504. The default allows two retries with jittered one- and two-second backoffs. A valid `Retry-After` on RFCXML 429 or 503 delays new HTTPS starts for all sources in that run. Values above 30 seconds end retries for that RFC and keep the shared delay, subject to the run deadline.
- The reconciliation report's `rfcxml` counters distinguish valid captures, 404, 410, malformed responses that passed HTTPS body validation, exhausted retries, and terminal fetch failures. The legacy `xml_unavailable` count still includes 404, 410, and fetch failures. HTTPS evidence for malformed XML preserves the captured bytes. Only valid parsed results supply RFCXML relations. A 404 or 410 uses normal TXT fallback. Failure or malformed XML caps otherwise complete fallback reference coverage at partial. Metrics include dispatched retries, peak lookahead, reserved bytes, and shared HTTPS concurrency and start spacing.
- One writer seals TXT indexes as results arrive. A second transaction inserts the immutable membership row, RFCXML observation when available, relation evidence and coverage for the same generation, starting catalog, and RFC. The manifest sorts its inputs canonically before the candidate becomes ready.
- On fatal failure, timeout, or SIGTERM, Sync stops admission, cancels HTTPS work and retry waits, reaps rsync, and joins parsing and any active blob install before releasing the corpus fence. It awaits the result of an already submitted readiness commit. Recovery reclaims early sealed indexes and blobs without surviving generation references. [Publication and recovery](publication.md)
- To reduce overlap, set both concurrency settings to `1` in TOML. For policy or schema rollback, follow the [deployment upgrade procedure](deployment.md#initialize-or-upgrade).

### TXT provenance and reuse

The default `dual_source` policy checks new TXT against the HTTPS body. A full ready, published, or retained baseline may supply a prior HTTPS-verified observation when the staged digest and full canonical bytes agree. A missing canonical file requires fresh HTTPS verification. A conflicting canonical file fails the run for investigation.

New byte matches bind separate rsync and HTTPS observations for the same RFC and BLAKE3 digest. The HTTPS observation remains the TXT citation and points to the matching rsync observation. The stored verification method distinguishes `dual_source`, `legacy_https_verified`, `rsync_integrity` and `https_capture`. Legacy records retain their original IDs and timestamps. HTTPS capture describes transport even when an RFCXML body cannot be parsed. The reconciliation report counts fresh matches, reused dual-source or legacy proofs, and mismatches separately. Disagreeing originals are kept in private quarantine.

With `persistent_bulk`, an operator can opt in to `rsync_bootstrap`. A full run at publication sequence zero then accepts TXT from the official rsync module with local checksum and CAS integrity checks. The rsync transport lacks the authentication supplied by HTTPS. The stored observation records the rsync URL and marks HTTPS verification as `unverified`. Its fetch time is the successful session completion for a transferred file. A checksum-confirmed cache hit has a new observation time and a null fetch time. Sample runs remain strict.

After the first publication, `rsync_bootstrap` requires an HTTPS comparison for new or changed TXT. An unchanged TXT can reuse its active generation observation only after rechecking the staged bytes against the canonical blob and its digest. A candidate prepared before a different first publication must still pass the active provenance check at promotion. The report counts HTTPS-verified and rsync-only TXT separately. The reused count includes both kinds of prior proof.

The rsync metrics report session count, transferred file count, checksum hits and maximum validated inventory bytes. On a warm bulk run the validated inventory includes files whose content rsync confirmed without transferring.

### Reconciliation progress

Sync emits an aggregate `ingestion_progress` record every 60 seconds during an active generation. The generation ID connects these records to the terminal candidate report. Daemon runs also include the job ID, which links a candidate to the separate `publication` record. A `ready` result means the candidate was committed. A successful `publication` records the expected and committed sequences. A failed promotion reports the failed attempt while the database remains the source of truth for candidate state and commit outcome.

TXT completion counts durably sealed TXT. XML completion counts committed membership with a classified XML outcome, including recorded unavailability and errors. Remaining counts describe unfinished selected work. Queue counts describe admitted documents and reserved XML slots, with high-water marks in the terminal metrics. On failure, the counts retain the point where work stopped. The report separates HTTPS body bytes read, logical TXT bytes in successfully transferred rsync files, validated rsync inventory size, and accepted input bytes. CPU and memory peaks are process or container measurements. The timing fixture additionally samples disk allocations for the mirror, PostgreSQL and bulk staging.

### Interrupted reconciliation

```mermaid
flowchart LR
    A[Staged TXT] --> B[Eligible TXT proof]
    B --> C[Canonical install and rehash]
    C --> D[Successful parse and committed provenance]
    D --> E[Bounded restart checkpoint]
    D --> F[Building candidate]
    F --> G[Ready]
    F --> H[Failed and recovered normally]
    E --> I[Eviction releases blob reference]
```

New verified TXT and successfully parsed RFCXML get their own committed checkpoints. Each checkpoint pins the original observation and canonical bytes independently of candidate state. A retry creates a new candidate, fetches the current catalog, and matches its exact digest, scope, effective TXT policy, publication sequence, parser and index versions, and registry version. A catalog mismatch at the end invalidates the attempt's checkpoints. Successful readiness clears them in the same transaction as the manifest.

Sync still stages the selected TXT on retry. It uses a completed-generation baseline first where the policy permits, and only reuses a checkpoint when the staged and canonical bytes agree. It reparses TXT with the current parser. Sync rehashes and reparses cached RFCXML and uses the original observation and timestamps. Reports count these as `checkpoint_txt` and `rfcxml.reused`. The `rfcxml.captured` counter counts fresh valid captures. An expired RFCXML checkpoint is fetched through the regular HTTPS queue, where a fresh 404 or 410 uses normal TXT fallback. Parse errors and failed requests never produce a checkpoint. A missing canonical file can be reacquired. A conflicting canonical file invalidates the checkpoint and stops the attempt for repair.

The cache admits up to 40,000 source records and 4 GiB of recorded body bytes across full and sample runs. Each record counts its length against the budget even when bytes are shared. TXT entries expire seven days after checkpoint creation, and RFCXML entries expire 24 hours after the original HTTPS fetch. Reuse keeps those deadlines. Sync removes expired entries and evicts the oldest remaining entries when space is needed. A crash before a checkpoint commits leaves the newly installed file eligible for ordinary GC.

## Bootstrap qualification

The production template uses strict TXT verification and batched rsync. To opt in to persistent bulk staging and accept rsync-only TXT evidence at sequence zero, set `ingestion.txt_provenance = "rsync_bootstrap"` and `ingestion.rsync.mode = "persistent_bulk"`. Use the dedicated staging filesystem, then run `--check-config`. Use the [deployment procedure](deployment.md#first-publication) to check the active generation, pinned MCP read, and outbox. [Return to strict batched ingestion](deployment.md#return-to-strict-batched-ingestion)

The 900-second objective measures a cold initialization from the first volume ownership step through publication, `/ready` and a byte-exact MCP read. Record authenticated notification delivery and image preparation separately. A planning corpus of 9,861 RFCs needs 9,863 HTTPS starts for XML and the two catalog captures. With the one-second minimum, their first-to-last start window alone is at least 9,862 seconds (2 h 44 min 22 s). Strict cold TXT verification increases the floor to 19,723 seconds. Private accelerated measurements describe the local collector. The production objective is infeasible at current pacing. [Timing contract and rate calculation](decisions/0009-cold-rfc-bootstrap.md)

Load matching native service and fixture images, including the publication and control fixtures, into the target Docker context. Use the pinned PostgreSQL image from the fixture Compose file. These commands use the private upstream and a new evidence directory for each profile.

```sh
evidence=$(mktemp -d /tmp/rfc-dual-qualify.XXXXXXXX)
for profile in quick paced full rollback; do
    python3 -B scripts/bootstrap/baseline.py --phase8 "$profile" --context default \
        --image-prefix "$IMAGE_PREFIX" --evidence "$evidence/$profile"
done
python3 -B scripts/bootstrap/image-cache.py --context default \
    --image-prefix "$IMAGE_PREFIX" --evidence "$evidence/image-cache"
python3 -B scripts/phase2/integration.py --context default \
    --image-prefix "$IMAGE_PREFIX" --benchmark 96 --repeats 3 \
    --serial-image-prefix "$SERIAL_IMAGE_PREFIX" --receipt "$evidence/serial.json"
```

The paced, full and rollback profiles need a host that permits the disposable ext4 mount under `/tmp` and the associated disk sampling. The full profile runs three cold and incremental pairs. Run it on an otherwise idle host. Incremental MCP timing uses the byte-reviewed RFC 2119 fixture at the new sequence. The harness separately checks the MCP source-policy response and verifies the changed synthetic original against its canonical bytes. The image-cache probe uses isolated Docker and containerd stores and sockets under `/tmp` for a checksum-verified offline import. The historical serial prefix uses revision `e2e5a41` and its matching Sync and ingestion-fixture images. [Observed results and prerequisites](implementation-status.md)

## Reading candidate metadata

- Manifest format 2 extends format 1 with the scope, the two catalog observation identities, the policy registry version, and catalog entries without TXT. A generation records the exact publication date or its available month precision.
- The document version stores LF-based line starts and section coverage. Section IDs are scoped to the document version. Raw headings, section bodies, and notices are half-open original byte ranges. Use the original blob to reconstruct matches and snippets.
- PostgreSQL chunks hold consecutive raw UTF-8 slices of at most 16 KiB. Document lexemes come from the intact TXT under pg_catalog.simple, so terms in separate chunks and terms split by a chunk boundary remain searchable together.
- Accepted TXT with invalid UTF-8 or a NUL byte is stored byte for byte in bytes mode. It retains LF line offsets, with text indexing and parsed TXT sections, notices, and references unavailable. Catalog metadata and independently verified RFCXML evidence are still available. The reconciliation report counts `invalid_utf8` and `contains_nul` independently. A body with both conditions contributes to both counters.
- Catalog and RFCXML evidence locators encode half-open byte ranges in their respective XML observation blobs. TXT evidence uses original TXT offsets. Global reference coverage is partial where sources are excluded, unavailable, or incomplete.
- A complete notice extraction describes the parser's result. Redistribution review comes from the [notice registry](../crates/sync/src/policy.json). The reviewed fixture digests permit whole-document reproduction. Other documents default to metadata-only policy.

## Repeat verification

~~~sh
scripts/phase0/check-rust.sh
python3 scripts/phase0/integration.py --project rfc-phase2-regression
python3 scripts/phase1/integration.py --image-prefix rfc-phase2-regression
python3 scripts/phase2/integration.py
~~~

The fixture profile uses synthetic sources and the two reviewed RFC originals. The benchmark uses disposable projects and fixed fixture observation timestamps. Compare with `--benchmark 6 96 --http-concurrency 1` and again with `--http-concurrency 2`. Use `--receipt /tmp/benchmark.json` for the per-run results and `--reference-receipt /tmp/serial.json` for comparison with a reference that uses the same observation identities. The fixture pace matches the official dispatch interval by default. Run `--live` only with explicit upstream authorization.

Use Phase 2's `--bulk` for strict cold and warm bulk reconciliation and `--bulk-bootstrap` for rsync-only provenance, faults and promotion. Phase 6's `--bulk-bootstrap` exercises daemon publication and an MCP read. These offline profiles require `mkfs.ext4` and permission to mount a disposable loop filesystem under `/tmp`. For the populated schema 7 upgrade drill, pass `--upgrade-from-schema7` with the prefix of prebuilt schema 7 Sync and ingestion fixture images. Phase 3's `--checkpoints` runs the restart, expiry, quota and corruption probes.
