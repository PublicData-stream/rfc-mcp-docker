# Phase 2 candidate ingestion

Provision the mirror and PostgreSQL 19 roles using the [storage instructions](storage.md). Run the sync image as UID 10002 in Docker Compose with the writable mirror, the scoped sync credentials, and outbound access to RFC Editor over HTTPS and rsync. Compose gives sync a private 512 MiB tmpfs at `/tmp`, a 2 GiB memory limit, and two CPUs. Size the host for the sync cgroup, including its tmpfs use. [Disposable Compose live profile](../scripts/phase2/live.yml)

Use these commands in a Compose project configured with the sync service.

~~~sh
docker compose run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --full
docker compose run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --sample
~~~

Full mode reconciles the expected TXT set from the RFC Editor catalog. Sample mode uses RFCs 2119, 8174, and 9110 and records its restricted scope. A missing expected TXT, a lost previously accepted RFC, a byte disagreement, or catalog drift fails the run. The JSON report identifies the failed input when available and includes a generation ID, digest, counts, and coverage warnings. For a disagreement, sync saves both originals and their digests in /mirror/quarantine. Only sync can read that directory. An abrupt interruption can leave a building generation and durable blobs for Phase 3 recovery.

The collector bounds requests, disk staging, and accepted input. HTTPS requires the RFC Editor hostname and native trust roots. The collector rejects redirects and encoded bodies. Rsync uses catalog-derived filenames and transfers into a fresh private directory. A previous verified TXT observation is eligible for reuse when the staged bytes and the canonical blob match its digest.

## Sync configuration

Production Compose mounts the [ingestion TOML](../deploy/ingestion.toml.example) read-only for Sync, including one-shot commands. For direct invocations, `RFC_SYNC_CONFIG_FILE` selects the file. Each file requires `schema_version = 1`. Omitted settings use the defaults below. The file must be regular UTF-8 and at most 16 KiB. Sync rejects unknown keys and invalid settings before connecting or enqueuing work.

| Key | Default | Range |
| --- | ---: | ---: |
| `ingestion.https.max_in_flight_requests` | 2 | 1–2 |
| `ingestion.https.min_request_start_interval_ms` | 1000 | 1000–60000 |
| `ingestion.https.request_timeout_seconds` | 30 | 5–30 |
| `ingestion.https.rfcxml_max_attempts` | 3 | 1–3 |
| `ingestion.rsync.mode` | `batched` | `batched`, `persistent_bulk` |
| `ingestion.rsync.batch_size` | 32 | 1–32 |
| `ingestion.rsync.transfer_timeout_seconds` | 120 | 30–120 |
| `ingestion.rsync.bulk_transfer_timeout_seconds` | 900 | 30–900 |
| `ingestion.pipeline.max_pending_documents` | 3 | 1–3 |
| `ingestion.pipeline.parser_workers` | 1 | 1–2 |
| `ingestion.max_accepted_bytes` | 4294967296 | 1–4294967296 |
| `ingestion.txt_provenance` | `dual_source` | `dual_source`, `rsync_bootstrap` |

The pending-document window must fit both concurrency settings. The HTTPS request timeout includes the fixed five-second connect timeout. The rsync timeouts apply to one batch or one bulk session, each with a fixed 30-second inactivity timeout. The accepted-byte cap is cumulative across the two catalog captures, accepted TXT and captured RFCXML. The 12-hour run deadline applies independently.

Run `rfc-mcp-docker-sync --check-config` to see the effective settings and resource reservations as JSON. The check uses the same startup validation without contacting PostgreSQL or upstream. Sync reserves 8 MiB for scratch and another 4 MiB per batch entry in batched mode. Its RAM estimate includes that scratch, 144 MiB per pending document, and 512 MiB of working memory. The 512 MiB scratch and 2 GiB RAM envelope is tightened by available space on `/tmp`, physical memory, and visible cgroup v2 memory limits. Run the check inside the target container to measure its limits. Sync uses the validated snapshot until restart.

When `RFC_SYNC_CONFIG_FILE` is unset, `RFC_SYNC_HTTP_CONCURRENCY` and `RFC_SYNC_PARSE_CONCURRENCY` are accepted for compatibility. Both accept `1` or `2`. Setting either alongside `RFC_SYNC_CONFIG_FILE` fails validation. [Compose provisioning and preflight](deployment.md#provision)

## Pipeline and recovery

Bulk mode runs one rsync session for the selected RFCs. It keeps full and sample working trees on a separate capped filesystem. Sync checks the rsync receipt and the complete inventory before admitting TXT files. Subsequent sessions use the existing files as checksum-verified transfer bases. A failed or interrupted session can leave some files updated. A retry must finish a fresh rsync session and validate the whole selection before using those bytes. [Bulk mount provisioning](deployment.md#bulk-rsync-staging)

```mermaid
flowchart LR
    A[Initial catalog and selected RFC IDs] --> B[Private rsync staging]
    B --> C[TXT verification and parsing]
    A --> D[Independent RFCXML HTTPS queue]
    C --> J[Join and parse XML by RFC ID]
    D --> J
    J --> E[Ordered blob and database commits]
    E --> F[End catalog digest and candidate validation]
    F --> G[Ready candidate]
    C --> H[Quarantine on byte disagreement]
```

In batched mode, Sync stages at most 32 TXT files of up to 4 MiB each and starts the next batch after removing the previous staging directory. In both modes RFCXML fills the pending-document window immediately after catalog selection, even during rsync. TXT verification and parsing can proceed while its RFCXML request is pending. The window refills as each RFC commits. The default three slots reserve at most 48 MiB for raw payloads, including one RFCXML body per slot. Each document shares a 128 MiB application parse budget between TXT and RFCXML, with the existing XML node and work limits. Catalog processing runs outside the document window and has a 20,000-entry cap.

- The shared HTTPS start gate spaces catalog, TXT, and RFCXML dispatches by the configured interval even when two responses overlap. Hourly catalog probes and reconciliations sharing the mirror also share the corpus fence. Each new lock holder waits for the configured interval before starting HTTPS.
- RFCXML retries timeouts, interrupted transfers, and HTTP 429, 500, 502, 503, and 504. The default allows two retries with jittered one- and two-second backoffs. A valid `Retry-After` on RFCXML 429 or 503 delays new HTTPS starts for all sources in that run. Values above 30 seconds end retries for that RFC and keep the shared delay, subject to the run deadline.
- The reconciliation report's `rfcxml` counters distinguish valid captures, 404, 410, malformed responses that passed HTTPS body validation, exhausted retries, and terminal fetch failures. The legacy `xml_unavailable` count still includes 404, 410, and fetch failures. HTTPS evidence for malformed XML preserves the captured bytes. Only valid parsed results supply RFCXML relations. A 404 or 410 uses normal TXT fallback. Failure or malformed XML caps otherwise complete fallback reference coverage at partial. Metrics include dispatched retries, peak lookahead, reserved bytes, and shared HTTPS concurrency and start spacing.
- The writer commits in ascending RFC order and ends each document transaction before taking the next prepared result. One supervised blocking task at a time performs blob installation and fsync so transfers can continue while the writer waits.
- On fatal failure, timeout, or SIGTERM, Sync stops admission, cancels HTTPS work and retry waits, reaps rsync, and joins parsing and any active blob install before releasing the corpus fence. Recovery handles unfinished generations and orphaned blobs after an abrupt interruption. [Publication and recovery](publication.md)
- To reduce overlap, set both concurrency settings to `1` in TOML. For policy or schema rollback, follow the [deployment upgrade procedure](deployment.md#initialize-or-upgrade).

### TXT provenance and reuse

The default `dual_source` policy checks new TXT against the HTTPS body. A full ready, published, or retained baseline may supply a prior HTTPS-verified observation when the staged digest and full canonical bytes agree. A missing canonical file requires fresh HTTPS verification. A conflicting canonical file fails the run for investigation.

With `persistent_bulk`, an operator can opt in to `rsync_bootstrap`. A full run at publication sequence zero then accepts TXT from the official rsync module with local checksum and CAS integrity checks. The rsync transport lacks the authentication supplied by HTTPS. The stored observation records the rsync URL and marks HTTPS verification as `unverified`. Its fetch time is the successful session completion for a transferred file. A checksum-confirmed cache hit has a new observation time and a null fetch time. Sample runs remain strict.

After the first publication, `rsync_bootstrap` requires an HTTPS comparison for new or changed TXT. An unchanged TXT can reuse its active generation observation only after rechecking the staged bytes against the canonical blob and its digest. A candidate prepared before a different first publication must still pass the active provenance check at promotion. The report counts HTTPS-verified and rsync-only TXT separately. The reused count includes both kinds of prior proof.

The rsync metrics report session count, transferred file count, checksum hits and maximum validated inventory bytes. On a warm bulk run the validated inventory includes files whose content rsync confirmed without transferring.

A future checkpoint cache could retain eligible proofs independently of a generation.

```mermaid
flowchart LR
    A[Staged TXT] --> B[Eligible TXT proof]
    B --> C[Canonical install and rehash]
    C --> D[Successful parse and committed provenance]
    D --> E[Independent reusable proof]
    D --> F[Building candidate]
    F --> G[Ready]
    F --> H[Failed and recovered normally]
    E --> I[Eviction releases blob reference]
```

A proof would pin its canonical blob through a reference independent of candidate state. To reuse it, check current catalog membership and staged bytes, revalidate the blob, and rebuild indexes with the current parser. Normal baseline reuse takes precedence. A proposed starting policy is seven days from verification, 4,096 proofs, and 512 MiB of uniquely referenced TXT. Expire old proofs, then evict least recently used proofs to satisfy both limits. A future additive migration would make proof insertion and removal atomic with the provenance and deletion queue under the coordinator fence. A crash before proof commit leaves the blob eligible for normal recovery. The current implementation uses full-generation baseline reuse.

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

The fixture profile uses synthetic sources and the two reviewed RFC originals. The benchmark uses disposable projects and fixed fixture observation timestamps. Compare with `--benchmark 6 96 --http-concurrency 1` and again with `--http-concurrency 2`. Use `--receipt /tmp/benchmark.json` for the per-run results and `--reference-receipt /tmp/serial.json` for comparison against the instrumented serial revision. The fixture pace matches the official dispatch interval by default. Run `--live` only with explicit upstream authorization.

Use Phase 2's `--bulk` for strict cold and warm bulk reconciliation and `--bulk-bootstrap` for rsync-only provenance, faults and promotion. Phase 6's `--bulk-bootstrap` exercises daemon publication and an MCP read. These offline profiles require `mkfs.ext4` and permission to mount a disposable loop filesystem under `/tmp`. For the published-data upgrade drill, pass `--upgrade-from` with the prefix of prebuilt schema 6 Sync and ingestion fixture images.
