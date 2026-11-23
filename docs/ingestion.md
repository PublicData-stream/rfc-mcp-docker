# Phase 2 candidate ingestion

Provision the mirror and PostgreSQL 19 roles using the [storage instructions](storage.md). Run the sync image as UID 10002 in Docker Compose with the writable mirror, the scoped sync credentials, and outbound access to RFC Editor over HTTPS and rsync. Use a private 144 MiB tmpfs at /tmp for transfer batches. [Disposable Compose live profile](../scripts/phase2/live.yml)

Use these commands in a Compose project configured with the sync service.

~~~sh
docker compose run --rm --no-deps sync --phase2-reconcile --full
docker compose run --rm --no-deps sync --phase2-reconcile --sample
~~~

Full mode reconciles the expected TXT set from the RFC Editor catalog. Sample mode uses RFCs 2119, 8174, and 9110 and records its restricted scope. A missing expected TXT, a lost previously accepted RFC, a byte disagreement, or catalog drift fails the run. The JSON report identifies the failed input when available and includes a generation ID, digest, counts, and coverage warnings. For a disagreement, sync saves both originals and their digests in /mirror/quarantine. Only sync can read that directory. An abrupt interruption can leave a building generation and durable blobs for Phase 3 recovery.

The collector bounds requests, disk staging, and accepted input. HTTPS requires the RFC Editor hostname and native trust roots. The collector rejects redirects and encoded bodies. Rsync uses catalog-derived filenames and transfers into a fresh private directory. A previous verified TXT observation is eligible for reuse when the staged bytes and the canonical blob match its digest.

## Reading candidate metadata

- Manifest format 2 extends format 1 with the scope, the two catalog observation identities, the policy registry version, and catalog entries without TXT. A generation records the exact publication date or its available month precision.
- The document version stores LF-based line starts and section coverage. Section IDs are scoped to the document version. Raw headings, section bodies, and notices are half-open original byte ranges. Use the original blob to reconstruct matches and snippets.
- PostgreSQL chunks hold consecutive raw UTF-8 slices of at most 16 KiB. Document lexemes come from the intact TXT under pg_catalog.simple, so terms in separate chunks and terms split by a chunk boundary remain searchable together.
- Verified TXT with invalid UTF-8 or a NUL byte is stored byte for byte in bytes mode. It retains LF line offsets, with text indexing and parsed TXT sections, notices, and references unavailable. Catalog metadata and independently verified RFCXML evidence are still available. The reconciliation report counts `invalid_utf8` and `contains_nul` independently. A body with both conditions contributes to both counters.
- Catalog and RFCXML evidence locators encode half-open byte ranges in their respective XML observation blobs. TXT evidence uses original TXT offsets. Global reference coverage is partial where sources are excluded, unavailable, or incomplete.
- A complete notice extraction describes the parser's result. Redistribution review comes from the [notice registry](../crates/sync/src/policy.json). The reviewed fixture digests permit whole-document reproduction. Other documents default to metadata-only policy.

## Repeat verification

~~~sh
scripts/phase0/check-rust.sh
python3 scripts/phase0/integration.py --project rfc-phase2-regression
python3 scripts/phase1/integration.py --image-prefix rfc-phase2-regression
python3 scripts/phase2/integration.py
python3 scripts/phase2/integration.py --live
~~~

The fixture profile uses synthetic sources and the two reviewed RFC originals. The live profile contacts the official endpoints and builds a scoped candidate in disposable volumes. Both profiles keep temporary data under /tmp.
