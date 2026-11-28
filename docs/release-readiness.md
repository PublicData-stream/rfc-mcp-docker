# Phase 8 release qualification

Qualify the exact Serve, Sync, Feed, and Maintenance images together with
the pinned PostgreSQL image on a Linux rootless Docker Compose host.
Verify cgroup v2 and the effective sandbox. Synthetic corpus and fault
fixtures use disposable projects and a localhost HTTPS proxy.
[Schema 13 startup verification](implementation-status.md#automatic-database-migration-at-startup)

## Repeat the checks

Install the [deployment prerequisites](deployment-prerequisites.md), Python 3 with `jsonschema`, OpenSSL, curl, and actionlint. Prefetch the locked Cargo dependencies. Docker may need registry access to resolve or fetch pinned build inputs. The fixture qualification uses private synthetic upstreams.

```sh
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp
export RFC_PHASE8_IMAGE_PREFIX=rfc-phase8-qualified
cargo fetch --locked
bash scripts/phase8/verify.sh
bash scripts/phase8/audit.sh /tmp/rfc-phase8-audit
```

To check live endpoint assumptions, run `python3 -B scripts/phase0/upstream.py`
separately. For an explicitly authorized live ingestion sample, use the
[standard Compose sample command](ingestion.md#run-acquisition) with the
daemon stopped and a bounded official-source window.

Use a new audit output directory for every run. The audit verifies the scanner archive against its pinned SHA-256 and retrieves the advisory database. It records the four candidate image IDs and the separate Compose PostgreSQL image ID, per-image CycloneDX SBOMs, resolved Rust dependency and license inventories, and advisory, license, and redacted secret results. Use `RFC_PHASE8_CONTEXT` to select the Docker context and `RFC_PHASE8_TRIVY_ARCHIVE` to reuse a downloaded scanner archive. The manual [GitHub Actions audit job](../.github/workflows/release-audit.yml) builds the four candidate images and exports the same redacted evidence. Scanner completion means the reports were generated. Review findings before assigning a release status.

The offline runner builds Maintenance alongside the Phase 0 service
images and uses that image set for migration, restore, client, and capacity
probes. It prints its report directory under `/tmp`. Package archives,
database dumps, mirror copies, synthetic credentials, and captures stay
under `/tmp`.

## Restore and client results

The Phase 8 rehearsal backs up a stack with an active, a retained, and a ready generation. It destroys the disposable source stack and its backing directories before provisioning the distinct restore stack. The destination validates the independent archive, reconstructs the generation manifests and fences, checks persisted byte coordinates, and then replays all five public tool reads over HTTPS. The same drill checks missing and same-length damaged backup files before starting the proxy. [Operator backup and restore](publication.md#backup-and-restore)

In the 2026-11-23 rehearsal, the fresh destination restored active generation 2 at publication sequence 2, retained generation 1, and ready generation 3. It verified 16 registered blobs by full BLAKE3 and length and checked 794 persisted ranges and line starts. The three reconstructed manifests and the decoded reads matched the baseline. Missing and same-length damaged inputs both stopped archive verification. The machine-readable result is `restore.json` in the Phase 8 evidence directory. [Later schema 13 rehearsal](implementation-status.md#automatic-database-migration-at-startup)

The compatibility checks use Python and curl with a verified HTTPS certificate and per-request MCP metadata. The table in the [MCP API guide](mcp.md#client-compatibility) identifies the tested clients, revision, and rejection behavior.

## Capacity methodology

The 2026-11-23 benchmark used two synthetic corpus sizes, with three warmup seconds and a 15-second measurement window at each concurrency. The mixed workload ran list, exact body search, original retrieval, section listing, and ripgrep through the TLS proxy. Separate test clients used distinct container addresses. The shared-source burst used the host loopback proxy. The distinct-source admission burst held a private PostgreSQL relation lock to keep admitted reads in flight long enough to test the cap.

Bulk fixture entries shared an original and used the metadata-only policy
in effect at the time. The ripgrep case scanned three reviewed synthetic
originals. Charged bytes per second describe those scans through MCP.
The fixture SQL plans used the same pinned PostgreSQL version and corpus
rows used to seed the serving stacks. The measurements apply to that image
set, host, corpus, and workload.

| Synthetic RFCs | Clients | Successful calls/s | HTTP 429 | Typed tool rejections | Search p95, ms | Peak sampled serve MiB | Serve FDs | DB readers |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | 3.7 | 0 | 0 | 240 | 9.0 | 18 | 1 |
| 1,000 | 4 | 14.7 | 0 | 0 | 257 | 8.8 | 24 | 4 |
| 1,000 | 16 | 23.3 | 0 | 55 | 2,050 | 11.6 | 78 | 16 |
| 1,000 | 32 | 28.1 | 690 | 14 | 2,603 | 11.6 | 77 | 16 |
| 10,000 | 1 | 3.7 | 0 | 0 | 257 | 8.7 | 16 | 1 |
| 10,000 | 4 | 14.7 | 0 | 0 | 253 | 9.1 | 24 | 4 |
| 10,000 | 16 | 23.5 | 0 | 46 | 1,990 | 12.0 | 80 | 16 |
| 10,000 | 32 | 25.0 | 743 | 23 | 2,734 | 12.2 | 81 | 16 |

Ripgrep charged 48,604–340,227 original bytes/s across the tiers. PostgreSQL peaked at 308.6 MiB in the mixed workload. The capacity report includes per-tool sample counts and p50/p95/p99 for successful calls, all typed outcomes, sampled memory per service, and the title, body, and relationship SQL plans.

At each corpus size, the 32-call shared-source burst returned 10 successes and 22 HTTP 429s. With distinct sources and the held relation lock, 16 calls received HTTP 429 and the 16 admitted searches returned typed deadlines. Both profiles peaked at the 16-request admission cap. After load, the five-tool listing succeeded, with zero in-flight requests, ripgrep workers, and serving DB readers. The Phase 7 deliberately blocked worker additionally proved both-slot saturation, cancellation, reaping, and lock release. [Capacity evidence](implementation-status.md#phase-8)

### SQL-native search

Run `scripts/phase8/capacity.py --native-search-only --search-baseline-image IMAGE`
with the candidate image prefix and an evidence directory under `/tmp`. The
report compares each image at 1 and 16 clients on the same populated fixtures.
It includes a first query after restarting PostgreSQL, warm per-query
latencies and outcome counts, CPU, memory, pool waits, temporary disk, cleanup,
and SQL plans and statement counts. The restart clears shared buffers. The
host filesystem cache remains warm.

Use the Phase 4 differential probe with `--baseline-search-image IMAGE` to
compare complete cursor chains and measure time to the first nonempty result.
Use the automatic upgrade probe with `--case online` and
`--baseline-serve-image IMAGE` to check the live schema 13 cutover, failed
replacement recovery, and continuation across a rollback. [Search verification](implementation-status.md#postgresql-native-search)

The 10,000-RFC local fixture met the single-client 100 ms p95 goal for
the warm query mix. Five native query shapes exceeded 500 ms p95 at
16 clients. The separate sparse-title cursor-chain case also exceeded
100 ms for its first result. These measurements cover local fixture runs.

## Bounded official-source verification

The Phase 0 HTTPS probe captured seven endpoints between 04:21:00 and 04:21:09 UTC on 2026-11-23. Atom and RSS each yielded 15 entries. The capped catalog prefix contained 261 complete entries. The Phase 2 sample on those images reconciled a ready candidate while the active pointer stayed unset. It recorded the legacy policy labels below, two unavailable RFCXML bodies, one incomplete reference classification, and one partial section index. [Later license-based sample](implementation-status.md#license-based-redistribution-and-whole-corpus-regex)

| Official TXT | Capture time, UTC | Bytes | BLAKE3 | Recorded policy and section coverage |
| --- | --- | ---: | --- | --- |
| [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119.txt) | 04:35:12 | 4,723 | `5f1bbd38bc7f5d4540b85db3aeca5f706d6d18c8a1e27d8c99603bb0f13965aa` | Whole-file only, complete sections |
| [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174.txt) | 04:35:16 | 6,071 | `2ffac1117c206df7d11b6b4917572c298948ce546fd5319b09217b8751a5f2eb` | Whole-file only, complete sections |
| [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.txt) | 04:35:18 | 502,941 | `dc6241aa10da5782c886cfb2e511ae157137cc01b3241fdd4480ee508191edf5` | Metadata only, partial sections |

## Supply-chain review

The 2026-11-23 audit predates the Maintenance image. Trivy 0.76.2 used vulnerability database schema 2, published at 2026-11-23 01:05 UTC. Both Cargo locks had zero matched advisories. The checked-out source and each candidate image had zero secret matches. The four CycloneDX reports contained 17 components for serve, 25 for sync, 17 for feed, and 59 for PostgreSQL. The separate locked Rust dependency and license inventories covered statically linked components. The recorded application and ripgrep lock SHA-256 values were `131194af5397c781fd833dc04e35b673924502690aae503c7bccb241aa429213` and `7a7d39cda8a03930e578f1dbb724e055771901842eca239e03b01e19da946a64`. [Trivy release](https://github.com/aquasecurity/trivy/releases/tag/v0.76.2)

- Each service image reported seven medium and four low package findings in Alpine libraries. Use the image reports to track installed and fixed package versions.
- The PostgreSQL image reported one critical, 26 high, 31 medium, three low, and one unknown finding. It contained `libxml2` `2.13.9-r2`, and the audit recorded Alpine's fix for CVE-2026-86144 in `2.13.9-r3`. At the time, the official `postgres:19.0-alpine3.24` tag resolved to the qualified index digest. An image refresh needs package verification, rescanning, and Compose requalification. [Alpine security database](https://secdb.alpinelinux.org/v3.24/main.json) · [Official PostgreSQL image](https://hub.docker.com/_/postgres)
- The remaining PostgreSQL-image matches came from the bundled `gosu` Go binary, including Go TLS advisory GO-2026-4337. Compose starts PostgreSQL as UID 70, and the pinned image's entrypoint calls `gosu` for root startup. That narrows the normal startup path. Binary-level applicability remains open for release review. [Go advisory](https://pkg.go.dev/vuln/GO-2026-4337) · [Pinned PostgreSQL entrypoint](https://github.com/docker-library/postgres/blob/8672300fdf4f4ce8a8eac7511031fd08b6d2c886/docker-entrypoint.sh)

## Release review

Rows A01–A39 passed in the 2026-11-23 local rootless run. A40 remained
open. Reapply the matrix to the exact candidate image set and include
fresh startup and migration results for the current schema. Each
`phaseN` reference is the corresponding log from `scripts/phase8/verify.sh`.
Test rows for parser, storage, and crash injection include the isolated
fixture builds. [Recorded commands and environment](implementation-status.md#phase-8)

| ID | Check | Observation and evidence |
| --- | --- | --- |
| A01 | Dependency pinning | The frozen Rust build and Phase 0 image build passed. See the recorded toolchain, image IDs, and scanner inventory. |
| A02 | Modern MCP | Discovery, listing, and direct calls passed at the pinned revision in `phase0`, `phase5`, and the HTTPS restore probe. |
| A03 | Protocol rejection | `phase0` and `phase5` rejected version and metadata mismatches, initialization, GET streams, and DELETE sessions. The restore probe repeated legacy rejection with two clients. |
| A04 | Tool catalog | `phase5` checked the exact five-tool order, schema snapshots, read-only annotations, and typed result contracts. |
| A05 | DB authorization | `phase1`, `phase6`, and `phase7` checked scoped grants and denied writes. Feed lacked database credentials and connectivity. |
| A06 | Volume authorization | `phase1` and `phase7` denied serve writes, confirmed intended sync writes, and checked that feed had no mirror mount. |
| A07 | Content identity | `phase1` verified full BLAKE3, concurrent same-digest reuse, and containment of unexpected or corrupt files. |
| A08 | Source integrity | `phase2` verified the fixture HTTPS comparison and quarantine. The live sample verified the three official TXT originals. |
| A09 | Original text | `phase2` and `phase5` compared original bytes against stored ranges and JSON-decoded reads. |
| A10 | Encoding | `phase2` and `phase5` preserved non-UTF-8 bytes and exercised invalid UTF-8 boundaries and explicit encoding errors. |
| A11 | Sections | `phase2` and `phase5` exercised parser edge cases, partial hierarchy, and duplicate heading selection. |
| A12 | Corpus format | A source and dependency search for Markdown conversion in `apps`, `crates`, `Cargo.lock`, and `Dockerfile` returned no matches. Review confirmed official TXT, XML metadata, and original-byte schemas. |
| A13 | Publication durability | `phase3` interrupted file installation, candidate readiness, and publication commits. `phase6` repeated publication failures through daemon entrypoints. |
| A14 | Reader and GC fences | `phase3` verified the post-lock tombstone check and exclusion through reader database loss. `phase7` verified the worker fence through reaping. |
| A15 | Snapshot consistency | `phase3`, `phase5`, and `phase7` exercised generation-guarded metadata, source bytes, notices, and worker reads. |
| A16 | Blob reuse | `phase2` and `phase3` verified reuse during metadata refresh and preservation of shared bytes during GC. |
| A17 | Rollback | `phase3` and `phase5` verified sequence-checked rollback and cursor visibility across generations. |
| A18 | Feed independence | `phase6` exercised startup and daily work with feed down and bounded coalescing after duplicate hints. |
| A19 | Notification loss | `phase6` verified fresh MCP reads through canceled, reordered, and forged hints, outbox recovery, and a serve restart. |
| A20 | DSL syntax | Frozen parser and compiler tests and `phase4` checked precedence, escapes, errors, complexity limits, and deterministic generated inputs. |
| A21 | Exact and token search | `phase4` checked case-sensitive punctuation and literals alongside token semantics. |
| A22 | Chunks | `phase2` and `phase4` verified literal matching across chunk boundaries and token matches across chunks. |
| A23 | Relationship directions | `phase4` compared forward and inverse operators and the legacy alias against an independent graph evaluator. |
| A24 | Unknown evidence | `phase4` tested missing and partial classifications through NOT, AND, OR, and inbound absence. |
| A25 | Query safety | The SQL compiler tests verified bound values. `phase4` exercised timeout and reader cleanup under a blocking lock. |
| A26 | Cursors | `phase4`, `phase5`, and `phase7` verified binding, order, expiry, publication pinning, and refusal after retirement. |
| A27 | HMAC canonicalization | The Rust golden vectors and `phase6` checked original bytes, key direction, paths, headers, and signature verification. |
| A28 | Replay | `phase6` and the Rust auth tests checked duplicate concurrency, epochs, bootstrap rules, time skew, and bounded replay state. |
| A29 | Authenticated responses | `phase6` showed forged identity and acknowledgment traffic left the outbox pending until verified delivery. |
| A30 | Key isolation | `phase6` and `phase7` checked per-service key access. Both receiver-first rotations passed signed overlap, sender switching, and expiry. |
| A31 | Ripgrep arguments | `phase5` and `phase7` checked literal option-like input, default regex restrictions, the pinned binary, and the worker boundary. |
| A32 | Sandbox files | `phase0` and `phase7` denied test secrets, parent process credentials, unselected paths, and writes. |
| A33 | Sandbox network | `phase0` and `phase7` denied new TCP/Unix sockets and inheritance of parent network descriptors. |
| A34 | Worker lifecycle | `phase7` exercised CPU and wall deadlines, slot saturation, failure-closed readiness, disconnect, reaping, and reader release. |
| A35 | Network boundaries | `phase7` checked private control and database paths, disjoint listener and dynamic addresses, and ingress boundaries before and after recreation. |
| A36 | Notices | `phase2` and `phase5` checked reviewed official originals, synthetic exact notices and encoding, and metadata-only policy. The bounded live sample recorded actual restrictions. |
| A37 | Response budgets | `phase5` and `phase7` checked full JSON frame limits, duplicate representations, excerpts, and notices. |
| A38 | Restore | `recovery` verified archive integrity, fresh volumes, retained and ready manifests, exact ranges, and replay of all five MCP reads. |
| A39 | Capacity | `capacity` measured both corpus sizes and verified bounded overload and cleanup. `phase6` observed one running and one coalesced pending job at queue saturation. |
| A40 | Supply chain and docs | **Open for public release.** The audit generated all inventories and found no matching source or image secrets. The pinned PostgreSQL image findings remain unresolved. |

The maintainer confirmed the existing MIT license for the application and project documentation. Original RFC fixtures retain their separate notices and provenance. Review resolved third-party licenses and scan findings before redistributing an image. [MIT license](../LICENSE) · [RFC fixture provenance](../fixtures/rfc/README.md)

Before public activation, repeat the required host checks on the exact release host and confirm ingress TLS, proxy header handling, firewall and egress policy, scoped secrets, and restore access. Qualify the live corpus separately and retain its explicit policy and coverage restrictions. Tags, registry publication, and public deployment are separate authorized operations.
