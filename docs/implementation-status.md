# Implementation status

## Phase 0

**Verified locally on rootless Linux amd64 on 2026-11-22.** The application entrypoints provide Phase 0 probes and version reporting. Storage, ingestion, publication, the public tool catalog, and internal HMAC are later phases.

### Environment and pins

Validation host was Linux amd64, kernel `7.0.0-44-generic`, rootless Docker Engine `29.10.0`, Docker Compose `v5.8.0`. Docker reported its built-in seccomp profile. Rust `1.99.1` reported compiler commit `58cf8f9c3`. Cargo output was under `/tmp/rust/target`. Runtime test mounts, keys, and captures used private directories under `/tmp`.

| Input | Index or package pin | Linux amd64 manifest | Linux arm64 manifest |
|---|---|---|---|
| Rust builder `1.99.1-alpine3.24` | `sha256:44819b27be4c97b0e4d7203bd2e02c4820f97700211b6a37fb4c246a2de59383` | `sha256:2c83bf1a38975801abfd05e92387242ab4d5f96fe05131e2a279f3009ad16393` | `sha256:0a555e8b51945db91fd7b8411b4272bb5b8bfa04261d31eaeb2ada564bc02632` |
| Alpine runtime `3.24` | `sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf` | `sha256:0f49f331d9212aff11b932863be4eb01cdf962969aa86dab7d1ee21ac6f5f52b` | `sha256:1829893486be257df5049ab3b7fc95afbd0fc09d89803cbc580bda1ba5386b29` |
| PostgreSQL `19.0-alpine3.24` | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` | `sha256:8e81a484097154610d0b72f2fd1b70e1f8c31fd6b743b6d0da8b2a8c1ab2ea1d` | `sha256:8de50f3aa8f3da32960999886322bb746614951eed44304ea838b63430bb808a` |
| `rmcp` | `3.11.0`, archive SHA-256 `391f153431ab68664d946513ea0cb3edbca78eaf99c5a983036cacab6220d85b` | | |
| ripgrep musl archives | `15.2.0` (`e89fff89ac`) | SHA-256 `33e15bcf1624b25cdd2a55813a47a2f95dbe126268203e76aa6a585d1e7b149c` | SHA-256 `800b1e7206afe799dfb5a6901f23147cfaabe0e52210538100f61e86e1740915` |
| Alpine rsync | `3.5.2-r1` from v3.24/main | | |

[Official Rust image](https://hub.docker.com/_/rust) · [Alpine image](https://hub.docker.com/_/alpine) · [PostgreSQL Alpine image source](https://github.com/docker-library/postgres/blob/master/19/alpine3.24/Dockerfile) · [rmcp 3.11.0](https://docs.rs/rmcp/3.11.0/rmcp/) · [ripgrep releases](https://github.com/BurntSushi/ripgrep/releases/tag/15.2.0)

### Commands and observations

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and `actionlint .github/workflows/ci.yml` passed locally. The Cargo script runs formatting, build, Clippy, five core and fixture tests, loopback wire assertions, doc tests, and rustdoc. The tests use local fixtures after dependency prefetch.
- The original `rmcp` integration was tested on loopback and in the unmodified Alpine serve build (`sha256:fd7e017e1b465d38e1c64be4b662f29f9316ffbd1ddbca91cfdd4e1e7da4cdab`) using the wire suite in its direct-baseline mode. Discover, list, and direct calls already worked. Legacy `initialize` returned a JSON-RPC rejection with HTTP 200, and `notifications/initialized` returned 202. The rejection middleware sets the correct HTTP boundary for these requests. The spec makes `clientInfo` optional. The suite asserts that omitting it succeeds and that omitting required version/capabilities yields HTTP 400 and JSON-RPC `-32602`. [MCP metadata rules](https://modelcontextprotocol.io/specification/2026-07-28/basic#general-fields) · [HTTP rules](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http)
- The Rust dependency graph from `cargo tree --workspace --frozen -e features` selected Rustls `0.23.46` with ring `0.17.14` and `rustls-native-certs` `0.8.5` for the HTTPS probes. The app graph had neither `native-tls` nor `openssl-sys`. The PostgreSQL Alpine image is built with OpenSSL and ICU and includes full ICU data. [PostgreSQL Alpine image source](https://github.com/docker-library/postgres/blob/master/19/alpine3.24/Dockerfile)
- `python3 scripts/phase0/upstream.py --output /tmp/rfc-mcp-phase0-upstream-capture` passed on 2026-11-22 UTC. All seven requests returned HTTP 200 with zero redirects. The status, effective URLs, byte counts, media types and SHA-256 values are in the capture report. [RFC Editor source inventory](https://www.rfc-editor.org/series/rfc-download/)

### Upstream observations

| Capture | Bytes | Content-Type | Parser requirements seen in the sample |
|---|---:|---|---|
| Atom | 12,967 | `application/atom+xml;charset=utf-8` | Namespaced `feed`; 15 complete entries |
| RSS | 13,598 | `application/xml;charset=utf-8` | `rss` root with one channel and 15 items |
| Catalog prefix | 196,608 | `application/xml;charset=utf-8` | Namespaced `rfc-index` and 261 complete `rfc-entry` nodes; `updates` and `obsoletes` are nested lists |
| Catalog XSD | 17,241 | `application/octet-stream` | XML Schema content; use root and namespace checks as well as media type |
| RFC 2119 TXT | 4,723 | `text/plain;charset=utf-8` | Original UTF-8 bytes with three form feeds |
| RFC 8174 TXT | 6,071 | `text/plain;charset=utf-8` | Original UTF-8 bytes with four form feeds |
| RFC 9110 XML | 1,223,686 | `application/xml;charset=utf-8` | `rfc` root with nested reference groups and 84 complete reference elements |

The catalog server ignored the Range header, so the probe enforced its byte cap locally and counted complete elements within the prefix. The catalog root namespace matched the XSD target namespace. The incomplete transfer supports format observations only. Both real TXT files are preserved with [capture hashes and fixture review](../fixtures/rfc/provenance.json). Feed and XML content remain data. Collection URLs come from fixed approved endpoints.

### Runtime and release scope

`python3 scripts/phase0/integration.py --project rfc-phase0-check` built and passed the rootless Compose gate. The final rerun with `--skip-build` passed on the same image IDs and added explicit GIN index use and both lock-release paths.

| Runtime target | Tested image ID |
|---|---|
| `serve` | `sha256:b894790c834375a14726caad44fccd2320d4fdd00aa964857f8a9b60233bc6d8` |
| `sync` | `sha256:2cb193c99e7657d5510a708ff1cf705c12e353199f3ae75873fca2c0a401b1a1` |
| `feed` | `sha256:0e73f5e0713e6d21a6a7279e002a437c58d264c4ab4844f0f9f3521a06a26d8e` |
| PostgreSQL | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |
| Disposable upstream and wire client | `sha256:44ce256ef2cf98cfde805d6d9493558435aac224a21f26b78c07be9187531700` |

- Container startup and effective isolation passed. The non-root services had zero effective capabilities, read-only roots, `no-new-privileges`, and enforced seccomp. Serve's mirror was read-only. Sync wrote the byte-identical rsync fixture. Serve and sync reached PostgreSQL through their respective private networks, while feed had neither database reachability nor corpus mount or credentials. No host ports were published.
- The strict MCP suite passed against the serve image without a session or initialization. Supported modern requests, missing and mismatched headers, unsupported versions, optional and required metadata, encoded names, legacy methods, and Origin were checked at the HTTP wire.
- PostgreSQL 19.0 reported `x86_64-pc-linux-musl` and data directory `/var/lib/postgresql/19/docker`. UTF-8, ICU provider `i`, locale `und`, and collation version `153.136` matched after restart. Accented ordering, distinct Unicode byte sequences, Unicode `simple` token matching, and a query using a `pg_trgm` GIN index passed. Bootstrap reported that the libc `locale` utility was absent. The SQL and restart checks confirmed the configured ICU behavior. Verified TLS and SCRAM worked. The reader SELECT and shared lock succeeded, the exclusive lock conflicted until commit or rollback, and explicitly read-write DML, schema creation and temporary table attempts returned insufficient privilege. The restricted writer executed fixture DML inside a rollback.
- The ripgrep worker matched the approved fixture under Landlock ABI 8 and the worker seccomp allowlist. The mounted secret, unselected file, process status, scratch writes and new sockets were denied after positive controls. Both inherited socket descriptors were closed. A first attempt denied a musl startup `poll` call. A trace performed under the same Docker restrictions identified it, and the repeat passed with bounded polling support and the socket syscalls still denied.
- `file` and `readelf` on binaries copied from the tested images reported static PIE for the three Rust applications and ripgrep, with no dynamic `NEEDED` entries. `apk --no-network list --installed` and `ldd` confirmed the rsync musl loader and libraries, Alpine musl `1.2.6-r3`, the trust bundle `20261020-r0`, and PostgreSQL ICU libs/data `78.1-r0` and OpenSSL `3.5.9-r1`. Rustls accepted the trusted fixture certificate and rejected the wrong CA and hostname in sync and feed. PostgreSQL loaded `pg_trgm` in the server process and executed the indexed query.

Arm64 evidence covers registry manifests. Hosted CI, public deployment, and subsequent phase gates are outside this local verification.

## Phase 1

**Verified locally on rootless Linux amd64 on 2026-11-22.** Linux kernel `7.0.0-44-generic`, Docker Engine `29.10.0`, Compose `v5.8.0`, Rust `1.99.1`, and ext4-backed named volumes under `/tmp`. PostgreSQL used the same pinned 19.0 Alpine image as Phase 0. The locked client uses `tokio-postgres 0.7.18`, `tokio-postgres-rustls 0.14.0`, and Rustls `0.23.46`. [Storage format and provisioning](storage.md)

| Tested target | Image ID |
|---|---|
| serve | `sha256:ce23a015d4ce73d2170fc84d2e9d22b6b2ae1fb6e4163b369c1b46c3842f293d` |
| sync | `sha256:679ea43f0ef593fcca25ebde4da234c8ded176af29d98313ccf89d3e5a2b2523` |
| feed | `sha256:574aee203ed9cd799d82cbcc15627a4c180d895708341443ef746a6a1dd19535` |
| PostgreSQL 19.0 | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |
| Phase 0 fixture client | `sha256:4bdd4cd9311846e22c7028c759552fc948fb226d7bffea3658afb450b9a3f83c` |
| Phase 1 fixture driver | `sha256:c04f57ba82ca91d87215e8fb14db804ac6b7666103a82e6be75912104d50d4d0` |

### Commands and observations

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, `cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings`, `cargo test --workspace --all-features --frozen`, and `actionlint .github/workflows/ci.yml` passed. Cargo output was under `/tmp/rust/target`. Storage unit tests covered exact bytes and bounds, digest changes, unsafe paths, quarantined collisions, deterministic manifest bytes, and process termination before and after file sync, after rename, and after directory sync before registration.
- `cargo tree --workspace --frozen -e features` confirmed the app dependency graph uses Rustls and omits `native-tls` and `openssl-sys`. `file` and `readelf -d` on all three rebuilt Alpine application binaries showed static PIE with zero dynamic `NEEDED` entries.
- `python3 scripts/phase0/integration.py --project rfc-phase1-regression` passed on the recorded images. Results covered the strict MCP wire suite, rootless isolation, exact rsync transfer, TLS failures, PostgreSQL restart and ICU/GIN behavior, and the ripgrep worker's Landlock and seccomp boundaries.
- `python3 scripts/phase1/integration.py --image-prefix rfc-phase1-regression` passed. In the source project, four concurrent installs converged on one file. Path traversal, symlink, and FIFO candidates were rejected. Reinstalling against a same-size corrupt entry preserved it and quarantined the incoming bytes. The serve mount denied writes and private staging access. Feed had neither database reachability nor storage or credential mounts. The first integration attempt stopped at a mount override that kept the original Compose service mount. The final harness uses a separate Compose override file and verified rejection of both inverted mount modes.
- PostgreSQL applied and reran schema version 1. Under explicit read-write transactions the serve role was denied corpus DML, DDL, `TRUNCATE`, temporary tables, role changes, sequence use, unsafe functions, and `COPY TO PROGRAM`. Sync built two ready generations, reused their document versions, and was denied frozen mutations and active-pointer updates. Both generations had manifest digest `b99769e6663431cd18814166422fcd2b4fb27e51a6d235a83d502e217918aeca`. Tests verified TLS and SCRAM, migration-owner default function grants, actual FTS matching, original-byte range constraints, and lock release on commit and rollback. Startup checks rejected wrong role credentials, bad TLS identities, and excess grants.
- After container recreation the reader reverified both generations on the source volumes. The harness restored the quiescent database dump and three reachable blobs to a fresh Compose project, then rechecked each digest, stored range, manifest, and runtime role boundary. The TXT inputs were the two checked-in RFC originals. Catalog data, index ranges, and provenance observations were fixture data. The active generation remained unset at sequence zero.

The Phase 1 gate covers the local rootless amd64 fixture profile and process interruption. Qualification on other hosts, physical power-loss behavior, live ingestion, publication recovery, and release restore of a serving corpus remain separate from this evidence.

## Phase 2

**Verified locally on rootless Linux amd64 on 2026-11-22.** Runtime code at 60a9c38 passed the fixture gate and the bounded live-upstream smoke on Linux 7.0.0-44-generic, Docker Engine 29.10.0, Docker Compose v5.8.0, Rust 1.99.1, and the Phase 0 PostgreSQL 19.0 image pin. The lockfile selects roxmltree 0.21.2 and Rustls 0.23.46. Cargo artifacts used /tmp/rust/target. The disposable projects kept fixture inputs, test credentials, and volume backing directories under /tmp. [Ingestion commands and storage contracts](ingestion.md)

| Tested image | Local image ID |
|---|---|
| Serve | sha256:8b061fef07cd16d6ab0a45a450178a23dad49b32ff7bf34f3abc00192aefdeb1 |
| Sync | sha256:b06d93e9dcef71de27751efb22cf4623c6a8bad26ea36b2689f79b5ec54b99cc |
| Feed | sha256:cd4e0a852980c97c1fd2e8464496496b836be90170539987d9cb45cea8f58e84 |
| Phase 2 fixture driver | sha256:7aa1a2ab20f726ff4818a65a6ecc2013fc88fd99a22f72b845df4131400feec2 |

- cargo fetch --locked, scripts/phase0/check-rust.sh, workspace Clippy and tests with --all-features --frozen, and actionlint passed. The frozen feature graph confirmed Rustls and roxmltree and omitted native-tls and openssl-sys. Format-1 and format-2 manifest tests verified deterministic encoding for identical inputs.
- python3 scripts/phase0/integration.py --project rfc-phase2-regression passed against rebuilt images. It covered the strict MCP wire behavior, exact rsync transfer, TLS rejection cases, the pinned PostgreSQL checks, and effective Landlock and seccomp restrictions.
- python3 scripts/phase1/integration.py --image-prefix rfc-phase2-regression passed. Both format-1 fixture generations retained their original manifest digest after schema version 2, container recreation, and restore into a fresh Compose project.
- python3 scripts/phase2/integration.py passed. A full fixture candidate verified five TXT bodies, including invalid UTF-8 preserved in bytes mode, and recorded one catalog-only RFC. The reader reconstructed the candidate manifests and checked stored line, heading, section, notice, evidence, and chunk byte ranges. An exact literal spanned a chunk boundary, and PostgreSQL retained that token and terms on either side. A metadata-only refresh reused all five verified TXT observations and document versions. The gate also exercised the migration from schema version 1, a scoped sample, typed catalog and reference edges, and incomplete RFCXML fallback.
- The fixture gate rejected a vanished catalog entry, catalog drift, malformed catalog, redirects, encoded and oversized HTTPS bodies, an interrupted rsync stream, a missing or symlinked TXT, and a byte mismatch. The mismatch went to private quarantine. A competing run was rejected while sync held the advisory lock. The active generation remained unset at sequence zero with an empty outbox, and the fixture verified the serve and feed permission boundaries.
- python3 scripts/phase2/integration.py --live verified RFCs 2119, 8174, and 9110 from official rsync and HTTPS in a fresh Compose project. The catalog digest matched across both captures and the scoped candidate reached ready with manifest c6feca1a6071e62c97b19f4f7447195ba5dceed516c065e211f12ac522d4695d. The report recorded two unavailable RFCXML bodies, one RFC with incomplete reference coverage, and one RFC with incomplete section coverage. The active pointer stayed unset.

The live sample is the extent of upstream verification. Full live reconciliation, publication, and production-scale capacity remain later verification work.

### NUL-containing TXT regression

Verified on 2026-11-23 with Rust `1.99.1` and rootless Docker `29.10.0` with Compose `v5.8.0` on Linux amd64. `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and frozen workspace Clippy with all targets and features passed. The Phase 0, 1, and 2 offline Compose probes passed against the rebuilt `rfc-nul-regression` app images. Phase 0 exercised the MCP wire, PostgreSQL, and service isolation. Phase 1 verified original bytes, ranges, and manifests after restoring into a fresh PostgreSQL and mirror project.

The Phase 2 fixture reached `ready` with six verified TXT bodies and reported `contains_nul = 1` and `invalid_utf8 = 1` for two separate documents. The valid UTF-8 NUL fixture retained its exact bytes, BLAKE3 digest, LF offsets, and verified RFCXML reference. Its sealed document version had unavailable text indexes and no TXT-derived rows. The reader reconstructed both manifests and confirmed reuse of the same document version and TXT observation after a catalog metadata refresh. A mismatched rsync body containing NUL still failed and went to quarantine. The active pointer stayed unset and the outbox stayed empty. This run used synthetic NUL input in disposable Compose projects.

## Phase 3

**Verified locally on rootless Linux amd64 on 2026-11-22.** The runtime changes are in `bde10fc` and `8204309`. Tests used kernel `7.0.0-44-generic`, Docker Engine `29.10.0`, Compose `v5.8.0`, Rust `1.99.1`, and the PostgreSQL 19.0 Alpine image pinned in Phase 0. Cargo output used `/tmp/rust/target`. The Compose projects kept test data, credentials, and volume backing directories under `/tmp`. [Operator commands, lock ordering, and backup procedure](publication.md)

| Tested image | Local image ID |
|---|---|
| Serve | `sha256:6c20eeb96ea212479f8b44a9ac54bcac8667129efe9a4ffcf64441e8a1053988` |
| Sync | `sha256:29034f624ddd9771b1b2eac90a0031c6c48bb944dc767248ae3831e761c2862d` |
| Feed | `sha256:528f7cff4a3a66184d0862feb1a391c33dba8fb9747229882a6ab7c320ba1c77` |
| PostgreSQL | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |
| Phase 3 fixture | `sha256:39de3e7a73acac9b939730468f262c7972abefc19f4ed79aa247aef3f9a38e72` |

### Commands

```sh
export CARGO_TARGET_DIR=/tmp/rust/target
cargo fetch --locked
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
actionlint .github/workflows/ci.yml
python3 scripts/phase0/integration.py --project rfc-phase3-final
python3 scripts/phase1/integration.py --image-prefix rfc-phase3-final
python3 scripts/phase2/integration.py --image-prefix rfc-phase3-final
python3 scripts/phase3/integration.py --image-prefix rfc-phase3-final
```

### Observations

- The locked Rust checks, full-feature Clippy and tests, and `actionlint .github/workflows/ci.yml` passed locally. The Phase 0 Compose probe passed strict MCP wire behavior, PostgreSQL 19 locks and privileges, TLS, exact fixture rsync, and the effective Landlock and seccomp controls. The Phase 1 and Phase 2 regressions passed storage and restore compatibility, original-byte ingestion and indexing, and the service permission boundaries. Phase 2's first run during parallel fixture image exports timed out in Compose setup. The rerun after image export passed, and the two leftover test volumes were removed.
- The Phase 3 Compose suite upgraded a schema 2 database to schema 3 and preserved the existing ready row byte for byte. On an initially empty pointer, an interrupted promotion and an injected outbox insert failure left the pointer at sequence zero. Process exit after commit made the new generation readable with a pending event. The suite rejected a scoped sample, a stale expected sequence, and a corrupted canonical file. Seven publication events included two successful rollbacks. Concurrent readers observed consistent metadata, manifests, and original bytes through a pointer change.
- The suite terminated sync around candidate insertion, file and directory sync, and readiness. Recovery cleared incomplete candidates and staging while the active generation remained readable. It terminated a PostgreSQL backend during an accepted read and proved the shared mirror fence still blocked removal. A reader waiting behind exclusive retirement observed the tombstone before accessing bytes. A held mirror coordinator blocked concurrent reconciliation even after the collector lost its PostgreSQL backend. Cancelling a fixture worker reaped it. The active and retained generations continued sharing unchanged blobs on the same inode.
- The suite interrupted retirement before and after its database commit and interrupted deletion before unlink, after unlink, and after directory sync. Recovery drained the queue; a queued digest could not be registered before cleanup. Count and age protections and explicit ready-candidate discard passed. A fresh Compose project restored the quiescent PostgreSQL dump, registered blob inventory, and readable generation locks, then reproduced the active and retained guarded reads and the outbox count.

The evidence covers synthetic fixtures, process interruption, and PostgreSQL connection loss on the local profile. Full live corpus publication, physical power loss, hosted CI, public deployment, and authenticated notification delivery remain separate verification work.

## Phase 4

The search gate passed locally on 2026-11-22 with rootless Docker Engine 29.10.0, Compose v5.8.0, Linux 7.0.0-44-generic amd64, Rust 1.99.1, and the Phase 0 PostgreSQL 19.0 Alpine image. Cargo used `/tmp/rust/target`. Test inputs, bind mounts, credentials, and cursor keys were under `/tmp`. The search library is available for Phase 5 MCP integration. [DSL reference](search.md)

| Tested image | Local image ID |
|---|---|
| Serve | `sha256:db235950e7b8773aad8167ea7b05474186fefcbdb2d9e2e0b5cd3619fb46b9fe` |
| Sync | `sha256:f0c9a1f43175654cdf0519802accb7f83195e12ebe2eaaa54b6b3ca4c057c1a7` |
| Feed | `sha256:659f503ee155a7596d7db3247845575aebd074947cee2632e5e1b60be95fd695` |
| PostgreSQL | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |
| Phase 4 fixture | `sha256:f0ac5a90b5632c59669094b18e4fc6a473e581cf7ccd538f6a503c9155212ff4` |

### Commands

```sh
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp
cargo fetch --locked
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
actionlint .github/workflows/ci.yml
python3 scripts/phase0/integration.py --project rfc-phase4-final
python3 -B scripts/phase1/integration.py --image-prefix rfc-phase4-final
python3 -B scripts/phase2/integration.py --image-prefix rfc-phase4-final
python3 -B scripts/phase3/integration.py --image-prefix rfc-phase4-final
python3 -B scripts/phase4/integration.py --image-prefix rfc-phase4-final
```

### Observations

- The locked workspace checks, all-feature Clippy and tests, and `actionlint` passed. Phase 0 verified the strict Streamable HTTP wire behavior, PostgreSQL TLS and ICU, and the effective Landlock and seccomp controls on the rebuilt images. Phases 1 and 2 verified least-privilege storage, restored manifests, original-byte ingestion and indexing, and the existing failure fences. Phase 3 verified publication, reader locking, recovery, and restore of the active and retained snapshots.
- The Phase 4 fixture upgraded schema 3 to 4, reran the migrations, preserved a legacy manifest row, and ran queries as the SELECT-only serve role. The suite compared 124 known and generated ASTs with an independent three-valued evaluator. It checked every relationship direction, duplicate evidence, catalog gaps, document-wide token AND, exact literals crossing chunks, invalid UTF-8, and corrupt originals.
- On the pinned PostgreSQL image, the position cap probe stored a terminal position of 16,383 and 255 positions for the repeated lexeme. The 2,048-byte test lexeme was ignored. Search uses document-wide lexeme membership for body token matching.
- The fixture checked authenticated cursor binding, tampering, expiry, complete pagination across publication and rollback, and refusal after retirement. A deliberate relation lock returned `deadline_exceeded`, and the reader connection closed. Exact and token excerpts and their complete notice bundle matched original byte ranges for the synthetically approved document. Policy-blocked results stayed metadata-only.
- Early runs exposed a JSON borrowing error for escaped literals and a blocking stdin loop that delayed fixture connection cleanup. Owned request strings and asynchronous fixture input resolved both. The first benchmark also exposed broad observation and coverage scans. The final SQL uses correlated lookups and the benchmark asserts the expected index use.

### Query plans and timings

The fixture published a seeded 10,000-document generation. Each timing is from a warm local request under the serve role, including connection checks and generation locking. The table shows six measured requests per case after one warmup. Most synthetic documents share a 192 KiB original blob.

| Case | RFCs evaluated | Median (ms) | Maximum (ms) |
|---|---:|---:|---:|
| Title token query | 27 | 42.07 | 44.66 |
| Inbound obsolescence | 512 | 50.16 | 62.40 |
| Empty partial page at row budget | 512 | 49.82 | 54.23 |
| Exact query at original-byte budget | 188 | 122.37 | 135.38 |
| 64 exact literals at comparison budget | 60 | 76.20 | 87.95 |

The compiled 32-candidate `EXPLAIN (ANALYZE, BUFFERS)` samples used `generation_documents_pkey` for the generation and RFC window and `source_observations_pkey` for the original URL. Inbound obsolescence used an index-only scan on `relations_inbound_obsolescence` with generation and target conditions and zero heap fetches. Reference lookups used `relations_inverse`. Both relationship samples used the coverage uniqueness index to check the applicable family. The fixture prints the full plans.

The measurements describe a synthetic local corpus. Live corpus performance, concurrent production traffic, hosted CI, and public MCP search require their own verification.

## Phase 5

**Verified locally on 2026-11-22.** The Phase 5 serving endpoint uses `rmcp` 3.11.0, schema 5, and PostgreSQL 19.0. Verification ran on rootless Docker Engine 29.10.0, Compose 5.8.0, Linux 7.0.0-44-generic amd64, and Rust 1.99.1. Cargo artifacts were under `/tmp/rust/target`. Compose used private networks and disposable bind mounts, database passwords, cursor keys, and certificates under `/tmp`. [MCP API and serving configuration](mcp.md)

| Image | Local image ID |
|---|---|
| Serve | `sha256:afbe7deaab3ac29c69b28bf3a487635dbac2a1d30db2fb06f26f6b15f4358961` |
| Sync | `sha256:1a0ed8dc9cb969211b90bfbe31a6c9d48fec5af10c887ba856cf79deca6b435e` |
| Feed | `sha256:e9b00b21d47ee5ee9a964d8410cd98ac0aece2ad3786f1e0112b6b322cb6d8db` |
| PostgreSQL | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |
| TLS proxy and upstream fixture | `sha256:0ab3ec8a69751f517b4408e04194e2b5304fc57f933607663c44c30efd916734` |
| Publication and review fixture | `sha256:25f3e57e9cac33f49353728fdcc93200e133d15715e320481d0c6c1bf2e1dca8` |

### Commands

```sh
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp
cargo fetch --locked
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
actionlint .github/workflows/ci.yml
python3 -B scripts/phase0/integration.py --project rfc-phase5-complete
python3 -B scripts/phase5/integration.py --image-prefix rfc-phase5-complete
for phase in 1 2 3 4; do
    python3 -B "scripts/phase$phase/integration.py" --image-prefix rfc-phase5-complete
done
```

### Observations

- The locked Rust checks, all-feature Clippy and tests, and workflow lint passed. The Phase 0–4 Compose regressions passed on the final service images, including effective Landlock and seccomp checks, PostgreSQL TLS and role checks, original-source ingestion, guarded publication and restore, and the generation-pinned DSL suite.
- The private gate upgraded schema 4 to 5, reran the migration, and preserved the existing whole-only document version. A client using the pinned protocol exercised discovery, direct calls, all five schemas, and typed errors through a localhost-published TLS proxy. Readiness reported an empty corpus before publication. Raw requests checked version and header mismatches, malformed messages, Host and Origin rejection, and legacy transport behavior.
- The original-byte checks served RFC 2119 and RFC 8174 under their existing single-response approvals. The UTF-8 fixture reassembled to 93,023 bytes and its largest serialized JSON-RPC response was 221,679 bytes. The non-UTF-8 fixture reassembled to 44 bytes and preserved its original notice in base64. Both reassemblies matched their full BLAKE3 identities. Tests covered escaped control bytes, CRLF, UTF-8 boundaries, section coverage and duplicate headings, absent or oversized notice bundles, and exact search snippets and heading ranges.
- The gate exercised rate limits for trusted and untrusted peers and the 16-request admission limit. PostgreSQL denied writes under the serving role, serve could not write to the mirror, and sync and feed lacked the cursor key. Fresh reads saw the committed pointer without a notification, while continuations stayed pinned across promotion and rollback and failed after retirement.
- Early fixture runs exposed a disposable CA missing its key-usage extension, a NUL in the binary fixture, and an incorrect assumption about the original headings fixture's coverage. The private suite now uses a strict TLS chain, an ingestible non-UTF-8 fixture, and an explicit orphan-section fixture. The disconnect probe also found a blocked PostgreSQL backend still present at the end of the observation window. With connection-loss checks enabled for serve, the private gate observed the blocked session cleared within 78 ms of disconnect. A separate lock deadline returned `deadline_exceeded` and left zero reader sessions. [PostgreSQL TCP settings](https://www.postgresql.org/docs/19/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)

Official RFCs require digest-scoped review receipts before enabling whole-document pagination. The Phase 5 implementation returned `sandbox_unavailable` for `rg`. Hosted CI, a live corpus, and public deployment have separate acceptance gates.

## Phase 6

**Verified locally on 2026-11-23.** Runtime commits `988b50c` and `360f246` ran with Rust 1.99.1, Docker Engine 29.10.0 in rootless mode, Compose 5.8.0, Linux 7.0.0-44-generic amd64, and the pinned Phase 0 PostgreSQL 19.0 Alpine image. Cargo used `/tmp/rust/target`. Fixture inputs, restricted key mounts, logs, and volume backing directories used `/tmp`. The gate used disposable Compose projects and synthetic upstream feeds. Production Compose verification remains Phase 7 work. [Control-plane configuration and runbook](control.md)

### Commands

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, `cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings`, `cargo test --workspace --all-features --frozen`, and `actionlint .github/workflows/ci.yml` passed.
- `python3 scripts/phase0/integration.py --project rfc-phase6-final` passed on the final images. It exercised the pinned MCP wire protocol, exact rsync transfers, TLS and least-privilege PostgreSQL checks, service isolation, and the Landlock and seccomp denial probes.
- The Phase 1 through 5 integration scripts passed with `--image-prefix rfc-phase6-final`. The Phase 4 rerun used `--skip-fixture-build` with its already-built driver. The Phase 3 rerun observed the waiting reader from a preconnected PostgreSQL session and verified the post-lock tombstone check.
- `python3 scripts/phase6/integration.py --image-prefix rfc-phase6-final` passed. The local redacted test output is in `/tmp/rfc-phase6-logs/phase6-verified.log`.

### Observations

- **Feed independence and visibility (A18–A19).** With feed unavailable, startup and daily reconciliation published verified snapshots. An hourly catalog digest change queued a full run. Concurrent duplicate nonces produced one acceptance, and repeated logical events shared a durable job record. The queue held one running job and one pending rerun. Process termination before and after event acceptance and publication commit exercised the durable recovery paths. Reordered hints and a restarted serve process left the active generation visible through fresh MCP reads.
- **Authentication and isolation (A27–A30).** Independent request and response golden vectors matched. Checks covered original-body and whitespace mutations, method, path and header rejection, clock skew, expired previous keys, old epochs, and replay-cache exhaustion. A forged identity response prevented command delivery, and forged acknowledgments left the outbox pending. The same rows cleared after a genuine signed acknowledgment. Compose checked per-service key mounts, read-only mirror access, denied database mutations, and ingress isolation after recreation.
- **Polling and upgrade.** Schema 6 upgraded a schema 5 database, preserved the existing job row, and reapplied cleanly. Atom and RSS each returned 304 after the client supplied validators. Repeated and reordered entries and a format switch produced the same logical IDs. Malformed and oversized feeds produced no events. Successive requests after HTTP 429 honored the fixture's four-second retry interval.
- Early runs exposed a missing JSON lock-error report in the refactored manual command and fixture issues involving Compose subnet overlap, container startup timing, and transient queue contention. The final run kept the original CLI report, selected an unused private subnet, and retried test submissions with fresh nonces. The runtime database permissions and lock deadlines remained in effect.

## Phase 7

**Verified locally on rootless Linux amd64 on 2026-11-23.** Runtime changes are in `ccd01c2` and `932df99`. The qualification used Rust 1.99.1, Docker Engine 29.10.0, Compose 5.8.0, Linux 7.0.0-44-generic, cgroup v2, and Landlock ABI 8. Cargo used `/tmp/rust/target`. Disposable originals, secrets, certificates, and volume backing directories used `/tmp`. [Operator configuration and verification](deployment.md)

| Image | Local image ID |
|---|---|
| Serve | `sha256:f61136884b17e10562be086859c9683f549b951b51899f8eff892d693acee37d` |
| Sync | `sha256:276f1a60df9501fdc0be8c671450bce527a2b43d7d0e667ffd3b8a7d7579bdb8` |
| Feed | `sha256:8581b1df79302eb5f26a26d5f173965134e94381b0c8f102a86c1320c935a269` |
| PostgreSQL 19.0 Alpine | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |

### Commands

```sh
export CARGO_TARGET_DIR=/tmp/rust/target TMPDIR=/tmp
cargo fetch --locked
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
actionlint .github/workflows/ci.yml
docker compose --env-file deploy/compose.env.example config --quiet
python3 -B scripts/phase0/integration.py --project rfc-phase7-qualified
for phase in 1 2 3 4 5 6 7; do
    python3 -B "scripts/phase$phase/integration.py" --image-prefix rfc-phase7-qualified
done
```

### Observations

- The locked Rust checks, full-feature Clippy and tests, workflow lint, Compose configuration check, and Phase 0 Docker probe passed. The built ripgrep reported 15.2.0 and `-pcre2`. Phase 0 verified the pinned Streamable HTTP protocol, TLS and separate database grants, and Landlock and seccomp enforcement in the rebuilt image.
- The Phase 1–6 regression suites passed on the qualified images. They covered storage and restore, original-byte ingestion, publication and recovery, the pinned search DSL, MCP reads, and authenticated control and feed behavior.
- Phase 5 exercised all five tools through a local HTTPS proxy. Ripgrep returned original byte ranges and notices for reviewed synthetic RFCs, rejected invalid and PCRE2-only regex syntax, treated option-like patterns as data, and handled BOMs, CRLF, Unicode, repeated and zero-width matches. Dense output reached the frame limit and returned explicit partial coverage or a typed resource error. Signed continuations stayed pinned across publication and rollback and became unavailable after generation retirement.
- The production profile initialized an empty PostgreSQL 19.0 cluster at `/var/lib/postgresql/19/docker`, applied schema 6 through the local peer socket, and restored a synthetic generation. It verified the runtime UIDs, file permissions and service secret mounts, effective cgroup limits, serve’s read-only mirror, feed’s isolation from PostgreSQL, and ingress reachability before and after recreation.
- The production sandbox probe denied the mounted test secret, metadata, staging and retired fixture paths, parent process credentials, writes, new TCP and Unix sockets, and inherited sockets. It enforced the address-space and CPU limits and preserved raw binary offsets. A disposable blocking executable exercised the production supervisor. Two occupied slots rejected a third call. Disconnect and wall deadlines killed and reaped workers and released the mirror fences. The canceled database reader closed. A simulated Landlock refusal disabled `rg` and returned HTTP 503 from readiness while the other read tools remained usable.
- Early fixture runs exposed a BOM placement that prevented the existing notice parser from recognizing authorship and a zero-width assertion that used a page smaller than the expected match count. The passing checks use a separate BOM line and an explicit page limit. A concurrent edit to the test overlay interrupted one teardown. The exact disposable project was cleaned up and the complete gate was rerun.

The gate used synthetic sources and a disposable localhost HTTPS proxy. Live corpus reconciliation, other host profiles, hosted CI, registry publication, and public deployment have separate acceptance gates.

## Phase 8

**Local qualification passed A01–A39 on 2026-11-23. Public release is unapproved with A40 open.** The qualification used Linux `7.0.0-44-generic` amd64, rootless Docker Engine `29.10.0`, Compose `5.8.0`, cgroup v2, and Landlock ABI 8. The toolchain was Rust `1.99.1`, rmcp `3.11.0`, ripgrep `15.2.0` without PCRE2, and PostgreSQL `19.0` on Alpine. Cargo used `/tmp/rust/target`. Temporary certificates, test credentials, original bytes, backups, and reports stayed under `/tmp`. [Acceptance matrix and release evidence](release-readiness.md)

| Qualified image | Local image ID |
| --- | --- |
| Serve | `sha256:5b7d3c33b32aa8360153962466849dbf4581ee497a672c22448eed7f182fb503` |
| Sync | `sha256:75d9c4b0657b329071b8c428904fc725919eeefc044517eaf44cc34ede284a68` |
| Feed | `sha256:4290ce2cf46ba4f33ad962fac004ba70a33151e73c741b46d945f3a4ceaac8e4` |
| PostgreSQL | `sha256:8b0ff4cdf221f0c53d7fcda681d3b461be725c492cd881c5ec9c0ad25023f188` |

### Commands and evidence

- `cargo fetch --locked` and `RFC_PHASE8_IMAGE_PREFIX=rfc-phase8-final bash scripts/phase8/verify.sh` passed. The runner checked frozen Rust builds, formatting, all-feature Clippy and tests, workflow lint, 258 HTTP mutations, Phase 0–7 Compose probes, destructive restore, and both capacity profiles. The runner started with Phase 8 changes in the working tree on base `a63480a`. The results, per-command logs, `restore.json`, and `capacity.json` are under `/tmp/rfc-phase8-evidence.lYnISDyA`.
- `python3 -B scripts/phase0/upstream.py` passed with seven bounded HTTPS captures at `/tmp/rfc-mcp-upstream-n6xorhp9`.
- `python3 -B scripts/phase2/integration.py --context rootless --image-prefix rfc-phase8-final --live` verified RFCs 2119, 8174, and 9110. Candidate manifest `405d8d2e2e97562b45c497881013d6477cb29b1d052bf49888e8d6c9d0c85fac` reached ready and the active pointer stayed unset. See the per-RFC results in the release report and the local receipt at `/tmp/rfc-phase8-observed/live-final.log`.
- `RFC_PHASE8_IMAGE_PREFIX=rfc-phase8-final bash scripts/phase8/audit.sh /tmp/rfc-phase8-audit-signed` uses a checksum-verified Trivy archive. The outputs include four CycloneDX SBOMs, dependency and license inventories, advisories, and redacted secret reports. The audited code commit is `16fb0bb`.

### Recovery and security

- The crash and recovery fixtures exercised PostgreSQL and file durability boundaries, original-byte and notice checks, missed and reordered notifications, concurrent-reader retirement and reclamation, and sequence-bound rollback. The Phase 6 test observed one running and one coalesced pending sync job. Both HMAC links passed signed overlap, sender switching, outbox delivery, and expired-key rejection.
- The final restore rehearsal destroyed the source project, verified independent backup bytes, and restored 16 blobs, 794 ranges and line starts, and three manifests into fresh volumes. Python and curl passed discovery, listing, and all five direct tool calls against the restored HTTPS service. Both clients observed HTTP 400 for the old revision and HTTP 405 for GET and DELETE.
- The 1,000- and 10,000-RFC capacity tests covered 1, 4, 16, and 32 clients. At 32 clients, the observed successful rates were 28.1 and 25.0 calls/s. The admission test held at 16 active requests, and each profile returned to zero DB readers and ripgrep workers after overload. [Capacity measurements](release-readiness.md#capacity-methodology)
- Initial testing found a startup race where Docker could allocate a fixed control listener address to a dynamic client. The updated example puts those clients in disjoint address pools. The qualification starts sync first and verifies address pools and reachability after recreation. An initial restore run also attempted the verifier while serve held its fixed address. The verifier now runs before serve opens. A burst hit the disposable proxy's own limits, which were raised in the fixture before rerunning the load gate.
- The maintainer confirmed the existing MIT license for the application and its documentation. Public release still requires disposition of the pinned PostgreSQL image findings under A40, review of redistribution terms for included components, qualification of the real hosting configuration, and full live-corpus validation. Hosted CI, registry publication, and public activation are separate gates. [Supply-chain review](release-readiness.md#supply-chain-review)

## GitHub Actions CI matrix

Branch pushes and pull requests run the frozen Rust checks, all-feature checks, and HTTP mutation smoke on native Linux amd64 and arm64. The Compose matrix runs Phases 0–7 and the Phase 8 restore on amd64, and Phases 0, 5, and 7 on arm64. Each architecture builds its image bundle once. The case jobs verify the archive checksum, source revision, architecture, and loaded image IDs before running the existing probes. Phase 8 capacity and live upstream checks remain part of local qualification. [CI workflow](../.github/workflows/ci.yml) · [Docker workflow](../.github/workflows/docker-e2e.yml)

### Local verification on 2026-11-23

- On Linux amd64, the offline Rust checks, all-feature Clippy and tests, and all 258 HTTP mutation cases passed. A fresh Rustup directory under `/tmp` installed Rust `1.99.1` and the required components. Workflow lint and production Compose configuration validation passed.
- Docker Engine `29.10.0` and Compose `5.8.0` built the nine amd64 service and fixture images on kernel `7.0.0-44-generic` from the working tree based on `636a987`. Both Docker contexts loaded the archive and reported the expected IDs. The runner and the Phase 3 probe rejected a missing preloaded fixture. The loader rejected an incorrect checksum, a different source revision, and a nonnative host before calling Docker load. Local evidence is under `/tmp/rfc-ci-e2e.n1WaHiUy`.
- The preloaded Phase 0–7 cases and Phase 8 destructive restore passed on rootful amd64. The restore report verified 16 blobs, 794 ranges and line starts, three manifests, and public MCP `2026-07-28` over HTTPS. Phase 0 and Phase 7 also passed on rootless amd64 with the same image IDs. The existing rootless Phase 0 invocation with `--skip-build` passed.

### ARM64 Rust CI fix

- In the [hosted PR run](https://github.com/PublicData-stream/rfc-mcp-docker/actions/runs/45118553865), the amd64 Rust job and both Compose matrices passed. ARM64 workspace Clippy failed with `unused_mut` because the seccomp syscall list was only mutated on x86-64. The ARM64 all-features checks and HTTP smoke were skipped, and the CI summary failed.
- The fix uses an immutable shared syscall list and appends the existing x86-64 entries through an iterator. The syscall entries, order, and filter actions are identical to the previous implementation.
- On local amd64 with Rust `1.99.1`, the offline workspace checks, all-features Clippy and tests, and all 258 HTTP smoke cases passed. The rootless Compose Phase 0 probe passed on rebuilt images, including the `2026-07-28` Streamable HTTP wire checks, the expected ripgrep match, and the Landlock and seccomp controls.
- On `aarch64-unknown-linux-gnu` with Rust `1.99.1` under Docker/QEMU, frozen workspace Clippy passed for all targets with default features and with all features, both with `-D warnings`. Hosted CI and native ARM64 runtime verification for the fix are pending.

### Phase 6 event recovery CI fix

- The hosted amd64 Compose probe received HTTP 503 when resending an event after a sync restart. The unmodified probe passed locally. Holding the PostgreSQL queue advisory lock in a disposable rootful Compose project reproduced the 503. Releasing the lock allowed a fresh signed request to receive a valid acknowledgment for the original event and job. [Hosted Phase 6 failure](https://github.com/PublicData-stream/rfc-mcp-docker/actions/runs/45121751933/job/137520063790?pr=1)
- The crash-recovery probe retries HTTP 503 within a 15-second window using a freshly signed request for the same event. It verifies the old epoch is rejected, checks the acknowledgment signature and event ID, and forces queue contention after the committed-event crash to verify the retry preserves the original job association.
- On 2026-11-23, the CI runner built one native amd64 image bundle and verified it in both Docker contexts. Phase 6 passed twice on rootful Docker and once on rootless Docker. All three runs observed the forced 503 and preserved the committed event's job. Phase 0 passed in both contexts with Docker Engine `29.10.0`, Compose `5.8.0`, and Linux `7.0.0-44-generic`. The bundle receipt and probe logs are under `/tmp/rfc-phase6-fix-compose.s7KJse`.
- The locked offline Rust checks, all-feature Clippy and tests, all 258 HTTP smoke cases, and workflow lint passed locally. The hosted result for this fix is pending.

## GHCR image publication

The release workflow verifies signed tags and exact-commit main CI, builds native AMD64v3 and ARM64 service archives, and promotes the tested manifests to three fixed-version OCI indexes. The workflow publishes a digest receipt for Compose and verifies anonymous pulls. [Release procedure](ghcr-release.md)

### Local verification on 2026-11-23

- The offline Rust checks, `actionlint`, and release shell and Python syntax checks passed. The source gate accepted the actual [successful main CI attempt](https://github.com/PublicData-stream/rfc-mcp-docker/actions/runs/45127806469) for `2aef81d`. The gate rejected invalid tag events, a mismatched build suffix, missing CI, and a newer failure following an older successful attempt.
- Native Linux AMD64v3 archives for the local candidate `1.0.0-build.a9e52bdb` reported the stamped version in all three services. The exact same images passed Phase 0 in rootless and rootful Docker Engine `29.10.0` with Compose `5.8.0`, including MCP wire, TLS, permissions, and ripgrep sandbox checks. Trivy `0.76.2` found no advisories in the two Cargo lockfiles and no critical or fixable high findings in the three images using the 2026-11-23 advisory database. The release audit policy also rejected synthetic blocking findings and missing scan results. Evidence is under `/tmp/rfc-release-validation.zlFnDuGR`.
- The pinned BuildKit exported the requested AMD64v3 and ARM64 platform descriptors. A disposable loopback registry verified both descriptors, all three index digests, anonymous downloads, rejection of missing artifacts and conflicting tags, digest-only upload, and partial publication retry. A second rehearsal combined the real AMD64v3 images with ARM64 descriptor fixtures and confirmed that Docker selected and ran all three AMD64v3 binaries by index digest. Registry evidence is under `/tmp/rfc-release-registry.buTb2xnX` and `/tmp/rfc-release-compose-pull.QhAboRrC`.
- Native ARM64 release-image qualification, the hosted release workflow, and anonymous GHCR pulls remain pending for the first authorized release. The release rehearsal used only the loopback registry.
