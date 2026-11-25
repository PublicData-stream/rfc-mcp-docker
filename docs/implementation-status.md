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


### Bounded reconciliation pipeline

The collector stages one rsync batch at a time and commits RFCs in ascending order. The Compose sync service has a 512 MiB scratch mount, a 2 GiB memory limit, and two CPUs. The document pipeline reserves 48 MiB of raw payload space across three slots. A shared gate enforces one second between HTTPS starts while allowing separately configured in-flight work. [Resource budgets and operator settings](ingestion.md#pipeline-and-recovery)

The serial baseline is signed commit `e2e5a41`. Measurements used the pinned Rust 1.99.1 Alpine binaries, PostgreSQL 19.0, Docker Engine 29.10.0, and Compose 5.8.0 on Linux amd64. The serial and candidate fixtures used fresh databases and mirrors, the same 512 MiB scratch and 2 GiB memory limits, deterministic delayed HTTPS and rsync, and fixed source observation timestamps for exact manifest comparison. The benchmark used the rootless engine. The offline Compose qualification used the rootful engine. Temporary evidence and image receipts are under `/tmp/rfc-pipeline.SI9kWP` on the qualification host.

The initial parallel probes exhausted the rootless Docker address pool. The rootful rerun used sequential Compose probes and loaded the pinned Rust image for the Phase 7 worker fixture. A cached fixture binary lacked the new metrics. Rebuilding the sync fixture variants from source resolved that mismatch. The Phase 3 waiting-reader assertion passed on rerun with a freshly rebuilt fixture. Two early large one-request trials failed on HTTPS after 56 and 59 commits during runs with slow blob installs. The candidate then moved blob fsync to a single supervised blocking task. The collector continues to fail closed on a required transfer error.

#### Paced fixture benchmark

Each row uses three successful cold runs. CPU is median process and child CPU time. Memory is the highest sync cgroup peak. Stage payload is the largest set of rsync TXT files before they were drained.

| RFCs | Collector | Median wall (s) | Median CPU (s) | Peak memory (MiB) | Peak stage payload (MiB) | HTTPS starts per run |
|---:|---|---:|---:|---:|---:|---:|
| 6 | Serial | 27.334 | 0.067 | 5.28 | 0.026 | 14 |
| 6 | Pipeline, one in flight | 21.970 | 0.072 | 5.15 | 0.026 | 14 |
| 6 | Pipeline, two in flight | 25.661 | 0.075 | 5.18 | 0.026 | 14 |
| 96 | Serial | 294.322 | 0.476 | 10.64 | 1.075 | 194 |
| 96 | Pipeline, one in flight | 249.178 | 0.518 | 8.64 | 1.075 | 194 |
| 96 | Pipeline, two in flight | 218.306 | 0.518 | 8.06 | 1.075 | 194 |

For 96 RFCs, two in flight reduced median wall time by 25.83% versus serial and 12.39% versus one in flight. This met the 10% gate and set the default to two. The shortest observed interval across the paced runs was 1,000,223 µs. The 194 starts imposed a 193-second floor between first and last dispatch. All paced repetitions agreed on manifest digests and accepted bytes for their corpus size. The 96-RFC manifest was `8fd33b84ce545b3d2827c8c6a4195ee883aa028967aa62185f9e19f6249411bb`. The manifest includes canonical TXT, document/index, membership/provenance, relation/evidence, and coverage identities.

In the serial 96-RFC runs, aggregate blob installation took a median 62.4 seconds, database work 18.0 seconds, rsync 0.94 seconds, and parsing 0.017 seconds. Wall time varied with host filesystem and database latency. With six RFCs, one in flight gave the lowest median. The separate unpaced six-RFC diagnostic measured 30.4 seconds for serial and 10.1 seconds for the pipeline at two in flight, one run each. All timing results are from private fixtures.

#### Offline verification

`cargo fetch --locked`, `scripts/phase0/check-rust.sh`, frozen all-feature Clippy and tests, and the effective Compose configuration check passed. The rebuilt default-two images passed offline Compose Phases 0, 1, 2, 3, 6, and 7. Those checks covered the pinned public MCP wire protocol and service roles, storage restore, ingestion and cancellation, publication and recovery, daemon scheduling and control, and the effective 512 MiB production sync mount. The fixture suite ran in disposable Compose projects.

The Phase 2 default check observed two in-flight HTTPS requests and two out-of-order completions. The 33-RFC stress run transferred 32 maximum-size TXT files followed by a second batch. It observed one staging directory at a time, 134,217,728 bytes of staged TXT, 134,221,824 bytes of scratch use on the 144 MiB fixture mount, 150,810,624 bytes of peak cgroup memory, and two active parsers at the configured limit. The fixture also exercised a full scratch mount, parse budget exhaustion, SIGTERM during rsync, an injected membership transaction failure, and successful recovery and retry. NUL and invalid UTF-8 originals, byte offsets, and provenance were checked in the regular Phase 2 run.

| Rebuilt image | Local image ID |
|---|---|
| Sync | `sha256:27c22b6ed9bcc60650fd663af830bc9fb3c2a6dbe4ec07009e5e21ff485f6e2e` |
| Ingestion fixture | `sha256:3b1b03642708d3d40893b875ca7e9772e23c0b43b382a38f943d6bcd36132c56` |
| Publication fixture | `sha256:f44d9a37033870425c6c01aa6d9f896a039b1cac2b496f0347ca3ff848bc3a0a` |
| Daemon fixture | `sha256:da2296e295dbb281db047139d1dfd8281ec4becd274647b4db0454f55e03a8f2` |


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

Branch pushes and pull requests run the frozen Rust checks, all-feature checks, and HTTP mutation smoke on native Linux amd64 and arm64. The amd64 Compose matrix runs Phases 0–7, the Phase 8 restore, legacy Phase 6, the paced bootstrap, and HTTPS publication and fault probes. The arm64 matrix runs Phases 0, 5, and 7 and HTTPS fault probes. Each architecture builds one image bundle. The case jobs verify the archive checksum, source revision, architecture, and loaded image IDs before running the probes. Phase 8 capacity and live upstream checks remain part of local qualification. [CI workflow](../.github/workflows/ci.yml) · [Docker workflow](../.github/workflows/docker-e2e.yml)

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

### Rust 1.100.0 minimum

- On 2026-11-23, Rust `1.100.0` (`0ef1bc386`) passed the offline Phase 0 Rust checks, all-feature Clippy and tests, and all 258 HTTP smoke cases on Linux amd64. Cargo reported `1.100.0` for all six workspace crates. A frozen workspace check with Rust `1.99.1` failed with the expected minimum-version error. Workflow lint and production Compose configuration validation passed.
- The same rebuilt amd64 images passed Phase 0 on rootful and rootless Docker Engine `29.10.0` with Compose `5.8.0`. The probes passed for MCP `2026-07-28` over Streamable HTTP, PostgreSQL TLS and permissions, exact rsync transfer, and Landlock and seccomp. Separate `linux/amd64/v3` builds of serve, sync, and feed passed the entrypoint version checks. The AMD64v3 serve image ran ripgrep `15.2.0` with PCRE2 disabled.
- The image bundle receipt and test logs are under `/tmp/rfc-rust100.EctmDcEt`. Cargo artifacts used `/tmp/rust/target`. Native ARM64 verification remains pending the hosted CI matrix.

### Schema 10 CI build isolation

The amd64 `phase2`, `phase6-legacy`, and `bootstrap-quick` jobs rejected the
schema 10 fixture database. The build log shows `rfc-storage` compiling for
the current checkout and being reused in the pinned historical compile.
Both builds shared the same Cargo target mount.
[Failing CI run](https://github.com/PublicData-stream/rfc-mcp-docker/actions/runs/45605432824)

The image build uses a separate BuildKit instance for the pinned schema 10
checkout. To reproduce a mixed bundle locally, create two distinct
`docker-container` builders on the same Docker context and pass their names
to `scripts/ci/docker-e2e.py build` with `--builder` and
`--schema10-builder`. The bundle carries the image sets for both revisions.

- On 2026-11-25, a native Linux amd64 build compiled `rfc-storage`
  independently on each BuildKit 0.34.1 instance. Rootful Docker 29.10.1
  and rootless Docker 29.10.0 both loaded and verified the 15-image
  archive. A current-only build also passed. The runner rejected absent
  or shared builder names for a mixed bundle before invoking Docker.
- The rootful Compose 5.8.0 replay passed `phase2`, `phase6-legacy`, and
  `bootstrap-quick`. The six-RFC private bootstrap reached a byte-exact
  MCP read at 33.293 seconds. The fixture made 14 HTTPS starts with a
  minimum interval of 1,000,443 microseconds.
- The offline Phase 0 Rust checks and workflow lint passed. The current
  `phase0`, `phase1`, and `phase6` controls passed on rootful Compose.
  Rootless Minikube 1.39.0 with Kubernetes 1.37.0 and
  containerd passed stock preflight, manual and Feed-triggered
  publication, exact MCP reads, authenticated outbox delivery, and repair
  after pod replacement. The warm run reused six TXT and three RFCXML
  originals with five HTTPS starts. Cleanup removed the disposable
  builders, image tags, profile, and both ext4 mounts.

## RustSec dependency checks

The RustSec job audits the committed workspace `Cargo.lock` on every CI run and at 04:17 UTC daily on the default branch. `CI summary` requires RustSec, Rust checks, and Docker E2E to succeed before the release workflow can accept that commit's main-branch CI result.

- On 2026-11-23, the checksum-verified `cargo-audit` 0.23.0 fetched fresh RustSec data and found zero vulnerabilities across 241 locked dependencies. The audit passed with one non-blocking unmaintained warning for `chrono 0.4.45`. [RUSTSEC-2026-0543](https://rustsec.org/advisories/RUSTSEC-2026-0543)
- `actionlint` passed. Local workflow probes rejected missing, empty, symlinked, malformed, and vulnerable lockfiles, an unavailable advisory database, and a corrupted scanner archive. The summary passed only when all three jobs succeeded across 64 status combinations.
- The offline workspace checks, Phase 0 MCP wire probe, frozen all-feature Clippy and tests, and all 258 HTTP smoke cases passed with Cargo artifacts under `/tmp/rust/target`. Hosted CI and scheduled execution remain unverified.

## GHCR image publication

The release workflow verifies signed tags and exact-commit main CI, builds native AMD64v3 and ARM64 service archives, and promotes the tested manifests to three fixed-version OCI indexes. The workflow publishes a digest receipt for Compose and verifies anonymous pulls. [Release procedure](ghcr-release.md)

### Local verification on 2026-11-23

- The offline Rust checks, `actionlint`, and release shell and Python syntax checks passed. The source gate accepted the actual [successful main CI attempt](https://github.com/PublicData-stream/rfc-mcp-docker/actions/runs/45127806469) for `2aef81d`. The gate rejected invalid tag events, a mismatched build suffix, missing CI, and a newer failure following an older successful attempt.
- Native Linux AMD64v3 archives for the local candidate `1.0.0-build.a9e52bdb` reported the stamped version in all three services. The exact same images passed Phase 0 in rootless and rootful Docker Engine `29.10.0` with Compose `5.8.0`, including MCP wire, TLS, permissions, and ripgrep sandbox checks. Trivy `0.76.2` found no advisories in the two Cargo lockfiles and no critical or fixable high findings in the three images using the 2026-11-23 advisory database. The release audit policy also rejected synthetic blocking findings and missing scan results. Evidence is under `/tmp/rfc-release-validation.zlFnDuGR`.
- The pinned BuildKit exported the requested AMD64v3 and ARM64 platform descriptors. A disposable loopback registry verified both descriptors, all three index digests, anonymous downloads, rejection of missing artifacts and conflicting tags, digest-only upload, and partial publication retry. A second rehearsal combined the real AMD64v3 images with ARM64 descriptor fixtures and confirmed that Docker selected and ran all three AMD64v3 binaries by index digest. Registry evidence is under `/tmp/rfc-release-registry.buTb2xnX` and `/tmp/rfc-release-compose-pull.QhAboRrC`.
- Native ARM64 release-image qualification, the hosted release workflow, and anonymous GHCR pulls remain pending for the first authorized release. The release rehearsal used only the loopback registry.

### Stable `latest` promotion on 2026-11-23

- `actionlint .github/workflows/publish-images.yml`, `cargo fetch --locked`, and `scripts/phase0/check-rust.sh` passed. The rebuilt rootless Compose Phase 0 project `rfc-mcp-phase0-cazqtymi` passed the MCP wire, PostgreSQL, rsync and TLS, permissions, and Landlock and seccomp probes on Docker Engine `29.10.0` with Compose `v5.8.0`.
- A disposable loopback registry verified that all three aliases resolved anonymously to the versioned OCI index digests, including both platform descriptors. The rehearsal covered first promotion, idempotent reruns, numerical version ordering, a failed second alias copy and retry, beta and build exclusion, and rejection of invalid annotations and digest conflicts. The fixture and command log are in `/tmp/rfc-latest-registry.lgPhSz`.
- Hosted GHCR promotion and anonymous alias verification are pending the first eligible stable release through the updated workflow.

## ripgrep 16.0.0

- The build pins the signed release's source commit `669cb4c755db20b26c3af3d923c042ae224c7496` and verifies its archive checksum. The initial build failed because ripgrep 16 uses a procedural macro and Cargo passed the static-link flag to the host compiler invocation. An explicit native Cargo target scopes the flag to the worker build. [Upstream release](https://github.com/BurntSushi/ripgrep/releases/tag/16.0.0)
- On 2026-11-23, `cargo fetch --locked` and `scripts/phase0/check-rust.sh` passed with Rust 1.100.0 and `/tmp/rust/target`. The same native AMD64 image bundle passed Phase 0 on rootful and rootless Docker Engine 29.10.0 with Compose 5.8.0 on Linux 7.0.0-44-generic. The serve image `sha256:157bf3ea115ba94a2de6ca9c7dd5ed3dcac1f135dab40e22decc99cd911d94eb` reported ripgrep 16.0.0 with PCRE2 disabled, matched the approved stdin fixture, and passed the Landlock ABI 8 and seccomp denial checks.
- Phases 5 and 7 passed on the same bundle in both Docker contexts. Phase 5 verified original byte ranges, signed pagination, literal option-like patterns, and coverage limits. Phase 7 verified production confinement, worker saturation, cancellation, reaping, and fail-closed behavior. The first rootless Phase 5 attempt hit a loopback port collision with the concurrent rootful proxy. The serial retry passed.
- The AMD64v3 serve image `sha256:c241380a88b8d4d805ad0a25ae8a4f286f242845379161bc756650692704ae8d` also passed rootless Phase 0 with the AMD64 support images. Its worker was static PIE with no ELF interpreter or `NEEDED` entries and reported PCRE2 disabled.
- The exported ripgrep lockfile SHA-256 was `4216c9689e2f80812a51abde30731dbbf5f36d404fda3945bb50f10973336361`. Trivy 0.76.2 used advisory database schema 2, updated at 13:07 UTC on 2026-11-23. Both Cargo lockfiles had zero matched advisories. Each service image had seven medium and four low findings and passed the stable release thresholds.
- The image bundle receipt, runtime patch, probe logs, and audit reports are under `/tmp/rfc-rg16-verify.oUkl52Vh`. Native ARM64 qualification remains pending hosted CI.

## Dual-queue bootstrap implementation Phase 0

The cold-start clock covers empty-volume initialization through an active publication, loopback readiness, and a byte-exact MCP read. The ADR records the proposed TXT trust policy, resource budget, rollback, and the HTTPS request-start floor for the 900-second target. [Cold RFC bootstrap and first publication](decisions/0009-cold-rfc-bootstrap.md)

### Reproduce the private baseline

Use Python 3 with `jsonschema`, OpenSSL, and a Docker Compose host with delegated cgroup v2 CPU and memory controllers. Preload the PostgreSQL image pinned in the Phase 1 Compose file and build the six runtime and fixture images before starting the clock.

```bash
for image in serve:serve sync:sync feed:feed phase0-fixtures:fixtures \
             phase3-fixture:publication-fixture phase6-fixture:control-fixture; do
    docker --context rootless build --target "${image%:*}" -t "rfc-bootstrap-phase0-${image#*:}" .
done
python3 -B scripts/bootstrap/baseline.py --context rootless --image-prefix rfc-bootstrap-phase0 \
    --evidence /tmp/rfc-bootstrap-baseline
```

Use a new evidence directory on every invocation. The runner refuses image pulls and uses a private synthetic source. It runs three fresh stacks for each fixture size and one controlled TXT disagreement. Each trial records separate setup and workflow times, database and notification timestamps, request and payload measures, and sampled resource peaks. It verifies membership, BLAKE3 manifests, byte ranges and exact fixture TXT bytes after the timed read. A failed trial keeps a receipt and any available service logs.

### Observed on 2026-11-23

The six paced trials used benchmark commit `61dd9d1`, the collector behavior at `db38619`, PostgreSQL 19.0, and preloaded images. The x86-64 KVM host provided 18 CPUs and 34.2 GiB RAM. Rootless Docker 29.10.0 and Compose 5.8.0 ran on Linux 7.0.0-44-generic with overlayfs and delegated cgroup v2 limits. The receipts record exact image IDs and the observed CPU and memory limits for Sync, Serve, Feed and PostgreSQL. Those limits matched the comparison profile.

| Fixture TXT count | T0–T1 trials (s) | Median T0–T1 (s) | Median collector wall (s) | Median T0 to observed notification (s) |
| ---: | --- | ---: | ---: | ---: |
| 6 | 27.349, 27.783, 30.712 | 27.783 | 17.140 | 28.514 |
| 96 | 212.995, 213.107, 212.124 | 212.995 | 198.786 | 213.070 |

Every trial published exactly one full fixture generation at sequence 1, returned the same generation and sequence over MCP, and delivered the authenticated notification on its first attempt. The startup jobs recorded zero retries. Each size reproduced its manifest digest. The 96-RFC fixture also reproduced the manifest of the earlier paced candidate benchmark. The TXT disagreement probe recorded a failed generation at 16.033 s with the first retry scheduled, sequence zero, an empty outbox and `/ready` still returning 503. The harness observed the failure before shutting down its disposable stack.

| Fixture TXT count | Rsync payload bytes | HTTPS payload bytes read | Accepted bytes | RFCXML 200 / 404 / 410 / errors | HTTPS starts | Starts/s range |
| ---: | ---: | ---: | ---: | --- | ---: | --- |
| 6 | 27,435 | 32,113 | 32,113 | 3 / 2 / 1 / 0 | 14 | 0.845–0.923 |
| 96 | 2,886,555 | 2,935,483 | 2,935,483 | 33 / 62 / 1 / 0 | 194 | 0.982–0.991 |

The shortest HTTPS start interval was 1,000,184 µs. The 194 starts in the 96-RFC trials impose a 193-second floor between first and last dispatch, compared with the median 198.786-second collector wall time. Across the 96-RFC trials the median cumulative stage times were 0.931 s for rsync, 2.405 s for catalog HTTPS, 63.700 s for TXT HTTPS, 115.460 s for RFCXML HTTPS, 0.022 s for parsing, 20.721 s for blob installation, 4.012 s for DB operations, and 374.855 s for queue waits across 386 waits. These spans overlap. Median PostgreSQL initialization was 3.925 s and migrations took 0.164 s. The JSON receipts include all stages and the separate publication, readiness and notification timestamps.

| Fixture TXT count | Median Sync and child CPU (s) | Peak Sync process RSS (MiB) | Peak Sync cgroup memory (MiB) | Rsync scratch high-water (MiB) | Sampled mirror / PGDATA peak (MiB) |
| ---: | ---: | ---: | ---: | ---: | --- |
| 6 | 0.098 | 12.09 | 8.60 | 0.051 | 0.125 / 48.50 |
| 96 | 0.702 | 12.41 | 14.12 | 1.098 | 3.40 / 66.75 |

PGDATA includes WAL. Disk peaks are five-second samples with a final sample. Scratch is measured at completed rsync batches. Sync CPU here covers the process and its reaped children at candidate completion. The receipts also record the full-workflow cgroup CPU and memory measurements for each service.

Preloaded image inspection and fixture extraction took 4.182 s before T0. In the 96-RFC trials the median database TLS setup was 0.136 s, proxy TLS 0.039 s, and synthetic fixture and upstream TLS preparation 0.045 s. Synthetic control-secret provisioning took 0.817 s. Compose DNS and real TLS handshakes took place inside the workflow. Manual production provisioning and cold registry pulls were outside this private benchmark and remain unmeasured. The trials qualify the measurement contract at fixture scale. The ADR’s 2 h 44 min request-start floor still applies to the 9,861-RFC planning case with the current HTTPS interval.

The full receipts and service logs are under `/tmp/rfc-bootstrap-phase0-evidence/final` on the verification host. Pilot logs are alongside them. Early pilots exposed a Docker metadata format error, a wrong bind source, a duplicate Serve test address, and a mismatched expected error string. The final runs used the corrected harness.

### Offline verification

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and frozen all-feature workspace Clippy and tests passed on Rust 1.100.0 with Cargo artifacts under `/tmp/rust/target`.
- The repository’s offline Compose Phases 0, 1, 2, 3, 6 and 7 passed sequentially with preloaded images on the rootless context. They covered MCP wire and sandbox checks, storage roles and restore, NUL and legacy-byte preservation, publication and recovery, daemon control and authenticated notification, and production Compose confinement. The exit statuses and logs are under `/tmp/rfc-bootstrap-phase0-evidence`.

## Dual-queue bootstrap implementation Phase 1

Sync reads a versioned ingestion TOML at startup. The daemon uses one snapshot for its jobs and hourly catalog probes. Each manual reconciliation uses one snapshot for its candidate. Production Compose requires `RFC_SYNC_CONFIG_HOST_FILE` and mounts it read-only for Sync. Official HTTPS starts are at least one second apart. The collector verifies TXT against the official HTTPS body or an eligible prior proof. [Settings and preflight](ingestion.md#sync-configuration)

### Verification on 2026-11-24

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, frozen all-feature workspace Clippy and tests, `actionlint`, and production `docker compose config --quiet` passed. Cargo used Rust 1.100.0 and `/tmp/rust/target`.
- The RustSec audit scanned 247 locked dependencies and passed with the existing `chrono 0.4.45` unmaintained warning. [RUSTSEC-2026-0543](https://rustsec.org/advisories/RUSTSEC-2026-0543)
- One native Linux amd64 image bundle was built locally and loaded into both Docker contexts. Tests ran on Docker Engine 29.10.0, Compose 5.8.0, and Linux 7.0.0-44-generic. The Rust, audit, image bundle, and Compose logs are under `/tmp/rfc-bootstrap-phase1.DZamDFRu`.
- Offline Compose Phase 0 passed on both engines. The rootless Phase 1 storage, Phase 3 publication and recovery, Phase 6 daemon and control, and Phase 8 destructive restore probes passed. Phase 6 rejected invalid daemon and manual configuration before job, generation, or upstream activity. The daemon completed two publications after the mounted file was changed to an invalid version, confirming that subsequent jobs used the startup snapshot.
- The isolated preflight probes accepted the defaults and the example file, exercised legacy settings and file rejections, and rejected the default profile under a 768 MiB cgroup or a 128 MiB scratch mount. The one-document profile passed with 768 MiB RAM and a 16 MiB tmpfs. Compose rendered the read-only bind exclusively for Sync, including when the tools profile was enabled.
- Phase 2 passed on rootless Docker. The omitted, default-file, and tuned sample profiles produced the same manifest at publication sequence zero. The tuned run used three one-file rsync batches, peaked at one pending document, one HTTPS request and one parser, and measured a shortest HTTPS start interval of 1,050,987 µs for the 1,050 ms setting. The input-cap and occupied-scratch probes rejected their runs. The byte-fidelity, HTTPS fault, cancellation, and recovery checks also passed.
- Phase 7 passed on both engines with the same preloaded images. The initial rootful attempt exposed a fixture helper incompatibility, which was corrected before the passing run. The initial rootless Phase 2 probe timed out waiting for `docker exec ... pidof rsync` during concurrent fixture runs. It passed when rerun in isolation.

## Dual-queue bootstrap implementation Phase 2

Sync supports one persistent rsync session for each catalog-selected TXT set on a dedicated capped ext4 filesystem. Admission starts after a successful transfer and exact inventory check. The default profile still uses strict batched ingestion. The optional bulk bootstrap policy records rsync-only evidence on schema 7 and requires HTTPS comparison for new or changed TXT after the first publication. [Ingestion settings](ingestion.md#sync-configuration) · [Bulk staging](deployment.md#bulk-rsync-staging)

### Verification on 2026-11-24

- The locked Phase 0 Rust checks and all-feature Clippy and tests passed. Cargo artifacts were under `/tmp/rust/target`. The default and bulk Compose configurations rendered successfully. Only Sync had the writable bulk staging mount.
- The local strict bulk fixture transferred 36 selected files in one rsync session. The warm run reported 36 checksum hits and zero transfers, with the same manifest. Changing one file while preserving its size and timestamp resulted in one replacement transfer. An upstream symlink over a cached name stopped admission.
- The six-RFC opt-in bootstrap fixture recorded six rsync-only TXT observations and zero TXT HTTPS requests. It preserved CRLF, NUL and invalid UTF-8 bytes and verified their offsets. Missing and oversized upstream files, catalog drift, timeout, SIGTERM, and exhausted staging bytes or inodes preserved the active publication. The fixture also rejected a stale bootstrap candidate, quarantined an HTTPS mismatch, and verified strict fallback and retained-generation rollback.
- The offline bulk daemon fixture published and delivered its authenticated outbox event. Serve reported ready and returned byte-exact RFC 2119 through MCP revision `2026-07-28`.
- The upgrade drill published a corpus with schema 6 binaries, migrated it to schema 7, reread the original bytes, and confirmed that the manifest, historical source identities and strict reuse survived.
- Phase 0 passed with the same final service images on rootless and rootful Docker Engine 29.10.0 and Compose 5.8.0. The rootless Phase 1 storage, Phase 3 publication and recovery, Phase 4 search, Phase 5 MCP, Phase 6 daemon and control, and Phase 7 isolation suites passed on schema 7. The Phase 8 destructive restore passed, verifying two readable generations, the active pointer, canonical blobs, manifests, byte ranges and pinned MCP reads. Logs and image receipts are under `/tmp/rfc-dual-phase2.pvcZ1jY9` on the qualification host.
- The standard Phase 2 suite and the bulk bootstrap suite passed on the final images in both Docker contexts. The first standard rootless run timed out starting the lock-contention fixture during concurrent Compose tests. The retry passed.

Verification used private fixtures. Official full-corpus acquisition and the 900-second first-publication measurement remain open. The current HTTPS request-start interval is still at least one second in the official profile.

## Dual-queue bootstrap implementation Phase 3

Sync schedules RFCXML as soon as the starting catalog selects RFC IDs. The bounded lookahead overlaps with rsync and TXT parsing. A shared HTTPS gate enforces the configured request spacing and concurrency, including RFCXML retries and provider cooldowns. The reconciliation report distinguishes missing XML, malformed captures, and fetch failures. [Pipeline and retry policy](ingestion.md#pipeline-and-recovery)

### Verification on 2026-11-24

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and fixture-enabled Clippy passed with Rust 1.100.0 and Cargo artifacts under `/tmp/rust/target`. The offline Phase 0 Compose probes passed on rootless and rootful Docker Engine 29.10.0 with Compose 5.8.0 on Linux amd64.
- The rootless Phase 2 suite verified byte ranges, coverage, source evidence, request spacing, and cancellation. The six-RFC paced run made 14 HTTPS starts, reached two in-flight requests, and recorded a minimum start spacing of 1,000,414 µs. The three-slot window reserved 48 MiB of raw payload capacity. The single-slot profile reserved 16 MiB.
- The bulk bootstrap fixture held rsync until the first three XML requests started, then held the first XML response until TXT parsing finished. The HTTP faults exercised 404, 410, malformed and wrong-RFC XML, 429 and 503 cooldowns, retries after 502 and interrupted bodies, timeouts, and HTTP body validation errors. SIGTERM during a response, retry wait, and a long shared cooldown preserved the active generation.
- The rootless Phase 2 strict bulk, Phase 3 publication and recovery, Phase 6 standard and bulk bootstrap, and Phase 7 isolation suites passed. The bulk daemon published its first fixture generation, delivered the authenticated notification, returned a successful `/ready`, and served the original bytes over MCP.
- A fixed-clock, unpaced six-RFC comparison against the Phase 2 images reproduced the manifest `ca0734a5fe6b5a3c700344a0695dd3756e8935a64357f8f48e2a31a393f6fb5e`, 32,113 accepted bytes, and 14 HTTPS starts. The one run per version took 9.323 s before the change and 10.101 s after it. This comparison verifies manifest equivalence at fixture scale. Logs and receipts are under `/tmp/rfc-dual-phase3` on the qualification host.

Verification used private fixtures. At the official one-second start interval, 9,861 RFCXML requests plus two catalog requests require at least 2 h 44 min 22 s between the first and last dispatch. A full official bootstrap through publication and readiness remains unmeasured. [Cold-start measurement contract](decisions/0009-cold-rfc-bootstrap.md#900-second-gate)

## Dual-queue bootstrap implementation Phase 4

Sync seals each accepted TXT index while its RFCXML request can still be pending. The join matches the generation, starting catalog digest and RFC number. A later RFC can commit membership, evidence and coverage and free its slot while an earlier XML request is held. Both commits use the single corpus writer, and the existing manifest serialization determines output order. [Pipeline and recovery](ingestion.md#pipeline-and-recovery)

### Verification on 2026-11-24

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and all-feature Clippy and tests passed with Rust 1.100.0 and Cargo target directory `/tmp/rust/target`.
- The offline Phase 0 Compose probes passed on rootful and rootless Docker Engine 29.10.0, Compose 5.8.0, and Linux amd64.
- The rootless Phase 2 standard suite verified the XML outcome matrix, candidate-only reconciliation, original bytes and evidence ranges, and recovery after an injected membership transaction error. The 33-RFC resource fixture processed 4 MiB TXT bodies and an 8 MiB RFCXML response. It used three slots and two parser workers, with 48 MiB reserved for payloads, 32.0 MiB peak Sync RSS, and 152.6 MiB peak cgroup memory. The one-slot profile reserved 16 MiB for payloads. The default paced fixture recorded a minimum HTTPS start spacing of 1,000,909 µs.
- While the private fixture held RFC 100's XML response, PostgreSQL showed its sealed TXT index, RFC 101's committed membership, and the candidate still building. The HTTP fixture also recorded the newly admitted RFC 2119 request before RFC 100 was released.
- The rootless Phase 2 strict bulk and bulk bootstrap suites passed. Bulk bootstrap preserved the active generation and outbox across a parser panic, catalog drift and SIGTERM after early TXT sealing. Recovery pruned the unreferenced sealed index. The rootful Phase 3 suite passed publication, interrupted reconciliation, reader fencing and restore.
- The rootful Phase 6 standard and bulk bootstrap suites passed. The bulk daemon published its first fixture generation, delivered the authenticated outbox event, returned a successful `/ready`, and served original bytes over MCP revision `2026-07-28`.
- The rootful Phase 7 probes verified the hardened Compose deployment, worker saturation and reaping, and fail-closed behavior with simulated missing Landlock.
- The six-RFC fixed-clock comparison with `3d03dc7` matched all 3,847 manifest bytes, 32,113 accepted bytes, and 14 HTTPS starts. Both runs independently checked original bytes and evidence ranges. Collector wall time was 8.780 s on Phase 3 and 13.143 s on Phase 4 in single, unpaced trials during local qualification. Cumulative CAS installation time was 0.894 s and 6.493 s respectively. These timings characterize the local fixture runs.

Logs and fixture receipts are under `/tmp/rfc-dual-phase4` on the qualification host.

## Dual-queue bootstrap implementation Phase 5

Schema 8 binds new dual-source TXT matches to separate HTTPS and rsync observations. Schema 9 retains bounded restart checkpoints for verified TXT and successfully parsed RFCXML. Reuse preserves the original observation identities and timestamps. Known catalog drift invalidates the attempt's checkpoints, and readiness clears them atomically. Sync starts HTTPS requests at least one second apart by default and limits new rsync-only TXT to first publication. Public MCP continues to serve the existing `2026-07-28` wire schema. [Ingestion](ingestion.md#interrupted-reconciliation) · [Upgrade and rollback](deployment.md#initialize-or-upgrade)

### Verification on 2026-11-24

- The locked Rust gate and all-feature Clippy and tests passed with Rust 1.100.0 and Cargo artifacts under `/tmp/rust/target`.
- The offline Phase 0 probes passed on rootful and rootless Docker Engine 29.10.0, Compose 5.8.0, and Linux 7.0.0-44-generic amd64. Rootless storage, search, and MCP passed. Rootful daemon and confinement suites passed. The bulk daemon fixture delivered the authenticated publication event, reached `/ready`, and served the original bytes over MCP.
- The populated schema 7 upgrade and migration rerun preserved exact observation IDs, timestamps and manifest bytes. Guarded Serve reads confirmed the active and retained generations. A subsequent strict candidate reused all six legacy TXT proofs without a TXT HTTPS request.
- The rootful strict bulk and bulk bootstrap fixtures passed. The strict warm run reused 36 TXT proofs and reproduced the cold fixture manifest. Changing one staged TXT required one new byte comparison. The bootstrap fixture exercised rsync-only provenance, capped staging faults, promotion fencing and retained-generation rollback.
- The isolated rootful standard ingestion suite passed, including the 33-RFC resource fixture with 4 MiB TXT and an 8 MiB RFCXML response. The HTTPS timeout fixture reused five saved TXT proofs and made one TXT HTTPS request on retry. Earlier rootless runs exposed assertions that assumed no checkpoint reuse and a transient Docker exec timeout in the rsync cancellation poll.
- The isolated rootful publication and recovery suite passed after the concurrent rootless run timed out waiting for a fixture reader in the advisory-lock queue. The checkpoint probe covered crash boundaries, pinning, forced synchronous commits, both cache limits, expiry, catalog drift and corruption. A six-RFC retry reused five TXT proofs and one RFCXML capture, fetched the changed TXT again, recovered the missing XML, and recorded a fresh 404 for expired XML. The corruption case kept the suspect file for repair and preserved the published original bytes.
- The rootful destructive restore rehearsal passed after creating the required evidence directory under `/tmp`. It verified both readable generations, the active pointer at sequence 2, canonical blobs, pinned MCP reads and the restored runtime role boundaries.

Verification used disposable Compose projects and private upstream fixtures. Logs and native image receipts are under `/tmp/rfc-phase5-checks`. Official full-corpus startup through publication and readiness remains unmeasured.

## Dual-queue bootstrap implementation Phase 6

Sync reports aggregate progress every 60 seconds during an active generation. The terminal candidate report and the confirmed daemon publication record share the job and generation IDs. The operator workflow uses `--check-config`, then either starts the daemon or explicitly promotes a candidate created with `--phase2-reconcile --full`. [Fresh-stack runbook](deployment.md#initialize-fresh-volumes) · [Reconciliation progress](ingestion.md#reconciliation-progress)

### Reproduce the private cold-start trials

Use the preloaded images and Python prerequisites from the Phase 0 baseline above. Use a host that permits the disposable ext4 loop mount and disk sampling for the test mirror under `/tmp`. Supply the preloaded image prefix and a new evidence directory.

```sh
python3 -B scripts/bootstrap/baseline.py --phase6 --context default \
    --image-prefix "$IMAGE_PREFIX" --evidence /tmp/rfc-bootstrap-phase6
```

The runner creates two independent cold six-RFC stacks using the same bulk configuration, synthetic source contents and fixed source capture times. It checks the daemon job and authenticated delivery. On the manual stack, the private fixture driver calls the shared manual engine, and the Sync CLI performs the explicit sequence-fenced promotion. The runner checks sequence zero and an empty outbox after reconciliation, then the pending notification and serving readiness after promotion. The final comparison verifies identical manifest bytes, including relations and coverage. A separate strict TXT disagreement exercises the failure and retry path.

### Observed on 2026-11-24

The final cold trials ran on an 18-CPU amd64 KVM host with 34.2 GiB RAM, rootful Docker Engine 29.10.0, Compose 5.8.0, and cgroup v2 on Linux `7.0.0-44-generic`. A native image bundle was built from the Phase 6 Sync source, then the same images were used on rootful and rootless Docker. The cold fixtures used preloaded images and PostgreSQL 19.0.

| Six-RFC workflow | Ingestion wall (s) | T0 to published and `/ready` (s) | T0 to byte-exact MCP `get` (s) |
| --- | ---: | ---: | ---: |
| Daemon | 9.794 | 40.564 | 40.602 |
| Manual candidate and explicit promotion | 9.798 | 22.867 | 22.906 |

Both published the same manifest and original bytes at sequence 1, using eight HTTPS starts at least one second apart. Each recorded three XML captures, two 404s, one 410, 27,435 transferred TXT payload bytes, and 32,113 accepted bytes. The admitted-document and XML window peaks were three. The receipts include sampled disk allocations for PostgreSQL, the mirror, and staging, along with Sync and service memory peaks. An initial trial exposed Serve's expected inability to sample private mirror directories. The final trials sampled the same disposable mirror from the host and recorded no disk sampling errors.

The daemon job finished without retries and its authenticated notification was observed at 40.687 s after one attempt. The manual trial reached `ready` at sequence zero with an empty outbox and `/ready` still returning 503. Explicit promotion created one pending outbox entry and Serve read the newly active generation. The strict disagreement was observed at 19.879 s with a failed generation, a retry scheduled, no outbox entry, sequence zero and `/ready` returning 503. The two final success timings are single fixture observations taken while other rootless probes ran. Full-corpus timing and the 900-second gate remain Phase 8 work.

### Offline verification

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and `cargo clippy` and `cargo test` with `--workspace --all-targets --all-features --frozen` passed on Rust 1.100.0. Rust artifacts were under `/tmp/rust/target`.
- The preloaded Phase 0 Compose probes passed on both Docker engines. Rootless Phase 1 storage, Phase 2 ingestion, Phase 6 daemon and control, and Phase 7 production isolation passed. The standalone rootful Phase 6 `--bulk-bootstrap` probe passed, including the job, generation and publication-log assertions. The original timed runner also passed its six-RFC strict fixture and disagreement control.
- The first rootless Phase 3 run failed an assertion on the result of terminating a fixture reader's PostgreSQL session. The same suite passed on rootful Docker, including candidate-only sample rejection, publication, reader and GC fencing, restore, and checkpoint recovery.
- The native image receipt, exact probe invocations, logs and final timing receipts are under `/tmp/rfc-phase6-checks.JLVfUB8l`. The bundle was built from the working tree before the Sync commit was signed, so its revision field names the parent. The `cold-final/` timing receipts record the signed Sync commit and the same image IDs.

## Dual-queue bootstrap implementation Phase 7

The existing Compose gates cover configuration preflight, original-byte ingestion, request and queue bounds, failure recovery, authenticated publication, and service confinement. The local profiles add a 3,072-RFC cold bootstrap, populated historical upgrades, and a paced comparison with the original serial collector.

### Reproduce the local qualification

Use the preloaded native application and fixture images, PostgreSQL image, and Python dependencies from the earlier gates. Build the `sync` and `phase2-fixture` targets from signed revisions `e2e5a41` and `968673e` for the historical profiles. Use the matching Sync and ingestion-fixture tags under each historical prefix. The capped staging probes require a host that can make and mount disposable ext4 images with `sudo -n`.

Use a separate BuildKit builder for each checkout revision when building the comparison images on one host. With a shared builder, give each revision a separate Cargo target-cache ID and force the affected compilation stages to run. Changing the mount ID alone can reuse a previously built layer.

```sh
export RFC_E2E_PRELOADED=1 TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/rust/target
evidence=$(mktemp -d /tmp/rfc-dual-phase7.XXXXXXXX)

cargo fetch --locked
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
python3 -B scripts/phase8/http-smoke.py
actionlint
docker compose --env-file deploy/compose.env.example config --quiet
RFC_SYNC_RSYNC_STAGE_DIR="$evidence/render-stage" docker compose -f compose.yml \
    -f deploy/compose.bulk-rsync.yml --env-file deploy/compose.env.example config --quiet

for context in rootless default; do
    python3 -B scripts/phase0/integration.py --context "$context" --project "$IMAGE_PREFIX"
    python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$IMAGE_PREFIX"
done
for phase in 1 2 6; do
    python3 -B "scripts/phase${phase}/integration.py" --context rootless --image-prefix "$IMAGE_PREFIX"
done
python3 -B scripts/phase3/integration.py --context default --image-prefix "$IMAGE_PREFIX"
python3 -B scripts/phase2/integration.py --context default --image-prefix "$IMAGE_PREFIX" --bulk
python3 -B scripts/phase2/integration.py --context default --image-prefix "$IMAGE_PREFIX" --bulk-bootstrap
python3 -B scripts/phase2/integration.py --context default --image-prefix "$IMAGE_PREFIX" \
    --upgrade-from "$SCHEMA6_PREFIX"
python3 -B scripts/phase2/integration.py --context default --image-prefix "$IMAGE_PREFIX" \
    --upgrade-from-schema7 "$SCHEMA7_PREFIX"
python3 -B scripts/phase6/integration.py --context default --image-prefix "$IMAGE_PREFIX" --bulk-bootstrap
python3 -B scripts/bootstrap/baseline.py --phase6 --context default --image-prefix "$IMAGE_PREFIX" \
    --evidence "$evidence/cold"
python3 -B scripts/bootstrap/baseline.py --phase7-scale --context default --image-prefix "$IMAGE_PREFIX" \
    --evidence "$evidence/scale"
python3 -B scripts/phase2/integration.py --context default --image-prefix "$IMAGE_PREFIX" \
    --benchmark 96 --repeats 3 --serial-image-prefix "$SCHEMA6_PREFIX" --receipt "$evidence/benchmark.json"
```

The scale profile accelerates requests inside the isolated fixture and preserves production TOML validation. The serial comparison uses the one-second request-start interval for all three profiles and ends at a validated candidate. Retain the scale and benchmark receipts separately when evaluating the Phase 8 first-publication objective.

### Private 3,072-RFC bootstrap on 2026-11-24

The preloaded rootful Compose stack published at sequence 1. The collector finished in 639.409 s, `/ready` responded after 687.992 s, and the byte-exact MCP `get` completed at 688.037 s from initialization. Authenticated notification delivery was observed at 688.792 s. The same host ran other disposable qualification workloads during this trial.

One rsync session transferred 3,072 TXT originals totaling 97,261,467 bytes. The fixture recorded zero TXT HTTPS requests and 3,074 total HTTPS starts at an observed 4.81 starts/s. RFCXML returned 1,025 captures, 2,046 HTTP 404s, and one HTTP 410. Every selected RFC had the expected original and XML association. The read-only verifier rehashed 4,098 blobs and checked 3,155,147 ranges and line starts. The queue peaked at three documents with 48 MiB reserved for raw bodies and two in-flight HTTPS requests. Peak Sync cgroup memory was 313.2 MiB at candidate completion. The sampled staging, mirror, and PostgreSQL peaks were 94.6 MiB, 100.1 MiB, and 184.0 MiB respectively.

The accelerated trial measures only the private fixture. The official HTTPS schedule and the full-corpus 900-second gate remain Phase 8 work.

### Offline regression on 2026-11-24

- The host used native amd64, 18 vCPUs, 34.2 GiB RAM, cgroup v2, Docker Engine 29.10.0 in both contexts, Compose 5.8.0, and Rust 1.100.0. Locked Cargo checks, all-feature Clippy and tests, 258 seeded HTTP mutations, workflow lint, and both production Compose renders passed.
- Offline Phase 0 and Phase 7 confinement passed on both engines with rebuilt stock Sync image `sha256:4aad68653aec90a555cea17b17af285bbda621a23f340bc4fdfe76975db956c6`. An earlier stock build reused historical Cargo artifacts and failed the schema check. A separate target cache and forced compilation resolved the mismatch. Rootless Phase 1 storage, Phase 2 ingestion, and Phase 6 control passed. Rootful Phase 3 verified publication, retained readers, recovery, checkpoint expiry and corruption, GC, and restore.
- The ingestion gate measured 1,000,814 µs minimum request-start spacing. Its 33-RFC resource case staged 128 MiB, used 128.004 MiB of scratch, and peaked at 152.3 MiB of Sync cgroup memory. The writer-gated run filled three XML slots with 48 MiB reserved, kept further XML requests waiting, and reproduced the control manifest. The first attempt hit the Sync role's advisory-lock permission boundary. The test now acquires its lock inside a restricted trigger in the disposable database, using the existing runtime grants.
- The rebuilt fixture CLI left the six-RFC manual candidate at `ready` with sequence zero, an empty outbox, and `/ready` returning 503. Explicit promotion then served the exact original bytes and matched the daemon's manifest byte for byte. The daemon delivered its authenticated notification. The strict TXT disagreement control kept sequence zero and scheduled a retry.
- Rootful strict bulk and bulk bootstrap passed. The fault cases included a mid-transfer rsync disconnect, invalid staged names, unselected files, empty and oversized TXT, binary signatures, exhausted staging bytes and inodes, parser panic, SIGTERM, and catalog drift. Releasing rsync and RFCXML on different schedules reproduced the same manifest. Rollback restored the first generation and its byte-exact reads.
- Populated schema 6 and 7 stores migrated to schema 9 twice. Both preserved legacy observation IDs, timestamps, manifest bytes, and guarded reads of published and retained generations. An unchanged run reused all six legacy TXT proofs. A changed RFC with differing source bytes was rejected. Once both sources agreed, Sync recorded a new paired rsync and HTTPS proof and reused the other five legacy proofs.

### Paced 96-RFC comparison on 2026-11-24

Three cold candidates per profile used identical synthetic bytes, request delays, host limits, and one-second start pacing. The historical collector used revision `e2e5a41`, schema 6, and its pinned Rust 1.99.1. Both current profiles used schema 9 and Rust 1.100.0. Other qualification workloads ran on the host during some trials.

| Profile | Collector wall trials (s) | Median wall (s) | RFCs/s at median | Median CPU (s) | Peak Sync cgroup memory (MiB) | Max PostgreSQL / mirror allocation (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| Historical serial | 322.338, 269.029, 305.979 | 305.979 | 0.314 | 0.486 | 12.04 | 66.78 / 3.40 |
| Current one-slot | 325.807, 248.832, 261.883 | 261.883 | 0.367 | 0.634 | 8.05 | 67.16 / 3.40 |
| Current pipelined | 205.206, 211.341, 195.711 | 205.206 | 0.468 | 0.625 | 9.62 | 67.18 / 3.40 |

All nine runs made 194 HTTPS starts and accepted 2,935,483 bytes. The shortest start interval was 1,000,208 µs. The current one-slot and pipelined profiles reserved 16 MiB and 48 MiB of raw payload capacity respectively. The read-only verifier validated each manifest. The two current profiles produced byte-identical manifests. Across both schemas, the comparison matched RFC membership, originals, byte ranges, relation evidence, and coverage.

The receipts and logs are under `/tmp/rfc-dual-phase7.peGcsqOy` on the qualification host. `benchmark.json` holds the nine runs and their image IDs. `scale/summary.json` and `cold-cli/summary.json` hold the cold publication evidence. `compose-results.txt` and `forced-image.log` record the probe exit statuses and the final image identities.

## Dual-queue bootstrap implementation Phase 8

Phase 8 adds cold and incremental trials to the private Compose benchmark, a six-RFC paced CI case, an offline image-import probe, and a schema 9 configuration rollback rehearsal. The production defaults remain strict batched TXT verification and at least one second between HTTPS starts. Persistent bulk with `rsync_bootstrap` is an opt-in for operators who accept rsync-only TXT evidence on first publication. [Settings and reproduction commands](ingestion.md#bootstrap-qualification) · [Strict rollback](deployment.md#return-to-strict-batched-ingestion)

### Qualification environment

The qualification host was an amd64 KVM guest on an AMD EPYC 4585PX with 18 vCPUs and 34.2 GiB RAM. It ran Linux `7.0.0-44-generic`, Docker 29.10.0 in both contexts, Compose 5.8.0, overlayfs, cgroup v2, PostgreSQL 19.0, and Rust 1.100.0 for the current native images. Timed trials used the rootful context and the preloaded `rfc-dual-phase7-rebuilt` image set. Each receipt records exact image IDs and the observed service CPU and memory limits. The initial quick run and cold pilot recorded source base `35c6523` with benchmark changes in the working tree. The definitive full suite used `eb8cdd9` plus the source-policy probe correction in the working tree. Image inspection and fixture binary extraction took 5.557 s before the full suite.

### Full-size private fixture

The three cold trials initialized fresh PostgreSQL, mirror and ext4 staging storage before publishing all 9,861 selected TXT originals. All reached sequence 1, returned HTTP 200 on `/ready`, and served a byte-exact RFC 2119 over HTTPS at MCP revision `2026-07-28`. The incremental passes retained sequence 1 for readers during ingestion and published sequence 2. Each reused 9,851 TXT proofs, obtained ten new dual-source matches, and verified the changed synthetic original under the existing source policy. All six authenticated notifications delivered on the first attempt. The cold manifests reproduced exactly across the three pairs and the pilot.

| Workflow completion (seconds) | Trial 1 | Trial 2 | Trial 3 | p50 | p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cold through MCP read | 1,364.022 | 1,313.921 | 1,339.594 | 1,339.594 | 1,364.022 |
| Incremental through changed-original verification | 584.257 | 462.585 | 460.715 | 462.585 | 584.257 |

The p95 is the nearest-rank estimate, which equals the maximum with three samples. At the median, the workflows processed 7.361 RFCs/s cold and 21.317 RFCs/s incrementally. Cold collector wall time ranged from 1,283.691 to 1,333.668 s. The candidate-to-publication log interval ranged from 15.919 to 18.789 s. The cold medians for rsync, blob installation, database work and XML requests were 117.126, 587.070, 580.527 and 142.287 s respectively. These cumulative stage spans overlap. Blob installation and database work accounted for the largest measured service times as the three-slot queue filled. The median incremental collector took 460.005 s, including 1.308 s for rsync, 342.383 s for blob installation, and 104.010 s for database work.

Each leg attempted 9,891 XML requests and sent 71,978,002 XML response body bytes. Each recorded 3,313 valid captures, including ten recovered retries, alongside 6,477 HTTP 404s, 51 HTTP 410s, ten malformed captures and ten exhausted retries. The median observed XML arrival rate was 7.537 starts/s cold and 21.568 starts/s incremental. Queue occupancy peaked at three documents and 48 MiB of reserved raw buffers. Median cumulative queue waits were 2,188.789 s cold and 675.354 s incremental.

Cold Sync used 44.163–45.005 process and child CPU s, with a maximum process RSS of 24.281 MiB and a maximum container memory peak of 1,145.867 MiB. Sync's cgroup consumed 35.262–38.217 additional CPU s during each incremental workflow. From cold start through incremental completion the process and container peaks reached 40.504 and 1,163.309 MiB. Cold sampled disk peaks reached 502.71 MiB for PostgreSQL, 407.11 MiB for the canonical mirror, and 323.56 MiB for bulk staging. The incremental peaks were 519.54, 407.49, and 323.60 MiB. Each cold verifier checked 13,185 blobs and 4,919,686 ranges and line starts. Each incremental verifier checked 13,195 blobs, 4,920,976 ranges and line starts, and both readable manifests.

### Paced 96-RFC comparison

Both current profiles used identical fixture bytes, the one-second HTTPS start interval, and fresh data. The strict batched profile made 194 HTTPS starts per trial. Bulk bootstrap made 98 by using rsync-only TXT evidence at sequence zero. The shortest observed spacing was 1,000,257 µs. Each run delivered the publication notification and each profile reproduced its manifest across its three trials.

| Current profile | Cold through MCP read (seconds) | p50 | p95 | Median collector wall (seconds) |
| --- | --- | ---: | ---: | ---: |
| Strict batched | 221.123, 207.513, 213.095 | 213.095 | 221.123 | 196.307 |
| Persistent bulk bootstrap | 119.504, 116.681, 119.914 | 119.504 | 119.914 | 100.960 |

The separate `scripts/phase2/integration.py --benchmark 96 --repeats 3 --serial-image-prefix rfc-p7-e2e5a41` comparison timed the historical and current collectors through candidate validation. The historical build used schema 6 and the pinned Rust 1.99.1 toolchain. The current profiles used schema 9. All nine runs made 194 HTTPS starts, read 2,935,483 HTTPS payload bytes, and passed the content and manifest verification. The minimum start spacing was 1,000,239 µs.

| Candidate collector | Collector wall trials (seconds) | p50 | p95 |
| --- | --- | ---: | ---: |
| Historical serial | 260.330, 254.870, 269.679 | 260.330 | 269.679 |
| Current one-slot | 249.622, 250.827, 242.348 | 249.622 | 250.827 |
| Current pipeline | 196.199, 195.754, 197.194 | 196.199 | 197.194 |

### Production feasibility

For the 9,861-RFC planning case, persistent bulk TXT still leaves 9,863 HTTPS starts for RFCXML and the two catalog captures. The one-second production interval imposes a 9,862-second floor from the first to the last start, or 2 h 44 min 22 s. Strict cold TXT verification takes 19,724 starts with a 19,723-second floor, or 5 h 28 min 43 s. Reserving 90 seconds each for initialization and publication leaves 720 seconds and requires approximately 13.7 or 27.4 starts per second, respectively, before retries. The production objective is infeasible at current pacing. [Timing contract](decisions/0009-cold-rfc-bootstrap.md)

### Observed checks

- The six-RFC paced quick profile reached sequence 1 and a byte-exact HTTPS MCP read at 30.809 s. It made 14 HTTPS starts, with minimum start spacing of 1,000,649 µs. Its six XML attempts delivered 662 response body bytes and the publication notification delivered on the first attempt. A local invocation of the `bootstrap-quick` case through the CI runner also passed and wrote the summary that the workflow uploads.
- The full-size pilot published and passed cold verification in 1,364.218 s. Its incremental pass published sequence 2, then the probe requested public original bytes from a metadata-only synthetic RFC and received `source_policy_restricted`. The definitive suite used the corrected policy-aware probe above.
- The strict rollback rehearsal preserved sequence 1, the incumbent MCP read and one delivered outbox entry through a deliberate TXT disagreement. After the fixture sources agreed, the retry obtained six matching HTTPS proofs and published sequence 2 with a new delivered outbox entry. The verifier checked both readable manifests and the unchanged historical proof IDs.
- The cold image-cache probe exported 174,190,592 bytes in 4.981 s and imported the checksum-verified archive into an empty image store in 20.072 s. Isolated backend and daemon startup took 0.356 s and warm image-ID inspection took 0.081 s. An early run exposed Docker auto-detecting the host containerd socket. The successful run used a dedicated temporary containerd backend. Registry pulls remain unmeasured.

### Offline verification

- `cargo fetch --locked`, `scripts/phase0/check-rust.sh`, and frozen all-feature workspace Clippy and tests passed with Cargo artifacts under `/tmp/rust/target`. The HTTP smoke passed 258 seeded mutations. Workflow lint and the bind-backed, bulk-stage, and Docker-managed mirror Compose renders passed. The Docker-managed render retained the service-specific mirror access modes.
- `python3 -B scripts/phase0/integration.py --context CONTEXT --project rfc-dual-phase7-rebuilt --skip-build` and the Phase 7 integration script passed in both `default` and `rootless`. Phases 1–5 passed on `default` using `python3 -B scripts/phaseN/integration.py --context default --image-prefix rfc-dual-phase7-rebuilt`. A build of the Phase 4 fixture hit an empty BuildKit dependency cache. The probe passed with the existing schema 9 search fixture, image `sha256:2245c322aea18a6646bd96223890bc5d7bc170a23fd5bfe7489df88f344dc4e9`.
- The standard rootful Phase 6 run timed out waiting for its post-restart outbox to drain. The same suite passed on `rootless`, covering authenticated control, crash recovery, and scheduled reconciliation. The rootful Phase 2 and Phase 6 `--bulk-bootstrap` runs passed, including fault cases and authenticated first-publication delivery.
- `python3 -B scripts/phase8/integration.py --context default --image-prefix rfc-dual-phase7-rebuilt --evidence "$evidence/restore"` passed the destructive restore. The restored pointer was `2|2` and both published and retained generations were readable. The verifier checked 16 canonical blobs, 794 ranges and line starts, and three manifests. Public compatibility checks passed over HTTPS at MCP revision `2026-07-28`.

### Files and evidence

- `scripts/bootstrap/` contains the added benchmark profiles, image-cache timing, and shared initialization. `scripts/phase2/endpoint.py` generates private per-RFC HTTP timing and outcome receipts. The Docker CI workflows and runner add the paced quick case to native amd64.
- `deploy/` and the README, ingestion, deployment and storage guides document the recognized configurations, first-publication checks, and backup or rollback procedures. The offline evidence directory is `/tmp/rfc-dual-phase8.kNsyYHym` on the qualification host. It contains `full-final/summary.json`, `paced/summary.json`, `serial.json`, `rollback-final/summary.json`, `image-cache-final/image-cache.json`, `restore/restore.json`, and the individual command logs and exit statuses.

## Contributor and security guidance

- On 2026-11-24, `scripts/phase0/check-rust.sh` passed with the pinned Rust toolchain and the existing locked dependency cache. The gate covered formatting, the frozen build, Clippy, workspace and documentation tests, documentation warnings, and the Phase 0 MCP wire probe. Cargo artifacts used `/tmp/rust/target`.
- Checked the guide links and anchors, shell example syntax, and referenced repository paths. The Codex Security resolver discovered the root policy and applied it to the repository root, `apps/`, and `crates/`.
- This documentation change used the offline gate. Runtime Compose probes apply when runtime dependencies, images, or isolation change.

## HTTPS-first RFC synchronization Phase 0

The baseline is `376fdc65c37c331ea433ebdb57ef70e7afbfdfbe` with PostgreSQL
schema 9 and ingestion TOML schema 1. The future collector uses HTTPS for
normal acquisition and mandatory rsync checksum audits for required TXT.
The first publication under that policy needs eligible HTTPS and audit
evidence for every required TXT. The new ADR records how to handle old
proofs, checkpoints, configuration and schema rollback.
[HTTPS-first decision](decisions/0010-https-first-rfc-synchronization.md)

### Current execution paths

| Area | Baseline behavior and source |
| --- | --- |
| Entry points and coordination | Sync without arguments loads config once, takes scheduler leadership (advisory lock `-3`), and queues startup work. The daemon schedules a daily full reconcile and an hourly catalog change check. One-shot `--phase2-reconcile --full\|--sample` calls the same collector. The daemon, manual execution, promotion and recovery share the corpus coordinator lock (`-1`). [CLI](../crates/sync/src/main.rs) · [Daemon](../crates/sync/src/daemon.rs) · [Coordinator](../crates/sync/src/lib.rs) |
| Feed hints and notifications | Feed polls RSS or Atom with conditional validators and a nominal 900 s interval, using its own HTTPS client and backoff. Private HMAC control messages carry stable event identity, observation digest, time and reason. Sync records event IDs and coalesces jobs under the queue transaction lock (`-4`). Serve receives generation, sequence and manifest hints via a separate authenticated link. [Feed](../apps/src/feed.rs) · [Schemas](../apps/src/control/mod.rs) · [Wire and authentication](../apps/src/control/wire.rs) |
| Selection and acquisition | Sync selects from its starting HTTPS catalog, stages TXT through rsync, and starts bounded RFCXML lookahead. Config v1 selects batched or persistent staging and eligible TXT proof. The Sync start gate covers catalog, TXT, RFCXML and retries. Parse and durable installation precede TXT sealing, and classified RFCXML is joined by RFC identity for membership. [Config](../crates/sync/src/config.rs) · [Upstream](../crates/sync/src/upstream.rs) · [Bulk](../crates/sync/src/bulk.rs) · [Pipeline](../crates/sync/src/pipeline.rs) · [Ingest](../crates/sync/src/ingest.rs) |
| Bytes, checkpoints and recovery | TXT parsing preserves original bytes and indexes byte ranges, including bytes mode for NUL-bearing originals. Catalog and RFCXML parsing apply separate outcome and coverage rules. Bounded restart checkpoints pin original observations and bind to catalog digest, scope, policy, sequence and parser, index and registry versions. Recovery reclaims abandoned candidate work and honors retained readers. [TXT](../crates/sync/src/text.rs) · [XML](../crates/sync/src/xml.rs) · [Checkpoints](../crates/sync/src/checkpoint.rs) · [Recovery](../crates/sync/src/recovery.rs) |
| Readiness and publication | Ending catalog equality and candidate validation precede manifest freeze. Manual full reconcile stops at `ready`. Explicit publish or daemon promotion verifies the sequence and commits the active pointer and outbox together. Serve checks the active generation on `/ready` and reads through generation guards. [Publication](../crates/sync/src/publication.rs) · [Serve routes](../apps/src/public.rs) |

The [migration runner](../migrations/migrate.sql) holds advisory lock
`-2` and executes as the schema owner. Migrations 0001–0006 cover storage,
ingestion, publication, search, reviewed pagination and control. Migration
0007 records rsync provenance and guards promotion of rsync-only proofs.
Migration 0008 guards the pairing of byte-matched HTTPS and rsync
observations. Migration 0009 adds bounded ingestion checkpoints and their
blob references. [Migration inventory](../migrations/) ·
[Storage schema check](../crates/storage/src/postgres.rs)

[Production Compose](../compose.yml) requires a read-only Sync config file
mount. The [schema 9 bulk overlay](https://github.com/PublicData-stream/rfc-mcp-docker/blob/376fdc65c37c331ea433ebdb57ef70e7afbfdfbe/deploy/compose.bulk-rsync.yml) adds a private
writable staging bind to Sync. The [runtime image targets](../Dockerfile)
use the default Rust features. The `phase6-fixture` target builds separate
Sync and Feed binaries with fixture features. Sync's fixture features
include the private origin and pacing controls used by the benchmark.
The [deployment examples](../deploy/) describe the current v1 config and
service-scoped secrets.

### Paced private baseline on 2026-11-24

Six cold trials ran serially on an amd64 KVM guest with 18 vCPUs and
34.2 GiB RAM, Linux `7.0.0-44-generic`, rootful Docker `29.10.0`, Compose
`5.8.0`, and PostgreSQL `19.0`. Sync used 2 CPUs, 2 GiB RAM, and 512 MiB
scratch. The preloaded stock Sync image was
`sha256:4aad68653aec90a555cea17b17af285bbda621a23f340bc4fdfe76975db956c6`.
The control fixture image was
`sha256:c2990a19b6d5c0ea0909a84f718e71eb5bb46619b4a3e7d794da61c4c83497b6`.
The runtime source inputs matched `35c6523` by Git diff, and the image
IDs matched the original build and qualification receipts. Image checks
and fixture binary extraction took 3.734 s outside T0–T1. Legacy bulk
staging setup took 0.308, 0.327 and 2.874 s before T0.

The fixture used the same 96 RFCs and source bytes across all six trials.
`--check-config` reported v1 defaults for strict batched `dual_source` and
a v1 file for persistent bulk `rsync_bootstrap`. Both reported two
in-flight HTTPS permits, 1,000 ms start spacing, three pending documents,
one parser worker, a 30 s HTTPS timeout and up to three RFCXML attempts.
Strict batched required 136 MiB of scratch and 1,080 MiB of memory in
preflight. Legacy bulk required 8 MiB of scratch and 952 MiB of memory,
plus a separate capped staging filesystem.

| Profile | Trial | Collector (s) | Published observed (s) | Ready observed (s) | T1 MCP read (s) | Outbox delivered (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Strict batched | 1 | 196.699 | 213.537 | 213.635 | 213.675 | 213.751 |
| Strict batched | 2 | 201.268 | 224.495 | 224.591 | 224.637 | 225.053 |
| Strict batched | 3 | 197.299 | 218.911 | 219.026 | 219.066 | 219.806 |
| Legacy bulk | 1 | 103.795 | 115.732 | 115.832 | 115.870 | 115.949 |
| Legacy bulk | 2 | 103.527 | 119.752 | 119.925 | 120.035 | 120.236 |
| Legacy bulk | 3 | 100.450 | 120.225 | 120.323 | 120.362 | 120.441 |

The collector column measures reconciliation wall time. The other columns
measure from fresh-volume initialization. Each trial published all 96 RFCs
at sequence 1, passed loopback `/ready`, served the exact RFC 2119 bytes
over HTTPS at MCP revision `2026-07-28`, and delivered its authenticated
outbox event. The database publication timestamps and observed milestones
are in the receipts. Manifests reproduced within each policy. Strict's
T1 median and worst were 219.066 and 224.637 s. Legacy bulk's were 120.035
and 120.362 s.

| Measurement | Strict batched | Legacy bulk |
| --- | ---: | ---: |
| HTTPS starts per trial (catalog / TXT / RFCXML) | 194 (2 / 96 / 96) | 98 (2 / 0 / 96) |
| Minimum HTTPS start spacing (µs) | 1,000,243 | 1,000,284 |
| Peak in-flight HTTPS | 2 | 2 |
| HTTPS payload bytes per trial | 2,935,483 | 48,928 |
| Rsync sessions per trial | 3 | 1 |
| Rsync transferred TXT files and payload bytes per trial | 96 / 2,886,555 | 96 / 2,886,555 |
| Peak validated staging inventory bytes | 1,127,680 | 2,886,555 |
| Median durably sealed TXT per collector second | 0.487 | 0.927 |
| Median cumulative catalog / TXT HTTPS / RFCXML HTTPS spans (s) | 2.406 / 63.632 / 115.316 | 2.406 / 0.000 / 115.318 |
| Median cumulative rsync / parse / CAS install spans (s) | 0.935 / 0.021 / 27.379 | 1.251 / 0.020 / 26.389 |
| Median cumulative database / candidate validation / queue spans (s) | 5.557 / 0.007 / 607.220 | 9.491 / 0.009 / 152.491 |
| Mean DB span latency by trial (ms) | 24.163 / 60.814 / 19.034 | 31.569 / 73.362 / 41.267 |
| Peak Sync process and child CPU (s) / RSS (MiB) | 0.733 / 12.414 | 0.633 / 12.242 |
| Peak Sync cgroup CPU (s) / memory (MiB) | 1.707 / 15.668 | 0.669 / 17.277 |
| Peak scratch / bulk staging allocation (MiB) | 1.098 / inapplicable | unreported / 2.980 |
| Sampled mirror / PostgreSQL allocation peaks (MiB) | 3.402 / 67.188 | 3.402 / 66.898 |

Each trial classified 33 RFCXML captures, 62 HTTP 404s and one HTTP 410.
Admission and XML lookahead both peaked at three, with 48 MiB reserved
for raw bodies. The paced strict case spent about 196–201 s in the
collector, consistent with its 194 starts at the one-second floor. Stage
spans can overlap. The DB latency is mean elapsed time across 230
instrumented application operations per trial. PostgreSQL-only commit
timing is outside this baseline. The receipts include individual setup
phases, fixture and manifest digests, and per-service cgroup maxima.

### Archived full-size capacity

The archived receipt records source base
`eb8cdd96de6178502eac40459f5a6df5c80c8bd5` with a working-tree probe
correction. It used the same preloaded image IDs on rootful Docker with
fixture-only acceleration. Its three 9,861-RFC legacy bulk cold trials reached
sequence 1 and a byte-exact MCP read in 1,364.022, 1,313.921 and 1,339.594 s.
The median was 1,339.594 s and the worst was 1,364.022 s. The corresponding
incremental trials changed ten RFCs, verified sequence 2, and completed
in 584.257, 462.585 and 460.715 s. Their median was 462.585 s and the worst
was 584.257 s. These are historical capacity measurements for the legacy
policy. Phase 8 of the HTTPS-first plan owns full-size qualification of
the replacement. [Archived dual-queue results](#full-size-private-fixture)

### Other Phase 0 observations

- A one-shot stock Sync service ran `--check-config` with the example
  file mounted read-only by Compose and networking disabled. It
  reported source `file`, the same strict v1 values as the timed
  default profile, and the 2 GiB cgroup limit.
- Host rsync was `3.4.1` (protocol 32). The packaged Alpine client was
  `3.5.2` (protocol 33), with checksum choices xxh128, xxh3, xxh64, MD5
  and MD4 plus `none`. Both advertised `--list-only`, `--checksum` and
  `--checksum-choice`. The packaged binary listed both files in a private
  daemon module. The first dry run skipped the module directory. With an
  explicit two-file list, `--dry-run --checksum --checksum-choice=auto`
  reported only the changed RFC despite matching sizes and mtimes, and
  preserved both local files. The trial establishes local compatibility.
  RFC Editor's rsync version, negotiated checksum, and availability of an
  equivalent RFCXML module remain unverified. All RFC acquisition in
  Phase 0 used private fixtures.
- The six-RFC manual and daemon rehearsal produced identical manifest
  bytes. The daemon reached T1 at 21.113 s and delivered the notification at
  21.524 s. Manual reconciliation reached a ready candidate at 35.307 s
  with sequence zero, `/ready` 503, and an empty outbox. Explicit promotion
  reached T1 at 36.586 s with one pending notification. The strict
  disagreement control failed at sequence zero without an outbox entry.
- The schema 9 strict rollback rehearsal kept sequence 1 ready and
  byte-readable through a deliberate TXT disagreement. After the
  fixture sources agreed, the daemon verified all six HTTPS TXT matches,
  published sequence 2, and delivered a new notification. Both manifests
  remained readable and the historical TXT observation IDs matched the
  pre-rollback snapshot.
- The Phase 0 Compose probes passed on rootful and rootless Docker. They
  exercised the packaged runtime images, MCP wire behavior, exact rsync
  transfer, TLS and PostgreSQL roles, and ripgrep with Landlock and
  seccomp. The cached locked dependency check and offline Rust gate
  passed, including formatting, frozen builds, Clippy, workspace and
  documentation tests, and the local MCP wire probe.

The host-local receipts and logs are under
`/tmp/rfc-https-first-phase0.Rd25Kx0h`. The paced summary, parity and
rollback summaries, image identities, stock config check, and both
rsync attempts are saved there.

### Reproduce the private checks

Use a verified native image set and a host that supports disposable ext4
staging. Run the timed trials serially. The existing harness names refer
to phases of the dual-queue implementation.

```sh
export TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/rust/target RFC_E2E_PRELOADED=1
evidence=$(mktemp -d /tmp/rfc-https-first-phase0.XXXXXXXX)
image_prefix=rfc-dual-phase7-rebuilt

python3 -B scripts/bootstrap/baseline.py --phase8 paced --context default \
    --image-prefix "$image_prefix" --evidence "$evidence/paced"
python3 -B scripts/bootstrap/baseline.py --phase6 --context default \
    --image-prefix "$image_prefix" --evidence "$evidence/parity"
python3 -B scripts/bootstrap/baseline.py --phase8 rollback --context default \
    --image-prefix "$image_prefix" --evidence "$evidence/rollback"
for context in default rootless; do
    python3 -B scripts/phase0/integration.py --context "$context" \
        --project "$image_prefix" --skip-build
done
cargo fetch --locked --offline
scripts/phase0/check-rust.sh
```

## HTTPS-first RFC synchronization Phase 1

The Sync runtime accepts ingestion TOML schema 2 and runs shared
discovery for manual reconciliation, daemon startup, scheduled work,
and Feed hints. It keeps PostgreSQL schema 9 and the incumbent
publication. A sealed selection can supply the later HTTPS queues.
New candidate publication stays gated. [Configuration and discovery](ingestion.md) ·
[Rollout decision](decisions/0010-https-first-rfc-synchronization.md#phase-1-rollout)

Discovery uses the original opening and ending HTTPS catalog captures,
a bounded list-only inventory from packaged rsync 3.5.2, and streamed
BLAKE3 verification of selected canonical TXT. The Feed control
payload retains its existing event identity and digest. Audit and
publication evidence are delivered in later phases.

### Verification on 2026-11-24

- The offline Rust gate passed, including the MCP wire probe. All-feature
  Clippy and tests passed. The separate Phase 2 and Phase 3 fixture-feature
  Clippy passes also passed.
- The Phase 0 Compose runtime probes passed serially on the rootful and
  rootless engines. They exercised rsync, TLS verification, PostgreSQL
  authorization, and the Landlock and seccomp restrictions. The checked
  images were built on rootless Docker and copied to the rootful engine.
- The disposable minikube test passed on Kubernetes 1.37.0 with the
  rootless Docker driver and containerd. The stock Sync image ran as UID
  10002 and rejected a write to its ConfigMap. The mounted file supplied
  schema 2, and `--check-config` observed a 2 GiB cgroup limit and
  512 MiB scratch. The profile and its temporary files were removed.
  Rootful image building and the first minikube start encountered a
  missing default `docker0` bridge. The rootless setup resolved both.
- The private discovery suite passed on both Compose engines. Manual and
  daemon reports sealed the same cold selection. Completed cold and warm
  runs recorded two catalog HTTPS starts with minimum spacing above
  one second and zero HTTPS TXT or RFCXML requests. Tests covered missing
  sources, Feed hints before catalog and mirror, duplicate hints, a
  frozen startup snapshot, catalog drift, malformed inventory, deadline
  and SIGTERM cleanup, and corrupt canonical TXT. The same-size,
  same-mtime change left the inventory digest unchanged and the
  separate checksum dry run detected the changed bytes.
- The populated fixture kept its candidate count, active sequence, source
  observations, and outbox through discovery. The stock CLI rejected
  promotion of an old ready candidate. The incumbent stayed ready and
  served exact RFC 2119 bytes. Explicit rollback then reactivated the
  retained generation and served the same bytes at the next sequence.
  The initial rootless probe collided with the daemon's fixed IP when
  testing invalid config. Executing the extra process inside the daemon
  container resolved it. The first rootful run timed out polling for the
  rsync child. The isolated rerun passed with a fixture-side signal.
- The targeted Phase 7 config-only probes passed on both engines.
  They verified v2 defaults and migration errors, denied undersized
  resources, and confirmed the mandatory Sync-only read-only bind in
  the production Compose configuration and a one-shot container.
  A broader rootless Phase 7 attempt timed out in the historical
  fixture digest helper. The broader rootful attempt passed its
  isolated preflight before reaching a stale v1 assertion, which
  was corrected along with the targeted probe.

All RFC requests in these checks used private fixtures. The production
upstream inventory and the cold-start timing target remain to be
qualified in the later phases.

To repeat the targeted Compose checks with matching local test images,
set `RFC_E2E_PRELOADED=1` and use their common image prefix.

```sh
python3 -B scripts/phase0/integration.py --context rootless --project "$prefix" --skip-build
python3 -B scripts/https-discovery/integration.py --context rootless --image-prefix "$prefix"
python3 -B scripts/phase7/integration.py --config-only --context rootless --image-prefix "$prefix"
```

Run the same commands with context `default` on a rootful engine.

## HTTPS-first RFC synchronization Phase 2

Manual and daemon reconciliation run independent bounded HTTPS TXT and
RFCXML acquisition through one deployment lease. Feed polls through
Sync's authenticated broker. Completed runs seal originals and
classifications in a private disk spool and confirm the ending catalog.
The spool is removed after each run. PostgreSQL stays at schema 9 and
new publication remains gated. [Acquisition](ingestion.md#run-acquisition) ·
[Shadow runbook](deployment.md#https-first-phase-3-shadow-runbook)

### Offline verification on 2026-11-25

- The frozen workspace gate passed, including the MCP wire probe. Frozen
  all-feature Clippy and tests passed. The scheduler test exercised lane
  rotation and cancellation, and the operator test exercised attachment
  and a single queued different scope.
- Phase 0 Compose passed on both Docker engines. It exercised PostgreSQL
  authorization, exact rsync, checked TLS, MCP, and Landlock and seccomp
  confinement. The config-only probes passed spool ownership and
  preflight, the Sync-only read-only config bind, and Feed's
  internal-only network on both engines.
- The rootless acquisition suite passed. Its cold six-RFC run made 14
  HTTPS starts and staged 28,097 artifact bytes with a peak of three
  active artifact jobs. Its minimum start intervals were 1,000,861 µs
  on the cold run and 1,001,080 µs on the warm run. The warm run reused
  incumbent HTTPS TXT and eligible RFCXML. Tests verified exact staged
  bytes while a response in the other queue was held, one-slot
  backpressure, spool and accepted-byte limits, differing XML outcomes,
  deterministic content summaries, and a parser panic. Both
  `Retry-After` forms held across a forced restart. The fixture also
  checked Feed pacing and hint convergence, manual routing, the
  promotion gate, incumbent reads, and retained rollback.
- The rootful acquisition run passed the cold, fault, pacing, Feed, and
  operator cases. Its warm run reused all six TXT and three RFCXML
  originals and made five HTTPS starts. The idle-catalog assertion
  timed out because the fixture passed the container variable where
  Compose expected its host setting. After correcting the setting, the
  focused rootful rerun passed the idle probe and retained rollback at
  a warm minimum start interval of 1,000,655 µs. The same focused
  rerun passed rootlessly at 1,000,570 µs. An earlier rootless fault
  probe was corrected to target an RFC with an XML response so that
  it reached the parser.
- A disposable minikube profile used the rootless Docker driver,
  Kubernetes 1.37.0, and containerd. The stock images passed the
  preflight pod. Sync ran as UID 10002, used the read-only v2 ConfigMap
  and its private spool, and reported a 2 GiB cgroup limit and 512 MiB
  scratch. Feed had neither mount. A second profile verified that the
  stock CLI reached a same-UID Unix socket peer across containers and
  returned its sample report. The Compose fixture exercised the actual
  daemon and denied an invocation using UID 10003. Both profiles and
  their files were removed.

All acquisition requests in the qualification use private upstream
fixtures. Official-source performance and production rollout remain
separate from this evidence.

With the matching local images loaded, run these probes in each engine
context. Use `default` for the rootful engine and `rootless` for the
rootless engine.

~~~sh
export RFC_E2E_PRELOADED=1
python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix" --skip-build
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix"
python3 -B scripts/phase7/integration.py --config-only --context "$context" --image-prefix "$prefix"
~~~

Add `--incumbent-only` to the acquisition command to repeat the warm
reuse, idle catalog, and rollback cases in a fresh fixture.

## HTTPS-first RFC synchronization Phase 3

Manual and daemon reconciliation install original HTTPS TXT and RFCXML
in BLAKE3 CAS and seal TXT indexes in PostgreSQL schema 10. Bounded
checkpoints preserve observations and TXT indexes across failures. The
coordinator joins by exact selection and RFC and confirms the ending
catalog. TXT awaits audit. The active generation and outbox stay on
their existing publication path.
[Processing and restart](ingestion.md#restart-durable-processing) ·
[Shadow runbook](deployment.md#https-first-phase-3-shadow-runbook)

### Offline verification on 2026-11-25

- The frozen Rust gate passed, including the MCP wire probe, and frozen
  all-feature Clippy and tests passed. The keyed-join regression checked
  stale selections, duplicates, incomplete joins, and digest equality
  across result orders.
- Phase 0 and the production config-mount probes passed on Docker Engine
  29.10.0 (rootless) and 29.10.1 (rootful). The rebuilt stock Sync image
  was `sha256:8bdd218ff0f3704ab906ad5c8e6550f38d0cfc842b7a70a1fa57b1938fbaabee`.
  Phase 0 exercised rsync, TLS, PostgreSQL authorization, and Landlock and
  seccomp confinement.
- The populated schema 9 copy migrated twice on rootless Docker. Historical
  source identities, methods, timestamps, and manifest bytes matched
  exactly. The incumbent served exact RFC 2119 bytes, the audit gate
  rejected capture-only membership, and retained rollback served those
  bytes at the next sequence. Warm minimum request-start spacing was
  1,000,904 µs.
- Minikube 1.39.0 with Kubernetes 1.37.0, Docker rootless, and containerd
  passed stock Sync and Feed preflight and a fixture Sync pod replacement.
  The replacement reused six TXT and three RFCXML checkpoints. The requests
  fell from 14 to five and the processing digest and checkpoint identities
  and expiries matched. Publication sequence stayed zero.
- The processing fixtures on both Docker engines verified 28,097 staged
  bytes across six RFCs, with at most three active artifact jobs. They
  checked exact CAS bytes, NUL and invalid UTF-8 bytes mode, crash recovery
  on both sides of CAS installation, checkpoint commit and catalog
  validation, corruption rejection, expiry, and garbage collection.
  Unchanged repeat runs made five HTTPS starts against 14 on cold runs.
- Both engines passed the held-lane and acquisition fault cases. Focused
  scheduler runs verified an unrelated HTTPS request during a delayed
  checkpoint write, one-slot and filesystem bounds, both `Retry-After`
  forms across restart, daemon operator routing, Feed convergence, and
  retained rollback. The cold and warm minimum start spacing was
  1,000,334 µs and 1,000,645 µs on rootless Docker and 1,000,516 µs and
  1,000,935 µs on rootful Docker. The scheduler runs used separate
  disposable projects from the crash and acquisition faults.
- Production-profile confinement passed on both engines, including
  concurrent worker limits, disconnect and deadline reaping, and
  corpus-fence release. The publication suites on both engines passed
  accepted reader fencing, retirement and unlink interruption, matched
  backup restore, and checkpoint recovery.

Early qualification found a missing checkpoint function in Sync's
privilege allowlist and a rootful PostgreSQL startup timeout. The final
images include the privilege correction, and the rootful Phase 0 rerun
passed. A rootless CAS byte-check attempt timed out starting an extra
container. The fixture now reads the test mirror directly. The rootless
crash fixture uses one active slot so it reaches a TXT checkpoint before
a bodyless XML result. The first timing probes missed the delayed write.
The focused reruns use timestamps from PostgreSQL and the HTTPS fixture.
The rootless publication suite passed after the harness targeted the
accepted reader by its held advisory lock and terminated the session
before starting GC. The Minikube setup needed a local tag for the pinned
PostgreSQL image and a writable socket directory. It also retried
transient API write timeouts.

Use matching local stock and fixture images with a common prefix to repeat
the Compose probes. Add `--scheduler-only` for the focused timing and
routing run. The upgrade case also takes the matching schema 9 publication
fixture prefix.

~~~sh
export TMPDIR=/tmp RFC_E2E_PRELOADED=1
python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix" --skip-build
python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$prefix" --config-only
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix"
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" \
  --upgrade-from-schema9 "$schema9_prefix"
python3 -B scripts/phase3/integration.py --context "$context" --image-prefix "$prefix"
python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$prefix"
~~~

For the Phase 3 pod replacement check, use the Phase 3 checkout, a dedicated
rootless Docker Minikube profile, and preloaded pinned PostgreSQL and
matching fixture images.

~~~sh
probe_root=$(mktemp -d /tmp/rfc-processing-kube.XXXXXXXX)
export MINIKUBE_HOME="$probe_root" KUBECONFIG="$probe_root/kubeconfig" TMPDIR="$probe_root"
export DOCKER_CONTEXT=rootless
minikube config set rootless true
minikube start -p rfc-processing --driver=docker --container-runtime=containerd \
  --kubernetes-version=v1.37.0 --cpus=2 --memory=4096 --disable-driver-mounts
python3 -B scripts/https-discovery/minikube.py --profile rfc-processing --image-prefix "$prefix"
minikube delete -p rfc-processing
rm -r -- "$probe_root"
~~~

All acquisition requests were to private fixture services. The full
official corpus startup target remains unmeasured.

## HTTPS-first RFC synchronization Phase 4

Implementation `aa2ce14` audits required HTTPS TXT with the packaged rsync
checksum dry run. A mismatch requires fresh HTTPS and rsync originals with
exact byte equality before Sync reseals the HTTPS checkpoint. The
Sync-only audit filesystem retains bounded receipts, attempts, and
quarantined pairs. Schema 10, the promotion gate, incumbent MCP reads, and
explicit rollback retain their earlier behavior.
[Audit and repair](ingestion.md#audit-and-repair) ·
[Shadow deployment and rollback](deployment.md#https-first-phase-4-shadow-runbook)

### Offline verification on 2026-11-25

- The frozen Rust gate passed, including the MCP wire probe. Frozen
  all-feature Clippy and tests also passed. The first Rust run exposed an
  outdated positive assertion for the smaller container profile. The
  updated assertion allows for the audit memory reservation. The parser
  regressions also reject wrong kinds, unexpected and duplicate names,
  incomplete receipts, disabled checksums, and oversized output.
- Phase 0 passed on rootful Docker 29.10.1 and rootless Docker 29.10.0.
  The shadow Compose config checks passed on both engines and verified the
  Sync-only audit and spool mounts, rejection of a missing or RAM-backed
  audit mount, config ownership, and resource preflight. One concurrent
  rootless Phase 0 launch reported an unhealthy PostgreSQL container.
  The isolated rerun of the committed image set passed.
- The packaged rsync 3.5.2 loopback check emitted explicit unchanged rows
  with XXH128 on protocol 33 and with MD4 when forced to protocol 29.
  A daemon offering only `none` failed negotiation with exit 4. The
  independently checked one-file rsync repair receipt described an exact
  four-byte fixture transfer.
- The final Sync image was
  `sha256:3a641f150b6a5acfccd8eb55d07cb225287576b10e981b4e0cc7de5c03990fdb`.
  The six-RFC cold fixture made 14 HTTPS starts and staged 28,097
  artifact bytes with at most three active artifact jobs. Both Docker
  engines recorded six explicit checksum matches using a 27,435-byte
  snapshot and zero rsync payload transfers. Each checksum session took
  109 ms in these runs.
- The audit suite passed on both Docker engines. The probes exercised
  held-audit overlap, missing and truncated sessions, timeout recovery,
  local snapshot mutation, metadata-only and
  content changes, exact repaired CAS, and restart across persisted receipts
  and repair attempts. It checked shared HTTP cooldown, transient convergence,
  persistent divergence, retry exhaustion, and the quarantine item cap.
  A preliminary run stopped at the quota probe because the earlier image
  charged each stored pair at its maximum possible size. The committed
  image accounts for the stored pair sizes. An earlier rootful convergence
  fixture supplied identical source bodies. The corrected fixture waits
  for a quarantined divergent pair before updating HTTPS.
- The populated rootless Compose fixture exercised persistent same-size
  disagreement, corrupt CAS, missing incumbent files, startup audit and
  checkpoint reuse, the promotion gate, and explicit retained rollback.
  The incumbent remained ready and served exact RFC 2119 bytes. The warm
  daemon run had a minimum HTTPS start spacing of 1,000,680 µs.
- The rootless scheduling probe passed. It observed HTTPS progress during
  a delayed checkpoint, verified the one-slot resource bounds, and exercised both
  forms of `Retry-After` across SIGKILL. It also checked daemon attachment
  and queuing for manual requests, plus Feed hint coalescing and source
  convergence.
- A disposable rootless Minikube 1.39.0 cluster ran Kubernetes 1.37.0 with
  containerd. Stock preflight and Feed isolation passed. Replacing a crashed
  Sync pod sealed its durable repair pair without another repair request.
  Cold and warm runs made 14 and five HTTPS starts, respectively. The
  replacement reused the other audit receipts and left publication sequence
  zero. The first pod correctly rejected the node's RAM-backed `/tmp`.
  The built-in 9p mount could not attach in the rootless node, so the
  rerun used a private 1 GiB ext4 image from the host's `/tmp` mounted
  at a node path under `/tmp`. The profile and mount were removed after
  qualification.

### Reproduce the private checks

Use matching stock and fixture images with a common prefix on each
engine. Run Compose timing probes serially for each engine. The
qualification used the pinned PostgreSQL 19.0 image and disposable
ext4 staging.

~~~sh
export TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/rust/target RFC_E2E_PRELOADED=1
cargo fetch --locked --offline
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix" --skip-build
python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$prefix" --config-only
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --audit-only
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --incumbent-only
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --scheduler-only
~~~

For Minikube, use a disposable profile with `MINIKUBE_HOME`, `KUBECONFIG`,
and `TMPDIR` under `/tmp`. Attach a disk-backed audit directory beneath
the node's `/tmp` and make it writable by UID/GID 10002. The probe creates
and removes its own subdirectory there.

~~~sh
python3 -B scripts/https-discovery/minikube.py --profile "$profile" \
  --image-prefix "$prefix" --audit-node-dir /tmp/rfc-audit-disk
~~~

Logs, image IDs, exit statuses, and the host mount helper are under
`/tmp/rfc-audit-qual.uQ1SHxN9` on the qualification host.

All RFC requests used private fixtures. Official rsync compatibility,
official corpus timing, and new-policy publication are later gates.

## Phase 8 Compose restore regression

The AMD64 restore job failed when Phase 8 called the Phase 7 deployment
constructor without the HTTPS spool. The shared deployment fixture now
provisions separate disk-backed spool and rsync audit mounts and releases
them after Compose teardown.
[Failed CI job](https://github.com/PublicData-stream/rfc-mcp-docker/actions/runs/45551939950/job/138998232001)

On 2026-11-25, the frozen Phase 0 Rust gate passed, including the MCP wire
probe. An injected probe exception confirmed that the shared fixture
closes the stack before releasing the two mounts.

Phase 0 Compose passed on both engines. The full Phase 7 probe passed on
rootful Docker, including service recreation and worker confinement. The
config-only probe passed on rootless Docker and verified the spool and
audit mount preflight.

The Phase 8 CI runner passed on native AMD64 with rootful Docker Engine
29.10.1 and rootless Docker Engine 29.10.0, both with Compose 5.8.0. Both
reports restored generation 2 at publication sequence 2, with generation
1 retained and generation 3 ready. Each verified 16 canonical blobs, 794
persisted ranges and line starts, and three manifests. Each rejected a
missing blob and a same-length corrupted blob before opening the restored
proxy and replaying all five MCP reads over HTTPS.

The rootless capacity probe passed for 1,000 and 10,000 synthetic RFCs.
Both profiles completed at concurrency 1, 4, 16, and 32, exercised
admission limits, and returned to zero active readers, requests, and
ripgrep workers. The disposable Compose resources, mounts, and loop
devices were released after the probes.

## HTTPS-first RFC synchronization Phase 5

Schema 11 seals the selected RFCs, ending catalog fence, RFCXML outcomes,
and TXT audit receipts in manifest format 3. Sync checks the audit window,
published sequence, canonical blobs, and manifest before promotion.
Manual reconciliation creates a candidate. Automatic promotion completes
the job and associated control hints in the pointer and outbox transaction.
Schema 9 remains the qualified production deployment.
[Shadow runbook](deployment.md#https-first-phase-5-shadow-runbook) ·
[Publication](publication.md#operator-commands)

### Private verification on 2026-11-25

- The frozen Rust gate, all-feature Clippy and tests, GitHub workflow
  lint, exported output schema validation, and local MCP wire checks
  passed. The HTTP mutation probe replayed 258 fixed-seed cases.
- Phase 0 Compose passed on rootful Docker 29.10.1 and rootless Docker
  29.10.0. The six-RFC rootless publication fixture made 14 HTTPS starts
  and recorded six checksum matches without rsync payload transfers. It
  passed manual publication, MCP source verification, repair replay,
  retained rollback, and automatic promotion after restart at readiness.
- The rootless scheduler probe passed at the normal HTTPS pace. It covered
  checkpoint backpressure, cooldown across SIGKILL, daemon attachment,
  manual scope queuing, and publication after Feed hints converged with the
  catalog and mirror. The rootful configuration probe verified the Sync-only
  spool and audit mounts and resource preflight.
- The rootless audit probe passed receipt replay, snapshot mutation,
  missing and truncated sessions, timeout recovery, exact-byte repair,
  divergence, cooldown, and quarantine bounds. The rootful storage probe
  restored two identical format 3 manifests into fresh volumes and verified
  the original ranges, role isolation, and readiness immutability.
- The final rootful publication probe rejected sample promotion, blocked a
  competing writer under the corpus lock, and promoted exactly once.
  PostgreSQL rejected missing, cross-selection, wrong-original, expired,
  and stale-sequence evidence. The daemon resumed a ready generation and
  delivered one outbox event for its sequence.
- The populated schema 10 migration ran twice with identical historical
  observations and manifests. The first schema 11 candidate downloaded
  all six HTTPS TXT originals and audited them. It refetched malformed
  historical RFCXML and preserved byte-exact retained rollback with the
  original rsync-only provenance. The pinned schema 10 acquisition and
  quick bootstrap cases passed on rootful Docker.
- The rootful search probe checked 124 queries against its independent
  graph evaluator. Its 10,000-RFC plans resolved proof metadata with
  one indexed row per evaluated RFC. The rootless isolation probe passed
  the production confinement, worker saturation, cancellation, and reaping
  checks.
- The final rootless MCP probe verified all five tools, original-byte
  ranges, and authenticated continuations across publication, rollback,
  and retirement. The rootful and rootless restore probes each verified
  16 canonical blobs, 794 byte ranges and line starts, three manifests,
  and the published pointer at sequence 2. They replayed all five tools
  through a restored HTTPS proxy and rejected missing and corrupted originals
  before startup. Discarding a sample removed its evidence, while recovery
  preserved the separate rsync observation for a retained repair.
- The isolated rootless checkpoint retry passed interrupted commits, quota
  eviction, expiry, corrupt-original rejection, and garbage collection.
  The first rootless publication lifecycle run timed out waiting for its
  fixture reader to queue on an advisory lock. The full rootful rerun
  passed publication, accepted-reader and post-lock tombstone checks,
  retirement and unlink recovery, backup and restore, and checkpoint
  recovery.
- Minikube 1.39.0 with Kubernetes 1.37.0 and containerd published the cold
  candidate and a repaired replacement. The replacement pod reused the
  durable pair without a new repair request. The warm run reused six TXT
  and three RFCXML observations, with five HTTPS starts versus 14 cold.
  A later namespace deletion timed out after the assertions had passed.
  The disposable profile and external mount were removed afterward.

### Repeat the checks

Use a private image prefix and run timing-sensitive Compose probes
serially. Use the schema 10 fixture driver from the pinned pre-upgrade
revision `29896b51f048e319ddadebc36ff0fe24ab85c7bf` for the populated
upgrade. The older acquisition and bootstrap policy cases in the Compose
matrix use matching images and scripts from that revision. Storage and
graph probes use synthetic records to exercise schema 11 constraints and
byte ranges independently of transport. The publication probe uses actual
private HTTPS and rsync observations.

~~~sh
export TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/rust/target RFC_E2E_PRELOADED=1
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix" --skip-build
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --publication-only
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --upgrade-from-schema10 "$schema10_prefix"
~~~

Use a disposable Minikube profile with `MINIKUBE_HOME`, `KUBECONFIG`, and
`TMPDIR` under `/tmp`. Attach a private disk-backed audit directory under
`/tmp` on the node and run `scripts/https-discovery/minikube.py` with that
directory and the same image prefix. All upstream traffic in these checks
used fixture services. Official-corpus timing and production cutover remain
separate gates.

## HTTPS-first RFC synchronization Phase 6

Base Compose uses schema 11 and requires the read-only v2 Sync TOML and two
private disk-backed staging mounts. The Sync daemon and one-shot commands
share the audited HTTPS-first engine. Feed polls through Sync's authenticated
broker and uses only the internal control network. The retired collector and
its compatibility probes run from pinned source. [Deployment and upgrade](deployment.md) ·
[Acquisition policy](ingestion.md)

### Private verification on 2026-11-25

- The frozen Rust gate, all-feature Clippy and tests, and `actionlint`
  passed. The HTTP boundary probe replayed 258 fixed-seed mutations.
- Base Compose rendered on rootful Docker 29.10.1 and rootless Docker 29.10.0.
  Phase 0 passed with identical image IDs on both engines, including PostgreSQL
  TLS and grants, the pinned MCP wire contract, rsync byte equality, and worker
  confinement. The isolated rootless run passed after the concurrent startup
  timeout.
- Base Compose configuration preflight passed on both engines after
  isolated retries. It verified the read-only v2 file, Sync-only staging,
  effective resource limits, missing and invalid staging rejection, and
  Feed isolation. The first two preflight runs hit Docker command timeouts.
- The cold rootless daemon rejected v1 configuration and invalid audit
  ownership before queuing work. On startup it swept an abandoned 1 GiB
  acquisition directory and published the complete fixture at sequence 1.
  It rejected invalid notification replies and delivered the original outbox
  event after restart. A scheduled run published again with Feed stopped.
- The isolated rootless scheduler probe passed delayed-checkpoint overlap,
  one-slot backpressure, both forms of `Retry-After` across SIGKILL,
  daemon attachment, and different-scope queuing. Authenticated hints
  coalesced and waited for catalog and mirror convergence. Replay and queue
  lock probes preserved the original job. The cold run's minimum start
  spacing was 1,000,632 µs. The first run missed the delayed-checkpoint
  overlap assertion.
- The rootful manual publication probe audited all six required TXT
  originals after 14 HTTPS starts. It fenced competing writers, rejected
  sample promotion, and verified readiness, exact MCP bytes, and evidence
  metadata. The NUL and invalid UTF-8 fixtures retained their exact
  canonical bytes. The daemon recovered a ready candidate after restart
  and delivered the outstanding manual and automatic outbox events.
- The rootful incumbent probe passed warm reuse, idle catalog checks,
  failure on missing incumbent TXT and damaged canonical content, and
  retained rollback with the incumbent readable through failures.
- The rootless confinement probe passed on the base deployment before and
  after container recreation. It verified read-only access, service and
  network isolation, Landlock and seccomp, worker saturation, cancellation,
  reaping, and reader-fence release.
- The rootful destructive restore verified 16 canonical blobs, 794 byte
  ranges and line starts, three manifests, and the active pointer at
  sequence 2. It replayed the five MCP tools through the restored proxy
  and rejected damaged or missing originals. The retry used an existing
  private evidence directory as required by the harness.
- The populated schema 9 and 10 fixtures each migrated twice to schema 11
  and preserved their manifests, historical observations, and original
  provenance. Both verified the first audited HTTPS publication and
  byte-exact retained rollback. The initial schema 9 stack timed out while
  PostgreSQL was syncing its initialized data. The isolated retry passed.
- The local `bootstrap-quick` CI case passed using the pinned historical
  checkout and matching schema 10 images.
- Rootless Minikube 1.39.0 with Kubernetes 1.37.0 and containerd passed both
  fresh-stack publication paths, a brokered Feed-triggered publication, and
  authenticated outbox delivery. Both paths served an exact RFC 2119 MCP
  read at the observed sequence. The manual path reused six TXT and three
  RFCXML originals on a warm run, with 14 HTTPS starts cold and five warm.
  Replacing the interrupted pod reused the recorded repair pair. Stock
  preflight verified the independent capped ext4 mounts, CPU and memory
  limits, scratch capacity, and isolation from Serve and Feed. The first
  attempt exposed an invalid wildcard control bind in the harness. A later
  attempt hit the pod status timeout. The passing probe used specific pod IPs
  and a longer bounded wait. The disposable profile and staging mounts
  were removed.

### Repeat the checks

Use preloaded stock and fixture images from the same source revision.
Run timing-sensitive probes serially in disposable projects and keep
temporary data under `/tmp`.

~~~sh
export TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/rust/target RFC_E2E_PRELOADED=1
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix" --skip-build
python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$prefix" --config-only
python3 -B scripts/phase6/integration.py --context "$context" --image-prefix "$prefix"
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --publication-only
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --upgrade-from-schema9 "$schema9_prefix"
python3 -B scripts/https-discovery/integration.py --context "$context" --image-prefix "$prefix" --upgrade-from-schema10 "$schema10_prefix"
python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$prefix"
restore_evidence=$(mktemp -d /tmp/rfc-phase6-restore.XXXXXXXX)
python3 -B scripts/phase8/integration.py --context "$context" --image-prefix "$prefix" --evidence "$restore_evidence"
~~~

Build the historical `phase3-fixture` target from `376fdc6` (schema 9)
and `29896b5` (schema 10) under the corresponding image prefixes.
Set `MINIKUBE_HOME`, `KUBECONFIG`, and `TMPDIR` under `/tmp` for a disposable
rootless Docker profile with containerd. Attach the two capped ext4
filesystems at separate node paths under `/tmp`, writable by the mapped
Sync UID/GID. Pass the two node paths to the probe.

~~~sh
python3 -B scripts/https-discovery/minikube.py --profile "$profile" --image-prefix "$prefix" \
    --spool-node-dir "$spool_node_dir" --audit-node-dir "$audit_node_dir"
~~~

Logs, exit statuses, image IDs, and the restore receipt are under
`/tmp/rfc-https-phase6.jy3N5U89` on the qualification host. The initial
Docker build hit an Alpine DNS failure. The final stock images were built
on rootless Docker and transferred to rootful Docker. The disposable
Compose projects, Minikube resources, staging images, and test image tags
were removed.

The private probes use packaged rsync and local HTTPS stand-ins. Official
corpus timing and public activation have their own qualification gates.

## HTTPS-first RFC synchronization Phase 7

The Compose `https-faults` case uses the stock Sync image and private
fixtures to check that acceleration stays in the fixture build. It also
exercises a required TXT timeout, an audit filesystem that fills after
preflight, and a database disconnect before publication commit. In each
fault case the incumbent remains readable and the active pointer and
outbox stay at their prior state. Retrying after the faults produces one
new publication.

The full local gate adds alternate rsync checksum negotiation, explicit
RFCXML evidence and coverage checks, independent XML freshness, and
byte-exact MCP reads for synthetic NUL and invalid UTF-8 inputs. The NUL
fixture uses RFC number 542 with synthetic content. The full gate reuses
the paced pipeline, recovery, migration, and confinement probes.

### Local qualification on 2026-11-25

The native Linux amd64 images were built from `b92e67f`. The probes used
Docker Engine 29.10.0 rootless and 29.10.1 rootful, Compose 5.8.0,
PostgreSQL 19.0, packaged rsync 3.5.2, and Rust 1.100.0. The verified image
bundle identified stock Sync as
`sha256:d8f2de005f3369a44a83a2a4d2a08292cf32f0659ff86b165671bc5ffc20e15a`.

- The locked fetch, frozen Rust gate, all-feature Clippy and tests, stock
  feature resolution, `actionlint`, and the HTTP boundary smoke exited 0.
  The CI image loader accepted the native amd64 bundle for `https-faults`.
- The Phase 0 Compose and Phase 7 deployment probes both exited 0 on
  each Docker engine. They checked service and cgroup isolation, verified
  TLS, Landlock and seccomp, worker cancellation and reaping, and reader
  fence release.
- The `https-faults` CI runner exited 0 on both engines. Stock Sync sent
  zero requests to the fixture with the acceleration switches set, and
  the fixture build rejected a redirect. The required TXT timed out
  within its retry budget. Real `ENOSPC` after preflight and a terminated
  PostgreSQL session preserved the incumbent until a successful retry.
- The rootless full HTTPS-first, audit, and incumbent cases exited 0.
  The cold fixture made 14 HTTPS starts, with a minimum observed start
  interval of 1,000,424 µs. A checkpoint recovery made five starts with
  the same processing digest. An expired RFCXML capture refreshed while
  all six TXT originals were reused and audited. The restricted rsync
  peer negotiated `md5` through `auto` with zero payload-transfer bytes.
- The publication cases exited 0 on both engines, including byte-exact MCP
  reads for both synthetic binary inputs, the sequence race, retained
  rollback, and daemon recovery of the ready candidate and outbox.
- The rootless cold-daemon and both populated upgrade cases exited 0.
  The daemon retried the same authenticated outbox event and reconciled
  again on schedule with Feed stopped. Schema 9 and 10 each migrated
  twice, retaining historical observations and manifests, then published
  an audited candidate and rolled back to byte-exact retained reads.
- The rootless destructive restore exited 0. It verified 16 canonical
  blobs, 794 byte ranges and line starts, three manifests, and the active
  pointer at sequence 2. The restored proxy served the retained and
  active generations over MCP `2026-07-28`.
- Rootless Minikube 1.39.0 with Kubernetes 1.37.0 and containerd exited 0.
  Both paths passed stock preflight, byte-exact reads, and authenticated
  delivery. The manual path recovered its repair pair after pod
  replacement, and the daemon handled a brokered Feed publication. The
  warm run reused six TXT and three RFCXML originals with five HTTPS
  starts. The profile and capped mounts were removed.

The first fault and publication attempts exited 1 because of assertions
about the warm selection ID and the MCP wire encoding. The first expanded
Rust check exited 101 because the new receipt assertion reused a moved
map. The corrected runs passed. The qualification uses private fixtures
on Linux amd64. Hosted ARM64 verification and official corpus timing are
separate gates.

The command logs, exit statuses, image manifest, and restore receipt are
under `/tmp/rfc-https-phase7.ZCoy6eeA` on the qualification host. The run
removed its disposable Compose resources, test image tags, and Minikube
profile and mounts.

### Repeat the checks

Use matching stock and fixture images from the native Compose bundle and
run timing-sensitive probes serially. Use `rfc-https-phase7-schema9` and
`rfc-https-phase7-schema10` as example prefixes for historical
`phase3-fixture` builds from `376fdc6` and `29896b5` respectively.

~~~sh
export TMPDIR=/tmp CARGO_TARGET_DIR=/tmp/rust/target RFC_E2E_PRELOADED=1
cargo fetch --locked
scripts/phase0/check-rust.sh
cargo clippy --workspace --all-targets --all-features --frozen -- -D warnings
cargo test --workspace --all-features --frozen
cargo tree --workspace --frozen -e features
actionlint
python3 -B scripts/phase8/http-smoke.py

evidence=$(mktemp -d /tmp/rfc-phase7.XXXXXXXX)
for context in rootless default; do
    python3 -B scripts/phase0/integration.py --context "$context" --project "$prefix" --skip-build
    python3 -B scripts/phase7/integration.py --context "$context" --image-prefix "$prefix"
    python3 -B scripts/ci/docker-e2e.py --context "$context" --image-prefix "$prefix" \
        run https-faults "$evidence/faults-$context"
done

python3 -B scripts/https-discovery/integration.py --context rootless --image-prefix "$prefix"
for mode in --publication-only --audit-only --incumbent-only --cold-daemon-only; do
    python3 -B scripts/https-discovery/integration.py --context rootless --image-prefix "$prefix" "$mode"
done
for schema in 9 10; do
    python3 -B scripts/https-discovery/integration.py --context rootless --image-prefix "$prefix" \
        "--upgrade-from-schema$schema" "rfc-https-phase7-schema$schema"
done

mkdir "$evidence/restore"
python3 -B scripts/phase8/integration.py --context rootless --image-prefix "$prefix" \
    --evidence "$evidence/restore"
~~~

Use the disposable rootless Minikube setup described in Phase 6 for its
separate runtime smoke. Pass the two capped ext4 mount paths under the
node's `/tmp` to `scripts/https-discovery/minikube.py`. Keep the host
profile, kubeconfig, and evidence under `/tmp`.
