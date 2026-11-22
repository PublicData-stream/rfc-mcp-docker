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
