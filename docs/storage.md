# Phase 1 storage and database

## Mirror layout and durability

The mirror is a Linux filesystem exposed through a Docker Compose named volume. Give sync UID 10002 write access to its root and mount it read-only for serve UID 10001. Feed has no mirror mount. Provision the mirror root for sync ownership with mode 0755. Sync creates the private `staging` and `quarantine` directories with mode 0700. Canonical files have mode 0444.

```text
/mirror/
  blobs/blake3/<first two hex>/<64 lowercase hex>
  staging/<unique install ID>/body
  quarantine/<unique install ID>/body
  quarantine/<unique install ID>/incident.txt
```

A blob is the original input, with no encoding or line-ending conversion. Its identity is all 32 bytes of unkeyed BLAKE3. Its database algorithm label is `blake3-256`. Lengths and offsets fit signed PostgreSQL `BIGINT`. Ranges are zero-based and half-open. Empty slices are valid when their endpoint is within the blob.

Sync streams up to the caller's byte limit into an exclusive staging file. After syncing and closing the file it performs a same-filesystem no-replace rename, then syncs both affected directories. It also syncs parent entries as directories are created. Only a successful durable install or successfully revalidated reuse returns a record that the writer repository can register. The volume must support directory sync, `openat2`, and `renameat2(RENAME_NOREPLACE)` under the deployed kernel and Docker seccomp profile.

An occupied path is reusable after length and digest verification. A suspect entry stays in place. Sync keeps the incoming file and bounded error record under quarantine and returns an error. An interrupted operation can leave a private staging entry or a complete unregistered canonical blob. A retry revalidates any canonical blob. Coordinate any repair or cleanup with the sync operator. Keep referenced blobs in place for readers.

## Provision PostgreSQL 19

1. Initialize the dedicated `rfc` database with the pinned PostgreSQL image and a separate persistent data volume. Use UTF-8, ICU `und`, and SCRAM password encryption. Use a PostgreSQL TLS certificate with a SAN matching the hostname shared on the private Compose database networks. The [Compose test stack](../scripts/phase1/compose.yml) shows the image digest, volume layout, socket permissions, and initialization arguments.
2. As the database administrator on a local connection, run the commands below from the repository or an equivalent read-only migration mount. Enter independent role passwords at the `psql` prompts and make them available to the matching service through secret files. The `rfc_owner` role is NOLOGIN. Migration objects and their default privileges belong to that role.

   ```sh
   psql -X -d rfc -f migrations/roles.sql
   psql -X -d rfc -c '\password rfc_serve'
   psql -X -d rfc -c '\password rfc_sync'
   psql -X -d rfc -f migrations/bootstrap.sql
   psql -X -d rfc -f migrations/migrate.sql
   ```

3. Set `pg_hba.conf` to admit the two runtime roles only to `rfc`, over TLS with `scram-sha-256`, from the private database networks. Reserve local administrative access for provisioning and upgrades. Keep the database port private. Give serve and sync their own `RFC_DB_PASSWORD_FILE` mount and configure `RFC_DB_HOST` and `RFC_DB_CA_FILE`. The database port defaults to 5432 and the mirror root to `/mirror`. Each runtime defaults to its designated login and validates both session and effective role.
4. Run sync's `--phase1-check-storage` first to verify its grants and prepare the mirror, then serve's equivalent check. The commands exit after checking runtime privileges and mount properties. Serve starts PostgreSQL transactions read-only by default and relies on database grants for authorization. The owner-run migration command applies and records versions 1 and 2 transactionally and can be rerun. When upgrading from version 1, rerun `bootstrap.sql` as an administrator before migrating to grant the sync role its session advisory lock.

The active pointer starts empty at publication sequence zero. Sync can prepare and freeze a `ready` candidate. PostgreSQL serializes its freeze with insertions of membership, edges, evidence links, and coverage. Document versions are sealed before membership and relation evidence can reference them. The runtime grants reserve active-pointer updates for the later publication phase.

Per-generation advisory lock keys are the nonnegative PostgreSQL generation IDs. Readers acquire a shared transaction-scoped lock. The exclusive test uses the same key from sync. Key `-1` is reserved for publication and GC coordination and `-2` serializes migrations.

## Manifest format 1

Encode the following ordered tuple as compact UTF-8 JSON. Use arrays, JSON strings, decimal integers, and `null` for absent values. The serializer emits no extra whitespace or trailing newline. Strings sort lexicographically by UTF-8 bytes, integer keys numerically, and nullable RFC keys with null first. Preserve Unicode sequences.

```text
[1, parser_version, index_version, members, edges, coverage]
members  = [[rfc_number, txt_digest, document_version_identity, catalog_metadata_identity], ...]
edges    = [[source_rfc, kind, target_rfc, [evidence_identity, ...]], ...]
coverage = [[scope, rfc_number_or_null, family, state], ...]
```

Sort each collection lexicographically by the shown fields and sort the evidence identities within each edge. Digests are lowercase hex. The writer derives source observation, document index, document version, catalog metadata, and evidence identities by hashing `[record_format, ordered_fields]` with unkeyed BLAKE3. The record format strings and field order are defined in the [writer repository](../crates/sync/src/lib.rs). The manifest and its digest exclude the database generation ID and insertion time. Source observation times remain provenance and participate in source identity.

The [serializer golden test](../crates/storage/src/manifest.rs) fixes the exact bytes, including a non-ASCII string. Its BLAKE3 digest is `6b5a42ee1e42b0746f35e4278e36ac04816a7e27c9f283ece2b2fc58c686119c`.

## Repeat verification

From the repository on Linux, run the locked offline checks and build and test both Compose profiles.

```sh
cargo fetch --locked
scripts/phase0/check-rust.sh
python3 scripts/phase0/integration.py --project rfc-phase1-regression
python3 scripts/phase1/integration.py --image-prefix rfc-phase1-regression
```

The Phase 1 runner makes two disposable Compose projects and keeps fixture inputs, credentials, dumps, and named-volume backing data under `/tmp`. It captures a quiescent database dump and the reachable blobs, initializes provisioned roles and database grants in the fresh project, imports blobs by digest, then restores schema, ownership, grants, and rows. The read repository checks all fixture manifests and byte ranges in the restored project. The fixture-only binary is built as a separate Docker target.
