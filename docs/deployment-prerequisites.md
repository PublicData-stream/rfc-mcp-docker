# Deployment prerequisites

- Use a Linux host with Docker Engine and Compose. The Phase 0 gate covers rootless Docker on Linux amd64. Run the same tests on each intended host architecture and kernel before deploying there.
- Use digest-pinned Alpine image inputs and musl binaries. Qualify the exact final service images and PostgreSQL image after base, TLS, native dependency, or ICU changes. An Alpine incompatibility needs a reproduction and documented evaluation of alternatives before proposing a different base.
- Check the hardened Compose profile and the host kernel's Landlock and seccomp enforcement. Ensure the mirror is read-only for the serve UID and writable for the sync UID. Give feed its own UID and neither mirror storage nor DB credentials.
- Provision PostgreSQL's data volume and socket tmpfs for Alpine's postgres UID 70, with the correct rootless UID mapping. Keep the database off published ports and runtime credentials scoped to each role. Verify TLS, SCRAM, and the UTF-8 ICU `und` initialization on an empty cluster. Compare recorded and actual ICU collation versions before upgrades. Rebuild affected indexes when collation semantics change before refreshing stored version metadata.
- Use distinct ingress, DB read/write, feed control, sync notification, and upstream egress boundaries. Review actual routing, firewall, secret ownership and the TLS proxy before public service. The Phase 0 harness uses private synthetic endpoints for egress and exercises the DB reachability boundaries.
- Before release, complete the later phase gates for storage durability, publication and recovery, search, HMAC, worker lifecycle, restore, and full production image validation.

See [production Compose deployment](deployment.md) for the operator procedure and [storage and database provisioning](storage.md) for the private restore drill.

## Repeat Phase 0

```sh
cargo fetch --locked
scripts/phase0/check-rust.sh
python3 scripts/phase0/integration.py
python3 scripts/phase0/upstream.py
```

The probes use Python 3 and the OpenSSL command line for disposable certificates. The Compose runner uses a unique project and a private directory under `/tmp` for bind mounts, ephemeral credentials and certificates. It removes the project containers and networks on exit. It uses the local Docker image store and prints the exact image IDs. The separate upstream probe is live and rate-limited. Its source captures stay under `/tmp`. See [the implementation status](implementation-status.md) for execution evidence.
