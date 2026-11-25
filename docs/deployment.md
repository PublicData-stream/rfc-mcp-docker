# Docker Compose deployment

The production profile runs one serve, one sync, one feed, and PostgreSQL 19. It requires a qualified Linux Docker host, an existing HTTPS reverse proxy, and a dedicated Docker ingress network shared with that proxy. Follow the [deployment prerequisites](deployment-prerequisites.md).

## Provision

1. Use the three qualified service image references in a private copy of [the environment example](../deploy/compose.env.example). For a registry release, use the three published index digests and verify the host's CPU baseline. For a local build, build the `serve`, `sync`, and `feed` Dockerfile targets and set their tags in that file. [GHCR release and digest verification](ghcr-release.md)
2. Create the mirror and PostgreSQL backing directories. Choose nonoverlapping IPv4 subnets. Put only serve and the existing proxy on the dedicated ingress bridge. Give the proxy a separate frontend network and assign the fixed backend addresses from the environment file.
3. Copy [the PostgreSQL access policy](../deploy/pg_hba.conf.example) and update its two private subnets if needed. Supply a database certificate with DNS SAN `postgres`, its private key, and the issuing CA. Use independent passwords for PostgreSQL bootstrap, `rfc_serve`, and `rfc_sync`.
4. Generate three distinct 48-byte raw keys under a private umask. Make separate per-service copies of each pairwise key. Set the cursor and notification keys for serve to UID 10001, the sync copies to UID 10002, and the feed copy to UID 10003. PostgreSQL private material belongs to UID 70. Set secret files to mode 0400 and give the designated container UID access using the host’s actual UID mapping. Verify ownership inside the containers because Compose mounts the backing files directly.
5. Copy the [v2 ingestion example](../deploy/ingestion.toml.example) to
   the regular file named by `RFC_SYNC_CONFIG_HOST_FILE`. Mode `0444` is
   suitable for this nonsecret file. Compose mounts it read-only for Sync.
   Translate settings from v1 before switching images. [Translate v1 settings](ingestion.md#translate-v1-settings)
6. Provision the private HTTPS spool and audit filesystems before rendering
   or starting the stack. [Staging setup](#private-sync-staging)

```sh
docker network create --internal --subnet 172.30.70.0/28 rfc-public-ingress
docker compose --env-file /path/to/rfc.env config --quiet
```

Set `RFC_NOTIFY_DYNAMIC_RANGE` and `RFC_CONTROL_DYNAMIC_RANGE` to ranges within their corresponding private subnets and outside the fixed listener addresses. This reserves the listener addresses during concurrent startup and recreation. An omitted range uses the full subnet for compatibility with existing environment files. For a qualified upgrade, add the ranges to the private environment file and recreate the affected Compose networks after draining the application services. Preserve the bind-backed mirror and database directories.

### Mirror volume

The default mirror is a Compose named volume backed by the host directory in `RFC_MIRROR_DIR`. Create that directory before running `volume-admin`. For a Docker-managed mirror on a fresh project, save the following overlay in your private configuration directory and include it after `compose.yml` in every Compose command.

```yaml
volumes:
  mirror:
    driver_opts: !reset {}
```

```sh
docker compose -f compose.yml -f /path/to/managed-mirror.yml --env-file /path/to/rfc.env config --quiet
```

Set `RFC_MIRROR_DIR` in the environment file so the base Compose file can interpolate it before merging. The overlay removes the bind driver options. Initialize the Docker-managed mirror with the same `volume-admin` step below. Both layouts provide Serve with read-only access and Sync with write access. To change the backing of an existing mirror, use a fresh project and the matched [backup and restore procedure](publication.md#backup-and-restore).

### Private Sync staging

Use dedicated ext4 filesystems for `RFC_SYNC_SPOOL_HOST_DIR` (5 GiB,
request 65,536 inodes when formatting) and `RFC_SYNC_AUDIT_HOST_DIR` (1 GiB,
4,096 inodes). Keep them separate from the mirror, PostgreSQL, and container
scratch. Mount with `nosuid,nodev,noexec`. Set the mounted filesystem roots
to mode `0700` and owner UID/GID `10002:10002` as seen by Sync. With
rootless Docker, translate both IDs through that engine's user namespace
mapping. Keep both filesystems across restarts. The spool holds in-flight
originals, the shared HTTPS lease and cooldown, and the operator socket.
The audit filesystem holds resumable receipts and bounded quarantine.

Use the actual production Compose profile for preflight. The report lists
scratch and cgroup reservations and the free bytes and inodes in both
filesystems. With the default accepted-byte budget, allow 4 GiB plus
640 MiB and at least 40,016 free inodes in the spool. The audit check
reserves room for a 256 MiB batch and one repair pair. Verify that the chosen
filesystems enforce the intended caps.

~~~sh
export RFC_ENV_FILE=/path/to/private/rfc.env
compose() { docker compose --env-file "$RFC_ENV_FILE" "$@"; }
compose config --quiet
compose run --rm --no-deps rfc-mcp-docker-sync --check-config
~~~

## Initialize or upgrade

### Initialize fresh volumes

Use the same project and environment file throughout. Begin the cold-start clock immediately before `volume-admin`. It adjusts only the backing directory roots. PostgreSQL owns `/var/lib/postgresql` and initializes data in `/var/lib/postgresql/19/docker`. The role-and-subnet HBA rules require TLS and SCRAM for runtime connections.

Run the database setup over the PostgreSQL container's local peer-authenticated socket. At each `\password` prompt, enter the password provisioned for that runtime role. The PostgreSQL bootstrap credentials stay with PostgreSQL.

```sh
export RFC_ENV_FILE=/path/to/rfc.env
compose() { docker compose --env-file "$RFC_ENV_FILE" "$@"; }
compose --profile tools run --rm --no-deps volume-admin
compose up -d --wait postgres
compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -c 'SHOW data_directory'
compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -f /migrations/roles.sql
compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -c '\password rfc_serve'
compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -c '\password rfc_sync'
compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -f /migrations/bootstrap.sql
compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -f /migrations/migrate.sql
compose run --rm --no-deps rfc-mcp-docker-sync --phase1-check-storage
compose run --rm --no-deps rfc-mcp-docker-serve --phase1-check-storage
compose run --rm --no-deps rfc-mcp-docker-sync --check-config
compose up -d --wait rfc-mcp-docker-serve
```

### First publication

For automatic publication, run `compose up -d rfc-mcp-docker-sync`. Startup queues a full reconciliation. Sync validates the candidate, publishes it against expected sequence `0`, and independently retries the authenticated notification. Start Feed after the bootstrap checks with `compose up -d rfc-mcp-docker-feed`.

For operator-controlled publication, leave Sync and Feed stopped while
running the two one-shot commands. Copy `generation_id` and
`expected_publication_sequence` from the successful `ready` report. A fresh
stack reports sequence `0`.

```sh
compose run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --full
compose run --rm --no-deps rfc-mcp-docker-sync --phase3-publish "$GENERATION" --expect-sequence 0
```

The manual candidate leaves the active pointer at sequence `0` until the explicit promotion. Confirm the pending outbox row and verify serving for sequence `1` before starting the daemon. It dispatches the pending notification and can enqueue another reconciliation at startup.

Use the [operating checks](#operate-and-recover) to match the active PostgreSQL generation and sequence `1` with a successful loopback `/ready` and a `get` for RFC 2119 over the configured HTTPS proxy. Send the full Streamable HTTP `2026-07-28` metadata from the [MCP request example](mcp.md) and set `arguments.format` to `base64`. Check the result's `generation_id`, `publication_sequence`, and original byte range and content against the expected RFC. This read completes the cold-start timing. Confirm that the outbox row for the same generation and sequence reaches `delivered` after the daemon runs. Serve container health uses `/live`, so verify `/ready` and `rfc_mcp_rg_available` before opening ingress to readers.

### Upgrade existing volumes

The upgrade starts from a populated schema 9 release and uses the current
matching schema 11 images. Follow the same maintenance procedure for a
schema 10 shadow stack.

1. Close public ingress. In the previous checkout, stop Feed, allow the
   running job to settle, stop Sync, and stop Serve. Save the previous
   image references and TOML. Verify a matched PostgreSQL and mirror
   backup including the incumbent sequence and retained generations.
   Keep the existing database roles and role passwords.
   [Backup and restore](publication.md#backup-and-restore)
2. Provision the private staging filesystems and translate the TOML.
   Use base Compose and remove any legacy bulk or HTTPS shadow overlay
   from the command. Use the same project and database and mirror backing
   directories. Preserve the shadow spool if it recorded a live upstream
   cooldown. Wait for known cooldown before switching upstream ownership.
3. Use the current checkout and environment file to apply and verify the migration.

   ~~~sh
   compose config --quiet
   compose run --rm --no-deps rfc-mcp-docker-sync --check-config
   compose up -d --wait postgres
   compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -f /migrations/migrate.sql
   compose exec postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc \
     -c 'SELECT max(version) FROM rfc.schema_migrations' \
     -c 'SELECT generation_id, publication_sequence FROM rfc.active_generation'
   compose run --rm --no-deps rfc-mcp-docker-sync --phase1-check-storage
   compose up -d --wait rfc-mcp-docker-serve
   ~~~

4. Confirm schema 11 and the backed-up sequence. Check `/ready`, an
   exact-byte MCP `get` pinned to the incumbent generation, and a read
   pinned to a retained generation. Historical evidence remains as
   recorded. Reopen ingress after verifying the serving path. Start Sync,
   verify a full audited publication and its delivered outbox event, then
   resume Feed.

### Rollback

For rollback within schema 11, stop Feed and Sync, read the active sequence,
and select a retained generation. Serve can continue handling reads.

~~~sh
compose stop rfc-mcp-docker-feed
compose stop -t 60 rfc-mcp-docker-sync
compose run --rm --no-deps rfc-mcp-docker-sync \
    --phase3-rollback "$RETAINED_GENERATION" --expect-sequence "$ACTIVE_SEQUENCE"
~~~

Check the new sequence, `/ready`, and a pinned MCP read. Restart Sync when
ready for it to drain notifications and reconcile again. To return to the
previous schema and binaries, restore the matched pre-upgrade snapshot into
fresh database and mirror volumes using the saved images, checkout, and
TOML. Verify historical reads and the backup's sequence before resuming
service. [Schema 9 deployment instructions](https://github.com/PublicData-stream/rfc-mcp-docker/blob/376fdc65c37c331ea433ebdb57ef70e7afbfdfbe/docs/deployment.md)

## Proxy and network boundaries

Route HTTPS `/mcp` to the serve ingress address on port 8780. Preserve the external Host, Origin, relevant MCP request headers, content type, and Accept. Strip client-supplied forwarding headers and set `X-Real-IP` from the validated client address at the configured trusted proxy. Keep the request body limit at 64 KiB and the backend response timeout above the server's 15-second deadline. Bound headers and configure header and body read timeouts. Health and metrics use serve loopback. The two control listeners use dedicated private addresses.

Verify the proxy and backend paths after container recreation. Sync owns upstream egress. Feed uses only the internal control network. Configure the host firewall for Docker forwarding and applicable IPv6 routes. Treat external proxy exposure, registry publication, and live upstream verification as separate operations.

The initial container memory limits are 768 MiB for serve, 2 GiB for sync, 128 MiB for feed, and 1 GiB for PostgreSQL. CPU limits are two for serve, sync, and PostgreSQL and 0.5 for feed. Confirm that the rootless daemon applies these settings with delegated cgroup v2 controllers. Check local logs and [control-plane health](control.md) after startup.

## Operate and recover

Use the same Compose project and private environment file for every command. Set `RFC_ENV_FILE` to its path. Inspect the project and the loopback serving health before opening or reopening ingress:

```sh
docker compose --env-file "$RFC_ENV_FILE" ps
docker compose --env-file "$RFC_ENV_FILE" exec -T rfc-mcp-docker-serve \
    wget -qO- http://127.0.0.1:8781/ready
docker compose --env-file "$RFC_ENV_FILE" exec -T rfc-mcp-docker-serve \
    wget -qO- http://127.0.0.1:8781/metrics
docker compose --env-file "$RFC_ENV_FILE" exec -T postgres psql -X -U postgres -d rfc \
    -c 'SELECT generation_id, publication_sequence FROM rfc.active_generation' \
    -c 'SELECT id, state, outcome FROM rfc.sync_jobs ORDER BY id DESC LIMIT 10' \
    -c 'SELECT o.generation_id, o.publication_sequence, o.delivery_state, o.attempts
        FROM rfc.notification_outbox o JOIN rfc.active_generation a USING (generation_id, publication_sequence)' \
    -c 'SELECT delivery_state, count(*) FROM rfc.notification_outbox GROUP BY delivery_state'
```

Compare the publication sequence and most recent successful sync job with the expected reconciliation window. Verify a recent RFC over the public MCP route after proxy or container changes. A healthy container with a stale publication warrants checking sync and feed logs, queued jobs, and the upstream response before retrying. An outbox delay can coexist with a current MCP result because serve resolves the committed active pointer. [Control plane](control.md)

HTTP 429 carries `Retry-After: 1`. Reduce client burst and concurrency, and verify that the proxy sets `X-Real-IP` from the actual client address. For a tool deadline, budget error, or partial scan, narrow the query or continue with a valid cursor. Check in-flight requests, admission rejections, database readers, and ripgrep availability before adjusting limits. Keep the sandbox and its failure checks enabled. [MCP tools and budgets](mcp.md)

Use the receiver-first procedure for each [HMAC key rotation](control.md#clock-and-key-rotation). Drain and back up before an upgrade, validate the new Compose configuration and pinned image identities, then apply migrations with the local PostgreSQL administrator. Check the schema version, TLS, roles, storage, readiness, proxy path, and a pinned MCP read before reopening ingress. Use the [publication runbook](publication.md) for GC, sequence-bound rollback, backup, restore, and corruption. Schema rollback requires a compatible backup and fresh-volume restore when the previous binary cannot read the upgraded schema.

For a planned shutdown, close the proxy route first. Stop Feed, let running Sync work settle, then stop Sync and Serve before PostgreSQL. If Sync must interrupt a job, use `compose stop -t 60 rfc-mcp-docker-sync` to allow cooperative cancellation. Start PostgreSQL and Serve, then Sync and Feed. Check unfinished jobs, publication sequence, and pending notifications after restart. Keep the independent backup available.

## Repeat the local qualification

```sh
python3 -B scripts/phase0/integration.py --context rootless --project rfc-phase7-qualified
python3 -B scripts/phase7/integration.py --context rootless --image-prefix rfc-phase7-qualified
python3 -B scripts/phase6/integration.py --context rootless --image-prefix rfc-phase7-qualified
```

The harness uses temporary credentials and bind-backed storage under `/tmp`, isolated upstream networks, and a disposable proxy published on host loopback. Run it on each intended host architecture and kernel before serving public traffic. [Observed results](implementation-status.md#phase-7)

Run the [Phase 8 release qualification](release-readiness.md) for the full acceptance matrix, fresh-stack restoration, anonymous load measurements, and supply-chain inventory.

<a id="https-first-phase-2-shadow-runbook"></a>
<a id="https-first-phase-3-shadow-runbook"></a>
<a id="https-first-phase-4-shadow-runbook"></a>
<a id="https-first-phase-5-shadow-runbook"></a>
<a id="bulk-rsync-staging"></a>
<a id="return-to-strict-batched-ingestion"></a>

### Historical deployment procedures

Use the [pinned Phase 5 shadow runbook](https://github.com/PublicData-stream/rfc-mcp-docker/blob/7117e2f2ec3604358813d909204af08562d090ee/docs/deployment.md)
or the [pinned schema 9 deployment guide](https://github.com/PublicData-stream/rfc-mcp-docker/blob/376fdc65c37c331ea433ebdb57ef70e7afbfdfbe/docs/deployment.md)
with matching images and configuration when reproducing historical checks.
