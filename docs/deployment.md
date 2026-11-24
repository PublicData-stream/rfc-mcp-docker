# Docker Compose deployment

The production profile runs one serve, one sync, one feed, and PostgreSQL 19. It requires a qualified Linux Docker host, an existing HTTPS reverse proxy, and a dedicated Docker ingress network shared with that proxy. Follow the [deployment prerequisites](deployment-prerequisites.md).

## Provision

1. Use the three qualified service image references in a private copy of [the environment example](../deploy/compose.env.example). For a registry release, use the three published index digests and verify the host's CPU baseline. For a local build, build the `serve`, `sync`, and `feed` Dockerfile targets and set their tags in that file. [GHCR release and digest verification](ghcr-release.md)
2. Create the mirror and PostgreSQL backing directories. Choose nonoverlapping IPv4 subnets. Put only serve and the existing proxy on the dedicated ingress bridge. Give the proxy a separate frontend network and assign the fixed backend addresses from the environment file.
3. Copy [the PostgreSQL access policy](../deploy/pg_hba.conf.example) and update its two private subnets if needed. Supply a database certificate with DNS SAN `postgres`, its private key, and the issuing CA. Use independent passwords for PostgreSQL bootstrap, `rfc_serve`, and `rfc_sync`.
4. Generate three distinct 48-byte raw keys under a private umask. Make separate per-service copies of each pairwise key. Set the cursor and notification keys for serve to UID 10001, the sync copies to UID 10002, and the feed copy to UID 10003. PostgreSQL private material belongs to UID 70. Set secret files to mode 0400 and give the designated container UID access using the host’s actual UID mapping. Verify ownership inside the containers because Compose mounts the backing files directly.
5. Copy [the ingestion example](../deploy/ingestion.toml.example) to the host file named by `RFC_SYNC_CONFIG_HOST_FILE`. Use a regular file readable by Sync's container UID 10002 under the host's rootless or rootful mapping. Mode `0444` is suitable for the example, which has no credentials. On an upgrade, carry any old `RFC_SYNC_HTTP_CONCURRENCY` and `RFC_SYNC_PARSE_CONCURRENCY` values over to the TOML equivalents and remove those environment settings from custom Compose overrides. [Ingestion settings](ingestion.md#sync-configuration)

```sh
docker network create --internal --subnet 172.30.70.0/28 rfc-public-ingress
docker compose --env-file /path/to/rfc.env config --quiet
```

Set `RFC_NOTIFY_DYNAMIC_RANGE` and `RFC_CONTROL_DYNAMIC_RANGE` to ranges within their corresponding private subnets and outside the fixed listener addresses. This reserves the listener addresses during concurrent startup and recreation. An omitted range uses the full subnet for compatibility with existing environment files. For a qualified upgrade, add the ranges to the private environment file and recreate the affected Compose networks after draining the application services. Preserve the bind-backed mirror and database directories.

### Bulk rsync staging

To enable `ingestion.rsync.mode = "persistent_bulk"`, provision a dedicated ext4 filesystem with at most 5 GiB and 24,576 inodes. Set an explicit inode count when formatting and use `--check-config` to check the resulting capacity. Leave room for one replacement file and the small sample tree. Set the mounted filesystem root to mode `0700` and map its owner to Sync's container UID 10002. Mount with `nosuid,nodev,noexec`. Use distinct filesystems for staging, the canonical mirror, and PostgreSQL. The staging tree is private to Sync and can be regenerated from upstream.

Set `RFC_SYNC_RSYNC_STAGE_DIR` to the filesystem's host mountpoint. Use the overlay for Sync startup and manual reconciliation. Run the preflight after the host mount is active.

```sh
docker compose -f compose.yml -f deploy/compose.bulk-rsync.yml --env-file /path/to/rfc.env config --quiet
docker compose -f compose.yml -f deploy/compose.bulk-rsync.yml --env-file /path/to/rfc.env run --rm --no-deps rfc-mcp-docker-sync --check-config
```

## Initialize or upgrade

### Initialize fresh volumes

Use the same project and environment file throughout. Include the bulk rsync overlay in every command if that profile is configured. Begin the cold-start clock immediately before `volume-admin`. It adjusts only the backing directory roots. PostgreSQL owns `/var/lib/postgresql` and initializes data in `/var/lib/postgresql/19/docker`. The role-and-subnet HBA rules require TLS and SCRAM for runtime connections.

Run the database setup over the PostgreSQL container's local peer-authenticated socket. At each `\password` prompt, enter the password provisioned for that runtime role. The PostgreSQL bootstrap credentials stay with PostgreSQL.

```sh
export RFC_ENV_FILE=/path/to/rfc.env
compose() { docker compose --env-file "$RFC_ENV_FILE" "$@"; }
# For bulk rsync, add -f compose.yml -f deploy/compose.bulk-rsync.yml to compose().
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

For operator-controlled publication, leave Sync and Feed stopped while running the two one-shot commands. Use the `generation_id` from the successful `ready` report as `GENERATION`.

```sh
compose run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --full
compose run --rm --no-deps rfc-mcp-docker-sync --phase3-publish "$GENERATION" --expect-sequence 0
```

The manual candidate leaves the active pointer at sequence `0` until the explicit promotion. Confirm the pending outbox row and verify serving for sequence `1` before starting the daemon. It dispatches the pending notification and can enqueue another reconciliation at startup.

Use the [operating checks](#operate-and-recover) to match the active PostgreSQL generation and sequence `1` with a successful loopback `/ready` and a `get` for RFC 2119 over the configured HTTPS proxy. Send the full Streamable HTTP `2026-07-28` metadata from the [MCP request example](mcp.md) and set `arguments.format` to `base64`. Check the result's `generation_id`, `publication_sequence`, and original byte range and content against the expected RFC. This read completes the cold-start timing. For automatic publication also confirm that the outbox row for the same generation and sequence reached `delivered`. Serve container health uses `/live`, so verify `/ready` and `rfc_mcp_rg_available` before opening ingress to readers.

### Upgrade existing volumes

For schema 9, close public ingress, drain Feed and Sync, and stop Serve. Take a matched database and mirror backup and retain the previous image references. Keep the existing roles and role passwords. Run the owner migrations, start the schema 9 images, and verify the active sequence, retained snapshots, `/ready` and a pinned MCP read before reopening ingress. Stop Sync and set `txt_provenance = "dual_source"` to turn off rsync-only trust while staying on schema 9. To return to schema 7 binaries, restore the matched pre-upgrade backup into fresh volumes and verify it with the previous images. The restored instance resumes at the backup's publication sequence. [Publication and recovery](publication.md)

## Proxy and network boundaries

Route HTTPS `/mcp` to the serve ingress address on port 8780. Preserve the external Host, Origin, relevant MCP request headers, content type, and Accept. Strip client-supplied forwarding headers and set `X-Real-IP` from the validated client address at the configured trusted proxy. Keep the request body limit at 64 KiB and the backend response timeout above the server's 15-second deadline. Bound headers and configure header and body read timeouts. Health and metrics use serve loopback. The two control listeners use dedicated private addresses.

Verify the proxy and backend paths after container recreation. Sync and feed have distinct outbound networks for their required upstream traffic. Configure the host firewall for Docker forwarding and applicable IPv6 routes. Treat external proxy exposure, registry publication, and live upstream verification as separate operations.

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

For a planned shutdown, close the proxy route first. Stop feed, let running sync work settle, then stop sync and serve before PostgreSQL. Use `docker compose stop` for a restart that preserves the backing directories. Check for unfinished work and pending notifications after startup. Keep the independent backup available.

## Repeat the local qualification

```sh
python3 -B scripts/phase0/integration.py --context rootless --project rfc-phase7-qualified
python3 -B scripts/phase7/integration.py --context rootless --image-prefix rfc-phase7-qualified
```

The harness uses temporary credentials and bind-backed storage under `/tmp`, isolated upstream networks, and a disposable proxy published on host loopback. Run it on each intended host architecture and kernel before serving public traffic. [Observed results](implementation-status.md#phase-7)

Run the [Phase 8 release qualification](release-readiness.md) for the full acceptance matrix, fresh-stack restoration, anonymous load measurements, and supply-chain inventory.
