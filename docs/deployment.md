# Docker Compose deployment

The production profile runs one serve, one sync, one feed, and PostgreSQL 19. It requires a qualified Linux Docker host, an existing HTTPS reverse proxy, and a dedicated Docker ingress network shared with that proxy. Follow the [deployment prerequisites](deployment-prerequisites.md).

<a id="https-first-phase-3-shadow-runbook"></a>
<a id="https-first-phase-4-shadow-runbook"></a>

## HTTPS-first Phase 5 shadow runbook

Keep incumbent Serve online with its qualified schema 9 images and mirror.
Phase 5 publication is available for private qualification and a separately
authorized shadow run. Build or preload matching schema 11 Serve, Sync, and
Feed images. Use a separate Compose project, private networks and
credentials, and a matched copy of the schema 9 database and mirror. Follow the
[backup and restore procedure](publication.md#backup-and-restore) to make the
copy. Point the shadow notification channel to a private receiver with its
own keys, and keep the shadow project off production ingress.

1. Provision a separate ext4 spool with 5 GiB and 65,536 inodes. Mount it
   `nosuid,nodev,noexec`, set mode `0700`, and map ownership to container
   UID/GID `10002:10002` using the deployment engine's namespace mapping.
   The spool holds run files and HTTPS cooldown state outside the canonical
   mirror and PostgreSQL. Allow at least 4 GiB plus 640 MiB free for the
   default accepted-byte budget and at least 40,016 free inodes.
2. Provision a separate 1 GiB disk-backed audit filesystem with 4,096
   inodes, mounted `nosuid,nodev,noexec` and owned by Sync at mode `0700`.
   Keep it outside the spool, canonical mirror, and PostgreSQL. Preserve
   this volume across process or pod replacement. Set
   `RFC_SYNC_SPOOL_HOST_DIR` and `RFC_SYNC_AUDIT_HOST_DIR` to their
   mountpoints and `RFC_SYNC_CONFIG_HOST_FILE` to a private copy of the
   [v2 TOML example](../deploy/ingestion.toml.example). Render the overlay
   and verify that Sync alone mounts both volumes and that Feed has only its
   internal control network. Run the preflight with the actual runtime UID
   and resource limits.
3. On the shadow copy, keep ingress closed and Feed, Sync, and Serve stopped.
   Start only PostgreSQL and apply `migrations/migrate.sql` with the owner
   migration procedure. Confirm schema 11, the incumbent
   sequence, retained snapshots, readiness, and an exact-byte pinned MCP
   read using matching schema 11 images before opening any shadow ingress.
   Keep the matched database and mirror backup and the corresponding schema 9
   or 10 images and TOML for rollback into fresh volumes.
4. Qualify first against private sources. Run `--phase2-reconcile --full`
   and check `ready`, matching catalogs, pacing, joined-document counts,
   and `txt_audited` equal to `expected_txt`. With the daemon stopped, run
   `--phase3-publish` using the generation ID and expected sequence from the
   report. Check the active pointer, `/ready`, a pinned MCP `get`, and the
   outbox. Repeat separately with the daemon and a private receiver. After an interruption,
   rerun the same scope with the original volumes to resume eligible receipts.
   Use the private audit `quarantine` directory to inspect disagreements.
   Preserve its receipts and bytes for diagnosis. A full quarantine or
   exhausted retry budget requires operator review of source convergence
   before another audit cycle.
   Run `--phase3-recover` when the shadow collector is stopped to reclaim
   expired and abandoned data.
5. Before any authorized official-source run, agree on sample or full scope,
   a time limit, and an upstream request cap. Drain incumbent Sync and
   Feed egress and wait out known cooldown before enabling shadow egress.
   Use a one-shot for bounded sample scope. A daemon startup queues a full
   run. Stop shadow Feed and Sync at the end of the agreed window, wait out
   any recorded cooldown, and resume incumbent collection. Verify the
   incumbent's readiness and queue or outbox progress. Retain the shadow
   spool's cooldown state through the handoff. Keep quarantine evidence for
   the recorded retention period or preserve it in a private incident archive
   before discarding the isolated snapshot and credentials.

~~~sh
export RFC_SHADOW_ENV=/path/to/private/shadow.env
shadow() {
  docker compose -p rfc-https-shadow --env-file "$RFC_SHADOW_ENV" \
    -f compose.yml -f deploy/compose.https-acquisition.yml "$@"
}
shadow config --quiet
shadow run --rm --no-deps rfc-mcp-docker-sync --check-config
shadow run --rm --no-deps rfc-mcp-docker-sync --phase2-reconcile --sample
~~~

Use the [private acquisition fixture](../scripts/https-discovery/integration.py)
with `--publication-only` for qualification. The sample command in the snippet uses the stock image
and is for the authorized official-source window. The remaining production
procedures apply to the qualified schema 9 release with its matching
checkout, images, and v1 configuration.

For the shadow publication rollback, stop the shadow Feed and Sync, then
use a retained generation with its original evidence and the current
active sequence:

~~~sh
shadow stop rfc-mcp-docker-feed rfc-mcp-docker-sync
shadow run --rm --no-deps rfc-mcp-docker-sync \
  --phase3-rollback "$retained_generation" --expect-sequence "$active_sequence"
~~~

## Provision

1. Use the three qualified service image references in a private copy of [the environment example](../deploy/compose.env.example). For a registry release, use the three published index digests and verify the host's CPU baseline. For a local build, build the `serve`, `sync`, and `feed` Dockerfile targets and set their tags in that file. [GHCR release and digest verification](ghcr-release.md)
2. Create the mirror and PostgreSQL backing directories. Choose nonoverlapping IPv4 subnets. Put only serve and the existing proxy on the dedicated ingress bridge. Give the proxy a separate frontend network and assign the fixed backend addresses from the environment file.
3. Copy [the PostgreSQL access policy](../deploy/pg_hba.conf.example) and update its two private subnets if needed. Supply a database certificate with DNS SAN `postgres`, its private key, and the issuing CA. Use independent passwords for PostgreSQL bootstrap, `rfc_serve`, and `rfc_sync`.
4. Generate three distinct 48-byte raw keys under a private umask. Make separate per-service copies of each pairwise key. Set the cursor and notification keys for serve to UID 10001, the sync copies to UID 10002, and the feed copy to UID 10003. PostgreSQL private material belongs to UID 70. Set secret files to mode 0400 and give the designated container UID access using the host’s actual UID mapping. Verify ownership inside the containers because Compose mounts the backing files directly.
5. Copy [the schema 9 ingestion example](../fixtures/synthetic/ingestion-v1.toml) to the host file named by `RFC_SYNC_CONFIG_HOST_FILE`. Use a regular file readable by Sync's container UID 10002 under the host's rootless or rootful mapping. Mode `0444` is suitable for the example, which has no credentials. On an upgrade, carry any old `RFC_SYNC_HTTP_CONCURRENCY` and `RFC_SYNC_PARSE_CONCURRENCY` values over to the TOML equivalents and remove those environment settings from custom Compose overrides. [Schema 9 collector](ingestion.md#schema-9-release-pipeline)

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

For schema 9, close public ingress, drain Feed and Sync, and stop Serve. Take a matched database and mirror backup and retain the previous image references. Keep the existing roles and role passwords. Run the owner migrations, start the schema 9 images, and verify the active sequence, retained snapshots, `/ready` and a pinned MCP read before reopening ingress. To return to earlier binaries that require an older schema, restore the matching pre-upgrade backup into fresh volumes and verify it with the saved images. The restored instance resumes at the backup's publication sequence. [Publication and recovery](publication.md)

### Return to strict batched ingestion

On schema 9, stop Feed and allow the running Sync job to settle before stopping Sync. Leave Serve running.

```sh
compose stop rfc-mcp-docker-feed
# Use the job check below to confirm that the running job has settled.
compose stop rfc-mcp-docker-sync
```

Install a TOML file with the following settings. Preserve the ingestion file path and the volume backing of the canonical mirror and database.

```toml
schema_version = 1

[ingestion]
txt_provenance = "dual_source"

[ingestion.https]
max_in_flight_requests = 1

[ingestion.rsync]
mode = "batched"

[ingestion.pipeline]
max_pending_documents = 1
parser_workers = 1
```

Remove the bulk rsync overlay from the Compose invocation. Leave any private staging filesystem available if you may return to bulk mode. Use the same project name and environment file.

```sh
compose config --quiet
compose run --rm --no-deps rfc-mcp-docker-sync --check-config
compose up -d --no-deps --force-recreate rfc-mcp-docker-sync
```

The restarted daemon queues a full run. Serve continues to read the active generation during the run. New strict membership requires a matching HTTPS proof or a reusable prior verified proof. Rsync-only observations remain historical evidence. On a TXT disagreement, check that the incumbent still passes the readiness and MCP probes. Resume Feed after verifying the daemon, active sequence and outbox. To reactivate a retained generation separately, use [sequence-fenced publication rollback](publication.md#operator-commands).

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
