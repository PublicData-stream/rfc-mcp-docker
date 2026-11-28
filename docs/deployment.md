# Docker Compose deployment

The production profile runs one serve, one sync, one feed, and PostgreSQL 19. It requires a qualified Linux Docker host, an existing HTTPS reverse proxy, and a dedicated Docker ingress network shared with that proxy. Follow the [deployment prerequisites](deployment-prerequisites.md).

## Provision

1. Use the four qualified image references in a private copy of [the environment example](../deploy/compose.env.example). For a registry release, use the four published index digests and verify the host's CPU baseline. For a local build, build the `serve`, `sync`, `feed`, and `maintenance` Dockerfile targets and set their tags in that file. [GHCR release and digest verification](ghcr-release.md)
2. Create the mirror and PostgreSQL backing directories. Provision a separate private backup filesystem at `RFC_DB_BACKUP_HOST_DIR`, owned by PostgreSQL UID/GID 70 with mode 0700 as seen inside Docker. Choose nonoverlapping IPv4 subnets. Put only serve and the existing proxy on the dedicated ingress bridge. Give the proxy a separate frontend network and assign the fixed backend addresses from the environment file.
3. Copy [the PostgreSQL access policy](../deploy/pg_hba.conf.example) and update its two private subnets if needed. Supply a database certificate with DNS SAN `postgres`, its private key, and the issuing CA. Use independent passwords for PostgreSQL bootstrap, `rfc_serve`, and `rfc_sync`.
4. Generate three distinct 48-byte raw keys under a private umask. Make separate per-service copies of each pairwise key. Set the cursor and notification keys for serve to UID 10001, the sync copies to UID 10002, and the feed copy to UID 10003. PostgreSQL private material belongs to UID 70. Give that UID independent mode-0400 copies of both runtime database passwords at `RFC_BOOTSTRAP_SERVE_DB_PASSWORD` and `RFC_BOOTSTRAP_SYNC_DB_PASSWORD` for fresh initialization. Set secret files to mode 0400 and give the designated container UID access using the host’s actual UID mapping. Verify ownership inside the containers because Compose mounts the backing files directly.
5. Copy the [v2 ingestion example](../deploy/ingestion.toml.example) to
   the regular file named by `RFC_SYNC_CONFIG_HOST_FILE`. Mode `0444` is
   suitable for this nonsecret file. Compose mounts it read-only for Sync.
   Translate settings from v1 before switching images. [Translate v1 settings](ingestion.md#translate-v1-settings)
6. Provision the private HTTPS spool and audit filesystems before rendering
   or starting the stack. [Staging setup](#private-sync-staging)

```sh
export RFC_ENV_FILE=/path/to/private/rfc.env COMPOSE_PROJECT_NAME=rfc-mcp-docker
compose() { docker compose --env-file "$RFC_ENV_FILE" "$@"; }
docker network create --internal --subnet 172.30.70.0/28 rfc-public-ingress
compose config --quiet
```

Set `COMPOSE_PROJECT_NAME` to the existing project when upgrading or
recovering a deployment. Keep the same Docker context and Compose overlays
on every command, including in the `compose` function.

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
compose config --quiet
compose run --rm --no-deps rfc-mcp-docker-sync --check-config
~~~

## Initialize or upgrade

During `compose up`, the maintenance job checks both application image
schema targets and Sync configuration before opening the database volume.
After maintenance, PostgreSQL listens on its private networks. Serve and
Sync start after their role-specific storage checks and the local rights
refresh. Inspect the job logs when startup fails.

~~~sh
compose logs serve-schema-preflight sync-config-preflight db-maintenance \
  serve-db-preflight sync-db-preflight sync-rights-preflight
~~~

Run the explicit preflight to inspect the installed image set and current
database state. A stopped database reports its major version and layout.
The maintenance job reads the SQL migration history when it opens the
stopped database.

~~~sh
python3 -B scripts/deploy/upgrade.py preflight --env-file "$RFC_ENV_FILE" \
  --project-name "$COMPOSE_PROJECT_NAME"
~~~

### Initialize fresh volumes

Provision the backing paths, keys, certificate, and private networks first.
Begin the cold-start clock immediately before `volume-admin`. It adjusts
the backing directory roots. Compose then initializes PostgreSQL 19 in
`/var/lib/postgresql/19/docker`, creates the scoped roles and schema, and
starts the requested services. Start Serve alone if the first publication
will be operator-controlled.

~~~sh
compose --profile tools run --rm --no-deps volume-admin
compose up -d --wait rfc-mcp-docker-serve
~~~

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

Use the matching schema 13 release. Prepare the current ingestion TOML,
private spool and audit filesystems, and the independent backup storage.
Preserve the leases and cooldown recorded in the old spool. Remove legacy
bulk and HTTPS shadow overrides from the target command.

- For an online search rollout on an existing PostgreSQL 19, schema 13 deployment, change only the qualified `RFC_SERVE_IMAGE` pin and run the host deploy command. Keep the installed Sync, Feed, Maintenance, and PostgreSQL pins and the existing cursor key. The command automatically chooses the online path after checking the running deployment. It keeps PostgreSQL, Sync, and Feed online and briefly interrupts MCP while replacing the single Serve container.
- For a full-stack upgrade, close the external ingress route and run the same host deploy command. It stops Feed, gracefully stops Sync and Serve, stops PostgreSQL when required, runs maintenance, and verifies Serve before it starts Sync and Feed.
- For a stopped deployment, use `compose up -d --wait` with the same project and backing volumes. To apply maintenance and check storage while keeping applications stopped, use the second command below.

~~~sh
python3 -B scripts/deploy/upgrade.py deploy --env-file "$RFC_ENV_FILE" \
  --project-name "$COMPOSE_PROJECT_NAME"

python3 -B scripts/deploy/upgrade.py preflight --apply --env-file "$RFC_ENV_FILE" \
  --project-name "$COMPOSE_PROJECT_NAME"
~~~

Use the same Compose project, Docker context, environment file, and overlays
throughout the operation. Pass `--context` and repeated `-f` options to the
host command when the deployment uses them. The host commands require
Python 3.11 or later. Pull the qualified digest references before running
the host command.

The deploy command checks discovery and expects HTTP 503 from readiness
when the active pointer is empty. With an active generation it requires
readiness and checks the MCP listing against the stored generation and
publication sequence. If it finds an eligible whole-file read, it also
reassembles the returned bytes and verifies their length and BLAKE3 digest.
Verify the external HTTPS proxy path separately before reopening ingress.

The online path sets a PostgreSQL temporary-file cap of at most 32 MiB per
Serve backend. It uses the existing PostgreSQL peer connection and applies
the setting to fresh Serve connections. It then checks readiness, MCP
discovery, and a native search. The application schema, corpus, and generation
history require no migration or re-ingestion. Maintenance provisions the cap
for stopped and fresh deployments. If replacement or verification fails,
the deploy command restores the previous image and role setting. Rerun
`deploy` to recover an interrupted attempt. Keep the previous Serve image
available locally during rollout. To roll back after a successful rollout,
restore the previous Serve pin and run `deploy` again.

The runner supports complete application schema histories 9 through 13. It
also imports a dedicated RFC PostgreSQL 18 cluster from `18/docker` or
`RFC_POSTGRES18_DATA_DIR`, a relative path beneath the PostgreSQL backing
volume. Use a cleanly stopped source and keep its directory for recovery.
The PostgreSQL 19 target uses a staged logical restore and rebuilds its
derived full-text index from the stored search text. Shared clusters,
external tablespaces, and ambiguous data directories require manual
handling.

A populated upgrade creates an archive and a complete verified mirror copy
on the independent filesystem before applying SQL. Budget space for the
archive and mirror on that filesystem and for the restore rehearsal in
the PostgreSQL backing filesystem. A PostgreSQL 18 import also needs space
for a private copy of the old cluster. Keep completed backups according
to the deployment's retention policy. [Backup and restore](publication.md#backup-and-restore)

On failure, leave ingress closed and check the maintenance and postflight
job logs. A retry verifies the current state and resumes from the failed
stage. A completed SQL change or PostgreSQL cutover remains in place while
postflight is repaired. Use a matched backup, the earlier image set, and
fresh volumes for rollback across schemas or PostgreSQL majors. Confirm
the active and retained generations, Serve readiness, audited publication,
and authenticated hint delivery before reopening public ingress.

For manual peer-socket recovery of an existing PostgreSQL 19 volume, keep
the application services stopped and start PostgreSQL with
`compose up -d --no-deps postgres`. The supplied-backup command uses the
same project and checks the archive against the quiescent database. It
uses `b3sum` installed on the host.

~~~sh
python3 -B scripts/deploy/upgrade.py --env-file "$RFC_ENV_FILE" \
  --project-name "$COMPOSE_PROJECT_NAME" --backup-dir "$BACKUP_DIR"
~~~

### Recover a schema mismatch after Compose recreation

Use this procedure when PostgreSQL reports healthy and Sync repeatedly
reports `unsupported storage schema version`. Release `2.5.0` expects
schema 11 and release `2.6.0` expects schema 12.

1. Close ingress. Stop Feed, allow any active Sync job to finish, then stop
   Sync and Serve. Preserve the PostgreSQL, mirror, spool, and audit backing
   directories. Record actual container image IDs and the full database
   migration history before changing images or applying SQL.

   ~~~sh
   compose images --format json
   compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc \
     -c 'SELECT version FROM rfc.schema_migrations ORDER BY version' \
     -c 'SELECT generation_id, publication_sequence FROM rfc.active_generation'
   ~~~

2. For a complete schema 9–12 history and a schema 13 target, use the
   [upgrade procedure](#upgrade-existing-volumes). For a recovery that
   stays on `2.6.0`, use its pinned checkout and matching image set. Verify
   the independent backup and apply its manual peer-socket migration below
   if the database needs an upgrade. If the database is already schema 12,
   proceed directly to the two storage checks. For absent, gapped, or newer
   history, identify the matching deployment or backup before proceeding.

   ~~~sh
   compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc -f /migrations/migrate.sql
   compose exec -T postgres psql -X -v ON_ERROR_STOP=1 -U postgres -d rfc \
     -c 'SELECT version FROM rfc.schema_migrations ORDER BY version' \
     -c 'SELECT generation_id, publication_sequence FROM rfc.active_generation'
   compose run --rm --no-deps rfc-mcp-docker-serve --phase1-check-storage
   compose run --rm --no-deps rfc-mcp-docker-sync --phase1-check-storage
   ~~~

3. Match the active pointer against the backup. Run
   `compose up -d --wait rfc-mcp-docker-serve`, verify the serving path, and
   resume services in the [upgrade order](#upgrade-existing-volumes).
   Check the new logs for schema preflight rejections, PostgreSQL
   open-transaction EOF, and repeated Feed delivery failures. Use the
   fresh-volume restore procedure to roll back across schemas.

### Rollback

For rollback within schema 13, stop Feed and Sync, read the active sequence,
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

Route HTTPS `/mcp` to the serve ingress address on port 8780. Preserve the external Host, Origin, relevant MCP request headers, content type, and Accept. Keep the request body limit at 64 KiB and the backend response timeout above the server's 15-second deadline. Bound headers and configure header and body read timeouts. Health and metrics use serve loopback. The two control listeners use dedicated private addresses.

At the edge proxy, strip client-supplied forwarding headers, including the
selected client IP header, and write that header with the observed client
IP. Serve defaults to `X-Real-IP` and one bare IP address. To select a
different header, set `RFC_MCP_CLIENT_IP_HEADER` in the Compose environment
file. In a multi-proxy deployment, set `RFC_MCP_CLIENT_IP_HEADER_MODE=chain`
and have each subsequent trusted proxy append its observed peer to the
same comma-separated field. For example, the header name can be
`X-Forwarded-For`. Set `RFC_MCP_TRUSTED_PROXIES` to the IPs of the immediate
ingress proxy and each trusted hop whose address can occur in the field.
When omitted, Compose trusts the address in `RFC_INGRESS_PROXY_IP`. Each
address on the list can also supply client identity if it reaches Serve
directly. Keep Serve reachable only on the private ingress route.

For TOML, copy the [Serve example](../deploy/serve.toml.example) to the
regular file named by `RFC_SERVE_CONFIG_HOST_FILE`. Give the Serve container
read access and include the [Serve overlay](../deploy/compose.serve-config.yml)
after the base file in every Compose command. Omit the equivalent
environment keys to use the TOML values. Serve reads the UTF-8 file on
startup and rejects unknown keys or files over 16 KiB.

```sh
docker compose -f compose.yml -f deploy/compose.serve-config.yml --env-file "$RFC_ENV_FILE" config --quiet
```

To move between header names, first configure the proxy to emit both names
with the values required by the old and new modes. Strip incoming copies
of both at the edge. Apply the new Serve settings and run
`compose up -d --no-deps --force-recreate rfc-mcp-docker-serve` using the
same Compose files. Verify MCP requests through the proxy, then remove the
old header. For rollback, restore emission of the previous header and
mode before recreating Serve with its previous settings. If the name
stays the same and the modes require different field values, coordinate
the proxy and Serve changes in a maintenance window.

Verify the proxy and backend paths after container recreation. Sync owns upstream egress. Feed uses only the internal control network. Configure the host firewall for Docker forwarding and applicable IPv6 routes. Treat external proxy exposure, registry publication, and live upstream verification as separate operations.

The initial container memory limits are 768 MiB for serve, 2 GiB for sync, 128 MiB for feed, and 1 GiB for PostgreSQL. CPU limits are two for serve, sync, and PostgreSQL and 0.5 for feed. Confirm that the rootless daemon applies these settings with delegated cgroup v2 controllers. Check local logs and [control-plane health](control.md) after startup.


### Size and operate the Serve database pool

Each Serve process has one pool. The built-in values match
[`deploy/serve.toml.example`](../deploy/serve.toml.example). Environment
variables override individual settings in the optional Serve TOML file.

| `[database.pool]` key | Environment override | Default |
| --- | --- | ---: |
| `min_idle` | `RFC_MCP_DB_POOL_MIN_IDLE` | 2 |
| `max_connections` | `RFC_MCP_DB_POOL_MAX_CONNECTIONS` | 8 |
| `acquire_timeout_ms` | `RFC_MCP_DB_POOL_ACQUIRE_TIMEOUT_MS` | 1000 |
| `idle_timeout_seconds` | `RFC_MCP_DB_POOL_IDLE_TIMEOUT_SECONDS` | 300 |
| `max_lifetime_seconds` | `RFC_MCP_DB_POOL_MAX_LIFETIME_SECONDS` | 1800 |
| `shutdown_timeout_ms` | `RFC_MCP_DB_POOL_SHUTDOWN_TIMEOUT_MS` | 2000 |

Allow 3–18 connections per replica and a minimum idle count between zero
and that maximum. The idle timeout applies above the background warm
target. An expired leased session retires after release. The acquisition
timeout bounds the wait for capacity. Once assigned a slot, opening and
verifying a connection uses the remaining operation time. Each capacity
class opens one connection at a time. Requests can exhaust their wait
while that class is already connecting after an outage. The maximum
connection lifetime is 86,400 seconds, the acquisition timeout is at most
15,000 ms, and shutdown allows at most 5,000 ms for database closure.

By default, six connections are available to public tools and one apiece
is reserved for readiness and authenticated publication notifications.
Serve can become live without a working database or published generation.
Full identity, TLS, schema, role, and privilege verification runs on
physical connection establishment and every checkout. A change to the
mounted password or CA file retires idle connections that used the previous
material when Serve next checks out a connection. Use a Serve restart for
immediate server-side certificate or password cutover. Recreate the
container when rotating a file-backed Compose secret by replacing the
host file with a new inode.

Multiply `max_connections` by the maximum simultaneous Serve replicas,
including replicas that overlap during a rollout. Reserve that many slots
within PostgreSQL's regular connection limit as reported by
`SHOW max_connections` after subtracting PostgreSQL's reserved slots.
Allow up to 20 additional slots per Sync daemon. The leader, scheduler,
outbox dispatcher, and current reconciliation or catalog probe can
occupy four slots. Up to 16 admitted private requests can each need a
short-lived writer. Count overlapping standalone reconciliation, Sync
CLI or recovery work, PostgreSQL health probes, and administrative
connections separately. Confirm the budget using `pg_stat_activity` and
the PostgreSQL PID and memory headroom on the deployment host.

On SIGTERM Serve closes its listeners, drains admitted requests for up to
15 seconds, cancels stragglers, and gracefully closes verified idle
connections in parallel. Compose allows 25 seconds for the whole stop.
Use loopback `/metrics` to check
`rfc_mcp_db_pool_{physical,idle,leased,connecting,retiring,waiters}`,
connection reuse, and graceful and forced close counters. PostgreSQL may
log forced disconnects after cancellation. Correlate by connection timing
and backend PID when investigating unexpected errors.

To roll back, select the previous Serve image and previous Serve TOML,
remove the pool environment overrides, and recreate Serve through Compose.
Check `/live`, `/ready`, and a pinned MCP read. Retain the cursor key across
the rollback and use the existing PostgreSQL data and mirror.

## Operate and recover

Use the Compose project and private environment file set during
[provisioning](#provision), along with any required context and overlays.

Run Sync monitoring commands inside the Sync container as UID 10002.
Use `kubectl exec` against the Sync container when running the private
Minikube probe.

```sh
docker compose --env-file "$RFC_ENV_FILE" exec -T rfc-mcp-docker-sync \
    rfc-mcp-docker-sync status
docker compose --env-file "$RFC_ENV_FILE" exec -T rfc-mcp-docker-sync \
    rfc-mcp-docker-sync watch
docker compose --env-file "$RFC_ENV_FILE" exec -T rfc-mcp-docker-sync \
    rfc-mcp-docker-sync diagnose --json
kubectl exec sync -- /usr/local/bin/rfc-mcp-docker-sync status
```

When Sync is stopped, run `status` or `diagnose` in a one-off Sync container
using the same Compose configuration and mounted settings. PostgreSQL can
still provide job, publication, and notification evidence.

```sh
docker compose --env-file "$RFC_ENV_FILE" run --rm --no-deps rfc-mcp-docker-sync status --json
docker compose --env-file "$RFC_ENV_FILE" run --rm --no-deps rfc-mcp-docker-sync diagnose
```

Use `docker compose --env-file "$RFC_ENV_FILE" logs rfc-mcp-docker-sync` and the run ID or durable job ID
from the report to find the relevant events. For notification delivery, use
the publication sequence. Set `RFC_SYNC_LOG_LEVEL=debug` in the Compose
environment and recreate Sync when you need per-RFC attempt and batch
detail. Restore the default `info` level after collecting the details.
[Sync CLI and log contract](ingestion.md#monitor-sync)

Inspect the project and the loopback serving health before opening or reopening ingress.

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

HTTP 429 carries `Retry-After: 1`. Reduce client burst and concurrency, and verify that the proxy sets the selected client IP header and that Serve trusts the intended proxy addresses. For a tool deadline, budget error, or partial scan, narrow the query or continue with a valid cursor. Check in-flight requests, admission rejections, database readers, and ripgrep availability before adjusting limits. Keep the sandbox and its failure checks enabled. [MCP tools and budgets](mcp.md)

Use the receiver-first procedure for each [HMAC key rotation](control.md#clock-and-key-rotation). Use the automatic database upgrade procedure with the new Compose configuration and the pinned image set. Check the schema version, TLS, roles, storage, readiness, proxy path, and a pinned MCP read before reopening ingress. Use the [publication runbook](publication.md) for GC, sequence-bound rollback, backup, restore, and corruption. Schema rollback requires a compatible backup and fresh-volume restore when the previous binary cannot read the upgraded schema.

For a planned shutdown, close the proxy route first. Stop Feed, let running Sync work settle, then stop Sync and Serve before PostgreSQL. If Sync must interrupt a job, use `compose stop -t 60 rfc-mcp-docker-sync` to allow cooperative cancellation. Start PostgreSQL and Serve, then Sync and Feed. Check unfinished jobs, publication sequence, and pending notifications after restart. Keep the independent backup available.

## Repeat the local qualification

```sh
docker --context rootless build --target maintenance -t rfc-phase7-qualified-maintenance .
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

## Source policy rollout

The schema 13 upgrade refreshes rights for active and retained generations
from their verified originals and catalog data. Confirm ordinary whole-file
reads, an attributed excerpt, and a metadata-only regex hit before reopening
ingress. See [private rights diagnostics](source-policy.md#diagnose-rights).
