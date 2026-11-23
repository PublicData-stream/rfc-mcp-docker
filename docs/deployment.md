# Docker Compose deployment

The production profile runs one serve, one sync, one feed, and PostgreSQL 19. It requires a qualified Linux Docker host, an existing HTTPS reverse proxy, and a dedicated Docker ingress network shared with that proxy. Follow the [deployment prerequisites](deployment-prerequisites.md).

## Provision

1. Use the three qualified service image references in a private copy of [the environment example](../deploy/compose.env.example). For a local build, build the `serve`, `sync`, and `feed` Dockerfile targets and set their tags in that file. Use immutable digests for registry images.
2. Create the mirror and PostgreSQL backing directories. Choose nonoverlapping IPv4 subnets. Put only serve and the existing proxy on the dedicated ingress bridge. Give the proxy a separate frontend network and assign the fixed backend addresses from the environment file.
3. Copy [the PostgreSQL access policy](../deploy/pg_hba.conf.example) and update its two private subnets if needed. Supply a database certificate with DNS SAN `postgres`, its private key, and the issuing CA. Use independent passwords for PostgreSQL bootstrap, `rfc_serve`, and `rfc_sync`.
4. Generate three distinct 48-byte raw keys under a private umask. Make separate per-service copies of each pairwise key. Set the cursor and notification keys for serve to UID 10001, the sync copies to UID 10002, and the feed copy to UID 10003. PostgreSQL private material belongs to UID 70. Set secret files to mode 0400 and give the designated container UID access using the host’s actual UID mapping. Verify ownership inside the containers because Compose mounts the backing files directly.

```sh
docker network create --internal --subnet 172.30.70.0/28 rfc-public-ingress
docker compose --env-file /path/to/rfc.env --profile tools run --rm --no-deps volume-admin
docker compose --env-file /path/to/rfc.env up -d --wait postgres
```

The volume operation adjusts only the backing directories themselves. PostgreSQL owns `/var/lib/postgresql` and expects its data in `/var/lib/postgresql/19/docker`. Verify with `SHOW data_directory`. The role-and-subnet HBA rules require TLS and SCRAM for runtime clients. Use the administrator’s local peer-authenticated socket for setup.

## Initialize or upgrade

For a new cluster, run the roles script and set the two role passwords interactively to match their provisioned files. For upgrades, start with existing roles. Back up the database and mirror as described in the [publication runbook](publication.md) before changing schema or images.

```sh
docker compose --env-file /path/to/rfc.env exec postgres psql -X -U postgres -d rfc -f /migrations/roles.sql
docker compose --env-file /path/to/rfc.env exec postgres psql -X -U postgres -d rfc -c '\password rfc_serve'
docker compose --env-file /path/to/rfc.env exec postgres psql -X -U postgres -d rfc -c '\password rfc_sync'
docker compose --env-file /path/to/rfc.env exec postgres psql -X -U postgres -d rfc -f /migrations/bootstrap.sql
docker compose --env-file /path/to/rfc.env exec postgres psql -X -U postgres -d rfc -f /migrations/migrate.sql
docker compose --env-file /path/to/rfc.env run --rm --no-deps rfc-mcp-docker-sync --phase1-check-storage
docker compose --env-file /path/to/rfc.env run --rm --no-deps rfc-mcp-docker-serve --phase1-check-storage
docker compose --env-file /path/to/rfc.env up -d --wait
```

Peer authentication runs as the PostgreSQL container UID. Sync schedules the first publication and retries notifications independently. Feed waits for sync to start and uses its authenticated retry path. Serve container health uses `/live`. The loopback-only `/ready` requires a readable generation and the ripgrep sandbox. Check `/ready` and `rfc_mcp_rg_available` from inside serve before exposing the public route.

## Proxy and network boundaries

Route HTTPS `/mcp` to the serve ingress address on port 8780. Preserve the external Host, Origin, relevant MCP request headers, content type, and Accept. Strip client-supplied forwarding headers and set `X-Real-IP` from the validated client address at the configured trusted proxy. Keep the request body limit at 64 KiB and the backend response timeout above the server's 15-second deadline. Bound headers and configure header and body read timeouts. Health and metrics use serve loopback. The two control listeners use dedicated private addresses.

Verify the proxy and backend paths after container recreation. Sync and feed have distinct outbound networks for their required upstream traffic. Configure the host firewall for Docker forwarding and applicable IPv6 routes. Treat external proxy exposure, registry publication, and live upstream verification as separate operations.

The initial container memory limits are 768 MiB for serve, 2 GiB for sync, 128 MiB for feed, and 1 GiB for PostgreSQL. CPU limits are two for serve, sync, and PostgreSQL and 0.5 for feed. Confirm that the rootless daemon applies these settings with delegated cgroup v2 controllers. Check local logs and [control-plane health](control.md) after startup.

## Repeat the local qualification

```sh
python3 -B scripts/phase0/integration.py --context rootless --project rfc-phase7-qualified
python3 -B scripts/phase7/integration.py --context rootless --image-prefix rfc-phase7-qualified
```

The harness uses temporary credentials and bind-backed storage under `/tmp`, isolated upstream networks, and a disposable proxy published on host loopback. Run it on each intended host architecture and kernel before serving public traffic. [Observed results](implementation-status.md#phase-7)
