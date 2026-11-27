#!/bin/sh
# Keep the volume lock in the supervisor even if PostgreSQL closes inherited FDs.
set -eu
umask 077
root=/var/lib/postgresql
exec 9>>"$root/.rfc-maintenance.lock"
flock -n 9 || { echo 'Database volume is in use by maintenance or another PostgreSQL instance' >&2; exit 1; }
if [ "$(cat "$PGDATA/PG_VERSION" 2>/dev/null)" != 19 ]; then
    echo 'PostgreSQL 19 data is unavailable. Run the Compose maintenance dependency first.' >&2
    exit 1
fi
database_id=$(LC_ALL=C pg_controldata "$PGDATA" | awk -F ': *' '/^Database system identifier:/ {print $2}')
case "$database_id" in ''|*[!0-9]*) echo 'Invalid PostgreSQL system identifier' >&2; exit 1;; esac
mkdir -p "$root/.rfc-peer"
cp /run/secrets/postgres_key /run/postgresql/server.key

docker-entrypoint.sh postgres -c ssl=on \
    -c ssl_cert_file=/run/secrets/postgres_cert \
    -c ssl_key_file=/run/postgresql/server.key \
    -c hba_file=/etc/rfc-pg-hba.conf \
    -c "unix_socket_directories=/run/postgresql,$root/.rfc-peer" 9>&- &
child=$!
trap 'kill -INT "$child" 2>/dev/null || :; wait "$child" || :; exit 0' INT TERM
while kill -0 "$child" 2>/dev/null; do
    if pg_isready -q -h /run/postgresql -t 1; then
        { printf '%s\n' "$database_id"; head -n 3 "$PGDATA/postmaster.pid"; } >"$root/.rfc-managed-system-id"
        break
    fi
    sleep 1
done
wait "$child"
