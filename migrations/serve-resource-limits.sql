\set ON_ERROR_STOP on
-- Use the most specific role/database setting, then the server default.
WITH defaults AS (
    SELECT substring(item FROM position('=' IN item) + 1) AS value,
           (setdatabase <> 0)::integer + 2 * (setrole <> 0)::integer AS priority
    FROM pg_catalog.pg_db_role_setting
    CROSS JOIN LATERAL pg_catalog.unnest(setconfig) AS config(item)
    WHERE setdatabase IN (0, (SELECT oid FROM pg_catalog.pg_database WHERE datname='rfc'))
      AND setrole IN (0, (SELECT oid FROM pg_catalog.pg_roles WHERE rolname='rfc_serve'))
      AND item LIKE 'temp_file_limit=%'
    UNION ALL
    SELECT setting, -1 FROM pg_catalog.pg_settings WHERE name='temp_file_limit'
), effective AS (
    SELECT CASE WHEN value ~ '^-?[0-9]+$' THEN value::bigint * 1024
                ELSE pg_catalog.pg_size_bytes(value) END AS bytes
    FROM defaults ORDER BY priority DESC LIMIT 1
)
SELECT format('ALTER ROLE rfc_serve IN DATABASE rfc SET temp_file_limit = %L', '32MB')
FROM effective WHERE bytes < 0 OR bytes > 32 * 1024 * 1024
\gexec
