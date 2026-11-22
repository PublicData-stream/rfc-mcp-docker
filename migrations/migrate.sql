\set ON_ERROR_STOP on
BEGIN;
SELECT pg_catalog.pg_advisory_xact_lock(-2::bigint);
SET LOCAL ROLE rfc_owner;
SET LOCAL search_path = pg_catalog, rfc;
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES REVOKE USAGE ON TYPES FROM PUBLIC;
CREATE SCHEMA IF NOT EXISTS rfc AUTHORIZATION rfc_owner;
CREATE TABLE IF NOT EXISTS rfc.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    applied_at timestamptz NOT NULL DEFAULT now()
);
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 1) AS apply_v1
\gset
\if :apply_v1
\ir 0001-storage.sql
INSERT INTO rfc.schema_migrations (version) VALUES (1);
\endif
COMMIT;
