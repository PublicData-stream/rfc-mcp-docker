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
\if :{?upgrade_from}
SELECT ARRAY(SELECT version FROM rfc.schema_migrations ORDER BY version) =
       ARRAY(SELECT generate_series(1, :upgrade_from)) AS source_matches
\gset
\if :source_matches
\else
DO $$ BEGIN RAISE EXCEPTION 'source migration history changed before upgrade'; END $$;
\endif
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 1) AS apply_v1
\gset
\if :apply_v1
\ir 0001-storage.sql
INSERT INTO rfc.schema_migrations (version) VALUES (1);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 2) AS apply_v2
\gset
\if :apply_v2
\ir 0002-ingestion.sql
INSERT INTO rfc.schema_migrations (version) VALUES (2);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 3) AS apply_v3
\gset
\if :apply_v3
\ir 0003-publication.sql
INSERT INTO rfc.schema_migrations (version) VALUES (3);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 4) AS apply_v4
\gset
\if :apply_v4
\ir 0004-search.sql
INSERT INTO rfc.schema_migrations (version) VALUES (4);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 5) AS apply_v5
\gset
\if :apply_v5
\ir 0005-reviewed-pagination.sql
INSERT INTO rfc.schema_migrations (version) VALUES (5);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 6) AS apply_v6
\gset
\if :apply_v6
\ir 0006-control.sql
INSERT INTO rfc.schema_migrations (version) VALUES (6);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 7) AS apply_v7
\gset
\if :apply_v7
\ir 0007-rsync-provenance.sql
INSERT INTO rfc.schema_migrations (version) VALUES (7);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 8) AS apply_v8
\gset
\if :apply_v8
\ir 0008-txt-comparison.sql
INSERT INTO rfc.schema_migrations (version) VALUES (8);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 9) AS apply_v9
\gset
\if :apply_v9
\ir 0009-ingestion-checkpoints.sql
INSERT INTO rfc.schema_migrations (version) VALUES (9);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 10) AS apply_v10
\gset
\if :apply_v10
\ir 0010-https-processing.sql
INSERT INTO rfc.schema_migrations (version) VALUES (10);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 11) AS apply_v11
\gset
\if :apply_v11
\ir 0011-https-publication.sql
INSERT INTO rfc.schema_migrations (version) VALUES (11);
\endif
SELECT NOT EXISTS (SELECT FROM rfc.schema_migrations WHERE version = 12) AS apply_v12
\gset
\if :apply_v12
\ir 0012-source-policy.sql
INSERT INTO rfc.schema_migrations (version) VALUES (12);
\endif
\if :{?upgrade_from}
SELECT ARRAY(SELECT version FROM rfc.schema_migrations ORDER BY version) =
       ARRAY(SELECT generate_series(1, :upgrade_to))
   AND (SELECT concat(generation_id, '|', publication_sequence)
        FROM rfc.active_generation) IS NOT DISTINCT FROM :'upgrade_pointer'
   AND (SELECT coalesce(string_agg(id::text, ',' ORDER BY id), '')
        FROM rfc.generations WHERE state IN ('published','retained')) = :'upgrade_readable'
   AS target_matches
\gset
\if :target_matches
\else
DO $$ BEGIN RAISE EXCEPTION 'upgrade changed the expected migration history or readable generations'; END $$;
\endif
\endif
COMMIT;
