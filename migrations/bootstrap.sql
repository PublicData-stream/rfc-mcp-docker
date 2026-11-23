\set ON_ERROR_STOP on
BEGIN;
REVOKE ALL ON DATABASE rfc FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE rfc TO rfc_serve, rfc_sync;
GRANT CREATE ON DATABASE rfc TO rfc_owner;

-- Remove built-in mutation capabilities irrelevant to this dedicated database.
SELECT format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC', oid::regprocedure)
FROM pg_proc WHERE pronamespace = 'pg_catalog'::regnamespace AND
    (proname LIKE 'pg%advisory%lock%' OR proname IN
     ('pg_notify', 'lo_create', 'lo_creat', 'lo_from_bytea', 'lo_import', 'lo_export',
      'lo_open', 'lo_put', 'lowrite', 'lo_truncate', 'lo_truncate64', 'lo_unlink'))
\gexec
GRANT EXECUTE ON FUNCTION pg_catalog.pg_advisory_xact_lock_shared(bigint) TO rfc_serve;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_try_advisory_xact_lock(bigint) TO rfc_sync;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_try_advisory_lock(bigint) TO rfc_sync;
COMMIT;
