\set ON_ERROR_STOP on
CREATE ROLE rfc_owner NOLOGIN NOINHERIT;
CREATE ROLE rfc_serve LOGIN NOINHERIT;
CREATE ROLE rfc_sync LOGIN NOINHERIT;
ALTER ROLE rfc_serve IN DATABASE rfc SET default_transaction_read_only = on;
ALTER ROLE rfc_serve IN DATABASE rfc SET search_path = pg_catalog, rfc;
ALTER ROLE rfc_sync IN DATABASE rfc SET search_path = pg_catalog, rfc;
\ir serve-resource-limits.sql
