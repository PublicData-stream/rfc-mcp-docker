REVOKE CREATE, TEMPORARY ON DATABASE rfc_phase0 FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA phase0 AUTHORIZATION phase0_owner;
GRANT USAGE ON SCHEMA phase0 TO phase0_reader, phase0_writer;
GRANT CONNECT ON DATABASE rfc_phase0 TO phase0_reader, phase0_writer;
GRANT CREATE ON DATABASE rfc_phase0 TO phase0_owner;

SET ROLE phase0_owner;
CREATE EXTENSION pg_trgm WITH SCHEMA phase0;
CREATE TABLE phase0.corpus (id integer PRIMARY KEY, title text NOT NULL);
INSERT INTO phase0.corpus VALUES (1, 'RFC café'), (2, 'RFC example');
CREATE INDEX ON phase0.corpus USING gin (title phase0.gin_trgm_ops);
GRANT SELECT ON phase0.corpus TO phase0_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON phase0.corpus TO phase0_writer;
RESET ROLE;
