ALTER TABLE rfc.document_versions
    ADD COLUMN section_state rfc.coverage_state,
    ADD COLUMN line_starts bigint[],
    ADD CHECK ((section_state IS NULL) = (line_starts IS NULL)),
    ADD CHECK (line_starts IS NULL OR
        (cardinality(line_starts) > 0 AND line_starts[1] = 0
            AND line_starts[cardinality(line_starts)] < byte_length));

ALTER TABLE rfc.generation_documents
    ADD COLUMN publication_month date,
    ADD COLUMN rfcxml_observation rfc.digest REFERENCES rfc.source_observations,
    ADD CHECK (publication_month IS NULL OR
        (publication_date IS NULL AND extract(day FROM publication_month) = 1));
CREATE INDEX generation_documents_rfcxml ON rfc.generation_documents (rfcxml_observation);

CREATE TABLE rfc.document_lexemes (
    document_version rfc.digest NOT NULL REFERENCES rfc.document_versions,
    lexeme text COLLATE pg_catalog."C" NOT NULL CHECK (octet_length(lexeme) BETWEEN 1 AND 2047),
    PRIMARY KEY (document_version, lexeme)
);
CREATE INDEX document_lexemes_lookup ON rfc.document_lexemes (lexeme, document_version);
CREATE TRIGGER insert_lexeme BEFORE INSERT ON rfc.document_lexemes
    FOR EACH ROW EXECUTE FUNCTION rfc.check_document_open();

CREATE TABLE rfc.generation_ingestion (
    generation_id bigint PRIMARY KEY REFERENCES rfc.generations,
    scope text NOT NULL CHECK (scope IN ('full', 'sample')),
    catalog_before rfc.digest NOT NULL REFERENCES rfc.source_observations,
    catalog_after rfc.digest NOT NULL REFERENCES rfc.source_observations,
    policy_registry text NOT NULL CHECK (octet_length(policy_registry) BETWEEN 1 AND 128),
    catalog_only jsonb NOT NULL CHECK (jsonb_typeof(catalog_only) = 'array')
);
CREATE INDEX generation_ingestion_catalog_before ON rfc.generation_ingestion (catalog_before);
CREATE INDEX generation_ingestion_catalog_after ON rfc.generation_ingestion (catalog_after);
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.generation_ingestion
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();

CREATE FUNCTION rfc.check_ingestion_sources() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM rfc.source_observations b JOIN rfc.source_observations a USING (raw_digest)
        WHERE b.identity = NEW.catalog_before AND a.identity = NEW.catalog_after
          AND b.kind = 'catalog' AND a.kind = 'catalog'
          AND b.https_verification = 'verified' AND a.https_verification = 'verified'
    ) THEN
        RAISE EXCEPTION 'ingestion requires two matching verified catalog observations' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ingestion_sources BEFORE INSERT ON rfc.generation_ingestion
    FOR EACH ROW EXECUTE FUNCTION rfc.check_ingestion_sources();

CREATE FUNCTION rfc.check_rfcxml_observation() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NEW.rfcxml_observation IS NOT NULL AND NOT EXISTS (
        SELECT FROM rfc.source_observations
        WHERE identity = NEW.rfcxml_observation AND kind = 'rfcxml'
          AND https_verification = 'verified'
          AND source_url = 'https://www.rfc-editor.org/rfc/rfc' || NEW.rfc_id || '.xml'
    ) THEN
        RAISE EXCEPTION 'membership requires RFCXML provenance for the selected RFC' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER membership_rfcxml BEFORE INSERT ON rfc.generation_documents
    FOR EACH ROW EXECUTE FUNCTION rfc.check_rfcxml_observation();

GRANT SELECT ON rfc.document_lexemes, rfc.generation_ingestion TO rfc_serve, rfc_sync;
GRANT INSERT ON rfc.document_lexemes, rfc.generation_ingestion TO rfc_sync;
GRANT INSERT (section_state, line_starts) ON rfc.document_versions TO rfc_sync;
