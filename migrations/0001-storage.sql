CREATE DOMAIN rfc.digest AS bytea CHECK (octet_length(VALUE) = 32);
CREATE DOMAIN rfc.rfc_number AS integer CHECK (VALUE > 0);
CREATE DOMAIN rfc.byte_offset AS bigint CHECK (VALUE >= 0);
CREATE DOMAIN rfc.coverage_state AS text CHECK (VALUE IN ('complete', 'partial', 'unavailable'));

CREATE TABLE rfc.blobs (
    digest rfc.digest PRIMARY KEY,
    algorithm text NOT NULL DEFAULT 'blake3-256' CHECK (algorithm = 'blake3-256'),
    byte_length rfc.byte_offset NOT NULL,
    integrity_state text NOT NULL DEFAULT 'verified' CHECK (integrity_state IN ('verified', 'quarantined', 'missing')),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (digest, byte_length)
);

CREATE TABLE rfc.source_observations (
    identity rfc.digest PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('txt', 'catalog', 'rfcxml', 'atom', 'rss')),
    source_url text NOT NULL CHECK (octet_length(source_url) BETWEEN 1 AND 2048),
    observed_at timestamptz NOT NULL,
    fetched_at timestamptz NOT NULL,
    etag text CHECK (octet_length(etag) <= 4096),
    last_modified text CHECK (octet_length(last_modified) <= 4096),
    raw_digest rfc.digest NOT NULL REFERENCES rfc.blobs,
    https_verification text NOT NULL CHECK (https_verification IN ('verified', 'mismatch', 'unverified'))
);
CREATE INDEX source_observations_blob ON rfc.source_observations (raw_digest);

CREATE TABLE rfc.rfc_identities (rfc_id rfc.rfc_number PRIMARY KEY);

CREATE TABLE rfc.document_versions (
    identity rfc.digest PRIMARY KEY,
    rfc_id rfc.rfc_number NOT NULL REFERENCES rfc.rfc_identities,
    txt_digest rfc.digest NOT NULL,
    byte_length rfc.byte_offset NOT NULL,
    parser_version text NOT NULL CHECK (octet_length(parser_version) BETWEEN 1 AND 128),
    index_version text NOT NULL CHECK (octet_length(index_version) BETWEEN 1 AND 128),
    encoding text NOT NULL CHECK (encoding IN ('utf8', 'bytes')),
    indexing_state rfc.coverage_state NOT NULL,
    notice_state rfc.coverage_state NOT NULL,
    notice_policy text NOT NULL CHECK (notice_policy IN ('unresolved', 'metadata_only', 'whole_only', 'excerpt_allowed')),
    index_identity rfc.digest NOT NULL,
    sealed boolean NOT NULL DEFAULT false,
    FOREIGN KEY (txt_digest, byte_length) REFERENCES rfc.blobs (digest, byte_length),
    UNIQUE (identity, byte_length),
    UNIQUE (identity, rfc_id)
);
CREATE INDEX document_versions_blob ON rfc.document_versions (txt_digest);

CREATE TABLE rfc.generations (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 0),
    state text NOT NULL DEFAULT 'building' CHECK (state IN ('building', 'ready', 'published', 'retained', 'deleting', 'deleted', 'failed')),
    parser_version text NOT NULL CHECK (octet_length(parser_version) BETWEEN 1 AND 128),
    index_version text NOT NULL CHECK (octet_length(index_version) BETWEEN 1 AND 128),
    manifest_format integer,
    manifest_bytes bytea,
    manifest_digest rfc.digest,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    retired_at timestamptz,
    CHECK (
        (manifest_format IS NULL AND manifest_bytes IS NULL AND manifest_digest IS NULL AND state IN ('building', 'failed'))
        OR (manifest_format IS NOT NULL AND manifest_format > 0 AND manifest_bytes IS NOT NULL
            AND octet_length(manifest_bytes) > 0 AND manifest_digest IS NOT NULL AND state <> 'building')
    )
);

CREATE TABLE rfc.active_generation (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation_id bigint REFERENCES rfc.generations,
    publication_sequence bigint NOT NULL CHECK (publication_sequence >= 0),
    CHECK ((publication_sequence = 0) = (generation_id IS NULL))
);
INSERT INTO rfc.active_generation (publication_sequence) VALUES (0);

CREATE TABLE rfc.generation_documents (
    generation_id bigint NOT NULL REFERENCES rfc.generations,
    rfc_id rfc.rfc_number NOT NULL,
    document_version rfc.digest NOT NULL,
    metadata_identity rfc.digest NOT NULL,
    catalog_observation rfc.digest NOT NULL REFERENCES rfc.source_observations,
    txt_observation rfc.digest NOT NULL REFERENCES rfc.source_observations,
    title text NOT NULL,
    stream text,
    publication_date date,
    current_status text,
    PRIMARY KEY (generation_id, rfc_id),
    FOREIGN KEY (document_version, rfc_id) REFERENCES rfc.document_versions (identity, rfc_id)
);
CREATE INDEX generation_documents_version ON rfc.generation_documents (document_version);
CREATE INDEX generation_documents_catalog ON rfc.generation_documents (catalog_observation);
CREATE INDEX generation_documents_txt ON rfc.generation_documents (txt_observation);

CREATE TABLE rfc.sections (
    document_version rfc.digest NOT NULL,
    blob_length rfc.byte_offset NOT NULL,
    section_id text NOT NULL CHECK (octet_length(section_id) BETWEEN 1 AND 256),
    parent_section_id text CHECK (parent_section_id <> section_id),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    depth integer NOT NULL CHECK (depth >= 0),
    display_number text,
    heading_text text NOT NULL,
    start_byte rfc.byte_offset NOT NULL,
    end_byte rfc.byte_offset NOT NULL,
    heading_start_byte rfc.byte_offset NOT NULL,
    heading_end_byte rfc.byte_offset NOT NULL,
    parse_state rfc.coverage_state NOT NULL,
    PRIMARY KEY (document_version, section_id),
    UNIQUE (document_version, ordinal),
    FOREIGN KEY (document_version, blob_length) REFERENCES rfc.document_versions (identity, byte_length),
    FOREIGN KEY (document_version, parent_section_id) REFERENCES rfc.sections (document_version, section_id),
    CHECK (start_byte <= heading_start_byte AND heading_start_byte <= heading_end_byte
           AND heading_end_byte <= end_byte AND end_byte <= blob_length)
);
CREATE INDEX sections_parent ON rfc.sections (document_version, parent_section_id);

CREATE TABLE rfc.text_chunks (
    document_version rfc.digest NOT NULL,
    blob_length rfc.byte_offset NOT NULL,
    start_byte rfc.byte_offset NOT NULL,
    end_byte rfc.byte_offset NOT NULL,
    search_text text NOT NULL CHECK (octet_length(search_text) <= 16384),
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('pg_catalog.simple'::regconfig, search_text)) STORED,
    PRIMARY KEY (document_version, start_byte),
    FOREIGN KEY (document_version, blob_length) REFERENCES rfc.document_versions (identity, byte_length),
    CHECK (start_byte < end_byte AND end_byte <= blob_length AND end_byte - start_byte <= 16384)
);
CREATE INDEX text_chunks_fts ON rfc.text_chunks USING gin (search_vector);

CREATE TABLE rfc.notice_ranges (
    document_version rfc.digest NOT NULL,
    blob_length rfc.byte_offset NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    category text NOT NULL CHECK (category IN ('copyright', 'license', 'authorship', 'disclaimer', 'other')),
    start_byte rfc.byte_offset NOT NULL,
    end_byte rfc.byte_offset NOT NULL,
    extraction_state rfc.coverage_state NOT NULL,
    review_state text NOT NULL CHECK (review_state IN ('unreviewed', 'reviewed', 'restricted')),
    PRIMARY KEY (document_version, ordinal),
    FOREIGN KEY (document_version, blob_length) REFERENCES rfc.document_versions (identity, byte_length),
    CHECK (start_byte < end_byte AND end_byte <= blob_length)
);

CREATE TABLE rfc.relation_evidence (
    identity rfc.digest PRIMARY KEY,
    observation rfc.digest REFERENCES rfc.source_observations,
    document_version rfc.digest,
    blob_length rfc.byte_offset,
    start_byte rfc.byte_offset,
    end_byte rfc.byte_offset,
    method text NOT NULL CHECK (method IN ('catalog', 'rfcxml', 'txt', 'reviewed')),
    xml_locator text CHECK (octet_length(xml_locator) BETWEEN 1 AND 4096),
    FOREIGN KEY (document_version, blob_length) REFERENCES rfc.document_versions (identity, byte_length) MATCH FULL,
    CHECK (observation IS NOT NULL OR document_version IS NOT NULL),
    CHECK ((start_byte IS NULL AND end_byte IS NULL) OR
           (start_byte IS NOT NULL AND end_byte IS NOT NULL AND blob_length IS NOT NULL
            AND start_byte < end_byte AND end_byte <= blob_length)),
    CHECK (xml_locator IS NULL OR observation IS NOT NULL)
);
CREATE INDEX evidence_observation ON rfc.relation_evidence (observation);
CREATE INDEX evidence_version ON rfc.relation_evidence (document_version);

CREATE TABLE rfc.rfc_relations (
    generation_id bigint NOT NULL REFERENCES rfc.generations,
    source_rfc rfc.rfc_number NOT NULL REFERENCES rfc.rfc_identities,
    kind text NOT NULL CHECK (kind IN ('normative_reference', 'informative_reference', 'unclassified_reference', 'updates', 'obsoletes')),
    target_rfc rfc.rfc_number NOT NULL REFERENCES rfc.rfc_identities,
    PRIMARY KEY (generation_id, source_rfc, kind, target_rfc)
);
CREATE INDEX relations_inverse ON rfc.rfc_relations (generation_id, target_rfc, kind, source_rfc);

CREATE TABLE rfc.edge_evidence (
    generation_id bigint NOT NULL,
    source_rfc rfc.rfc_number NOT NULL,
    kind text NOT NULL,
    target_rfc rfc.rfc_number NOT NULL,
    evidence rfc.digest NOT NULL REFERENCES rfc.relation_evidence,
    PRIMARY KEY (generation_id, source_rfc, kind, target_rfc, evidence),
    FOREIGN KEY (generation_id, source_rfc, kind, target_rfc) REFERENCES rfc.rfc_relations
);
CREATE INDEX edge_evidence_identity ON rfc.edge_evidence (evidence);

CREATE TABLE rfc.relation_coverage (
    generation_id bigint NOT NULL REFERENCES rfc.generations,
    scope text NOT NULL CHECK (scope IN ('document', 'catalog')),
    rfc_id rfc.rfc_number REFERENCES rfc.rfc_identities,
    family text NOT NULL CHECK (family IN ('all_references', 'classified_references', 'updates', 'obsoletes')),
    state rfc.coverage_state NOT NULL,
    UNIQUE NULLS NOT DISTINCT (generation_id, scope, rfc_id, family),
    CHECK ((scope = 'document') = (rfc_id IS NOT NULL))
);

CREATE TABLE rfc.sync_jobs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY CHECK (id >= 0),
    generation_id bigint REFERENCES rfc.generations,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'running', 'succeeded', 'failed')),
    trigger_id uuid,
    outcome text CHECK (octet_length(outcome) <= 256),
    diagnostic text CHECK (octet_length(diagnostic) <= 4096),
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz
);

CREATE TABLE rfc.control_events (
    id uuid PRIMARY KEY,
    observation_digest rfc.digest,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL CHECK (status IN ('accepted', 'coalesced', 'completed', 'failed')),
    job_id bigint REFERENCES rfc.sync_jobs
);
ALTER TABLE rfc.sync_jobs ADD FOREIGN KEY (trigger_id) REFERENCES rfc.control_events;

CREATE TABLE rfc.notification_outbox (
    publication_sequence bigint PRIMARY KEY CHECK (publication_sequence > 0),
    generation_id bigint NOT NULL REFERENCES rfc.generations,
    event_id uuid NOT NULL UNIQUE,
    delivery_state text NOT NULL DEFAULT 'pending' CHECK (delivery_state IN ('pending', 'delivered', 'failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz,
    last_error text CHECK (octet_length(last_error) <= 4096),
    CHECK ((delivery_state = 'delivered') = (delivered_at IS NOT NULL))
);
CREATE INDEX outbox_due ON rfc.notification_outbox (next_attempt_at, publication_sequence) WHERE delivery_state = 'pending';

CREATE FUNCTION rfc.check_document_open() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    PERFORM FROM rfc.document_versions WHERE identity = NEW.document_version AND NOT sealed FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'document version is missing or sealed' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION rfc.check_document_seal() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF OLD.sealed OR NOT NEW.sealed THEN
        RAISE EXCEPTION 'document version can only be sealed once' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER seal_document BEFORE UPDATE ON rfc.document_versions
    FOR EACH ROW EXECUTE FUNCTION rfc.check_document_seal();
CREATE TRIGGER insert_section BEFORE INSERT ON rfc.sections
    FOR EACH ROW EXECUTE FUNCTION rfc.check_document_open();
CREATE TRIGGER insert_chunk BEFORE INSERT ON rfc.text_chunks
    FOR EACH ROW EXECUTE FUNCTION rfc.check_document_open();
CREATE TRIGGER insert_notice BEFORE INSERT ON rfc.notice_ranges
    FOR EACH ROW EXECUTE FUNCTION rfc.check_document_open();

CREATE FUNCTION rfc.check_generation_open() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    PERFORM FROM rfc.generations WHERE id = NEW.generation_id AND state = 'building' FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'generation is missing or frozen' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION rfc.check_membership() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM rfc.document_versions d
        JOIN rfc.source_observations o ON o.identity = NEW.txt_observation
        JOIN rfc.source_observations c ON c.identity = NEW.catalog_observation
        WHERE d.identity = NEW.document_version AND d.sealed
          AND o.raw_digest = d.txt_digest AND o.kind = 'txt' AND o.https_verification = 'verified'
          AND c.kind = 'catalog' AND c.https_verification = 'verified'
    ) THEN
        RAISE EXCEPTION 'membership requires a sealed version and matching verified TXT observation' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION rfc.check_evidence_version() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NEW.document_version IS NOT NULL AND NOT EXISTS (
        SELECT FROM rfc.document_versions WHERE identity = NEW.document_version AND sealed
    ) THEN
        RAISE EXCEPTION 'relation evidence requires a sealed document version' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER evidence_sealed BEFORE INSERT ON rfc.relation_evidence
    FOR EACH ROW EXECUTE FUNCTION rfc.check_evidence_version();

CREATE FUNCTION rfc.check_generation_freeze() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF OLD.state <> 'building' OR NEW.state NOT IN ('ready', 'failed') THEN
        RAISE EXCEPTION 'generation can only be finalized from building' USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'ready' AND EXISTS (
        SELECT FROM rfc.rfc_relations e
        WHERE e.generation_id = NEW.id AND NOT EXISTS (
            SELECT FROM rfc.edge_evidence p
            WHERE (p.generation_id, p.source_rfc, p.kind, p.target_rfc) =
                  (e.generation_id, e.source_rfc, e.kind, e.target_rfc)
        )
    ) THEN
        RAISE EXCEPTION 'every edge requires evidence before readiness' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER freeze_generation BEFORE UPDATE ON rfc.generations
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_freeze();
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.generation_documents
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();
CREATE TRIGGER membership_sealed BEFORE INSERT ON rfc.generation_documents
    FOR EACH ROW EXECUTE FUNCTION rfc.check_membership();
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.rfc_relations
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.edge_evidence
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.relation_coverage
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();

GRANT USAGE ON SCHEMA rfc TO rfc_serve, rfc_sync;
GRANT USAGE ON TYPE rfc.digest, rfc.rfc_number, rfc.byte_offset, rfc.coverage_state TO rfc_serve, rfc_sync;
GRANT SELECT ON rfc.schema_migrations, rfc.blobs, rfc.source_observations, rfc.rfc_identities,
    rfc.document_versions, rfc.generations, rfc.active_generation, rfc.generation_documents,
    rfc.sections, rfc.text_chunks, rfc.notice_ranges, rfc.relation_evidence, rfc.rfc_relations,
    rfc.edge_evidence, rfc.relation_coverage TO rfc_serve;
GRANT SELECT ON ALL TABLES IN SCHEMA rfc TO rfc_sync;
GRANT INSERT ON rfc.source_observations, rfc.rfc_identities, rfc.generation_documents,
    rfc.sections, rfc.notice_ranges, rfc.relation_evidence, rfc.rfc_relations, rfc.edge_evidence,
    rfc.relation_coverage, rfc.control_events TO rfc_sync;
GRANT INSERT (digest, byte_length) ON rfc.blobs TO rfc_sync;
GRANT INSERT (identity, rfc_id, txt_digest, byte_length, parser_version, index_version,
    encoding, indexing_state, notice_state, notice_policy, index_identity),
    UPDATE (sealed) ON rfc.document_versions TO rfc_sync;
GRANT INSERT (document_version, blob_length, start_byte, end_byte, search_text) ON rfc.text_chunks TO rfc_sync;
GRANT INSERT (parser_version, index_version), UPDATE (state, manifest_format, manifest_bytes, manifest_digest)
    ON rfc.generations TO rfc_sync;
GRANT INSERT (generation_id, state, trigger_id, outcome, diagnostic, started_at, finished_at),
    UPDATE (generation_id, state, trigger_id, outcome, diagnostic, started_at, finished_at) ON rfc.sync_jobs TO rfc_sync;
GRANT UPDATE (status, job_id) ON rfc.control_events TO rfc_sync;
GRANT USAGE ON SEQUENCE rfc.generations_id_seq, rfc.sync_jobs_id_seq TO rfc_sync;
