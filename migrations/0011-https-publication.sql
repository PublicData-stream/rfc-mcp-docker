CREATE TABLE rfc.generation_audit (
    generation_id bigint PRIMARY KEY REFERENCES rfc.generation_ingestion ON DELETE CASCADE,
    selection rfc.digest NOT NULL,
    inventory rfc.digest NOT NULL,
    policy text NOT NULL CHECK (policy = 'https-first-rsync-audit-v1'),
    expected_rfc_ids integer[] NOT NULL CHECK (cardinality(expected_rfc_ids) BETWEEN 1 AND 20000),
    expected_sequence bigint NOT NULL CHECK (expected_sequence >= 0),
    started_us bigint NOT NULL CHECK (started_us > 0),
    expires_us bigint NOT NULL CHECK (expires_us > started_us AND expires_us - started_us <= 604800000000),
    UNIQUE (generation_id, selection)
);

CREATE TABLE rfc.generation_source_evidence (
    generation_id bigint NOT NULL,
    selection rfc.digest NOT NULL,
    rfc_id rfc.rfc_number NOT NULL,
    txt_observation rfc.digest NOT NULL REFERENCES rfc.source_observations,
    document_version rfc.digest NOT NULL REFERENCES rfc.document_versions,
    txt_digest rfc.digest NOT NULL REFERENCES rfc.blobs,
    byte_length rfc.byte_offset NOT NULL CHECK (byte_length BETWEEN 1 AND 4194304),
    method text NOT NULL CHECK (method IN ('rsync_checksum_match', 'dual_source_byte_match')),
    receipt rfc.digest NOT NULL,
    rsync_url text NOT NULL CHECK (rsync_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || rfc_id || '.txt'),
    started_us bigint NOT NULL,
    observed_us bigint NOT NULL CHECK (observed_us >= started_us),
    checksum_choice text,
    algorithm text CHECK (algorithm IN ('xxh128', 'xxh3', 'xxh64', 'md5', 'md4', 'sha1')),
    rsync_observation rfc.digest REFERENCES rfc.source_observations,
    rfcxml_outcome text NOT NULL CHECK (rfcxml_outcome IN ('captured', 'not_found', 'gone',
        'malformed', 'retries_exhausted', 'fetch_failed')),
    rfcxml_observation rfc.digest REFERENCES rfc.source_observations,
    rfcxml_audit text NOT NULL DEFAULT 'audit_not_applicable' CHECK (rfcxml_audit = 'audit_not_applicable'),
    PRIMARY KEY (generation_id, rfc_id),
    FOREIGN KEY (generation_id, selection) REFERENCES rfc.generation_audit (generation_id, selection) ON DELETE CASCADE,
    CHECK ((rfcxml_outcome IN ('captured', 'malformed')) = (rfcxml_observation IS NOT NULL)),
    CHECK (CASE method WHEN 'rsync_checksum_match' THEN checksum_choice IS NOT DISTINCT FROM 'auto'
        AND rsync_observation IS NULL ELSE checksum_choice IS NULL AND algorithm IS NULL
        AND rsync_observation IS NOT NULL END)
);
CREATE INDEX generation_source_rsync_observation ON rfc.generation_source_evidence (rsync_observation)
    WHERE rsync_observation IS NOT NULL;
CREATE INDEX generation_source_txt_observation ON rfc.generation_source_evidence (txt_observation);
CREATE INDEX generation_source_rfcxml_observation ON rfc.generation_source_evidence (rfcxml_observation)
    WHERE rfcxml_observation IS NOT NULL;
CREATE INDEX generation_source_document_version ON rfc.generation_source_evidence (document_version);
CREATE INDEX generation_source_txt_digest ON rfc.generation_source_evidence (txt_digest);

CREATE TRIGGER generation_open BEFORE INSERT ON rfc.generation_audit
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.generation_source_evidence
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();

CREATE OR REPLACE FUNCTION rfc.reject_capture_only_membership() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF EXISTS (SELECT FROM rfc.source_observations WHERE identity = NEW.txt_observation AND capture_only)
        AND NOT EXISTS (SELECT FROM rfc.generation_source_evidence e WHERE
            (e.generation_id, e.rfc_id, e.txt_observation, e.document_version) =
            (NEW.generation_id, NEW.rfc_id, NEW.txt_observation, NEW.document_version)) THEN
        RAISE EXCEPTION 'HTTPS TXT capture alone does not satisfy the audit gate' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION rfc.check_generation_audit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE
    audit rfc.generation_audit%ROWTYPE;
    ingestion rfc.generation_ingestion%ROWTYPE;
BEGIN
    PERFORM rfc.require_coordinator();
    SELECT * INTO audit FROM rfc.generation_audit WHERE generation_id = NEW.id;
    SELECT * INTO ingestion FROM rfc.generation_ingestion WHERE generation_id = NEW.id;
    IF audit.generation_id IS NULL OR NEW.manifest_format IS DISTINCT FROM 3 THEN
        RAISE EXCEPTION 'generation requires HTTPS-first audit evidence' USING ERRCODE = '23514';
    END IF;
    IF audit.expires_us <= extract(epoch FROM clock_timestamp()) * 1000000 THEN
        RAISE EXCEPTION 'candidate audit cycle expired; reconcile again' USING ERRCODE = '55000';
    END IF;
    IF NOT EXISTS (SELECT FROM rfc.active_generation WHERE singleton
        AND publication_sequence = audit.expected_sequence) THEN
        RAISE EXCEPTION 'candidate publication sequence changed; reconcile again' USING ERRCODE = '40001';
    END IF;
    IF OLD.state = 'ready' AND ingestion.scope <> 'full' THEN
        RAISE EXCEPTION 'sample candidate is not eligible for promotion' USING ERRCODE = '55000';
    END IF;
    IF (SELECT array_agg(d.rfc_id::integer ORDER BY d.rfc_id) FROM rfc.generation_documents d
        WHERE d.generation_id = NEW.id) IS DISTINCT FROM audit.expected_rfc_ids
        OR (SELECT array_agg(e.rfc_id::integer ORDER BY e.rfc_id) FROM rfc.generation_source_evidence e
        WHERE e.generation_id = NEW.id) IS DISTINCT FROM audit.expected_rfc_ids THEN
        RAISE EXCEPTION 'candidate membership and audit must equal the sealed selection' USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (
        SELECT FROM rfc.source_observations opening JOIN rfc.source_observations closing USING (raw_digest)
        JOIN rfc.blobs b ON b.digest = opening.raw_digest
        WHERE opening.identity = ingestion.catalog_before AND closing.identity = ingestion.catalog_after
          AND opening.source_url = 'https://www.rfc-editor.org/rfc-index.xml'
          AND closing.source_url = opening.source_url AND opening.observed_at <= closing.observed_at
          AND b.integrity_state = 'verified'
    ) OR (SELECT count(*) FROM rfc.relation_coverage c WHERE c.generation_id = NEW.id
        AND c.scope = 'catalog' AND c.family IN ('all_references','classified_references','updates','obsoletes')) <> 4 THEN
        RAISE EXCEPTION 'candidate catalog fence or coverage is incomplete' USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT FROM rfc.generation_documents d
        JOIN rfc.document_versions v ON v.identity = d.document_version
        JOIN rfc.source_observations h ON h.identity = d.txt_observation
        JOIN rfc.blobs b ON b.digest = v.txt_digest
        LEFT JOIN rfc.generation_source_evidence e ON (e.generation_id, e.rfc_id) = (d.generation_id, d.rfc_id)
        LEFT JOIN rfc.source_observations peer ON peer.identity = e.rsync_observation
        LEFT JOIN rfc.source_observations x ON x.identity = e.rfcxml_observation
        LEFT JOIN rfc.blobs xb ON xb.digest = x.raw_digest
        WHERE d.generation_id = NEW.id AND (
            (e.txt_observation, e.document_version, e.txt_digest, e.byte_length, e.rfcxml_observation)
               IS DISTINCT FROM (d.txt_observation, v.identity, v.txt_digest, b.byte_length, d.rfcxml_observation)
            OR h.kind <> 'txt' OR h.https_verification <> 'verified' OR h.raw_digest <> v.txt_digest
            OR h.source_url <> 'https://www.rfc-editor.org/rfc/rfc' || d.rfc_id || '.txt'
            OR b.integrity_state <> 'verified' OR NOT v.sealed OR v.section_state IS NULL
            OR (v.parser_version, v.index_version) <> (NEW.parser_version, NEW.index_version)
            OR d.catalog_observation <> ingestion.catalog_before
            OR e.started_us < audit.started_us OR e.observed_us >= audit.expires_us
            OR e.observed_us > extract(epoch FROM clock_timestamp()) * 1000000
            OR extract(epoch FROM h.fetched_at) * 1000000 > e.observed_us
            OR (e.method = 'dual_source_byte_match' AND (
                (peer.kind, peer.source_url, peer.raw_digest, peer.https_verification)
                    IS DISTINCT FROM ('txt', e.rsync_url, v.txt_digest, 'unverified')
                OR peer.fetched_at IS NULL
                OR extract(epoch FROM peer.observed_at) * 1000000 <> e.observed_us
                OR extract(epoch FROM h.observed_at) * 1000000 < audit.started_us
                OR h.fetched_at > peer.observed_at))
            OR (x.identity IS NOT NULL AND xb.integrity_state <> 'verified')
            OR (SELECT count(*) FROM rfc.relation_coverage c WHERE c.generation_id = d.generation_id
                AND c.rfc_id = d.rfc_id AND c.scope = 'document'
                AND c.family IN ('all_references','classified_references','updates','obsoletes')) <> 4
        )
    ) THEN
        RAISE EXCEPTION 'candidate contains ineligible source evidence or coverage' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER require_https_audit BEFORE UPDATE OF state ON rfc.generations
    FOR EACH ROW WHEN ((OLD.state = 'building' AND NEW.state = 'ready')
        OR (OLD.state = 'ready' AND NEW.state = 'published'))
    EXECUTE FUNCTION rfc.check_generation_audit();

CREATE OR REPLACE VIEW rfc.reachable_source_observations AS
    SELECT catalog_observation AS identity FROM rfc.generation_documents
    UNION SELECT txt_observation FROM rfc.generation_documents
    UNION SELECT rfcxml_observation FROM rfc.generation_documents WHERE rfcxml_observation IS NOT NULL
    UNION SELECT catalog_before FROM rfc.generation_ingestion
    UNION SELECT catalog_after FROM rfc.generation_ingestion
    UNION SELECT observation FROM rfc.relation_evidence WHERE observation IS NOT NULL
    UNION SELECT observation_digest FROM rfc.control_events
    UNION SELECT observation FROM rfc.ingestion_checkpoints
    UNION SELECT txt_observation FROM rfc.generation_source_evidence
    UNION SELECT rfcxml_observation FROM rfc.generation_source_evidence WHERE rfcxml_observation IS NOT NULL
    UNION SELECT rsync_observation FROM rfc.generation_source_evidence WHERE rsync_observation IS NOT NULL;

CREATE OR REPLACE VIEW rfc.unused_document_versions AS
    SELECT d.identity FROM rfc.document_versions d
    WHERE NOT EXISTS (SELECT FROM rfc.generation_documents m WHERE m.document_version = d.identity)
      AND NOT EXISTS (SELECT FROM rfc.relation_evidence e WHERE e.document_version = d.identity)
      AND NOT EXISTS (SELECT FROM rfc.ingestion_checkpoints c WHERE c.document_version = d.identity)
      AND NOT EXISTS (SELECT FROM rfc.generation_source_evidence e WHERE e.document_version = d.identity);

GRANT SELECT ON rfc.generation_audit, rfc.generation_source_evidence TO rfc_sync, rfc_serve;
GRANT INSERT ON rfc.generation_audit, rfc.generation_source_evidence TO rfc_sync;
