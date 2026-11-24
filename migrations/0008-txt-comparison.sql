ALTER TABLE rfc.source_observations
    ADD COLUMN comparison_rsync rfc.digest REFERENCES rfc.source_observations,
    ADD COLUMN verification_method text GENERATED ALWAYS AS (
        CASE
            WHEN comparison_rsync IS NOT NULL THEN 'dual_source'
            WHEN source_url LIKE 'rsync://%' THEN 'rsync_integrity'
            WHEN source_url LIKE 'https://%' AND https_verification = 'verified' THEN
                CASE WHEN kind = 'txt' THEN 'legacy_https_verified' ELSE 'https_capture' END
            ELSE 'unclassified'
        END
    ) STORED;

CREATE INDEX source_observations_comparison ON rfc.source_observations (comparison_rsync)
    WHERE comparison_rsync IS NOT NULL;

CREATE FUNCTION rfc.check_txt_comparison() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NEW.comparison_rsync IS NULL THEN
        IF current_user = 'rfc_sync' AND NEW.kind = 'txt' AND NEW.https_verification = 'verified' THEN
            RAISE EXCEPTION 'new verified TXT requires a separate rsync observation' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.kind <> 'txt' OR NEW.https_verification <> 'verified'
        OR NEW.source_url !~ '^https://www[.]rfc-editor[.]org/rfc/rfc[1-9][0-9]*[.]txt$'
        OR NOT EXISTS (
            SELECT FROM rfc.source_observations peer
            WHERE peer.identity = NEW.comparison_rsync AND peer.kind = 'txt'
              AND peer.https_verification = 'unverified' AND peer.comparison_rsync IS NULL
              AND peer.raw_digest = NEW.raw_digest
              AND peer.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/' ||
                  split_part(NEW.source_url, '/', 5)
        ) THEN
        RAISE EXCEPTION 'TXT comparison requires matching official rsync evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER guard_txt_comparison BEFORE INSERT ON rfc.source_observations
    FOR EACH ROW EXECUTE FUNCTION rfc.check_txt_comparison();

CREATE VIEW rfc.reachable_source_observations AS
    SELECT catalog_observation AS identity FROM rfc.generation_documents
    UNION SELECT txt_observation FROM rfc.generation_documents
    UNION SELECT rfcxml_observation FROM rfc.generation_documents WHERE rfcxml_observation IS NOT NULL
    UNION SELECT catalog_before FROM rfc.generation_ingestion
    UNION SELECT catalog_after FROM rfc.generation_ingestion
    UNION SELECT observation FROM rfc.relation_evidence WHERE observation IS NOT NULL
    UNION SELECT observation_digest FROM rfc.control_events;

CREATE OR REPLACE FUNCTION rfc.mark_unreachable_blobs() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    PERFORM rfc.require_coordinator();
    DELETE FROM rfc.relation_evidence e WHERE NOT EXISTS (
        SELECT FROM rfc.edge_evidence p WHERE p.evidence = e.identity);
    DELETE FROM rfc.document_lexemes WHERE document_version IN (SELECT identity FROM rfc.unused_document_versions);
    DELETE FROM rfc.sections WHERE document_version IN (SELECT identity FROM rfc.unused_document_versions);
    DELETE FROM rfc.text_chunks WHERE document_version IN (SELECT identity FROM rfc.unused_document_versions);
    DELETE FROM rfc.notice_ranges WHERE document_version IN (SELECT identity FROM rfc.unused_document_versions);
    DELETE FROM rfc.document_versions WHERE identity IN (SELECT identity FROM rfc.unused_document_versions);
    WITH roots AS MATERIALIZED (SELECT identity FROM rfc.reachable_source_observations),
    reachable AS (
        SELECT identity FROM roots
        UNION SELECT o.comparison_rsync FROM roots r JOIN rfc.source_observations o USING (identity)
            WHERE o.comparison_rsync IS NOT NULL
    )
    DELETE FROM rfc.source_observations o WHERE NOT EXISTS (
        SELECT FROM reachable r WHERE r.identity = o.identity);
    WITH removed AS (
        DELETE FROM rfc.blobs b
        WHERE b.integrity_state = 'verified'
          AND NOT EXISTS (SELECT FROM rfc.document_versions d WHERE d.txt_digest = b.digest)
          AND NOT EXISTS (SELECT FROM rfc.source_observations o WHERE o.raw_digest = b.digest)
          AND NOT EXISTS (SELECT FROM rfc.control_events c WHERE c.observation_digest = b.digest)
        RETURNING b.digest, b.byte_length
    ) INSERT INTO rfc.blob_deletion_queue (digest, byte_length) SELECT digest, byte_length FROM removed;
    UPDATE rfc.generations SET state = 'deleted' WHERE state = 'deleting';
END
$$;
