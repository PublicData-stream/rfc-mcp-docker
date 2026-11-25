-- New HTTPS TXT observations record transport proof only. Historical rows
-- keep their existing method, identity, and manifest representation.
ALTER TABLE rfc.source_observations
    ADD COLUMN capture_only boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT official_txt_capture CHECK (NOT capture_only OR (
        kind = 'txt' AND https_verification = 'verified' AND comparison_rsync IS NULL
        AND source_url ~ '^https://www[.]rfc-editor[.]org/rfc/rfc[1-9][0-9]*[.]txt$'
        AND fetched_at IS NOT NULL AND observed_at <= fetched_at
    )),
    DROP COLUMN verification_method;

ALTER TABLE rfc.source_observations ADD COLUMN verification_method text GENERATED ALWAYS AS (
    CASE
        WHEN comparison_rsync IS NOT NULL THEN 'dual_source'
        WHEN source_url LIKE 'rsync://%' THEN 'rsync_integrity'
        WHEN source_url LIKE 'https://%' AND https_verification = 'verified' THEN
            CASE WHEN kind = 'txt' AND NOT capture_only THEN 'legacy_https_verified' ELSE 'https_capture' END
        ELSE 'unclassified'
    END
) STORED;

CREATE OR REPLACE FUNCTION rfc.check_txt_comparison() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NEW.comparison_rsync IS NULL THEN
        IF NOT NEW.capture_only AND current_user = 'rfc_sync' AND NEW.kind = 'txt' AND NEW.https_verification = 'verified' THEN
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

CREATE FUNCTION rfc.reject_capture_only_membership() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF EXISTS (SELECT FROM rfc.source_observations WHERE identity = NEW.txt_observation AND capture_only) THEN
        RAISE EXCEPTION 'HTTPS TXT capture alone does not satisfy the audit gate' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER guard_capture_only_membership BEFORE INSERT OR UPDATE OF txt_observation ON rfc.generation_documents
    FOR EACH ROW EXECUTE FUNCTION rfc.reject_capture_only_membership();

ALTER TABLE rfc.ingestion_checkpoints
    ADD COLUMN evidence_state text NOT NULL DEFAULT 'legacy',
    ADD COLUMN document_version rfc.digest REFERENCES rfc.document_versions,
    ADD CONSTRAINT checkpoint_evidence_state CHECK (
        (evidence_state = 'legacy' AND document_version IS NULL)
        OR (kind = 'txt' AND evidence_state = 'https' AND document_version IS NOT NULL)
        OR (kind = 'rfcxml' AND evidence_state IN ('https', 'malformed') AND document_version IS NULL)
    );
CREATE INDEX ingestion_checkpoints_document_version ON rfc.ingestion_checkpoints (document_version)
    WHERE document_version IS NOT NULL;

CREATE OR REPLACE VIEW rfc.unused_document_versions AS
    SELECT d.identity FROM rfc.document_versions d
    WHERE NOT EXISTS (SELECT FROM rfc.generation_documents m WHERE m.document_version = d.identity)
      AND NOT EXISTS (SELECT FROM rfc.relation_evidence e WHERE e.document_version = d.identity)
      AND NOT EXISTS (SELECT FROM rfc.ingestion_checkpoints c WHERE c.document_version = d.identity);

CREATE FUNCTION rfc.store_source_checkpoint(
    target_scope text, current_context bytea, target_rfc integer, target_kind text, proof bytea,
    target_state text, target_version bytea
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE now_at timestamptz := clock_timestamp();
DECLARE usage rfc.ingestion_checkpoint_usage%ROWTYPE;
BEGIN
    PERFORM rfc.require_coordinator();
    PERFORM set_config('synchronous_commit', 'on', true);
    INSERT INTO rfc.ingestion_checkpoints (scope, context, rfc_id, kind, observation, byte_length, recorded_at, expires_at,
        evidence_state, document_version)
        SELECT target_scope, current_context, target_rfc, target_kind, o.identity, b.byte_length, now_at,
            CASE WHEN target_kind = 'rfcxml' THEN o.fetched_at + interval '24 hours' ELSE now_at + interval '168 hours' END,
            target_state, target_version
        FROM rfc.source_observations o JOIN rfc.blobs b ON b.digest = o.raw_digest
        WHERE o.identity = proof AND o.kind = target_kind AND b.integrity_state = 'verified'
          AND b.byte_length BETWEEN 1 AND CASE WHEN target_kind = 'txt' THEN 4194304 ELSE 8388608 END
          AND CASE WHEN target_kind = 'txt' AND target_state = 'https' THEN
              o.verification_method IN ('https_capture', 'legacy_https_verified', 'dual_source')
              AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || target_rfc || '.txt'
              AND EXISTS (
                  SELECT FROM rfc.document_versions d WHERE d.identity = target_version
                    AND d.sealed AND d.rfc_id = target_rfc AND d.txt_digest = o.raw_digest
              )
          WHEN target_kind = 'txt' AND target_state = 'legacy' THEN
              target_version IS NULL AND ((o.verification_method = 'dual_source'
                  AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || target_rfc || '.txt')
              OR (o.verification_method = 'rsync_integrity'
                  AND o.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || target_rfc || '.txt'))
          WHEN target_kind = 'rfcxml' AND target_state IN ('https', 'malformed', 'legacy') THEN
              target_version IS NULL AND o.verification_method = 'https_capture'
              AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || target_rfc || '.xml'
          ELSE false END
        ON CONFLICT (scope, rfc_id, kind) DO UPDATE
            SET context = EXCLUDED.context, observation = EXCLUDED.observation,
                byte_length = EXCLUDED.byte_length, evidence_state = EXCLUDED.evidence_state,
                document_version = EXCLUDED.document_version,
                recorded_at = CASE WHEN EXCLUDED.evidence_state <> 'legacy'
                    AND ingestion_checkpoints.observation = EXCLUDED.observation
                    THEN ingestion_checkpoints.recorded_at ELSE EXCLUDED.recorded_at END,
                expires_at = CASE WHEN EXCLUDED.evidence_state <> 'legacy'
                    AND ingestion_checkpoints.observation = EXCLUDED.observation
                    THEN ingestion_checkpoints.expires_at ELSE EXCLUDED.expires_at END
            WHERE (ingestion_checkpoints.context, ingestion_checkpoints.observation,
                   ingestion_checkpoints.evidence_state, ingestion_checkpoints.document_version)
                IS DISTINCT FROM (EXCLUDED.context, EXCLUDED.observation,
                   EXCLUDED.evidence_state, EXCLUDED.document_version);
    IF NOT FOUND AND NOT EXISTS (
        SELECT FROM rfc.ingestion_checkpoints c
        WHERE (c.scope, c.context, c.rfc_id, c.kind, c.observation) =
              (target_scope, current_context, target_rfc, target_kind, proof)
          AND c.evidence_state = target_state
          AND c.document_version IS NOT DISTINCT FROM target_version
    ) THEN
        RAISE EXCEPTION 'checkpoint requires verified source evidence for its RFC' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT usage FROM rfc.ingestion_checkpoint_usage;
    IF usage.entries > 40000 OR usage.bytes > 4294967296 THEN
        PERFORM rfc.prune_ingestion_checkpoints(NULL, NULL);
        SELECT * INTO STRICT usage FROM rfc.ingestion_checkpoint_usage;
        IF usage.entries > 40000 OR usage.bytes > 4294967296 THEN
            WITH oldest AS (
                SELECT scope, rfc_id, kind, row_number() OVER age AS entry,
                       sum(byte_length) OVER age - byte_length AS bytes_before
                FROM rfc.ingestion_checkpoints
                WINDOW age AS (ORDER BY recorded_at, scope, rfc_id, kind ROWS UNBOUNDED PRECEDING)
            )
            DELETE FROM rfc.ingestion_checkpoints c USING oldest o
            WHERE (c.scope, c.rfc_id, c.kind) = (o.scope, o.rfc_id, o.kind)
              AND (o.entry <= usage.entries - 40000 OR o.bytes_before < usage.bytes - 4294967296);
        END IF;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION rfc.store_ingestion_checkpoint(
    target_scope text, current_context bytea, target_rfc integer, target_kind text, proof bytea
) RETURNS void LANGUAGE sql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
    SELECT rfc.store_source_checkpoint(target_scope, current_context, target_rfc, target_kind, proof, 'legacy', NULL)
$$;

GRANT EXECUTE ON FUNCTION rfc.store_source_checkpoint(text, bytea, integer, text, bytea, text, bytea) TO rfc_sync;
