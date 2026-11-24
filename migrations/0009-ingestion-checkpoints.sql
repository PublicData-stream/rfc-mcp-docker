CREATE TABLE rfc.ingestion_checkpoints (
    scope text NOT NULL CHECK (scope IN ('full', 'sample')),
    context rfc.digest NOT NULL,
    rfc_id rfc.rfc_number NOT NULL,
    kind text NOT NULL CHECK (kind IN ('txt', 'rfcxml')),
    observation rfc.digest NOT NULL REFERENCES rfc.source_observations,
    byte_length rfc.byte_offset NOT NULL
        CHECK (byte_length BETWEEN 1 AND CASE WHEN kind = 'txt' THEN 4194304 ELSE 8388608 END),
    recorded_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (scope, rfc_id, kind)
);
CREATE INDEX ingestion_checkpoints_age ON rfc.ingestion_checkpoints (recorded_at, scope, rfc_id, kind);
CREATE INDEX ingestion_checkpoints_expiry ON rfc.ingestion_checkpoints (expires_at);
CREATE INDEX ingestion_checkpoints_observation ON rfc.ingestion_checkpoints (observation);

CREATE TABLE rfc.ingestion_checkpoint_usage (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    entries integer NOT NULL CHECK (entries >= 0),
    bytes bigint NOT NULL CHECK (bytes >= 0)
);
INSERT INTO rfc.ingestion_checkpoint_usage (entries, bytes) VALUES (0, 0);

CREATE FUNCTION rfc.count_ingestion_checkpoint() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
DECLARE added_count integer := 0;
DECLARE removed_count integer := 0;
DECLARE added_bytes bigint := 0;
DECLARE removed_bytes bigint := 0;
BEGIN
    IF TG_OP <> 'DELETE' THEN
        SELECT count(*), coalesce(sum(byte_length), 0) INTO added_count, added_bytes FROM added;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        SELECT count(*), coalesce(sum(byte_length), 0) INTO removed_count, removed_bytes FROM removed;
    END IF;
    IF (added_count, added_bytes) <> (removed_count, removed_bytes) THEN
        UPDATE rfc.ingestion_checkpoint_usage
        SET entries = entries + added_count - removed_count, bytes = bytes + added_bytes - removed_bytes;
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER count_inserted_ingestion_checkpoints AFTER INSERT ON rfc.ingestion_checkpoints
    REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION rfc.count_ingestion_checkpoint();
CREATE TRIGGER count_updated_ingestion_checkpoints AFTER UPDATE ON rfc.ingestion_checkpoints
    REFERENCING OLD TABLE AS removed NEW TABLE AS added FOR EACH STATEMENT
    EXECUTE FUNCTION rfc.count_ingestion_checkpoint();
CREATE TRIGGER count_removed_ingestion_checkpoints AFTER DELETE ON rfc.ingestion_checkpoints
    REFERENCING OLD TABLE AS removed FOR EACH STATEMENT EXECUTE FUNCTION rfc.count_ingestion_checkpoint();

CREATE FUNCTION rfc.prune_ingestion_checkpoints(target_scope text, current_context bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    PERFORM rfc.require_coordinator();
    DELETE FROM rfc.ingestion_checkpoints c
    WHERE c.expires_at <= statement_timestamp()
       OR (c.scope = target_scope AND c.context <> current_context);
END
$$;

-- Null RFC and kind clear the complete context at readiness or on catalog drift.
CREATE FUNCTION rfc.remove_ingestion_checkpoints(
    target_scope text, current_context bytea, target_rfc integer, target_kind text
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    PERFORM rfc.require_coordinator();
    PERFORM set_config('synchronous_commit', 'on', true);
    DELETE FROM rfc.ingestion_checkpoints c
    WHERE c.scope = target_scope AND c.context = current_context
      AND (target_rfc IS NULL OR c.rfc_id = target_rfc)
      AND (target_kind IS NULL OR c.kind = target_kind);
END
$$;

CREATE FUNCTION rfc.store_ingestion_checkpoint(
    target_scope text, current_context bytea, target_rfc integer, target_kind text, proof bytea
) RETURNS void LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE now_at timestamptz := clock_timestamp();
DECLARE usage rfc.ingestion_checkpoint_usage%ROWTYPE;
BEGIN
    PERFORM rfc.require_coordinator();
    PERFORM set_config('synchronous_commit', 'on', true);
    INSERT INTO rfc.ingestion_checkpoints (scope, context, rfc_id, kind, observation, byte_length, recorded_at, expires_at)
        SELECT target_scope, current_context, target_rfc, target_kind, o.identity, b.byte_length, now_at,
            CASE WHEN target_kind = 'rfcxml' THEN o.fetched_at + interval '24 hours' ELSE now_at + interval '168 hours' END
        FROM rfc.source_observations o JOIN rfc.blobs b ON b.digest = o.raw_digest
        WHERE o.identity = proof AND o.kind = target_kind AND b.integrity_state = 'verified'
          AND CASE WHEN target_kind = 'txt' THEN
              (o.verification_method = 'dual_source'
                  AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || target_rfc || '.txt')
              OR (o.verification_method = 'rsync_integrity'
                  AND o.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || target_rfc || '.txt')
          ELSE o.verification_method = 'https_capture'
              AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || target_rfc || '.xml' END
        ON CONFLICT (scope, rfc_id, kind) DO UPDATE
            SET context = EXCLUDED.context, observation = EXCLUDED.observation,
                byte_length = EXCLUDED.byte_length, recorded_at = EXCLUDED.recorded_at, expires_at = EXCLUDED.expires_at
            WHERE (ingestion_checkpoints.context, ingestion_checkpoints.observation)
                IS DISTINCT FROM (EXCLUDED.context, EXCLUDED.observation);
    IF NOT FOUND AND NOT EXISTS (
        SELECT FROM rfc.ingestion_checkpoints c
        WHERE (c.scope, c.context, c.rfc_id, c.kind, c.observation) =
              (target_scope, current_context, target_rfc, target_kind, proof)
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

CREATE OR REPLACE VIEW rfc.reachable_source_observations AS
    SELECT catalog_observation AS identity FROM rfc.generation_documents
    UNION SELECT txt_observation FROM rfc.generation_documents
    UNION SELECT rfcxml_observation FROM rfc.generation_documents WHERE rfcxml_observation IS NOT NULL
    UNION SELECT catalog_before FROM rfc.generation_ingestion
    UNION SELECT catalog_after FROM rfc.generation_ingestion
    UNION SELECT observation FROM rfc.relation_evidence WHERE observation IS NOT NULL
    UNION SELECT observation_digest FROM rfc.control_events
    UNION SELECT observation FROM rfc.ingestion_checkpoints;

GRANT SELECT ON rfc.ingestion_checkpoints TO rfc_sync;
GRANT EXECUTE ON FUNCTION rfc.prune_ingestion_checkpoints(text, bytea),
    rfc.remove_ingestion_checkpoints(text, bytea, integer, text),
    rfc.store_ingestion_checkpoint(text, bytea, integer, text, bytea) TO rfc_sync;
