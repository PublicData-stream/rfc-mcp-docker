ALTER TABLE rfc.generations ADD COLUMN last_publication_sequence bigint UNIQUE
    CHECK (last_publication_sequence > 0);
ALTER TABLE rfc.generations DROP CONSTRAINT generations_check;
ALTER TABLE rfc.generations ADD CONSTRAINT generation_manifest_lifecycle CHECK (
    (manifest_format IS NULL AND manifest_bytes IS NULL AND manifest_digest IS NULL
        AND state IN ('building', 'failed', 'deleting', 'deleted')) OR
    (manifest_format IS NOT NULL AND manifest_format > 0 AND manifest_bytes IS NOT NULL
        AND octet_length(manifest_bytes) > 0 AND manifest_digest IS NOT NULL AND state <> 'building')
);

CREATE TABLE rfc.blob_deletion_queue (
    digest rfc.digest PRIMARY KEY,
    byte_length rfc.byte_offset,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION rfc.check_blob_not_queued() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF EXISTS (SELECT FROM rfc.blob_deletion_queue WHERE digest = NEW.digest) THEN
        RAISE EXCEPTION 'blob deletion is pending' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER blob_not_queued BEFORE INSERT ON rfc.blobs
    FOR EACH ROW EXECUTE FUNCTION rfc.check_blob_not_queued();

-- A privileged lifecycle operation must run on the same session as the
-- sync coordinator. bigint key -1 is represented by two all-ones OIDs.
CREATE FUNCTION rfc.holds_exclusive_lock(key bigint) RETURNS boolean LANGUAGE sql
SET search_path = pg_catalog, rfc, pg_temp AS $$
    SELECT EXISTS (SELECT FROM pg_locks WHERE pid = pg_backend_pid()
        AND locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
        AND classid = ((key >> 32) & 4294967295)::oid AND objid = (key & 4294967295)::oid
        AND objsubid = 1 AND mode = 'ExclusiveLock' AND granted)
$$;

CREATE FUNCTION rfc.require_coordinator() RETURNS void LANGUAGE plpgsql
SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    IF NOT rfc.holds_exclusive_lock(-1) THEN
        RAISE EXCEPTION 'corpus coordinator must be held' USING ERRCODE = '55000';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION rfc.check_generation_freeze() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF current_user = 'rfc_owner' AND OLD.state <> 'building' THEN
        IF ROW(NEW.manifest_format, NEW.manifest_bytes, NEW.manifest_digest)
               IS DISTINCT FROM ROW(OLD.manifest_format, OLD.manifest_bytes, OLD.manifest_digest)
            OR NOT ((OLD.state IN ('ready', 'retained') AND NEW.state = 'published')
                    OR (OLD.state = 'published' AND NEW.state = 'retained')
                    OR (OLD.state IN ('retained', 'ready', 'failed') AND NEW.state = 'deleting')
                    OR (OLD.state = 'deleting' AND NEW.state = 'deleted')) THEN
            RAISE EXCEPTION 'invalid generation transition' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.state <> 'building' OR NEW.state NOT IN ('ready', 'failed') THEN
        RAISE EXCEPTION 'generation can only be finalized from building' USING ERRCODE = '23514';
    END IF;
    IF NEW.state = 'ready' AND EXISTS (
        SELECT FROM rfc.rfc_relations e WHERE e.generation_id = NEW.id AND NOT EXISTS (
            SELECT FROM rfc.edge_evidence p
            WHERE (p.generation_id, p.source_rfc, p.kind, p.target_rfc) =
                  (e.generation_id, e.source_rfc, e.kind, e.target_rfc))) THEN
        RAISE EXCEPTION 'every edge requires evidence before readiness' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

-- All body and provenance roots of one immutable generation.
CREATE FUNCTION rfc.generation_blobs(generation bigint) RETURNS SETOF rfc.digest
LANGUAGE sql STABLE SET search_path = pg_catalog, rfc AS $$
    WITH members AS (
        SELECT * FROM rfc.generation_documents WHERE generation_id = generation
    ), evidence AS (
        SELECT e.* FROM rfc.edge_evidence p JOIN rfc.relation_evidence e ON e.identity = p.evidence
        WHERE p.generation_id = generation
    ), observations AS (
        SELECT catalog_observation AS id FROM members UNION SELECT txt_observation FROM members
        UNION SELECT rfcxml_observation FROM members UNION SELECT observation FROM evidence
        UNION SELECT catalog_before FROM rfc.generation_ingestion WHERE generation_id = generation
        UNION SELECT catalog_after FROM rfc.generation_ingestion WHERE generation_id = generation
    ), versions AS (
        SELECT document_version FROM members UNION SELECT document_version FROM evidence
    )
    SELECT o.raw_digest FROM observations x JOIN rfc.source_observations o ON o.identity = x.id
    UNION SELECT d.txt_digest FROM versions x JOIN rfc.document_versions d ON d.identity = x.document_version
$$;

CREATE FUNCTION rfc.promote_generation(target bigint, expected bigint, rollback boolean) RETURNS bigint
LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE previous bigint;
DECLARE sequence bigint;
BEGIN
    PERFORM rfc.require_coordinator();
    SELECT generation_id, publication_sequence INTO STRICT previous, sequence
        FROM rfc.active_generation WHERE singleton FOR UPDATE;
    IF sequence <> expected THEN
        RAISE EXCEPTION 'publication sequence conflict (current %)', sequence USING ERRCODE = '40001';
    END IF;
    IF NOT EXISTS (SELECT FROM rfc.generations g JOIN rfc.generation_ingestion i ON i.generation_id = g.id
        WHERE g.id = target AND g.state = CASE WHEN rollback THEN 'retained' ELSE 'ready' END
        AND i.scope = 'full' AND EXISTS (SELECT FROM rfc.generation_documents WHERE generation_id = g.id)) THEN
        RAISE EXCEPTION 'generation is not eligible for promotion' USING ERRCODE = '55000';
    END IF;
    IF NOT rollback AND EXISTS (
        SELECT rfc_id FROM rfc.generation_documents WHERE generation_id = previous
        EXCEPT SELECT rfc_id FROM rfc.generation_documents WHERE generation_id = target
    ) THEN
        RAISE EXCEPTION 'candidate drops an RFC from the active generation' USING ERRCODE = '23514';
    END IF;
    IF previous IS NOT NULL THEN
        UPDATE rfc.generations SET state = 'retained', retired_at = clock_timestamp()
            WHERE id = previous AND state = 'published';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'active generation is not published' USING ERRCODE = '23514';
        END IF;
    END IF;
    sequence := sequence + 1;
    UPDATE rfc.generations SET state = 'published', published_at = clock_timestamp(),
        retired_at = NULL, last_publication_sequence = sequence WHERE id = target;
    UPDATE rfc.active_generation SET generation_id = target, publication_sequence = sequence WHERE singleton;
    INSERT INTO rfc.notification_outbox (publication_sequence, generation_id, event_id)
        VALUES (sequence, target, gen_random_uuid());
    RETURN sequence;
END
$$;

CREATE FUNCTION rfc.retire_generation(target bigint, keep_count integer, keep_hours integer, discard boolean)
RETURNS boolean LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE lifecycle text;
DECLARE demoted timestamptz;
DECLARE sequence bigint;
BEGIN
    PERFORM rfc.require_coordinator();
    IF target < 0 OR keep_count < 3 OR keep_hours < 24 THEN
        RAISE EXCEPTION 'retention cannot be shorter than three generations or 24 hours';
    END IF;
    IF NOT rfc.holds_exclusive_lock(target) THEN
        RAISE EXCEPTION 'exclusive generation lock must be held' USING ERRCODE = '55000';
    END IF;
    SELECT state, retired_at, last_publication_sequence INTO lifecycle, demoted, sequence
        FROM rfc.generations WHERE id = target FOR UPDATE;
    IF NOT FOUND OR EXISTS (SELECT FROM rfc.active_generation WHERE generation_id = target) THEN
        RETURN false;
    END IF;
    IF discard THEN
        IF lifecycle <> 'ready' THEN RETURN false; END IF;
    ELSIF lifecycle = 'retained' THEN
        IF demoted IS NULL OR sequence IS NULL OR demoted > clock_timestamp() - make_interval(hours => keep_hours)
            OR (SELECT count(*) FROM rfc.generations WHERE state = 'retained' AND last_publication_sequence > sequence) < keep_count THEN
            RETURN false;
        END IF;
    ELSIF lifecycle <> 'failed' THEN
        RETURN false;
    END IF;
    UPDATE rfc.generations SET state = 'deleting' WHERE id = target;
    DELETE FROM rfc.edge_evidence WHERE generation_id = target;
    DELETE FROM rfc.rfc_relations WHERE generation_id = target;
    DELETE FROM rfc.relation_coverage WHERE generation_id = target;
    DELETE FROM rfc.generation_ingestion WHERE generation_id = target;
    DELETE FROM rfc.generation_documents WHERE generation_id = target;
    RETURN true;
END
$$;

-- Only records without a remaining generation path are pruned. The foreign
-- keys also protect against omitting a reference when this schema evolves.
CREATE VIEW rfc.unused_document_versions AS
    SELECT d.identity FROM rfc.document_versions d
    WHERE NOT EXISTS (SELECT FROM rfc.generation_documents m WHERE m.document_version = d.identity)
      AND NOT EXISTS (SELECT FROM rfc.relation_evidence e WHERE e.document_version = d.identity);

CREATE FUNCTION rfc.mark_unreachable_blobs() RETURNS void
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
    DELETE FROM rfc.source_observations o
        WHERE NOT EXISTS (SELECT FROM rfc.generation_documents m WHERE m.catalog_observation = o.identity)
          AND NOT EXISTS (SELECT FROM rfc.generation_documents m WHERE m.txt_observation = o.identity)
          AND NOT EXISTS (SELECT FROM rfc.generation_documents m WHERE m.rfcxml_observation = o.identity)
          AND NOT EXISTS (SELECT FROM rfc.generation_ingestion i WHERE i.catalog_before = o.identity)
          AND NOT EXISTS (SELECT FROM rfc.generation_ingestion i WHERE i.catalog_after = o.identity)
          AND NOT EXISTS (SELECT FROM rfc.relation_evidence e WHERE e.observation = o.identity)
          AND NOT EXISTS (SELECT FROM rfc.control_events c WHERE c.observation_digest = o.identity);
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

CREATE FUNCTION rfc.queue_orphan_blobs(candidates bytea[]) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    PERFORM rfc.require_coordinator();
    INSERT INTO rfc.blob_deletion_queue (digest)
        SELECT c.digest FROM unnest(candidates) AS c(digest)
        WHERE NOT EXISTS (SELECT FROM rfc.blobs b WHERE b.digest = c.digest)
          AND NOT EXISTS (SELECT FROM rfc.control_events e WHERE e.observation_digest = c.digest)
        ON CONFLICT DO NOTHING;
END
$$;

CREATE FUNCTION rfc.finish_blob_deletion(target bytea) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    PERFORM rfc.require_coordinator();
    DELETE FROM rfc.blob_deletion_queue WHERE digest = target
        AND NOT EXISTS (SELECT FROM rfc.blobs WHERE digest = target)
        AND NOT EXISTS (SELECT FROM rfc.control_events WHERE observation_digest = target);
END
$$;

GRANT SELECT ON rfc.blob_deletion_queue TO rfc_sync;
GRANT EXECUTE ON FUNCTION rfc.generation_blobs(bigint), rfc.promote_generation(bigint, bigint, boolean),
    rfc.retire_generation(bigint, integer, integer, boolean), rfc.mark_unreachable_blobs(),
    rfc.queue_orphan_blobs(bytea[]), rfc.finish_blob_deletion(bytea) TO rfc_sync;
