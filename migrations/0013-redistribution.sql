-- Old receipts and generation policy rows are retained for format-4 verification.
DROP TRIGGER require_source_policy ON rfc.generations;
DROP FUNCTION rfc.check_generation_policy();
DROP FUNCTION rfc.approve_policy(bytea,text,bytea,bytea,text);
DROP TABLE rfc.policy_approvals;
ALTER TABLE rfc.policy_state DROP COLUMN registry_digest, DROP COLUMN classifier;
ALTER TABLE rfc.generation_ingestion ALTER COLUMN policy_registry DROP NOT NULL;
ALTER TABLE rfc.generation_audit ADD COLUMN fixture boolean NOT NULL DEFAULT false;
REVOKE INSERT ON rfc.generation_policy, rfc.policy_receipts FROM rfc_sync;
REVOKE INSERT (policy_receipt) ON rfc.document_versions FROM rfc_sync;

CREATE TABLE rfc.source_rights (
    generation_id bigint NOT NULL,
    rfc_id rfc.rfc_number NOT NULL,
    identity rfc.digest NOT NULL,
    evidence jsonb NOT NULL CHECK (jsonb_typeof(evidence) = 'object' AND octet_length(evidence::text) <= 131072),
    PRIMARY KEY (generation_id, rfc_id),
    FOREIGN KEY (generation_id, rfc_id) REFERENCES rfc.generation_documents ON DELETE CASCADE
);
GRANT SELECT ON rfc.source_rights TO rfc_sync, rfc_serve;
GRANT INSERT, UPDATE (identity, evidence) ON rfc.source_rights TO rfc_sync;

CREATE FUNCTION rfc.check_source_rights_write() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    PERFORM rfc.require_coordinator();
    IF NOT EXISTS (SELECT FROM rfc.generations g WHERE g.id = NEW.generation_id
        AND (g.state = 'building'
             OR (g.state IN ('published','retained') AND rfc.holds_exclusive_lock(-4)))) THEN
        RAISE EXCEPTION 'rights changes require an open candidate or the maintenance fence' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER source_rights_write BEFORE INSERT OR UPDATE ON rfc.source_rights
    FOR EACH ROW EXECUTE FUNCTION rfc.check_source_rights_write();

ALTER TABLE rfc.policy_restrictions ADD COLUMN denied integer NOT NULL DEFAULT 0;
UPDATE rfc.policy_restrictions SET denied = CASE ceiling
    WHEN 'metadata_only' THEN 7 WHEN 'whole_only' THEN 6 ELSE 4 END;
ALTER TABLE rfc.policy_restrictions ALTER COLUMN ceiling DROP NOT NULL,
    ADD CHECK (denied BETWEEN 1 AND 15),
    ALTER COLUMN denied DROP DEFAULT;
GRANT SELECT (denied) ON rfc.policy_restrictions TO rfc_serve;

CREATE VIEW rfc.source_restrictions AS
    SELECT rfc_id, bit_or(denied) AS denied FROM rfc.policy_restrictions GROUP BY rfc_id;
GRANT SELECT ON rfc.source_restrictions TO rfc_sync, rfc_serve;

CREATE FUNCTION rfc.deny_source_rights(number integer, mask integer, code text, reference text, expected bigint)
RETURNS bigint LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE current_epoch bigint;
DECLARE prior integer;
BEGIN
    IF NOT rfc.holds_exclusive_lock(-4) THEN
        RAISE EXCEPTION 'rights fence must be held' USING ERRCODE = '55000';
    END IF;
    SELECT rights_epoch INTO STRICT current_epoch FROM rfc.policy_state WHERE singleton FOR UPDATE;
    IF current_epoch <> expected THEN
        RAISE EXCEPTION 'rights epoch conflict (current %)', current_epoch USING ERRCODE = '40001';
    END IF;
    IF mask NOT BETWEEN 1 AND 15 THEN
        RAISE EXCEPTION 'invalid rights restriction' USING ERRCODE = '23514';
    END IF;
    SELECT coalesce(denied, 0) INTO prior FROM rfc.source_restrictions WHERE rfc_id = number;
    IF (coalesce(prior, 0) & mask) = mask THEN
        RETURN current_epoch;
    END IF;
    current_epoch := current_epoch + 1;
    INSERT INTO rfc.policy_restrictions (epoch, rfc_id, denied, reason, review_ref)
        VALUES (current_epoch, number, mask, code, reference);
    UPDATE rfc.policy_state SET rights_epoch = current_epoch WHERE singleton;
    RETURN current_epoch;
END
$$;
GRANT EXECUTE ON FUNCTION rfc.deny_source_rights(integer,integer,text,text,bigint) TO rfc_sync;

CREATE OR REPLACE FUNCTION rfc.restrict_policy(number integer, cap text, code text, reference text, expected bigint)
RETURNS bigint LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE mask integer;
BEGIN
    mask := CASE cap WHEN 'metadata_only' THEN 7 WHEN 'whole_only' THEN 6 WHEN 'whole_paginated' THEN 4 END;
    IF mask IS NULL THEN
        RAISE EXCEPTION 'invalid rights ceiling' USING ERRCODE = '23514';
    END IF;
    RETURN rfc.deny_source_rights(number, mask, code, reference, expected);
END
$$;

CREATE OR REPLACE FUNCTION rfc.check_generation_audit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE
    audit rfc.generation_audit%ROWTYPE;
    ingestion rfc.generation_ingestion%ROWTYPE;
BEGIN
    PERFORM rfc.require_coordinator();
    SELECT * INTO audit FROM rfc.generation_audit WHERE generation_id = NEW.id;
    SELECT * INTO ingestion FROM rfc.generation_ingestion WHERE generation_id = NEW.id;
    IF audit.generation_id IS NULL OR NEW.manifest_format IS DISTINCT FROM 5 THEN
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

CREATE OR REPLACE FUNCTION rfc.promote_generation(target bigint, expected bigint, rollback boolean) RETURNS bigint
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
