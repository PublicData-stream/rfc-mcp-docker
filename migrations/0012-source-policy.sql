CREATE TABLE rfc.policy_receipts (
    identity rfc.digest PRIMARY KEY,
    rfc_id rfc.rfc_number NOT NULL,
    source_digest rfc.digest NOT NULL,
    registry_digest rfc.digest NOT NULL,
    decision text NOT NULL CHECK (decision IN ('metadata_only','whole_only','whole_paginated','excerpt_allowed')),
    receipt jsonb NOT NULL CHECK (jsonb_typeof(receipt) = 'object' AND octet_length(receipt::text) <= 131072),
    UNIQUE (identity, rfc_id, source_digest, decision),
    CHECK ((receipt ->> 'rfc', receipt ->> 'source_digest', receipt ->> 'registry_digest', receipt ->> 'decision')
        IS NOT DISTINCT FROM (rfc_id::text, encode(source_digest, 'hex'), encode(registry_digest, 'hex'), decision))
);

ALTER TABLE rfc.document_versions ADD COLUMN policy_receipt rfc.digest;
ALTER TABLE rfc.document_versions ADD CONSTRAINT document_policy_receipt
    FOREIGN KEY (policy_receipt, rfc_id, txt_digest, notice_policy)
    REFERENCES rfc.policy_receipts (identity, rfc_id, source_digest, decision);
CREATE INDEX document_versions_policy_receipt ON rfc.document_versions (policy_receipt)
    WHERE policy_receipt IS NOT NULL;

ALTER TABLE rfc.notice_ranges DROP CONSTRAINT notice_ranges_category_check;
ALTER TABLE rfc.notice_ranges ADD CHECK (category IN
    ('copyright','license','authorship','disclaimer','legend','restriction','third_party','other'));

CREATE TABLE rfc.generation_policy (
    generation_id bigint PRIMARY KEY REFERENCES rfc.generations ON DELETE CASCADE,
    registry_digest rfc.digest NOT NULL,
    classifier text NOT NULL CHECK (octet_length(classifier) BETWEEN 1 AND 128),
    fixture boolean NOT NULL
);
CREATE TRIGGER generation_open BEFORE INSERT ON rfc.generation_policy
    FOR EACH ROW EXECUTE FUNCTION rfc.check_generation_open();

CREATE TABLE rfc.policy_state (
    singleton boolean PRIMARY KEY CHECK (singleton),
    rights_epoch bigint NOT NULL DEFAULT 0 CHECK (rights_epoch >= 0),
    registry_digest rfc.digest,
    classifier text,
    CHECK ((registry_digest IS NULL) = (classifier IS NULL))
);
INSERT INTO rfc.policy_state (singleton) VALUES (true);

CREATE TABLE rfc.policy_approvals (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    registry_digest rfc.digest NOT NULL,
    classifier text NOT NULL,
    audit_digest rfc.digest NOT NULL,
    source_manifest rfc.digest NOT NULL,
    review_ref text NOT NULL CHECK (octet_length(review_ref) BETWEEN 1 AND 1024),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE rfc.policy_restrictions (
    epoch bigint PRIMARY KEY CHECK (epoch > 0),
    rfc_id rfc.rfc_number NOT NULL,
    ceiling text NOT NULL CHECK (ceiling IN ('metadata_only','whole_only','whole_paginated')),
    reason text NOT NULL CHECK (reason IN ('rights_revoked','legal_obligation_changed','unresolved_rights')),
    review_ref text NOT NULL CHECK (octet_length(review_ref) BETWEEN 1 AND 1024),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX policy_restrictions_current ON rfc.policy_restrictions (rfc_id, epoch DESC)
    INCLUDE (ceiling);

CREATE FUNCTION rfc.policy_rank(policy text) RETURNS integer
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT CASE policy WHEN 'whole_only' THEN 1 WHEN 'whole_paginated' THEN 2
        WHEN 'excerpt_allowed' THEN 3 ELSE 0 END
$$;

CREATE FUNCTION rfc.approve_policy(registry bytea, classifier_id text, report bytea, manifest bytea, reference text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
BEGIN
    IF registry IS NULL OR classifier_id IS NULL OR octet_length(classifier_id) NOT BETWEEN 1 AND 128 THEN
        RAISE EXCEPTION 'invalid policy bundle';
    END IF;
    PERFORM FROM rfc.policy_state WHERE singleton FOR UPDATE;
    IF EXISTS (SELECT FROM rfc.policy_state WHERE singleton
        AND registry_digest = registry AND classifier = classifier_id)
        AND EXISTS (SELECT FROM rfc.policy_approvals WHERE registry_digest = registry AND classifier = classifier_id
            AND audit_digest = report AND source_manifest = manifest AND review_ref = reference) THEN
        RETURN;
    END IF;
    INSERT INTO rfc.policy_approvals (registry_digest, classifier, audit_digest, source_manifest, review_ref)
        VALUES (registry, classifier_id, report, manifest, reference);
    UPDATE rfc.policy_state SET registry_digest = registry, classifier = classifier_id WHERE singleton;
END
$$;

CREATE FUNCTION rfc.restrict_policy(number integer, cap text, code text, reference text, expected bigint)
RETURNS bigint LANGUAGE plpgsql STRICT SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE current_epoch bigint;
DECLARE prior text;
BEGIN
    IF NOT rfc.holds_exclusive_lock(-4) THEN
        RAISE EXCEPTION 'rights fence must be held' USING ERRCODE = '55000';
    END IF;
    SELECT rights_epoch INTO STRICT current_epoch FROM rfc.policy_state WHERE singleton FOR UPDATE;
    IF current_epoch <> expected THEN
        RAISE EXCEPTION 'rights epoch conflict (current %)', current_epoch USING ERRCODE = '40001';
    END IF;
    SELECT ceiling INTO prior FROM rfc.policy_restrictions WHERE rfc_id = number ORDER BY epoch DESC LIMIT 1;
    IF cap = prior THEN
        RETURN current_epoch;
    END IF;
    IF rfc.policy_rank(cap) >= rfc.policy_rank(coalesce(prior, 'excerpt_allowed')) THEN
        RAISE EXCEPTION 'a rights restriction must narrow permissions' USING ERRCODE = '23514';
    END IF;
    current_epoch := current_epoch + 1;
    INSERT INTO rfc.policy_restrictions (epoch, rfc_id, ceiling, reason, review_ref)
        VALUES (current_epoch, number, cap, code, reference);
    UPDATE rfc.policy_state SET rights_epoch = current_epoch WHERE singleton;
    RETURN current_epoch;
END
$$;

CREATE FUNCTION rfc.check_generation_policy() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE bundle rfc.generation_policy%ROWTYPE;
BEGIN
    SELECT * INTO bundle FROM rfc.generation_policy WHERE generation_id = NEW.id;
    IF bundle.generation_id IS NULL OR NEW.manifest_format IS DISTINCT FROM 4 OR EXISTS (
        SELECT FROM rfc.generation_documents m
        JOIN rfc.document_versions d ON d.identity = m.document_version
        LEFT JOIN rfc.policy_receipts p ON p.identity = d.policy_receipt
        WHERE m.generation_id = NEW.id AND (
            p.identity IS NULL OR p.registry_digest <> bundle.registry_digest
            OR (p.receipt ->> 'classifier') IS DISTINCT FROM bundle.classifier
            OR coalesce(p.receipt -> 'notices', 'null'::jsonb) IS DISTINCT FROM (
                SELECT coalesce(jsonb_agg(jsonb_build_object('category', n.category,
                    'start_byte', n.start_byte, 'end_byte', n.end_byte) ORDER BY n.ordinal), '[]'::jsonb)
                FROM rfc.notice_ranges n WHERE n.document_version = d.identity)
            OR (d.notice_policy IN ('excerpt_allowed', 'whole_paginated') AND
                (d.notice_state <> 'complete' OR NOT EXISTS (
                    SELECT FROM rfc.notice_ranges n WHERE n.document_version = d.identity)))
        )
    ) THEN
        RAISE EXCEPTION 'candidate policy evidence is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER require_source_policy BEFORE UPDATE OF state ON rfc.generations
    FOR EACH ROW WHEN ((OLD.state = 'building' AND NEW.state = 'ready')
        OR (OLD.state = 'ready' AND NEW.state = 'published'))
    EXECUTE FUNCTION rfc.check_generation_policy();

GRANT SELECT ON rfc.policy_receipts, rfc.generation_policy TO rfc_sync, rfc_serve;
GRANT INSERT ON rfc.policy_receipts, rfc.generation_policy TO rfc_sync;
GRANT SELECT ON rfc.policy_state TO rfc_sync;
GRANT SELECT (singleton, rights_epoch) ON rfc.policy_state TO rfc_serve;
GRANT SELECT ON rfc.policy_restrictions TO rfc_sync;
GRANT SELECT (rfc_id, epoch, ceiling) ON rfc.policy_restrictions TO rfc_serve;
GRANT INSERT (policy_receipt) ON rfc.document_versions TO rfc_sync;
GRANT EXECUTE ON FUNCTION rfc.approve_policy(bytea,text,bytea,bytea,text),
    rfc.restrict_policy(integer,text,text,text,bigint) TO rfc_sync;

CREATE OR REPLACE FUNCTION rfc.check_generation_audit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, rfc, pg_temp AS $$
DECLARE
    audit rfc.generation_audit%ROWTYPE;
    ingestion rfc.generation_ingestion%ROWTYPE;
BEGIN
    PERFORM rfc.require_coordinator();
    SELECT * INTO audit FROM rfc.generation_audit WHERE generation_id = NEW.id;
    SELECT * INTO ingestion FROM rfc.generation_ingestion WHERE generation_id = NEW.id;
    IF audit.generation_id IS NULL OR NEW.manifest_format IS DISTINCT FROM 4 THEN
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
    -- Serialize the check with pins and cross-generation rights changes.
    PERFORM FROM rfc.policy_state WHERE singleton FOR UPDATE;
    IF NOT rollback AND EXISTS (
        SELECT FROM rfc.generation_documents m
        JOIN rfc.document_versions d ON d.identity = m.document_version
        JOIN rfc.policy_receipts p ON p.identity = d.policy_receipt
        WHERE m.generation_id = target AND p.receipt ->> 'approval_method' = 'automatic'
        AND NOT EXISTS (SELECT FROM rfc.policy_state WHERE singleton
            AND registry_digest = p.registry_digest AND classifier = p.receipt ->> 'classifier')
    ) THEN
        RAISE EXCEPTION 'candidate policy registry is not approved' USING ERRCODE = '55000';
    END IF;
    IF rollback AND EXISTS (
        SELECT FROM rfc.generation_documents m
        JOIN rfc.document_versions d ON d.identity = m.document_version
        JOIN LATERAL (SELECT ceiling FROM rfc.policy_restrictions r WHERE r.rfc_id = m.rfc_id
            ORDER BY r.epoch DESC LIMIT 1) cap ON true
        WHERE m.generation_id = target AND rfc.policy_rank(d.notice_policy) > rfc.policy_rank(cap.ceiling)
    ) THEN
        RAISE EXCEPTION 'rollback would restore revoked permissions' USING ERRCODE = '55000';
    END IF;
    IF NOT rollback AND EXISTS (
        SELECT FROM rfc.generation_documents old_m
        JOIN rfc.generations old_g ON old_g.id = old_m.generation_id
            AND old_g.state IN ('published', 'retained')
        JOIN rfc.document_versions old_d ON old_d.identity = old_m.document_version
        LEFT JOIN rfc.policy_receipts old_p ON old_p.identity = old_d.policy_receipt
        LEFT JOIN rfc.generation_documents new_m ON (new_m.generation_id, new_m.rfc_id) = (target, old_m.rfc_id)
        LEFT JOIN rfc.document_versions new_d ON new_d.identity = new_m.document_version
        LEFT JOIN rfc.policy_receipts new_p ON new_p.identity = new_d.policy_receipt
        LEFT JOIN LATERAL (SELECT ceiling FROM rfc.policy_restrictions r WHERE r.rfc_id = old_m.rfc_id
            ORDER BY r.epoch DESC LIMIT 1) cap ON true
        WHERE (old_d.notice_policy <> 'excerpt_allowed' OR old_p.identity IS NOT NULL)
          AND (
            least(rfc.policy_rank(old_d.notice_policy),
                rfc.policy_rank(coalesce(cap.ceiling, 'excerpt_allowed'))) > rfc.policy_rank(new_d.notice_policy)
            OR (old_d.notice_policy = 'excerpt_allowed' AND new_d.notice_policy = 'excerpt_allowed'
                AND cap.ceiling IS NULL
                AND (old_p.receipt -> 'license', old_p.receipt -> 'template')
                    IS DISTINCT FROM (new_p.receipt -> 'license', new_p.receipt -> 'template'))
          )
    ) THEN
        RAISE EXCEPTION 'candidate requires cross-generation rights restriction' USING ERRCODE = '55000';
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
    DELETE FROM rfc.policy_receipts p WHERE NOT EXISTS (
        SELECT FROM rfc.document_versions d WHERE d.policy_receipt = p.identity);
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
