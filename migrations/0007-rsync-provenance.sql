ALTER TABLE rfc.source_observations
    ADD COLUMN source_transport text GENERATED ALWAYS AS (
        CASE
            WHEN source_url LIKE 'https://%' THEN 'https'
            WHEN source_url LIKE 'rsync://%' THEN 'rsync'
            ELSE 'other'
        END
    ) STORED,
    ALTER COLUMN fetched_at DROP NOT NULL,
    ADD CONSTRAINT source_transport_evidence CHECK (
        CASE WHEN source_transport = 'rsync' THEN
            kind = 'txt' AND https_verification = 'unverified'
            AND source_url ~ '^rsync://rsync[.]rfc-editor[.]org/rfcs-text-only/rfc[1-9][0-9]*[.]txt$'
            AND etag IS NULL AND last_modified IS NULL
            AND (fetched_at IS NULL OR fetched_at <= observed_at)
        ELSE fetched_at IS NOT NULL END
    );

CREATE OR REPLACE FUNCTION rfc.check_membership() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM rfc.document_versions d
        JOIN rfc.source_observations o ON o.identity = NEW.txt_observation
        JOIN rfc.source_observations c ON c.identity = NEW.catalog_observation
        CROSS JOIN rfc.active_generation a
        WHERE d.identity = NEW.document_version AND d.sealed
          AND o.raw_digest = d.txt_digest AND o.kind = 'txt'
          AND c.kind = 'catalog' AND c.https_verification = 'verified'
          AND a.singleton AND (
              o.https_verification = 'verified'
              OR (
                  o.source_transport = 'rsync'
                  AND o.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || NEW.rfc_id || '.txt'
                  AND (
                      a.publication_sequence = 0
                      OR EXISTS (
                          SELECT FROM rfc.generation_documents live
                          WHERE live.generation_id = a.generation_id AND live.rfc_id = NEW.rfc_id
                            AND live.txt_observation = NEW.txt_observation
                      )
                  )
              )
          )
    ) THEN
        RAISE EXCEPTION 'membership requires a sealed version and eligible TXT observation' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

-- Promotion holds the active row lock. When this trigger fires the pointer
-- still identifies the previous generation, even if it is already retained.
CREATE FUNCTION rfc.check_rsync_promotion() RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, rfc AS $$
BEGIN
    IF EXISTS (
        SELECT FROM rfc.active_generation a
        JOIN rfc.generation_documents candidate ON candidate.generation_id = NEW.id
        JOIN rfc.source_observations o ON o.identity = candidate.txt_observation
        WHERE a.singleton AND a.publication_sequence > 0 AND o.source_transport = 'rsync'
          AND NOT EXISTS (
              SELECT FROM rfc.generation_documents live
              WHERE live.generation_id = a.generation_id AND live.rfc_id = candidate.rfc_id
                AND live.txt_observation = candidate.txt_observation
          )
    ) THEN
        RAISE EXCEPTION 'candidate contains an rsync-only TXT observation outside active membership' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER guard_rsync_promotion BEFORE UPDATE OF state ON rfc.generations
    FOR EACH ROW WHEN (OLD.state = 'ready' AND NEW.state = 'published')
    EXECUTE FUNCTION rfc.check_rsync_promotion();
