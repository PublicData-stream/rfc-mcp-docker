ALTER TABLE rfc.sync_jobs
    ADD COLUMN expected_publication_sequence bigint CHECK (expected_publication_sequence >= 0),
    ADD COLUMN retries integer NOT NULL DEFAULT 0 CHECK (retries BETWEEN 0 AND 2),
    ADD COLUMN next_attempt_at timestamptz NOT NULL DEFAULT now();

CREATE UNIQUE INDEX one_unfinished_sync_job_per_state ON rfc.sync_jobs (state)
    WHERE state IN ('pending', 'running');
CREATE INDEX control_events_job ON rfc.control_events (job_id)
    WHERE status IN ('accepted', 'coalesced');

GRANT UPDATE (expected_publication_sequence, retries, next_attempt_at)
    ON rfc.sync_jobs TO rfc_sync;
GRANT UPDATE (delivery_state, attempts, next_attempt_at, delivered_at, last_error)
    ON rfc.notification_outbox TO rfc_sync;
