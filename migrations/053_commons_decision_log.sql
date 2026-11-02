-- Migration 053: Commons decision audit log + expanded intake status model
--
-- Adds INSERT-only audit trail for commons intake decisions, expands the
-- intake_status field to support async ingest pipeline (approved → ingesting →
-- ingested | needs_merge_review | failed), and adds retry/lease columns.

-- 1. Immutable decision log (INSERT-only audit trail)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS koi_commons_decisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    share_id INT NOT NULL REFERENCES koi_shared_documents(id),
    event_id UUID,
    action TEXT NOT NULL CHECK (action IN ('approve', 'reject')),
    reviewer TEXT,
    note TEXT,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_commons_decisions_share
    ON koi_commons_decisions(share_id);
CREATE INDEX IF NOT EXISTS idx_commons_decisions_time
    ON koi_commons_decisions(decided_at);

-- 2. Expand intake_status to support async ingest pipeline
-- Valid states: none, staged, approved, ingesting, needs_merge_review, ingested, failed, rejected
ALTER TABLE koi_shared_documents
    DROP CONSTRAINT IF EXISTS chk_intake_status;

ALTER TABLE koi_shared_documents
    ADD CONSTRAINT chk_intake_status
    CHECK (intake_status IN ('none', 'staged', 'approved', 'ingesting', 'needs_merge_review', 'ingested', 'failed', 'rejected'));

-- 3. Add retry and lease tracking columns for ingest worker
ALTER TABLE koi_shared_documents
    ADD COLUMN IF NOT EXISTS retry_count INT DEFAULT 0;

ALTER TABLE koi_shared_documents
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;

ALTER TABLE koi_shared_documents
    ADD COLUMN IF NOT EXISTS ingest_started_at TIMESTAMPTZ;

-- 4. Index for worker claim query (approved or retry-eligible failed rows)
CREATE INDEX IF NOT EXISTS idx_shared_docs_ingestable
    ON koi_shared_documents(reviewed_at ASC)
    WHERE intake_status IN ('approved', 'failed');
