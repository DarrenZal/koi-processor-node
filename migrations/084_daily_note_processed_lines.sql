-- Migration 084: daily_note_processed_lines + web_submissions.metadata
--
-- Phase 2 of agent-inbox-pipeline (an internal planning document).
--
-- daily_note_processed_lines: idempotency + outcome tracking for daily-note
-- agent queue. One row per (note_path, line_hash, url_hash) — multi-URL lines
-- get one row per URL.
--
-- web_submissions.metadata: arbitrary jsonb passthrough for source_line_hash,
-- disposition, and other processor-injected metadata. Plan originally assumed
-- koi_memories.metadata but /web/ingest writes to web_submissions, not
-- koi_memories.

CREATE TABLE IF NOT EXISTS daily_note_processed_lines (
    note_path     text        NOT NULL,
    line_hash     text        NOT NULL,
    url_hash      text        NOT NULL,
    verb          text        NOT NULL,
    status        text        NOT NULL,
    reason        text,
    episode_id    uuid,
    memory_id     uuid,
    source_tier   text,
    processed_at  timestamptz NOT NULL DEFAULT now(),
    ack_at        timestamptz,
    PRIMARY KEY (note_path, line_hash, url_hash),
    CONSTRAINT chk_dnpl_status CHECK (status IN (
        'OK', 'AMBIGUOUS', 'FAILED', 'DEFERRED', 'INTERACTIVE_FLAGGED'
    )),
    CONSTRAINT chk_dnpl_verb CHECK (verb IN (
        'ingest', 'index', 'look at', 'read', 'idea'
    )),
    CONSTRAINT chk_dnpl_source_tier CHECK (source_tier IS NULL OR source_tier IN (
        'aiohttp', 'playwright', 'scrapling', 'none'
    ))
);

CREATE INDEX IF NOT EXISTS idx_dnpl_status
    ON daily_note_processed_lines (status, processed_at);

CREATE INDEX IF NOT EXISTS idx_dnpl_ack
    ON daily_note_processed_lines (ack_at)
    WHERE ack_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_dnpl_url_hash
    ON daily_note_processed_lines (note_path, url_hash);

ALTER TABLE web_submissions
    ADD COLUMN IF NOT EXISTS metadata jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_web_submissions_metadata_gin
    ON web_submissions USING gin (metadata);
