-- =============================================================================
-- 125_extraction_provenance_and_quality — who produced this graph, and was it good
-- =============================================================================
-- Issue: #64 (document ingest: persist actual model provenance and gate semantic
--        quality).
--
-- TWO SEPARATE GAPS, KEPT SEPARATE HERE ON PURPOSE
--
-- (1) PROVENANCE. `document_window_extractions` records `route_used` and nothing
--     else about the producer. Worse, `route_used` can be WRONG, not merely thin:
--     `ROUTE_USED` in scripts/extract_deep_documents.py is a module-level constant
--     derived from the CONFIGURED transport, while `_extract_window()` degrades
--     through `DOC_EXTRACTOR_TRANSPORT_FALLBACK` and returns only the completion
--     text — the transport that actually served the window is discarded. With a
--     fallback configured, a window served by the fallback is recorded as having
--     used the primary. Separately, `_call_openai()` ignores its `model` argument
--     and uses the module-level OPENAI_MODEL, while the caller logs ANTHROPIC_MODEL
--     — so an openai_compat run prints an Anthropic model as its producer.
--
--     The columns below take the ACTUAL values, captured at the call site.
--
-- (2) QUALITY. The completion gate establishes that every stage ran and met a
--     structural floor. It does not establish that the extraction is any good: the
--     original Buehler (2024) run passed strict with 25 facts and 25 discourse
--     moves; a curated re-run of the same document yielded 115 entity endpoints,
--     213 facts and 52 discourse moves. Both are "complete".
--
--     `document_extraction_runs` gives the semantic verdict its own home and its
--     own status, so a green structural result can never be read as a quality
--     result. The two statuses are separate columns, deliberately — collapsing
--     them into one "status" is how the distinction gets lost again.
--
-- NOTE ON COUNT-ONLY THRESHOLDS (#64 requirement 5): nothing in this schema floors
-- on a raw count. A fact count rewards hallucinated volume — the failure mode that
-- makes a thin extraction and a padded one look equally green. The semantic report
-- is a JSONB document holding per-dimension verdicts with the evidence that
-- produced them; the thresholds live in a catalog next to the gate, not here.
--
-- Date: 2026-09-13
-- Database: personal_koi
-- Reversible: yes — 125_extraction_provenance_and_quality_down.sql (DROPs the new
--             columns and the new table; no existing row is modified by this file,
--             so reversing it loses only data this file created).
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. Per-window producer identity.
-- -----------------------------------------------------------------------------
ALTER TABLE document_window_extractions
    ADD COLUMN IF NOT EXISTS provider  TEXT,
    ADD COLUMN IF NOT EXISTS model     TEXT,
    ADD COLUMN IF NOT EXISTS transport TEXT,
    ADD COLUMN IF NOT EXISTS producer  JSONB;

COMMENT ON COLUMN document_window_extractions.provider IS
  'Who actually served this window: anthropic | openai_compatible | claude_subscription. Captured at the call site AFTER any transport fallback, so it names the producer rather than the configuration.';
COMMENT ON COLUMN document_window_extractions.model IS
  'Exact model/version string the producer was asked for, e.g. claude-sonnet-5 or gpt-4o-mini. For the openai transport this is DOC_EXTRACTOR_OPENAI_MODEL, NOT DOC_EXTRACTOR_MODEL.';
COMMENT ON COLUMN document_window_extractions.transport IS
  'The transport that actually served this window: claude_p | api | openai. May differ from DOC_EXTRACTOR_TRANSPORT when a fallback fired.';
COMMENT ON COLUMN document_window_extractions.producer IS
  'Full producer receipt: endpoint host (never the key), attempt count, fallback_from, repair passes, prompt/schema version, latency_ms, and any token/error metadata the transport returned.';
COMMENT ON COLUMN document_window_extractions.route_used IS
  'Coarse route label. Since migration 125 this is written from the ACTUAL transport after fallback; before 125 it was the configured transport and is unreliable for any run that had DOC_EXTRACTOR_TRANSPORT_FALLBACK set. Prefer the transport/provider/model columns.';

-- Find every window produced by a given model — the question an operator asks after
-- discovering a model was bad ("what did it write, and where is it?").
CREATE INDEX IF NOT EXISTS idx_docwin_provider_model
    ON document_window_extractions (provider, model);

-- -----------------------------------------------------------------------------
-- 2. Per-run provenance + the semantic verdict, separate from structural status.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS document_extraction_runs (
    document_rid        TEXT        NOT NULL,
    run_id              TEXT        NOT NULL,
    tier                TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at            TIMESTAMPTZ,

    -- Provenance, aggregated from the windows of this run. Arrays, not a single
    -- value: a run with a fallback legitimately has more than one producer, and
    -- flattening that to "the model" is how the misattribution happened in the
    -- first place.
    providers           TEXT[]      NOT NULL DEFAULT '{}',
    models              TEXT[]      NOT NULL DEFAULT '{}',
    transports          TEXT[]      NOT NULL DEFAULT '{}',
    prompt_version      TEXT,
    schema_version      TEXT,
    extraction_params   JSONB,

    -- STRUCTURAL: every stage ran and met its floor.
    structural_status   TEXT        NOT NULL DEFAULT 'unknown',
    structural_evidence JSONB,

    -- SEMANTIC: is the extraction actually good. Independent of the above, and
    -- 'not_evaluated' is a distinct state from 'pass' — an unevaluated run must
    -- never read as a passing one.
    semantic_status     TEXT        NOT NULL DEFAULT 'not_evaluated',
    semantic_report     JSONB,

    -- An explicit, attributed waiver. A waiver records a DECISION; it does not
    -- rewrite semantic_status, so the underlying verdict stays legible.
    waiver_reason       TEXT,
    waived_by           TEXT,
    waived_at           TIMESTAMPTZ,

    -- Issue #62's identity contract, recorded per run so a graph written under
    -- pinning can be told apart from one written before it.
    identity_contract   TEXT,
    identity_evidence   JSONB,

    PRIMARY KEY (document_rid, run_id),

    CONSTRAINT document_extraction_runs_structural_status_check
        CHECK (structural_status IN ('unknown', 'pass', 'fail')),
    CONSTRAINT document_extraction_runs_semantic_status_check
        CHECK (semantic_status IN ('not_evaluated', 'pass', 'review', 'fail')),
    -- A waiver must say who and why. A waiver with no attribution is an anonymous
    -- override, which is the thing waivers are supposed to prevent.
    CONSTRAINT document_extraction_runs_waiver_attributed
        CHECK ((waiver_reason IS NULL AND waived_by IS NULL AND waived_at IS NULL)
               OR (waiver_reason IS NOT NULL AND waived_by IS NOT NULL AND waived_at IS NOT NULL))
);

COMMENT ON TABLE document_extraction_runs IS
  'One row per (document, extraction run). Holds the run''s actual producers and its TWO independent verdicts: structural_status (every stage ran and met its floor) and semantic_status (the extraction is good enough to promote). Issue #64. Re-extracting the same document with a different model creates a NEW row, so runs stay separately attributable and comparable rather than overwriting each other.';
COMMENT ON COLUMN document_extraction_runs.semantic_status IS
  'not_evaluated | pass | review | fail. ''not_evaluated'' is deliberately NOT ''pass'': a run nobody assessed must not read as one that passed.';
COMMENT ON COLUMN document_extraction_runs.semantic_report IS
  'Per-dimension verdicts WITH the evidence that produced them (source coverage, citation spans supported by the source text, duplicate/contradiction handling, endpoint integrity, required output classes). Not a single score — a score is what lets a thin extraction and a padded one look alike.';

CREATE INDEX IF NOT EXISTS idx_docruns_document
    ON document_extraction_runs (document_rid, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_docruns_semantic_status
    ON document_extraction_runs (semantic_status)
    WHERE semantic_status <> 'pass';

-- -----------------------------------------------------------------------------
-- 3. Assertions that can actually fail.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
    -- The four new columns exist.
    IF (SELECT count(*) FROM information_schema.columns
         WHERE table_name = 'document_window_extractions'
           AND column_name IN ('provider', 'model', 'transport', 'producer')) <> 4 THEN
        RAISE EXCEPTION '125: document_window_extractions is missing one of the producer columns';
    END IF;

    -- The waiver constraint REJECTS a partial waiver. Asserted by attempting one
    -- and requiring the failure — a constraint nobody has seen refuse anything is
    -- not evidence that it refuses anything.
    BEGIN
        INSERT INTO document_extraction_runs (document_rid, run_id, waiver_reason)
        VALUES ('__125_assert__', '__125_assert__', 'reason with no attribution');
        RAISE EXCEPTION '125: the waiver-attribution constraint did NOT reject a partial waiver';
    EXCEPTION
        WHEN check_violation THEN
            NULL;  -- expected
    END;

    -- semantic_status defaults to not_evaluated, not pass.
    INSERT INTO document_extraction_runs (document_rid, run_id)
    VALUES ('__125_assert__', '__125_assert__');
    IF (SELECT semantic_status FROM document_extraction_runs
         WHERE document_rid = '__125_assert__') <> 'not_evaluated' THEN
        RAISE EXCEPTION '125: semantic_status does not default to not_evaluated';
    END IF;
    DELETE FROM document_extraction_runs WHERE document_rid = '__125_assert__';

    IF EXISTS (SELECT 1 FROM document_extraction_runs WHERE document_rid = '__125_assert__') THEN
        RAISE EXCEPTION '125: assertion fixture was not cleaned up';
    END IF;
END $$;

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('personal:125_extraction_provenance_and_quality', 'v1_provenance_and_quality')
ON CONFLICT (migration_id) DO NOTHING;

COMMIT;
