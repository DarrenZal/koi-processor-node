-- =============================================================================
-- Migration 115: predicate_raw (irreversibility guard) + resolver_decisions log
-- =============================================================================
-- Date:     2026-08-31
-- Plan:     an internal planning document, Phase 0 #3-#4
--
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/115_predicate_raw_and_resolver_decisions.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/115_predicate_raw_and_resolver_decisions_down.sql
--
-- Both changes are pure additions (nullable column, new table) -- safe to apply
-- before the corresponding application code ships, and safe to leave applied
-- if that code is ever rolled back.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. knowledge_facts.predicate_raw
-- -----------------------------------------------------------------------------
-- The write path at api/routers/knowledge_router.py:~1038 case-folds every
-- predicate with .upper() before it reaches the `predicate` column. That is a
-- real, load-bearing decision (entity_relationships/allowed_predicates expect
-- snake_case; the coercion is what makes .upper() a bug there, not here) but it
-- is irreversible: once a caller's original casing/spelling is discarded, no
-- later canonicalization pass can recover what the extractor actually said.
--
-- predicate_raw stores exactly what the caller sent, untouched. Populate on
-- every future write; NEVER overwrite an existing row's value; never derive it
-- FROM `predicate` (that would just be re-deriving .upper()'s output, which
-- defeats the point). Existing rows get NULL, not a backfilled guess -- there
-- is nothing to backfill from; the raw string was already discarded before
-- this column existed.
ALTER TABLE knowledge_facts
    ADD COLUMN IF NOT EXISTS predicate_raw TEXT;

COMMENT ON COLUMN knowledge_facts.predicate_raw IS
  'The predicate exactly as the caller sent it, before any case-folding or canonicalization. Populated on write only (migration 115, 2026-08-31); NULL on rows written before this column existed. Never overwrite once set -- this is the irreversibility guard for future predicate-vocabulary work.';


-- -----------------------------------------------------------------------------
-- 1b. knowledge_facts.confidence
-- -----------------------------------------------------------------------------
-- The deep-extraction JSON schema REQUIRES confidence (enum high/medium/low)
-- on every emitted fact. FactInput (api/routers/knowledge_router.py) declares
-- no such field, and Pydantic's default extra='ignore' means the value is
-- silently dropped on every write -- verified by execution (model_dump keys
-- are exactly {subject,predicate,object,...}, no confidence). This column
-- gives it somewhere to land. Mapping from the enum to REAL is a documented,
-- simple, monotonic choice (high=1.0, medium=0.6, low=0.3) matching
-- entity_relationships' own convention of treating an unqualified assert as
-- confidence=1.0 -- see api/routers/knowledge_router.py for where it's
-- applied. NULL on existing rows and on any future write that omits it
-- (paired with FactInput's extra='forbid', a caller that OMITS confidence
-- entirely still writes successfully with NULL; only an extra/unknown field
-- becomes a 422).
ALTER TABLE knowledge_facts
    ADD COLUMN IF NOT EXISTS confidence REAL;

COMMENT ON COLUMN knowledge_facts.confidence IS
  'Mapped from the extraction schema''s enum (high=1.0, medium=0.6, low=0.3). NULL = not supplied (all rows before migration 115, 2026-08-31, and any caller that omits the field). Previously silently dropped by FactInput''s Pydantic extra=ignore default.';


-- -----------------------------------------------------------------------------
-- 2. resolver_decisions
-- -----------------------------------------------------------------------------
-- Every resolve_entity() call in api/personal_ingest_api.py makes one or more
-- named decisions (tier N accepted a candidate at score S for reason R; tier N
-- rejected a candidate at score S for reason R; nothing matched, so create).
-- Today those decisions are logger.info() calls that land in
-- ~/.config/personal-koi/stderr.log (364MB, unrotated, read by nothing) and are
-- gone the moment the log rotates or the disk fills. Rejections in particular
-- are the only negative labels this system will ever get for free -- ~1,180/day
-- destroyed. This table is simultaneously the audit trail, the eval-set seed,
-- and the labelling queue: query the whole population by tier/reason/caller,
-- or sample from it for hand-labelling.
--
-- Deliberately NOT a replacement for resolver_shadow.py's sampled legacy/strict
-- A/B comparison (different purpose: policy divergence measurement at 10%
-- sample). This table is the full population, both policies collapsed into
-- whatever the live resolver actually did.
CREATE TABLE IF NOT EXISTS resolver_decisions (
    id                BIGSERIAL PRIMARY KEY,
    decided_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_id        TEXT NOT NULL,   -- correlates every row from one resolve_entity() call
    caller             TEXT NOT NULL,   -- resolution_caller, e.g. 'personal_ingest_api.resolve_entity'
    entity_type       TEXT,
    query_text        TEXT NOT NULL,
    query_normalized  TEXT NOT NULL,
    tier              TEXT NOT NULL,
    decision          TEXT NOT NULL,
    candidate_uri     TEXT,
    candidate_text    TEXT,
    score             REAL,
    reason            TEXT,

    CONSTRAINT chk_resolver_decisions_tier CHECK (tier IN (
        'tier1_exact', 'tier1_1_alias', 'tier1_1b_cross_type',
        'tier1_5_contextual', 'tier2a_fuzzy', 'tier2b_semantic', 'tier3_create'
    )),
    CONSTRAINT chk_resolver_decisions_decision CHECK (decision IN (
        'accepted', 'rejected', 'created'
    ))
);

CREATE INDEX IF NOT EXISTS idx_resolver_decisions_decided_at
    ON resolver_decisions (decided_at);
CREATE INDEX IF NOT EXISTS idx_resolver_decisions_tier_decision
    ON resolver_decisions (tier, decision);
CREATE INDEX IF NOT EXISTS idx_resolver_decisions_candidate_uri
    ON resolver_decisions (candidate_uri) WHERE candidate_uri IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_resolver_decisions_attempt_id
    ON resolver_decisions (attempt_id);

COMMENT ON TABLE resolver_decisions IS
  'Full (unsampled) log of every resolve_entity() accept/reject/create decision. Written best-effort on a connection separate from the caller''s transaction (see api/resolver_decisions_log.py) -- a logging failure must never affect entity resolution. Migration 115, 2026-08-31, Phase 0 #4 of the pipeline hardening audit.';


-- -----------------------------------------------------------------------------
-- Migration bookkeeping
-- -----------------------------------------------------------------------------
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('115_predicate_raw_and_resolver_decisions', 'v1_phase0_guards')
ON CONFLICT (migration_id) DO NOTHING;
