-- =============================================================================
-- 133: cross-source discourse link candidates and reviewed relations
-- =============================================================================
-- Status:   numbered 133 for the one candidate-only canary on personal_koi
--           (2026-10-19). Creates eight new tables and nothing else. Apply in
--           one transaction: psql -X -1 -v ON_ERROR_STOP=1 -f <this file>.
--           The tests and scripts/discourse_links_demo.py also apply it to
--           throwaway databases they create and drop themselves. Rollback:
--           133_discourse_link_candidates_down.sql.
-- Date:     2026-10-19 (correction passes 1 and 2 and the A1-A3 pass the same day)
-- Issue:    gaiaaiagent/koi-processor#60 (Stage 1 prototype, offline)
-- Base:     regen-prod a5e1990 (session_discourse_moves as of migration 123)
--
-- NO CHANGE TO EXISTING TABLES
-- ----------------------------
-- * session_discourse_moves.resolves_move_id stays a same-document parent link.
-- * entity_relationships keeps the projector's governance stances.
-- * Moves are upserted in place by write_discourse_moves, so their text can
--   change under the same id. Everything here therefore binds to content.
--
-- WHAT EVERY ROW IS BOUND TO
-- --------------------------
-- representation: SHA-256 of json_build_array(move_type, title, detail,
--   status)::text, computed in SQL only (api/discourse_links.REPR_SQL). The
--   text behind every hash a candidate, judgment or binding records is kept in
--   discourse_claim_representations, so a historical judgment can show what it
--   judged after the move changes.
-- binding: how a move is tied to the source version it was extracted from
--   (api/discourse_links.BINDING_KIND_SQL):
--     recorded_extraction       a row in discourse_move_extraction_bindings,
--                               written by the extraction run, with the source
--                               version it observed when it read its input
--     content_addressed_source  the move's source_rid is document:<sha256>:
--                               source attribution, not extraction provenance
--     inferred_timestamp        legacy Substack: the koi_memories row has not
--                               changed since deep extraction. Inferred
--     unknown
--   Only recorded_extraction whose association was captured with the input
--   can be admitted. The others stay candidates.
--
-- WHAT SIMILARITY IS ALLOWED TO DO
-- --------------------------------
-- Discovery writes only discourse_link_candidates (plus embeddings and
-- representations). Judgments need a server-minted authority.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS vector;

-- 1. Exact text behind every representation hash recorded below. Content-addressed.
CREATE TABLE IF NOT EXISTS discourse_claim_representations (
    representation_sha256  text PRIMARY KEY,
    move_id                uuid NOT NULL,
    move_type              text NOT NULL,
    title                  text NOT NULL,
    detail                 text,
    status                 text,
    first_seen_at          timestamptz NOT NULL DEFAULT now()
);

-- 2. Recorded extraction provenance, written by the extraction run that wrote the
--    move. One row per move: the latest extraction. Judgments keep their own
--    copy of what they relied on.
CREATE TABLE IF NOT EXISTS discourse_move_extraction_bindings (
    move_id                      uuid PRIMARY KEY REFERENCES session_discourse_moves(id) ON DELETE CASCADE,
    source_rid                   text NOT NULL,
    source_version_at_extraction text,          -- observed when the input was read; NULL if not established
    source_version_at_record     text,          -- observed when the binding was written
    association                  text NOT NULL CHECK (association IN (
                                     'captured_with_input',              -- same version before and after the read, unchanged at record
                                     'changed_while_reading',            -- the version moved during the read
                                     'source_changed_during_extraction', -- read at one version, recorded at another
                                     'unestablished')),                  -- no captured input: version unknown
    extraction_input_sha256      text NOT NULL, -- the exact chunk text the extractor read
    representation_sha256        text NOT NULL REFERENCES discourse_claim_representations(representation_sha256),
    run_id                       text NOT NULL,
    recorded_by                  text NOT NULL,
    recorded_at                  timestamptz NOT NULL DEFAULT now()
);

-- 3. Move embeddings, one row per (move, model).
CREATE TABLE IF NOT EXISTS discourse_move_embeddings (
    move_id                uuid NOT NULL REFERENCES session_discourse_moves(id) ON DELETE CASCADE,
    model                  text NOT NULL,
    representation_sha256  text NOT NULL,
    text_sha256            text NOT NULL,
    dimension              integer NOT NULL CHECK (dimension > 0),
    embedding              vector NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (move_id, model)
);

-- 4. Candidates. Keyed by the unordered pair with each end's representation and
--    bound version ('unknown' when NULL).
CREATE TABLE IF NOT EXISTS discourse_link_candidates (
    id                            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_key                 text NOT NULL UNIQUE,
    query_move_id                 uuid NOT NULL REFERENCES session_discourse_moves(id),
    target_move_id                uuid NOT NULL REFERENCES session_discourse_moves(id),
    query_source_rid              text NOT NULL,
    target_source_rid             text NOT NULL,
    query_representation_sha256   text NOT NULL REFERENCES discourse_claim_representations(representation_sha256),
    target_representation_sha256  text NOT NULL REFERENCES discourse_claim_representations(representation_sha256),
    query_binding_kind            text NOT NULL,
    target_binding_kind           text NOT NULL,
    query_bound_version           text,
    target_bound_version          text,
    method                        text NOT NULL,
    method_params                 jsonb NOT NULL,
    lexical_rank                  integer,
    lexical_score                 double precision,
    semantic_rank                 integer,
    semantic_score                double precision,
    fused_score                   double precision NOT NULL,
    status                        text NOT NULL DEFAULT 'candidate'
                                  CHECK (status IN ('candidate', 'admitted', 'declined')),
    discovered_by                 text NOT NULL,
    run_id                        text NOT NULL,
    discovered_at                 timestamptz NOT NULL DEFAULT now(),
    last_seen_at                  timestamptz NOT NULL DEFAULT now(),
    times_seen                    integer NOT NULL DEFAULT 1,
    -- A label suggestion by any caller, agents included. Caller-declared, never counted.
    proposed_relation             text,
    proposed_semantics            text,
    proposed_by                   text,
    proposal_rationale            text,
    reviewed_by                   text,
    review_authority              text,
    reviewed_at                   timestamptz,
    review_rationale              text,
    CHECK (query_move_id <> target_move_id),
    CHECK (query_source_rid <> target_source_rid),
    CHECK (lexical_rank IS NOT NULL OR semantic_rank IS NOT NULL),
    CHECK ((proposed_relation IS NULL) = (proposed_by IS NULL)),
    CHECK ((status = 'candidate') = (reviewed_by IS NULL)),
    CHECK ((reviewed_by IS NULL) = (review_authority IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_dlc_query_move  ON discourse_link_candidates (query_move_id);
CREATE INDEX IF NOT EXISTS idx_dlc_target_move ON discourse_link_candidates (target_move_id);
CREATE INDEX IF NOT EXISTS idx_dlc_status      ON discourse_link_candidates (status);
CREATE INDEX IF NOT EXISTS idx_dlc_run         ON discourse_link_candidates (run_id);

-- 5. Judgments. Bound to both ends' representation and recorded binding, per
--    actor. Append-only: withdrawal and correction are events (table 6).
CREATE TABLE IF NOT EXISTS discourse_relations (
    id                          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    from_move_id                uuid NOT NULL REFERENCES session_discourse_moves(id),
    to_move_id                  uuid NOT NULL REFERENCES session_discourse_moves(id),
    relation                    text NOT NULL,
    semantics                   text NOT NULL,
    from_representation_sha256  text NOT NULL REFERENCES discourse_claim_representations(representation_sha256),
    to_representation_sha256    text NOT NULL REFERENCES discourse_claim_representations(representation_sha256),
    from_binding_kind           text NOT NULL CHECK (from_binding_kind = 'recorded_extraction'),
    to_binding_kind             text NOT NULL CHECK (to_binding_kind = 'recorded_extraction'),
    from_bound_version          text NOT NULL,
    to_bound_version            text NOT NULL,
    origin                      text NOT NULL CHECK (origin IN ('authored', 'reviewed_candidate', 'correction')),
    candidate_id                uuid REFERENCES discourse_link_candidates(id),
    asserted_by                 text NOT NULL,
    authority_basis             text NOT NULL,
    rationale                   text NOT NULL,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    CHECK (from_move_id <> to_move_id),
    CHECK ((origin = 'reviewed_candidate') = (candidate_id IS NOT NULL)),
    CHECK (
           (semantics = 'epistemic-relation/v0' AND relation IN ('supports', 'challenges', 'qualifies', 'responds_to'))
        OR (semantics = 'identity/v0'           AND relation = 'equivalent_to')
        OR (semantics = 'lineage/v0'            AND relation = 'restates')
        OR (semantics = 'design-alignment/v0'   AND relation = 'aligns_with')
        OR (semantics = 'topical/v0'            AND relation = 'related_to')
    ),
    UNIQUE (from_move_id, to_move_id, relation, semantics,
            from_representation_sha256, to_representation_sha256,
            from_bound_version, to_bound_version, asserted_by)
);

CREATE INDEX IF NOT EXISTS idx_dr_from ON discourse_relations (from_move_id);
CREATE INDEX IF NOT EXISTS idx_dr_to   ON discourse_relations (to_move_id);

-- 6. Review events. A judgment has at most one terminal event (withdraw or
--    correct). Reopening a declined candidate copies its prior review here.
CREATE TABLE IF NOT EXISTS discourse_review_events (
    id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    target_kind              text NOT NULL CHECK (target_kind IN ('relation', 'candidate')),
    relation_id              uuid REFERENCES discourse_relations(id),
    candidate_id             uuid REFERENCES discourse_link_candidates(id),
    action                   text NOT NULL,
    actor                    text NOT NULL,
    authority_basis          text NOT NULL,
    rationale                text NOT NULL,
    replaced_by_relation_id  uuid REFERENCES discourse_relations(id),
    prior_state              jsonb,
    created_at               timestamptz NOT NULL DEFAULT now(),
    CHECK ((target_kind = 'relation') = (relation_id IS NOT NULL)),
    CHECK ((target_kind = 'candidate') = (candidate_id IS NOT NULL)),
    CHECK ((target_kind = 'relation' AND action IN ('withdraw', 'correct'))
        OR (target_kind = 'candidate' AND action = 'reopen')),
    CHECK ((action = 'correct') = (replaced_by_relation_id IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dre_one_terminal_event
    ON discourse_review_events (relation_id) WHERE target_kind = 'relation';
CREATE INDEX IF NOT EXISTS idx_dre_candidate ON discourse_review_events (candidate_id);

-- 7. Run ledger. A canary run is one transaction: it either commits with its
--    effects or leaves nothing but a 'failed' row. manifest_sha256 and
--    approval_scope are what was approved; the run refuses anything else.
CREATE TABLE IF NOT EXISTS discourse_link_runs (
    run_id           text PRIMARY KEY,
    kind             text NOT NULL CHECK (kind = 'canary'),
    manifest_sha256  text NOT NULL,
    approval_scope   jsonb NOT NULL,
    status           text NOT NULL CHECK (status IN ('completed', 'failed', 'rolled_back')),
    error            text,
    summary          jsonb,
    started_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

-- 8. Everything a run created or changed, written in the same savepoint as the
--    change itself. Rollback replays only these rows, and only while each
--    affected row is still exactly as the run left it.
CREATE TABLE IF NOT EXISTS discourse_link_run_effects (
    id                     bigserial PRIMARY KEY,
    run_id                 text NOT NULL REFERENCES discourse_link_runs(run_id),
    effect                 text NOT NULL CHECK (effect IN ('embedding_inserted', 'embedding_replaced',
                                                         'candidate_inserted', 'candidate_seen',
                                                         'representation_inserted')),
    move_id                uuid,
    model                  text,
    candidate_id           uuid,
    representation_sha256  text,
    after_state            jsonb,
    prior_state            jsonb,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlre_run ON discourse_link_run_effects (run_id);

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('133_discourse_link_candidates', 'v1_candidate_only_canary')
ON CONFLICT (migration_id) DO NOTHING;
