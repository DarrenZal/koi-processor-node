-- Migration 107: entity_closure
-- An append-only closure log for entity_registry + a derived projection onto
-- entity_registry.metadata->'closure'. Backs POST /entities/{uri}/close and
-- GET /entities/{uri}/closure (api/routers/admin_router.py).
--
-- WHY THIS EXISTS
-- ---------------
-- The entity surface had no closure operation. You could merge two entities,
-- retype one, or set its wallet address — but you could not mark a research
-- hypothesis REFUTED. Facts can be retracted (POST /knowledge/facts/{id}/retract);
-- claims-as-entities could not be closed at all. Consequence, observed live on
-- 2026-07-14: THREAD4 killed the RAGE holonomy hypothesis on 2026-06-29
-- (permutation p = 0.980); the refutation was ingested as *facts*, but the
-- hypothesis entity kept full retrieval standing, and on 2026-07-14 an agent
-- retrieved it as "the cleanest novelty bet" and proposed the dead experiment.
-- Nothing in the substrate could object.
--
-- THE SHAPE (and why it is this shape)
-- ------------------------------------
-- Asserting, relating and accumulating evidence are MONOTONE: they commute, they
-- are order-independent, they never retract a conclusion. Closure — retracting a
-- claim, adjudicating a contradiction, changing authority — is NON-MONOTONE, and
-- by CALM (Hellerstein 2010; Ameloot/Neven/Van den Bussche, JACM 2013) it admits
-- no coordination-free implementation. You cannot get it for free.
--
-- So we do not try. We split it:
--   * entity_closure_log — APPEND-ONLY. Never UPDATEd, never DELETEd. One row per
--     closure act, each an attributable, timestamped decision (who / when / on what
--     evidence / under what authority). The log stays monotone.
--   * entity_registry.metadata->'closure' — the DERIVED PROJECTION: the latest act
--     for that entity. Non-monotone, but a deterministic function of the log, so it
--     is always recomputable and never authoritative on its own.
-- Replay determinism without pretending to schedule invariance. The projection
-- exists only so that retrieval surfaces (unified_search, entity reads) carry an
-- entity's standing without a join.
--
-- REOPENING is just another act (status = OPEN). The log records the whole history;
-- nothing is ever overwritten. A closure is a decision, not a deletion.
--
-- See rage-research/notes/confluence-audit.md; migration 101 (entity_merge) is the
-- sibling non-monotone operation and the pattern this follows.

-- 1. The append-only log.
CREATE TABLE IF NOT EXISTS entity_closure_log (
    id            SERIAL PRIMARY KEY,
    entity_uri    text        NOT NULL REFERENCES entity_registry(fuseki_uri),
    status        text        NOT NULL,
    rationale     text        NOT NULL,
    scope         text,                  -- REQUIRED when status = 'SCOPED': the regime in which the claim still stands
    evidence_uris text[]      NOT NULL DEFAULT '{}',   -- what closed it (RIDs, URLs, file paths, commit SHAs)
    authority     text,                  -- under what authority the act was taken
    closed_by     text        NOT NULL,  -- the actor
    closed_at     timestamptz NOT NULL DEFAULT NOW(),

    -- The status lattice. Derived from the verdicts the research record actually
    -- produced, not invented: THREAD1 came back INCONCLUSIVE (the instrument could
    -- not test the claim), THREAD4 REFUTED on one regime while leaving another open
    -- (SCOPED), and kg-sheaf2 produced a real but narrow win (SCOPED).
    --
    -- SCOPED is the load-bearing one. A binary open/refuted forces a wrong answer
    -- for exactly the claims that matter: "dead on the concept graph, still open on
    -- federated cross-schema data" is neither OPEN nor REFUTED, and collapsing it to
    -- either loses the finding.
    CONSTRAINT entity_closure_status_valid CHECK (
        status IN ('OPEN', 'SUPPORTED', 'REFUTED', 'SCOPED', 'SUPERSEDED', 'INCONCLUSIVE')
    ),
    CONSTRAINT entity_closure_scope_required CHECK (
        status <> 'SCOPED' OR (scope IS NOT NULL AND scope <> '')
    )
);

CREATE INDEX IF NOT EXISTS idx_entity_closure_log_entity
    ON entity_closure_log (entity_uri, closed_at DESC);
CREATE INDEX IF NOT EXISTS idx_entity_closure_log_status
    ON entity_closure_log (status)
    WHERE status IN ('REFUTED', 'SCOPED', 'SUPERSEDED');

-- 2. Partial index for the proposal-time gate: "is there a closed hypothesis
--    overlapping what this agent is about to propose?" Only ever scans entities
--    that carry a closure projection.
CREATE INDEX IF NOT EXISTS idx_entity_registry_closure_status
    ON entity_registry ((metadata -> 'closure' ->> 'status'))
    WHERE metadata -> 'closure' IS NOT NULL;

COMMENT ON TABLE entity_closure_log IS
    'Append-only log of non-monotone closure acts on entities (REFUTED/SCOPED/SUPERSEDED/...). Never UPDATE or DELETE a row here — reopening is a new act with status=OPEN. entity_registry.metadata->''closure'' is the derived latest-act projection and is always recomputable from this table. See rage-research/notes/confluence-audit.md.';
COMMENT ON COLUMN entity_closure_log.scope IS
    'Required when status=SCOPED. The regime in which the claim STILL STANDS (e.g. "open for federated cross-schema data; dead on the single embedding-derived concept graph"). Without this, a scoped kill degrades to a wrong binary.';
COMMENT ON COLUMN entity_closure_log.evidence_uris IS
    'What closed it: RIDs, arXiv URLs, repo paths, commit SHAs. A closure act with no evidence is a bare assertion of authority — permitted, but legible as such.';
