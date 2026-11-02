-- =============================================================================
-- Migration 123: document history -- supersession journal + source claims
-- =============================================================================
-- Apply:    psql -d personal_koi -f migrations/123_document_history.sql
--           (NO -1: the file opens its own transaction, so SET LOCAL is in scope.
--            Optional: -v lock_timeout=10s)
-- Rollback: psql -d personal_koi -f migrations/123_document_history_down.sql
--
-- ADDITIVE ONLY toward existing tables. Two new tables, one new view, ONE new
-- nullable column (session_discourse_moves.superseded_at). No ALTER, no index and
-- no constraint on koi_memories or koi_memory_chunks. koi_memories.superseded_at
-- and .previous_version_id are PRE-EXISTING (migration 003's own CREATE TABLE) and
-- are neither created nor dropped here or by the down migration.
--
-- The ONE data mutation toward an existing table is leg E, which stamps the new
-- column on discourse moves whose parent document was ALREADY superseded before
-- this migration. It is undone by the down migration dropping the column.
--
-- NON-GOAL, stated so nobody re-derives it: this migration and every runtime path
-- it enables DELETE NOTHING FROM document_ingestion_log, in any policy. That table
-- has no foreign key to koi_memories (verify: pg_constraint where contype='f' and
-- conrelid='document_ingestion_log'::regclass -- compare against the same query for
-- koi_memory_chunks, which has one), so a hard delete manufactures no orphan, and
-- its content_hash is the only DB->archive pointer for a hard-deleted document
-- (verify: rows where content_hash IS DISTINCT FROM replace(document_rid,'document:','')
-- over document_rid LIKE 'document:%' -- compare against the unfiltered count of the
-- same set; koi_memories.content_hash is NULL for document rows, so it cannot serve).
-- =============================================================================
\set ON_ERROR_STOP on
\if :{?lock_timeout}
\else
  \set lock_timeout 3s
\endif

-- REPEATABLE READ is load-bearing, not hygiene: the claim backfill, the journal
-- adoption, leg E and the closing assertion must see ONE set of koi_memories rows.
-- Under READ COMMITTED a row committed by a concurrent ingest between them aborts
-- the migration at the assertion, non-deterministically. A serialization failure
-- (40001) or "tuple concurrently updated" here is a RETRY -- the file is idempotent.
BEGIN ISOLATION LEVEL REPEATABLE READ;

-- lock_timeout is MANDATORY, not decorative. The one contended statement is the
-- ALTER on session_discourse_moves (ACCESS EXCLUSIVE) -- the nightly deep extractor
-- and the website sensor both write that table. A timeout is a RETRY; it is never a
-- reason to raise the timeout in the file. No other migration in this repo sets one
-- (grep -l lock_timeout migrations/*.sql -> 0; positive control, same command for
-- CREATE TABLE -> many) -- this is the first, deliberately.
SET LOCAL lock_timeout = :'lock_timeout';

-- ---------------------------------------------------------------------------
-- (0) LEDGER FIRST. This is load-bearing ordering, not bookkeeping.
--     Every backfill leg and acceptance criterion AC1/I1 reads
--       (SELECT applied_at FROM koi_migrations WHERE migration_id='123_document_history')
--     as the snapshot boundary. Stamped last (inside or outside the transaction),
--     that subquery is empty, `created_at < NULL` is NULL, ZERO rows enter the
--     snapshot, the backfill claims nothing, AND AC1's anti-join also returns 0 and
--     certifies the empty result. Measured at the seam of the two source drafts:
--       cutoff_is_null | rows_selected_by_backfill | population_POSCTL
--       t              | 0                         | <non-zero>
--     Stamping first makes applied_at = the transaction-start instant, which is also
--     this transaction's snapshot, so the boundary and the visible rows agree.
--
--     checksum is a SEMANTIC TAG, matching every migration from 114 onward, and the
--     INSERT lives inside the migration file exactly as 118 does:
--       migrations/118_entity_merge_reversal.sql, last statement, verbatim --
--       INSERT INTO koi_migrations (migration_id, checksum)
--       VALUES ('118_entity_merge_reversal', 'v1_merge_reversal')
--       ON CONFLICT (migration_id) DO NOTHING;
--     There is NO `CK=$(shasum -a 256 ...)` recipe and no post-COMMIT UPDATE.
-- ---------------------------------------------------------------------------
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('123_document_history', 'v1_document_history')
ON CONFLICT (migration_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- (1) Ownership + retention policy, one row per (source_kind, source_id, source_url).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS document_source_claim (
    source_kind      text        NOT NULL,
    source_id        text        NOT NULL,
    source_url       text        NOT NULL,
    -- NOT NULL, and never nulled on removal: a retired or released claim keeps
    -- pointing at the last known rid, because THAT POINTER IS THE TOMBSTONE and the
    -- only surviving record of which bytes the source held. The earlier
    -- release-by-NULLing design (document_rid NULL, CHECK ((document_rid IS NULL) =
    -- (released_at IS NOT NULL))) is incompatible with it: under this DDL that
    -- UPDATE raises a NOT NULL violation and aborts the whole apply transaction,
    -- rolling back its own state='applied' write -- the retry-forever shape this
    -- design exists to prevent. A removal is a RETIRE, never a release.
    document_rid     text        NOT NULL,
    content_hash     text,
    history_policy   text        NOT NULL DEFAULT 'keep'
                       CHECK (history_policy IN ('keep','drop')),
    claim_state      text        NOT NULL DEFAULT 'live'
                       CHECK (claim_state IN ('live','retired','released')),
    first_claimed_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at     timestamptz NOT NULL DEFAULT now(),
    retired_at       timestamptz,
    released_at      timestamptz,
    PRIMARY KEY (source_kind, source_id, source_url),
    CONSTRAINT chk_dsc_state CHECK (
         (claim_state = 'live'     AND retired_at IS NULL     AND released_at IS NULL)
      OR (claim_state = 'retired'  AND retired_at IS NOT NULL AND released_at IS NULL)
      OR (claim_state = 'released' AND released_at IS NOT NULL))
);

-- Serves G3, the cross-source destructive gate -- the ONE hot query on this table:
--   SELECT count(*) FROM document_source_claim WHERE document_rid = $pred AND claim_state='live'
-- Partial on the hot predicate: a released or retired claim is dead weight there, and
-- (document_rid) non-partial was rejected because it serves no hot query this does not.
-- The non-hot readers (the D1/D2 drift lines, /versions' claim lookup) scan a table
-- whose row count is bounded by the claim population; widening this index is a
-- separate migration if that ever stops being true.
CREATE INDEX IF NOT EXISTS idx_dsc_rid_live
    ON document_source_claim (document_rid) WHERE claim_state = 'live';

-- Serves also_at_url on GET /documents/{rid}/versions:
--   SELECT ... FROM document_source_claim c1 JOIN document_source_claim c2
--    ON c2.source_url = c1.source_url WHERE c1.document_rid = $rid
-- This is the index the legacy source_url projection (leg B) exists to feed: projecting
-- legacy source_url = rid instead makes every legacy duplicate-URL group invisible to it.
-- HONEST PLAN NOTE: at the size the backfill leaves (and with no ANALYZE yet) the planner
-- chooses a Hash Join over two Seq Scans for that statement -- measured by EXPLAIN inside
-- the dry-run transaction. The index is sized for an unbounded, per-request path on a
-- growing table, not for today's row count. Re-measure with EXPLAIN; never quote a cost.
CREATE INDEX IF NOT EXISTS idx_dsc_url
    ON document_source_claim (source_url);

-- NOT CREATED, deliberately: idx_dsc_site (source_kind, source_id) is strictly
-- redundant -- PRIMARY KEY (source_kind, source_id, source_url) is a btree whose
-- leading prefix is exactly that.

COMMENT ON TABLE document_source_claim IS $c$
Ownership + retention policy per (source_kind, source_id, source_url).

A claim row means "this source's CURRENT version for this URL". document_rid is NOT NULL and is
never cleared: on removal the claim is RETIRED with the rid still pinned, because that pointer is
the tombstone -- the only record of which bytes the source held once the row may be gone.

history_policy is STICKY: written on first claim, NEVER updated by an ingest, changed only by
POST /history/policy. That absence from the ingest's ON CONFLICT ... DO UPDATE SET list IS the
stickiness, and it is what makes a keep_history true->false flip in YAML non-retroactive BY
CONSTRUCTION.

claim_state is a SOURCE-SIDE fact only: retired = the source removed the page; released = the
operator stopped maintaining the source. Whether the claim's document_rid names a superseded row is
COMPUTED by joining koi_memories (drift lines D1/D2), never stored here -- storing it makes that
drift check zero by construction, i.e. an instrument that can never read anything.

Absence of a claim row means UNKNOWN OWNER, never "unowned": the supersede/delete guard fails CLOSED
on an empty claim set.

MANY claims may share one document_rid ON PURPOSE: document_rid = sha256(converted markdown), so
identical bytes reached from two sources are ONE koi_memories row with TWO owners. This table is the
global answer to "is any OTHER source still pointing at these bytes?" -- which the sensor's per-site
scan (website_sensor.py:rid_shared_by_other_entries, "Other manifest entries whose live or previous
version is this rid ... Retiring it for one key would silently retire it for the others")
structurally cannot see.

Deliberately NO FK to koi_memories(rid): the claim must outlive a history_policy='drop' hard delete,
for the same reason document_supersession has none.
$c$;

COMMENT ON COLUMN document_source_claim.history_policy IS
 'keep | drop. STICKY: set at first claim, changed only by POST /history/policy. Every row written '
 'by the 123 backfill is keep: they predate keep_history, rows that predate the website sensor have '
 'no git-archive commit, and the drop path is a hard DELETE of the row and its chunks. A legacy '
 'claim never authorizes a destructive act regardless of this value.';

COMMENT ON COLUMN document_source_claim.claim_state IS
 'live | retired | released. SOURCE-SIDE fact only. retired: the source removed the page (rid stays '
 'pinned as the tombstone). released: the operator stopped maintaining the source; released claims '
 'do NOT gate INV-3, and koi-history release --confirm must say so before it runs. Only '
 'koi-history disown (legacy kinds only) ever deletes a claim row.';

-- ---------------------------------------------------------------------------
-- (2) Write-ahead supersession journal AND version chain, append-only.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS document_supersession (
    id              bigserial   PRIMARY KEY,
    chain_id        uuid        NOT NULL,
    seq             integer,                -- allocated in the apply txn; NULL until terminal-applied
    run_id          text,
    source_kind     text        NOT NULL,
    source_id       text        NOT NULL,
    source_url      text        NOT NULL,
    predecessor_rid text,
    successor_rid   text,                   -- NULL at intent time and for removals
    reason          text        NOT NULL
                      CHECK (reason IN ('changed','removed','revive','manual','legacy')),
    history_policy  text        NOT NULL CHECK (history_policy IN ('keep','drop')),
    -- 'applied_shared' is a TERMINAL SUCCESS, not a refusal: the successor is ingested
    -- --staged, i.e. born with superseded_at set, so refusing a shared rid leaves the new
    -- version PERMANENTLY INVISIBLE while reconcile retries forever. The shared case runs
    -- the non-destructive half (reveal successor, advance claim, allocate seq, write the
    -- chain edge) and skips every destructive statement.
    state           text        NOT NULL DEFAULT 'pending'
                      CHECK (state IN ('pending','applied','applied_shared','refused','failed','cancelled')),
    refusal         text,
    attempts        integer     NOT NULL DEFAULT 0,   -- intent re-observations (nightly)
    apply_attempts  integer     NOT NULL DEFAULT 0,   -- failed/refused APPLY passes only
    last_error      text,
    archive_commit  text,
    effects         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    intent_at       timestamptz NOT NULL DEFAULT now(),
    applied_at      timestamptz,
    CONSTRAINT chk_dsup_ends     CHECK (predecessor_rid IS NOT NULL OR successor_rid IS NOT NULL),
    -- widened with the state CHECK: a shared-claim edge is a real chain position -- "B
    -- revises A" is true regardless of who else holds A -- so it must be allowed a seq.
    -- Left at (state = 'applied') = (seq IS NOT NULL) this constraint REJECTS the seq
    -- allocation the applied_shared path performs.
    CONSTRAINT chk_dsup_seq      CHECK ((state IN ('applied','applied_shared')) = (seq IS NOT NULL)),
    CONSTRAINT chk_dsup_refusal  CHECK (state <> 'refused' OR refusal IS NOT NULL),
    CONSTRAINT chk_dsup_selfedge CHECK (successor_rid IS NULL OR successor_rid <> predecessor_rid)
);

-- Correctness index: allocates seq exactly once per chain. NOT partial -- in PG 14
-- NULLs are DISTINCT in a unique btree (NULLS NOT DISTINCT arrived in PG 15 and is
-- opt-in), so unlimited (chain_id, NULL) pending rows coexist, and the SAME index
-- also serves the flat /versions walk `WHERE chain_id = $c ORDER BY seq` over every
-- state. One index instead of two; idx_dsup_chain (chain_id) is therefore NOT created.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dsup_chain_seq
    ON document_supersession (chain_id, seq);

-- THE idempotency mechanism for POST /history/intent: at most one OPEN journal row
-- per (predecessor, source). 'refused' is inside the predicate deliberately, so a
-- nightly re-run returns the existing refusal instead of stacking a new row every
-- morning; POST /history/resolve is what frees the slot. predecessor_rid IS NOT NULL
-- is in the predicate, which is why a NULL-predecessor intent is a 400: it would have
-- no arbiter and would silently accumulate duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dsup_open_pred
    ON document_supersession (predecessor_rid, source_kind, source_id, source_url)
    WHERE state IN ('pending','failed','refused') AND predecessor_rid IS NOT NULL;

-- /versions entry lookup from any member rid, over ALL states (uq_dsup_open_pred is
-- open-only and cannot serve it), and the two probes of the server-side chain_id
-- resolver: probe (1) reads successor_rid, probe (2) reads predecessor_rid.
CREATE INDEX IF NOT EXISTS idx_dsup_pred
    ON document_supersession (predecessor_rid) WHERE predecessor_rid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dsup_succ
    ON document_supersession (successor_rid)   WHERE successor_rid IS NOT NULL;

-- reconcile work queue. The predicate EXCLUDES 'refused': a refusal is a surfaced
-- condition, not a stuck one, and must not be retried automatically. Keeps the drain
-- O(open) while the journal itself is append-only and unbounded.
CREATE INDEX IF NOT EXISTS idx_dsup_open
    ON document_supersession (intent_at) WHERE state IN ('pending','failed');

COMMENT ON TABLE document_supersession IS $c$
Write-ahead supersession journal + version chain. The intent row is written BEFORE the ingest
subprocess spawns, so a SIGKILL / reboot / launchd bootout leaves a resumable to-do naming both rids
instead of an invisible inconsistency.

A row is an EDGE (predecessor -> successor), not a version. seq numbers the edges of a chain and is
allocated in the apply transaction. GET /documents/{rid}/versions materialises versions as
edges[0].predecessor_rid followed by each edge's successor_rid, ordered by seq -- a FLAT walk, no
recursion: a bidirectional recursive CTE with two self-references is a hard Postgres error, and
carrying a depth column defeats the RECURSIVE UNION's dedup (measured: a linear acyclic 3-node chain
emitted two orders of magnitude more rows than it has distinct rids).

THE CHAIN IS HELD TOGETHER BY (chain_id, seq), NOT BY successor_rid. That is deliberate: an adopted
legacy edge whose successor cannot be resolved would otherwise fragment its page's history into
singletons, silently. chain_id for adopted edges is DERIVED from the source triple
(md5(kind||'|'||id||'|'||url)::uuid) so every edge of one page lands in one chain and a row adopted
later by reconcile joins the chain the migration started instead of forking a parallel one.

chk_dsup_selfedge is not cosmetic. It is the replacement for the sensor guard that plan step 15
deletes -- website_sensor.py:_db_retire, "if new_rid and new_rid != old_rid:". Without it an intent
whose successor resolves to its own predecessor stamps superseded_at on a row, nulls it again, and
then DELETEs that live row's chunks, with no error anywhere.

NO FK to koi_memories: the row must outlive a history_policy='drop' hard delete -- that surviving row
IS the tombstone and the audit trail.

effects records every mutation the apply made so a revert is mechanical. effects is MERGED with `||`,
which is a SHALLOW merge (verify: '{"a":1,"n":{"x":1,"y":2}}'::jsonb || '{"n":{"y":9}}'::jsonb drops
"x"). `||` therefore protects NOTHING; what makes a replay a no-op is the state guard
`WHERE id = $1 AND state IN ('pending','failed')` repeated in the write. `||` is retained only so
writers with DISJOINT TOP-LEVEL KEYS compose.

attempts and apply_attempts are SEPARATE on purpose, and MAX_ATTEMPTS keys off apply_attempts ONLY.
attempts counts intent re-observations (the nightly sensor re-reporting an unchanged open row);
apply_attempts counts only failed or refused apply passes. One counter cannot do both: three quiet
mornings with no apply failure at all would trip a max-apply-attempts threshold and refuse a
perfectly healthy intent.

There is deliberately NO 'reverted' state in v1: no endpoint writes one. Migration 118's own comment
records why -- "Existed unwritten from migration 101 until 2026-09-02". Adding it later is one
ALTER ... DROP/ADD CONSTRAINT (Parking Lot).

koi_memories.previous_version_id and .version are NOT written by this design as chain state: NO
ACTION FK, unindexed, non-monotonic after un-retire, three dead readers. previous_version_id is READ
once, by leg D's successor probe (1), because the old sensor's retire path is the only thing that
ever wrote it; and it is CLEARED by the history_policy='drop' path, which would otherwise raise
23503 on the row delete.
$c$;

-- ---------------------------------------------------------------------------
-- (3) The source-triple projection, in ONE place.
--     The backfill legs, POST /history/reconcile's orphan adoption + claim sweep,
--     and koi-history status --invariants ALL derive (source_kind, source_id,
--     source_url) from a document row. Three hand-copies are three chances to drift,
--     and a drifted copy mints claims under a triple the sensor's next intent can
--     never match -- a permanent, silent UNKNOWN_OWNER.
--
--     ANTI-COERCION (this is the point of the `managed` column): a row whose
--     retrieval_method says website-sensor but whose site id is empty, or which
--     carries no URL at all, has NO DERIVABLE MANAGED IDENTITY. The rejected form
--     COALESCEd those to m.source_sensor and to m.rid, which mints a VALID-LOOKING
--     WRONG claim with no flag. Here such a row is flagged malformed_managed,
--     projected as a non-authorizing legacy claim keyed on its own rid, counted by
--     invariant I9, and refused by the pre-backfill guard below so no operator can
--     ship one unknowingly.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW document_source_projection AS
SELECT m.id, m.rid, m.created_at, m.updated_at, m.superseded_at,
       m.metadata->>'content_hash' AS content_hash,
       (m.metadata->>'retrieval_method' LIKE 'website-sensor:%')          AS managed_claimed,
       (m.metadata->>'retrieval_method' LIKE 'website-sensor:%'
        AND NULLIF(split_part(m.metadata->>'retrieval_method', ':', 2), '') IS NOT NULL
        AND COALESCE(NULLIF(m.metadata->>'source_url',''),
                     NULLIF(m.metadata->>'url','')) IS NOT NULL)          AS managed,
       (m.metadata->>'retrieval_method' LIKE 'website-sensor:%'
        AND NOT (NULLIF(split_part(m.metadata->>'retrieval_method', ':', 2), '') IS NOT NULL
                 AND COALESCE(NULLIF(m.metadata->>'source_url',''),
                              NULLIF(m.metadata->>'url','')) IS NOT NULL)) AS malformed_managed,
       CASE WHEN m.metadata->>'retrieval_method' LIKE 'website-sensor:%'
             AND NULLIF(split_part(m.metadata->>'retrieval_method', ':', 2), '') IS NOT NULL
             AND COALESCE(NULLIF(m.metadata->>'source_url',''),
                          NULLIF(m.metadata->>'url','')) IS NOT NULL
            THEN 'website-sensor' ELSE 'legacy' END                       AS source_kind,
       CASE WHEN m.metadata->>'retrieval_method' LIKE 'website-sensor:%'
             AND NULLIF(split_part(m.metadata->>'retrieval_method', ':', 2), '') IS NOT NULL
             AND COALESCE(NULLIF(m.metadata->>'source_url',''),
                          NULLIF(m.metadata->>'url','')) IS NOT NULL
            THEN split_part(m.metadata->>'retrieval_method', ':', 2)
            ELSE m.rid END                                                AS source_id,
       COALESCE(NULLIF(m.metadata->>'source_url',''),
                NULLIF(m.metadata->>'url',''),
                m.rid)                                                    AS source_url
  FROM koi_memories m
 WHERE m.rid LIKE 'document:%';

COMMENT ON VIEW document_source_projection IS
 'The (source_kind, source_id, source_url) derivation for document rows, in ONE place: the 123 '
 'backfill legs, POST /history/reconcile''s adoption and claim sweep, and koi-history status '
 '--invariants all read it. A managed (website-sensor) identity is projected ONLY when the site id '
 'and a URL are both derivable; anything else is legacy, keyed on the rid, which UNIQUE(rid) makes '
 'collision-free, and is flagged malformed_managed when it claimed to be managed. source_url falls '
 'back to the rid ONLY for a legacy row with no URL at all -- the existing production convention '
 '(ingest_document.ingest_path, verbatim "source_document = source_url or document_rid"). For a '
 'managed row that fallback is unreachable by construction, which is what stops a claim being '
 'minted at a source_url the sensor''s next intent can never match.';

-- ---------------------------------------------------------------------------
-- (4) PRE-BACKFILL GUARD. Fails fast and names the rows, before any claim is written.
--     Both arms are anti-joins with a paired positive control; neither is a literal.
-- ---------------------------------------------------------------------------
DO $$
DECLARE problems text;
BEGIN
  SELECT string_agg(x, '; ') INTO problems FROM (
    -- the ledger row must exist BEFORE the legs run: it IS the snapshot boundary (F4)
    SELECT 'koi_migrations row for 123_document_history is missing: the snapshot '
           'boundary would be NULL and every leg would select zero rows' AS x
     WHERE NOT EXISTS (SELECT 1 FROM koi_migrations WHERE migration_id='123_document_history')
    UNION ALL
    -- F3: a row that claims to be sensor-managed but whose identity is underivable
    SELECT format('malformed website-sensor rows (%s): %s -- fix metadata.retrieval_method / '
                  'metadata.source_url on these rids, or supersede them, then re-run this file '
                  'unchanged. They must NOT be silently claimed under a coerced identity.',
                  cnt, sample)
      FROM (SELECT count(*) AS cnt,
                   string_agg(left(rid,26), ',' ORDER BY rid) FILTER (WHERE rn <= 5) AS sample
              FROM (SELECT p.rid, row_number() OVER (ORDER BY p.rid) AS rn
                      FROM document_source_projection p WHERE p.malformed_managed) z) q
     WHERE cnt > 0
  ) t;
  IF problems IS NOT NULL THEN
    RAISE EXCEPTION 'migration 123 preflight failed: %', problems;
  END IF;
  -- POSITIVE CONTROL for the arm above: it must have had rows to examine at all.
  IF NOT EXISTS (SELECT 1 FROM document_source_projection) THEN
    RAISE WARNING 'migration 123 preflight: document_source_projection is EMPTY -- the '
                  'malformed-managed check passed vacuously';
  END IF;
  RAISE NOTICE 'migration 123 preflight PASSED';
END $$;

-- ---------------------------------------------------------------------------
-- (5) Snapshot. One temp table, so every leg and every pass condition below sees the
--     SAME row set, and the boundary is the ledger row this file wrote.
--     COALESCE, not a bare `<`: koi_memories.created_at is NULLABLE (\d koi_memories
--     shows no NOT NULL), and a NULL would be silently dropped from the snapshot
--     while AC1/I1 still counted the row.
-- ---------------------------------------------------------------------------
CREATE TEMP TABLE _mig123_doc ON COMMIT DROP AS
SELECT p.*
  FROM document_source_projection p
 WHERE COALESCE(p.created_at, '-infinity'::timestamptz)
       < (SELECT applied_at FROM koi_migrations WHERE migration_id = '123_document_history');

-- ---------------------------------------------------------------------------
-- (6) Leg A -- managed winner: exactly one claim per (site_id, source_url).
--     DISTINCT ON + a TOTAL ORDER makes the winner DEFINED. Without it the surviving
--     row of a same-triple pair is scan-order dependent, and a claim that lands on the
--     wrong row silently poisons both the ingest-side cutover and the cross-source
--     guard with no constraint to catch it. Tie-break: live over superseded, then
--     newest, then rid (unique, therefore total).
--     The loser is NOT dropped: leg B demotes it to a legacy claim keyed on its rid,
--     so AC1 stays 0, the loser is protected from every destructive act (a legacy
--     claim is non-authorizing), and it surfaces as drift line D3 instead of vanishing.
-- ---------------------------------------------------------------------------
INSERT INTO document_source_claim
    (source_kind, source_id, source_url, document_rid, content_hash,
     history_policy, claim_state, first_claimed_at, last_seen_at)
SELECT DISTINCT ON (d.source_id, d.source_url)
       'website-sensor', d.source_id, d.source_url, d.rid, d.content_hash,
       'keep',      -- conservative: 'keep' can never authorise a delete. These rows
                    -- predate keep_history, so no promise was made; the fail-safe
                    -- reading of "no promise" is do not destroy.
       'live',      -- claim_state is a SOURCE-SIDE fact and the backfill observed
                    -- neither a removal nor a release. Deriving it from superseded_at
                    -- would make drift line D1/D2 zero by construction.
       d.created_at, COALESCE(d.updated_at, d.created_at)
  FROM _mig123_doc d
 WHERE d.managed
 ORDER BY d.source_id, d.source_url, (d.superseded_at IS NULL) DESC, d.created_at DESC, d.rid DESC
ON CONFLICT (source_kind, source_id, source_url) DO NOTHING;

-- ---------------------------------------------------------------------------
-- (7) Leg B -- legacy rows AND managed losers. source_id = rid => unique by construction.
--     source_url is the row's REAL url (falling back to the rid only when it has none),
--     NOT the rid: also_at_url and idx_dsc_url exist to surface the legacy
--     duplicate-source_url groups, and projecting legacy source_url = rid makes every
--     one of them invisible. Verify with the two projections side by side:
--       WITH proj AS (SELECT <this projection> su, rid FROM koi_memories WHERE rid LIKE 'document:%')
--       SELECT count(*) FROM (SELECT su FROM proj GROUP BY su HAVING count(DISTINCT rid) > 1) z;
--     against the same query with su := rid. This projection must see strictly more groups.
-- ---------------------------------------------------------------------------
INSERT INTO document_source_claim
    (source_kind, source_id, source_url, document_rid, content_hash,
     history_policy, claim_state, first_claimed_at, last_seen_at)
SELECT 'legacy', d.rid, d.source_url, d.rid, d.content_hash,
       'keep', 'live', d.created_at, COALESCE(d.updated_at, d.created_at)
  FROM _mig123_doc d
 WHERE NOT EXISTS (SELECT 1 FROM document_source_claim c WHERE c.document_rid = d.rid)
ON CONFLICT (source_kind, source_id, source_url) DO NOTHING;

-- ---------------------------------------------------------------------------
-- (8) Leg C -- second identity for bytes on record under two URLs (the live d8 shape,
--     produced because extract_deep_document's ON CONFLICT (document_rid) DO UPDATE SET
--     omits source_url while upsert_document_memory's conflict arm replaces metadata
--     wholesale: first writer wins the log, last writer wins the memory row).
--     Recording BOTH identities is what makes the cross-source guard fail closed on
--     those rids instead of silently acting under one of the two.
-- ---------------------------------------------------------------------------
INSERT INTO document_source_claim
    (source_kind, source_id, source_url, document_rid, content_hash,
     history_policy, claim_state, first_claimed_at, last_seen_at)
SELECT 'legacy', d.rid, l.source_url, d.rid, d.content_hash,
       'keep', 'live', d.created_at, COALESCE(d.updated_at, d.created_at)
  FROM _mig123_doc d
  JOIN document_ingestion_log l ON l.document_rid = d.rid
 WHERE l.source_url IS NOT NULL
   AND l.source_url NOT LIKE 'document:%'
   AND l.source_url IS DISTINCT FROM d.source_url
ON CONFLICT (source_kind, source_id, source_url) DO NOTHING;

-- ---------------------------------------------------------------------------
-- (9) Leg D -- adopt pre-existing superseded rows into the journal.
--     NOT hypothetical: the sensor's own _db_retire has already executed against live
--     data, leaving both shapes -- a removal (successor NULL) and a CHANGE with a live
--     successor. Without this, the invariant "every superseded row is named on a journal
--     row" fails the moment 123 commits.
--     reason='legacy' is honest: this system did not perform the supersession, and
--     `reason <> 'legacy'` is the exact predicate for "supersessions this design did".
--
--     SUCCESSOR RESOLUTION, two probes, in order:
--       (1) koi_memories.previous_version_id, which the old sensor wrote
--           (website_sensor.py:_db_retire, "SET previous_version_id = $2,
--           version = COALESCE($3, 1) + 1"). EXACT, per edge, survives multi-hop chains.
--       (2) the live claim for the same triple, used ONLY when this is the NEWEST
--           superseded row for that triple (otherwise A would be linked to C across B).
--     If neither resolves, successor_rid stays NULL and effects.successor_source records
--     'unresolved'. That is NOT a benign default: the chain is held by (chain_id, seq),
--     which is derived from the triple, so the page's history stays in ONE chain, and
--     invariant I10 counts the unresolved edges so they are visible rather than silent.
--
--     STATE: 'applied'. The supersession is a completed fact -- the row is stamped and
--     de-chunked already. Writing 'pending' would assert the opposite AND put a legacy
--     row into reconcile's drain, where the apply would either refuse it or act on an
--     already-superseded predecessor. reconcile's drain therefore also excludes
--     reason='legacy' rows by contract.
--
--     intent_at = now(), NOT the historical stamp: AC3/AC4/I5/I6 are AGE BUDGETS on open
--     rows, and back-dating starts them before the journal existed. The historical
--     instant is preserved twice -- as applied_at, and in effects.superseded_at_pre_123.
--     applied_at < intent_at on these rows is therefore expected and correct.
-- ---------------------------------------------------------------------------
WITH dead AS (
  SELECT d.* FROM _mig123_doc d WHERE d.superseded_at IS NOT NULL
), todo AS (
  SELECT d.*,
         md5(d.source_kind || '|' || d.source_id || '|' || d.source_url)::uuid AS chain_id,
         (SELECT k.rid FROM koi_memories k
           WHERE k.previous_version_id = d.id ORDER BY k.created_at, k.rid LIMIT 1) AS succ_by_prev,
         (SELECT c.document_rid FROM document_source_claim c
           WHERE (c.source_kind, c.source_id, c.source_url) = (d.source_kind, d.source_id, d.source_url)
             AND c.document_rid IS DISTINCT FROM d.rid
             AND NOT EXISTS (SELECT 1 FROM dead d2
                              WHERE (d2.source_kind, d2.source_id, d2.source_url)
                                    = (d.source_kind, d.source_id, d.source_url)
                                AND d2.superseded_at > d.superseded_at)) AS succ_by_claim
    FROM dead d
   WHERE NOT EXISTS (SELECT 1 FROM document_supersession x WHERE x.predecessor_rid = d.rid)
)
INSERT INTO document_supersession
    (chain_id, seq, source_kind, source_id, source_url, predecessor_rid, successor_rid,
     reason, history_policy, state, intent_at, applied_at, effects)
SELECT t.chain_id,
       COALESCE((SELECT max(x.seq) FROM document_supersession x WHERE x.chain_id = t.chain_id), 0)
         + row_number() OVER (PARTITION BY t.chain_id
                              ORDER BY t.superseded_at, t.created_at, t.rid),
       t.source_kind, t.source_id, t.source_url, t.rid,
       NULLIF(COALESCE(t.succ_by_prev, t.succ_by_claim), t.rid),  -- chk_dsup_selfedge belt-and-braces
       'legacy',
       COALESCE((SELECT c.history_policy FROM document_source_claim c
                  WHERE (c.source_kind, c.source_id, c.source_url)
                        = (t.source_kind, t.source_id, t.source_url)), 'keep'),
       'applied',
       now(), t.superseded_at,
       jsonb_build_object(
         'adopted_by_migration', '123',
         'note', 'superseded before migration 123; no journal row existed',
         'superseded_at_pre_123', t.superseded_at,
         'successor_source', CASE WHEN t.succ_by_prev IS NOT NULL AND t.succ_by_claim IS NOT NULL
                                    AND t.succ_by_prev = t.succ_by_claim THEN 'previous_version_id+claim'
                                  WHEN t.succ_by_prev  IS NOT NULL THEN 'previous_version_id'
                                  WHEN t.succ_by_claim IS NOT NULL THEN 'claim'
                                  ELSE 'unresolved' END,
         'previous_version_id_successor', t.succ_by_prev,
         'chunks_present_at_backfill',
            (SELECT count(*) FROM koi_memory_chunks kc WHERE kc.document_rid = t.rid))
  FROM todo t;

-- ---------------------------------------------------------------------------
-- (10) The one nullable column. LAST of the DDL, so the ACCESS EXCLUSIVE lock on
--      session_discourse_moves is held from here to COMMIT (~ms) and not across two
--      CREATE TABLEs, seven indexes, a view and four backfill legs.
--      Nullable, no default => catalog-only, no table rewrite (PG 11+). Same shape as
--      migration 103's own "ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL
--      DEFAULT 'session'", which applied cleanly on this database.
--
--      WHY A COLUMN AND NOT A DERIVED ANTI-JOIN against koi_memories.superseded_at:
--      the derived form is wrong on the drop path and fails OPEN. session_discourse_moves
--      has exactly two foreign keys -- to knowledge_episodes and to itself (verify:
--      pg_constraint contype='f' conrelid='session_discourse_moves'::regclass) -- and
--      NONE to koi_memories. So after a history_policy='drop' hard delete there is no
--      koi_memories row, NOT EXISTS(...) is TRUE, and every discourse move of the deleted
--      document stays visible FOREVER, with the unknown state (parent row gone) silently
--      coerced to the benign one (not superseded). A stored stamp survives the delete.
--
--      WHY NOT reuse session_discourse_moves.status: chk_discourse_status_by_source is
--      source_type-partitioned and admits no retire state for documents (verify:
--      pg_get_constraintdef of that constraint -- the 'document' arm lists
--      asserted/supported/contested/speculative/open/deferred).
--      WHY NOT DELETE the moves: unrecoverable.
--      The column is needed at all because move ids are content-derived
--      (extract_deep_documents.write_discourse_moves mints uuid5 over document_rid), so a
--      new version writes a DISJOINT id set and the old moves answer as current forever.
-- ---------------------------------------------------------------------------
ALTER TABLE session_discourse_moves ADD COLUMN IF NOT EXISTS superseded_at timestamptz;

COMMENT ON COLUMN session_discourse_moves.superseded_at IS
 'Set by POST /history/apply on the predecessor rid''s moves; CLEARED by the same endpoint''s '
 'revive/reveal arm and by ingest_document''s ON CONFLICT reveal, which must travel together with '
 'koi_memories.superseded_at = NULL (invariant I11 catches the drift). Stamped for pre-existing '
 'superseded rows by migration 123 leg E. Chosen over a status-CHECK migration and over a DELETE; '
 'chosen over deriving the predicate from koi_memories because that form fails OPEN once the '
 'parent row is hard-deleted under history_policy=drop.';

-- NO index is created on this column. Measured, not assumed:
--   * idx_discourse_source_rid (source_rid) WHERE source_rid IS NOT NULL already exists and serves
--     the per-rid path: EXPLAIN on the heaviest source_rid -> "Index Scan using
--     idx_discourse_source_rid". Re-measure with the query; never quote a count in this file.
--   * the discourse LIST path (knowledge_router.py, "FROM session_discourse_moves WHERE {where}
--     ORDER BY {order_by} LIMIT") filters source_type and orders by created_at DESC. An index on
--     source_type EXISTS (idx_discourse_source_type) and the planner correctly ignores it -- the
--     predicate matches almost every row -- so the plan is Seq Scan + top-N sort. No index leading
--     with source_rid can serve that query at all.

-- ---------------------------------------------------------------------------
-- (11) Leg E -- the debt the COLUMN design owes and the derived design got for free.
--      Rows superseded BEFORE 123 have unstamped moves, so the read filter (which is now
--      a column read) would not hide them: a silent regression against the rejected
--      derived predicate. Stamp them with the HISTORICAL instant, not now().
--      Must follow the ALTER; is undone by the down migration dropping the column.
-- ---------------------------------------------------------------------------
UPDATE session_discourse_moves d
   SET superseded_at = m.superseded_at
  FROM koi_memories m
 WHERE d.source_type = 'document'
   AND d.source_rid = m.rid
   AND m.superseded_at IS NOT NULL
   AND d.superseded_at IS NULL;

-- ---------------------------------------------------------------------------
-- (12) Closing assertion. Three jobs: defeat the CREATE TABLE IF NOT EXISTS trap (a
--      re-run against a table created from an EARLIER draft silently does nothing),
--      assert this migration's own acceptance criteria BEFORE COMMIT rather than
--      discovering them afterwards, and carry a PAIRED POSITIVE CONTROL for every
--      anti-join so a zero cannot mean "the backfill wrote nothing".
--      Offending rids are named, bounded to a sample so the message stays readable.
-- ---------------------------------------------------------------------------
DO $$
DECLARE missing text;
        claims_written bigint;
        rows_to_claim  bigint;
BEGIN
  SELECT count(*) INTO claims_written FROM document_source_claim;
  SELECT count(*) INTO rows_to_claim  FROM _mig123_doc;

  SELECT string_agg(x, '; ') INTO missing FROM (
    SELECT 'session_discourse_moves.superseded_at' AS x WHERE NOT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='session_discourse_moves' AND column_name='superseded_at')
    UNION ALL SELECT 'document_source_claim.claim_state' WHERE NOT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='document_source_claim' AND column_name='claim_state')
    UNION ALL SELECT 'document_source_claim.document_rid must be NOT NULL' WHERE EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='document_source_claim' AND column_name='document_rid'
        AND is_nullable = 'YES')
    UNION ALL SELECT 'document_supersession.apply_attempts' WHERE NOT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='document_supersession' AND column_name='apply_attempts')
    UNION ALL SELECT 'state CHECK must admit applied_shared' WHERE NOT EXISTS (
      SELECT 1 FROM pg_constraint
       WHERE conrelid='document_supersession'::regclass
         AND pg_get_constraintdef(oid) LIKE '%applied_shared%'
         AND conname <> 'chk_dsup_seq')
    UNION ALL SELECT 'chk_dsup_seq must admit applied_shared' WHERE NOT EXISTS (
      SELECT 1 FROM pg_constraint WHERE conname='chk_dsup_seq'
         AND conrelid='document_supersession'::regclass
         AND pg_get_constraintdef(oid) LIKE '%applied_shared%')
    UNION ALL SELECT 'view document_source_projection' WHERE to_regclass('document_source_projection') IS NULL
    -- ⚠ THE FOUR INDEX CHECKS BELOW MEASURE A NAME, NOT AN INDEX, and are WEAKER than the
    -- comment above them claims. to_regclass() resolves any relation, so a plain TABLE
    -- carrying the index's name satisfies them. Verified: `CREATE TEMP TABLE
    -- idx_dsc_rid_live (x int)` then a relname lookup -> 1 match. They therefore do NOT
    -- close the "re-run against a table from an EARLIER draft silently does nothing" hole
    -- this block was written to close -- the defence has the defect it was built to catch.
    -- Kept because they are inert and do catch an outright-missing object, but they are not
    -- evidence that the right index exists. A real check joins pg_index and compares
    -- indexdef; that is a follow-up, not a blocker, because CREATE INDEX in this same
    -- transaction either succeeded or aborted it.
    UNION ALL SELECT 'index uq_dsup_open_pred'         WHERE to_regclass('uq_dsup_open_pred') IS NULL
    UNION ALL SELECT 'index uq_dsup_chain_seq'         WHERE to_regclass('uq_dsup_chain_seq') IS NULL
    UNION ALL SELECT 'index idx_dsc_rid_live'          WHERE to_regclass('idx_dsc_rid_live') IS NULL
    UNION ALL SELECT 'index idx_dsc_url'               WHERE to_regclass('idx_dsc_url') IS NULL
    -- A1: every snapshotted document row is claimed (live OR superseded: a superseded
    --     row keeps a pinned claim, which is what protects its bytes from a third party)
    UNION ALL SELECT format('unclaimed snapshotted document rows (%s): %s', cnt, sample) FROM (
      SELECT count(*) AS cnt, string_agg(left(rid,26), ',' ORDER BY rid) FILTER (WHERE rn <= 5) AS sample
        FROM (SELECT d.rid, row_number() OVER (ORDER BY d.rid) AS rn
                FROM _mig123_doc d
               WHERE NOT EXISTS (SELECT 1 FROM document_source_claim c WHERE c.document_rid = d.rid)) z
      ) q WHERE cnt > 0
    -- A1 POSITIVE CONTROL: the anti-join above returns 0 against an EMPTY claim table too,
    -- which is exactly what a failed backfill leaves. Pair it AGAINST THE POPULATION, not
    -- against the snapshot: if the ledger row is stamped late the snapshot is itself empty,
    -- and a control phrased "claims = 0 while snapshot > 0" is then vacuous too.
    UNION ALL SELECT format('claim table is EMPTY while document_source_projection holds %s '
                            'document rows', pop)
      FROM (SELECT (SELECT count(*) FROM document_source_projection) AS pop) q
     WHERE claims_written = 0 AND pop > 0
    -- and name the cause directly: this is the koi_migrations-ordering seam (F4). An empty
    -- snapshot silently produces an empty ownership table that A1's anti-join CERTIFIES.
    UNION ALL SELECT format('snapshot is EMPTY while the projection holds %s document rows -- the '
                            'koi_migrations boundary subquery returned NULL or a past instant; the '
                            'ledger INSERT must PRECEDE the backfill legs', pop)
      FROM (SELECT (SELECT count(*) FROM document_source_projection) AS pop) q
     WHERE rows_to_claim = 0 AND pop > 0
    -- INV-1: every LIVE document row has a LIVE claim
    UNION ALL SELECT format('live document rows with no live claim (%s): %s', cnt, sample) FROM (
      SELECT count(*) AS cnt, string_agg(left(rid,26), ',' ORDER BY rid) FILTER (WHERE rn <= 5) AS sample
        FROM (SELECT p.rid, row_number() OVER (ORDER BY p.rid) AS rn
                FROM document_source_projection p
               WHERE p.superseded_at IS NULL
                 AND NOT EXISTS (SELECT 1 FROM document_source_claim c
                                  WHERE c.document_rid = p.rid AND c.claim_state = 'live')) z
      ) q WHERE cnt > 0
    -- INV-2 / AC6: every superseded document row is named on a journal row in the
    -- accounted-for states. 'refused' is deliberately EXCLUDED: a refusal is precisely
    -- the case where a hidden row has NO accepted explanation.
    UNION ALL SELECT format('superseded rows with no accounted-for journal edge (%s): %s', cnt, sample) FROM (
      SELECT count(*) AS cnt, string_agg(left(rid,26), ',' ORDER BY rid) FILTER (WHERE rn <= 5) AS sample
        FROM (SELECT m.rid, row_number() OVER (ORDER BY m.rid) AS rn
                FROM koi_memories m
               WHERE m.rid LIKE 'document:%' AND m.superseded_at IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM document_supersession d
                                  WHERE d.predecessor_rid = m.rid
                                    AND d.state IN ('applied','applied_shared','pending','failed'))) z
      ) q WHERE cnt > 0
    -- I12: no superseded document row keeps UNSTAMPED discourse moves (leg E's job)
    UNION ALL SELECT format('superseded rows with unstamped discourse moves (%s)', cnt) FROM (
      SELECT count(*) AS cnt
        FROM session_discourse_moves d JOIN koi_memories m ON m.rid = d.source_rid
       WHERE d.source_type='document' AND m.superseded_at IS NOT NULL AND d.superseded_at IS NULL) q
     WHERE cnt > 0
    -- I11: and no LIVE row carries stamped moves (the un-stamp arm's detector)
    UNION ALL SELECT format('live rows with stamped discourse moves (%s)', cnt) FROM (
      SELECT count(*) AS cnt
        FROM session_discourse_moves d JOIN koi_memories m ON m.rid = d.source_rid
       WHERE d.source_type='document' AND m.superseded_at IS NULL AND d.superseded_at IS NOT NULL) q
     WHERE cnt > 0
    -- the claim-count identity: both sides computed HERE, in this run. No literal.
    UNION ALL SELECT format('claim count identity broken by %s', delta) FROM (
      SELECT (SELECT count(*) FROM document_source_claim)
             - ((SELECT count(*) FROM _mig123_doc)
                + (SELECT count(*) FROM _mig123_doc d
                     JOIN document_ingestion_log l ON l.document_rid = d.rid
                    WHERE l.source_url IS NOT NULL AND l.source_url NOT LIKE 'document:%'
                      AND l.source_url IS DISTINCT FROM d.source_url)) AS delta) q
     WHERE delta <> 0
    -- ⚠ VACUOUS -- KEPT, BUT IT IS NOT EVIDENCE. This was written to prove "no claim was
    -- silently dropped by an ON CONFLICT". It CANNOT prove that, and it cannot fail:
    -- count(*) - count(DISTINCT (source_kind,source_id,source_url)) over a table whose
    -- PRIMARY KEY is exactly (source_kind,source_id,source_url) is structurally always 0.
    -- A swallowed INSERT leaves NO row, so there is no duplicate to count. Verified: a
    -- reviewer constructed precisely that failure and this reported zero. The real coverage
    -- for a dropped claim is the count identity immediately above and the I1 anti-join.
    -- Left in place because it is inert, not wrong -- but do not cite it afterwards.
    UNION ALL SELECT format('claim PK collisions: %s', c) FROM (
      SELECT count(*) - count(DISTINCT (source_kind,source_id,source_url)) AS c
        FROM document_source_claim) q WHERE c <> 0
  ) t;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION 'migration 123 assertion failed: %', missing;
  END IF;
  RAISE NOTICE 'migration 123 assertion PASSED (claims=%, snapshot rows=%)',
               claims_written, rows_to_claim;
END $$;

COMMIT;