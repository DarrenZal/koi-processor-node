-- 114_canon_watch.sql — claim supersession events + canon dependency watcher
-- (substrate-first membrane walking skeleton; PREREG-correction-drill-001 §2)
--
-- Ordering note (plan v2.1 §5): the canonicalization registry / claim_profile /
-- immutability guard are NOT prerequisites of these tables; the hard constraint
-- (registry+profile before the GUARD) is untouched by this migration.
--
-- Registration: recorded in koi_migrations below (schema_migrations is the
-- DEPRECATED ledger; see scripts/run_migrations.sh guard).

CREATE TABLE IF NOT EXISTS claim_supersession_events (
  event_id          BIGSERIAL PRIMARY KEY,
  source_event_key  TEXT UNIQUE NOT NULL,     -- deterministic: md5(old||'->'||new||'|'||origin)
  old_rid           TEXT NOT NULL,
  new_rid           TEXT,                     -- NULL = withdrawal
  kind              TEXT NOT NULL DEFAULT 'unclassified'
    -- supersession kinds per plan v2.1 §3.4, plus 'rejection': an immutable
    -- negative-verdict event derived from attestation history (D9). A later
    -- approval changes a CASE'S resolution; it never deletes a rejection row.
    CHECK (kind IN ('correction','refinement','scope_change','merge',
                    'withdrawal','restatement','rejection','unclassified')),
  actor             TEXT,
  reason            TEXT,
  parent_event_id   BIGINT REFERENCES claim_supersession_events(event_id),
  occurred_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cse_old ON claim_supersession_events(old_rid);
CREATE INDEX IF NOT EXISTS idx_cse_new ON claim_supersession_events(new_rid);

CREATE TABLE IF NOT EXISTS canon_dependencies (
  dependency_id   BIGSERIAL PRIMARY KEY,
  assertion_slug  TEXT NOT NULL,
  claim_rid       TEXT NOT NULL,
  repo            TEXT NOT NULL,
  note_path       TEXT NOT NULL,
  manifest_commit TEXT,
  projected_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (assertion_slug, claim_rid)
);

CREATE TABLE IF NOT EXISTS canon_review_cases (
  case_id         BIGSERIAL PRIMARY KEY,
  assertion_slug  TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','resolved')),
  resolution      TEXT CHECK (resolution IN
                    ('update_canon','retain_with_rationale','unaffected','defer')),
  opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at     TIMESTAMPTZ
);
-- one OPEN case per assertion (coalescing invariant; B2/B12)
CREATE UNIQUE INDEX IF NOT EXISTS uq_open_case_per_assertion
  ON canon_review_cases(assertion_slug) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS canon_dependency_impacts (
  impact_id        BIGSERIAL PRIMARY KEY,
  dependency_id    BIGINT NOT NULL REFERENCES canon_dependencies(dependency_id),
  causal_event_id  BIGINT NOT NULL REFERENCES claim_supersession_events(event_id),
  case_id          BIGINT REFERENCES canon_review_cases(case_id),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (dependency_id, causal_event_id)   -- idempotency (B4/B11)
);

CREATE TABLE IF NOT EXISTS canon_watch_status (
  id                 INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  last_scan_at       TIMESTAMPTZ,
  last_event_id_seen BIGINT,
  open_cases         INT,
  task_projection    TEXT
);

-- Backfill: mirror existing claims.supersedes_rid edges as unclassified linkage
-- events. Deterministic key; rerunnable; inserts NOTHING beyond real edges.
INSERT INTO claim_supersession_events (source_event_key, old_rid, new_rid, kind, actor, reason)
SELECT md5(c.supersedes_rid || '->' || c.claim_rid || '|backfill:supersedes_rid'),
       c.supersedes_rid, c.claim_rid, 'unclassified', 'migration-114',
       'mirrors claims.supersedes_rid'
FROM claims c
WHERE c.supersedes_rid IS NOT NULL
ON CONFLICT (source_event_key) DO NOTHING;

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('114_canon_watch', 'v1_walking_skeleton')
ON CONFLICT (migration_id) DO NOTHING;
