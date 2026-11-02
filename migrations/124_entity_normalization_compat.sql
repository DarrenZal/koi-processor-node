-- =============================================================================
-- 124_entity_normalization_compat — normalizer parity as a READ path, not a rewrite
-- =============================================================================
-- Issue: #61 (entity registry: normalization drift permits same-label, same-type
--        duplicates), in support of #62 (document ingest identity integrity).
--
-- WHAT THIS DOES
--   1. Installs `koi_normalize_entity_text(TEXT)` — an IMMUTABLE SQL mirror of
--      api/resolution_primitives.py::normalize_entity_text.
--   2. Adds an EXPRESSION INDEX on (entity_type, koi_normalize_entity_text(entity_text))
--      so a compatibility read against the CURRENT normalization of stored canonical
--      labels is an index lookup, not a scan.
--   3. Adds a read-only view `entity_current_norm_duplicates` reporting live
--      same-current-normalized-label + same-type duplicate sets (issue #61 AC6).
--
-- WHAT THIS DELIBERATELY DOES NOT DO
--   It does NOT convert entity_registry.normalized_text to a GENERATED column, and it
--   does NOT backfill it. migrations/DRAFT_normalized_text_generated.sql analysed that
--   option and found it is a live-behaviour change, not a schema change: re-measured
--   2026-09-13 on 36,484 rows, it would silently recompute 1,268 rows and collapse
--   **125 currently-distinct live groups (254 rows)** onto shared
--   (entity_type, normalized_text) pairs — which `... LIMIT 1` with no ORDER BY would
--   then arbitrate nondeterministically. Those 125 groups need an operator merge pass,
--   not an implicit rewrite inside an unrelated deploy.
--
--   So this migration closes the READ side only. Nothing that exists today changes
--   value, and no write path changes behaviour as a result of this file alone. A
--   caller opts in by querying through the function. That makes it additive and
--   trivially reversible (see 124_entity_normalization_compat_down.sql).
--
-- KNOWN DIVERGENCE FROM THE PYTHON NORMALIZER (inherited from the DRAFT's analysis,
-- re-verified live 2026-09-13). This cluster has lc_ctype='C', so SQL lower() folds
-- ASCII only, while Python str.lower() is full Unicode. For non-ASCII cased letters
-- (accented names) the SQL function and the Python function can disagree. 1,682 of
-- 36,484 rows contain non-ASCII text; ~56 of the measured mismatches trace to this.
-- Callers that must be exact for those names compare in Python over a candidate set
-- (that is what api/ingest_identity.py does for the document-ingest preflight); the
-- index below is a fast *superset* filter, never the final authority.
--
-- Also inherited, and load-bearing: `replace(x,'  ',' ')` is a single-pass,
-- non-overlapping scan in BOTH Python and Postgres (a run of n spaces reduces to
-- ceil(n/2)). Do NOT "fix" it into regexp_replace(x,'\s+',' ','g') — that would stop
-- matching the Python function this mirrors. And the trim charset is spelled with
-- chr(11), NOT E'\v': in Postgres E'\v' is the literal letter 'v', which silently
-- turned "Venmo" into "enmo" in an early draft of the DRAFT file.
--
-- Date: 2026-09-13
-- Database: personal_koi
-- Reversible: yes — 124_entity_normalization_compat_down.sql
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. The canonical IMMUTABLE SQL normalizer.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION koi_normalize_entity_text(t TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT ltrim(
             replace(
               replace(
                 replace(
                   -- ASCII whitespace only. chr(11) = VT, deliberately NOT E'\v'.
                   btrim(lower(t), ' ' || chr(9) || chr(10) || chr(13) || chr(12) || chr(11)),
                 '_', ' '),
               '-', ' '),
             '  ', ' '),   -- single-pass, non-overlapping: matches Python's
                           -- non-idempotent .replace('  ',' ') exactly.
           '@')
$$;

COMMENT ON FUNCTION koi_normalize_entity_text(TEXT) IS
  'SQL-IMMUTABLE mirror of api/resolution_primitives.py::normalize_entity_text (byte-identical twin at api/personal_ingest_api.py::normalize_entity_text). Used as a COMPATIBILITY READ so a registration can find rows whose stored normalized_text was written by an older normalizer (issue #61). KNOWN divergence: ASCII-only lower() under this cluster''s lc_ctype=C, so accented names may not agree with the Python function — treat this as a superset filter and confirm in Python. See migrations/124_entity_normalization_compat.sql.';

-- -----------------------------------------------------------------------------
-- 2. Expression index backing the compatibility read.
--    NOT UNIQUE — 44 live duplicate sets already exist (see the view below), so a
--    unique index would fail to build and, if it did build, would start rejecting
--    legitimate writes for a pre-existing data problem this migration does not fix.
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_entity_registry_current_norm
    ON entity_registry (entity_type, koi_normalize_entity_text(entity_text));

-- Label-only expression index. The composite above cannot serve a lookup that does
-- NOT constrain entity_type, and the cross-type advisory in api/ingest_identity.py is
-- exactly such a lookup (same label, ANY type).
CREATE INDEX IF NOT EXISTS idx_entity_registry_current_norm_label
    ON entity_registry (koi_normalize_entity_text(entity_text));

COMMENT ON INDEX idx_entity_registry_current_norm IS
  'Backs the issue-#61 compatibility read: find live rows whose CURRENT normalization of entity_text matches a requested label, even when the stored normalized_text was written by an older normalizer.';

-- -----------------------------------------------------------------------------
-- 3. Read-only duplicate report (issue #61 AC6).
--    Reports live same-type sets that share a CURRENT-normalized canonical label.
--    `stored_norms` shows whether the set is already visible under the stored value
--    (a pre-existing duplicate) or only becomes visible under current normalization
--    (a drift-created duplicate that exact-match lookups cannot currently see).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW entity_current_norm_duplicates AS
SELECT
    entity_type,
    koi_normalize_entity_text(entity_text)          AS current_norm,
    count(*)                                        AS live_rows,
    array_agg(fuseki_uri ORDER BY fuseki_uri)       AS uris,
    array_agg(DISTINCT entity_text)                 AS labels,
    array_agg(DISTINCT normalized_text)             AS stored_norms,
    (count(DISTINCT normalized_text) > 1)           AS drift_created
FROM entity_registry
WHERE merged_into IS NULL
  AND revoked_at IS NULL
GROUP BY entity_type, koi_normalize_entity_text(entity_text)
HAVING count(*) > 1;

COMMENT ON VIEW entity_current_norm_duplicates IS
  'Issue #61 AC6 audit surface. One row per live same-type set sharing a current-normalized canonical label. drift_created=true means the rows carry DIFFERENT stored normalized_text values, i.e. they are invisible to each other at Tier-1 exact match today and are the direct product of normalizer drift. Read-only; remediation is an operator merge pass, never an implicit rewrite.';

-- -----------------------------------------------------------------------------
-- 4. Assertion: the function must agree with the Python normalizer on the cases
--    issue #61 names. These are constants, so this fails at apply time if someone
--    edits the function body in a way that breaks the mirror.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    bad TEXT;
BEGIN
    SELECT string_agg(format('%L -> %L (expected %L)', inp, koi_normalize_entity_text(inp), want), '; ')
      INTO bad
      FROM (VALUES
              ('GPT-4',      'gpt 4'),
              ('GPT-4o',     'gpt 4o'),
              ('GPT-4.1',    'gpt 4.1'),
              ('gpt-5.4',    'gpt 5.4'),
              ('  Spaced  ', 'spaced'),
              ('@handle',    'handle'),
              ('a_b-c',      'a b c')
           ) AS t(inp, want)
     WHERE koi_normalize_entity_text(inp) IS DISTINCT FROM want;

    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'koi_normalize_entity_text no longer mirrors the Python normalizer: %', bad;
    END IF;
END $$;

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('personal:124_entity_normalization_compat', 'v1_compat_read_only')
ON CONFLICT (migration_id) DO NOTHING;

COMMIT;
