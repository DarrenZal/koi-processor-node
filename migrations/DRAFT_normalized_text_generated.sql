-- =============================================================================
-- DRAFT: entity_registry.normalized_text as a GENERATED ALWAYS AS (...) STORED
-- column
-- =============================================================================
-- Status:   DRAFT ONLY. NOT applied anywhere. NOT numbered into the real
--           migration sequence (currently at 115_*). Renumber when ready.
-- Date:     2026-08-31
-- Database: personal_koi, PostgreSQL 14.15 (verified live)
-- Trigger:  reported audit -- 1,212 rows fail a Tier-1 exact match on their
--           own name because the Python normalizer and the stored
--           normalized_text disagree.
--
-- =============================================================================
-- VERDICT
-- =============================================================================
-- GENERATED ALWAYS AS (...) STORED is ACHIEVABLE. Every step of the Python
-- normalizer (api/resolution_primitives.py::normalize_entity_text, byte-
-- identical twin in api/personal_ingest_api.py:620) has a direct IMMUTABLE
-- SQL equivalent -- no plpgsql loop, no external call, confirmed against the
-- LIVE Postgres build below. A trigger-based fallback is drafted in section 5
-- for the record, but is NOT the recommendation.
--
-- The blocker is not "can this be expressed in SQL" -- it can. The blocker is
-- that applying it is a live-behavior change, not just a schema change. See
-- RISK NOTES in the returned report for the two things that must be reviewed
-- before this runs: (a) every existing row's normalized_text gets silently
-- recomputed the moment the column conversion happens, and (b) 121 groups of
-- currently-DISTINCT live entities (246 rows) collapse onto a shared
-- (entity_type, normalized_text) pair once the fix is applied, which changes
-- what Tier-1 `... LIMIT 1` with no ORDER BY returns for those names.
--
-- =============================================================================
-- STEP-BY-STEP: python normalize_entity_text() vs. SQL, each step verified
-- =============================================================================
--   Python                          SQL equivalent           Verified how
--   ------------------------------  -----------------------  --------------------------------
--   text.lower()                    lower(text)               live: lower() is IMMUTABLE here
--                                                              (provolatile='i'), BUT lc_ctype
--                                                              is 'C' on this cluster, so it is
--                                                              ASCII-only casing -- see CAVEAT 1.
--   .strip()                        btrim(text, <charset>)    live: btrim() is IMMUTABLE;
--                                                              default charset is space ONLY,
--                                                              so an explicit charset is passed
--                                                              -- see CAVEAT 2.
--   .replace('_', ' ')              replace(text, '_', ' ')   trivial, exact.
--   .replace('-', ' ')              replace(text, '-', ' ')   trivial, exact.
--   .replace('  ', ' ')             replace(text, '  ', ' ')  live-verified: Postgres replace()
--                                                              is the SAME single-pass, non-
--                                                              overlapping, left-to-right scan
--                                                              as Python str.replace(). A run of
--                                                              n spaces reduces to ceil(n/2)
--                                                              spaces under BOTH -- this is NOT a
--                                                              full whitespace collapse and must
--                                                              NOT be "fixed" into a regex \s+
--                                                              collapse, or the SQL function stops
--                                                              matching the Python one it must
--                                                              replace. (0 live rows currently
--                                                              have a run of 3+ spaces, so this
--                                                              quirk has zero current impact --
--                                                              documented for future data.)
--   .lstrip('@')                    ltrim(text, '@')           live-verified: ltrim(text,'@')
--                                                              strips a leading run of '@' and
--                                                              nothing else, byte-for-byte
--                                                              matching Python's lstrip('@') on
--                                                              every edge case tried (including
--                                                              "a leading space then '@'", where
--                                                              neither strips the space).
--
-- CAVEAT 1 (real, not blocking, 56/1260 of today's measured mismatches):
--   lc_ctype='C' means Postgres lower() only touches ASCII A-Z. Python's
--   str.lower() is full Unicode case-folding. Verified live:
--     lower('Ünïcödé Nàme') -> 'Ünïcödé nàme'   (Ü NOT lowercased)
--     lower('CAFÉ')          -> 'cafÉ'           (É NOT lowercased)
--   vs. Python: 'ünïcödé nàme', 'café'. This is a genuine, permanent gap
--   between what this SQL function computes and what the Python normalizer
--   computes for non-ASCII cased letters -- installing an ICU collation would
--   close it but is a separate, larger infra change (initdb-time or PG15+
--   nondeterministic-collation work), out of scope here. Until that happens,
--   a GENERATED column closes the WRITE-side drift (many call sites minting
--   arbitrary values) but does NOT make the STORED value byte-identical to
--   what the Python-side Tier-1 query parameter computes for accented names
--   -- Tier-1 exact match on accented names can still miss after this ships,
--   same as it can today. 1,682 of 33,297 rows (5%) contain non-ASCII text;
--   56 of the 1,260 measured mismatches trace to this specific gap.
--
-- CAVEAT 2 (real, negligible current impact):
--   Python .strip() strips a broad Unicode whitespace set (NBSP U+00A0, NEL
--   U+0085, the U+2000..U+200A run, U+2028/29, U+202F, U+205F, U+1680 --
--   28 codepoints total, confirmed via str.isspace() enumeration). The SQL
--   below only strips the 6 ASCII whitespace controls (space, tab, LF, CR,
--   FF, VT) via explicit chr() codes -- NOT an E'...' escape string, because
--   E'\v' is NOT vertical tab in Postgres (any character after a backslash
--   that isn't a recognized escape is taken literally, so E'\v' silently
--   becomes the letter 'v' -- this was caught live while drafting this file:
--   an early draft using E' \t\n\r\f\v' as the trim charset stripped leading
--   /trailing 'v'/'V' from real entity names, e.g. "Venmo" -> "enmo",
--   "Vertex AI" -> "ertex ai". Using chr(11) instead of \v avoids the trap.
--   Live data check: 0 rows contain tab/LF/CR, exactly 1 row contains NBSP.
--   Not worth closing for 1 row; documented so nobody "fixes" the gap by
--   reaching for E'\v' and reintroduces the v-eating bug.
--
-- =============================================================================
-- LIVE MEASUREMENTS (2026-08-31, all inside BEGIN;...ROLLBACK; -- nothing
-- applied, nothing persisted; the function below was created and dropped in
-- the same transaction purely to run these counts)
-- =============================================================================
--   total entity_registry rows                                    33,297
--   live (merged_into IS NULL) rows                                33,045
--   rows where stored normalized_text != this draft function       1,260  (1,256 live)
--     -- close to, not identical to, the audited "1,212" -- same defect
--     -- class; the small delta is most likely rows written/merged between
--     -- the audit and this measurement. Re-run the query in section 4
--     -- immediately before applying to get a fresh number.
--   ... of which explained by entity_text containing _ / - / leading @ /
--       a double-space (i.e. the missing-transformation-steps bug)         1,215
--   ... of which explained by non-ASCII casing (CAVEAT 1)                     56
--   breakdown by resolution_tier (migration 111 instrumentation, so only
--   meaningful for rows created since 2026-08-22):
--       NULL (pre-instrumentation)   31,331 rows, 1,258 mismatches
--       tier3_created                 1,885 rows,     2 mismatches
--       federation                       41 rows,     0 mismatches
--       manual                            29 rows,     1 mismatch
--       tier3_created_ambiguous          11 rows,     0 mismatches
--   NEW (entity_type, normalized_text) collision groups this migration would
--   create among currently-distinct LIVE rows (i.e. rows that do NOT already
--   share a normalized_text today but WOULD after the fix):
--       121 groups, 246 rows involved. Sample:
--         Project "regen koi mcp"    <- {regen-koi-mcp, "regen-koi mcp", "regen koi mcp"}  (3 rows)
--         Project "poietic match"    <- {poietic_match, poietic-match, "poietic match"}    (3 rows)
--         Concept "vault sync"       <- {vault_sync, vault-sync, "vault sync"}             (3 rows)
--         Concept "civic intelligence" <- {civic-intelligence, "civic intelligence"}       (2 rows)
--       ... 117 more groups, all the same shape: hyphen/underscore variants
--       of what is almost certainly ONE real entity, kept apart today only
--       because Tier-1 couldn't see they were the same normalized string.
--   pre-existing collision groups (already share normalized_text today,
--   NOT caused by this migration):  44 groups
--
-- WHY THE COLLISIONS MATTER OPERATIONALLY: entity_registry has NO live
-- UNIQUE constraint on (normalized_text, entity_type) -- migration 020
-- declared entity_registry_text_type_key but it is absent from the current
-- live constraint list (pg_constraint), so this migration will NOT fail on
-- apply. But resolve_entity_by_exact_match() (personal_ingest_api.py ~799)
-- runs `WHERE normalized_text = $1 AND entity_type = $2 ... LIMIT 1` with NO
-- ORDER BY. Today that returns >=0 rows deterministically per name. After
-- this migration, for the 246 rows above, it returns one row nondeterminis-
-- tically chosen from 2-3 real candidates that used to be distinguishable.
-- This is very likely fixing an entity-duplication bug (three "Regen KOI
-- MCP" Project rows that should be one), but the 121 groups deserve an
-- operator pass through scripts/alias_audit.py / apply_dedup_merges.py
-- BEFORE or immediately after this ships, not silent LIMIT-1 arbitration.
--
-- =============================================================================
-- REQUIRED CODE CHANGES -- NOT INCLUDED IN THIS FILE, BUT MUST LAND IN THE
-- SAME DEPLOY. A GENERATED STORED column rejects ANY explicit value in an
-- INSERT's column list (verified live: "cannot insert a non-DEFAULT value
-- into column normalized_text") and rejects it in an ON CONFLICT DO UPDATE
-- SET too, even via EXCLUDED (verified live: "column normalized_text can
-- only be updated to DEFAULT"). Every one of these breaks on the FIRST
-- entity write after this migration applies unless fixed first:
--
--   1. api/personal_ingest_api.py :: store_new_entity()
--      Two INSERT INTO entity_registry statements (~line 1572 with-embedding
--      branch, ~line 1592 without-embedding branch) both list normalized_text
--      in the column list and pass the `normalized` local as its value.
--      Fix: drop normalized_text from both column lists and both VALUES/
--      positional-param lists (renumber the $n placeholders after it).
--
--   2. api/domain_event_handlers.py :: _apply_entity()  (~line 141-168)
--      Reads `normalized_text = payload.get("normalized_text",
--      entity_text.lower().strip())` -- note this fallback is ALSO the
--      simplified computation, not normalize_entity_text() -- then INSERTs
--      it and, on conflict, does `normalized_text = EXCLUDED.normalized_text`.
--      Fix: delete the `normalized_text = payload.get(...)` line, drop
--      normalized_text from the INSERT column list and VALUES list, and
--      delete the `normalized_text = EXCLUDED.normalized_text,` line from
--      the ON CONFLICT SET clause entirely (confirmed live: even naming the
--      generated column in a SET clause errors, referencing EXCLUDED or not).
--
--   3. api/personal_ingest_api.py :: emit_domain_event() payload builders
--      (~line 2931 inside /ingest, ~line 4405 inside /register-entity) both
--      set `"normalized_text": canonical.name.lower().strip()` in the event
--      payload dict. THIS is the actual root of the drift for any entity
--      touched by federation: it is a materially different computation from
--      normalize_entity_text() (no _/- replace, no double-space collapse, no
--      leading-@ strip) and is exactly what (2) above trusts and writes.
--      Not required to change for the migration to be safe (the field
--      becomes inert once (2) stops reading it), but it is dead/misleading
--      data on the wire afterward -- clean it up in the same pass.
--
--   4. api/personal_ingest_api.py :: ensure_schema()  (~line 2049-2078)
--      Dev/test bootstrap `CREATE TABLE IF NOT EXISTS entity_registry (...
--      normalized_text TEXT NOT NULL ...)`. IF NOT EXISTS makes this a no-op
--      against prod (table already exists), but a FRESH dev/test DB
--      bootstrapped from this code path gets the OLD plain-column schema
--      while prod has the GENERATED one -- update it for parity or dev/test
--      silently diverges from prod on this exact column.
--
--   No other INSERT INTO entity_registry site in the repo lists
--   normalized_text in its column list (checked: tests/*, scripts/*,
--   api/routers/*, src/knowledge_graph/entity_resolver.py,
--   src/core/koi_event_bridge_v2.py -- all either omit the column, or are
--   test fixtures that would need the same column-list edit if kept).
--
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Canonical IMMUTABLE SQL normalizer -- single source of truth for the
--    generated column. Style follows koi_facets_well_formed() (migration 111):
--    LANGUAGE sql IMMUTABLE PARALLEL SAFE, no plpgsql needed.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION koi_normalize_entity_text(t TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT ltrim(
             replace(
               replace(
                 replace(
                   -- ASCII whitespace only (CAVEAT 2). chr(11) = VT, deliberately
                   -- NOT written as E'\v' (see CAVEAT 2 note on the E'\v' trap).
                   btrim(lower(t), ' ' || chr(9) || chr(10) || chr(13) || chr(12) || chr(11)),
                 '_', ' '),
               '-', ' '),
             '  ', ' '),   -- single-pass, non-overlapping -- matches Python's
                           -- non-idempotent .replace('  ',' ') exactly. Do
                           -- NOT change this to regexp_replace(t,'\s+',' ','g').
           '@')
$$;

COMMENT ON FUNCTION koi_normalize_entity_text(TEXT) IS
  'SQL-immutable mirror of api/resolution_primitives.py::normalize_entity_text (byte-identical twin in api/personal_ingest_api.py:620). Backs entity_registry.normalized_text as a GENERATED STORED column. KNOWN divergence from the Python function: ASCII-only lower() under this cluster''s lc_ctype=C (non-ASCII cased letters, e.g. accented names, do not fold) and ASCII-only whitespace trim (misses ~28 Unicode space codepoints Python .strip() recognizes). See migrations/DRAFT_normalized_text_generated.sql for the full analysis.';


-- -----------------------------------------------------------------------------
-- 2. Recommended: DROP + ADD as GENERATED ALWAYS ... STORED.
-- -----------------------------------------------------------------------------
-- There is no in-place "ALTER COLUMN ... ADD GENERATED" for a plain existing
-- column on this Postgres version (verified live: syntax error). The only
-- path is drop-and-recreate, which is a full-table rewrite: EVERY row's
-- normalized_text is recomputed from entity_text at DDL time, under an
-- ACCESS EXCLUSIVE lock for the duration (table size: 33,297 rows -- expect
-- this to be fast, but it is a lock, not a background operation; there is no
-- CONCURRENTLY option for this kind of ALTER TABLE).
--
-- DROP COLUMN also drops every index that depends on the column. THREE do:
-- idx_entity_registry_normalized (btree), idx_entity_registry_normalized_trgm
-- (GIN, pg_trgm), idx_entity_type_normalized (btree, entity_type+normalized_text
-- composite). All three must be recreated after ADD COLUMN, or trigram/
-- composite lookups silently fall back to a sequential scan.

-- ⚠️ DO NOT RUN AS-IS. Wrap in BEGIN/COMMIT, re-run the section-4 dry-run
-- queries first inside that same transaction, inspect the collision list,
-- and only COMMIT if the operator has reviewed it. This file intentionally
-- ships with no BEGIN/COMMIT of its own so it cannot be `psql -f`'d blind.

ALTER TABLE entity_registry DROP COLUMN normalized_text;

ALTER TABLE entity_registry
    ADD COLUMN normalized_text TEXT
    GENERATED ALWAYS AS (koi_normalize_entity_text(entity_text)) STORED
    NOT NULL;
    -- NOT NULL is safe: entity_text is itself NOT NULL and
    -- koi_normalize_entity_text() never returns NULL for a non-NULL input.

CREATE INDEX IF NOT EXISTS idx_entity_registry_normalized
    ON entity_registry (normalized_text);

CREATE INDEX IF NOT EXISTS idx_entity_registry_normalized_trgm
    ON entity_registry USING gin (normalized_text gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_entity_type_normalized
    ON entity_registry (entity_type, normalized_text);

COMMENT ON COLUMN entity_registry.normalized_text IS
  'GENERATED ALWAYS AS (koi_normalize_entity_text(entity_text)) STORED. Cannot drift from entity_text again -- no INSERT/UPDATE can assign it a different value (verified: Postgres rejects both an explicit column-list value and an ON CONFLICT ... SET, even via EXCLUDED). See migrations/DRAFT_normalized_text_generated.sql for the pre-migration audit (1,260 mismatched rows, 121 new collision groups) and the 4 call sites that must stop passing this column explicitly.';


-- =============================================================================
-- 3. FALLBACK (not recommended, drafted for completeness only): BEFORE
--    INSERT OR UPDATE trigger instead of GENERATED.
-- =============================================================================
-- Weaker than GENERATED in one specific way: a trigger can still be bypassed
-- by anything that disables triggers (ALTER TABLE ... DISABLE TRIGGER, a
-- session with triggers off, certain bulk-load paths), and it re-derives the
-- value procedurally on every write rather than the column being provably a
-- pure function of entity_text at the type-system level. Its advantage is
-- operational: it does NOT require a DROP COLUMN / table rewrite, so it can
-- be added without touching the other 33,297 rows' stored values at DDL
-- time -- existing rows keep their (possibly wrong) stored value until the
-- next UPDATE touches them, and a SEPARATE, explicit backfill UPDATE can be
-- run under operator control (batched, resumable, reviewable per-batch)
-- rather than as one implicit rewrite. Use this instead of section 2 if the
-- 121-group collision list needs to be worked through gradually rather than
-- all at once.
--
-- CREATE OR REPLACE FUNCTION koi_entity_registry_set_normalized_text()
-- RETURNS TRIGGER LANGUAGE plpgsql AS $$
-- BEGIN
--     NEW.normalized_text := koi_normalize_entity_text(NEW.entity_text);
--     RETURN NEW;
-- END$$;
--
-- DROP TRIGGER IF EXISTS tr_entity_registry_normalized_text ON entity_registry;
-- CREATE TRIGGER tr_entity_registry_normalized_text
--     BEFORE INSERT OR UPDATE OF entity_text ON entity_registry
--     FOR EACH ROW EXECUTE FUNCTION koi_entity_registry_set_normalized_text();
-- -- "UPDATE OF entity_text" is deliberate, matching the tr_entity_facets_registered
-- -- precedent in migration 111: an unqualified UPDATE trigger fires on every
-- -- alias/metadata/merged_into write in the system.
--
-- -- Under the trigger approach, the 4 call sites in the REQUIRED CODE CHANGES
-- -- section above do NOT need to change -- an explicit normalized_text value
-- -- in an INSERT/UPDATE is silently OVERWRITTEN by the trigger before it hits
-- -- the heap (trigger runs BEFORE, so NEW.normalized_text is reassigned prior
-- -- to the write), rather than being rejected outright. That is also its
-- -- weakness relative to GENERATED: a caller that thinks it is setting
-- -- normalized_text explicitly is silently ignored rather than erroring, so
-- -- the 4 call sites should still be cleaned up for clarity even though they
-- -- would not break.


-- =============================================================================
-- 4. DRY-RUN QUERIES -- re-run these (in a BEGIN;...ROLLBACK;, with section 1's
--    function created temporarily) immediately before applying section 2, to
--    get fresh numbers. Do not trust the 2026-08-31 counts in the header
--    comment if any meaningful time has passed.
-- =============================================================================
-- SELECT count(*) AS total_rows,
--        count(*) FILTER (WHERE normalized_text IS DISTINCT FROM koi_normalize_entity_text(entity_text)) AS mismatches
-- FROM entity_registry;
--
-- WITH recomputed AS (
--     SELECT fuseki_uri, entity_type, entity_text, normalized_text AS old_norm,
--            koi_normalize_entity_text(entity_text) AS new_norm
--     FROM entity_registry
--     WHERE merged_into IS NULL
-- ),
-- new_groups AS (
--     SELECT entity_type, new_norm, count(*) AS n, array_agg(DISTINCT old_norm) AS distinct_old_norms
--     FROM recomputed
--     GROUP BY entity_type, new_norm
--     HAVING count(*) > 1
-- )
-- SELECT entity_type, new_norm, n, distinct_old_norms
-- FROM new_groups
-- WHERE cardinality(distinct_old_norms) > 1
-- ORDER BY n DESC;


-- =============================================================================
-- Migration bookkeeping -- DO NOT UNCOMMENT until this file is renumbered
-- into the real sequence (next free slot after 115_*) and both this DDL and
-- the 4 required code changes have been reviewed together.
-- =============================================================================
-- INSERT INTO koi_migrations (migration_id, checksum)
-- VALUES ('1NN_normalized_text_generated', 'v1_generated_column')
-- ON CONFLICT (migration_id) DO NOTHING;
