-- 108_pgvector_cost_model.sql
--
-- Make the query planner actually USE the HNSW vector indexes.
--
-- PROBLEM (measured 2026-07-31 on personal_koi, 26.5k entities / 619 MB):
-- pgvector ships its distance functions with the DEFAULT procost = 1, i.e. it tells the
-- planner that a 3072-dimension cosine distance costs the same as one integer add. It is
-- also invisible to the cost model that a 3072-dim vector is 12,296 bytes — larger than
-- an 8 KB page — so EVERY embedding lives out-of-line in TOAST (entity_registry: 9 MB
-- heap, relpages=1125, vs 348 MB of TOAST), and Postgres does not cost detoasting either.
--
-- Result: the planner priced a Seq Scan over the whole table at ~1,500 cost units when it
-- actually took 6,500-24,000 ms, and NEVER chose the perfectly valid halfvec HNSW index
-- (which answers the same query in 72-237 ms). Every semantic entity resolution — and any
-- other vector search in the database — was doing a full sequential scan. It only became
-- VISIBLE when book-scale ingests needed ~300 resolutions in one transaction (~33 min)
-- and blew past timeouts, but the defect applies to every vector query.
--
--   before                        Seq Scan          6,691 ms
--   after (no query hints)        HNSW Index Scan     114 ms
--   A/B, same session/statement, only procost varied: 146 ms vs 1,555 ms (warm cache)
--
-- RECALL WAS VERIFIED, NOT ASSUMED: stratified probes across Concept / Person /
-- Organization / Project / Location, 14 probes x top-5 = 70/70 results IDENTICAL to the
-- exact sequential-scan ordering. This is a speedup, not a quality trade.
--
-- *** THE TWO STATEMENTS BELOW MUST BE APPLIED TOGETHER. ***
-- With the COST fix alone, a FILTERED ANN query (the shape entity resolution uses:
-- `WHERE entity_type = $2 ORDER BY embedding <=> $1 LIMIT n`) exhausts its ef_search
-- candidate list before finding a row of the requested type and returns ZERO ROWS —
-- measured, not theorised. That would silently break entity resolution and mint duplicate
-- entities across the graph. hnsw.iterative_scan is what makes filtered ANN correct.
--
-- NOTE: hnsw.iterative_scan requires pgvector >= 0.8.0 and applies to NEW connections
-- only, so RESTART the service after applying (asyncpg holds long-lived pooled conns).
--
-- WHERE THIS MATTERS (surveyed 2026-08-01):
--   personal_koi (laptop)  — APPLIED. 6,691 ms -> 114 ms.
--   a peer node        — DEFECT PRESENT AND MATERIAL (measured 213 ms vs 3.1 ms with
--                            the index forced, 68x). pgvector 0.8.2, applies cleanly.
--                            NOT YET APPLIED — needs a service restart afterwards.
--   another node      — DOES NOT MANIFEST. An older pgvector, vector(1536), a PLAIN
--                            (non-expression) HNSW index, 56 MB table — the planner
--                            already picks Index Scan on every live path. Applying this
--                            there is unnecessary; hnsw.iterative_scan does not even
--                            exist before 0.8.0.
--   a bioregional node — present but negligible (2,293 rows) and pgvector 0.5.1.
--
-- ROLLBACK:
--   ALTER FUNCTION cosine_distance(halfvec, halfvec)     COST 1;
--   ALTER FUNCTION cosine_distance(vector, vector)       COST 1;
--   ALTER FUNCTION cosine_distance(sparsevec, sparsevec) COST 1;
--   ALTER DATABASE <db> RESET hnsw.iterative_scan;
--   -- then restart the service.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
        -- Cost is expressed in units of cpu_operator_cost. 10000 is far above the default
        -- 1 and comfortably exceeds the seq-scan estimate for tables of this size, so the
        -- planner prefers the index without needing per-query enable_seqscan hints.
        -- Guard EACH signature independently. `halfvec` and `sparsevec` only exist from
        -- pgvector 0.7/0.8; an older node still runs a version without them, where an
        -- unguarded halfvec ALTER raises undefined_function and aborts the migration.
        -- Only `vector` is present in every version, so it is the one that must succeed.
        BEGIN
            EXECUTE 'ALTER FUNCTION cosine_distance(halfvec, halfvec) COST 10000';
        EXCEPTION WHEN undefined_function OR undefined_object THEN
            RAISE NOTICE 'halfvec cosine_distance absent (pgvector < 0.7) — skipped';
        END;
        BEGIN
            EXECUTE 'ALTER FUNCTION cosine_distance(sparsevec, sparsevec) COST 10000';
        EXCEPTION WHEN undefined_function OR undefined_object THEN
            RAISE NOTICE 'sparsevec cosine_distance absent (older pgvector) — skipped';
        END;
        EXECUTE 'ALTER FUNCTION cosine_distance(vector, vector) COST 10000';
        RAISE NOTICE 'pgvector distance functions re-costed to 10000';
    ELSE
        RAISE NOTICE 'pgvector extension absent — nothing to do';
    END IF;
END $$;

-- Filtered ANN correctness. Set per-database because it must apply to every connection
-- the pooled service opens. `relaxed_order` keeps scanning until enough rows satisfy the
-- filter, at a small ordering-strictness cost that the recall check above found immaterial.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET hnsw.iterative_scan = %L',
                   current_database(), 'relaxed_order');
    RAISE NOTICE 'hnsw.iterative_scan=relaxed_order set on %  (RESTART THE SERVICE)',
                 current_database();
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'could not set hnsw.iterative_scan (pgvector < 0.8?): %', SQLERRM;
END $$;
