\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 083a: MediaWiki redirect resolver for any database user (fresh installs only)
--
-- 083_mediawiki_redirect_resolver refuses to run unless the database user is the
-- maintainer's own role, so a fresh install by anyone else fails there. The objects
-- it creates (two functions, one view, two indexes) do not depend on the user.
-- This file carries 083's statements after its guard block, except that the view is
-- created only when absent. The runner applies it in place of 083 on fresh personal
-- installs. 083 itself is untouched.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).
-- Like 083, it must not run inside a transaction (CREATE INDEX CONCURRENTLY).

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install

-- ---------------------------------------------------------------------------
-- Function: resolve a redirect title to its canonical page_state.id
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION mediawiki_resolve_redirect(p_title TEXT, p_wiki_id INT)
RETURNS BIGINT
LANGUAGE sql STABLE
AS $$
    WITH RECURSIVE chain(id, title, redirect_target, is_redirect, depth, visited) AS (
        -- Base case: find the page matching the input title
        -- Subquery for tie-break (ORDER BY/LIMIT not allowed in UNION ALL legs)
        SELECT b.id, b.title, b.redirect_target, b.is_redirect, 0, ARRAY[b.title]
        FROM (
            SELECT ps.id, ps.title, ps.redirect_target, ps.is_redirect
            FROM mediawiki_page_state ps
            WHERE ps.title = p_title AND ps.wiki_id = p_wiki_id
            ORDER BY ps.id DESC
            LIMIT 1
        ) b

        UNION ALL

        -- Recursive case: follow redirect_target (strip #section and |label)
        SELECT nxt.id, nxt.title, nxt.redirect_target, nxt.is_redirect,
               c.depth + 1,
               c.visited || nxt.title
        FROM chain c
        JOIN LATERAL (
            SELECT ps.id, ps.title, ps.redirect_target, ps.is_redirect
            FROM mediawiki_page_state ps
            WHERE ps.title = split_part(split_part(c.redirect_target, '#', 1), '|', 1)
              AND ps.wiki_id = p_wiki_id
            ORDER BY ps.id DESC
            LIMIT 1
        ) nxt ON true
        WHERE c.is_redirect = true
          AND c.depth < 5
          AND NOT (nxt.title = ANY(c.visited))  -- cycle detection
    )
    SELECT id FROM chain
    WHERE is_redirect = false  -- found the canonical (non-redirect) page
    ORDER BY depth ASC
    LIMIT 1
$$;

-- ---------------------------------------------------------------------------
-- Helper function: returns (canonical_id, hops, resolution_status) for a
-- single page_state row. The view calls this per-row via LATERAL.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION mediawiki_resolve_redirect_info(
    p_title TEXT, p_wiki_id INT, p_is_redirect BOOLEAN, p_self_id BIGINT
)
RETURNS TABLE(canonical_id BIGINT, hops INT, resolution_status TEXT)
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    cur_title TEXT;
    cur_target TEXT;
    cur_is_redirect BOOLEAN;
    cur_id BIGINT;
    visited TEXT[];
    depth INT := 0;
BEGIN
    -- Non-redirects resolve to self immediately
    IF NOT p_is_redirect THEN
        RETURN QUERY SELECT p_self_id, 0, NULL::TEXT;
        RETURN;
    END IF;

    cur_title := p_title;
    visited := ARRAY[cur_title];

    -- Walk the chain
    LOOP
        -- Get current row's redirect_target
        SELECT ps.redirect_target INTO cur_target
        FROM mediawiki_page_state ps
        WHERE ps.title = cur_title AND ps.wiki_id = p_wiki_id
        ORDER BY ps.id DESC LIMIT 1;

        IF cur_target IS NULL THEN
            -- Should not happen (we started from a redirect), but guard
            RETURN QUERY SELECT NULL::BIGINT, NULL::INT, 'missing_target'::TEXT;
            RETURN;
        END IF;

        -- Strip #section and |label
        cur_target := split_part(split_part(cur_target, '#', 1), '|', 1);

        -- Look up the target
        SELECT ps.id, ps.title, ps.is_redirect
        INTO cur_id, cur_title, cur_is_redirect
        FROM mediawiki_page_state ps
        WHERE ps.title = cur_target AND ps.wiki_id = p_wiki_id
        ORDER BY ps.id DESC LIMIT 1;

        depth := depth + 1;

        IF cur_id IS NULL THEN
            RETURN QUERY SELECT NULL::BIGINT, NULL::INT, 'missing_target'::TEXT;
            RETURN;
        END IF;

        IF NOT cur_is_redirect THEN
            -- Found canonical page
            RETURN QUERY SELECT cur_id, depth, 'resolved'::TEXT;
            RETURN;
        END IF;

        IF cur_title = ANY(visited) THEN
            RETURN QUERY SELECT NULL::BIGINT, NULL::INT, 'cycle'::TEXT;
            RETURN;
        END IF;

        IF depth >= 5 THEN
            RETURN QUERY SELECT NULL::BIGINT, NULL::INT, 'depth_exceeded'::TEXT;
            RETURN;
        END IF;

        visited := visited || cur_title;
    END LOOP;
END
$$;

-- ---------------------------------------------------------------------------
-- View: every page_state row enriched with canonical resolution info
-- ---------------------------------------------------------------------------

-- 083 drops and recreates this view. 083a creates it only when it is absent, so it never
-- drops an existing view (and the grants on it, such as koi_sql_ro's).
SELECT to_regclass('public.v_mediawiki_page_resolved') IS NULL AS koi_create_mw_view \gset
\if :koi_create_mw_view
CREATE VIEW v_mediawiki_page_resolved AS
SELECT
    ps.id,
    ps.wiki_id,
    ps.title,
    ps.is_redirect,
    ps.redirect_target,
    info.canonical_id,
    -- Derive canonical_page_id, canonical_title, canonical_rid from canonical_id
    canon.page_id AS canonical_page_id,
    canon.title AS canonical_title,
    CASE WHEN info.canonical_id IS NOT NULL
         THEN 'mediawiki:' || w.wiki_name || ':' || canon.page_id
         ELSE NULL
    END AS canonical_rid,
    info.hops,
    info.resolution_status
FROM mediawiki_page_state ps
JOIN mediawiki_wikis w ON w.id = ps.wiki_id
LEFT JOIN LATERAL mediawiki_resolve_redirect_info(
    ps.title, ps.wiki_id, ps.is_redirect, ps.id
) info ON true
LEFT JOIN mediawiki_page_state canon ON canon.id = info.canonical_id;
\endif

-- ---------------------------------------------------------------------------
-- Supporting indexes (CONCURRENTLY — no write blocking)
-- ---------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mw_page_wiki_title
    ON mediawiki_page_state (wiki_id, title);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mw_page_wiki_redirect_target
    ON mediawiki_page_state (wiki_id, redirect_target)
    WHERE is_redirect = true;

\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
