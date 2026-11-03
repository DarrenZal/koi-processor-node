"""Tier 2b semantic match is served by the HNSW index and keeps its contract.

Contract: the single most similar live row (merged_into IS NULL) of the requested
type, or of any type for the untyped variant, whose cosine similarity exceeds the
threshold, or none.

Until 2026-10-12 the query ordered by the computed similarity with the threshold in
WHERE. The planner cannot serve that from idx_entity_registry_embedding_3072_hnsw,
so it sorted every live row of the type (the whole table without a type): 2-58 s
per call under load, and /ingest of an unmatched name hit the pool's 60 s
command_timeout. The fix orders an inner LIMIT 10 by the raw distance expression
and applies the threshold and final order outside.

Recall against the exact form, measured read-only on personal_koi 2026-10-12 with
stored embeddings as queries and each probe's own row excluded: see the
RECALL_EVIDENCE note at the bottom of this file.

Every test runs against KOI_TEST_DATABASE_URL inside a transaction that is rolled back,
except the bare-connection leak test, which writes nothing.
"""

from __future__ import annotations

import uuid

import asyncpg
import numpy as np
import pytest
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio

DIM = 3072
HNSW_INDEX = "idx_entity_registry_embedding_3072_hnsw"

# The pre-2026-10-12 Tier 2b SQL, kept verbatim as the exact reference.
OLD_TYPED = """
    SELECT id, fuseki_uri, entity_text, entity_type,
           1 - (embedding_3072::halfvec(3072)
                <=> $1::halfvec(3072)) AS similarity
    FROM entity_registry
    WHERE embedding_3072 IS NOT NULL
      AND entity_type = $2
      AND merged_into IS NULL
      AND 1 - (embedding_3072::halfvec(3072)
               <=> $1::halfvec(3072)) > $3
    ORDER BY similarity DESC
    LIMIT 1
"""
OLD_ANY_TYPE = """
    SELECT id, fuseki_uri, entity_text, entity_type,
           1 - (embedding_3072::halfvec(3072)
                <=> $1::halfvec(3072)) AS similarity
    FROM entity_registry
    WHERE embedding_3072 IS NOT NULL
      AND merged_into IS NULL
      AND 1 - (embedding_3072::halfvec(3072)
               <=> $1::halfvec(3072)) > $2
    ORDER BY similarity DESC
    LIMIT 1
"""


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()   # nothing this file inserts ever persists
    await c.close()


def _tag() -> str:
    return uuid.uuid4().hex[:10]


def _unit(rng, dim=DIM):
    v = rng.standard_normal(dim)
    return v / np.linalg.norm(v)


def _at_similarity(q, sim, rng):
    """A unit vector whose cosine similarity to unit vector q is exactly sim."""
    o = rng.standard_normal(len(q))
    o -= o.dot(q) * q
    o /= np.linalg.norm(o)
    return sim * q + np.sqrt(1.0 - sim * sim) * o


def _vec(v) -> str:
    return "[" + ",".join(f"{x:.7f}" for x in v) + "]"


async def _insert(conn, rows):
    """rows: (uri, text, type, vector, merged_into)."""
    await conn.executemany(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, "
        "embedding_3072, merged_into) VALUES ($1, $2, $3, $4, $5::vector, $6)",
        [(u, t, ty, t.lower(), _vec(v), m) for (u, t, ty, v, m) in rows])


async def _force_index(conn):
    """Make the planner use the HNSW index on a near-empty test table.

    personal_koi gets there through migration 109's cost model; the test database
    does not carry it, and at test-table sizes a seq scan would win, so the tests
    would otherwise never exercise the index (or the filtered-scan starvation).
    """
    await conn.execute("SET LOCAL enable_seqscan = off")
    await conn.execute("SET LOCAL enable_bitmapscan = off")
    await conn.execute("SET LOCAL enable_sort = off")


async def _plan(conn, sql, *args) -> str:
    rows = await conn.fetch("EXPLAIN " + sql, *args)
    return "\n".join(r[0] for r in rows)


async def _setting(conn) -> str | None:
    return await conn.fetchval("SELECT current_setting('hnsw.iterative_scan', true)")


# ---------------------------------------------------------------------------
# Same answer as the exact query, from the index
# ---------------------------------------------------------------------------

async def test_typed_query_uses_the_hnsw_index(conn):
    from api import personal_ingest_api as pia

    await _force_index(conn)
    q = _vec(_unit(np.random.default_rng(1)))
    plan = await _plan(conn, pia._TIER2B_SQL_TYPED, q, "Concept", 0.88)
    assert HNSW_INDEX in plan, plan
    plan = await _plan(conn, pia._TIER2B_SQL_ANY_TYPE, q, 0.88)
    assert HNSW_INDEX in plan, plan


async def test_matches_exact_and_respects_type_and_merged_filters(conn):
    from api import personal_ingest_api as pia

    await _force_index(conn)
    rng = np.random.default_rng(7)
    t = _tag()
    typ, other = f"T2bTest{t}", f"T2bOther{t}"
    q = _unit(rng)
    best = f"orn:test.t2b:{t}:best"
    survivor = f"orn:test.t2b:{t}:survivor"
    rows = [
        (best, f"Best {t}", typ, _at_similarity(q, 0.95, rng), None),
        (survivor, f"Survivor {t}", typ, _at_similarity(q, 0.20, rng), None),
        # Closer than best, but merged away: must never be returned.
        (f"orn:test.t2b:{t}:tomb", f"Tomb {t}", typ, _at_similarity(q, 0.99, rng), survivor),
        # Closer than best, but another type: excluded by the typed variant only.
        (f"orn:test.t2b:{t}:other", f"Other {t}", other, _at_similarity(q, 0.98, rng), None),
    ]
    rows += [(f"orn:test.t2b:{t}:n{i}", f"Noise {i} {t}", typ,
              _at_similarity(q, float(s), rng), None)
             for i, s in enumerate(rng.uniform(0.0, 0.93, 60))]
    await _insert(conn, rows)

    got = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, _vec(q), typ, 0.88)
    exact = await conn.fetchrow(OLD_TYPED, _vec(q), typ, 0.88)
    assert got["fuseki_uri"] == exact["fuseki_uri"] == best
    assert got["similarity"] == pytest.approx(exact["similarity"], abs=1e-9)
    assert got["similarity"] == pytest.approx(0.95, abs=0.005)

    got = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_ANY_TYPE, _vec(q), 0.88)
    exact = await conn.fetchrow(OLD_ANY_TYPE, _vec(q), 0.88)
    assert got["fuseki_uri"] == exact["fuseki_uri"] == f"orn:test.t2b:{t}:other"

    # Further queries scattered around the cluster: every answer equals the exact one.
    for _ in range(8):
        qq = _at_similarity(q, float(rng.uniform(0.85, 0.99)), rng)
        for thr in (0.5, 0.88):
            got = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, _vec(qq), typ, thr)
            exact = await conn.fetchrow(OLD_TYPED, _vec(qq), typ, thr)
            assert (got and got["fuseki_uri"]) == (exact and exact["fuseki_uri"])


async def test_returns_none_below_the_threshold(conn):
    from api import personal_ingest_api as pia

    await _force_index(conn)
    rng = np.random.default_rng(11)
    t = _tag()
    typ = f"T2bTest{t}"
    q = _unit(rng)
    near = f"orn:test.t2b:{t}:near"
    await _insert(conn, [(near, f"Near {t}", typ, _at_similarity(q, 0.80, rng), None)])

    assert await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, _vec(q), typ, 0.85) is None
    assert await conn.fetchrow(OLD_TYPED, _vec(q), typ, 0.85) is None
    # Positive twin: the same row clears a lower threshold.
    got = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, _vec(q), typ, 0.75)
    assert got["fuseki_uri"] == near


async def test_iterative_scan_finds_a_match_behind_other_types(conn):
    """A filtered HNSW scan must not starve when other types crowd the neighbourhood.

    12 rows of another type sit closer to the query than the only row of the
    requested type, and ef_search is cut to 4. Without iterative scan the index
    returns 4 candidates, the type filter removes all of them, and the result is
    empty (migration 109 measured the same on personal_koi at ef_search 40).
    The test database has no database-level hnsw.iterative_scan, so the helper's
    own SET LOCAL is what makes this pass; the negative twin proves the fixture
    really starves.

    Why so small: with 60-200 crowd rows at ef_search 40 the target, the outlier
    of a tight cluster, was unreachable in some HNSW builds (the scan ended after
    53 of 61 rows, iterative or not), so the test depended on the random graph
    rather than on the setting. 12 rows passed 16 of 16 builds.
    """
    from api import personal_ingest_api as pia

    await _force_index(conn)
    await conn.execute("SET LOCAL hnsw.ef_search = 4")
    rng = np.random.default_rng(23)
    t = _tag()
    typ, crowd = f"T2bRare{t}", f"T2bCrowd{t}"
    q = _unit(rng)
    target = f"orn:test.t2b:{t}:target"
    rows = [(f"orn:test.t2b:{t}:c{i}", f"Crowd {i} {t}", crowd,
             _at_similarity(q, float(s), rng), None)
            for i, s in enumerate(rng.uniform(0.93, 0.99, 12))]
    rows.append((target, f"Target {t}", typ, _at_similarity(q, 0.90, rng), None))
    await _insert(conn, rows)

    await conn.execute("SET LOCAL hnsw.iterative_scan = off")
    starved = await conn.fetchrow(pia._TIER2B_SQL_TYPED, _vec(q), typ, 0.85)
    assert starved is None, "fixture does not starve a filtered scan; test proves nothing"

    got = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, _vec(q), typ, 0.85)
    assert got is not None and got["fuseki_uri"] == target


async def test_the_statement_runs_with_iterative_scan_on(conn):
    """Deterministic twin of the starvation test: the setting is live in the call."""
    from api import personal_ingest_api as pia

    await conn.execute("SELECT '[1,0]'::vector <=> '[0,1]'::vector")
    await conn.execute("SET LOCAL hnsw.iterative_scan = off")
    row = await pia._fetchrow_hnsw(
        conn, "SELECT current_setting('hnsw.iterative_scan') AS v")
    assert row["v"] == "relaxed_order"
    assert await _setting(conn) == "off"


# ---------------------------------------------------------------------------
# The setting does not leak
# ---------------------------------------------------------------------------

async def test_setting_does_not_leak_on_a_bare_connection():
    """Pooled connections are long-lived: nothing may outlive the call."""
    from api import personal_ingest_api as pia
    c = await asyncpg.connect(targets.database_url())
    try:
        # Load pgvector so the GUC exists, then set a sentinel the helper never uses.
        await c.execute("SELECT '[1,0]'::vector <=> '[0,1]'::vector")
        await c.execute("SET hnsw.iterative_scan = strict_order")
        q = _vec(_unit(np.random.default_rng(3)))
        assert await pia._fetchrow_hnsw(
            c, pia._TIER2B_SQL_TYPED, q, f"T2bNoSuchType{_tag()}", 0.5) is None
        assert not c.is_in_transaction()
        assert await _setting(c) == "strict_order"
    finally:
        await c.close()


async def test_setting_does_not_leak_out_of_a_released_savepoint(conn):
    """/ingest calls resolve_entity inside a per-entity savepoint that is RELEASEd.

    A SET LOCAL issued in a savepoint survives RELEASE and lasts to the outer
    commit. The helper rolls its own savepoint back, so the caller's value stands
    both directly and through an enclosing released savepoint.
    """
    from api import personal_ingest_api as pia

    await conn.execute("SELECT '[1,0]'::vector <=> '[0,1]'::vector")
    await conn.execute("SET LOCAL hnsw.iterative_scan = strict_order")
    q = _vec(_unit(np.random.default_rng(5)))
    typ = f"T2bNoSuchType{_tag()}"

    await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, q, typ, 0.5)
    assert await _setting(conn) == "strict_order"

    async with conn.transaction():   # the /ingest per-entity savepoint
        await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, q, typ, 0.5)
    assert await _setting(conn) == "strict_order"


async def test_a_failed_query_leaves_the_caller_transaction_usable(conn):
    from api import personal_ingest_api as pia

    await conn.execute("SELECT '[1,0]'::vector <=> '[0,1]'::vector")
    await conn.execute("SET LOCAL hnsw.iterative_scan = strict_order")
    with pytest.raises(asyncpg.PostgresError):
        await pia._fetchrow_hnsw(conn, "SELECT * FROM t2b_no_such_table")
    assert await conn.fetchval("SELECT 1") == 1
    assert await _setting(conn) == "strict_order"


async def _plan_counts(conn, sql):
    """(generic_plans, custom_plans) of asyncpg's cached statement for sql."""
    row = await conn.fetchrow(
        "SELECT generic_plans, custom_plans FROM pg_prepared_statements "
        "WHERE statement = $1", sql)
    return (row["generic_plans"], row["custom_plans"]) if row else None


async def test_cached_statement_is_planned_per_call_not_generically(conn):
    """asyncpg caches the statement per connection; it must never run a generic plan.

    After 5 executions PostgreSQL may switch a prepared statement to a generic plan.
    For Tier 2b that plan cannot see the type value and is always the HNSW scan with
    `entity_type = $2` as a post-filter, which for a rare type walks ~18k index tuples
    (8-43 s on personal_koi under load, 2026-10-12 review). The helper forces a
    custom plan on every call. Positive twin: forcing a generic plan on the same
    cached statement moves the generic counter, so the counters observe the
    statement asyncpg actually runs.
    """
    from api import personal_ingest_api as pia

    await conn.execute("SELECT '[1,0]'::vector <=> '[0,1]'::vector")
    q = _vec(_unit(np.random.default_rng(41)))
    sql = pia._TIER2B_SQL_TYPED
    types = [f"T2bMix{i}{_tag()}" for i in range(7)] + [f"T2bRare{_tag()}"]
    for typ in types:
        await pia._fetchrow_hnsw(conn, sql, q, typ, 0.5)
    generic, custom = await _plan_counts(conn, sql)
    assert generic == 0 and custom >= len(types), (generic, custom)
    assert await conn.fetchval("SELECT current_setting('plan_cache_mode')") == "auto"

    async with conn.transaction():
        await conn.execute("SET LOCAL plan_cache_mode = force_generic_plan")
        await conn.fetchrow(sql, q, types[-1], 0.5)
    assert (await _plan_counts(conn, sql))[0] == 1


# ---------------------------------------------------------------------------
# resolve_entity reaches the new query
# ---------------------------------------------------------------------------

async def test_resolve_entity_tier2b_runs_the_index_query(conn, monkeypatch):
    from api import personal_ingest_api as pia
    from api import resolver_shadow
    from api.personal_ingest_api import ExtractedEntity, resolve_entity

    await _force_index(conn)
    rng = np.random.default_rng(31)
    t = _tag()
    typ = f"T2bTest{t}"
    q = _unit(rng)
    target = f"orn:test.t2b:{t}:target"
    await _insert(conn, [(target, f"Xylophant Quorum {t}", typ, _at_similarity(q, 0.97, rng), None)])

    calls = []
    real = pia._fetchrow_hnsw

    async def spy(c, sql, *args):
        row = await real(c, sql, *args)
        calls.append((sql, args, row))
        return row

    async def fake_embedding(_text, *_a, **_kw):
        return [float(x) for x in q]

    async def large_type(_conn, _typ):
        return pia._TIER2B_EXACT_MAX_ROWS + 1

    monkeypatch.setattr(pia, "_fetchrow_hnsw", spy)
    # Route this 1-row type to the HNSW form; the size routing itself is tested in
    # tests/test_tier2b_plan_choice.py.
    monkeypatch.setattr(pia, "_tier2b_live_count", large_type)
    monkeypatch.setattr(pia, "generate_query_embedding", fake_embedding)
    monkeypatch.setattr(pia, "embedding_provider", object(), raising=False)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", True, raising=False)
    monkeypatch.setattr(pia, "_log_resolver_decision", lambda *a, **kw: None)
    monkeypatch.setattr(resolver_shadow, "emit_nonblocking", lambda *_a, **_kw: None)

    # A name no lexical tier can match, so resolution reaches Tier 2b.
    await resolve_entity(conn, ExtractedEntity(name=f"Brindle Okapi {t}", type=typ))

    assert len(calls) == 1
    sql, args, row = calls[0]
    assert sql is pia._TIER2B_SQL_TYPED and args[1] == typ
    assert row is not None and row["fuseki_uri"] == target


# RECALL_EVIDENCE (personal_koi, read-only, 2026-10-12, load average 40-170).
# Probes: 40 rows' stored embedding_3072 as the query, each excluding its own row
# (33 live rows over 11 types: Concept 7, Person 7, Project 4, Organization 4,
# Claim 3, Location 2, Meeting 2, Protocol, Practice, Decision, Pattern; plus 7
# merged-away rows), and 4 of them again through the untyped variant.
# Ground truth: the exact top-2 per probe over every live row in one chunked
# sequential pass. New form: the inner CTE above with iterative_scan
# relaxed_order and default ef_search 40, max_scan_tuples 20000.
#   typed:   40/40 same pick after the threshold, 40/40 same nearest row
#   untyped:  4/4 same pick, 4/4 same nearest row
# Three probes cleared the threshold (Meeting 0.981 and 0.950, Person 0.977).
# Closest to their threshold without clearing it: Claim 0.851 and Concept 0.859
# (0.88), Project 0.883 (0.92). Tightest margins between first and second, all
# ranked correctly: Person 0.7043 vs 0.7030, Location 0.7359 vs 0.7345.
# The 9-row Decision type returned all 8 other rows, so the filtered scan did
# not starve. EXPLAIN ANALYZE, old -> new: Person 1,908 -> 51 ms, Concept
# 4,573 -> 368 ms, untyped 10,901 -> 53 ms; shared buffers about 69k -> 1k.
