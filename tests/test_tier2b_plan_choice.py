"""Tier 2b picks the exact or the HNSW form by the size of the type (2026-10-12).

After d74dc1a every typed Tier 2b call used the HNSW form, which post-filters
entity_type. For a mid-size type a query far from it walks thousands of index tuples
before it collects 10 rows of the type: on personal_koi (read-only, load average
15-23) CaseStudy (374 rows) took 5.8 s, Incident (414) 3.1 s and Claim (2582) 1.6 s,
and the Claim walk returned the wrong nearest row. The exact form (inner ORDER BY the
computed similarity, which the index cannot serve) was faster than that for every
type up to Claim, and HNSW was faster from Organization (4490) up. resolve_entity now
uses the exact form for types with at most _TIER2B_EXACT_MAX_ROWS (4000) live
embedded rows, from a cached per-type count, and the HNSW form above that.

Every database test runs against KOI_TEST_DATABASE_URL inside a transaction that is
rolled back.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid

import asyncpg
import numpy as np
import pytest
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio

DIM = 3072
HNSW_INDEX = "idx_entity_registry_embedding_3072_hnsw"

# The pre-2026-10-12 Tier 2b SQL, verbatim: the exact contract.
EXACT_REFERENCE = """
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


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()
    await c.close()


@pytest.fixture
def pia(monkeypatch):
    """The module, with an empty, expired type-count cache and a fresh lock."""
    from api import personal_ingest_api as pia

    monkeypatch.setattr(pia, "_tier2b_type_counts", {})
    monkeypatch.setattr(pia, "_tier2b_counts_expire_at", 0.0)
    monkeypatch.setattr(pia, "_tier2b_counts_failing", False)
    monkeypatch.setattr(pia, "_tier2b_counts_lock", asyncio.Lock())
    return pia


def _tag() -> str:
    return uuid.uuid4().hex[:10]


def _unit(rng, dim=DIM):
    v = rng.standard_normal(dim)
    return v / np.linalg.norm(v)


def _at_similarity(q, sim, rng):
    o = rng.standard_normal(len(q))
    o -= o.dot(q) * q
    o /= np.linalg.norm(o)
    return sim * q + np.sqrt(1.0 - sim * sim) * o


def _vec(v) -> str:
    return "[" + ",".join(f"{x:.7f}" for x in v) + "]"


async def _insert(conn, rows):
    """rows: (uri, text, type, vector or None, merged_into)."""
    await conn.executemany(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, "
        "embedding_3072, merged_into) VALUES ($1, $2, $3, $4, $5::vector, $6)",
        [(u, t, ty, t.lower(), None if v is None else _vec(v), m) for (u, t, ty, v, m) in rows])


async def _force_index(conn):
    """Make the planner prefer the HNSW index wherever it can use it (see
    tests/test_tier2b_semantic_hnsw.py): the test table is too small otherwise."""
    await conn.execute("SET LOCAL enable_seqscan = off")
    await conn.execute("SET LOCAL enable_bitmapscan = off")
    await conn.execute("SET LOCAL enable_sort = off")


async def _plan(conn, sql, *args) -> str:
    rows = await conn.fetch("EXPLAIN " + sql, *args)
    return "\n".join(r[0] for r in rows)


def _spy_tier2b(monkeypatch, pia, q):
    """Record every _fetchrow_hnsw call resolve_entity makes; stub the embedding."""
    from api import resolver_shadow

    calls = []
    real = pia._fetchrow_hnsw

    async def spy(c, sql, *args, **kw):
        row = await real(c, sql, *args, **kw)
        calls.append((sql, args, row))
        return row

    async def fake_embedding(_text, *_a, **_kw):
        return [float(x) for x in q]

    monkeypatch.setattr(pia, "_fetchrow_hnsw", spy)
    monkeypatch.setattr(pia, "generate_query_embedding", fake_embedding)
    monkeypatch.setattr(pia, "embedding_provider", object(), raising=False)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", True, raising=False)
    monkeypatch.setattr(pia, "_log_resolver_decision", lambda *a, **kw: None)
    monkeypatch.setattr(resolver_shadow, "emit_nonblocking", lambda *_a, **_kw: None)
    return calls


def _tier2b_calls(pia, calls):
    return [c for c in calls if c[0] in (pia._TIER2B_SQL_TYPED, pia._TIER2B_SQL_TYPED_EXACT)]


def _refreshes(pia, calls):
    return [c for c in calls if c[0] is pia._TIER2B_TYPE_COUNTS_SQL]


async def _resolve_sql(conn, pia, calls, typ):
    """Resolve a name no lexical tier matches; return the Tier 2b SQL and row."""
    from api.personal_ingest_api import ExtractedEntity, resolve_entity

    calls.clear()
    await resolve_entity(conn, ExtractedEntity(name=f"Brindle Okapi {_tag()}", type=typ))
    tier2b = _tier2b_calls(pia, calls)
    assert len(tier2b) == 1
    sql, args, row = tier2b[0]
    assert args[1] == typ
    return sql, row


# ---------------------------------------------------------------------------
# The exact form: plan and results
# ---------------------------------------------------------------------------

async def test_exact_form_never_plans_the_hnsw_index(conn, pia):
    """Even with every alternative disabled the exact form cannot use the index.

    Positive twin: the HNSW form under the same settings does use it, so the
    absence is a property of the SQL, not of the settings.
    """
    await _force_index(conn)
    await conn.execute("SET LOCAL hnsw.iterative_scan = relaxed_order")
    await conn.execute("SET LOCAL plan_cache_mode = force_custom_plan")
    q = _vec(_unit(np.random.default_rng(1)))
    for typ in ("Location", "Concept", f"PcNoSuchType{_tag()}"):
        assert HNSW_INDEX not in await _plan(conn, pia._TIER2B_SQL_TYPED_EXACT, q, typ, 0.88)
        assert HNSW_INDEX in await _plan(conn, pia._TIER2B_SQL_TYPED, q, typ, 0.88)


async def test_exact_form_returns_the_hnsw_row_and_the_reference_row(conn, pia):
    rng = np.random.default_rng(7)
    t = _tag()
    typ, other = f"PcTest{t}", f"PcOther{t}"
    q = _unit(rng)
    best = f"orn:test.pc:{t}:best"
    survivor = f"orn:test.pc:{t}:survivor"
    rows = [
        (best, f"Best {t}", typ, _at_similarity(q, 0.95, rng), None),
        (survivor, f"Survivor {t}", typ, _at_similarity(q, 0.20, rng), None),
        # Closer than best, but merged away: never returned.
        (f"orn:test.pc:{t}:tomb", f"Tomb {t}", typ, _at_similarity(q, 0.99, rng), survivor),
        # Closer than best, but another type.
        (f"orn:test.pc:{t}:other", f"Other {t}", other, _at_similarity(q, 0.98, rng), None),
        # Unembedded row of the type.
        (f"orn:test.pc:{t}:noemb", f"NoEmb {t}", typ, None, None),
    ]
    rows += [(f"orn:test.pc:{t}:n{i}", f"Noise {i} {t}", typ,
              _at_similarity(q, float(s), rng), None)
             for i, s in enumerate(rng.uniform(0.0, 0.93, 40))]
    await _insert(conn, rows)

    async def three(qv, typ_, thr):
        exact = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED_EXACT, _vec(qv), typ_, thr)
        hnsw = await pia._fetchrow_hnsw(conn, pia._TIER2B_SQL_TYPED, _vec(qv), typ_, thr)
        ref = await conn.fetchrow(EXACT_REFERENCE, _vec(qv), typ_, thr)
        return exact, hnsw, ref

    exact, hnsw, ref = await three(q, typ, 0.88)
    assert exact["fuseki_uri"] == hnsw["fuseki_uri"] == ref["fuseki_uri"] == best
    assert exact["similarity"] == pytest.approx(ref["similarity"], abs=1e-9)
    assert exact["similarity"] == pytest.approx(hnsw["similarity"], abs=1e-9)
    assert set(exact.keys()) == set(hnsw.keys())

    for _ in range(8):
        qq = _at_similarity(q, float(rng.uniform(0.80, 0.99)), rng)
        for thr in (0.5, 0.88, 0.97):
            exact, hnsw, ref = await three(qq, typ, thr)
            assert ((exact and exact["fuseki_uri"]) == (hnsw and hnsw["fuseki_uri"])
                    == (ref and ref["fuseki_uri"]))

    # None below the threshold, and for a type with no rows.
    assert (await three(q, typ, 0.999)) == (None, None, None)
    assert (await three(q, f"PcNoSuchType{t}", 0.0)) == (None, None, None)


async def test_small_type_is_resolved_without_the_hnsw_walk(conn, pia, monkeypatch):
    """A small type is routed to the exact form, and the exact form finds its row
    whether or not an HNSW walk would.

    On personal_koi the walk for a mid-size type ran thousands of tuples (seconds)
    and, for Claim, returned the wrong row. Here 12 rows of another type crowd the
    only row of the requested type, and a walk stopped after ef_search 4 candidates
    (iterative scan off) returns nothing: the fixture check, as in
    tests/test_tier2b_semantic_hnsw.py. Inside resolve_entity, _fetchrow_hnsw turns
    iterative scan on, which would let the HNSW form reach the row too, so the
    routing itself is proved by the `sql is _TIER2B_SQL_TYPED_EXACT` assertion; the
    row assertion shows the exact form answers correctly.
    """
    await _force_index(conn)
    await conn.execute("SET LOCAL hnsw.ef_search = 4")
    rng = np.random.default_rng(23)
    t = _tag()
    typ, crowd = f"PcRare{t}", f"PcCrowd{t}"
    q = _unit(rng)
    target = f"orn:test.pc:{t}:target"
    rows = [(f"orn:test.pc:{t}:c{i}", f"Crowd {i} {t}", crowd,
             _at_similarity(q, float(s), rng), None)
            for i, s in enumerate(rng.uniform(0.975, 0.995, 12))]
    # 0.97 clears the semantic_threshold (0.95) resolve_entity uses for an unknown type.
    rows.append((target, f"Xylophant Quorum {t}", typ, _at_similarity(q, 0.97, rng), None))
    await _insert(conn, rows)

    await conn.execute("SET LOCAL hnsw.iterative_scan = off")
    short = await conn.fetchrow(pia._TIER2B_SQL_TYPED, _vec(q), typ, 0.85)
    assert short is None, "fixture does not cut the HNSW walk short; test proves nothing"
    assert (await conn.fetchrow(EXACT_REFERENCE, _vec(q), typ, 0.85))["fuseki_uri"] == target

    calls = _spy_tier2b(monkeypatch, pia, q)
    sql, row = await _resolve_sql(conn, pia, calls, typ)
    assert sql is pia._TIER2B_SQL_TYPED_EXACT
    assert row is not None and row["fuseki_uri"] == target


# ---------------------------------------------------------------------------
# resolve_entity routes by type size
# ---------------------------------------------------------------------------

async def test_routing_at_the_threshold(conn, pia, monkeypatch):
    """count <= 4000: exact. count > 4000: HNSW. A type the cache lacks: exact."""
    assert pia._TIER2B_EXACT_MAX_ROWS == 4000
    t = _tag()
    at, above, absent = f"PcAt{t}", f"PcAbove{t}", f"PcAbsent{t}"
    calls = _spy_tier2b(monkeypatch, pia, _unit(np.random.default_rng(11)))
    monkeypatch.setattr(pia, "_tier2b_type_counts", {at: 4000, above: 4001})
    monkeypatch.setattr(pia, "_tier2b_counts_expire_at", time.monotonic() + 600)

    assert (await _resolve_sql(conn, pia, calls, at))[0] is pia._TIER2B_SQL_TYPED_EXACT
    assert (await _resolve_sql(conn, pia, calls, above))[0] is pia._TIER2B_SQL_TYPED
    assert (await _resolve_sql(conn, pia, calls, absent))[0] is pia._TIER2B_SQL_TYPED_EXACT
    assert _refreshes(pia, calls) == []


async def test_routing_counts_only_live_embedded_rows(conn, pia, monkeypatch):
    """The cached count comes from the database: merged-away and unembedded rows of
    the type do not count, so 5 live rows route exact at 5 and HNSW at 4."""
    rng = np.random.default_rng(31)
    t = _tag()
    typ = f"PcSized{t}"
    q = _unit(rng)
    live = [(f"orn:test.pc:{t}:l{i}", f"Live {i} {t}", typ, _at_similarity(q, 0.5, rng), None)
            for i in range(5)]
    dead = [(f"orn:test.pc:{t}:m{i}", f"Merged {i} {t}", typ, _at_similarity(q, 0.5, rng),
             live[0][0]) for i in range(3)]
    noemb = [(f"orn:test.pc:{t}:e{i}", f"NoEmb {i} {t}", typ, None, None) for i in range(3)]
    await _insert(conn, live + dead + noemb)
    calls = _spy_tier2b(monkeypatch, pia, q)

    monkeypatch.setattr(pia, "_TIER2B_EXACT_MAX_ROWS", 5)
    assert (await _resolve_sql(conn, pia, calls, typ))[0] is pia._TIER2B_SQL_TYPED_EXACT
    assert len(_refreshes(pia, calls)) == 1
    assert pia._tier2b_type_counts[typ] == 5
    monkeypatch.setattr(pia, "_TIER2B_EXACT_MAX_ROWS", 4)
    assert (await _resolve_sql(conn, pia, calls, typ))[0] is pia._TIER2B_SQL_TYPED


# ---------------------------------------------------------------------------
# The count cache
# ---------------------------------------------------------------------------

async def test_ttl_expiry_triggers_one_refresh(conn, pia, monkeypatch):
    rng = np.random.default_rng(41)
    t = _tag()
    typ = f"PcCount{t}"
    await _insert(conn, [(f"orn:test.pc:{t}:{i}", f"C {i} {t}", typ, _unit(rng), None)
                         for i in range(3)])
    calls = _spy_tier2b(monkeypatch, pia, _unit(rng))

    assert await pia._tier2b_live_count(conn, typ) == 3
    assert await pia._tier2b_live_count(conn, f"PcNoSuchType{t}") == 0
    assert len(_refreshes(pia, calls)) == 1
    assert pia._tier2b_counts_expire_at == pytest.approx(
        time.monotonic() + pia._TIER2B_COUNT_TTL_S, abs=5)

    await _insert(conn, [(f"orn:test.pc:{t}:x", f"X {t}", typ, _unit(rng), None)])
    assert await pia._tier2b_live_count(conn, typ) == 3       # within the TTL
    assert len(_refreshes(pia, calls)) == 1

    pia._tier2b_counts_expire_at = time.monotonic() - 1        # TTL elapsed
    assert await pia._tier2b_live_count(conn, typ) == 4
    assert await pia._tier2b_live_count(conn, typ) == 4
    assert len(_refreshes(pia, calls)) == 2


async def test_failed_refresh_keeps_the_last_counts_and_resolve_still_works(
        conn, pia, monkeypatch, caplog):
    """A failed refresh keeps the last good counts (the big type stays on HNSW),
    is logged once per failing streak, waits the retry interval, never aborts the
    caller's transaction, and a later success logs the recovery. Before any
    successful refresh a failure leaves every type on the exact form."""
    rng = np.random.default_rng(53)
    t = _tag()
    typ = f"PcBig{t}"
    q = _unit(rng)
    target = f"orn:test.pc:{t}:target"
    await _insert(conn, [(target, f"Xylophant Quorum {t}", typ, _at_similarity(q, 0.97, rng), None)])
    calls = _spy_tier2b(monkeypatch, pia, q)
    real_counts_sql = pia._TIER2B_TYPE_COUNTS_SQL
    monkeypatch.setattr(pia, "_TIER2B_TYPE_COUNTS_SQL", "SELECT * FROM pc_no_such_table")

    with caplog.at_level(logging.INFO, logger=pia.logger.name):
        # Never filled, refresh fails: exact, and the row is still found.
        sql, row = await _resolve_sql(conn, pia, calls, typ)
        assert sql is pia._TIER2B_SQL_TYPED_EXACT
        assert row is not None and row["fuseki_uri"] == target
        assert await conn.fetchval("SELECT 1") == 1
        assert pia._tier2b_type_counts == {}
        assert pia._tier2b_counts_expire_at == pytest.approx(
            time.monotonic() + pia._TIER2B_COUNT_RETRY_S, abs=5)

        # Filled earlier, refresh fails again: the last counts route HNSW.
        pia._tier2b_type_counts = {typ: 9000}
        pia._tier2b_counts_expire_at = 0.0                     # retry interval elapsed
        sql, row = await _resolve_sql(conn, pia, calls, typ)
        assert sql is pia._TIER2B_SQL_TYPED
        assert pia._tier2b_type_counts == {typ: 9000}
        assert await conn.fetchval("SELECT 1") == 1

        # The query works again: real counts replace the kept ones, recovery logged.
        monkeypatch.setattr(pia, "_TIER2B_TYPE_COUNTS_SQL", real_counts_sql)
        pia._tier2b_counts_expire_at = 0.0
        assert (await _resolve_sql(conn, pia, calls, typ))[0] is pia._TIER2B_SQL_TYPED_EXACT
        assert pia._tier2b_type_counts[typ] == 1
    msgs = [r.getMessage() for r in caplog.records]
    assert len([m for m in msgs if "type-count refresh failed" in m]) == 1
    assert len([m for m in msgs if "type-count refresh recovered" in m]) == 1
    assert pia._tier2b_counts_failing is False


async def test_count_refresh_has_its_own_statement_timeout(conn, pia):
    """The count query is capped server-side, and the cap does not outlive it."""
    assert pia._TIER2B_COUNT_TIMEOUT_MS == 5000
    before = await conn.fetchval("SHOW statement_timeout")
    with pytest.raises(asyncpg.QueryCanceledError):
        await pia._fetchrow_hnsw(conn, "SELECT pg_sleep(2)", statement_timeout_ms=50)
    assert await conn.fetchval("SELECT 1") == 1
    assert await conn.fetchval("SHOW statement_timeout") == before


async def test_concurrent_callers_start_one_refresh(pia, monkeypatch):
    started = []

    async def slow_fetch(_conn, sql, *args, **_kw):
        started.append(sql)
        await asyncio.sleep(0.05)
        return {"types": ["Person"], "counts": [7]}

    monkeypatch.setattr(pia, "_fetchrow_hnsw", slow_fetch)
    got = await asyncio.gather(*(pia._tier2b_live_count(None, "Person") for _ in range(5)))
    assert len(started) == 1
    # The refreshing caller gets the new count; the others did not wait and used the
    # unfilled cache (0, exact).
    assert got == [7, 0, 0, 0, 0]
    assert await pia._tier2b_live_count(None, "Person") == 7
    assert len(started) == 1
