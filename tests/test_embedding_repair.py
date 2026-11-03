"""Tests for scripts/backfill_null_embeddings.py — the embedding self-heal job.

The lesson this codebase keeps relearning (memory:
`feedback_test_fixtures_encode_the_assumption`) is that the FIXTURE, not the
assertion, decides whether a test can fail. Two concrete traps are designed
around here, and both would produce a green suite over a broken job:

  * **An empty backlog.** Every surface reads 0 pending in the live DB today. A
    fixture drawn from live state passes under *any* implementation, including
    one that never calls the provider at all. Every test below that exercises
    repair seeds a non-empty backlog first.

  * **A schema-valid vector.** An all-zero or zero-padded 3072-vector inserts
    cleanly and indexes fine, so asserting "a vector was written" proves
    nothing. T3 uses vectors that are valid to Postgres and wrong to a human.

DB-backed tests run against a throwaway table (`_test_embedding_repair_*`) with
SURFACES monkeypatched to point at it, so production tables are never touched.
They skip when POSTGRES_URL is unset.

Run:  python -m pytest tests/test_embedding_repair.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import pytest_asyncio
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import asyncpg  # noqa: E402

from scripts.backfill_null_embeddings import (  # noqa: E402
    BACKOFF_SECONDS,
    QUARANTINE_AFTER,
    Surface,
    backoff_active,
    build_provider,
    enter_backoff,
    vector_is_sane,
)
import scripts.backfill_null_embeddings as repair  # noqa: E402

needs_db = targets.requires_database

DIM = repair.DIM


# ---------------------------------------------------------------- fixtures
def real_vector(seed: float = 0.01):
    """A vector that looks like a genuine embedding: nonzero across all 3072."""
    return [seed + (i % 7) * 0.001 for i in range(DIM)]


def all_zero_vector():
    return [0.0] * DIM


def padded_vector():
    """768 real dims + 2304 zeros — the exact _pad_to_dim(nomic-768) signature.

    This is the fixture insight for T3: schema-valid, index-clean, and
    semantically useless. `EMBEDDING_FALLBACK=ollama:nomic-embed-text` is set in
    production, so the object that produces this is one edit away from the write
    path.
    """
    return [0.5 + (i % 5) * 0.01 for i in range(768)] + [0.0] * (DIM - 768)


class StubProvider:
    """Counts calls so a test can prove how many the provider actually took."""

    def __init__(self, behaviour="ok", fail_texts=()):
        self.behaviour = behaviour
        self.fail_texts = set(fail_texts)
        self.embed_calls = 0
        self.batch_calls = 0

    async def embed(self, text):                      # the canary path
        self.embed_calls += 1
        if self.behaviour == "down":
            raise RuntimeError("credit_balance_exhausted")
        return real_vector()

    async def embed_batch_or_none(self, texts, prompt_type="unknown"):
        self.batch_calls += 1
        if self.behaviour == "down":
            return None
        out = []
        for t in texts:
            if t in self.fail_texts:
                out.append(all_zero_vector())         # provider "succeeds" with junk
            elif self.behaviour == "zero":
                out.append(all_zero_vector())
            elif self.behaviour == "padded":
                out.append(padded_vector())
            else:
                out.append(real_vector())
        return out


class Args:
    def __init__(self, **kw):
        self.surface = None
        self.limit = 500
        self.batch_size = 100
        self.cost_abort_usd = 1.00
        self.dry_run = False
        self.ignore_backoff = False
        self.state_file = "/tmp/_test_embedding_repair_state.json"
        self.run_log_dir = "/tmp/_test_embedding_repair_runs"
        self.__dict__.update(kw)


@pytest.fixture
def state_file(tmp_path):
    return tmp_path / "state.json"


@pytest_asyncio.fixture
async def scratch(monkeypatch):
    """A throwaway table wired in as the ONLY surface, plus a live connection.

    Yields (conn, table). Dropped unconditionally on teardown. Production tables
    are never in SURFACES for the duration of a test.
    """
    conn = await asyncpg.connect(targets.database_url())
    table = "_test_embedding_repair"
    await conn.execute(f"DROP TABLE IF EXISTS {table}")
    await conn.execute(
        f"CREATE TABLE {table} (id TEXT PRIMARY KEY, body TEXT, "
        f"embedding_3072 vector({DIM}))")
    surface = Surface(
        key="scratch", table=table, id_col="id", vec_col="embedding_3072",
        text_sql="body", extra_where="body IS NOT NULL AND length(body) > 0",
        order_sql="id ASC",
    )
    monkeypatch.setattr(repair, "SURFACES", {"scratch": surface})
    try:
        yield conn, table, surface
    finally:
        await conn.execute(f"DROP TABLE IF EXISTS {table}")
        await conn.close()


async def seed(conn, table, n=3):
    """A NON-EMPTY backlog. Without this every test below is vacuous."""
    for i in range(n):
        await conn.execute(
            f"INSERT INTO {table} (id, body) VALUES ($1, $2)", f"row-{i}", f"text {i}")


# =============================================================== T7 provider
def test_build_provider_refuses_the_fallback_chain(monkeypatch):
    """T7 — the loaded gun.

    Fixture insight: EMBEDDING_FALLBACK is *set in production*. Every existing
    test of create_embedding_provider() runs with it unset, so the factory never
    returns the chain and no existing fixture can observe this at all.
    """
    from api.embedding_provider import (
        FallbackChainEmbeddingProvider, OllamaEmbeddingProvider, OpenAIEmbeddingProvider)

    class FakeOllama(OllamaEmbeddingProvider):
        def __init__(self):
            self.dimension = 768
            self.model_name = "nomic-embed-text"

    class FakeChain(FallbackChainEmbeddingProvider):
        def __init__(self, primary):
            self._primary = primary
            self.dimension = getattr(primary, "dimension", 0)

    import api.embedding_provider as ep

    # A chain wrapping a NON-OpenAI primary must be refused outright.
    monkeypatch.setattr(ep, "create_embedding_provider", lambda: FakeChain(FakeOllama()))
    provider, reason = build_provider()
    assert provider is None
    assert "OpenAIEmbeddingProvider" in reason

    # A chain wrapping the OpenAI primary must be UNWRAPPED, not returned as-is,
    # because the chain is what owns _pad_to_dim.
    class FakeOpenAI(OpenAIEmbeddingProvider):
        def __init__(self):
            self.dimension = DIM
            self.model_name = "text-embedding-3-large"

    inner = FakeOpenAI()
    monkeypatch.setattr(ep, "create_embedding_provider", lambda: FakeChain(inner))
    provider, reason = build_provider()
    assert reason is None
    assert provider is inner
    assert not isinstance(provider, FallbackChainEmbeddingProvider)


def test_build_provider_rejects_a_dimension_mismatch(monkeypatch):
    from api.embedding_provider import OpenAIEmbeddingProvider
    import api.embedding_provider as ep

    class WrongDim(OpenAIEmbeddingProvider):
        def __init__(self):
            self.dimension = 1024          # the dead legacy column's dimension
            self.model_name = "text-embedding-3-small"

    monkeypatch.setattr(ep, "create_embedding_provider", lambda: WrongDim())
    provider, reason = build_provider()
    assert provider is None and "1024" in reason


# =========================================================== T3 poison gate
@pytest.mark.parametrize("vec,ok,why_substr", [
    (real_vector(), True, ""),
    (None, False, "None"),
    ([0.1] * (DIM - 1), False, "dimension"),
    (all_zero_vector(), False, "all-zero"),
    (padded_vector(), False, "zero-padded"),
])
def test_vector_is_sane_rejects_every_schema_valid_but_wrong_vector(vec, ok, why_substr):
    """T3 (unit half). all_zero and padded both INSERT fine and index fine."""
    sane, why = vector_is_sane(vec)
    assert sane is ok
    assert why_substr in why


@needs_db
@pytest.mark.asyncio
async def test_a_poison_vector_is_never_written_and_the_row_stays_null(scratch, state_file, tmp_path):
    """T3 (integration half) — the write gate.

    Mutation: delete the vector_is_sane call from repair_surface and this fails,
    because the padded vector is perfectly acceptable to Postgres.
    """
    conn, table, surface = scratch
    await seed(conn, table, n=2)
    st = {}
    with (tmp_path / "run.log").open("w") as rl:
        outcome = await repair.repair_surface(
            conn, StubProvider("padded"), surface, Args(), st, [0.0], rl)

    assert outcome == "ok"
    remaining = await conn.fetchval(f"SELECT count(*) FROM {table} WHERE embedding_3072 IS NULL")
    assert remaining == 2, "a zero-padded fallback vector was written to the DB"
    assert all(v == 1 for v in st["row_failures"].values())


@needs_db
@pytest.mark.asyncio
async def test_a_good_vector_is_written(scratch, tmp_path):
    """Guard the guard: prove the write path works, so the test above can fail."""
    conn, table, surface = scratch
    await seed(conn, table, n=2)
    with (tmp_path / "run.log").open("w") as rl:
        await repair.repair_surface(conn, StubProvider("ok"), surface, Args(), {}, [0.0], rl)
    remaining = await conn.fetchval(f"SELECT count(*) FROM {table} WHERE embedding_3072 IS NULL")
    assert remaining == 0


# ============================================================== T1/T2 storm
@needs_db
@pytest.mark.asyncio
async def test_an_outage_costs_one_canary_call_then_zero(scratch, state_file, monkeypatch):
    """T1 — the 27.8x amplifier, as behaviour.

    Fixture is load-bearing in TWO ways: the provider must always fail, AND the
    backlog must be non-empty. With 0 pending rows the job short-circuits at
    'Nothing to do' before it ever reaches the canary, and this passes against
    an implementation with no circuit breaker at all.

    Mutation: remove the backoff_active() early return in run() -> the second
    run calls the provider -> fails.
    """
    conn, table, _ = scratch
    await seed(conn, table, n=50)
    stub = StubProvider("down")
    monkeypatch.setattr(repair, "build_provider", lambda: (stub, None))

    rc1 = await repair.run(Args(state_file=str(state_file)))
    assert rc1 == 0, "a down provider must exit 0; nonzero is what launchd respawned on"
    assert stub.embed_calls == 1, "the canary must be the ONLY call"
    assert stub.batch_calls == 0, "no batch may run once the canary failed"

    st = json.loads(state_file.read_text())
    assert st["backoff_until"]
    assert st["consecutive_failed_runs"] == 1

    rc2 = await repair.run(Args(state_file=str(state_file)))
    assert rc2 == 0
    assert stub.embed_calls == 1, "the second run inside the backoff window made an API call"
    assert stub.batch_calls == 0


@needs_db
@pytest.mark.asyncio
async def test_a_mid_run_batch_failure_stops_and_backs_off(scratch, state_file, monkeypatch):
    """The other half of the storm: don't grind through the backlog batch by batch."""
    conn, table, _ = scratch
    await seed(conn, table, n=250)

    stub = StubProvider("ok")
    calls = {"n": 0}

    async def flaky(texts, prompt_type="unknown"):
        calls["n"] += 1
        return None if calls["n"] > 1 else [real_vector() for _ in texts]

    stub.embed_batch_or_none = flaky
    monkeypatch.setattr(repair, "build_provider", lambda: (stub, None))

    rc = await repair.run(Args(state_file=str(state_file), batch_size=100))
    assert rc == 0
    assert calls["n"] == 2, "should stop at the FIRST failing batch, not attempt all three"
    st = json.loads(state_file.read_text())
    assert st["backoff_until"], "a mid-run provider failure must open the circuit breaker"


def test_backoff_is_exponential_and_capped():
    st = {}
    delays = []
    for _ in range(len(BACKOFF_SECONDS) + 2):
        before = st.get("consecutive_failed_runs", 0)
        enter_backoff(st, "test")
        delays.append(BACKOFF_SECONDS[min(before, len(BACKOFF_SECONDS) - 1)])
        st.pop("backoff_until")           # let the next call proceed
    assert delays[:len(BACKOFF_SECONDS)] == BACKOFF_SECONDS
    assert delays[-1] == BACKOFF_SECONDS[-1], "must plateau, not grow unbounded"


def test_backoff_active_tolerates_a_corrupt_state_file():
    assert backoff_active({}) is None
    assert backoff_active({"backoff_until": "not-a-date"}) is None


# ============================================================ T5 quarantine
@needs_db
@pytest.mark.asyncio
async def test_a_permanently_failing_row_is_quarantined_not_retried_forever(scratch, tmp_path):
    """T5 — bounded recurring spend.

    Fixture insight: the loop. A single-run fixture cannot observe the
    288-retries-per-day behaviour that unbounds cost. Here one row always comes
    back as junk while two succeed, driven through 8 simulated runs.

    Mutation: remove the quarantine filter -> attempts grow linearly with run
    count -> fails.
    """
    conn, table, surface = scratch
    await conn.execute(f"INSERT INTO {table} (id, body) VALUES ('good-1','fine')")
    await conn.execute(f"INSERT INTO {table} (id, body) VALUES ('good-2','also fine')")
    await conn.execute(f"INSERT INTO {table} (id, body) VALUES ('poison','BAD')")

    st = {}
    attempts = 0
    for _ in range(8):
        provider = StubProvider("ok", fail_texts={"BAD"})
        with (tmp_path / "run.log").open("a") as rl:
            await repair.repair_surface(conn, provider, surface, Args(), st, [0.0], rl)
        attempts += provider.batch_calls

    assert attempts <= QUARANTINE_AFTER, (
        f"the poison row was attempted in {attempts} of 8 runs; quarantine should "
        f"cap it at {QUARANTINE_AFTER}")
    good_left = await conn.fetchval(
        f"SELECT count(*) FROM {table} WHERE embedding_3072 IS NULL AND id LIKE 'good-%'")
    assert good_left == 0, "healthy rows must still be repaired alongside a poison row"
    assert st["row_failures"]["scratch:poison"] >= QUARANTINE_AFTER


# ============================================================== T4 surfaces
def test_every_semantic_read_surface_is_covered():
    """T4 — the coverage gap itself, asserted as data.

    Facts and entities were unscheduled for months while chunks self-healed;
    session_chunks was never gauged at all. Mutation: delete any key -> fails.
    """
    assert set(repair.SURFACES) == {"chunks", "facts", "entities", "sessions"}
    for key, s in repair.SURFACES.items():
        assert s.vec_col.endswith("_3072"), (
            f"{key} must target the column search READS, not the dead 1024-dim one")


@needs_db
@pytest.mark.asyncio
async def test_each_surfaces_selection_query_is_valid_against_the_live_schema():
    """Every surface's SQL actually runs. A typo in extra_where or text_sql would
    otherwise surface only at 3am on the surface with the smallest backlog."""
    conn = await asyncpg.connect(targets.database_url())
    try:
        for key, s in repair.SURFACES.items():
            n = await repair.count_pending(conn, s)
            assert n >= 0
            rows = await repair.fetch_pending(conn, s, 1)
            for _, text in rows:
                assert text is not None, f"{key}: text_sql produced NULL despite extra_where"
    finally:
        await conn.close()


@needs_db
@pytest.mark.asyncio
async def test_merged_entity_tombstones_are_never_repaired():
    """Boundary row. merged_into rows are tombstones, not read paths — embedding
    them costs money and re-creates the duplicate the merge removed."""
    conn = await asyncpg.connect(targets.database_url())
    try:
        s = repair.SURFACES["entities"]
        assert "merged_into IS NULL" in s.extra_where
        leaked = await conn.fetchval(f"""
            SELECT count(*) FROM {s.table}
            WHERE {s.vec_col} IS NULL AND {s.extra_where} AND merged_into IS NOT NULL
        """)
        assert leaked == 0
    finally:
        await conn.close()


@needs_db
@pytest.mark.asyncio
async def test_superseded_facts_are_repaired_not_skipped():
    """The opposite boundary. Excluding valid_to rows under-reported the backlog
    8.4x on 2026-08-01; they still bypass cosine dedup until embedded."""
    s = repair.SURFACES["facts"]
    assert "valid_to" not in s.extra_where, "superseded facts must stay in scope"


# ================================================================== T6 plist
def test_the_application_name_cannot_trip_the_facts_write_guard():
    """knowledge_facts carries tr_layers_only_guard_facts, which RAISES on any
    write when application_name starts with 'deep-extract:layers_only:'."""
    assert not repair.APPLICATION_NAME.startswith("deep-extract:layers_only:")
