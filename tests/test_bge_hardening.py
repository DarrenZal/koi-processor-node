"""Pure/asyncio tests for the embedding-server hardening logic (offline).

Exercises src/core/bge_hardening.py (new on this branch, so the import fails on
base and passes here). Standard library only; async tests use asyncio.run so no
pytest-asyncio plugin is needed.
"""
from __future__ import annotations

import asyncio
import pathlib
import sys

import pytest

CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "core"
sys.path.insert(0, str(CORE))

import bge_hardening as bh  # noqa: E402


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
def test_config_defaults(monkeypatch):
    for name in (
        bh.MAX_CONCURRENCY_ENV, bh.INGEST_MAX_ENV, bh.ACQUIRE_TIMEOUT_ENV,
        bh.OPENAI_TIMEOUT_ENV, bh.MAX_INPUT_CHARS_ENV, bh.BIND_HOST_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    assert bh.max_concurrency() == bh.DEFAULT_MAX_CONCURRENCY
    assert bh.ingest_max() == bh.DEFAULT_INGEST_MAX
    assert bh.acquire_timeout() == bh.DEFAULT_ACQUIRE_TIMEOUT
    assert bh.openai_timeout() == bh.DEFAULT_OPENAI_TIMEOUT
    assert bh.max_input_chars() == bh.DEFAULT_MAX_INPUT_CHARS
    # Default bind is left unchanged (all interfaces) until the deploy-time check.
    assert bh.bind_host() == "0.0.0.0"
    # The total budget stays under the bridge's 120s caller timeout.
    assert bh.acquire_timeout() + bh.openai_timeout() < 120


def test_config_env_overrides(monkeypatch):
    monkeypatch.setenv(bh.MAX_CONCURRENCY_ENV, "16")
    monkeypatch.setenv(bh.ACQUIRE_TIMEOUT_ENV, "45")
    monkeypatch.setenv(bh.MAX_INPUT_CHARS_ENV, "4096")
    monkeypatch.setenv(bh.BIND_HOST_ENV, "127.0.0.1")
    assert bh.max_concurrency() == 16
    assert bh.acquire_timeout() == 45.0
    assert bh.max_input_chars() == 4096
    assert bh.bind_host() == "127.0.0.1"


def test_config_rejects_garbage_and_nonpositive(monkeypatch):
    monkeypatch.setenv(bh.MAX_CONCURRENCY_ENV, "not-a-number")
    monkeypatch.setenv(bh.OPENAI_TIMEOUT_ENV, "-5")
    assert bh.max_concurrency() == bh.DEFAULT_MAX_CONCURRENCY
    assert bh.openai_timeout() == bh.DEFAULT_OPENAI_TIMEOUT


def test_ingest_max_clamped_below_total(monkeypatch):
    monkeypatch.setenv(bh.MAX_CONCURRENCY_ENV, "4")
    monkeypatch.setenv(bh.INGEST_MAX_ENV, "100")
    # Never let ingest take every slot: at least one stays for queries.
    assert bh.ingest_max() == 3


# --------------------------------------------------------------------------- #
# Lane + input cap
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("value", ["ingest", "INGEST", " ingest ", "Ingest"])
def test_normalize_lane_ingest(value):
    assert bh.normalize_lane(value) == "ingest"


@pytest.mark.parametrize("value", [None, "", "query", "search", "nonsense", 5, "ingestx"])
def test_normalize_lane_defaults_to_query(value):
    assert bh.normalize_lane(value) == "query"


def test_input_too_large():
    assert bh.input_too_large("x" * 10, limit=8) is True
    assert bh.input_too_large("x" * 8, limit=8) is False
    assert bh.input_too_large("", limit=8) is False


# --------------------------------------------------------------------------- #
# LaneLimiter
# --------------------------------------------------------------------------- #
def test_limiter_ingest_limit_always_reserves_a_query_slot():
    lim = bh.LaneLimiter(total=4, ingest_limit=100)
    assert 1 <= lim.ingest_limit <= lim.total - 1


def test_limiter_query_not_starved_by_ingest():
    async def scenario():
        lim = bh.LaneLimiter(total=2, ingest_limit=1)
        release = asyncio.Event()

        async def hold_ingest():
            async with lim.slot("ingest", timeout=5):
                await release.wait()

        holder = asyncio.create_task(hold_ingest())
        await asyncio.sleep(0.05)  # let the ingest holder acquire

        # A second ingest is blocked by the ingest limit and times out.
        second_ingest_timed_out = False
        try:
            async with lim.slot("ingest", timeout=0.2):
                pass
        except asyncio.TimeoutError:
            second_ingest_timed_out = True
        assert second_ingest_timed_out

        # ...but a query still gets the reserved global slot promptly.
        query_ran = False
        async with lim.slot("query", timeout=0.5):
            query_ran = True
        assert query_ran

        release.set()
        await holder

    asyncio.run(scenario())


def test_limiter_acquire_timeout_and_reuse():
    async def scenario():
        lim = bh.LaneLimiter(total=1, ingest_limit=1)
        release = asyncio.Event()

        async def hold():
            async with lim.slot("query", timeout=5):
                await release.wait()

        holder = asyncio.create_task(hold())
        await asyncio.sleep(0.05)

        timed_out = False
        try:
            async with lim.slot("query", timeout=0.2):
                pass
        except asyncio.TimeoutError:
            timed_out = True
        assert timed_out, "a request must 503 (TimeoutError) rather than queue forever"

        release.set()
        await holder

        # The slot is released and reusable after the holder finishes.
        reused = False
        async with lim.slot("query", timeout=0.5):
            reused = True
        assert reused

    asyncio.run(scenario())


def test_limiter_releases_on_body_exception():
    async def scenario():
        lim = bh.LaneLimiter(total=1, ingest_limit=1)
        with pytest.raises(ValueError):
            async with lim.slot("query", timeout=1):
                raise ValueError("boom")
        # If the slot leaked, this acquire would hang and time out.
        ok = False
        async with lim.slot("query", timeout=0.5):
            ok = True
        assert ok

    asyncio.run(scenario())
