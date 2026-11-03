"""`/health` must answer "do embeddings work", not "was an object constructed".

The field read `"embedding_available": embedding_provider is not None` — true whenever a
provider object existed at startup, which is true whenever an API key is configured,
which says nothing about whether the account has credit. On 2026-08-17 the OpenAI
balance was exhausted for 45 minutes and `/health` reported `embedding_available: true`
next to `"status": "healthy"` for the whole window.

That field is a gate, not a display:

    tests/eval/run_eval.py:1124        if not health.get("embedding_available")
    scripts/koi_sustained_write.py:213 raise SystemExit("PREFLIGHT FAIL: ...")

Both would have passed into a run that could not embed a single row. The name always
promised capacity; only the value was wrong.

It now reads the embedding-repair job's canary state, and the property under test is
that it FAILS CLOSED. Every way of not knowing — file missing, unreadable, stale, or the
provider in backoff — must report false. The tempting fallback ("if we can't tell, fall
back to whether a provider object exists") is precisely the original bug, so these tests
exist mainly to stop someone reintroducing it as a kindness.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def load_helper(state_path: str):
    """Exec just the helper, not the FastAPI app.

    Importing api.personal_ingest_api pulls in the whole application and its startup
    side effects; this test is about one pure function. Slicing it out keeps the test
    fast and free of a database.
    """
    src = (REPO / "api" / "personal_ingest_api.py").read_text()
    start = src.index("EMBED_REPAIR_STATE = os.getenv(")
    end = src.index('@app.get("/health")')
    ns: dict = {"os": os, "asyncio": asyncio}
    exec(compile(src[start:end], "health_helper", "exec"), ns)  # noqa: S102
    ns["EMBED_REPAIR_STATE"] = state_path
    return ns["_embedding_health"]


def write_state(tmp_path: Path, payload, age_seconds: float = 0) -> str:
    p = tmp_path / "embedding-repair-state.json"
    p.write_text(payload if isinstance(payload, str) else json.dumps(payload))
    if age_seconds:
        t = time.time() - age_seconds
        os.utime(p, (t, t))
    return str(p)


def test_reports_available_when_a_recent_canary_succeeded(tmp_path):
    fn = load_helper(write_state(tmp_path, {
        "consecutive_failed_runs": 0,
        "last_success_at": "2026-08-17T20:31:25+00:00",
    }))
    r = fn()
    assert r["embedding_available"] is True
    assert r["embedding_check"]["last_success_at"], (
        "a positive answer must carry WHEN it was verified — 'consecutive_failed_runs: 0' "
        "cannot distinguish 'checked and fine' from 'never checked'"
    )


def test_provider_in_backoff_reports_unavailable_with_the_reason(tmp_path):
    """The exact shape of the 2026-08-17 incident."""
    fn = load_helper(write_state(tmp_path, {
        "consecutive_failed_runs": 3,
        "backoff_until": "2026-08-17T19:29:33+00:00",
        "backoff_reason": "canary embed failed: RateLimitError: credit_balance_exhausted",
    }))
    r = fn()
    assert r["embedding_available"] is False
    assert "credit_balance_exhausted" in r["embedding_check"]["reason"], (
        "an operator reading /health must learn WHY, not just that something is off"
    )


@pytest.mark.parametrize("payload,age,label", [
    (None, 0, "file missing entirely"),
    ("{ not json", 0, "unreadable state file"),
    ({"consecutive_failed_runs": 0}, 3000, "stale — the repair job stopped running"),
])
def test_every_way_of_not_knowing_fails_closed(tmp_path, payload, age, label):
    """No path may answer true without positive, recent evidence.

    Stale is the subtle one: `consecutive_failed_runs: 0` looks healthy, but if the job
    has not run for an hour that zero is a memory of the last time anyone looked. The
    provider could have died in between.
    """
    path = (str(tmp_path / "absent.json") if payload is None
            else write_state(tmp_path, payload, age_seconds=age))
    r = load_helper(path)()
    assert r["embedding_available"] is False, label
    assert r["embedding_check"].get("reason") or r["embedding_check"].get("source"), (
        f"{label}: reported unavailable with no explanation"
    )


def test_it_does_not_fall_back_to_provider_is_not_none(tmp_path):
    """Guard against the fix being 'softened' back into the bug.

    The natural-looking kindness — when state is missing, fall back to whether a
    provider object exists — restores the exact behaviour that made /health lie for 45
    minutes. The helper must not reference the provider object at all.
    """
    src = (REPO / "api" / "personal_ingest_api.py").read_text()
    start = src.index("def _embedding_health()")
    end = src.index('@app.get("/health")')
    body = src[start:end]
    assert "embedding_provider" not in body, (
        "_embedding_health consults the provider object again; that is configuration, "
        "not capacity, and is what this whole file exists to prevent"
    )


def test_the_canary_runs_even_when_there_is_nothing_to_repair():
    """/health now depends on that state file, so it must be a real heartbeat.

    The repair job used to skip its canary whenever the queue was empty and then clear
    consecutive_failed_runs on a path that never contacted the provider. A quiet period
    would therefore record a clean bill of health with nothing behind it.
    """
    src = (REPO / "scripts" / "backfill_null_embeddings.py").read_text()
    canary_at = src.index('await provider.embed("koi embedding repair canary")')
    nothing_at = src.index('print("Nothing to do.")')
    assert canary_at < nothing_at, (
        "the 'nothing to do' early return is back above the canary, so an idle queue "
        "again reports provider health without probing the provider"
    )
    assert 'st["last_success_at"]' in src, "the canary no longer records when it succeeded"
