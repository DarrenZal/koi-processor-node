"""Behavioural tests for the hardened embedding server (A8).

Needs FastAPI; skips cleanly where the stack is absent. The upstream OpenAI call
is stubbed, so no network and no key are used. Proves the input cap (413), the
cache bypass of the limiter, that the unused cache-admin routes are gone, that
docs are disabled, and that no route carries an auth dependency (bge serves no
private data, so by design it has no service key - the loopback bind is its
protection).
"""
from __future__ import annotations

import pathlib
import sys

import pytest

CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "core"
sys.path.insert(0, str(CORE))

# FastAPI/pydantic may be absent OR present-but-broken; probe under a broad guard.
try:
    import fastapi  # noqa: F401
    from pydantic import BaseModel

    class _Probe(BaseModel):
        x: int = 0

    _Probe()
    from fastapi.testclient import TestClient
except Exception as exc:  # pragma: no cover - environment dependent
    pytest.skip(f"fastapi/pydantic stack not functional here: {exc}", allow_module_level=True)

import bge_server as bs  # noqa: E402


@pytest.fixture
def client(monkeypatch):
    calls = {"n": 0}

    async def fake_embedding(text: str):
        calls["n"] += 1
        return [0.1, 0.2, 0.3]

    # Replace the upstream call; reset the cache so tests are independent.
    monkeypatch.setattr(bs, "get_openai_embedding", fake_embedding)
    monkeypatch.setattr(bs, "embedding_cache", {})
    c = TestClient(bs.app)
    c._fake_calls = calls  # type: ignore[attr-defined]
    return c


def test_encode_returns_embedding(client):
    r = client.post("/encode", json={"text": "hello"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["embedding"] == [0.1, 0.2, 0.3]
    assert body["dim"] == 1024


def test_encode_caches_and_bypasses_upstream_on_hit(client):
    assert client.post("/encode", json={"text": "same"}).status_code == 200
    assert client.post("/encode", json={"text": "same"}).status_code == 200
    # Second identical request is served from cache: upstream called once.
    assert client._fake_calls["n"] == 1


def test_encode_accepts_input_field_and_lane(client):
    r = client.post("/encode", json={"input": "via-input", "lane": "ingest"})
    assert r.status_code == 200, r.text


def test_encode_missing_text_is_400(client):
    assert client.post("/encode", json={}).status_code == 400


def test_encode_oversized_input_is_413(client, monkeypatch):
    monkeypatch.setenv("KOI_BGE_MAX_INPUT_CHARS", "16")
    r = client.post("/encode", json={"text": "y" * 17})
    assert r.status_code == 413, r.text
    # Nothing was sent upstream for a rejected oversize input.
    assert client._fake_calls["n"] == 0


def test_health_is_public(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"
    assert "max_concurrency" in r.json()


def test_docs_disabled(client):
    assert client.get("/docs").status_code == 404
    assert client.get("/redoc").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_cache_admin_routes_removed(client):
    assert client.get("/cache/stats").status_code == 404
    assert client.post("/cache/clear").status_code == 404


def test_route_census_no_auth_dependency_and_expected_paths(client):
    """bge serves no private data (plan D8), so by design no route carries a
    service-key dependency, and the only custom routes are /encode and /health."""
    from fastapi.routing import APIRoute

    framework_paths = {"/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"}
    custom = []
    for route in bs.app.routes:
        if not isinstance(route, APIRoute) or route.path in framework_paths:
            continue
        custom.append(route.path)
        for dep in route.dependant.dependencies:
            assert getattr(dep.call, "koi_service", None) is None, (
                f"{route.path} unexpectedly carries a service-key dependency"
            )
    assert set(custom) == {"/encode", "/health"}, custom
