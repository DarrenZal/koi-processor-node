"""Behavioural tests for the service-key FastAPI dependency and a route census.

These need FastAPI (and, for the real-app census, the bridge's own dependencies),
so they skip where those are not installed. The pure key logic they rely on is
covered offline in test_service_auth.py; here we prove the dependency enforces,
warns and stays public on /health, and that every non-public bridge route
declares the auth dependency.
"""
from __future__ import annotations

import pathlib
import sys

import pytest

CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "core"
sys.path.insert(0, str(CORE))

import service_auth as sa  # noqa: E402

# FastAPI/pydantic may be absent OR present-but-broken (version skew can raise
# AttributeError at import, which importorskip does not catch). Probe the whole
# stack under a broad guard and skip the module cleanly if it is not functional.
try:
    import fastapi  # noqa: F401
    from pydantic import BaseModel

    class _Probe(BaseModel):
        x: int = 0

    _Probe()
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient
except Exception as exc:  # pragma: no cover - environment dependent
    pytest.skip(
        f"fastapi/pydantic stack not functional here: {exc}",
        allow_module_level=True,
    )

KEY = "k" * 40


def _build_app():
    dep = sa.make_service_key_dependency("BRIDGE")
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/protected", dependencies=[Depends(dep)])
    def protected():
        return {"ok": True}

    return app


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ("KOI_SERVICE_KEYS", "KOI_AUTH_MODE_BRIDGE"):
        monkeypatch.delenv(var, raising=False)
    yield


def test_enforce_mode_rejects_missing_and_wrong(monkeypatch):
    monkeypatch.setenv("KOI_SERVICE_KEYS", KEY)
    client = TestClient(_build_app())
    assert client.get("/protected").status_code == 401
    assert client.get("/protected", headers={"X-KOI-Service-Key": "x" * 40}).status_code == 401
    assert client.get("/protected", headers={"X-KOI-Service-Key": "short"}).status_code == 401
    assert client.get("/protected", headers={"X-KOI-Service-Key": KEY}).status_code == 200


def test_health_is_public(monkeypatch):
    monkeypatch.setenv("KOI_SERVICE_KEYS", KEY)
    client = TestClient(_build_app())
    assert client.get("/health").status_code == 200


def test_warn_mode_allows_but_logs(monkeypatch, caplog):
    monkeypatch.setenv("KOI_SERVICE_KEYS", KEY)
    monkeypatch.setenv("KOI_AUTH_MODE_BRIDGE", "warn")
    client = TestClient(_build_app())
    with caplog.at_level("WARNING"):
        resp = client.get("/protected")  # no key
    assert resp.status_code == 200
    assert any("WOULD REJECT" in rec.getMessage() for rec in caplog.records)


def test_non_ascii_header_is_401_not_500(monkeypatch):
    monkeypatch.setenv("KOI_SERVICE_KEYS", KEY)
    client = TestClient(_build_app())
    # A high-byte header value reaches the app decoded as latin-1; the dependency
    # must reject it (401), never 500. Send raw bytes so the HTTP client does not
    # refuse a non-ASCII value before it reaches the server.
    resp = client.get("/protected", headers={"X-KOI-Service-Key": b"\xe9" * 40})
    assert resp.status_code == 401


def test_real_bridge_route_census(monkeypatch):
    """AC3: every non-public route on the real bridge app declares the auth
    dependency. Skips unless the bridge's own dependencies are installed."""
    pytest.importorskip("asyncpg")
    pytest.importorskip("pydantic")
    import importlib

    from fastapi.routing import APIRoute

    bridge = importlib.import_module("koi_event_bridge_v2")
    public = {"/health", "/", "/openapi.json", "/docs", "/redoc", "/docs/oauth2-redirect"}
    for route in bridge.app.routes:
        if not isinstance(route, APIRoute):
            continue
        if route.path in public:
            continue
        calls = [dep.call for dep in route.dependant.dependencies]
        assert any(getattr(call, "koi_service", None) == "BRIDGE" for call in calls), (
            f"route {route.path} has no service-key dependency"
        )
