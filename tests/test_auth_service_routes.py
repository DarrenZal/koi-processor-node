"""The activation page's error text and the legacy status route. No database or network."""
import datetime as dt

import pytest

pytest.importorskip("jwt")
pytest.importorskip("python_multipart")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.services import auth_service  # noqa: E402


class _PendingDB:
    async def fetchrow(self, *_a, **_k):
        return {"id": 1, "user_email": None, "status": "pending", "session_token": None,
                "expires_at": dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)}

    async def execute(self, *_a, **_k):
        return "UPDATE 0"


@pytest.fixture
def client():
    app = FastAPI()
    app.include_router(auth_service.router, prefix="/api/koi")

    async def fake_db():
        yield _PendingDB()

    app.dependency_overrides[auth_service.get_db] = fake_db
    return TestClient(app, raise_server_exceptions=False)


def test_unknown_error_value_is_not_echoed(client):
    marker = '<b id="marker">x</b>'
    r = client.get("/api/koi/activate", params={"error": marker})
    assert r.status_code == 200
    assert marker not in r.text
    assert "marker" not in r.text
    assert '<div class="error">' in r.text


def test_known_error_code_renders_its_message(client):
    r = client.get("/api/koi/activate", params={"error": "invalid"})
    assert "Invalid or expired code" in r.text


def test_no_error_renders_no_error_block(client):
    assert '<div class="error">' not in client.get("/api/koi/activate").text


def test_legacy_status_route_reports_a_pending_request(client):
    r = client.get("/api/koi/auth/status", params={"device_code": "a" * 64})
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "pending", "authenticated": False}
