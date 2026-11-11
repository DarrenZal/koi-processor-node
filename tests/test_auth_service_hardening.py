"""auth_service hardening: callback and success HTML escaping.

Covers two changes to src/services/auth_service.py:
  (a) auth_callback escapes the ValueError text before putting it in the
      "Verification Failed" page (it can carry a JWT `kid`).
  (b) the device-flow success page escapes the verified email.

Offline note: auth_service declares a Form() route and imports PyJWT; neither is
needed by the handlers under test, so both are stubbed in sys.modules before
import. The handlers are driven
directly with small async fakes - no DB, no network, no FastAPI app.
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import time
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

sys.modules.setdefault("jwt", types.ModuleType("jwt"))
if "multipart" not in sys.modules:
    _mp = types.ModuleType("multipart")
    _mp.__version__ = "0-stub"  # FastAPI's Form() guard only checks this import
    _mp_sub = types.ModuleType("multipart.multipart")
    _mp_sub.parse_options_header = lambda *a, **k: (b"", {})
    _mp.multipart = _mp_sub
    sys.modules["multipart"] = _mp
    sys.modules["multipart.multipart"] = _mp_sub

import src.services.auth_service as auth  # noqa: E402


class _FakeDB:
    """Async db stand-in: returns one auth_request, swallows writes."""

    def __init__(self, auth_request):
        self._auth_request = auth_request

    async def fetchrow(self, *a, **k):
        return self._auth_request

    async def execute(self, *a, **k):
        return None


class _FakeResp:
    status_code = 200

    def json(self):
        return {"id_token": "fake-id-token"}


class _FakeClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, *a, **k):
        return _FakeResp()


def _drive_callback(monkeypatch, *, validate_raises=None, validate_returns=None,
                    callback_url=None):
    """Run auth_callback to the point the email/error reaches the HTML."""
    auth_request = {
        "id": 1,
        "device_code": "dc",
        "user_code": "UC-123",
        "user_email": None,
        "status": "pending",
        "expires_at": types.SimpleNamespace(timestamp=lambda: time.time() + 600),
        "callback_url": callback_url,
    }
    monkeypatch.setattr(auth, "load_client_secrets",
                        lambda: {"client_id": "cid", "client_secret": "sec"})
    monkeypatch.setattr(auth, "httpx", types.SimpleNamespace(AsyncClient=_FakeClient))

    async def _validate(id_token, client_id):
        if validate_raises is not None:
            raise validate_raises
        return validate_returns

    monkeypatch.setattr(auth, "validate_google_id_token", _validate)
    return asyncio.run(auth.auth_callback(code="c", state="s", db=_FakeDB(auth_request)))


def test_callback_escapes_validation_error(monkeypatch):
    # A ValueError message can carry a JWT `kid`; it must not reach the page raw.
    payload = '<img src=x onerror=alert(1)>'
    resp = _drive_callback(monkeypatch, validate_raises=ValueError(f"Unknown key ID: {payload}"))
    body = resp.body.decode()
    assert payload not in body            # raw markup must not be reflected
    assert "<img" not in body             # no live tag survives (only &lt;img)
    assert "&lt;img src=x onerror=alert(1)&gt;" in body  # escaped form is present
    assert "Verification Failed" in body  # page still renders


def test_success_page_escapes_email(monkeypatch):
    # verified_email is domain-checked but still interpolated into HTML; escape it.
    payload = '<b>xss</b>'
    email = f"evil{payload}@regen.network"
    result = _drive_callback(monkeypatch, validate_returns={"email": email}, callback_url=None)
    html = result if isinstance(result, str) else result.body.decode()
    assert payload not in html                 # raw markup must not be reflected
    assert "&lt;b&gt;xss&lt;/b&gt;" in html    # escaped form is present
    assert "Authentication Successful" in html  # page still renders

