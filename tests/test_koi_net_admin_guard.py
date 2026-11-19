"""The admin guard fails closed (2026-11-06).

Loopback alone is not authority: with no admin token configured every caller gets 503,
loopback included; a wrong bearer gets 401; a non-loopback caller gets 403; the right bearer
from loopback passes. Loopback is decided by address (IPv4-mapped IPv6 unwrapped), not by
name. PR #15's "local operator" check (`_enforce_local_admin(request) is None`) inherits this.
"""
import json

import pytest
from starlette.requests import Request

import api.koi_net_router as knr

TOKEN = "guard-test-token"


def _request(host, bearer=None):
    headers = [(b"authorization", f"Bearer {bearer}".encode())] if bearer is not None else []
    return Request({"type": "http", "method": "POST", "path": "/koi-net/edges/approve", "headers": headers,
                    "client": (host, 40000)})


def _code(response):
    return None if response is None else (response.status_code, json.loads(response.body)["error_code"])


@pytest.fixture
def token(monkeypatch, tmp_path):
    monkeypatch.setenv("KOI_STATE_DIR", str(tmp_path))
    monkeypatch.delenv("KOI_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("KOI_COMMONS_SERVICE_TOKEN", raising=False)
    return tmp_path


def test_no_token_configured_refuses_loopback_with_503(token):
    assert _code(knr._enforce_local_admin(_request("127.0.0.1"))) == (503, "ADMIN_TOKEN_UNCONFIGURED")
    assert _code(knr._enforce_local_admin(_request("127.0.0.1", "anything"))) == (503, "ADMIN_TOKEN_UNCONFIGURED")


def test_an_unreadable_token_file_counts_as_unconfigured(token):
    (token / "admin_token").mkdir()  # exists, cannot be read as a file
    assert _code(knr._enforce_local_admin(_request("127.0.0.1", TOKEN))) == (503, "ADMIN_TOKEN_UNCONFIGURED")


def test_wrong_or_missing_bearer_gets_401(token, monkeypatch):
    monkeypatch.setenv("KOI_ADMIN_TOKEN", TOKEN)
    assert _code(knr._enforce_local_admin(_request("127.0.0.1", "wrong"))) == (401, "UNAUTHORIZED")
    assert _code(knr._enforce_local_admin(_request("127.0.0.1"))) == (401, "UNAUTHORIZED")


def test_non_loopback_gets_403_even_with_the_right_bearer(token, monkeypatch):
    monkeypatch.setenv("KOI_ADMIN_TOKEN", TOKEN)
    for host in ("198.51.100.7", "10.100.0.2", "localhost", "::ffff:198.51.100.7"):
        assert _code(knr._enforce_local_admin(_request(host, TOKEN))) == (403, "FORBIDDEN"), host


def test_the_right_bearer_from_loopback_passes(token, monkeypatch):
    (token / "admin_token").write_text(TOKEN + "\n")
    for host in ("127.0.0.1", "::1", "::ffff:127.0.0.1", "127.0.0.2"):
        assert knr._enforce_local_admin(_request(host, TOKEN)) is None, host


def test_commons_guard_fails_closed_too(token, monkeypatch):
    assert _code(knr._enforce_commons_admin(_request("127.0.0.1"))) == (503, "ADMIN_TOKEN_UNCONFIGURED")
    assert _code(knr._enforce_commons_admin(_request("198.51.100.7"))) == (403, "FORBIDDEN")
    monkeypatch.setenv("KOI_COMMONS_SERVICE_TOKEN", "commons-token")
    assert knr._enforce_commons_admin(_request("198.51.100.7", "commons-token")) is None
    monkeypatch.setenv("KOI_ADMIN_TOKEN", TOKEN)
    assert knr._enforce_commons_admin(_request("127.0.0.1", TOKEN)) is None
    assert _code(knr._enforce_commons_admin(_request("127.0.0.1", "wrong"))) == (401, "UNAUTHORIZED")


# --- api.auth.enforce_local_admin (vault_sync_router, discourse_links_router): the same rule ---
# This guard was still fail-open: it admitted a loopback caller when no token was configured.

import api.auth as auth  # noqa: E402


def _auth_code(response):
    return None if response is None else (response.status_code, json.loads(response.body)["error_code"])


def test_api_auth_refuses_loopback_with_503_when_no_token_is_configured(token):
    assert _auth_code(auth.enforce_local_admin(_request("127.0.0.1"))) == (503, "ADMIN_TOKEN_UNCONFIGURED")
    (token / "admin_token").write_text("\n")  # an empty file is not a token
    assert _auth_code(auth.enforce_local_admin(_request("127.0.0.1", ""))) == (503, "ADMIN_TOKEN_UNCONFIGURED")


def test_api_auth_wrong_bearer_401_non_loopback_403_right_bearer_passes(token, monkeypatch):
    monkeypatch.setenv("KOI_ADMIN_TOKEN", TOKEN)
    assert _auth_code(auth.enforce_local_admin(_request("127.0.0.1", "wrong"))) == (401, "UNAUTHORIZED")
    for host in ("198.51.100.7", "localhost", "::ffff:198.51.100.7"):
        assert _auth_code(auth.enforce_local_admin(_request(host, TOKEN))) == (403, "FORBIDDEN"), host
    for host in ("127.0.0.1", "::1", "::ffff:127.0.0.1"):
        assert auth.enforce_local_admin(_request(host, TOKEN)) is None, host
    # The body keeps the "error" key discourse_links_router reads.
    assert "error" in json.loads(auth.enforce_local_admin(_request("127.0.0.1", "wrong")).body)


def test_both_guards_decide_by_the_same_rule(token, monkeypatch):
    """One rule: for every caller shape, the router guard and api.auth agree on the status."""
    cases = [("127.0.0.1", None), ("127.0.0.1", TOKEN), ("127.0.0.1", "wrong"), ("198.51.100.7", TOKEN)]
    for configured in (False, True):
        if configured:
            monkeypatch.setenv("KOI_ADMIN_TOKEN", TOKEN)
        for host, bearer in cases:
            a, b = knr._enforce_local_admin(_request(host, bearer)), auth.enforce_local_admin(_request(host, bearer))
            assert (None if a is None else a.status_code) == (None if b is None else b.status_code), (configured, host, bearer)
