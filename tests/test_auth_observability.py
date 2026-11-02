"""Tests for the log-only auth observability middleware.

The load-bearing tests are the LOG-ONLY ones: they must fail loudly if anyone
ever gives this middleware the ability to reject a request.
"""

import ast
import asyncio
import inspect
import json
import os

import pytest

from api import auth_observability as ao


# =========================================================================
# 1. The non-rejection invariant -- mechanical (AST) proof
# =========================================================================

def _module_ast():
    src = inspect.getsource(ao)
    return ast.parse(src), src


RESPONSE_CTORS = {
    "Response", "JSONResponse", "PlainTextResponse", "HTMLResponse",
    "RedirectResponse", "FileResponse", "StreamingResponse", "HTTPException",
}


def test_module_constructs_no_response_object():
    """No branch can build a denial: the module never constructs a Response."""
    tree, _ = _module_ast()
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = getattr(fn, "id", None) or getattr(fn, "attr", None)
            if name in RESPONSE_CTORS:
                offenders.append((name, getattr(node, "lineno", "?")))
    assert offenders == [], f"auth_observability must not build responses: {offenders}"


def test_module_contains_no_raise_statement():
    """It also cannot abort a request by raising."""
    tree, _ = _module_ast()
    raises = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Raise)]
    assert raises == [], f"unexpected raise statements at lines {raises}"


def test_call_exits_are_all_delegations():
    """Every path out of __call__ goes through `await self.app(...)`."""
    tree, _ = _module_ast()
    call_fn = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "__call__":
            call_fn = node
    assert call_fn is not None
    delegations = 0
    for node in ast.walk(call_fn):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            fn = node.value.func
            if isinstance(fn, ast.Attribute) and fn.attr == "app":
                delegations += 1
    assert delegations == 2, f"expected exactly 2 self.app delegations, got {delegations}"


def test_ast_scan_has_a_positive_control():
    """Positive control: the same scan DOES fire on code that builds a
    response and raises. Proves the two assertions above can fail."""
    bad = ast.parse(
        "from x import JSONResponse\n"
        "def f():\n"
        "    r = JSONResponse(status_code=401, content={})\n"
        "    raise ValueError(no)\n"
    )
    found = [getattr(n.func, "id", None) for n in ast.walk(bad)
             if isinstance(n, ast.Call)]
    assert "JSONResponse" in found
    assert any(isinstance(n, ast.Raise) for n in ast.walk(bad))


# =========================================================================
# 2. The non-rejection invariant -- behavioural proof
# =========================================================================

class _Stub:
    """Minimal ASGI app that records that it was reached."""

    def __init__(self):
        self.reached = 0

    async def __call__(self, scope, receive, send):
        self.reached += 1
        await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"x-stub", b"1")]})
        await send({"type": "http.response.body", "body": b"ok"})


def _scope(method="POST", path="/sql", headers=None, client=("10.0.0.9", 5555)):
    hdrs = [(k.encode(), v.encode()) for k, v in (headers or {}).items()]
    return {"type": "http", "method": method, "path": path,
            "headers": hdrs, "client": client, "query_string": b""}


def _drive(mw, scope):
    sent = []

    async def send(m):
        sent.append(m)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    asyncio.run(mw(scope, receive, send))
    return sent


@pytest.mark.parametrize("mode", ["log", "enforce", "bogus", None])
def test_would_reject_request_still_passes_through(monkeypatch, mode):
    """A request the policy would REJECT is still delivered untouched -- in
    every value of KOI_AUTH_MODE, including enforce."""
    if mode is None:
        monkeypatch.delenv("KOI_AUTH_MODE", raising=False)
    else:
        monkeypatch.setenv("KOI_AUTH_MODE", mode)
    monkeypatch.setenv("KOI_MCP_TOKEN", "mcp-secret")

    stub = _Stub()
    mw = ao.AuthObservationMiddleware(stub)
    scope = _scope(method="POST", path="/sql")  # admin scope, no credential
    assert ao.decide("POST", "/sql", {})["verdict"].startswith("would_reject")

    sent = _drive(mw, scope)

    assert stub.reached == 1, "middleware must always delegate"
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 200, "status must be the app's, never a 401/403"
    assert sent[0]["headers"] == [(b"x-stub", b"1")], "headers passed verbatim"
    assert sent[1]["body"] == b"ok"


def test_pass_through_survives_broken_observation(monkeypatch):
    """Even if the observation code blows up, the request is unaffected."""
    def boom(scope):
        raise RuntimeError("observation exploded")

    monkeypatch.setattr(ao, "observe", boom)
    stub = _Stub()
    mw = ao.AuthObservationMiddleware(stub)
    sent = _drive(mw, _scope())
    assert stub.reached == 1
    assert sent[0]["status"] == 200


def test_shipped_emit_swallows_sink_failures(monkeypatch):
    """_emit runs in a finally block, so a raising sink would mask the
    response. The shipped _emit must swallow everything."""
    class _BadLogger:
        def info(self, msg):
            raise RuntimeError("sink exploded")

    monkeypatch.setattr(ao, "_AUTH_LOG", _BadLogger())
    stub = _Stub()
    sent = _drive(ao.AuthObservationMiddleware(stub), _scope())
    assert stub.reached == 1
    assert sent[0]["status"] == 200


def test_observe_never_raises():
    """The real observe() swallows malformed scopes instead of raising."""
    rec = ao.observe({"type": "http"})           # no method/path/headers/client
    assert rec["tag"] == ao.LOG_TAG
    rec2 = ao.observe({"type": "http", "headers": "not-iterable-pairs"})
    assert rec2["tag"] == ao.LOG_TAG


def test_non_http_scope_passes_through():
    stub = _Stub()
    mw = ao.AuthObservationMiddleware(stub)

    async def send(m):
        pass

    async def receive():
        return {"type": "lifespan.startup"}

    asyncio.run(mw({"type": "lifespan"}, receive, send))
    assert stub.reached == 1


# =========================================================================
# 3. Policy classification
# =========================================================================

@pytest.mark.parametrize("method,path,expect_class,expect_scope,expect_rule", [
    ("GET", "/health", "public", "", "R00_public_health"),
    # only GET /health is public; a mutating verb on it is not
    ("POST", "/health", "token", ao.SCOPE_CLAIMS, "R34_mutating_default"),
    ("POST", "/koi-net/events/poll", "exempt", "", "R01_koinet_federation"),
    ("GET", "/koi-net/health", "exempt", "", "R01_koinet_federation"),
    ("POST", "/koi-net/vault-sync/trigger", "exempt", "", "R01_koinet_federation"),
    ("POST", "/sql", "token", ao.SCOPE_ADMIN, "R20_admin_exact"),
    ("POST", "/reload-schemas", "token", ao.SCOPE_ADMIN, "R20_admin_exact"),
    ("POST", "/sync-relationships", "token", ao.SCOPE_ADMIN, "R20_admin_exact"),
    ("GET", "/diagnostics/config", "token", ao.SCOPE_ADMIN, "R23_diagnostics"),
    ("PATCH", "/entities/foo/wallet", "token", ao.SCOPE_ADMIN, "R24_entity_wallet"),
    ("GET", "/claims/identity", "token", ao.SCOPE_CLAIMS, "R30_claims_router"),
    ("POST", "/claims/", "token", ao.SCOPE_CLAIMS, "R30_claims_router"),
    ("GET", "/documents/abc", "token", ao.SCOPE_CLAIMS, "R31_documents_router"),
    ("POST", "/entities/merge", "token", ao.SCOPE_CLAIMS, "R33_entities_merge"),
    ("POST", "/tasks/ingest", "token", ao.SCOPE_CLAIMS, "R34_mutating_default"),
    ("PATCH", "/tasks/foo", "token", ao.SCOPE_CLAIMS, "R34_mutating_default"),
    ("POST", "/search", "token", ao.SCOPE_MCP, "R90_read_default"),
    ("POST", "/query", "token", ao.SCOPE_MCP, "R90_read_default"),
    ("POST", "/entity/resolve", "token", ao.SCOPE_MCP, "R90_read_default"),
    ("GET", "/stats", "token", ao.SCOPE_MCP, "R90_read_default"),
    ("GET", "/openapi.json", "token", ao.SCOPE_MCP, "R10_docs_static"),
])
def test_classification(method, path, expect_class, expect_scope, expect_rule):
    cls, scope, rule = ao.classify(method, path, {})
    assert (cls, scope, rule) == (expect_class, expect_scope, expect_rule)


def test_cors_preflight_classified_separately():
    cls, _, rule = ao.classify(
        "OPTIONS", "/search", {"access-control-request-method": "POST"})
    assert (cls, rule) == ("preflight", "R02_cors_preflight")


# =========================================================================
# 4. Verdicts
# =========================================================================

@pytest.fixture
def tokens(monkeypatch):
    monkeypatch.setenv("KOI_ADMIN_TOKEN", "admin-secret")
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", "claims-secret")
    monkeypatch.setenv("KOI_MCP_TOKEN", "mcp-secret")
    ao._ADMIN_TOKEN_CACHE["at"] = 0.0      # bust the 60s cache
    yield
    ao._ADMIN_TOKEN_CACHE["at"] = 0.0


def _bearer(tok):
    return {"authorization": "Bearer " + tok}


def test_health_is_public_with_no_credential(tokens):
    assert ao.decide("GET", "/health", {})["verdict"] == "public"


def test_koinet_is_exempt_not_would_reject(tokens):
    d = ao.decide("POST", "/koi-net/events/poll", {})
    assert d["verdict"] == "exempt"
    assert d["required_scope"] == ""


def test_no_credential_on_read_surface(tokens):
    d = ao.decide("GET", "/stats", {})
    assert d["verdict"] == "would_reject_no_credential"
    assert d["required_scope"] == ao.SCOPE_MCP


def test_mcp_token_allows_read_surface(tokens):
    d = ao.decide("GET", "/stats", _bearer("mcp-secret"))
    assert d["verdict"] == "allow"
    assert d["cred_scope"] == ao.SCOPE_MCP


def test_mcp_token_cannot_reach_admin(tokens):
    d = ao.decide("POST", "/sql", _bearer("mcp-secret"))
    assert d["verdict"] == "would_reject_wrong_scope"


def test_admin_token_satisfies_every_scope(tokens):
    for path in ("/sql", "/claims/", "/stats"):
        assert ao.decide("POST", path, _bearer("admin-secret"))["verdict"] == "allow"


def test_claims_token_covers_claims_and_read_but_not_admin(tokens):
    assert ao.decide("POST", "/claims/", _bearer("claims-secret"))["verdict"] == "allow"
    assert ao.decide("GET", "/stats", _bearer("claims-secret"))["verdict"] == "allow"
    assert ao.decide("POST", "/sql", _bearer("claims-secret"))["verdict"] == "would_reject_wrong_scope"


def test_unknown_token_is_distinguished_and_fingerprinted(tokens):
    d = ao.decide("GET", "/stats", _bearer("some-other-token"))
    assert d["verdict"] == "would_reject_unknown_token"
    assert len(d["cred_fp"]) == 8
    assert "some-other-token" not in json.dumps(d), "secret must never be logged"


def test_known_token_is_not_fingerprinted(tokens):
    d = ao.decide("GET", "/stats", _bearer("mcp-secret"))
    assert d["cred_fp"] == ""
    assert "mcp-secret" not in json.dumps(d)


def test_session_cookie_is_undetermined_not_rejected(tokens):
    d = ao.decide("GET", "/stats", {"cookie": "koi_session=abc123"})
    assert d["verdict"] == "undetermined_session_cookie"


def test_unset_scope_token_is_reported_distinctly(monkeypatch):
    monkeypatch.delenv("KOI_MCP_TOKEN", raising=False)
    monkeypatch.setenv("KOI_ADMIN_TOKEN", "admin-secret")
    ao._ADMIN_TOKEN_CACHE["at"] = 0.0
    d = ao.decide("GET", "/stats", {})
    assert d["verdict"] == "would_reject_no_credential_scope_token_unset"
    ao._ADMIN_TOKEN_CACHE["at"] = 0.0


# =========================================================================
# 5. Mode handling
# =========================================================================

@pytest.mark.parametrize("raw,expect", [
    (None, "log"), ("", "log"), ("log", "log"), ("LOG", "log"),
    # An unrecognized value is recorded as enforce, matching api.read_auth,
    # which fails closed on it.
    ("enforce", "enforce"), ("ENFORCE", "enforce"), ("nonsense", "enforce"),
])
def test_current_mode(monkeypatch, raw, expect):
    if raw is None:
        monkeypatch.delenv("KOI_AUTH_MODE", raising=False)
    else:
        monkeypatch.setenv("KOI_AUTH_MODE", raw)
    assert ao.current_mode() == expect


# =========================================================================
# 6. Log record shape
# =========================================================================

def test_emitted_record_is_json_and_has_the_analysis_fields(monkeypatch, tokens):
    captured = []
    monkeypatch.setattr(ao, "_emit", lambda rec: captured.append(rec))
    stub = _Stub()
    mw = ao.AuthObservationMiddleware(stub)
    _drive(mw, _scope(method="GET", path="/stats",
                      headers={"user-agent": "personal-koi-mcp/1", "host": "h"},
                      client=("127.0.0.1", 4242)))
    assert len(captured) == 1
    rec = captured[0]
    for field in ("tag", "ts", "mode", "client_ip", "method", "path", "origin", "rule",
                  "required_scope", "verdict", "cred_kind", "status",
                  "duration_ms", "route", "endpoint"):
        assert field in rec, field
    assert rec["tag"] == "KOIAUTH"
    assert rec["client_ip"] == "127.0.0.1"
    assert rec["status"] == 200
    assert rec["verdict"] == "would_reject_no_credential"
    json.loads(json.dumps(rec, default=str))
