"""Log-only authentication observability for the personal KOI API.

WHAT THIS IS
------------
One global ASGI middleware that, for every HTTP request, evaluates the
*intended* bearer-token policy and writes a single structured JSON line
describing what the decision WOULD have been.  It denies nothing.

THE NON-REJECTION INVARIANT  (the whole point of this module)
------------------------------------------------------------
``AuthObservationMiddleware.__call__`` below has exactly two exits and both of
them are ``await self.app(...)`` delegations to the wrapped application.  This
module constructs no Response object of any kind, never emits an
``http.response.start`` message of its own, and contains no ``raise``
statement.  There is therefore no code path -- reachable or not, and
*regardless of any environment variable* -- by which this module can deny,
redirect, stall, or alter a request or a response.
``tests/test_auth_observability.py`` asserts both properties mechanically
(AST scan) and behaviourally (pass-through of a would-reject request).

``KOI_AUTH_MODE`` selects only what is written in the ``mode`` field of the log
line; this middleware still denies nothing in any mode.  Enforcement for the
tranche-1 read routes lives in a separate module, ``api.read_auth`` (a per-route
dependency), which reads the same variable with the same parser, so the
recorded mode is the mode those routes act on.

INTENDED POLICY (evaluated, not enforced)
-----------------------------------------
  public   ``GET /health`` only -- no credential needed.  Monitoring scripts
           guard on it and fail CLOSED, so it must never require a credential.
  exempt   ``/koi-net/*`` -- already authenticated by signed envelope +
           approved-edge; it carries peers' federation polls.  Never gets a
           bearer requirement.
  admin    ``KOI_ADMIN_TOKEN`` (env, else ``$KOI_STATE_DIR/admin_token``)
  claims   ``KOI_CLAIMS_SERVICE_TOKEN`` -- the service-write token
  mcp      ``KOI_MCP_TOKEN`` (NEW) -- the broad read/query surface

Scopes nest: an admin token satisfies claims and mcp; a claims token satisfies
mcp.  See ``GRANTS``.

OUTPUT
------
JSONL, one object per request, to ``$KOI_AUTH_LOG_PATH`` (default
``$KOI_STATE_DIR/auth-observations.jsonl``), rotated at 64 MiB x 6.  Every
line carries ``"tag": "KOIAUTH"`` so it is greppable wherever it lands; if the
file cannot be opened the same lines go to stderr (and thus journald) with the
literal prefix ``KOIAUTH ``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import sys
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional, Tuple

from api.auth import read_admin_token  # reuse the existing admin-token reader

LOG_TAG = "KOIAUTH"

_setup_logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Mode
# --------------------------------------------------------------------------

def current_mode() -> str:
    """``log`` when unset, empty or ``log``; ``enforce`` for any other value.

    An unrecognized value counts as ``enforce`` because ``api.read_auth`` acts
    on it that way (a typo must not reopen reads); recording ``log`` for it
    would make the log disagree with what the routes did.  This middleware only
    records the value.
    """
    raw = (os.getenv("KOI_AUTH_MODE") or "").strip().lower()
    return "log" if raw in ("", "log") else "enforce"


def warn_if_enforce_requested() -> None:
    """Called once at app startup so the operator is told what enforce means."""
    raw = (os.getenv("KOI_AUTH_MODE") or "").strip().lower()
    if raw not in ("", "log", "enforce"):
        _setup_logger.warning(
            "KOI_AUTH_MODE=%r is not log/enforce; it is treated as enforce.", raw)
    if current_mode() == "enforce":
        _setup_logger.warning(
            "KOI_AUTH_MODE=enforce: the tranche-1 read routes (api/read_auth.py) "
            "deny requests without a valid credential. Every other route is "
            "still observed only; this middleware denies nothing."
        )


# --------------------------------------------------------------------------
# Scopes and tokens
# --------------------------------------------------------------------------

# Fixed cred_kind labels. cred_kind MUST always be one of these -- it is never
# built from request content. See inspect_credential() for why.
KIND_NONE = "none"
KIND_BEARER = "bearer"
KIND_X_API_KEY = "x-api-key"
KIND_OTHER_SCHEME = "other_scheme"              # non-bearer scheme + a value
KIND_MALFORMED_NO_SCHEME = "malformed_no_scheme"  # one opaque token, no scheme
KIND_MALFORMED_EMPTY = "malformed_empty"          # header present but empty/whitespace
CRED_KINDS = frozenset({
    KIND_NONE, KIND_BEARER, KIND_X_API_KEY, KIND_OTHER_SCHEME,
    KIND_MALFORMED_NO_SCHEME, KIND_MALFORMED_EMPTY,
})


SCOPE_ADMIN = "admin"
SCOPE_CLAIMS = "claims"
SCOPE_MCP = "mcp"

# Which required-scopes a presented token satisfies.
GRANTS: Dict[str, Tuple[str, ...]] = {
    SCOPE_ADMIN: (SCOPE_ADMIN, SCOPE_CLAIMS, SCOPE_MCP),
    SCOPE_CLAIMS: (SCOPE_CLAIMS, SCOPE_MCP),
    SCOPE_MCP: (SCOPE_MCP,),
}

_ADMIN_TOKEN_CACHE: Dict[str, Any] = {"value": None, "at": 0.0}
_ADMIN_TOKEN_TTL = 60.0


def _admin_token() -> Optional[str]:
    now = time.monotonic()
    if now - _ADMIN_TOKEN_CACHE["at"] > _ADMIN_TOKEN_TTL:
        try:
            _ADMIN_TOKEN_CACHE["value"] = read_admin_token()
        except Exception:
            _ADMIN_TOKEN_CACHE["value"] = None
        _ADMIN_TOKEN_CACHE["at"] = now
    return _ADMIN_TOKEN_CACHE["value"]


def _configured_tokens() -> Dict[str, str]:
    """Scope -> configured secret, for scopes that actually have one set."""
    out: Dict[str, str] = {}
    admin = _admin_token()
    if admin:
        out[SCOPE_ADMIN] = admin
    claims = os.getenv("KOI_CLAIMS_SERVICE_TOKEN") or ""
    if claims:
        out[SCOPE_CLAIMS] = claims
    mcp = os.getenv("KOI_MCP_TOKEN") or ""
    if mcp:
        out[SCOPE_MCP] = mcp
    return out


def _fingerprint(secret: str) -> str:
    """Non-reversible 8-hex-char tag so distinct unknown callers can be told
    apart in the log without the secret ever being written."""
    return hashlib.sha256(secret.encode("utf-8", "replace")).hexdigest()[:8]


# --------------------------------------------------------------------------
# Route classification
#
# Ordered rules, first match wins.  Every log line records which rule matched
# (``rule``) so the classification itself can be audited from the log data
# rather than taken on faith.
# --------------------------------------------------------------------------

MUTATING = ("POST", "PUT", "PATCH", "DELETE")

# POST/PATCH-shaped endpoints that are semantically READS; they belong to the
# broad mcp surface, not to the service-write surface.
READ_SHAPED_POSTS = frozenset({
    "/search", "/query", "/entity-search", "/entity/resolve",
    "/entities/mentioned-in", "/get-contextual-candidates", "/search-sessions",
    "/knowledge/recall-walk", "/chat", "/web/evaluate", "/web/preview",
    "/tools/parse-relate-clause", "/intents/match",
    "/commitments/routing-suggestions", "/commitments/extract-from-transcript",
})

ADMIN_EXACT = frozenset({"/sql", "/reload-schemas", "/sync-relationships"})


def classify(method: str, path: str, headers: Dict[str, str]) -> Tuple[str, str, str]:
    """Return ``(verdict_class, required_scope, rule)``.

    ``verdict_class`` is one of ``public`` / ``exempt`` / ``preflight`` /
    ``token`` (meaning: a token is required, and ``required_scope`` says which).
    """
    m = method.upper()

    if m == "GET" and path == "/health":
        return "public", "", "R00_public_health"

    if path == "/koi-net" or path.startswith("/koi-net/"):
        # Signed-envelope + approved-edge authenticated already. Untouched.
        return "exempt", "", "R01_koinet_federation"

    if m == "OPTIONS" and "access-control-request-method" in headers:
        return "preflight", "", "R02_cors_preflight"

    if path in ("/docs", "/redoc", "/openapi.json", "/demo",
                "/docs/oauth2-redirect") or path.startswith("/static/"):
        return "token", SCOPE_MCP, "R10_docs_static"

    # ---- admin -----------------------------------------------------------
    if path in ADMIN_EXACT or path.startswith("/sql/"):
        return "token", SCOPE_ADMIN, "R20_admin_exact"
    if path.startswith("/diagnostics/"):
        return "token", SCOPE_ADMIN, "R23_diagnostics"
    if m == "PATCH" and path.startswith("/entities/") and path.endswith("/wallet"):
        return "token", SCOPE_ADMIN, "R24_entity_wallet"

    # ---- claims / service-write -----------------------------------------
    # Mirrors what claims_router already enforces by hand today (all methods,
    # including GET /claims/identity).
    if path == "/claims" or path.startswith("/claims/"):
        return "token", SCOPE_CLAIMS, "R30_claims_router"
    # documents_router guards BOTH POST /documents/ingest and GET /documents/{rid}
    if path == "/documents" or path.startswith("/documents/"):
        return "token", SCOPE_CLAIMS, "R31_documents_router"
    if m == "POST" and path == "/entities/merge":
        return "token", SCOPE_CLAIMS, "R33_entities_merge"

    if m in MUTATING and path not in READ_SHAPED_POSTS:
        return "token", SCOPE_CLAIMS, "R34_mutating_default"

    # ---- broad read surface ---------------------------------------------
    return "token", SCOPE_MCP, "R90_read_default"


# --------------------------------------------------------------------------
# Credential inspection
# --------------------------------------------------------------------------

def inspect_credential(headers: Dict[str, str]) -> Dict[str, Any]:
    """Identify what credential (if any) was presented, and which scopes it
    grants.  Never logs the secret itself -- only a scope name or an 8-char
    fingerprint."""
    tokens = _configured_tokens()
    auth = headers.get("authorization", "")
    presented: Optional[str] = None
    kind = "none"

    unmatched_fp_src: Optional[str] = None

    if auth:
        scheme, _, rest = auth.partition(" ")
        s = scheme.lower()
        if s == "bearer" and rest.strip():
            presented, kind = rest.strip(), "bearer"
        else:
            # SECURITY (fixed 2026-09-08): cred_kind is a FIXED LABEL, never
            # derived from header content. The previous line was
            #     kind = "non_bearer:" + (s or "malformed")
            # For a bare `Authorization: <token>` with no scheme, partition()
            # puts the WHOLE TOKEN in `scheme`, so this lowercased a live
            # credential into the durable log. Lowercasing resembled
            # sanitising; it was a mangled secret. Reported by the Codex lane;
            # synthetic credentials only, no real exposure established.
            if not scheme:
                kind = KIND_MALFORMED_EMPTY
            elif rest.strip():
                kind = KIND_OTHER_SCHEME
            else:
                kind = KIND_MALFORMED_NO_SCHEME
            # Distinct malformed callers stay distinguishable by fingerprint of
            # the whole header -- a one-way digest, never the value.
            unmatched_fp_src = auth.strip()
    elif headers.get("x-api-key"):
        presented, kind = headers["x-api-key"].strip(), "x-api-key"

    granted: Tuple[str, ...] = ()
    matched_scope = ""
    fp = ""
    if presented:
        fp = _fingerprint(presented)
        for scope_name, secret in tokens.items():
            if secrets.compare_digest(presented.encode("utf-8"), secret.encode("utf-8")):
                matched_scope = scope_name
                granted = GRANTS[scope_name]
                break
    elif unmatched_fp_src:
        # Malformed header: nothing was "presented" to match against a scope, so
        # the fingerprint is of the header shape, not of a credential we were
        # offered. This keeps distinct malformed callers distinguishable.
        fp = _fingerprint(unmatched_fp_src)

    has_cookie = "koi_session=" in headers.get("cookie", "")

    return {
        "cred_kind": kind,
        "cred_scope": matched_scope,
        # Original guard, unchanged for presented credentials: a token we could
        # match is never fingerprinted, and neither is one we could not match
        # only because no tokens are configured. The malformed arm is additive --
        # `presented` is None there, so this fingerprints header shape, never an
        # offered secret. (Widening this to `not matched_scope` regressed
        # test_known_token_is_not_fingerprinted; caught by that test.)
        "cred_fp": fp if ((presented and not matched_scope) or unmatched_fp_src) else "",
        "granted": list(granted),
        "session_cookie": has_cookie,
        "tokens_configured": sorted(tokens.keys()),
    }


def decide(method: str, path: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Pure policy evaluation.  No side effects, no I/O beyond reading the
    configured tokens.  Returns the fields that go into the log line."""
    verdict_class, required, rule = classify(method, path, headers)

    if verdict_class in ("public", "exempt", "preflight"):
        return {"verdict": verdict_class, "required_scope": required,
                "rule": rule, "cred_kind": "none", "cred_scope": "",
                "cred_fp": "", "granted": [], "session_cookie": False,
                "tokens_configured": []}

    cred = inspect_credential(headers)

    if cred["cred_scope"] and required in cred["granted"]:
        verdict = "allow"
    elif cred["cred_scope"]:
        verdict = "would_reject_wrong_scope"
    elif cred["cred_kind"] != "none":
        verdict = "would_reject_unknown_token"
    elif cred["session_cookie"]:
        # A session cookie may well be valid; validating it needs a DB round
        # trip that does not belong in middleware. Say so rather than guess.
        verdict = "undetermined_session_cookie"
    elif required not in cred["tokens_configured"]:
        verdict = "would_reject_no_credential_scope_token_unset"
    else:
        verdict = "would_reject_no_credential"

    out = {"verdict": verdict, "required_scope": required, "rule": rule}
    out.update(cred)
    return out


# --------------------------------------------------------------------------
# Log sink
# --------------------------------------------------------------------------

def _default_log_path() -> str:
    explicit = os.getenv("KOI_AUTH_LOG_PATH")
    if explicit:
        return explicit
    state_dir = os.getenv("KOI_STATE_DIR") or "/tmp"
    return os.path.join(state_dir, "auth-observations.jsonl")


def _build_sink() -> Tuple[logging.Logger, str]:
    log = logging.getLogger("koi.authobs")
    log.setLevel(logging.INFO)
    log.propagate = False          # keep this stream out of the main app log
    if log.handlers:
        return log, getattr(log, "_koi_sink", "already-configured")

    path = _default_log_path()
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        handler: logging.Handler = RotatingFileHandler(
            path, maxBytes=64 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        sink = path
    except Exception as exc:  # pragma: no cover - fallback only
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter(LOG_TAG + " %(message)s"))
        sink = "stderr (file sink unavailable: %s)" % exc
    log.addHandler(handler)
    setattr(log, "_koi_sink", sink)
    return log, sink


_AUTH_LOG, _AUTH_SINK = _build_sink()


def log_sink_description() -> str:
    return _AUTH_SINK


def _emit(record: Dict[str, Any]) -> None:
    """Write one JSONL line.  Swallows everything: observability must never be
    able to affect the request."""
    try:
        _AUTH_LOG.info(json.dumps(record, default=str, separators=(",", ":")))
    except Exception:
        try:
            sys.stderr.write(LOG_TAG + " emit-failed\n")
        except Exception:
            pass


# --------------------------------------------------------------------------
# Observation
# --------------------------------------------------------------------------

def _headers_from_scope(scope: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in scope.get("headers") or []:
        try:
            out[k.decode("latin-1").lower()] = v.decode("latin-1")
        except Exception:
            continue
    return out


def observe(scope: Dict[str, Any]) -> Dict[str, Any]:
    """Build the log record for a request.  Returns a dict; on any internal
    problem returns a minimal record with ``observe_error`` set.  Callers treat
    the result as opaque."""
    try:
        headers = _headers_from_scope(scope)
        method = scope.get("method", "?")
        path = scope.get("path", "")
        client = scope.get("client") or (None, None)
        decision = decide(method, path, headers)
        record = {
            "tag": LOG_TAG,
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "mode": current_mode(),
            "client_ip": client[0],
            "client_port": client[1],
            "method": method,
            "path": path,
            "has_query": bool(scope.get("query_string")),
            "http_host": headers.get("host", ""),
            # Origin feeds the S7 CORS allowlist: which browser origins call us.
            "origin": headers.get("origin", "")[:200],
            "xff": headers.get("x-forwarded-for", "")[:80],
            "ua": headers.get("user-agent", "")[:120],
        }
        record.update(decision)
        return record
    except Exception as exc:
        return {"tag": LOG_TAG, "observe_error": repr(exc)[:200],
                "path": str(scope.get("path", ""))[:200]}


def _route_template(scope: Dict[str, Any], path: str) -> str:
    """Best-effort ``/entity/{entity_uri}``-style grouping key.  Starlette 0.27
    does not put the route on the scope, but it does put ``path_params`` there
    once the router has matched, and this middleware shares that scope dict."""
    params = scope.get("path_params") or {}
    tmpl = path
    for key, value in params.items():
        sval = str(value)
        if sval and sval in tmpl:
            tmpl = tmpl.replace(sval, "{" + key + "}", 1)
    return tmpl


def _endpoint_name(scope: Dict[str, Any]) -> str:
    ep = scope.get("endpoint")
    if ep is None:
        return ""
    return "%s.%s" % (getattr(ep, "__module__", "?"),
                      getattr(ep, "__qualname__", getattr(ep, "__name__", "?")))


# --------------------------------------------------------------------------
# The middleware
# --------------------------------------------------------------------------

class AuthObservationMiddleware:
    """Observe-only auth middleware.

    ===================== READ THIS ======================================
    Every exit from ``__call__`` is a delegation to ``self.app``.  There is
    no branch that constructs a response, and no ``raise`` in this module.
    Log-only is therefore a property of the code, not of configuration.
    ======================================================================
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)      # exit 1: pass through
            return

        # Belt and braces: observe() already swallows everything, and this
        # wrapper means even a monkeypatched/refactored observe() cannot
        # escape into the request path.
        try:
            record = observe(scope)
        except Exception as _obs_exc:
            record = {"tag": LOG_TAG, "observe_error": repr(_obs_exc)[:200]}
        started = time.monotonic()
        status_holder: Dict[str, Any] = {"status": None}

        async def _send(message: Dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                status_holder["status"] = message.get("status")
            await send(message)                        # verbatim delegation

        try:
            await self.app(scope, receive, _send)      # exit 2: pass through
        finally:
            try:
                record["status"] = status_holder["status"]
                record["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
                record["route"] = _route_template(scope, record.get("path", ""))
                record["endpoint"] = _endpoint_name(scope)
            except Exception:
                pass
            _emit(record)


__all__ = [
    "AuthObservationMiddleware",
    "classify",
    "decide",
    "inspect_credential",
    "current_mode",
    "warn_if_enforce_requested",
    "log_sink_description",
    "LOG_TAG",
]
