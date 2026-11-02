"""Read authentication and scope binding for the tranche-1 read routes.

WHAT THIS IS
------------
A FastAPI dependency, one per tranche-1 route, that validates the caller's
credential, binds it to a ``ReadScope`` and returns that scope to the handler.
The handler passes the scope into its SQL, so authentication, scope resolution
and visibility are one path. The observation middleware
(``api.auth_observability``) is separate and still denies nothing.

TRANCHE 1 (route ids)
---------------------
  search            POST /search, GET /search
  query             POST /query
  discourse-search  GET /knowledge/discourse-search
  unified-search    GET /knowledge/unified-search
  sql               POST /sql (personal scope only; field/public get 403; always
                    enforced, whatever KOI_AUTH_MODE says: the early D7 token gate)

CREDENTIALS
-----------
``Authorization: Bearer <token>`` or ``X-API-Key: <token>``, the same carriers
the observation middleware recognizes, so its log predicts enforcement. The
``koi_session`` cookie is NOT a read credential here: tranche 1 has no browser
caller, and a bearer requirement is what keeps pages in the operator's browser
from reading these routes (03 client C3).

A token resolves, in order, to:
  1. an operator env token (admin/claims/mcp)        -> personal
  2. a ``KOI_READ_SCOPES_FILE`` entry                 -> its field/public scope
  3. a valid row in ``session_tokens``                -> personal if the email is
     in ``KOI_PERSONAL_PRINCIPALS``, otherwise no read scope (403 in enforce,
     public in log). Invalid, revoked or expired sessions are unknown tokens.

MODES (``KOI_AUTH_MODE``)
-------------------------
  log (default, unset or empty)
      No credential -> personal (today's behavior, kept only until enforcement).
      A present but unrecognized credential -> public (least privilege), never
      personal. A wrong-scope /sql call -> 403.
  enforce
      No credential or an unrecognized one -> 401. Wrong scope -> 403.
  any other value
      Treated as enforce, with a startup warning. A typo never reopens reads.

FAILURES ARE EXPLICIT, NEVER BROADER
------------------------------------
An invalid scopes file or kill-switch value -> 503 naming the configuration
error. A session lookup that fails -> 503 in enforce, public in log.

ROUTE KILL SWITCH (``KOI_READ_ROUTES_DISABLED``)
------------------------------------------------
Comma-separated route ids (or ``all``). A listed route returns 503 for every
caller. This is the recovery lever: it takes a misbehaving route out of service
without removing authentication from it. An unknown id disables every
tranche-1 route, because a typo must not leave the intended route serving.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, FrozenSet, Mapping, Optional, Tuple

from fastapi import HTTPException, Request, Response

from api.read_scope import (
    ReadScope,
    ReadScopeConfigError,
    load_scope_map,
    personal_scope,
    public_scope,
    scope_from_token_value,
)

logger = logging.getLogger("koi.readauth")

T1_ROUTES: Tuple[str, ...] = ("search", "query", "discourse-search", "unified-search", "sql")


def auth_mode() -> str:
    """``log`` when unset, empty or ``log``; ``enforce`` for every other value."""
    raw = (os.getenv("KOI_AUTH_MODE") or "").strip().lower()
    return "log" if raw in ("", "log") else "enforce"


def disabled_routes() -> FrozenSet[str]:
    """Route ids taken out of service. Raises ReadScopeConfigError on an
    unknown id; callers then disable every tranche-1 route."""
    raw = os.getenv("KOI_READ_ROUTES_DISABLED", "")
    items = {x.strip() for x in raw.split(",") if x.strip()}
    unknown = items - set(T1_ROUTES) - {"all"}
    if unknown:
        raise ReadScopeConfigError(
            f"KOI_READ_ROUTES_DISABLED has unknown route ids {sorted(unknown)}; "
            f"valid: {list(T1_ROUTES) + ['all']}")
    return frozenset(T1_ROUTES) if "all" in items else frozenset(items)


def personal_principals() -> FrozenSet[str]:
    raw = os.getenv("KOI_PERSONAL_PRINCIPALS", "")
    return frozenset(x.strip().lower() for x in raw.split(",") if x.strip())


def check_config() -> Optional[str]:
    """Validate every read-auth setting once at startup. Returns an error string
    for the log, or None. Request-time checks still apply; this only makes a bad
    value visible before the first request."""
    problems = []
    try:
        disabled_routes()
    except ReadScopeConfigError as e:
        problems.append(str(e))
    try:
        load_scope_map()
    except ReadScopeConfigError as e:
        problems.append(str(e))
    raw = (os.getenv("KOI_AUTH_MODE") or "").strip().lower()
    if raw not in ("", "log", "enforce"):
        problems.append(f"KOI_AUTH_MODE={raw!r} is not log/enforce; treated as enforce")
    return "; ".join(problems) or None


def _fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8", "replace")).hexdigest()[:8]


def credential_from_headers(headers: Mapping[str, str]) -> Tuple[str, Optional[str]]:
    """Return ``(kind, token)``. ``kind`` is ``none``, ``bearer``, ``x-api-key``
    or ``malformed``. A malformed Authorization header is a present credential
    that matches nothing; it is never treated as absent. So is a repeated
    Authorization or X-API-Key header: the observation log keeps the last copy
    and Starlette's ``get`` the first, so neither may be trusted."""
    getlist = getattr(headers, "getlist", None)
    if getlist and (len(getlist("authorization")) > 1 or len(getlist("x-api-key")) > 1):
        return "malformed", None
    auth = (headers.get("authorization") or "").strip()
    if auth:
        parts = auth.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            return "bearer", parts[1]
        return "malformed", None
    api_key = (headers.get("x-api-key") or "").strip()
    if api_key:
        return "x-api-key", api_key
    return "none", None


async def _session_email(token: str, pool: Any) -> Optional[str]:
    """Email for a valid, unrevoked, unexpired session token, else None.
    Database errors propagate; the caller decides (never as success)."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_email, expires_at, revoked_at FROM session_tokens WHERE token_hash = $1",
            hashlib.sha256(token.encode()).hexdigest(),
        )
    if not row or row["revoked_at"] is not None:
        return None
    if row["expires_at"] is None or row["expires_at"].timestamp() < time.time():
        return None
    return row["user_email"]


@dataclass(frozen=True)
class ReadAuthDecision:
    scope: Optional[ReadScope]   # None means denied
    status: int                  # HTTP status when denied, 200 when allowed
    reason: str


def _allow(scope: ReadScope, reason: str) -> ReadAuthDecision:
    return ReadAuthDecision(scope, 200, reason)


def _deny(status: int, reason: str) -> ReadAuthDecision:
    return ReadAuthDecision(None, status, reason)


async def resolve_request_scope(headers: Mapping[str, str], pool: Any, *, mode: str,
                                personal_only: bool = False) -> ReadAuthDecision:
    """The whole policy for one request. No side effects beyond the session
    lookup. ``mode`` is ``log`` or ``enforce``."""
    kind, token = credential_from_headers(headers)
    enforce = mode == "enforce"

    if kind == "none":
        if enforce:
            return _deny(401, "no_credential")
        decision = _allow(personal_scope(principal="anonymous:log-mode"), "no_credential_log_mode")
    else:
        scope: Optional[ReadScope] = None
        reason = "unknown_credential"
        if token:
            try:
                scope = scope_from_token_value(token)
            except ReadScopeConfigError as e:
                logger.error("read-auth configuration error: %s", e)
                return _deny(503, "config_error")
            if scope is not None:
                reason = "operator_token" if scope.is_personal else "scope_file"
            elif pool is not None:
                try:
                    email = await _session_email(token, pool)
                except Exception as e:
                    logger.warning("read-auth session lookup failed: %s", type(e).__name__)
                    if enforce:
                        return _deny(503, "session_lookup_failed")
                    email = None
                if email is not None:
                    if email.strip().lower() in personal_principals():
                        scope, reason = personal_scope(principal=email), "session_personal"
                    elif enforce:
                        return _deny(403, "session_principal_has_no_read_scope")
                    else:
                        reason = "session_principal_has_no_read_scope"
        if scope is None:
            if enforce:
                return _deny(401, reason)
            decision = _allow(public_scope(principal="unrecognized:log-mode"), reason + "_log_mode")
        else:
            decision = _allow(scope, reason)

    if personal_only and not decision.scope.is_personal:
        return _deny(403, "personal_scope_required")
    return decision


_DENY_DETAIL = {
    401: "Authentication required: send Authorization: Bearer <token>.",
    403: "This credential's read scope does not permit this route.",
}


def make_read_scope_dependency(route_id: str, pool_provider: Callable[[], Any], *,
                               personal_only: bool = False, always_enforce: bool = False):
    """FastAPI dependency for one tranche-1 route. ``pool_provider`` returns the
    pool used for session-token lookup (read late: the pool is created at
    startup). ``always_enforce`` makes the route require a valid credential in
    log mode too."""
    if route_id not in T1_ROUTES:
        raise ValueError(f"unknown tranche-1 route id {route_id!r}")

    async def read_scope_dependency(request: Request, response: Response) -> ReadScope:
        try:
            disabled = disabled_routes()
        except ReadScopeConfigError as e:
            logger.error("read-auth configuration error: %s", e)
            raise HTTPException(503, "Read routes are disabled: KOI_READ_ROUTES_DISABLED is invalid.")
        if route_id in disabled:
            raise HTTPException(503, f"{route_id} is disabled by KOI_READ_ROUTES_DISABLED.")

        mode = "enforce" if always_enforce else auth_mode()
        decision = await resolve_request_scope(
            request.headers, pool_provider(), mode=mode, personal_only=personal_only)
        kind, token = credential_from_headers(request.headers)
        if decision.scope is None or not decision.scope.is_personal:
            logger.info("read-auth route=%s mode=%s status=%s reason=%s cred=%s fp=%s",
                        route_id, mode, decision.status, decision.reason, kind,
                        _fingerprint(token) if token else "")
        if decision.scope is None:
            if decision.status == 503:
                detail = "Read authorization is unavailable (configuration or credential store error)."
            else:
                detail = _DENY_DETAIL[decision.status]
            headers = {"WWW-Authenticate": "Bearer"} if decision.status == 401 else None
            raise HTTPException(decision.status, detail, headers=headers)
        response.headers["X-KOI-Read-Scope"] = decision.scope.view
        return decision.scope

    return read_scope_dependency


__all__ = [
    "T1_ROUTES",
    "ReadAuthDecision",
    "auth_mode",
    "check_config",
    "credential_from_headers",
    "disabled_routes",
    "make_read_scope_dependency",
    "personal_principals",
    "resolve_request_scope",
]
