"""Reusable FastAPI auth dependencies for pool-backed routers.

Why this exists:
  ``src/services/auth_service.get_authorized_user`` already validates session
  Bearer tokens, but opens a fresh ``asyncpg`` connection per request because
  auth_service owns its own DB lifecycle. Routers that are constructed with a
  shared ``asyncpg.Pool`` (claims, intent, etc.) should validate through the
  same pool to avoid per-request connect/disconnect overhead.

  This module exposes factories — pass a pool, get typed FastAPI dependencies
  bound to it:

      # At router-creation time
      require_auth = make_require_auth(pool)

      # On an endpoint
      @router.post("/", status_code=201)
      async def create_claim(
          req: ClaimCreateRequest,
          user_email: str = Depends(require_auth),
      ):
          # user_email is guaranteed — 401 is raised before the body runs
          ...

Three factories:
  * ``make_require_auth(pool)`` — hard gate; 401 on missing / invalid / revoked / expired.
  * ``make_optional_auth(pool)`` — soft; returns the email if a valid token is
    present, otherwise None. Use on reads that want audit context without
    blocking anonymous access.
  * ``make_service_token_auth(pool, env_var)`` — accepts either a valid
    session token OR a fixed service token from env (for CI / backend-to-
    backend callers that don't have an OAuth identity).

Two token-delivery mechanisms, both equivalent:
  * ``Authorization: Bearer <token>`` header — used by MCPs and API clients.
  * ``koi_session`` HttpOnly cookie — used by browsers (the /claims portal).
    Set by the web OAuth flow (``GET /api/koi/auth/web/login``).

Both resolve to the same ``session_tokens`` table. A single request can
include either; header takes precedence if both are present (explicit wins).

All three factories call the same validator so there's a single source of
truth for what "authenticated" means.

SECURITY NOTES
  * Token hashing matches ``auth_service.hash_token`` (SHA-256 hex) — the DB
    only ever stores hashes, never raw tokens.
  * ``session_tokens`` rows are checked for ``revoked_at`` and ``expires_at``
    on every request.
  * The ``Authorization`` header parser is strict (``Bearer <token>``, two
    whitespace-separated parts, case-insensitive scheme).
  * The session cookie must be set with ``HttpOnly; Secure; SameSite=Lax`` —
    see auth_service.set_session_cookie.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from typing import Awaitable, Callable, Optional

from asyncpg.pool import Pool
from fastapi import Cookie, Header, HTTPException

logger = logging.getLogger(__name__)

# Cookie name used by the browser OAuth flow. Must match what
# auth_service.web_login / auth_service.auth_callback set via Set-Cookie.
SESSION_COOKIE_NAME = "koi_session"


# ---------------------------------------------------------------------------
# Internal: token validation (single source of truth)
# ---------------------------------------------------------------------------

def _hash_token(token: str) -> str:
    """SHA-256 hash — must match src/services/auth_service.hash_token."""
    return hashlib.sha256(token.encode()).hexdigest()


def _parse_bearer(authorization: Optional[str]) -> Optional[str]:
    """Return the raw token from an Authorization header, or None if malformed."""
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1]


def _extract_token(authorization: Optional[str], cookie_token: Optional[str]) -> Optional[str]:
    """Pick a session token from either the Authorization header or a cookie.

    Header wins if both are present — explicit Bearer is a stronger signal
    than an auto-attached cookie, and matches the principle of least-surprise
    for API clients that also happen to have a browser cookie lying around.
    """
    header_token = _parse_bearer(authorization)
    if header_token is not None:
        return header_token
    if cookie_token:
        return cookie_token
    return None


async def _validate_session_token(token: str, pool: Pool) -> str:
    """Validate a session token against ``session_tokens``.

    Returns the authenticated user's email.
    Raises :class:`HTTPException` (401) on invalid / revoked / expired tokens.
    """
    token_hash = _hash_token(token)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT user_email, expires_at, revoked_at
            FROM session_tokens
            WHERE token_hash = $1
            """,
            token_hash,
        )
    if not row:
        raise HTTPException(status_code=401, detail="Invalid session token")
    if row["revoked_at"]:
        raise HTTPException(status_code=401, detail="Session token has been revoked")
    if row["expires_at"].timestamp() < time.time():
        raise HTTPException(status_code=401, detail="Session token has expired")
    return row["user_email"]


# ---------------------------------------------------------------------------
# Public factories
# ---------------------------------------------------------------------------

RequireAuthDep = Callable[..., Awaitable[str]]
OptionalAuthDep = Callable[..., Awaitable[Optional[str]]]


def make_require_auth(pool: Pool) -> RequireAuthDep:
    """Factory: FastAPI dependency that requires a valid session token.

    Accepts either ``Authorization: Bearer <token>`` header (API clients /
    MCPs) or ``koi_session`` HttpOnly cookie (browsers). Returns the
    authenticated user's email. Raises 401 on any failure.
    """

    async def require_auth(
        authorization: Optional[str] = Header(None),
        koi_session: Optional[str] = Cookie(None),
    ) -> str:
        token = _extract_token(authorization, koi_session)
        if token is None:
            raise HTTPException(
                status_code=401,
                detail=(
                    "Authentication required. Provide an Authorization: Bearer "
                    "header (for API clients) or sign in to obtain a session "
                    "cookie (for browsers)."
                ),
            )
        return await _validate_session_token(token, pool)

    return require_auth


def make_optional_auth(pool: Pool) -> OptionalAuthDep:
    """Factory: FastAPI dependency that optionally binds a session to the request.

    Returns ``user_email`` if a valid token is present (header or cookie),
    otherwise ``None``. Never raises. Use on read endpoints where anonymous
    access is allowed but an authenticated identity (when present) should be
    recorded for audit.
    """

    async def optional_auth(
        authorization: Optional[str] = Header(None),
        koi_session: Optional[str] = Cookie(None),
    ) -> Optional[str]:
        token = _extract_token(authorization, koi_session)
        if token is None:
            return None
        try:
            return await _validate_session_token(token, pool)
        except HTTPException:
            return None

    return optional_auth


def make_service_token_auth(
    pool: Pool,
    env_var: str = "KOI_CLAIMS_SERVICE_TOKEN",
    service_identity: str = "service:claims-service",
) -> RequireAuthDep:
    """Factory: dependency that accepts a valid session token (header or
    cookie) OR a fixed service token read from environment.

    Use for backend-to-backend callers (CI jobs, scheduled anchoring, the
    claims-service daemon itself) that need to write but don't have an OAuth
    identity.

    The service token — if set — is compared constant-time against the
    header-only token (never cookie; services don't have cookies). When it
    matches, the dependency returns ``service_identity`` so endpoints can
    still populate ``operator_uri`` / ``created_by`` with a sensible value.
    """

    import secrets as _secrets

    async def service_or_session(
        authorization: Optional[str] = Header(None),
        koi_session: Optional[str] = Cookie(None),
    ) -> str:
        # Try service-token match first (header only — services don't use cookies)
        header_token = _parse_bearer(authorization)
        if header_token is not None:
            service_token = os.getenv(env_var, "")
            if service_token and _secrets.compare_digest(header_token, service_token):
                return service_identity

        # Fall through to regular session validation (either delivery mechanism)
        token = _extract_token(authorization, koi_session)
        if token is None:
            raise HTTPException(
                status_code=401,
                detail=(
                    "Authentication required. Provide an Authorization: Bearer "
                    "header (for API clients) or sign in to obtain a session "
                    "cookie (for browsers)."
                ),
            )
        return await _validate_session_token(token, pool)

    return service_or_session


__all__ = [
    "SESSION_COOKIE_NAME",
    "make_require_auth",
    "make_optional_auth",
    "make_service_token_auth",
    "RequireAuthDep",
    "OptionalAuthDep",
]
