"""Shared admin authentication helpers.

Extracted from koi_net_router to be reusable across routers.
"""

import os
from typing import Optional

from fastapi import Request
from fastapi.responses import JSONResponse


def read_admin_token() -> Optional[str]:
    """Read admin token from env or state dir file."""
    admin_token = os.getenv("KOI_ADMIN_TOKEN")
    if admin_token:
        return admin_token

    state_dir = os.getenv("KOI_STATE_DIR", "")
    token_path = os.path.join(state_dir, "admin_token") if state_dir else ""
    if token_path and os.path.exists(token_path):
        with open(token_path) as f:
            return f.read().strip()
    return None


def enforce_local_admin(request: Request) -> Optional[JSONResponse]:
    """Return error response if request is not localhost/admin, else None."""
    client_host = request.client.host if request.client else None
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        return JSONResponse(
            status_code=403,
            content={"error": "Endpoint is localhost-only"},
        )

    admin_token = read_admin_token()
    if admin_token:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer ") or auth_header[7:] != admin_token:
            return JSONResponse(
                status_code=401,
                content={"error": "Invalid or missing admin token"},
            )
    return None
