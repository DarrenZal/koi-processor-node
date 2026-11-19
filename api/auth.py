"""Shared admin authentication: the one fail-closed rule for every admin route.

api.koi_net_router's guards (`_enforce_local_admin`, `_enforce_commons_admin`) and this
module's `enforce_local_admin` (vault_sync_router, discourse_links_router) all decide by
`local_admin_refusal` (register entry 26). This guard used to be fail-open: it admitted a
loopback caller when no token was configured.

- With no admin token configured, every caller is refused with 503, loopback included.
  Loopback alone is not authority.
- A caller that is not a loopback address gets 403. Loopback is decided by address,
  IPv4-mapped IPv6 unwrapped, never by a name such as "localhost".
- A wrong or missing bearer gets 401. Bearers are compared in constant time.
- A token file that cannot be read, or is empty, counts as not configured.
"""

import hmac
import ipaddress
import os
from typing import Optional, Tuple

from fastapi import Request
from fastapi.responses import JSONResponse


def read_admin_token() -> Optional[str]:
    """The configured admin token, from KOI_ADMIN_TOKEN or <KOI_STATE_DIR>/admin_token, or None."""
    admin_token = os.getenv("KOI_ADMIN_TOKEN")
    if admin_token:
        return admin_token

    state_dir = os.getenv("KOI_STATE_DIR", "")
    token_path = os.path.join(state_dir, "admin_token") if state_dir else ""
    if token_path and os.path.exists(token_path):
        try:
            with open(token_path) as f:
                return f.read().strip() or None
        except (OSError, UnicodeError):
            return None
    return None


def is_loopback_client(request: Request) -> bool:
    """Whether the TCP peer is a loopback address. A client host that is not an IP is not."""
    try:
        ip = ipaddress.ip_address(request.client.host if request.client else "")
    except ValueError:
        return False
    if ip.version == 6 and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return ip.is_loopback


def bearer_matches(request: Request, token: Optional[str]) -> bool:
    auth_header = request.headers.get("Authorization", "")
    return bool(token) and auth_header.startswith("Bearer ") and hmac.compare_digest(
        auth_header[7:].encode(), token.encode()
    )


def local_admin_refusal(request: Request, forbidden_message: str = "Endpoint is localhost-only"
                        ) -> Optional[Tuple[int, str, str]]:
    """None for a loopback caller presenting the configured admin bearer, else
    (status, error_code, message)."""
    if not is_loopback_client(request):
        return 403, "FORBIDDEN", forbidden_message
    admin_token = read_admin_token()
    if not admin_token:
        return 503, "ADMIN_TOKEN_UNCONFIGURED", "Admin token is not configured"
    if not bearer_matches(request, admin_token):
        return 401, "UNAUTHORIZED", "Invalid or missing admin token"
    return None


def enforce_local_admin(request: Request) -> Optional[JSONResponse]:
    """Return an error response unless the caller is the local operator, else None."""
    refusal = local_admin_refusal(request)
    if refusal is None:
        return None
    status_code, code, message = refusal
    return JSONResponse(status_code=status_code, content={"error": message, "error_code": code})
