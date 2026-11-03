"""
RFC 8628 Device Authorization Grant Implementation

Security improvements over previous implementation:
1. user_code: Short code user manually types (prevents phishing)
2. state_id: Opaque ID for OAuth state (device_code never sent to Google)
3. POST endpoints: Secrets in body, not URL (prevents logging exposure)
4. Rate limiting: Prevents brute-force attacks on user codes
5. JWT validation: Verifies Google ID tokens with proper claim checks

LOGGING RULES (SECURITY CRITICAL):
- NEVER log: device_code, session_token, session_token_hash, access_token, id_token
- OK to log: user_code (public), user_email (for audit), IP addresses, timestamps
- Log request failures without including request/response bodies
"""

import os
import json
import time
import secrets
import hashlib
import httpx
import urllib.parse
import string
from typing import Optional, Dict, Any
from fastapi import APIRouter, Cookie, HTTPException, Request, Depends, Header, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel
import asyncpg
from loguru import logger

# Configuration
CLIENT_SECRET_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "config", "client_secret.json")
REDIRECT_URI = "https://regen.gaiaai.xyz/api/koi/auth/callback"
VERIFICATION_URI = "https://regen.gaiaai.xyz/activate"
SCOPES = [
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid"
]

# Lifetimes
SESSION_TOKEN_LIFETIME_SECONDS = 3600  # 1 hour
DEVICE_CODE_LIFETIME_SECONDS = 600     # 10 minutes
POLL_INTERVAL_SECONDS = 5              # Recommended poll interval

# Rate Limiting Configuration
ACTIVATE_RATE_LIMIT = 5          # Max attempts per IP per minute for /activate
ACTIVATE_LOCKOUT_THRESHOLD = 5   # Lock out code after this many failed attempts
TOKEN_RATE_LIMIT = 60            # Max requests per IP per minute for /auth/token
SLOW_DOWN_INTERVAL = 10          # Increased interval when client polls too fast

# =============================================================================
# Rate Limiter (In-Memory)
# =============================================================================
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock

@dataclass
class RateLimitEntry:
    """Track rate limit state for an IP or code."""
    count: int = 0
    window_start: float = 0.0
    slow_down_until: float = 0.0

class RateLimiter:
    """
    Simple in-memory rate limiter with sliding window.
    Thread-safe for async use.
    """
    def __init__(self, max_requests: int, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._entries: Dict[str, RateLimitEntry] = defaultdict(RateLimitEntry)
        self._lock = Lock()

    def is_allowed(self, key: str) -> tuple[bool, int]:
        """
        Check if request is allowed for given key (IP, code, etc).
        Returns (allowed, retry_after_seconds).
        """
        now = time.time()
        with self._lock:
            entry = self._entries[key]

            # Check slow_down penalty
            if entry.slow_down_until > now:
                return False, int(entry.slow_down_until - now)

            # Reset window if expired
            if now - entry.window_start > self.window_seconds:
                entry.count = 0
                entry.window_start = now

            # Check limit
            if entry.count >= self.max_requests:
                return False, int(self.window_seconds - (now - entry.window_start))

            # Allow and increment
            entry.count += 1
            return True, 0

    def apply_slow_down(self, key: str, seconds: int = 5):
        """Apply slow_down penalty to a key."""
        with self._lock:
            self._entries[key].slow_down_until = time.time() + seconds

    def get_fail_count(self, key: str) -> int:
        """Get current failure count for a key."""
        with self._lock:
            return self._entries.get(key, RateLimitEntry()).count

    def cleanup_old_entries(self, max_age: int = 3600):
        """Remove entries older than max_age seconds."""
        now = time.time()
        with self._lock:
            keys_to_remove = [
                k for k, v in self._entries.items()
                if now - v.window_start > max_age
            ]
            for k in keys_to_remove:
                del self._entries[k]

# Global rate limiters
activate_limiter = RateLimiter(max_requests=ACTIVATE_RATE_LIMIT, window_seconds=60)
token_limiter = RateLimiter(max_requests=TOKEN_RATE_LIMIT, window_seconds=60)
code_fail_limiter = RateLimiter(max_requests=ACTIVATE_LOCKOUT_THRESHOLD, window_seconds=600)

# =============================================================================
# Google JWT Validation
# =============================================================================
import base64
import jwt  # PyJWT library

# Google's public keys for JWT verification (cached)
_google_public_keys: Dict[str, Any] = {}
_google_keys_fetched_at: float = 0
GOOGLE_KEYS_CACHE_SECONDS = 3600  # Refresh keys every hour

async def get_google_public_keys() -> Dict[str, Any]:
    """
    Fetch Google's public keys for JWT verification.
    Keys are cached for 1 hour.
    """
    global _google_public_keys, _google_keys_fetched_at

    if time.time() - _google_keys_fetched_at < GOOGLE_KEYS_CACHE_SECONDS and _google_public_keys:
        return _google_public_keys

    async with httpx.AsyncClient() as client:
        resp = await client.get("https://www.googleapis.com/oauth2/v3/certs")
        if resp.status_code != 200:
            raise ValueError("Failed to fetch Google public keys")

        keys_data = resp.json()
        _google_public_keys = {key["kid"]: key for key in keys_data.get("keys", [])}
        _google_keys_fetched_at = time.time()

    return _google_public_keys


async def validate_google_id_token(id_token: str, expected_client_id: str) -> Dict[str, Any]:
    """
    Validate a Google ID token (JWT) and return the claims.

    Validates:
    - Signature against Google's public keys
    - Issuer (iss) is Google
    - Audience (aud) matches our client ID
    - Token is not expired (exp)
    - Email is verified (email_verified)

    Returns the decoded claims on success.
    Raises ValueError on validation failure.
    """
    try:
        # Decode header to get key ID (kid)
        header = jwt.get_unverified_header(id_token)
        kid = header.get("kid")

        if not kid:
            raise ValueError("No key ID (kid) in token header")

        # Get Google's public keys
        public_keys = await get_google_public_keys()

        if kid not in public_keys:
            # Refresh keys and try again (key rotation)
            global _google_keys_fetched_at
            _google_keys_fetched_at = 0
            public_keys = await get_google_public_keys()

            if kid not in public_keys:
                raise ValueError(f"Unknown key ID: {kid}")

        # Convert JWK to public key
        key_data = public_keys[kid]
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))

        # Decode and validate
        claims = jwt.decode(
            id_token,
            public_key,
            algorithms=["RS256"],
            audience=expected_client_id,
            issuer=["https://accounts.google.com", "accounts.google.com"],
            options={
                "verify_exp": True,
                "verify_aud": True,
                "verify_iss": True,
            }
        )

        # Additional checks
        if not claims.get("email_verified", False):
            raise ValueError("Email is not verified")

        if not claims.get("email"):
            raise ValueError("No email in token")

        return claims

    except jwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except jwt.InvalidAudienceError:
        raise ValueError("Invalid audience - token not intended for this application")
    except jwt.InvalidIssuerError:
        raise ValueError("Invalid issuer - token not from Google")
    except jwt.InvalidSignatureError:
        raise ValueError("Invalid signature - token may be tampered")
    except jwt.DecodeError as e:
        raise ValueError(f"Failed to decode token: {e}")

def hash_token(token: str) -> str:
    """SHA-256 hash a token for secure storage."""
    return hashlib.sha256(token.encode()).hexdigest()

def generate_session_token() -> str:
    """Generate a cryptographically secure session token."""
    return secrets.token_urlsafe(32)

def generate_device_code() -> str:
    """Generate a cryptographically secure device code (64 hex chars)."""
    return secrets.token_hex(32)

def generate_state_id() -> str:
    """Generate an opaque state ID for OAuth (never expose device_code to Google)."""
    return secrets.token_urlsafe(32)

def generate_user_code() -> str:
    """
    Generate a human-friendly user code (e.g., "WDJB-QK4Z").
    - 8 characters split by hyphen
    - Uses unambiguous consonants + digits only
    - No vowels (prevents accidental words), no confusable chars (0/O, 1/I/L, S/5)
    - Entropy: ~34.5 bits (20^8 / 2^34.5), sufficient with rate limiting
    """
    # Base-20 alphabet: consonants only, no confusables
    # Excludes: A,E,I,O,U (vowels), S (looks like 5), L (looks like 1), O (looks like 0)
    alphabet = "BCDFGHJKMNPQRTVWXYZ2346789"
    code = ''.join(secrets.choice(alphabet) for _ in range(8))
    return f"{code[:4]}-{code[4:]}"

router = APIRouter(tags=["auth"])

# Pydantic Models
class DeviceCodeRequest(BaseModel):
    """Request to start device authorization flow."""
    client_id: Optional[str] = None  # Optional client identifier

class DeviceCodeResponse(BaseModel):
    """RFC 8628 Device Authorization Response."""
    device_code: str          # Secret: MCP keeps this, uses to poll
    user_code: str            # Public: User types this at verification_uri
    verification_uri: str     # URL where user enters the code
    expires_in: int           # Seconds until codes expire
    interval: int             # Recommended polling interval

class TokenRequest(BaseModel):
    """Request to exchange device_code for session token."""
    device_code: str
    grant_type: str = "urn:ietf:params:oauth:grant-type:device_code"

class TokenResponse(BaseModel):
    """Successful token response."""
    access_token: str
    token_type: str = "Bearer"
    expires_in: int
    email: Optional[str] = None  # User email for client-side display

class TokenErrorResponse(BaseModel):
    """RFC 8628 error response."""
    error: str
    error_description: Optional[str] = None

# DB Dependency
async def get_db():
    dsn = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5433/eliza")
    conn = await asyncpg.connect(dsn)
    try:
        yield conn
    finally:
        await conn.close()

def load_client_secrets():
    if not os.path.exists(CLIENT_SECRET_FILE):
        raise FileNotFoundError(f"Client secret file not found at {CLIENT_SECRET_FILE}")

    with open(CLIENT_SECRET_FILE, 'r') as f:
        data = json.load(f)
        creds = data.get('web') or data.get('installed')
        if not creds:
            raise ValueError("Invalid client_secret.json: must contain 'web' or 'installed'")
        return creds


# =============================================================================
# RFC 8628 Device Authorization Endpoints
# =============================================================================

@router.post("/auth/device/code", response_model=DeviceCodeResponse)
async def request_device_code(
    request: DeviceCodeRequest = None,
    db=Depends(get_db)
):
    """
    RFC 8628 Device Authorization Request.

    Returns a device_code (secret) and user_code (public).
    MCP client displays: "Go to {verification_uri} and enter code: {user_code}"

    SECURITY: This prevents phishing because user must manually type a code
    they see on THEIR device. Attacker cannot force victim to type unknown code.
    """
    device_code = generate_device_code()
    user_code = generate_user_code()
    state_id = generate_state_id()
    expires_at = time.time() + DEVICE_CODE_LIFETIME_SECONDS

    # Store the auth request
    await db.execute("""
        INSERT INTO auth_requests (device_code, user_code, state_id, status, expires_at)
        VALUES ($1, $2, $3, 'pending', to_timestamp($4))
    """, device_code, user_code, state_id, expires_at)

    logger.info(f"[Auth] Device code requested, user_code={user_code}")

    return DeviceCodeResponse(
        device_code=device_code,
        user_code=user_code,
        verification_uri=VERIFICATION_URI,
        expires_in=DEVICE_CODE_LIFETIME_SECONDS,
        interval=POLL_INTERVAL_SECONDS
    )


@router.post("/auth/token")
async def exchange_device_code(
    http_request: Request,
    request: TokenRequest,
    db=Depends(get_db)
):
    """
    RFC 8628 Device Access Token Request.

    MCP client polls this endpoint with device_code until user completes auth.

    SECURITY:
    - POST request keeps device_code out of URL/logs
    - Returns session token ONCE, then marks as 'used'
    - Rate limited to prevent abuse
    """
    # Get client IP for rate limiting
    client_ip = http_request.client.host if http_request.client else "unknown"
    forwarded = http_request.headers.get("x-forwarded-for")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()

    # Check rate limit (60 requests per minute per IP)
    allowed, retry_after = token_limiter.is_allowed(client_ip)
    if not allowed:
        # Return slow_down error per RFC 8628
        token_limiter.apply_slow_down(client_ip, SLOW_DOWN_INTERVAL)
        return TokenErrorResponse(
            error="slow_down",
            error_description=f"Too many requests. Wait {retry_after} seconds."
        )

    device_code = request.device_code

    if not device_code or len(device_code) < 32:
        return TokenErrorResponse(
            error="invalid_request",
            error_description="device_code is required"
        )

    row = await db.fetchrow("""
        SELECT id, user_email, status, session_token, expires_at
        FROM auth_requests
        WHERE device_code = $1
    """, device_code)

    if not row:
        return TokenErrorResponse(
            error="invalid_grant",
            error_description="Invalid or expired device code"
        )

    if row["expires_at"].timestamp() < time.time():
        return TokenErrorResponse(
            error="expired_token",
            error_description="Device code has expired"
        )

    if row["status"] == "pending":
        # User hasn't completed auth yet - keep polling
        return TokenErrorResponse(
            error="authorization_pending",
            error_description="User has not yet completed authorization"
        )

    if row["status"] == "used":
        # Token was already retrieved
        return TokenErrorResponse(
            error="invalid_grant",
            error_description="Token has already been retrieved"
        )

    if row["status"] == "rejected":
        return TokenErrorResponse(
            error="access_denied",
            error_description="Email domain not allowed. Only @regen.network emails are permitted."
        )

    if row["status"] == "authenticated":
        # Success! Return the session token
        plain_token = row["session_token"]

        if not plain_token:
            return TokenErrorResponse(
                error="server_error",
                error_description="Session token not found"
            )

        # Mark as used and NULL out the plain token
        await db.execute("""
            UPDATE auth_requests
            SET status = 'used', used_at = CURRENT_TIMESTAMP, session_token = NULL
            WHERE device_code = $1
        """, device_code)

        logger.info(f"[Auth] Session token retrieved for {row['user_email']}")

        return TokenResponse(
            access_token=plain_token,
            token_type="Bearer",
            expires_in=SESSION_TOKEN_LIFETIME_SECONDS,
            email=row['user_email']
        )

    # Unknown status
    return TokenErrorResponse(
        error="server_error",
        error_description=f"Unknown status: {row['status']}"
    )


# =============================================================================
# User-Facing Activation Pages
# =============================================================================

ACTIVATE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Activate Device - Regen Network</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a472a 0%, #2d5a3d 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }}
        .container {{
            background: white;
            border-radius: 16px;
            padding: 40px;
            max-width: 400px;
            width: 100%;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        }}
        .logo {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .logo h1 {{
            color: #1a472a;
            font-size: 24px;
        }}
        .logo p {{
            color: #666;
            margin-top: 8px;
        }}
        h2 {{
            color: #333;
            margin-bottom: 20px;
            text-align: center;
        }}
        .instructions {{
            background: #f5f5f5;
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 24px;
            color: #555;
            font-size: 14px;
            line-height: 1.5;
        }}
        .form-group {{
            margin-bottom: 20px;
        }}
        label {{
            display: block;
            margin-bottom: 8px;
            color: #333;
            font-weight: 500;
        }}
        input[type="text"] {{
            width: 100%;
            padding: 16px;
            font-size: 24px;
            text-align: center;
            letter-spacing: 4px;
            text-transform: uppercase;
            border: 2px solid #ddd;
            border-radius: 8px;
            outline: none;
            transition: border-color 0.2s;
        }}
        input[type="text"]:focus {{
            border-color: #1a472a;
        }}
        input[type="text"]::placeholder {{
            letter-spacing: 2px;
            font-size: 18px;
        }}
        button {{
            width: 100%;
            padding: 16px;
            background: #1a472a;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }}
        button:hover {{
            background: #2d5a3d;
        }}
        .error {{
            background: #fee;
            border: 1px solid #fcc;
            color: #c00;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 20px;
            text-align: center;
        }}
        .footer {{
            text-align: center;
            margin-top: 24px;
            color: #999;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="logo">
            <h1>Regen Network</h1>
            <p>Knowledge Organization Infrastructure</p>
        </div>

        <h2>Activate Your Device</h2>

        <div class="instructions">
            Enter the code displayed in your Claude Code / MCP client to authorize access to private Regen documentation.
        </div>

        {error_html}

        <form method="POST" action="/api/koi/activate">
            <div class="form-group">
                <label for="code">Device Code</label>
                <input type="text" id="code" name="code" placeholder="XXXX-XXXX"
                       maxlength="9" autocomplete="off" autofocus required
                       pattern="[A-Za-z0-9]{{{{4}}}}-?[A-Za-z0-9]{{{{4}}}}">
            </div>
            <button type="submit">Continue with Google</button>
        </form>

        <div class="footer">
            Only @regen.network email addresses are permitted.
        </div>
    </div>

    <script>
        // Auto-format code input with hyphen
        document.getElementById('code').addEventListener('input', function(e) {{
            let value = e.target.value.toUpperCase().replace(/[^A-Z0-9]/g, '');
            if (value.length > 4) {{
                value = value.slice(0, 4) + '-' + value.slice(4, 8);
            }}
            e.target.value = value;
        }});
    </script>
</body>
</html>
"""

SUCCESS_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Success - Regen Network</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #1a472a 0%, #2d5a3d 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }}
        .container {{
            background: white;
            border-radius: 16px;
            padding: 40px;
            max-width: 400px;
            width: 100%;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            text-align: center;
        }}
        .checkmark {{
            width: 80px;
            height: 80px;
            background: #1a472a;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 24px;
        }}
        .checkmark svg {{
            width: 40px;
            height: 40px;
            stroke: white;
            stroke-width: 3;
        }}
        h1 {{
            color: #1a472a;
            margin-bottom: 16px;
        }}
        p {{
            color: #666;
            line-height: 1.6;
        }}
        .email {{
            background: #f5f5f5;
            padding: 12px 20px;
            border-radius: 8px;
            margin: 20px 0;
            font-weight: 500;
            color: #333;
        }}
        .close-hint {{
            margin-top: 24px;
            color: #999;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="checkmark">
            <svg viewBox="0 0 24 24" fill="none">
                <path d="M5 13l4 4L19 7" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
        </div>
        <h1>Authentication Successful!</h1>
        <p>You have authorized access to private Regen Network documentation.</p>
        <div class="email">{email}</div>
        <p class="close-hint">You can close this window and return to Claude Code.</p>
    </div>
</body>
</html>
"""


@router.get("/activate", response_class=HTMLResponse)
async def activate_page(error: Optional[str] = None):
    """
    Serve the HTML page where users enter their device code.
    """
    error_html = ""
    if error:
        error_messages = {
            "invalid": "Invalid or expired code. Please check and try again.",
            "expired": "This code has expired. Please request a new one in Claude Code.",
            "used": "This code has already been used.",
            "locked": "Too many failed attempts. This code has been locked for security.",
        }
        error_html = f'<div class="error">{error_messages.get(error, error)}</div>'

    return ACTIVATE_HTML.format(error_html=error_html)


@router.post("/activate", response_class=HTMLResponse)
async def activate_submit(
    request: Request,
    code: str = Form(...),
    db=Depends(get_db)
):
    """
    Validate the user code and redirect to Google OAuth.

    SECURITY: User manually types code from their device, preventing phishing.
    Rate limited to prevent brute-force guessing of user codes.
    """
    # Get client IP for rate limiting
    client_ip = request.client.host if request.client else "unknown"
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        client_ip = forwarded.split(",")[0].strip()

    # Check IP rate limit (5 attempts per minute)
    allowed, retry_after = activate_limiter.is_allowed(client_ip)
    if not allowed:
        logger.warning(f"[Auth] Rate limit exceeded for IP {client_ip}")
        return HTMLResponse(
            content=f"<h1>Too Many Requests</h1><p>Please wait {retry_after} seconds before trying again.</p>",
            status_code=429
        )

    # Normalize code (remove hyphen, uppercase)
    user_code = code.upper().replace("-", "")
    if len(user_code) == 8:
        user_code = f"{user_code[:4]}-{user_code[4:]}"

    # Check if this code has been failed too many times (lockout)
    code_key = f"code:{user_code}"
    allowed, _ = code_fail_limiter.is_allowed(code_key)
    if not allowed:
        logger.warning(f"[Auth] Code {user_code} locked out due to too many failed attempts")
        return RedirectResponse(url="/activate?error=locked", status_code=303)

    # Look up the auth request by user_code
    row = await db.fetchrow("""
        SELECT id, device_code, state_id, status, expires_at
        FROM auth_requests
        WHERE user_code = $1
    """, user_code)

    if not row:
        # Track failed attempt for this code
        code_fail_limiter.is_allowed(code_key)  # Increment failure count
        return RedirectResponse(url="/activate?error=invalid", status_code=303)

    if row["expires_at"].timestamp() < time.time():
        return RedirectResponse(url="/activate?error=expired", status_code=303)

    if row["status"] != "pending":
        return RedirectResponse(url="/activate?error=used", status_code=303)

    # Update status to show user started OAuth
    await db.execute("""
        UPDATE auth_requests SET status = 'authorizing' WHERE id = $1
    """, row["id"])

    # Redirect to Google OAuth with opaque state_id (NOT device_code!)
    creds = load_client_secrets()
    params = {
        "client_id": creds["client_id"],
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": row["state_id"],  # Opaque ID - device_code never sent to Google!
    }
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"

    logger.info(f"[Auth] User submitted code {user_code}, redirecting to Google")
    return RedirectResponse(url=auth_url, status_code=303)


@router.get("/auth/callback", response_class=HTMLResponse)
async def auth_callback(code: str, state: str, db=Depends(get_db)):
    """
    Handles the OAuth callback from Google.

    SECURITY:
    - state contains opaque state_id (NOT device_code)
    - Looks up auth_request by state_id
    - Google tokens used only for identity verification, not stored
    """
    # Look up auth request by state_id
    auth_request = await db.fetchrow("""
        SELECT id, device_code, user_code, user_email, status, expires_at, callback_url
        FROM auth_requests
        WHERE state_id = $1
    """, state)

    if not auth_request:
        return HTMLResponse(content="<h1>Error</h1><p>Invalid or expired authorization request.</p>", status_code=400)

    if auth_request['status'] not in ('pending', 'authorizing'):
        return HTMLResponse(content="<h1>Error</h1><p>This authorization has already been completed.</p>", status_code=400)

    if auth_request['expires_at'].timestamp() < time.time():
        return HTMLResponse(content="<h1>Error</h1><p>Authorization request expired. Please try again.</p>", status_code=400)

    try:
        creds = load_client_secrets()
    except Exception as e:
        return HTMLResponse(content=f"<h1>Error</h1><p>Server configuration error.</p>", status_code=500)

    # Exchange code for tokens (to verify identity)
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "code": code,
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(token_url, data=data)
        if resp.status_code != 200:
            # Log only that it failed, not the response body (may contain secrets)
            logger.error("[Auth] Token exchange failed with Google")
            return HTMLResponse(content="<h1>Error</h1><p>Failed to verify with Google.</p>", status_code=400)

        tokens = resp.json()

    # Validate ID token (JWT) - this is more secure than using userinfo endpoint
    # Validates: signature, issuer, audience, expiry, email_verified
    id_token = tokens.get("id_token")
    if not id_token:
        logger.error("[Auth] No id_token in Google response")
        return HTMLResponse(content="<h1>Error</h1><p>Invalid response from Google.</p>", status_code=400)

    try:
        claims = await validate_google_id_token(id_token, creds["client_id"])
    except ValueError as e:
        logger.warning(f"[Auth] JWT validation failed: {e}")
        await db.execute("UPDATE auth_requests SET status = 'rejected' WHERE id = $1", auth_request['id'])
        return HTMLResponse(
            content=f"<h1>Verification Failed</h1><p>{str(e)}</p>",
            status_code=400
        )

    verified_email = claims.get("email", "").lower()

    # Verify it's a @regen.network email (strict domain check)
    if not verified_email.endswith("@regen.network"):
        logger.warning(f"[Auth] Rejected non-regen email (domain check failed)")
        await db.execute("UPDATE auth_requests SET status = 'rejected' WHERE id = $1", auth_request['id'])
        return HTMLResponse(
            content="<h1>Access Denied</h1><p>Only @regen.network email addresses are permitted.</p>",
            status_code=403
        )

    # Generate session token and store HASH only in long-lived table
    session_token = generate_session_token()
    session_token_hash = hash_token(session_token)
    session_expiry = time.time() + SESSION_TOKEN_LIFETIME_SECONDS

    # Update auth_request with PLAIN token (temporary) and hash
    await db.execute("""
        UPDATE auth_requests
        SET status = 'authenticated',
            user_email = $2,
            session_token = $3,
            session_token_hash = $4,
            authenticated_at = CURRENT_TIMESTAMP
        WHERE id = $1
    """, auth_request['id'], verified_email, session_token, session_token_hash)

    # Store ONLY hash in session_tokens (long-lived table)
    await db.execute("""
        INSERT INTO session_tokens (token_hash, user_email, expires_at)
        VALUES ($1, $2, to_timestamp($3))
    """, session_token_hash, verified_email, session_expiry)

    logger.info(f"[Auth] Authenticated {verified_email} via user_code={auth_request['user_code']!r} flow={'web' if auth_request['callback_url'] else 'device'}")

    # Branch on flow type:
    #   - Web flow (callback_url set) → set HttpOnly cookie, 303 back to portal
    #   - Device flow (callback_url NULL) → return SUCCESS_HTML, MCP polls /auth/token
    if auth_request["callback_url"]:
        # Mark the auth_request as 'used' so the device-code path can't also consume it.
        await db.execute("""
            UPDATE auth_requests
            SET status = 'used',
                used_at = CURRENT_TIMESTAMP,
                session_token = NULL
            WHERE id = $1
        """, auth_request["id"])
        response = RedirectResponse(url=auth_request["callback_url"], status_code=303)
        _set_session_cookie(response, session_token, session_expiry)
        return response

    return SUCCESS_HTML.format(email=verified_email)


# =============================================================================
# Legacy endpoints (kept for backwards compatibility, will be deprecated)
# =============================================================================

@router.get("/auth/initiate")
async def initiate_auth_legacy(
    user_email: str,
    device_code: str,
    db=Depends(get_db)
):
    """
    DEPRECATED: Use POST /auth/device/code instead.

    Kept for backwards compatibility during migration.
    """
    logger.warning("[Auth] Legacy /auth/initiate endpoint used - please migrate to POST /auth/device/code")

    # Generate new-style codes
    user_code = generate_user_code()
    state_id = generate_state_id()
    expires_at = time.time() + DEVICE_CODE_LIFETIME_SECONDS

    await db.execute("""
        INSERT INTO auth_requests (device_code, user_code, state_id, user_email, status, expires_at)
        VALUES ($1, $2, $3, $4, 'pending', to_timestamp($5))
        ON CONFLICT (device_code) DO UPDATE SET
            user_code = EXCLUDED.user_code,
            state_id = EXCLUDED.state_id,
            user_email = EXCLUDED.user_email,
            status = 'pending',
            expires_at = EXCLUDED.expires_at
    """, device_code, user_code, state_id, user_email, expires_at)

    creds = load_client_secrets()
    # Still use state_id, not device_code
    params = {
        "client_id": creds["client_id"],
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state_id,
        "login_hint": user_email
    }
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"

    return {"auth_url": auth_url, "state": state_id}


# =============================================================================
# Authorization Dependency (for protected endpoints)
# =============================================================================

async def get_authorized_user(
    authorization: Optional[str] = Header(None),
    db=Depends(get_db)
) -> str:
    """
    FastAPI dependency that validates session token and returns user email.

    Usage:
        @app.get("/protected")
        async def protected_endpoint(user_email: str = Depends(get_authorized_user)):
            return {"user": user_email}
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")

    # Extract token from "Bearer <token>" format
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid authorization header format")

    token = parts[1]
    token_hash = hash_token(token)

    # Look up token hash in session_tokens table
    row = await db.fetchrow("""
        SELECT user_email, expires_at, revoked_at
        FROM session_tokens
        WHERE token_hash = $1
    """, token_hash)

    if not row:
        raise HTTPException(status_code=401, detail="Invalid session token")

    if row["revoked_at"]:
        raise HTTPException(status_code=401, detail="Session token has been revoked")

    if row["expires_at"].timestamp() < time.time():
        raise HTTPException(status_code=401, detail="Session token has expired")

    return row["user_email"]


@router.get("/auth/status")
async def check_auth_status_legacy(device_code: str, db=Depends(get_db)):
    """
    DEPRECATED: Use POST /auth/token instead.

    Kept for backwards compatibility during migration.
    """
    logger.warning("[Auth] Legacy GET /auth/status endpoint used - please migrate to POST /auth/token")

    # Delegate to the new endpoint logic
    request = TokenRequest(device_code=device_code)
    result = await exchange_device_code(request, db)

    # Convert to old response format
    if isinstance(result, TokenResponse):
        return {
            "status": "authenticated",
            "authenticated": True,
            "session_token": result.access_token
        }
    elif isinstance(result, TokenErrorResponse):
        if result.error == "authorization_pending":
            return {"status": "pending", "authenticated": False}
        elif result.error == "access_denied":
            return {"status": "rejected", "authenticated": False, "message": result.error_description}
        else:
            return {"status": result.error, "authenticated": False, "message": result.error_description}

    return result


# =============================================================================
# Browser (web) OAuth flow
# =============================================================================
# Complements the RFC 8628 device-code flow (for MCPs / CLIs) with a direct
# browser redirect flow (for the /claims portal and future web apps). Both
# flows land in the same `/auth/callback` and produce the same session_token
# in `session_tokens`. The only difference is delivery:
#   - device flow: MCP polls /auth/token, receives the plain token, stores it
#   - web flow: backend sets an HttpOnly cookie and 303s the browser back
#
# The `auth_requests.callback_url` column distinguishes the two (NULL = device,
# non-NULL = web). See migration 088.

SESSION_COOKIE_NAME = "koi_session"
SESSION_COOKIE_MAX_AGE = SESSION_TOKEN_LIFETIME_SECONDS  # align with token expiry

# Only accept callback paths that clearly belong to our portal surface. Prevents
# the `/auth/web/login?next=...` endpoint from becoming an open redirect.
_ALLOWED_CALLBACK_PREFIXES = ("/claims", "/claims/", "/registry", "/digests", "/")


def _is_safe_callback(path: str) -> bool:
    """Only allow relative paths pointing at our own routes."""
    if not path or not path.startswith("/"):
        return False
    # Reject protocol-relative (//evil.com) and multi-slash tricks.
    if path.startswith("//"):
        return False
    # Reject any scheme/host in the path (belt-and-braces on top of startswith "/").
    if "://" in path:
        return False
    return any(path == prefix or path.startswith(prefix.rstrip("/") + "/")
               or path.startswith(prefix) for prefix in _ALLOWED_CALLBACK_PREFIXES)


def _set_session_cookie(response, token: str, expiry_ts: float) -> None:
    """Attach the session cookie to a response.

    HttpOnly (no JS access), Secure (HTTPS only), SameSite=Lax (sent on
    top-level navigations; blocks the dangerous cross-site POST cases but
    works across tabs on our own site).
    """
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_COOKIE_MAX_AGE,
        expires=int(expiry_ts),
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )


@router.get("/auth/web/login")
async def web_login(next: str = "/claims", db=Depends(get_db)):
    """Start the browser-facing OAuth flow.

    Generates a one-time auth_request tied to a callback URL, then redirects
    the browser to Google OAuth. After the user consents, `/auth/callback`
    sets the session cookie and 303s back to ``next``.

    Open-redirect protection: ``next`` must be a relative path to one of
    our own routes (see ``_is_safe_callback``).
    """
    if not _is_safe_callback(next):
        return HTMLResponse(
            content=(
                "<h1>Invalid callback URL</h1>"
                "<p>The `next` parameter must be a relative path to a Regen route.</p>"
            ),
            status_code=400,
        )

    # Web flow has no device; we synthesize a placeholder device_code so the
    # NOT-NULL constraint on auth_requests.device_code is satisfied. Prefix
    # distinguishes it from real device codes.
    synthetic_device_code = f"web:{secrets.token_hex(24)}"
    state_id = generate_state_id()
    expires_at = time.time() + DEVICE_CODE_LIFETIME_SECONDS

    await db.execute(
        """
        INSERT INTO auth_requests (device_code, state_id, status, expires_at, callback_url)
        VALUES ($1, $2, 'authorizing', to_timestamp($3), $4)
        """,
        synthetic_device_code, state_id, expires_at, next,
    )

    try:
        creds = load_client_secrets()
    except Exception:
        return HTMLResponse(content="<h1>Error</h1><p>Server configuration error.</p>", status_code=500)

    params = {
        "client_id": creds["client_id"],
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state_id,
        "hd": "regen.network",  # Hint to Google to show the @regen.network chooser
    }
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urllib.parse.urlencode(params)}"

    logger.info(f"[Auth] Web login initiated, redirecting to Google (next={next})")
    return RedirectResponse(url=auth_url, status_code=303)


@router.get("/auth/me")
async def auth_me(
    authorization: Optional[str] = Header(None),
    koi_session: Optional[str] = Cookie(None),
    db=Depends(get_db),
):
    """Return the currently authenticated user, or 401 if none.

    Used by the portal to decide whether to show the dashboard or the
    sign-in prompt. Accepts either a Bearer header or the session cookie.
    """
    # Header beats cookie (matches auth_deps priority).
    token = None
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]
    if token is None and koi_session:
        token = koi_session

    if token is None:
        raise HTTPException(status_code=401, detail="Not authenticated")

    token_hash = hash_token(token)
    row = await db.fetchrow(
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

    return {
        "email": row["user_email"],
        "expires_at": row["expires_at"].isoformat(),
    }


@router.post("/auth/web/logout")
async def web_logout(
    koi_session: Optional[str] = Cookie(None),
    db=Depends(get_db),
):
    """Clear the session cookie and revoke the underlying token.

    Uses POST to avoid CSRF-triggered logout via <img> etc. The cookie is
    SameSite=Lax which already blocks cross-site POSTs in modern browsers.
    """
    if koi_session:
        token_hash = hash_token(koi_session)
        await db.execute(
            """
            UPDATE session_tokens
            SET revoked_at = CURRENT_TIMESTAMP
            WHERE token_hash = $1 AND revoked_at IS NULL
            """,
            token_hash,
        )

    response = JSONResponse(content={"status": "logged_out"})
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
    return response
