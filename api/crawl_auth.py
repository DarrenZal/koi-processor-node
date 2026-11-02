"""
Auth + identity validation for the agentic crawl endpoints.

Two models:
- Bound-token namespaces (`ops`, `web`, `mcp`): each token is 1:1 mapped to a
  specific identity at config time via env vars of the form
  ``CRAWL_TOKEN__{namespace}__{identifier}=<token>``. The server derives
  ``submitted_by`` from the token; the client never asserts it.
- Telegram HMAC namespace: one ``CRAWL_TOKEN_TELEGRAM`` + one
  ``CRAWL_SECRET_TELEGRAM``. The plugin signs ``<identity>|<ts>`` with the
  shared secret and sends it in ``X-Identity-Claim``. Server verifies HMAC,
  rejects if timestamp is > 60 s old.

Phase 1 ships with ``AGENTIC_CRAWL_ENABLED=false`` so all endpoints respond
503 regardless of token config.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_-]{1,15}:[a-z0-9][a-z0-9_-]{1,47}$")

NAMESPACE_VALIDATORS: dict[str, re.Pattern[str]] = {
    "telegram": re.compile(r"^tg\d{5,15}$"),
    "web": re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$"),
    "mcp": re.compile(r"^[a-z0-9_-]{4,48}$"),
    "ops": re.compile(r".+"),
}

HMAC_MAX_AGE_SECONDS = 60


class CrawlAuthError(Exception):
    """Base class for auth failures."""

    status_code: int = 401
    message: str = "authentication failed"

    def __init__(self, message: Optional[str] = None, status_code: Optional[int] = None):
        super().__init__(message or self.message)
        if message:
            self.message = message
        if status_code is not None:
            self.status_code = status_code


class NotConfiguredError(CrawlAuthError):
    status_code = 503
    message = "identity configuration missing"


class FeatureDisabledError(CrawlAuthError):
    status_code = 503
    message = "agentic crawl disabled"


@dataclass
class AuthResult:
    submitted_by: str
    namespace: str


@dataclass(frozen=True)
class IdentityConfig:
    bound_tokens: dict[str, str]
    telegram_token: Optional[str]
    telegram_secret: Optional[str]

    @property
    def any_configured(self) -> bool:
        if self.telegram_token and self.telegram_secret:
            return True
        return bool(self.bound_tokens)


_IDENTITY_CONFIG = IdentityConfig(
    bound_tokens={},
    telegram_token=None,
    telegram_secret=None,
)


def is_feature_enabled() -> bool:
    return os.environ.get("AGENTIC_CRAWL_ENABLED", "").lower() == "true"


def _load_bound_tokens_from_env() -> dict[str, str]:
    """Return ``{token_value -> submitted_by}`` for bound-token namespaces.

    Reads env vars of the form ``CRAWL_TOKEN__<namespace>__<identifier>``.
    """
    mapping: dict[str, str] = {}
    for key, value in os.environ.items():
        if not key.startswith("CRAWL_TOKEN__") or not value:
            continue
        parts = key[len("CRAWL_TOKEN__"):].split("__", 1)
        if len(parts) != 2:
            continue
        namespace, identifier = parts
        namespace = namespace.lower()
        if namespace == "telegram":
            continue  # telegram uses shared-token + HMAC, not bound-token
        sb = canonicalize_submitted_by(f"{namespace}:{identifier}")
        mapping[value] = sb
    return mapping


def _load_identity_config_from_env() -> IdentityConfig:
    return IdentityConfig(
        bound_tokens=_load_bound_tokens_from_env(),
        telegram_token=os.environ.get("CRAWL_TOKEN_TELEGRAM") or None,
        telegram_secret=os.environ.get("CRAWL_SECRET_TELEGRAM") or None,
    )


def reload_identity_config() -> IdentityConfig:
    """Rebuild the in-memory identity config snapshot from process env."""
    global _IDENTITY_CONFIG
    _IDENTITY_CONFIG = _load_identity_config_from_env()
    logger.info(
        "crawl_auth: identity config loaded bound_tokens=%d telegram=%s any=%s",
        len(_IDENTITY_CONFIG.bound_tokens),
        bool(_IDENTITY_CONFIG.telegram_token and _IDENTITY_CONFIG.telegram_secret),
        _IDENTITY_CONFIG.any_configured,
    )
    return _IDENTITY_CONFIG


def get_identity_config() -> IdentityConfig:
    return _IDENTITY_CONFIG


def any_tokens_configured() -> bool:
    """True iff at least one bound token OR the telegram pair is configured."""
    return _IDENTITY_CONFIG.any_configured


def canonicalize_submitted_by(raw: str) -> str:
    """Lowercase + strip; callers should use this before storage/compare."""
    return (raw or "").strip().lower()


def validate_submitted_by(sb: str) -> None:
    if not _NAMESPACE_RE.match(sb):
        raise CrawlAuthError(
            "submitted_by must match <namespace>:<identifier>",
            status_code=400,
        )
    namespace, identifier = sb.split(":", 1)
    validator = NAMESPACE_VALIDATORS.get(namespace)
    if validator is None:
        raise CrawlAuthError(
            f"unknown namespace '{namespace}' — register in NAMESPACE_VALIDATORS",
            status_code=400,
        )
    if not validator.match(identifier):
        if namespace == "telegram":
            raise CrawlAuthError(
                "telegram identifier must be tg<numeric_id>",
                status_code=400,
            )
        raise CrawlAuthError(
            f"{namespace} identifier failed validator",
            status_code=400,
        )


def _verify_telegram_claim(claim: str, secret: str) -> str:
    try:
        identity, ts_str, sig_hex = claim.split("|", 2)
    except ValueError as exc:
        raise CrawlAuthError("malformed X-Identity-Claim", status_code=400) from exc
    try:
        ts = int(ts_str)
    except ValueError as exc:
        raise CrawlAuthError("malformed X-Identity-Claim timestamp", status_code=400) from exc
    age = int(time.time()) - ts
    if age > HMAC_MAX_AGE_SECONDS or age < -HMAC_MAX_AGE_SECONDS:
        raise CrawlAuthError("identity claim expired", status_code=401)
    expected = hmac.new(
        secret.encode("utf-8"),
        f"{identity}|{ts}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, sig_hex.lower()):
        raise CrawlAuthError("identity claim signature mismatch", status_code=401)
    return identity


def authenticate_request(
    *,
    authorization_header: Optional[str],
    identity_claim_header: Optional[str],
    body_submitted_by: Optional[str],
) -> AuthResult:
    """Validate bearer token + derive ``submitted_by``.

    Raises:
        FeatureDisabledError: AGENTIC_CRAWL_ENABLED != 'true'
        NotConfiguredError: no tokens configured at all (service mis-setup)
        CrawlAuthError: bad/missing bearer, invalid identity, namespace mismatch
    """
    if not is_feature_enabled():
        raise FeatureDisabledError()
    if not any_tokens_configured():
        raise NotConfiguredError("per-surface tokens not configured")

    if not authorization_header or not authorization_header.lower().startswith("bearer "):
        raise CrawlAuthError("missing or malformed Authorization header")
    token = authorization_header.split(" ", 1)[1].strip()
    if not token:
        raise CrawlAuthError("empty bearer token")

    # Telegram HMAC path
    tg_token = _IDENTITY_CONFIG.telegram_token
    tg_secret = _IDENTITY_CONFIG.telegram_secret
    if tg_token and tg_secret and hmac.compare_digest(token, tg_token):
        if not identity_claim_header:
            raise CrawlAuthError("X-Identity-Claim required for telegram namespace")
        identity = _verify_telegram_claim(identity_claim_header, tg_secret)
        sb = canonicalize_submitted_by(f"telegram:{identity}")
        validate_submitted_by(sb)
        return AuthResult(submitted_by=sb, namespace="telegram")

    # Bound-token path
    bound = _IDENTITY_CONFIG.bound_tokens
    if token in bound:
        derived = bound[token]
        if body_submitted_by is not None:
            raise CrawlAuthError(
                "body submitted_by not accepted for bound-token namespace",
                status_code=400,
            )
        validate_submitted_by(derived)
        namespace = derived.split(":", 1)[0]
        return AuthResult(submitted_by=derived, namespace=namespace)

    raise CrawlAuthError("unknown bearer token")


# Load a snapshot on import so requests fail closed even before the app's
# startup path explicitly reloads after env is finalized.
reload_identity_config()
