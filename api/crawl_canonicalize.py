"""
Canonicalization for crawl start URLs.

Used before DB insert / dedup so ``https://Peninsulastreams.CA:443/`` and
``https://peninsulastreams.ca/`` collapse to one key. Rules (from plan §68-76):

- Lowercase scheme + host
- Strip default ports (``:80`` for http, ``:443`` for https)
- Strip URL fragment (``#...``)
- Empty path becomes ``/``; otherwise preserve path verbatim (``/about`` and
  ``/about/`` stay distinct)
- Preserve query string verbatim
- Do NOT strip ``www.``
- Reject schemes other than ``http`` / ``https``

Distinct from ``URLValidator.validate`` (SSRF / private-IP blocking); this
module does no network resolution. Callers should pass the result through
``URLValidator.validate`` before any fetch.
"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


class StartUrlError(ValueError):
    """Raised when the start URL cannot be canonicalized."""


def canonicalize_start_url(url: str) -> str:
    if not url or not isinstance(url, str):
        raise StartUrlError("start_url must be a non-empty string")
    raw = url.strip()
    if not raw:
        raise StartUrlError("start_url must be a non-empty string")
    parts = urlsplit(raw)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        raise StartUrlError(f"unsupported scheme '{parts.scheme}'")
    if not parts.hostname:
        raise StartUrlError("start_url is missing a host")

    host = parts.hostname.lower()
    port = parts.port
    if port is not None and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    else:
        netloc = host

    path = parts.path or "/"
    # Preserve trailing-slash distinctions; only normalize the fully-empty path.

    return urlunsplit((scheme, netloc, path, parts.query, ""))
