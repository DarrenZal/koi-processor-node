"""
Vision OCR bridge for the agentic crawler.

Two functions:
- ``fetch_image_bytes(url)`` — SSRF-safe image fetch with 5 MB cap and
  content-type enforcement. Uses a direct aiohttp fetch first, then retries
  through ``SCRAPING_PROXY_URL`` when configured so partner-grid logos hosted
  behind reputation-based WAFs still resolve. Off-domain images are allowed
  (not fetching them would defeat partner extraction) but every URL passes
  ``URLValidator.validate()`` before any socket is opened.
- ``vision_extract_orgs(url, role, context)`` — end-to-end: fetch image,
  invoke ``crawl_llm.extract_orgs_from_image``, return orgs + usage.

All LLM calls go through ``crawl_llm``, never the main chat abstraction.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from typing import Optional, Tuple

import aiohttp

from api import crawl_llm
from api.web_fetcher import (
    FETCH_TIMEOUT,
    MAX_REDIRECT_HOPS,
    URLValidationError,
    URLValidator,
    USER_AGENT,
    _absolute_redirect_target,
    _validate_hop,
)

logger = logging.getLogger(__name__)

MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB
_PROXY_URL = os.environ.get("SCRAPING_PROXY_URL", "")


async def _fetch_image_once(
    url: str,
    *,
    headers: dict[str, str],
    proxy: str | None = None,
) -> Optional[Tuple[bytes, str, str]]:
    timeout = aiohttp.ClientTimeout(total=FETCH_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        current = url
        for _ in range(MAX_REDIRECT_HOPS + 1):
            async with session.get(
                current, headers=headers, allow_redirects=False, proxy=proxy
            ) as response:
                if 300 <= response.status < 400:
                    loc = response.headers.get("Location")
                    if not loc:
                        logger.info("vision_ocr: 3xx with no Location on %s", current)
                        return None
                    next_url = _absolute_redirect_target(current, loc)
                    validated = _validate_hop(next_url)
                    if validated is None:
                        return None
                    current = validated
                    continue

                if response.status != 200:
                    logger.info("vision_ocr: non-200 %s for %s", response.status, current)
                    return None

                content_type = (
                    response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                )
                if not content_type.startswith("image/"):
                    logger.info(
                        "vision_ocr: rejected non-image content-type '%s' for %s",
                        content_type,
                        current,
                    )
                    return None

                declared_length = response.headers.get("Content-Length")
                if declared_length:
                    try:
                        if int(declared_length) > MAX_IMAGE_BYTES:
                            logger.info(
                                "vision_ocr: image %s exceeds 5 MB (declared %s)",
                                current,
                                declared_length,
                            )
                            return None
                    except ValueError:
                        pass

                chunks: list[bytes] = []
                total = 0
                while True:
                    chunk = await response.content.read(min(64 * 1024, MAX_IMAGE_BYTES + 1 - total))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > MAX_IMAGE_BYTES:
                        logger.info("vision_ocr: image %s exceeded 5 MB during read", current)
                        return None
                    chunks.append(chunk)

                body = b"".join(chunks)

                return body, content_type, current

        logger.info("vision_ocr: too many redirects for %s", url)
        return None


async def fetch_image_bytes(
    url: str,
) -> Optional[Tuple[bytes, str, str]]:
    """Fetch an image via the same redirect-safe path as ``_fetch_html_aiohttp``.

    Returns ``(bytes, content_type, final_url)`` on success, or ``None`` if
    the URL fails validation, the content type isn't an image, the size cap
    is exceeded, or any I/O error occurs (logged as a warning).
    """
    try:
        url = URLValidator().validate(url)
    except URLValidationError as exc:
        logger.warning("vision_ocr: SSRF block on image %s: %s", url, exc)
        return None

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "image/*,*/*;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": url,
    }

    try:
        fetched = await _fetch_image_once(url, headers=headers)
        if fetched is not None:
            return fetched
        if _PROXY_URL:
            logger.info("vision_ocr: retrying via residential proxy for %s", url)
            return await _fetch_image_once(url, headers=headers, proxy=_PROXY_URL)
        return None
    except Exception as exc:
        logger.warning("vision_ocr: fetch failed for %s: %s", url, exc)
        return None


async def vision_extract_orgs(
    *,
    image_url: str,
    role: str,
    context: str = "",
    model: str = crawl_llm.DEFAULT_MODEL,
) -> Tuple[list[dict], dict[str, int], Optional[str]]:
    """Fetch + OCR one image. Returns ``(orgs, usage, fetched_url)``.

    ``fetched_url`` is the final URL after any redirects (used to populate
    ``source_image`` on extracted entities); ``None`` when the fetch failed.
    Orgs list is always present (empty on any failure).
    """
    fetched = await fetch_image_bytes(image_url)
    if fetched is None:
        return [], {"prompt_tokens": 0, "completion_tokens": 0}, None
    img_bytes, content_type, final_url = fetched

    try:
        orgs, usage = await crawl_llm.extract_orgs_from_image(
            image_bytes=img_bytes,
            content_type=content_type,
            context=context,
            role=role,
            model=model,
        )
    except Exception as exc:
        logger.warning("vision_ocr: LLM invocation failed for %s: %s", image_url, exc)
        return [], {"prompt_tokens": 0, "completion_tokens": 0}, final_url

    return orgs, usage, final_url
