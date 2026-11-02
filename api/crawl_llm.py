"""
Dedicated OpenAI module for the agentic web-crawl subsystem.

Lives outside the main chat abstraction so the crawler can use multimodal
input and access usage counts for cost tracking without forcing the existing
text-only abstraction to grow those capabilities. AC49 verifies this module
makes no reference to that abstraction.

Phase 1 surface:
- compact_page(html, base_url) -> CompactedPage
- analyze_page(compacted, goal, world_model_summary) -> (PageAnalysis, usage)
- COST_TABLE + estimate_usd(usage, model)

Phase 2 adds extract_orgs_from_image; Phase 4 adds parse_relate_clause.
Both are stubbed here with raise NotImplementedError so the import surface
is stable from the start.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin

from pydantic import ValidationError

from api.prompts.crawl_page_analysis import (
    PageAnalysis,
    build_messages,
    build_retry_messages,
)

logger = logging.getLogger(__name__)

DEFAULT_MODEL = os.environ.get("CRAWL_LLM_MODEL", "gpt-4o-mini")
MAX_INPUT_TOKENS = 20_000
MAX_HTML_BYTES = 5 * 1024 * 1024
MAX_LINKS_AFTER_TRUNC = 80
MAX_IMAGES_AFTER_TRUNC = 20

# Per-1M-token rates in USD. Values audited against OpenAI pricing at plan-write time.
COST_TABLE: dict[str, dict[str, float]] = {
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
}


class CrawlLLMError(RuntimeError):
    """Raised on unrecoverable LLM invocation failure (2 malformed attempts)."""


@dataclass
class CompactedPage:
    url: str
    title: str | None
    meta_description: str | None
    headings: list[str] = field(default_factory=list)
    main_text: str = ""
    structured_data: list[dict] = field(default_factory=list)
    links: list[dict] = field(default_factory=list)  # {url, anchor}
    images: list[dict] = field(default_factory=list)  # {src, alt, context}
    truncations: list[str] = field(default_factory=list)
    token_count: int = 0

    def to_json(self) -> str:
        payload = {
            "url": self.url,
            "title": self.title,
            "meta_description": self.meta_description,
            "headings": self.headings,
            "main_text": self.main_text,
            "structured_data": self.structured_data,
            "links": self.links,
            "images": self.images,
            "truncations": self.truncations,
        }
        return json.dumps(payload, ensure_ascii=False)


def _count_tokens(text: str, model: str = DEFAULT_MODEL) -> int:
    try:
        import tiktoken
    except ImportError:
        # Conservative fallback: 4 chars/token.
        return max(1, len(text) // 4)
    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))


def _extract_trafilatura(html: str) -> str | None:
    try:
        import trafilatura
    except ImportError:
        return None
    try:
        return trafilatura.extract(html, include_comments=False, include_tables=False) or None
    except Exception:
        return None


def compact_page(html: str, base_url: str) -> CompactedPage:
    """Distill raw HTML into a bounded JSON-friendly structure the LLM can chew.

    Enforces MAX_INPUT_TOKENS by progressively dropping sections when over budget.
    """
    if len(html.encode("utf-8", errors="ignore")) > MAX_HTML_BYTES:
        # Truncate brute-force at byte level before parsing; a 5 MB page with
        # useful content first is still largely captured.
        html = html[: MAX_HTML_BYTES * 2]  # upper-bound on char count ~2x bytes

    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    title = (soup.title.string or "").strip() if soup.title and soup.title.string else None
    meta_desc_tag = soup.find("meta", attrs={"name": "description"})
    meta_description = None
    if meta_desc_tag and meta_desc_tag.get("content"):
        meta_description = meta_desc_tag["content"].strip()

    headings: list[str] = []
    for level in ("h1", "h2", "h3"):
        for node in soup.find_all(level):
            text = node.get_text(" ", strip=True)
            if text:
                headings.append(f"{level}: {text}")

    main_text = _extract_trafilatura(html)
    if not main_text:
        body = soup.body or soup
        main_text = body.get_text(" ", strip=True) if body else ""

    structured_data: list[dict] = []
    for node in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            structured_data.append(json.loads(node.string or "{}"))
        except Exception:
            continue

    links: list[dict] = []
    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        absolute = urljoin(base_url, href)
        if absolute in seen_urls:
            continue
        seen_urls.add(absolute)
        anchor = a.get_text(" ", strip=True)[:200]
        links.append({"url": absolute, "anchor": anchor})

    images: list[dict] = []
    seen_imgs: set[str] = set()
    for img in soup.find_all("img", src=True):
        src = img["src"].strip()
        if not src or src.startswith("data:"):
            continue
        absolute = urljoin(base_url, src)
        if absolute in seen_imgs:
            continue
        seen_imgs.add(absolute)
        parent_heading = ""
        parent = img.find_parent(["section", "article", "div"])
        if parent is not None:
            for sib in parent.find_all(["h1", "h2", "h3", "h4"], limit=1):
                parent_heading = sib.get_text(" ", strip=True)
                break
        images.append(
            {
                "src": absolute,
                "alt": (img.get("alt") or "").strip()[:200],
                "context": parent_heading[:200],
            }
        )

    page = CompactedPage(
        url=base_url,
        title=title,
        meta_description=meta_description,
        headings=headings,
        main_text=main_text or "",
        structured_data=structured_data,
        links=links,
        images=images,
    )
    _enforce_token_budget(page)
    return page


def _enforce_token_budget(page: CompactedPage) -> None:
    token_count = _count_tokens(page.to_json())
    original = token_count
    if token_count <= MAX_INPUT_TOKENS:
        page.token_count = token_count
        return

    # 1. Truncate main_text.
    if page.main_text:
        max_chars = MAX_INPUT_TOKENS * 3  # ~3 chars/token heuristic
        if len(page.main_text) > max_chars:
            page.main_text = page.main_text[:max_chars]
            page.truncations.append("main_text")
            token_count = _count_tokens(page.to_json())

    # 2. Drop structured_data.
    if token_count > MAX_INPUT_TOKENS and page.structured_data:
        page.structured_data = []
        page.truncations.append("structured_data")
        token_count = _count_tokens(page.to_json())

    # 3. Cap links.
    if token_count > MAX_INPUT_TOKENS and len(page.links) > MAX_LINKS_AFTER_TRUNC:
        page.links = page.links[:MAX_LINKS_AFTER_TRUNC]
        page.truncations.append("links")
        token_count = _count_tokens(page.to_json())

    # 4. Cap images.
    if token_count > MAX_INPUT_TOKENS and len(page.images) > MAX_IMAGES_AFTER_TRUNC:
        page.images = page.images[:MAX_IMAGES_AFTER_TRUNC]
        page.truncations.append("images")
        token_count = _count_tokens(page.to_json())

    # 5. Last-resort: truncate main_text hard.
    while token_count > MAX_INPUT_TOKENS and len(page.main_text) > 500:
        page.main_text = page.main_text[: max(500, len(page.main_text) // 2)]
        page.truncations.append("main_text_hard")
        token_count = _count_tokens(page.to_json())

    page.token_count = token_count
    if page.truncations:
        logger.info(
            "compact_page truncated: url=%s original_tokens=%d final_tokens=%d dropped=%s",
            page.url,
            original,
            token_count,
            page.truncations,
        )


def _normalize_usage(raw: Any) -> dict[str, int]:
    if raw is None:
        return {"prompt_tokens": 0, "completion_tokens": 0}
    if isinstance(raw, dict):
        return {
            "prompt_tokens": int(raw.get("prompt_tokens", 0) or 0),
            "completion_tokens": int(raw.get("completion_tokens", 0) or 0),
        }
    return {
        "prompt_tokens": int(getattr(raw, "prompt_tokens", 0) or 0),
        "completion_tokens": int(getattr(raw, "completion_tokens", 0) or 0),
    }


def estimate_usd(usage: dict[str, int], model: str = DEFAULT_MODEL) -> float:
    rates = COST_TABLE.get(model) or COST_TABLE[DEFAULT_MODEL]
    prompt = usage.get("prompt_tokens", 0)
    completion = usage.get("completion_tokens", 0)
    return (prompt * rates["input"] + completion * rates["output"]) / 1_000_000


def _openai_client():
    from openai import OpenAI

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise CrawlLLMError("OPENAI_API_KEY not configured for crawl_llm")
    return OpenAI(api_key=api_key)


async def _chat_completion(
    messages: list[dict],
    *,
    model: str,
    json_mode: bool = True,
    max_tokens: int = 2048,
) -> tuple[str, dict[str, int]]:
    client = _openai_client()
    kwargs: dict[str, Any] = dict(
        model=model,
        messages=messages,
        temperature=0.0,
        max_tokens=max_tokens,
    )
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    response = await asyncio.to_thread(client.chat.completions.create, **kwargs)
    content = response.choices[0].message.content or "{}"
    return content, _normalize_usage(getattr(response, "usage", None))


async def analyze_page(
    *,
    page: CompactedPage,
    goal: str,
    world_model_summary: str,
    allowed_types: list[str],
    allowed_predicates: list[str],
    model: str = DEFAULT_MODEL,
) -> tuple[PageAnalysis, dict[str, int]]:
    """Analyze a single page. One retry on malformed output before raising."""
    compacted_json = page.to_json()
    messages = build_messages(
        goal=goal,
        compacted_page_json=compacted_json,
        world_model_summary=world_model_summary,
        allowed_types=allowed_types,
        allowed_predicates=allowed_predicates,
    )
    content, usage = await _chat_completion(messages, model=model)
    try:
        analysis = _validate_analysis(content, allowed_types, allowed_predicates)
        return analysis, usage
    except (ValidationError, ValueError) as exc:
        logger.warning("crawl_llm.analyze_page retry on %s: %s", page.url, exc)
        retry_messages = build_retry_messages(
            goal=goal,
            compacted_page_json=compacted_json,
            world_model_summary=world_model_summary,
            allowed_types=allowed_types,
            allowed_predicates=allowed_predicates,
            prior_error=str(exc),
            prior_output=content,
        )
        retry_content, retry_usage = await _chat_completion(retry_messages, model=model)
        combined = {
            "prompt_tokens": usage["prompt_tokens"] + retry_usage["prompt_tokens"],
            "completion_tokens": usage["completion_tokens"] + retry_usage["completion_tokens"],
        }
        analysis = _validate_analysis(retry_content, allowed_types, allowed_predicates)
        return analysis, combined


def _validate_analysis(
    content: str,
    allowed_types: list[str],
    allowed_predicates: list[str],
) -> PageAnalysis:
    data = json.loads(content)
    analysis = PageAnalysis.model_validate(data)
    types_set = set(allowed_types)
    preds_set = set(allowed_predicates)
    bad_types = [e.type for e in analysis.entities if e.type not in types_set]
    if bad_types:
        raise ValueError(f"Unknown entity types: {sorted(set(bad_types))}")
    bad_preds = [r.predicate for r in analysis.relationships if r.predicate not in preds_set]
    if bad_preds:
        raise ValueError(f"Unknown predicates: {sorted(set(bad_preds))}")
    return analysis


_IMAGE_SYSTEM_PROMPT = (
    "You are looking at a single image from a web page. Extract any "
    "organization, company, sponsor, funder, or partner names that appear as "
    "recognizable text or logos. Also extract Person names if the image is a "
    "team photo with captions, or Concept names if the image is an "
    "infographic with labeled nodes.\n\n"
    "Return JSON with this exact shape (no prose):\n"
    "{\"orgs\": [{\"name\": str, \"confidence\": float 0..1}]}\n\n"
    "Rules:\n"
    "- Confidence = your certainty that the text/logo corresponds to a real, "
    "stable named entity. Stylized logos you can read confidently → high. "
    "Blurry or partially-obscured logos → low. Decorative icons or stock "
    "imagery with no identifying text → drop entirely (return empty list).\n"
    "- Do NOT invent names you cannot see. If you are not sure, omit the entry.\n"
    "- Return up to 20 entries max. If more are present, return the most "
    "prominent 20."
)


async def extract_orgs_from_image(
    *,
    image_bytes: bytes,
    content_type: str,
    context: str = "",
    role: str = "unknown",
    model: str = DEFAULT_MODEL,
) -> tuple[list[dict], dict[str, int]]:
    """Run gpt-4o-mini vision on a single image. Return (orgs, usage).

    One retry on malformed JSON / bad shape. On second failure: returns
    ``([], usage)`` and logs a warning — vision failures never terminate the
    crawl (the caller still gets the cost for both attempts).
    """
    import base64

    if not content_type.startswith("image/"):
        raise ValueError(f"extract_orgs_from_image: non-image content_type '{content_type}'")

    b64 = base64.b64encode(image_bytes).decode("ascii")
    data_url = f"data:{content_type};base64,{b64}"

    user_text = (
        f"Image role (LLM-assigned from page context): {role}\n"
        f"Surrounding context: {context or '(none)'}\n\n"
        "Extract named entities visible in this image as specified."
    )
    messages = [
        {"role": "system", "content": _IMAGE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": user_text},
                {"type": "image_url", "image_url": {"url": data_url}},
            ],
        },
    ]

    content, usage = await _chat_completion(messages, model=model, max_tokens=1024)
    first_error: Optional[Exception] = None
    try:
        return _parse_orgs_payload(content), usage
    except (ValueError, json.JSONDecodeError) as exc:
        first_error = exc
        logger.warning("extract_orgs_from_image retry: %s", exc)

    retry_messages = list(messages)
    retry_messages.append({"role": "assistant", "content": content[:2000]})
    retry_messages.append(
        {
            "role": "user",
            "content": (
                "Your previous reply did not match the required JSON shape:\n"
                f"{first_error}\n\n"
                "Re-emit a valid JSON object with the `orgs` key only."
            ),
        }
    )
    retry_content, retry_usage = await _chat_completion(
        retry_messages, model=model, max_tokens=1024
    )
    combined = {
        "prompt_tokens": usage["prompt_tokens"] + retry_usage["prompt_tokens"],
        "completion_tokens": usage["completion_tokens"] + retry_usage["completion_tokens"],
    }
    try:
        return _parse_orgs_payload(retry_content), combined
    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning("extract_orgs_from_image dropped after retry: %s", exc)
        return [], combined


def _parse_orgs_payload(content: str) -> list[dict]:
    data = json.loads(content)
    orgs = data.get("orgs")
    if not isinstance(orgs, list):
        raise ValueError("response missing 'orgs' list")
    cleaned: list[dict] = []
    for entry in orgs[:20]:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("name") or "").strip()
        if not name:
            continue
        try:
            confidence = float(entry.get("confidence", 0.0))
        except (TypeError, ValueError):
            continue
        confidence = max(0.0, min(1.0, confidence))
        cleaned.append({"name": name, "confidence": confidence})
    return cleaned


async def parse_relate_clause(*_args, **_kwargs):
    instruction = (_kwargs.get("instruction") if _kwargs else None) or (_args[0] if _args else "")
    model = _kwargs.get("model", DEFAULT_MODEL) if _kwargs else DEFAULT_MODEL
    system_prompt = (
        "Extract only explicit 'relate/connect/include/partner with/about' targets "
        "from the user's instruction. Return JSON with shape "
        "{\"targets\": [{\"label\": str, \"predicate_hint\": str|null, \"type_hint\": str|null}]}.\n"
        "Rules:\n"
        "- If there are no explicit relate/connect clauses, return {\"targets\": []}.\n"
        "- Keep labels exactly as written where practical.\n"
        "- predicate_hint should be a BKC-style predicate string when explicit, else null.\n"
        "- type_hint should be Organization, Project, Concept, Person, or null only when strongly implied.\n"
        "- No prose."
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": instruction or ""},
    ]
    content, usage = await _chat_completion(messages, model=model, max_tokens=512)
    try:
        data = json.loads(content)
        targets = data.get("targets", [])
        if not isinstance(targets, list):
            raise ValueError("targets must be a list")
        normalized: list[dict[str, Any]] = []
        for item in targets[:10]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()
            if not label:
                continue
            predicate_hint = item.get("predicate_hint")
            type_hint = item.get("type_hint")
            normalized.append(
                {
                    "label": label,
                    "predicate_hint": (str(predicate_hint).strip() or None) if predicate_hint is not None else None,
                    "type_hint": (str(type_hint).strip() or None) if type_hint is not None else None,
                }
            )
        return normalized, usage
    except Exception as exc:
        logger.warning("parse_relate_clause retry: %s", exc)
        retry_messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"Instruction:\n{instruction or ''}\n\n"
                    f"Previous output was invalid: {content}\n"
                    "Return only valid JSON with a top-level 'targets' array."
                ),
            },
        ]
        retry_content, retry_usage = await _chat_completion(retry_messages, model=model, max_tokens=512)
        combined = {
            "prompt_tokens": usage["prompt_tokens"] + retry_usage["prompt_tokens"],
            "completion_tokens": usage["completion_tokens"] + retry_usage["completion_tokens"],
        }
        data = json.loads(retry_content)
        targets = data.get("targets", [])
        if not isinstance(targets, list):
            raise CrawlLLMError("parse_relate_clause returned invalid targets payload")
        normalized = []
        for item in targets[:10]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()
            if not label:
                continue
            predicate_hint = item.get("predicate_hint")
            type_hint = item.get("type_hint")
            normalized.append(
                {
                    "label": label,
                    "predicate_hint": (str(predicate_hint).strip() or None) if predicate_hint is not None else None,
                    "type_hint": (str(type_hint).strip() or None) if type_hint is not None else None,
                }
            )
        return normalized, combined
