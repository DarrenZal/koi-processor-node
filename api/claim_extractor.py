"""Claim extraction pipeline — AI-powered extraction of structured impact claims from documents.

Separate product pipeline from NLP enrichment (llm_enricher.py). Different goals:
- Source provenance required for every claim
- Quality filters: min length, no boilerplate, confidence threshold
- Template stripping before extraction
- Few-shot examples in prompt
- Returns candidates; creation is a separate explicit step
"""

import asyncio
import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Boilerplate phrases to filter out
_BOILERPLATE = {
    "lorem ipsum", "click here", "read more", "subscribe",
    "copyright", "all rights reserved", "terms of service",
    "privacy policy", "cookie policy",
}

_FEW_SHOT_EXAMPLES = """
Example claims extracted from documents:

Document: "CEC has partnered with 22 farms across Santa Barbara County since 2021, transitioning 450 acres from conventional to regenerative practices."
Extracted:
- statement: "CEC has partnered with 22 farms across Santa Barbara County, transitioning 450 acres from conventional to regenerative agriculture practices since 2021"
  claimant_name: "Community Environmental Council"
  claim_type: "ecological"
  confidence: 0.92
  metadata: {"quantity": 22, "unit": "farms", "area_acres": 450, "start_date": "2021-01-01", "subject_location": "Santa Barbara County", "sdg_tags": ["SDG2", "SDG15"]}

Document: "Through our Zero Food Print program, participating restaurants have funded the conversion of over 10,000 acres of farmland to climate-beneficial practices."
Extracted:
- statement: "Zero Food Print program funded conversion of over 10,000 acres of farmland to climate-beneficial practices through participating restaurants"
  claimant_name: "Zero Food Print"
  claim_type: "ecological"
  confidence: 0.88
  metadata: {"quantity": 10000, "unit": "acres", "theme_tags": ["climate_beneficial_agriculture"]}

Document: "The Regen Network community has governed 15 credit classes across 8 countries, enabling $2.3M in ecosystem service payments."
Extracted:
- statement: "Regen Network community governed 15 credit classes across 8 countries, enabling $2.3M in ecosystem service payments"
  claimant_name: "Regen Network"
  claim_type: "financial"
  confidence: 0.85
  metadata: {"credit_classes": 15, "countries": 8, "payment_amount_usd": 2300000, "sdg_tags": ["SDG13", "SDG15"]}
"""

_EXTRACTION_PROMPT = """You are an impact claim extraction system. Extract structured impact claims from the provided document text.

An impact claim is a specific, measurable assertion about environmental, social, or financial impact made by an identifiable entity (person or organization).

{few_shot}

Rules:
1. Each claim must have an identifiable claimant (who is making or associated with the claim)
2. Claims must be specific and measurable where possible (quantities, dates, locations)
3. Do NOT extract generic statements, mission statements, or aspirational goals without evidence
4. Do NOT extract boilerplate, headers, footers, or changelog entries
5. Assign a confidence score (0.0-1.0) based on specificity and verifiability
6. Classify as: ecological, social, financial, or governance
7. Extract metadata fields: quantity, unit, dates, locations, SDG tags, methodology where available

Respond with a JSON array of extracted claims. Each claim object:
{{
  "statement": "...",
  "claimant_name": "...",
  "claim_type": "ecological|social|financial|governance",
  "confidence": 0.0-1.0,
  "metadata": {{...}}
}}

If no valid claims found, return an empty array: []

Document text:
{document_text}
"""


def _strip_templates(text: str) -> str:
    """Remove common template/boilerplate sections."""
    # Remove changelog sections
    text = re.sub(r'(?i)## changelog.*?(?=\n## |\Z)', '', text, flags=re.DOTALL)
    # Remove table of contents
    text = re.sub(r'(?i)## table of contents.*?(?=\n## |\Z)', '', text, flags=re.DOTALL)
    # Remove excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _is_boilerplate(text: str) -> bool:
    """Check if text is likely boilerplate."""
    lower = text.lower()
    return any(bp in lower for bp in _BOILERPLATE)


def _parse_llm_response(response_text: str) -> List[Dict[str, Any]]:
    """Parse LLM response into claim candidates."""
    import json

    # Try to extract JSON from the response
    # Handle markdown code blocks
    text = response_text.strip()
    if text.startswith("```"):
        # Strip code block markers
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and "claims" in result:
            return result["claims"]
        return []
    except json.JSONDecodeError:
        # Try to find JSON array in the response
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        logger.warning("Failed to parse LLM extraction response as JSON")
        return []


# Transport, selected by CLAIM_EXTRACTOR_TRANSPORT:
#   'claude_p' (DEFAULT): the `claude -p` CLI on the Claude Max SUBSCRIPTION (api/claude_p.py).
#   'anthropic_api': the Anthropic Messages API — BILLS per token against ANTHROPIC_API_KEY.
#   'openai': an OpenAI-compatible endpoint — BILLS per token against OPENAI_API_KEY.
# Exactly one transport runs; there is no cross-transport fallback. The old chain
# (Anthropic API, then OpenAI) silently billed gpt-4.1 on every call for months once the
# Anthropic key ran out of credit, with only a WARNING to show for it.
# The only caller is POST /claims/extract, hit by batch ingest CLIs (ingest_document.py
# windows with a 240s HTTP timeout), so subprocess latency is acceptable and the
# deadline below stays under that timeout.
CLAIM_EXTRACTOR_TRANSPORT = os.getenv("CLAIM_EXTRACTOR_TRANSPORT", "claude_p").strip().lower()
CLAIM_EXTRACTOR_MODEL = os.getenv("CLAIM_EXTRACTOR_MODEL", "claude-sonnet-4-6")
CLAIM_EXTRACTOR_DEADLINE_S = float(os.getenv("CLAIM_EXTRACTOR_DEADLINE_S", "200"))


async def _llm_complete(prompt: str, *, max_tokens: int = 4096) -> "str | None":
    """Return an LLM completion for `prompt` from the configured transport, or None."""
    if CLAIM_EXTRACTOR_TRANSPORT == "claude_p":
        from api.claude_p import ClaudePError, complete

        try:
            return await complete(prompt, model=CLAIM_EXTRACTOR_MODEL,
                                  deadline_s=CLAIM_EXTRACTOR_DEADLINE_S)
        except ClaudePError as e:
            logger.error("claim_extractor: claude_p transport failed (%s)", e)
            return None
    if CLAIM_EXTRACTOR_TRANSPORT == "anthropic_api":
        return await _anthropic_api_complete(prompt, max_tokens=max_tokens)
    if CLAIM_EXTRACTOR_TRANSPORT == "openai":
        return await _openai_complete(prompt, max_tokens=max_tokens)
    logger.error("claim_extractor: unknown CLAIM_EXTRACTOR_TRANSPORT=%r", CLAIM_EXTRACTOR_TRANSPORT)
    return None


async def _anthropic_api_complete(prompt: str, *, max_tokens: int) -> "str | None":
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        logger.error("claim_extractor: CLAIM_EXTRACTOR_TRANSPORT=anthropic_api but ANTHROPIC_API_KEY is not set")
        return None
    try:
        import anthropic

        from api.provider_http import provider_timeout, PROVIDER_MAX_RETRIES

        # #36, and a worse variant than that issue describes. TWO bugs here:
        #  1. SDK defaults are Timeout(read=600, write=600, pool=600) with
        #     max_retries=2 -> up to 30 MINUTES on one call.
        #  2. `client.messages.create` is the SYNCHRONOUS SDK, and this is an
        #     `async def`. Called bare it BLOCKS THE EVENT LOOP for the whole
        #     duration — so one slow Anthropic call freezes the entire shared :8351
        #     service, not merely a thread-pool worker. That matches the "CPU frozen
        #     (event loop blocked)" symptom recorded in #36 more precisely than the
        #     embedding path does.
        client = anthropic.Anthropic(
            api_key=anthropic_key,
            timeout=provider_timeout(),
            max_retries=PROVIDER_MAX_RETRIES,
        )
        msg = await asyncio.to_thread(
            client.messages.create,
            model=CLAIM_EXTRACTOR_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text
    except Exception as e:
        logger.error("claim_extractor: Anthropic API transport failed (%s)", e)
        return None


async def _openai_complete(prompt: str, *, max_tokens: int) -> "str | None":
    openai_key = os.environ.get("OPENAI_API_KEY")
    if not openai_key:
        logger.error("claim_extractor: CLAIM_EXTRACTOR_TRANSPORT=openai but OPENAI_API_KEY is not set")
        return None
    try:
        import httpx

        base = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        model = os.getenv("CLAIM_EXTRACTOR_OPENAI_MODEL",
                          os.getenv("DOC_EXTRACTOR_OPENAI_MODEL", "gpt-4.1"))
        from api.provider_http import provider_async_client

        # #36: a single scalar timeout does not reliably fire on a half-closed
        # POOLED socket (observed: 40 min hang against timeout=300). Use per-phase
        # ceilings + a pool that evicts idle sockets + TCP keepalive.
        async with provider_async_client(read=60.0) as http:
            r = await http.post(
                f"{base}/chat/completions",
                headers={"Authorization": f"Bearer {openai_key}"},
                json={"model": model, "max_tokens": max_tokens,
                      "messages": [{"role": "user", "content": prompt}]},
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error("claim_extractor: OpenAI transport failed (%s)", e)
        return None


async def extract_claims_from_text(
    document_text: str,
    source_document: str,
    confidence_threshold: float = 0.7,
) -> List[Dict[str, Any]]:
    """Extract structured impact claims from document text using Claude.

    Returns list of claim candidate dicts, NOT persisted claims.
    """
    # Pre-filter
    cleaned = _strip_templates(document_text)
    if len(cleaned) < 50:
        return []

    # Build prompt.
    # NOTE: input is HARD-CAPPED at 10K chars — only the first ~10K of a long document
    # is ever seen here. A caller that needs whole-document coverage MUST window the text
    # itself and aggregate (see ingest_document.py:extract_document_claims, which windows
    # at 9K with overlap; server-side content_hash dedup collapses the overlaps). Raising
    # this cap affects every caller's token cost, so it's left to the caller to window.
    prompt = _EXTRACTION_PROMPT.format(
        few_shot=_FEW_SHOT_EXAMPLES,
        document_text=cleaned[:10000],  # hard cap — callers window for full coverage (see note above)
    )

    # LLM call through the one configured transport (see _llm_complete).
    response_text = await _llm_complete(prompt)
    if not response_text:
        return []
    raw_candidates = _parse_llm_response(response_text)

    # Apply quality filters
    candidates = []
    for c in raw_candidates:
        statement = c.get("statement", "")
        confidence = c.get("confidence", 0.0)

        # Min length
        if len(statement) < 20:
            continue
        # Confidence threshold
        if confidence < confidence_threshold:
            continue
        # Boilerplate check
        if _is_boilerplate(statement):
            continue

        # Ensure source provenance
        c["source_document"] = source_document
        candidates.append(c)

    logger.info(f"claim_extractor: {len(raw_candidates)} raw → {len(candidates)} after filters (threshold={confidence_threshold})")
    return candidates
