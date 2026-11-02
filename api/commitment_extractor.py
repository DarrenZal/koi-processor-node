"""Commitment extraction pipeline — AI-powered extraction of structured commitments from transcripts.

Adapts the proven draft_commitment_from_text prompt (MCP tools) for batch extraction.
Follows the same pattern as claim_extractor.py: LLM extraction, confidence threshold,
structured candidates returned for human review.
"""

import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_FEW_SHOT_EXAMPLES = """
Example commitments and needs extracted from a bioregional mapping workshop transcript:

Transcript excerpt: "I can offer our team for about 200 hours of watershed restoration work through the summer. We've got expertise in riparian planting and stream bank stabilization."
Extracted:
- pledger_name: "Sarah"
  pledger_organization: "Regenerate Cascadia"
  title: "Watershed restoration labor"
  description: "200 hours of watershed restoration work including riparian planting and stream bank stabilization"
  declaration_type: "commitment"
  offer_type: "labor"
  quantity: 200
  unit: "hours"
  validity_start: null
  validity_end: null
  estimated_value_usd: null
  routing_tags: ["watershed-restoration", "riparian-planting", "stream-bank-stabilization"]
  wants: ["monitoring data from partner sites"]
  limits: ["summer season only"]
  confidence: 0.88
  source_snippet: "I can offer our team for about 200 hours of watershed restoration work through the summer."

Transcript excerpt: "We have four portable soil monitoring kits that we can lend out quarterly. Each kit includes pH meters, moisture sensors, and sampling tools."
Extracted:
- pledger_name: "Randy"
  pledger_organization: "Kinship Earth"
  title: "Soil monitoring equipment loan"
  description: "Quarterly loan of 4 portable soil monitoring kits including pH meters, moisture sensors, and sampling tools"
  declaration_type: "commitment"
  offer_type: "goods"
  quantity: 4
  unit: "kits/quarter"
  validity_start: null
  validity_end: null
  estimated_value_usd: null
  routing_tags: ["soil-monitoring", "equipment-sharing", "ecological-assessment"]
  wants: ["soil data reports from kit users"]
  limits: ["quarterly rotation", "must return in working condition"]
  confidence: 0.91
  source_snippet: "We have four portable soil monitoring kits that we can lend out quarterly."

Transcript excerpt: "Our rent for the community workshop space is about $1,500 a month. We really need that covered in cash — can't pay the landlord in volunteer hours."
Extracted:
- pledger_name: "Sarah"
  pledger_organization: "Regenerate Cascadia"
  title: "Workshop space rental"
  description: "Monthly rent for community workshop space, $1,500/month, must be paid in fiat currency"
  declaration_type: "need"
  offer_type: "service"
  need_category: "housing"
  fiat_only: true
  monthly_amount_usd: 1500
  estimated_value_usd: 1500
  routing_tags: ["workspace", "overhead", "facilities"]
  wants: []
  limits: ["must be fiat payment"]
  confidence: 0.85
  source_snippet: "Our rent for the community workshop space is about $1,500 a month."

Transcript excerpt: "We could use help with food for our volunteer crews — if someone has a garden surplus or can do group meals, that would save us around $300 a month."
Extracted:
- pledger_name: "Randy"
  pledger_organization: "Kinship Earth"
  title: "Volunteer crew food support"
  description: "Food for volunteer crews, approximately $300/month, open to in-kind contributions like garden surplus or group meals"
  declaration_type: "need"
  offer_type: "goods"
  need_category: "food"
  fiat_only: false
  monthly_amount_usd: 300
  estimated_value_usd: 300
  routing_tags: ["food", "volunteer-support", "community-meals"]
  wants: []
  limits: []
  confidence: 0.82
  source_snippet: "We could use help with food for our volunteer crews"
"""

_EXTRACTION_PROMPT = """You are a commitment extraction system for a bioregional knowledge commons. Extract structured commitments AND needs from the provided transcript text.

A commitment is a concrete offer of labor, goods, service, knowledge, or stewardship that someone pledges to contribute. A need is a concrete resource requirement that a person or organization has. Both must be:
- Specific and actionable (not vague aspirations)
- Attributed to an identifiable person or organization

Commitments are offers/pledges. Needs are requirements/costs/expenses that must be met.

{few_shot}

Rules:
1. Each entry must have an identifiable pledger (person or org)
2. If the pledger's organization is mentioned or identifiable from context, include it
3. offer_type must be exactly one of: labor, goods, service, knowledge, stewardship
4. routing_tags should include 2-5 relevant domain keywords for pool matching
5. wants = things the pledger wants in return or as conditions (reciprocity)
6. limits = constraints, conditions, or boundaries on the commitment
7. If dates are relative (e.g. "through June", "next quarter"), interpret relative to today
8. Assign confidence 0.0-1.0 based on how explicit and concrete the commitment is
9. Include the relevant quote from the transcript as source_snippet
10. Do NOT extract generic statements, aspirations, or questions as commitments
11. Do NOT invent information not present in the transcript
12. For needs: set declaration_type to "need", include need_category (housing, food, compute, storage, transport, utilities, equipment, services), fiat_only (true if must be paid in cash/stablecoin, false if substitutable via vouchers or in-kind), and monthly_amount_usd if mentioned{bioregion_hint}

Respond with a JSON object containing a "commitments" array. Each object:
{{
  "pledger_name": "...",
  "pledger_organization": "..." or null,
  "title": "short title (under 80 chars)",
  "description": "full description",
  "declaration_type": "commitment" or "need",
  "offer_type": "labor|goods|service|knowledge|stewardship",
  "need_category": "string or null (for needs only)",
  "fiat_only": boolean or null (for needs only),
  "monthly_amount_usd": number or null (for needs only),
  "quantity": number or null,
  "unit": "string" or null,
  "validity_start": "ISO date" or null,
  "validity_end": "ISO date" or null,
  "estimated_value_usd": number or null,
  "routing_tags": ["tag1", "tag2"],
  "wants": ["reciprocity item 1"],
  "limits": ["constraint 1"],
  "confidence": 0.0-1.0,
  "source_snippet": "relevant quote from transcript"
}}

If no valid commitments or needs found, return: {{"commitments": []}}

Transcript text:
{document_text}
"""


def _parse_llm_response(response_text: str) -> List[Dict[str, Any]]:
    """Parse LLM response into commitment candidates."""
    import json

    text = response_text.strip()
    # Strip markdown code blocks
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        result = json.loads(text)
        if isinstance(result, dict) and "commitments" in result:
            return result["commitments"]
        if isinstance(result, list):
            return result
        return []
    except json.JSONDecodeError:
        # Try to find JSON object or array in the response
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group())
                if isinstance(parsed, dict) and "commitments" in parsed:
                    return parsed["commitments"]
            except json.JSONDecodeError:
                pass
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        logger.warning("Failed to parse LLM commitment extraction response as JSON")
        return []


def _normalize_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize LLM output — set missing keys to null so callers can rely on field existence."""
    c.setdefault("pledger_name", "")
    c.setdefault("pledger_organization", None)
    c.setdefault("title", "")
    c.setdefault("description", "")
    # NOTE: these two setdefaults are themselves coercions — a MISSING declaration_type
    # silently becomes "commitment", the strongest state on the gradient. Recorded below
    # rather than changed, so no downstream value shifts; see _coercions.
    _missing_decl = "declaration_type" not in c or c.get("declaration_type") is None
    _missing_offer = "offer_type" not in c or c.get("offer_type") is None
    c.setdefault("declaration_type", "commitment")
    c.setdefault("offer_type", "labor")
    c.setdefault("need_category", None)
    c.setdefault("fiat_only", None)
    c.setdefault("monthly_amount_usd", None)
    c.setdefault("validity_start", None)
    c.setdefault("validity_end", None)
    c.setdefault("quantity", None)
    c.setdefault("unit", None)
    c.setdefault("estimated_value_usd", None)
    c.setdefault("source_snippet", "")
    c.setdefault("wants", [])
    c.setdefault("limits", [])
    c.setdefault("routing_tags", [])
    c.setdefault("confidence", 0.5)

    # --- Coercion audit (added 2026-08-28) -------------------------------------------
    # This function had FOUR silent coercions. The declaration_type ones are the sharp
    # end: an unrecognised or missing value became "commitment" — the STRONGEST reading
    # on the care -> offer/need -> promise -> witnessed-commitment gradient — and that
    # path has already produced a real on-chain mint from what the source text called an
    # "offer". Failing toward the strongest claim is the coercive direction.
    #
    # Deliberately behaviour-preserving: the resulting VALUES are unchanged, so nothing
    # downstream shifts. What changes is that the coercion is no longer silent. Widening
    # declaration_type to carry the full gradient (offer/promise) is a schema change and
    # is tracked separately — it must not be smuggled in under a logging fix.
    # Defect class: feedback_silent_coercion_of_unknown_states.
    _coercions: List[Dict[str, Any]] = []
    if _missing_decl:
        _coercions.append({"field": "declaration_type", "raw": None, "assigned": "commitment",
                           "reason": "absent from LLM output"})
    if _missing_offer:
        _coercions.append({"field": "offer_type", "raw": None, "assigned": "labor",
                           "reason": "absent from LLM output"})

    # Validate offer_type
    valid_types = {"labor", "goods", "service", "knowledge", "stewardship"}
    if c["offer_type"] not in valid_types:
        _coercions.append({"field": "offer_type", "raw": c["offer_type"], "assigned": "labor",
                           "reason": "not in valid_types"})
        c["offer_type"] = "labor"

    # Validate declaration_type
    if c["declaration_type"] not in ("commitment", "need"):
        _coercions.append({"field": "declaration_type", "raw": c["declaration_type"],
                           "assigned": "commitment",
                           "reason": "not in (commitment, need) — coerced to STRONGEST state"})
        c["declaration_type"] = "commitment"

    if _coercions:
        c["_coercions"] = _coercions
        # requires_review is the signal a human/automated acceptance step must honour.
        # A coerced declaration_type must never reach a minting path unreviewed.
        c["requires_review"] = True
        for _c in _coercions:
            logger.warning(
                "commitment_extractor: coerced %s=%r -> %r (%s); candidate marked requires_review. "
                "title=%r pledger=%r",
                _c["field"], _c["raw"], _c["assigned"], _c["reason"],
                c.get("title", "")[:60], c.get("pledger_name", "")[:40],
            )
    else:
        c.setdefault("requires_review", False)

    # For needs, carry estimated_value_usd from monthly_amount_usd if not set
    if c["declaration_type"] == "need" and not c["estimated_value_usd"] and c["monthly_amount_usd"]:
        c["estimated_value_usd"] = c["monthly_amount_usd"]

    return c


async def extract_commitments_from_text(
    document_text: str,
    source_document: str,
    bioregion: Optional[str] = None,
    confidence_threshold: float = 0.6,
) -> Dict[str, Any]:
    """Extract structured commitments from transcript text using OpenAI.

    Returns dict with 'candidates' list and 'summary' string.
    Candidates are NOT persisted — caller decides whether to create.
    """
    if len(document_text) < 50:
        return {"candidates": [], "summary": "Text too short for extraction."}

    bioregion_hint = ""
    if bioregion:
        bioregion_hint = f"\n12. The bioregion context is: {bioregion}. Use this to inform routing_tags."

    prompt = _EXTRACTION_PROMPT.format(
        few_shot=_FEW_SHOT_EXAMPLES,
        document_text=document_text[:15000],  # Limit input size
        bioregion_hint=bioregion_hint,
    )

    # Use OpenAI (matches server LLM_BACKEND=openai configuration)
    try:
        from openai import OpenAI

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY not set, cannot extract commitments")
            return {"candidates": [], "summary": "OpenAI API key not configured."}

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You extract structured commitments from bioregional mapping workshop transcripts. Return strict JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=4096,
        )

        response_text = response.choices[0].message.content or ""
        raw_candidates = _parse_llm_response(response_text)

    except ImportError:
        logger.error("openai package not installed, cannot extract commitments")
        return {"candidates": [], "summary": "OpenAI package not available."}
    except Exception as e:
        logger.error(f"Commitment extraction LLM call failed: {e}")
        return {"candidates": [], "summary": f"LLM extraction failed: {e}"}

    # Normalize and filter
    candidates = []
    for c in raw_candidates:
        c = _normalize_candidate(c)

        # Min title length
        if len(c.get("title", "")) < 3:
            continue
        # Confidence threshold
        if c.get("confidence", 0) < confidence_threshold:
            continue
        # Must have a pledger
        if not c.get("pledger_name", "").strip():
            continue

        c["source_document"] = source_document
        candidates.append(c)

    summary_parts = []
    if candidates:
        summary_parts.append(f"Extracted {len(candidates)} commitment(s) from transcript.")
        by_type = {}
        for c in candidates:
            t = c["offer_type"]
            by_type[t] = by_type.get(t, 0) + 1
        summary_parts.append("Types: " + ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())))
    else:
        summary_parts.append("No commitments extracted above confidence threshold.")

    logger.info(
        f"commitment_extractor: {len(raw_candidates)} raw → {len(candidates)} after filters "
        f"(threshold={confidence_threshold}, source={source_document})"
    )

    return {
        "candidates": candidates,
        "summary": " ".join(summary_parts),
    }
