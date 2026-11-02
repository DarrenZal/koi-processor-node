"""
B9.5 — CRAG confidence gate.

Lightweight confidence assessment between retrieval/reranking and answer
generation. Uses only signals already available in EvidenceBundle outputs:
no new models, no new API costs.

Three-way gate decision:
  CONFIDENT        → proceed to normal LLM generation
  LOW_CONFIDENCE   → retry text_search with deeper settings, then proceed
  VERY_LOW_CONFIDENCE → abstain with structured response

All CRAG logic (signal computation, gate assessment, retry orchestration)
runs in personal_ingest_api.py after execute_plan() returns.
execute_plan() is unchanged.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from api.schemas.query_plan import EvidenceBundle, RetrievalOp

logger = logging.getLogger(__name__)


class GateDecision(str, Enum):
    CONFIDENT = "CONFIDENT"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    VERY_LOW_CONFIDENCE = "VERY_LOW_CONFIDENCE"


@dataclass
class ConfidenceSignals:
    """Cheap deterministic signals computed from EvidenceBundle outputs."""

    top_text_confidence: float = 0.0
    """Max confidence across text_search bundles (FlashRank rerank_score)."""

    text_result_count: int = 0
    """Number of text_search bundles returned."""

    entity_match_count: int = 0
    """Number of entity_lookup bundles returned."""

    entity_lookup_ran: bool = False
    """Whether an ENTITY_LOOKUP step was part of the plan."""

    text_search_ran: bool = True
    """Whether a TEXT_SEARCH step was part of the plan. If False, zero text
    results is expected (e.g., shallow depth tier) and should not trigger
    low/very-low confidence."""

    confidence_spread: float = 0.0
    """max - median of text_search confidences. High spread = one good match
    surrounded by noise; low spread = even coverage."""

    def to_dict(self) -> dict:
        return asdict(self)


def compute_signals(
    evidence: list[EvidenceBundle],
    plan_had_entity_lookup: bool = True,
    plan_had_text_search: bool = True,
) -> ConfidenceSignals:
    """Compute confidence signals from a list of EvidenceBundle.

    Args:
        evidence: All bundles returned by execute_plan().
        plan_had_entity_lookup: Whether the assembled plan included an
            ENTITY_LOOKUP step (used to condition the entity_match_count
            signal — only down-rank on zero when the step actually ran).
        plan_had_text_search: Whether the assembled plan included a
            TEXT_SEARCH step. If False, zero text results is expected
            (e.g., shallow depth tier) and the gate skips text-based signals.
    """
    from api.schemas.query_plan import RetrievalOp

    text_bundles = [b for b in evidence if b.retrieval_op == RetrievalOp.TEXT_SEARCH]
    entity_bundles = [b for b in evidence if b.retrieval_op == RetrievalOp.ENTITY_LOOKUP]

    text_confidences = [b.confidence for b in text_bundles]

    top_text = max(text_confidences) if text_confidences else 0.0
    text_count = len(text_bundles)
    entity_count = len(entity_bundles)

    # Confidence spread: max - median (meaningful only with ≥3 results)
    if len(text_confidences) >= 3:
        spread = max(text_confidences) - statistics.median(text_confidences)
    else:
        spread = 0.0

    return ConfidenceSignals(
        top_text_confidence=round(top_text, 4),
        text_result_count=text_count,
        entity_match_count=entity_count,
        entity_lookup_ran=plan_had_entity_lookup,
        text_search_ran=plan_had_text_search,
        confidence_spread=round(spread, 4),
    )


# ---------------------------------------------------------------------------
# Gate thresholds (v1: global, hardcoded)
# ---------------------------------------------------------------------------

# Top reranker score thresholds
# NOTE: FlashRank ms-marco-MiniLM-L-12-v2 scores on this corpus cluster
# tightly at 0.05-0.10. These thresholds are calibrated against Octo's
# wiki-heavy corpus (2,806 entities, 3,602 chunks). Reranker scores are
# NOT well-calibrated [0,1] — treat them as relative, not absolute.
_CONFIDENT_TOP_SCORE = 0.02
"""Above this, top text match is likely relevant. Set low because FlashRank
scores on this corpus rarely exceed 0.10 even for strong matches.
Calibrated against Octo corpus: non-multi_query text_search scores
cluster at 0.016-0.033. Only retry when below 0.02 to avoid over-firing
on normal single-query retrieval (reduces retry rate from 22% to ~8%)."""

_LOW_CONFIDENCE_TOP_SCORE = 0.01
"""Below this, retrieval has essentially no relevant content."""

# Minimum text results to be confident
_MIN_TEXT_RESULTS = 2
"""Fewer than this and we're probably missing context."""


def assess(signals: ConfidenceSignals) -> GateDecision:
    """Three-way gate decision from confidence signals.

    Decision logic (evaluated top-down, first match wins):

    0. If text_search was not part of the plan (e.g., shallow depth tier),
       skip text-based signals entirely. Gate on entity results only:
       - zero entities AND entity_lookup ran → VERY_LOW_CONFIDENCE
       - otherwise → CONFIDENT (trust the planner's depth choice)

    1. VERY_LOW_CONFIDENCE if:
       - No text results at all, OR
       - Top score < _LOW_CONFIDENCE_TOP_SCORE AND text count < _MIN_TEXT_RESULTS

    2. LOW_CONFIDENCE if:
       - Top score < _CONFIDENT_TOP_SCORE, OR
       - Text count < _MIN_TEXT_RESULTS, OR
       - Entity lookup ran, zero matches, AND top text score is below confident
         (advisory down-rank — only applies when entity step ran and text is also weak)

    3. CONFIDENT otherwise.
    """
    # Shallow plans (no text_search step): trust the planner's depth choice.
    # Only abstain if entity_lookup was requested and returned nothing.
    if not signals.text_search_ran:
        if signals.entity_lookup_ran and signals.entity_match_count == 0:
            return GateDecision.VERY_LOW_CONFIDENCE
        return GateDecision.CONFIDENT

    # Very low: essentially no usable retrieval
    if signals.text_result_count == 0:
        return GateDecision.VERY_LOW_CONFIDENCE

    if (signals.top_text_confidence < _LOW_CONFIDENCE_TOP_SCORE
            and signals.text_result_count < _MIN_TEXT_RESULTS):
        return GateDecision.VERY_LOW_CONFIDENCE

    # Low confidence checks
    if signals.top_text_confidence < _CONFIDENT_TOP_SCORE:
        return GateDecision.LOW_CONFIDENCE

    if signals.text_result_count < _MIN_TEXT_RESULTS:
        return GateDecision.LOW_CONFIDENCE

    # Advisory entity check: only down-rank when entity_lookup actually ran
    # AND text signals are also borderline (not strongly confident)
    if (signals.entity_lookup_ran
            and signals.entity_match_count == 0
            and signals.top_text_confidence < _CONFIDENT_TOP_SCORE * 1.2):
        return GateDecision.LOW_CONFIDENCE

    return GateDecision.CONFIDENT


# ---------------------------------------------------------------------------
# Abstention response
# ---------------------------------------------------------------------------

ABSTENTION_ANSWER = (
    "I don't have enough reliable context to answer this question accurately. "
    "Try rephrasing or asking about a more specific topic."
)
