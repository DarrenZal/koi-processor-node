"""
Tests for B9.5 CRAG confidence gate.

Tests signal computation from synthetic EvidenceBundle lists, gate logic
for all three outcomes, and abstention response format.
"""

import pytest
from datetime import datetime, timezone

from api.confidence_gate import (
    ConfidenceSignals,
    GateDecision,
    assess,
    compute_signals,
    ABSTENTION_ANSWER,
    _CONFIDENT_TOP_SCORE,
    _LOW_CONFIDENCE_TOP_SCORE,
    _MIN_TEXT_RESULTS,
)
from api.schemas.query_plan import (
    EvidenceBundle,
    RetrievalOp,
    SourceType,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _text_bundle(confidence: float, text: str = "chunk") -> EvidenceBundle:
    return EvidenceBundle(
        source_uri=f"chunk:{confidence}",
        source_type=SourceType.LOCAL_DOCUMENT,
        retrieval_op=RetrievalOp.TEXT_SEARCH,
        confidence=confidence,
        text=text,
    )


def _entity_bundle(confidence: float = 0.8, label: str = "entity") -> EvidenceBundle:
    return EvidenceBundle(
        source_uri=f"entity:{label}",
        source_type=SourceType.LOCAL_AUTHORITATIVE,
        retrieval_op=RetrievalOp.ENTITY_LOOKUP,
        confidence=confidence,
        text=label,
        metadata={"label": label},
    )


def _web_bundle() -> EvidenceBundle:
    return EvidenceBundle(
        source_uri="web:test",
        source_type=SourceType.LOCAL_WEB,
        retrieval_op=RetrievalOp.TEXT_SEARCH,  # web bundles aren't TEXT_SEARCH
        confidence=0.5,
        text="web source",
    )


# ---------------------------------------------------------------------------
# Signal computation tests
# ---------------------------------------------------------------------------

class TestComputeSignals:
    def test_empty_evidence(self):
        signals = compute_signals([], plan_had_entity_lookup=True, plan_had_text_search=True)
        assert signals.top_text_confidence == 0.0
        assert signals.text_result_count == 0
        assert signals.entity_match_count == 0
        assert signals.entity_lookup_ran is True
        assert signals.text_search_ran is True
        assert signals.confidence_spread == 0.0

    def test_shallow_plan_no_text_search(self):
        bundles = [_entity_bundle(0.9, "eelgrass"), _entity_bundle(0.85, "herring")]
        signals = compute_signals(bundles, plan_had_entity_lookup=True, plan_had_text_search=False)
        assert signals.text_search_ran is False
        assert signals.text_result_count == 0
        assert signals.entity_match_count == 2

    def test_text_bundles_only(self):
        bundles = [_text_bundle(0.8), _text_bundle(0.6), _text_bundle(0.4)]
        signals = compute_signals(bundles, plan_had_entity_lookup=False)
        assert signals.top_text_confidence == 0.8
        assert signals.text_result_count == 3
        assert signals.entity_match_count == 0
        assert signals.entity_lookup_ran is False
        # spread = max(0.8) - median(0.6) = 0.2
        assert signals.confidence_spread == 0.2

    def test_entity_bundles_counted(self):
        bundles = [_entity_bundle(0.9, "e1"), _entity_bundle(0.7, "e2"), _text_bundle(0.5)]
        signals = compute_signals(bundles, plan_had_entity_lookup=True)
        assert signals.entity_match_count == 2
        assert signals.text_result_count == 1
        assert signals.top_text_confidence == 0.5

    def test_spread_requires_three_results(self):
        bundles = [_text_bundle(0.9), _text_bundle(0.1)]
        signals = compute_signals(bundles, plan_had_entity_lookup=False)
        assert signals.confidence_spread == 0.0  # not enough for meaningful spread

    def test_spread_with_many_results(self):
        # 5 results: [0.9, 0.7, 0.5, 0.3, 0.1] → median = 0.5, spread = 0.4
        bundles = [_text_bundle(c) for c in [0.9, 0.7, 0.5, 0.3, 0.1]]
        signals = compute_signals(bundles, plan_had_entity_lookup=False)
        assert signals.confidence_spread == 0.4

    def test_to_dict(self):
        signals = ConfidenceSignals(
            top_text_confidence=0.75,
            text_result_count=8,
            entity_match_count=3,
            entity_lookup_ran=True,
            confidence_spread=0.15,
        )
        d = signals.to_dict()
        assert d["top_text_confidence"] == 0.75
        assert d["text_result_count"] == 8
        assert d["entity_match_count"] == 3
        assert d["entity_lookup_ran"] is True
        assert d["confidence_spread"] == 0.15


# ---------------------------------------------------------------------------
# Gate decision tests
# ---------------------------------------------------------------------------

class TestAssess:
    def test_confident_strong_retrieval(self):
        """Strong top score + enough results = CONFIDENT."""
        signals = ConfidenceSignals(
            top_text_confidence=0.7,
            text_result_count=8,
            entity_match_count=3,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.CONFIDENT

    def test_confident_no_entity_lookup(self):
        """Strong text but no entity lookup step = CONFIDENT (entity signal not advisory)."""
        signals = ConfidenceSignals(
            top_text_confidence=0.6,
            text_result_count=8,
            entity_match_count=0,
            entity_lookup_ran=False,
        )
        assert assess(signals) == GateDecision.CONFIDENT

    def test_low_confidence_weak_top_score(self):
        """Top score below confident threshold = LOW_CONFIDENCE."""
        signals = ConfidenceSignals(
            top_text_confidence=_CONFIDENT_TOP_SCORE - 0.01,
            text_result_count=8,
            entity_match_count=3,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.LOW_CONFIDENCE

    def test_low_confidence_few_results(self):
        """Too few results = LOW_CONFIDENCE even with good top score."""
        signals = ConfidenceSignals(
            top_text_confidence=0.8,
            text_result_count=_MIN_TEXT_RESULTS - 1,
            entity_match_count=3,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.LOW_CONFIDENCE

    def test_low_confidence_zero_entities_with_weak_text(self):
        """Entity lookup ran, zero matches, and text is borderline = LOW_CONFIDENCE."""
        signals = ConfidenceSignals(
            top_text_confidence=_CONFIDENT_TOP_SCORE * 1.1,  # just above confident but below 1.2x
            text_result_count=8,
            entity_match_count=0,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.LOW_CONFIDENCE

    def test_confident_zero_entities_but_strong_text(self):
        """Entity lookup ran, zero matches, but text is very strong = CONFIDENT.
        Entity signal is advisory, not universal penalty."""
        signals = ConfidenceSignals(
            top_text_confidence=_CONFIDENT_TOP_SCORE * 1.5,  # well above 1.2x
            text_result_count=8,
            entity_match_count=0,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.CONFIDENT

    def test_very_low_no_text_results(self):
        """Zero text results = VERY_LOW_CONFIDENCE."""
        signals = ConfidenceSignals(
            top_text_confidence=0.0,
            text_result_count=0,
            entity_match_count=5,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.VERY_LOW_CONFIDENCE

    def test_very_low_weak_score_and_few_results(self):
        """Very weak score + few results = VERY_LOW_CONFIDENCE."""
        signals = ConfidenceSignals(
            top_text_confidence=_LOW_CONFIDENCE_TOP_SCORE - 0.01,
            text_result_count=_MIN_TEXT_RESULTS - 1,
            entity_match_count=0,
            entity_lookup_ran=True,
        )
        assert assess(signals) == GateDecision.VERY_LOW_CONFIDENCE

    def test_low_not_very_low_weak_score_enough_results(self):
        """Weak top score but enough results = LOW not VERY_LOW."""
        signals = ConfidenceSignals(
            top_text_confidence=_LOW_CONFIDENCE_TOP_SCORE + 0.005,
            text_result_count=_MIN_TEXT_RESULTS + 1,
            entity_match_count=0,
            entity_lookup_ran=False,
            text_search_ran=True,
        )
        assert assess(signals) == GateDecision.LOW_CONFIDENCE

    def test_shallow_plan_with_entities_is_confident(self):
        """Shallow plan (no text_search step) with entity results = CONFIDENT."""
        signals = ConfidenceSignals(
            top_text_confidence=0.0,
            text_result_count=0,
            entity_match_count=5,
            entity_lookup_ran=True,
            text_search_ran=False,
        )
        assert assess(signals) == GateDecision.CONFIDENT

    def test_shallow_plan_no_entities_is_very_low(self):
        """Shallow plan, entity_lookup ran but found nothing = VERY_LOW."""
        signals = ConfidenceSignals(
            top_text_confidence=0.0,
            text_result_count=0,
            entity_match_count=0,
            entity_lookup_ran=True,
            text_search_ran=False,
        )
        assert assess(signals) == GateDecision.VERY_LOW_CONFIDENCE

    def test_shallow_plan_no_entity_step_is_confident(self):
        """Shallow plan, no entity_lookup step at all = CONFIDENT (trust planner)."""
        signals = ConfidenceSignals(
            top_text_confidence=0.0,
            text_result_count=0,
            entity_match_count=0,
            entity_lookup_ran=False,
            text_search_ran=False,
        )
        assert assess(signals) == GateDecision.CONFIDENT


# ---------------------------------------------------------------------------
# Integration: compute_signals → assess pipeline
# ---------------------------------------------------------------------------

class TestSignalToGatePipeline:
    def test_strong_retrieval_is_confident(self):
        bundles = [_text_bundle(c) for c in [0.8, 0.7, 0.65, 0.6, 0.55, 0.5, 0.45, 0.4]]
        bundles += [_entity_bundle(0.9, "eelgrass"), _entity_bundle(0.85, "herring")]
        signals = compute_signals(bundles, plan_had_entity_lookup=True)
        assert assess(signals) == GateDecision.CONFIDENT

    def test_empty_retrieval_is_very_low(self):
        signals = compute_signals([], plan_had_entity_lookup=True)
        assert assess(signals) == GateDecision.VERY_LOW_CONFIDENCE

    def test_weak_retrieval_is_low(self):
        """Scores below _CONFIDENT_TOP_SCORE (0.02) = LOW_CONFIDENCE."""
        bundles = [_text_bundle(0.015), _text_bundle(0.012), _text_bundle(0.01)]
        signals = compute_signals(bundles, plan_had_entity_lookup=False)
        assert assess(signals) == GateDecision.LOW_CONFIDENCE


# ---------------------------------------------------------------------------
# Abstention content
# ---------------------------------------------------------------------------

class TestAbstention:
    def test_abstention_answer_is_non_empty(self):
        assert len(ABSTENTION_ANSWER) > 20

    def test_abstention_answer_does_not_mention_error(self):
        """Abstention is not an error — it should be phrased as a quality decision."""
        assert "error" not in ABSTENTION_ANSWER.lower()
        assert "fail" not in ABSTENTION_ANSWER.lower()
