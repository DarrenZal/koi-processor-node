from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

import pytest

from api import resolver_shadow
from api.resolution_primitives import (
    normalize_entity_text,
    passes_token_overlap_legacy,
    passes_token_overlap_strict,
)


def _load_analyzer():
    path = Path(__file__).parents[1] / "scripts" / "analyze_resolver_shadow.py"
    spec = importlib.util.spec_from_file_location("analyze_resolver_shadow", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


def test_policy_extraction_preserves_meeting_date_guard_in_strict_policy():
    a = normalize_entity_text("2026-01-27 Landscape Hub Cultivator Meeting")
    b = normalize_entity_text("2026-02-10 Landscape Hub Cultivator Meeting")
    assert passes_token_overlap_strict(a, b, "Meeting") is False
    assert passes_token_overlap_legacy(a, b, "Meeting") is True


def test_shadow_records_counterfactual_without_changing_active_result(monkeypatch):
    emitted = []
    monkeypatch.setattr(resolver_shadow, "emit_nonblocking", emitted.append)
    attempt = resolver_shadow.start_attempt(
        caller="test",
        engine="shared_multi_tier",
        entity_type="Meeting",
        query_norm="2026 01 27 landscape meeting",
        active_policy="legacy",
        sampled_override=True,
    )
    attempt.observe_candidate(
        uri="urn:old-meeting",
        score=0.96,
        tier="fuzzy",
        legacy_accepts=True,
        strict_accepts=False,
        elapsed_ns=1000,
    )
    record = attempt.finish(
        active_uri="urn:old-meeting",
        active_outcome="fuzzy",
        legacy_fallback="unresolved",
        strict_fallback="fallthrough_unobserved",
    )
    assert record["outcome_diverged"] is True
    assert record["legacy_uri"] == "urn:old-meeting"
    assert record["strict_uri"] == "fallthrough_unobserved"
    assert emitted == [record]


def test_disabled_shadow_attempt_is_inert(monkeypatch):
    monkeypatch.setenv("KOI_RESOLVER_SHADOW_ENABLED", "false")
    attempt = resolver_shadow.start_attempt(
        caller="test",
        engine="test",
        entity_type="Concept",
        query_norm="alpha",
        active_policy="legacy",
    )
    assert attempt.sampled is False
    assert attempt.finish(
        active_uri=None,
        active_outcome="unresolved",
        legacy_fallback="unresolved",
        strict_fallback="unresolved",
    ) is None


def test_counterfactual_fuzzy_selection_survives_active_semantic_fallthrough(monkeypatch):
    emitted = []
    monkeypatch.setattr(resolver_shadow, "emit_nonblocking", emitted.append)
    attempt = resolver_shadow.start_attempt(
        caller="test",
        engine="personal_ingest",
        entity_type="Concept",
        query_norm="alpha practice",
        active_policy="strict_fuzzy+legacy_semantic",
        sampled_override=True,
    )
    attempt.observe_candidate(
        uri="urn:legacy-fuzzy",
        score=0.91,
        tier="fuzzy",
        legacy_accepts=True,
        strict_accepts=False,
        elapsed_ns=1000,
    )
    attempt.observe_candidate(
        uri="urn:active-semantic",
        score=0.90,
        tier="semantic",
        legacy_accepts=True,
        strict_accepts=True,
        elapsed_ns=1000,
    )
    record = attempt.finish(
        active_uri="urn:active-semantic",
        active_outcome="semantic",
        legacy_fallback="create",
        strict_fallback="create",
    )
    assert record["legacy_uri"] == "urn:legacy-fuzzy"
    assert record["strict_uri"] == "urn:active-semantic"
    assert record["outcome_diverged"] is True


def test_analyzer_treats_divergence_as_successful_policy_split():
    analyzer = _load_analyzer()
    start = datetime.now(timezone.utc) - timedelta(days=8)
    records = []
    for index in range(1000):
        records.append({
            "observed_at": (start + timedelta(minutes=index * 12)).isoformat(),
            "caller": "knowledge",
            "entity_type": "Concept",
            "outcome_diverged": index == 0,
            "candidate_divergences": 1 if index == 0 else 0,
            "shadow_overhead_ms": 1,
            "resolver_elapsed_ms": 100,
        })
    report, exit_code = analyzer.analyze(
        records,
        minimum_attempts=1000,
        minimum_days=7,
        expected_callers={"knowledge"},
        fixture_callers=set(),
        max_overhead_ratio=0.05,
    )
    assert exit_code == 3
    assert report["verdict"] == "explicit_policy_split"


def test_fixture_coverage_unblocks_zero_traffic_caller():
    analyzer = _load_analyzer()
    now = datetime.now(timezone.utc)
    records = [
        {
            "observed_at": (now - timedelta(days=8)).isoformat(),
            "caller": "knowledge",
            "entity_type": "Concept",
            "outcome_diverged": False,
            "candidate_divergences": 0,
            "shadow_overhead_ms": 1,
            "resolver_elapsed_ms": 100,
        },
        *[
            {
                "observed_at": now.isoformat(),
                "caller": "knowledge",
                "entity_type": "Concept",
                "outcome_diverged": False,
                "candidate_divergences": 0,
                "shadow_overhead_ms": 1,
                "resolver_elapsed_ms": 100,
            }
            for _ in range(999)
        ],
    ]
    report, exit_code = analyzer.analyze(
        records,
        minimum_attempts=1000,
        minimum_days=7,
        expected_callers={"knowledge", "bundle"},
        fixture_callers={"bundle"},
        max_overhead_ratio=0.05,
    )
    assert exit_code == 0
    assert report["missing_callers"] == []
