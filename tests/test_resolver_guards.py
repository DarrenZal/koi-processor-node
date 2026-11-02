"""Pure-unit tests for the P1 resolver name guards (2026-07-13).

No DB, no embeddings — these exercise the shared guard functions directly:

  - passes_person_name_guard        (api/resolution_primitives.py)
  - passes_distinctive_token_check  (api/resolution_primitives.py)
  - passes_semantic_match_guard     (api/resolution_primitives.py)
  - passes_token_overlap_strict     (api/resolution_primitives.py — the rich
    Tier-2a gate that now composes the single-word, distinctive-token and
    person guards)

The MUST-REJECT / MUST-STILL-PASS tables mirror the false-merges observed in
production meeting-note processing (see Meta/Entity Resolution Issues.md) plus
the boundary cases called out in the P1 plan. JW values were computed against
the repo's own jaro_winkler_similarity before choosing thresholds; where a
value is load-bearing it is asserted inline.
"""

import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from api.resolution_primitives import (
    passes_person_name_guard,
    passes_distinctive_token_check,
    passes_semantic_match_guard,
    passes_token_overlap_strict,
    normalize_entity_text as norm,
    jaro_winkler_similarity as jw,
)


# ---------------------------------------------------------------------------
# passes_person_name_guard
# ---------------------------------------------------------------------------

PERSON_REJECT = [
    # (query, candidate, reason)
    ("Abel Calloway", "Abel", "multi-vs-single: bare first name"),
    ("Danica Tidewell-Thornbury", "Dan", "multi-vs-single (hyphen -> 3 tokens vs 1)"),
    ("Calla Northwick", "Calla Amberly", "same first, last JW 0.42 < 0.85"),
    ("Saffi Wilsley", "Saffi Wilson", "same first, last JW 0.848 < 0.85 (boundary)"),
    ("Kaia Orrell", "Kaia Tamworth", "same first, last JW 0.43 < 0.85"),
    ("Avery Glenmoor Nettleton", "Avery Penrose", "same first, last JW 0.60 < 0.85"),
]

PERSON_PASS = [
    ("John Smith", "John Smith", "identity"),
]


@pytest.mark.parametrize("a,b,reason", PERSON_REJECT)
def test_person_guard_rejects(a, b, reason):
    assert passes_person_name_guard(norm(a), norm(b)) is False, (
        f"expected REJECT for {a!r} vs {b!r} ({reason})"
    )


@pytest.mark.parametrize("a,b,reason", PERSON_PASS)
def test_person_guard_passes(a, b, reason):
    assert passes_person_name_guard(norm(a), norm(b)) is True, (
        f"expected PASS for {a!r} vs {b!r} ({reason})"
    )


def test_person_guard_boundary_wilsley_wilson():
    """Pin the 0.85 last-token boundary: Wilsley vs Wilson JW is just below."""
    last_jw = jw("wilsley", "wilson")
    assert last_jw < 0.85, f"expected last-token JW < 0.85, got {last_jw:.3f}"
    assert last_jw == pytest.approx(0.848, abs=0.01)


def test_person_guard_single_vs_single_defers_true():
    """Single vs single returns True — the single-word JW>=0.95 rule (in
    passes_token_overlap_strict) is the gate for those, not this guard."""
    assert passes_person_name_guard("kaia", "kevan") is True


# ---------------------------------------------------------------------------
# passes_distinctive_token_check (Org/Project/Concept)
# ---------------------------------------------------------------------------

def test_distinctive_disjoint_rejects():
    # {guelph} vs {melbourne} — disjoint distinctive sets.
    assert passes_distinctive_token_check(
        norm("University of Guelph"), norm("University of Melbourne")
    ) is False


def test_distinctive_superset_rejects():
    # {dweb, cascadia} strict-superset of {dweb}; full JW 0.90 < 0.97.
    assert passes_distinctive_token_check(
        norm("DWeb Camp Cascadia"), norm("DWeb Camp")
    ) is False


def test_distinctive_shared_token_defers_true():
    # Share "region"/"global"; neither disjoint nor superset -> defer to the
    # token-overlap-count rule (which rejects these under Organization).
    assert passes_distinctive_token_check(norm("Region OS"), norm("Region Compass")) is True
    assert passes_distinctive_token_check(norm("Global Culture"), norm("Global Unity")) is True


def test_distinctive_identity_passes():
    assert passes_distinctive_token_check(
        norm("Cedar Grove Clinics"), norm("Cedar Grove Clinics")
    ) is True


# ---------------------------------------------------------------------------
# passes_token_overlap_strict — the composed Tier-2a gate
# (single-word guard + distinctive rule + person guard + count/ratio)
# ---------------------------------------------------------------------------

TOK_REJECT = [
    # Org/Project/Concept
    ("Region OS", "Region Compass", "Organization", "1-token overlap < 2"),
    ("DWeb Camp Cascadia", "DWeb Camp", "Organization", "distinctive superset"),
    ("Sovereign Nature Initiative", "Sovereign AI Factory", "Organization", "1-token overlap"),
    ("University of Guelph", "University of Melbourne", "Organization", "distinctive disjoint"),
    ("Project Cloud Atlas", "CLAUDE", "Project", "single-word guard: JW 0.53"),
    ("Global Culture", "Global Unity", "Organization", "1-token overlap"),
    ("Earth Law Center", "Earth Regeneration Fund", "Organization", "1-token overlap"),
    ("MOVE37", "MOVE37XR", "Project", "strict-prefix-extension (existing rule)"),
    # Person
    ("Kaia Orrell", "Kaia Tamworth", "Person", "person guard last-token"),
    ("Avery Glenmoor Nettleton", "Avery Penrose", "Person", "person guard last-token"),
    ("Calla Northwick", "Calla Amberly", "Person", "person guard last-token"),
    ("Abel Calloway", "Abel", "Person", "person guard multi-vs-single"),
]

TOK_PASS = [
    ("crystallisation", "crystallization", "Concept", "single-word JW 0.97 >= 0.95"),
    ("RiverlinkAI", "Riverlink AI", "Organization", "single-word JW 0.99"),
    ("Cedar Grove Clinics", "Cedar Grove Clinics", "Organization", "identity"),
    ("John Smith", "John Smith", "Person", "identity"),
]


@pytest.mark.parametrize("a,b,etype,reason", TOK_REJECT)
def test_token_overlap_check_rejects(a, b, etype, reason):
    assert passes_token_overlap_strict(norm(a), norm(b), etype) is False, (
        f"expected REJECT [{etype}] {a!r} vs {b!r} ({reason})"
    )


@pytest.mark.parametrize("a,b,etype,reason", TOK_PASS)
def test_token_overlap_check_passes(a, b, etype, reason):
    assert passes_token_overlap_strict(norm(a), norm(b), etype) is True, (
        f"expected PASS [{etype}] {a!r} vs {b!r} ({reason})"
    )


# ---------------------------------------------------------------------------
# passes_semantic_match_guard
# ---------------------------------------------------------------------------

def test_semantic_guard_person_rejects_same_first_name():
    # Person threshold 0.92; even at very high similarity the person guard
    # rejects a differing surname.
    assert passes_semantic_match_guard(
        "Person", norm("Kaia Orrell"), norm("Kaia Tamworth"),
        similarity=0.99, threshold=0.92,
    ) is False


def test_semantic_guard_short_name_needs_bump():
    # Short (<12 char / <=2 token) Concept name just over threshold but below
    # threshold + 0.03 -> rejected.
    assert passes_semantic_match_guard(
        "Concept", norm("herring"), norm("herring"),
        similarity=0.89, threshold=0.88,
    ) is False
    # ... clearing the +0.03 margin passes.
    assert passes_semantic_match_guard(
        "Concept", norm("herring"), norm("herring"),
        similarity=0.92, threshold=0.88,
    ) is True


def test_semantic_guard_distinctive_disjoint_rejects():
    assert passes_semantic_match_guard(
        "Organization", norm("University of Guelph"), norm("University of Melbourne"),
        similarity=0.95, threshold=0.85,
    ) is False


def test_semantic_guard_long_matching_multiword_passes():
    # Long, well-overlapping multi-word org name clears all sub-guards.
    assert passes_semantic_match_guard(
        "Organization", norm("Regenerative Finance Alliance"),
        norm("Regenerative Finance Alliance"),
        similarity=0.95, threshold=0.85,
    ) is True
