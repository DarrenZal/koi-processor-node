"""Measurement defects in the semantic-quality layer — PR #66 review findings M1/M2.

Every test here was written BEFORE its fix and was seen to fail against the
shape it encodes. The failing assertion lines are recorded in the PR thread.

Three of the four defects were in `api/extraction_quality.py` (finding M1) and
share one character: the number reported was internally consistent, came from
the document rather than the output, and was still wrong, because the layer read
a citation that the merge had already widened (a), credited every earlier window
with a later fact (b), or divided distinct-by-total in the one dimension no test
had ever assessed (c). The fourth (M2) is `scripts/ingest_document.py` reading
`quality` from the wrong level of the result dict, so the gate evidence said
`not_evaluated` for every run regardless of what the assessment found.

The existing suite for this module (tests/test_ingest_identity.py
::TestSemanticQuality, ::test_a_thorough_extraction_is_not_penalised_for_being_
thorough) covers the dimensions' intent. This file covers the ways the
measurements diverged from it.
"""

from __future__ import annotations

from api import extraction_quality as eq
from api.extraction_quality import DEFAULT_THRESHOLDS, assess_extraction
from scripts.ingest_document import build_gate_evidence


# ── Fixtures ─────────────────────────────────────────────────────────────────

class _W:
    """A window that knows its chunk band — the extractor's Window, and the
    gate's `_Window` after `_windows_for_coverage`."""

    def __init__(self, index, chunk_indices):
        self.index = index
        self.chunk_indices = chunk_indices
        self.chunk_index_base = min(chunk_indices)


class _BaseOnlyW:
    """A window that knows only its base. `document_window_extractions` stores
    `chunk_index_base` and not the band, so any caller that hands the plan rows
    over without reconstructing bands produces exactly this shape."""

    def __init__(self, index, base):
        self.index = index
        self.chunk_index_base = base
        self.chunk_indices = None


def _doc(n):
    """n chunks, n one-chunk windows, distinctive tokens per chunk."""
    chunks = {i: f"chunk {i} alpha{i} beta{i} gamma{i} content words here" for i in range(n)}
    windows = [_W(i, [i]) for i in range(n)]
    return chunks, windows


def _supported_facts(n):
    """One supported fact per chunk, distinct predicates — passes every fact
    dimension at either tier, so a test can isolate the dimension it is about."""
    return [{"subject": f"alpha{i}", "object": f"beta{i}", "predicate": f"PRED_{i}",
             "fact_text": f"alpha{i} relates to beta{i}", "chunk_range": [i, i]}
            for i in range(n)]


_VERIFIED = {"mode": "strict",
             "verification": {"binding_verified": True,
                              "checked_facts": 8, "offending_facts": 0}}


def _r(x):
    """Dimension values are rounded to 4 places; compare at that precision."""
    return round(x, 4)


def _names(report):
    return [d.name for d in report.dimensions]


def _dim(report, name):
    hit = [d for d in report.dimensions if d.name == name]
    assert hit, f"no dimension {name!r} in {_names(report)}"
    return hit[0]


# ═══════════════════════════════════════════════════════════════════════════
# M1a — the widened chunk_range is not a citation
# ═══════════════════════════════════════════════════════════════════════════

class TestChunkCoverageReadsThePerWindowRanges:

    def test_a_fact_seen_in_two_windows_cites_two_chunks_not_the_span_between(self):
        """document:ed30e680 reported chunk_coverage 456/456 where 173 of 456
        chunks were cited. `merge_extractions` widens `chunk_range` to
        [min lo, max hi] across the windows a fact appeared in, and the layer read
        that span as a citation of every chunk inside it. A fact seen in window 1
        and window 12 was credited with citing everything in between.

        The merge now also emits `chunk_ranges`, the ranges the windows actually
        cited. When it is present, that is what a citation IS.
        """
        chunks, windows = _doc(6)
        fact = {"subject": "alpha0", "object": "beta0", "predicate": "P",
                "fact_text": "t", "chunk_range": [0, 5], "chunk_ranges": [[0, 0], [5, 5]]}
        report = assess_extraction(merged={"facts": [fact]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        cov = _dim(report, "chunk_coverage")
        assert cov.value == _r(2 / 6), cov.detail
        assert cov.evidence["uncited_chunks"] == [1, 2, 3, 4]

    def test_a_legacy_payload_with_only_chunk_range_is_read_as_before(self):
        """CONTROL. The behaviour is chosen by the data present, not by a flag:
        the gate's un-merged payloads and cached payloads written before the
        merge emitted `chunk_ranges` carry only `chunk_range`, and for those the
        single range is the whole citation. Same fact, list removed — the span is
        credited in full, as it always was.
        """
        chunks, windows = _doc(6)
        fact = {"subject": "alpha0", "object": "beta0", "predicate": "P",
                "fact_text": "t", "chunk_range": [0, 5]}
        report = assess_extraction(merged={"facts": [fact]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert _dim(report, "chunk_coverage").value == _r(6 / 6)

    def test_window_coverage_and_in_range_read_the_same_ranges(self):
        """The other citation readers must not keep the old view of the fact.
        Two one-chunk windows out of six were touched, so 2 of 6 windows are
        covered; and a fabricated range in the list is caught even though the
        widened span the merge would carry for it is clean."""
        chunks, windows = _doc(6)
        seen_twice = {"subject": "alpha0", "object": "beta0", "predicate": "P",
                      "fact_text": "t", "chunk_range": [0, 5],
                      "chunk_ranges": [[0, 0], [5, 5]]}
        report = assess_extraction(merged={"facts": [seen_twice]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert _dim(report, "window_coverage").value == _r(2 / 6)

        # Per-window ranges [0,0] and [5,3]: the merge widens these to [0,5] —
        # in range, well-formed — while one of the ranges it was built from is not.
        malformed = {"subject": "alpha0", "object": "beta0", "predicate": "P",
                     "fact_text": "t", "chunk_range": [0, 5],
                     "chunk_ranges": [[0, 0], [5, 3]]}
        report = assess_extraction(merged={"facts": [malformed]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert _dim(report, "citation_in_range").value == 0.0

    def test_citation_support_holds_if_any_cited_span_supports_the_fact(self):
        """A fact the merge saw in two windows is supported if EITHER window's
        span contains its endpoint language. The widened span happened to make
        this pass by accident (the union of six chunks contains everything); the
        per-window reading must not turn a genuinely supported fact into an
        unsupported one because its OTHER sighting was a weaker span."""
        chunks, windows = _doc(6)
        # Endpoint language lives in chunk 5 only; chunk 0 is a second, thinner sighting.
        fact = {"subject": "alpha5", "object": "beta5", "predicate": "P",
                "fact_text": "t", "chunk_range": [0, 5], "chunk_ranges": [[0, 0], [5, 5]]}
        report = assess_extraction(merged={"facts": [fact]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert _dim(report, "citation_support").value == 1.0

        nowhere = {"subject": "zeta", "object": "omega", "predicate": "P",
                   "fact_text": "t", "chunk_range": [0, 5], "chunk_ranges": [[0, 0], [5, 5]]}
        report = assess_extraction(merged={"facts": [nowhere]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert _dim(report, "citation_support").value == 0.0


# ═══════════════════════════════════════════════════════════════════════════
# M1b — a fact at chunk 100 does not cover the windows before it
# ═══════════════════════════════════════════════════════════════════════════

class TestWindowCoverageWithoutChunkIndices:

    def test_a_late_fact_covers_only_the_window_whose_band_it_falls_in(self):
        """document:ed30e680 reported window_coverage 16/16 where 10 of 16 windows
        overlapped a citation. For a window lacking `chunk_indices` the fallback
        was `cr[0] >= base` — true for EVERY window whose base is at or before the
        citation, so one fact at chunk 25 marked the windows at bases 0, 10 and 20
        all covered.

        A window's band is [its base, the next window's base), the last window
        open-ended. A window is covered only if a cited range intersects its band.
        """
        chunks = {i: f"chunk {i} text" for i in range(30)}
        windows = [_BaseOnlyW(0, 0), _BaseOnlyW(1, 10), _BaseOnlyW(2, 20)]
        fact = {"subject": "s", "object": "o", "predicate": "P",
                "fact_text": "t", "chunk_range": [25, 25]}
        report = assess_extraction(merged={"facts": [fact]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        cov = _dim(report, "window_coverage")
        assert cov.value == _r(1 / 3), cov.detail
        assert cov.evidence["uncovered_windows"] == [0, 1]

    def test_a_range_straddling_two_bands_covers_both_and_no_others(self):
        chunks = {i: f"chunk {i} text" for i in range(30)}
        windows = [_BaseOnlyW(0, 0), _BaseOnlyW(1, 10), _BaseOnlyW(2, 20)]
        fact = {"subject": "s", "object": "o", "predicate": "P",
                "fact_text": "t", "chunk_range": [8, 12]}
        report = assess_extraction(merged={"facts": [fact]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        cov = _dim(report, "window_coverage")
        assert cov.value == _r(2 / 3), cov.detail
        assert cov.evidence["uncovered_windows"] == [2]

    def test_windows_with_chunk_indices_keep_the_intersection_test(self):
        """Windows WITH `chunk_indices` (the extractor's own) use the intersection
        test — and were over-credited too, which this test found: the base
        fallback sat in an `elif` under the intersection test, so a window whose
        indices did not intersect fell through and was credited by base anyway.
        One fact at chunk 4 reported 5 of 6 windows covered, not 1 of 6."""
        chunks, windows = _doc(6)
        fact = {"subject": "s", "object": "o", "predicate": "P",
                "fact_text": "t", "chunk_range": [4, 4]}
        report = assess_extraction(merged={"facts": [fact]}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert _dim(report, "window_coverage").value == _r(1 / 6)


# ═══════════════════════════════════════════════════════════════════════════
# M1c — discourse_variety was distinct-over-total, on a tier no test assessed
# ═══════════════════════════════════════════════════════════════════════════

def _moves(counts: dict) -> list:
    out = []
    for move_type, n in counts.items():
        out += [{"move_type": move_type, "title": f"{move_type} {i}", "chunk_range": [0, 0]}
                for i in range(n)]
    return out


_EIGHT_TYPES = ("thesis", "claim", "evidence", "premise",
                "counterpoint", "open_question", "definition", "implication")


class TestDiscourseConcentration:
    """`discourse_variety` was `distinct move types / total moves`, capped at 8
    types by the taxonomy — the very distinct-over-total shape the module header
    says it removed for predicates, and it survived because it lives only on the
    `thorough` tier and no test had assessed that tier. A 480-move book scores
    8/480 = 0.017 and FAILS for being long.

    Its replacement is concentration: the share of moves carried by the single
    most common `move_type`. Higher is worse, like `predicate_concentration`.
    """

    def _thorough(self, moves):
        chunks, windows = _doc(8)
        return assess_extraction(
            merged={"facts": _supported_facts(8)}, chunks_by_index=chunks,
            windows=windows, tier="thorough", discourse_moves=moves,
            identity_evidence=_VERIFIED)

    def test_positive_control_a_long_varied_argument_passes(self):
        """480 moves spread over all eight types (each 12.5%, well under 30%),
        full coverage, every fact supported. This is the book the old dimension
        failed."""
        report = self._thorough(_moves({t: 60 for t in _EIGHT_TYPES}))
        assert report.status == "pass", report.as_dict()["failed"]
        dim = _dim(report, "discourse_concentration")
        assert dim.status == "pass", (dim.detail, dim.value)
        assert dim.value == _r(60 / 480)
        assert dim.evidence["moves_total"] == 480
        assert dim.evidence["distinct_move_types"] == 8

    def test_negative_control_one_move_type_repeated_for_volume_fails(self):
        report = self._thorough(_moves({"claim": 480}))
        dim = _dim(report, "discourse_concentration")
        assert dim.value == 1.0
        assert dim.status == "fail"
        assert "discourse_concentration" in report.as_dict()["failed"]
        assert report.status == "fail"

    def test_between_the_thresholds_routes_to_review(self):
        # 0.70 < 0.80 <= 0.90: the review band, per the starting thresholds.
        report = self._thorough(_moves({"claim": 384, "evidence": 96}))
        dim = _dim(report, "discourse_concentration")
        assert dim.value == _r(0.80)
        assert dim.status == "review"

    def test_unmeasurable_control_too_few_moves_is_review_never_pass_or_fail(self):
        """Three moves cannot tell concentration from coincidence — three moves
        of one type is 1.0 and would FAIL a sound short extraction; three of
        three types is 0.33 and would PASS on no evidence. Under five, the value
        is None, and None routes to REVIEW."""
        for moves in (_moves({"claim": 3}), _moves({"claim": 1, "evidence": 1, "thesis": 1})):
            report = self._thorough(moves)
            dim = _dim(report, "discourse_concentration")
            assert dim.value is None, moves
            assert dim.status == "review"
            assert "too few moves" in dim.detail
            assert report.status == "review"
        # Five is enough to measure.
        assert _dim(self._thorough(_moves({"claim": 5})), "discourse_concentration").value == 1.0

    def test_the_dimension_exists_on_thorough_only_and_variety_is_gone(self):
        chunks, windows = _doc(8)
        thorough = self._thorough(_moves({t: 60 for t in _EIGHT_TYPES}))
        standard = assess_extraction(
            merged={"facts": _supported_facts(8)}, chunks_by_index=chunks,
            windows=windows, tier="standard", identity_evidence=_VERIFIED)
        assert "discourse_concentration" in _names(thorough)
        assert "discourse_concentration" not in _names(standard)
        assert "discourse_variety" not in _names(thorough)
        assert "discourse_variety" not in _names(standard)
        assert "discourse_variety" not in DEFAULT_THRESHOLDS["thorough"]
        assert "discourse_variety" not in DEFAULT_THRESHOLDS["standard"]

    def test_the_contract_version_moved_because_the_dimension_set_did(self):
        """Stored `semantic_report` rows carry the version; a v1 row has a
        `discourse_variety` dimension and a v2 row does not, and a reader
        comparing them across runs needs to be able to tell."""
        assert eq.QUALITY_CONTRACT_VERSION == "extraction-quality-v2"
        report = self._thorough(_moves({"claim": 480}))
        assert report.contract_version == "extraction-quality-v2"
        assert report.as_dict()["contract_version"] == "extraction-quality-v2"


# ═══════════════════════════════════════════════════════════════════════════
# M2 — the gate evidence read `quality` from the wrong level
# ═══════════════════════════════════════════════════════════════════════════

class TestGateEvidenceReadsQualityFromTheExtractResult:
    """`build_gate_evidence` read `result["quality"]`, but the extractor returns
    `quality` inside the dict `ingest_path` stores under `result["extract"]` —
    every sibling key in the same block reads `ext.get(...)`. So `semantic_status`
    was `not_evaluated` and `semantic_ok` 0 on every run, including runs whose
    assessment had passed, and the gate had no way to see the difference."""

    @staticmethod
    def _result(extract):
        return {"tier": "thorough", "document_rid": "d", "rag": {},
                "extract": extract, "claims": {}}

    def test_a_passing_assessment_reaches_the_gate(self):
        ev = build_gate_evidence(self._result({"quality": {"status": "pass"}}))
        assert ev["semantic_status"] == "pass"
        assert ev["semantic_ok"] == 1

    def test_review_is_not_ok(self):
        ev = build_gate_evidence(self._result({"quality": {"status": "review"}}))
        assert ev["semantic_status"] == "review"
        assert ev["semantic_ok"] == 0

    def test_an_absent_assessment_stays_not_evaluated(self):
        ev = build_gate_evidence(self._result({}))
        assert ev["semantic_status"] == "not_evaluated"
        assert ev["semantic_ok"] == 0
        # And with no extract stage at all (rag tier).
        ev = build_gate_evidence({"tier": "rag", "document_rid": "d", "rag": {}})
        assert ev["semantic_status"] == "not_evaluated"
        assert ev["semantic_ok"] == 0


# ═══════════════════════════════════════════════════════════════════════════
# endpoint_integrity for a check-only run (the operator gate has no graph)
# ═══════════════════════════════════════════════════════════════════════════

class TestEndpointIntegrityCheckOnly:
    """The operator gate resolves the payload's endpoints without persisting
    anything, so there is no `verification` block — there was nothing written to
    verify. It reports the resolution's bijection instead, under `check_only`.
    A real run's `verification` wins when both are present; a check-only run
    must never fabricate a `verification` block to be read."""

    def _report(self, check_only):
        chunks, windows = _doc(4)
        return assess_extraction(
            merged={"facts": _supported_facts(4)}, chunks_by_index=chunks,
            windows=windows, tier="standard",
            identity_evidence={"mode": "strict", "check_only": check_only})

    def test_a_bijective_check_only_resolution_passes(self):
        report = self._report({"bijective": True, "resolved_endpoints": 8,
                               "distinct_uris": 8, "would_preregister": 2})
        dim = _dim(report, "endpoint_integrity")
        assert dim.value == 1.0
        assert dim.status == "pass"
        assert "check-only" in dim.detail
        assert "8 resolved endpoint(s) -> 8 distinct URI(s)" in dim.detail
        assert "2 would be pre-registered" in dim.detail
        assert dim.evidence == {"identity_mode": "strict", "scope": "check_only"}

    def test_a_non_bijective_check_only_resolution_fails(self):
        report = self._report({"bijective": False, "resolved_endpoints": 8,
                               "distinct_uris": 7, "would_preregister": 0})
        dim = _dim(report, "endpoint_integrity")
        assert dim.value == 0.0
        assert dim.status == "fail"
        assert "check-only" in dim.detail

    def test_a_real_verification_wins_over_check_only(self):
        chunks, windows = _doc(4)
        report = assess_extraction(
            merged={"facts": _supported_facts(4)}, chunks_by_index=chunks,
            windows=windows, tier="standard",
            identity_evidence={"mode": "strict",
                               "verification": {"binding_verified": False,
                                                "checked_facts": 4, "offending_facts": 1},
                               "check_only": {"bijective": True, "resolved_endpoints": 8,
                                              "distinct_uris": 8, "would_preregister": 0}})
        dim = _dim(report, "endpoint_integrity")
        assert dim.value == 0.0
        assert "check-only" not in dim.detail
        assert dim.evidence.get("scope") != "check_only"
