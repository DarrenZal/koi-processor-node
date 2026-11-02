"""Semantic-quality assessment for a document extraction — separate from completion.

Issue #64. The document-ingest completion gate establishes that every stage ran and
met a structural floor. Its own catalog says so: "green = every stage ran and met its
structural floor, NOT the extracted knowledge is good. A gibberish document that
chunks and yields >= 1 fact passes strict."

That is not a hypothetical. The original Buehler (2024) run passed the strict gate
with 25 facts and 25 discourse moves; a curated re-run of the *same document* yielded
115 entity endpoints, 213 facts and 52 discourse moves. Both were "complete". An
operator reading a green gate could not tell them apart.

THE DESIGN CONSTRAINT THAT SHAPES EVERYTHING HERE
-------------------------------------------------
#64 requirement 5: "avoid inert count-only thresholds that reward hallucinated
volume." A floor like `facts >= 40` is satisfied just as well by forty invented facts
as by forty real ones — and an extractor that pads is *easier* to satisfy than one
that is careful. So no dimension below is a raw count. Every one is a RATIO whose
denominator is a property of the source document, and the two load-bearing ones
(coverage, citation support) get WORSE when output is padded:

  - coverage divides by the document's own window count, so extracting the intro
    forty times over does not improve it;
  - citation support divides by the number of facts, so an unsupported fact lowers
    the score it was meant to raise.

Everything is computed deterministically from the payload and the source text. No
model is called: a model grading another model's output would make the measurement
depend on the thing being measured, and would cost a provider call per document.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

logger = logging.getLogger(__name__)

# v2 (2026-09-14): the DIMENSION SET changed, not just a threshold. `discourse_variety`
# (distinct move types / moves) is gone and `discourse_concentration` replaces it, and
# every citation-reading dimension now reads the per-window `chunk_ranges` list where
# the merge supplies one. Stored `semantic_report` rows (document_extraction_runs,
# migration 125) carry this string, so a v1 row and a v2 row for the same document are
# not comparable dimension-for-dimension — and the version is how a reader tells.
QUALITY_CONTRACT_VERSION = "extraction-quality-v2"

STATUS_PASS = "pass"
STATUS_REVIEW = "review"
STATUS_FAIL = "fail"
STATUS_NOT_EVALUATED = "not_evaluated"

# Tokens too common to be evidence that a fact is grounded in its cited span.
_STOPWORDS = frozenset("""
a an and are as at be by for from has have in is it its of on or that the their this
to was were will with which who whom whose then than these those there here we our
""".split())

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _content_tokens(text: Optional[str]) -> set:
    if not text:
        return set()
    return {t for t in _TOKEN_RE.findall(text.lower())
            if t not in _STOPWORDS and len(t) > 2}


def _cited_ranges(fact: dict) -> list:
    """The spans a fact actually cites, as (lo, hi) pairs.

    `merge_extractions` widens `chunk_range` to [min lo, max hi] across every
    window a fact appeared in, and ALSO emits `chunk_ranges`, the ranges those
    windows cited. The widened span is not a citation: a fact seen in window 1 and
    window 12 did not cite the ten windows in between, and reading it as though it
    had is how document:ed30e680 reported chunk_coverage 456/456 with 173 chunks
    cited (review finding M1). Every dimension that reads a citation reads THIS.

    When only `chunk_range` is present — the gate's un-merged payloads, cached
    payloads written before the merge emitted the list — the single range is the
    whole citation, as before. The behaviour is chosen by the data present, not by
    a flag, so an older payload measures exactly as it did.
    """
    ranges = fact.get("chunk_ranges") or ([fact["chunk_range"]] if fact.get("chunk_range") else [])
    out = []
    for cr in ranges:
        if not cr:
            continue
        out.append((cr[0], cr[-1] if len(cr) > 1 else cr[0]))
    return out


@dataclass
class Dimension:
    """One quality dimension: a verdict, the number behind it, and the evidence."""
    name: str
    status: str
    value: Optional[float]
    threshold: Optional[float]
    detail: str
    evidence: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "name": self.name, "status": self.status, "value": self.value,
            "threshold": self.threshold, "detail": self.detail,
            "evidence": self.evidence,
        }


@dataclass
class QualityReport:
    status: str
    dimensions: list
    tier: str
    contract_version: str = QUALITY_CONTRACT_VERSION
    waiver: Optional[dict] = None

    def as_dict(self) -> dict:
        return {
            "status": self.status,
            "tier": self.tier,
            "contract_version": self.contract_version,
            "dimensions": [d.as_dict() for d in self.dimensions],
            "waiver": self.waiver,
            "failed": [d.name for d in self.dimensions if d.status == STATUS_FAIL],
            "review": [d.name for d in self.dimensions if d.status == STATUS_REVIEW],
        }


# Per-tier thresholds. `fail_below` is a hard floor; between `fail_below` and
# `review_below` the dimension enters REVIEW rather than passing silently.
#
# These are STARTING values, chosen against the two Buehler runs (the thin original
# and the curated re-run) and stated as such. They are not derived from a labelled
# corpus, because there isn't one yet — #64 requirement 6 asks for a small labelled
# audit set and shadow evaluation before automatic promotion, and that is exactly
# what should move these numbers. Until then the honest posture is: this layer
# reports and routes to review; it is not the arbiter of promotion.
DEFAULT_THRESHOLDS = {
    "standard": {
        "window_coverage":   {"fail_below": 0.50, "review_below": 0.80},
        "chunk_coverage":    {"fail_below": 0.40, "review_below": 0.75},
        "citation_support":  {"fail_below": 0.60, "review_below": 0.85},
        "citation_in_range": {"fail_below": 0.95, "review_below": 1.00},
        "endpoint_integrity": {"fail_below": 1.00, "review_below": 1.00},
        "predicate_concentration": {"higher_is_better": False,
                                    "fail_above": 0.75, "review_above": 0.60},
    },
    "thorough": {
        "window_coverage":   {"fail_below": 0.60, "review_below": 0.85},
        "chunk_coverage":    {"fail_below": 0.40, "review_below": 0.75},
        "citation_support":  {"fail_below": 0.60, "review_below": 0.85},
        "citation_in_range": {"fail_below": 0.95, "review_below": 1.00},
        "endpoint_integrity": {"fail_below": 1.00, "review_below": 1.00},
        "predicate_concentration": {"higher_is_better": False,
                                    "fail_above": 0.75, "review_above": 0.60},
        # Starting values, like everything else in this table. 0.90 is a dump —
        # nine moves in ten of one type; above 0.70 a reader should look.
        "discourse_concentration": {"higher_is_better": False,
                                    "fail_above": 0.90, "review_above": 0.70},
    },
}

# Below this many moves, discourse concentration is not measured (value None →
# REVIEW). Three moves of one type is 1.0 and would fail a sound short extraction;
# three of three types is 0.33 and would pass on no evidence.
_MIN_MOVES_TO_MEASURE = 5


def _verdict(value: Optional[float], th: Optional[dict]) -> str:
    """Verdict for a dimension. `th["higher_is_better"]` defaults True.

    The direction has to be explicit. `predicate_concentration` is the one
    dimension where a HIGH number is the bad news, and an earlier version of this
    module applied the higher-is-better comparison to a concentration-shaped
    measure — which is how it ended up scoring a 213-fact extraction WORSE than a
    25-fact one on the real fixture.
    """
    if th is None:
        return STATUS_NOT_EVALUATED
    if value is None:
        # A dimension that could not be measured is NOT a pass. This is the
        # difference between "we checked and it was fine" and "we could not check",
        # and collapsing them is how an unmeasured thing starts reading as a good
        # one.
        return STATUS_REVIEW
    if th.get("higher_is_better", True):
        if value < th["fail_below"]:
            return STATUS_FAIL
        if value < th["review_below"]:
            return STATUS_REVIEW
        return STATUS_PASS
    if value > th["fail_above"]:
        return STATUS_FAIL
    if value > th["review_above"]:
        return STATUS_REVIEW
    return STATUS_PASS


def assess_extraction(
    *,
    merged: dict,
    chunks_by_index: dict,
    windows: Sequence[Any],
    tier: str,
    discourse_moves: Optional[Sequence[dict]] = None,
    identity_evidence: Optional[dict] = None,
    thresholds: Optional[dict] = None,
    waiver: Optional[dict] = None,
) -> QualityReport:
    """Assess one extraction. Deterministic; calls no model.

    `chunks_by_index` maps GLOBAL chunk_index -> chunk text. It is a mapping, not a
    list, deliberately: the extractor emits global chunk coordinates and a document's
    chunk indices need neither start at 0 nor be contiguous, so list position is not
    the same thing as chunk index. Treating them as the same would silently compare
    each fact against the wrong span and report a citation-support number that looks
    plausible and means nothing.

    `windows` carries the document's window plan, which supplies the coverage
    denominator.
    """
    th_all = thresholds or DEFAULT_THRESHOLDS
    th = th_all.get(tier) or {}
    facts = merged.get("facts") or []
    moves = list(discourse_moves or [])
    dims: list = []

    # ── 1. Window coverage ──────────────────────────────────────────────────
    # What fraction of the document's windows contributed at least one fact?
    # This is the check that catches the thin run: an extraction that only really
    # read the introduction scores low here no matter how many facts it produced,
    # because the denominator is the document, not the output.
    n_windows = len(windows) or 1

    # A window that carries `chunk_indices` (the extractor's Window, the gate's
    # reconstructed `_Window`) is covered if a cited range intersects them. A
    # window that carries only `chunk_index_base` gets a band derived from the
    # sorted bases — [its base, the next base), the last window open-ended — and
    # is covered only if a cited range intersects that band.
    #
    # The earlier fallback for base-only windows was `cr[0] >= base`, which is
    # true of EVERY window at or before the citation: one fact at chunk 100 marked
    # the windows based at 0, 10, 20 ... all covered, and document:ed30e680
    # reported 16/16 where 10 windows overlapped a citation (review finding M1).
    # It also sat in an `elif` under the intersection test, so a window WITH
    # indices that did not intersect fell through to it and was credited anyway.
    bases = sorted({getattr(w, "chunk_index_base", None) for w in windows} - {None})

    def _window_covers(w, lo: int, hi: int) -> bool:
        idxs = getattr(w, "chunk_indices", None)
        if idxs:
            return any(lo <= c <= hi for c in idxs)
        base = getattr(w, "chunk_index_base", None)
        if base is None:
            return False
        nxt = next((b for b in bases if b > base), None)     # band is [base, nxt)
        return hi >= base and (nxt is None or lo < nxt)

    covered = set()
    for f in facts:
        for lo, hi in _cited_ranges(f):
            for w in windows:
                if _window_covers(w, lo, hi):
                    covered.add(getattr(w, "index", None))
    covered.discard(None)
    coverage = len(covered) / n_windows
    dims.append(Dimension(
        name="window_coverage", status=_verdict(coverage, th.get("window_coverage")),
        value=round(coverage, 4), threshold=(th.get("window_coverage") or {}).get("fail_below"),
        detail=f"{len(covered)} of {n_windows} windows produced at least one fact",
        evidence={"windows_total": n_windows, "windows_with_facts": len(covered),
                  "uncovered_windows": sorted(
                      {getattr(w, "index", i) for i, w in enumerate(windows)} - covered)[:20]},
    ))

    # ── 1b. Chunk coverage ─────────────────────────────────────────────────
    # What fraction of the document's CHUNKS is cited by at least one fact? This is
    # the depth metric, and on the real fixtures it is the one that actually
    # separates a thin extraction from a thorough one: the original Buehler run
    # scores 0.21 and the curated re-run 0.93.
    #
    # window_coverage alone could not tell them apart — both scored 1.0, because the
    # thin run touched all six windows, just shallowly. "Read every section" and
    # "read every section properly" are different claims and need different
    # measurements.
    #
    # Padding cannot inflate this: twenty invented facts all citing chunk 0 add one
    # chunk to the numerator, the same as one fact would, while the denominator is
    # the document and does not move.
    #
    # Neither can the merge: the per-window ranges are what count (see
    # `_cited_ranges`), so a fact seen at chunk 1 and chunk 400 cites two chunks,
    # not four hundred.
    n_chunks = len(chunks_by_index)
    known = set(chunks_by_index)
    lo_bound = min(known) if known else 0
    hi_bound = max(known) if known else 0
    cited_chunks: set = set()
    for f in facts:
        for lo, hi in _cited_ranges(f):
            for ci in range(max(lo, lo_bound), min(hi, hi_bound) + 1):
                if ci in chunks_by_index:
                    cited_chunks.add(ci)
    chunk_cov = (len(cited_chunks) / len(chunks_by_index)) if chunks_by_index else None
    dims.append(Dimension(
        name="chunk_coverage", status=_verdict(chunk_cov, th.get("chunk_coverage")),
        value=round(chunk_cov, 4) if chunk_cov is not None else None,
        threshold=(th.get("chunk_coverage") or {}).get("fail_below"),
        detail=f"{len(cited_chunks)} of {len(chunks_by_index)} source chunks are cited "
               f"by at least one fact",
        evidence={"uncited_chunks": sorted(set(chunks_by_index) - cited_chunks)[:25]},
    ))

    # ── 2. Citation in-range ────────────────────────────────────────────────
    # Every fact claims a chunk_range. A range outside the document's chunk count
    # is a fabricated citation — cheap to detect, and it should essentially never
    # happen, hence a 0.95 floor rather than a lenient one.
    #
    # A fact is in range only if EVERY span it cites is: one fabricated citation
    # among several sightings is still a fabricated citation. The widened span
    # cannot stand in for this — [min lo, max hi] of ([0,0], [5,3]) is a clean
    # [0,5], while one of the ranges it was built from is malformed.
    in_range = 0
    bad_ranges = []
    for f in facts:
        ranges = _cited_ranges(f)
        offending = [[lo, hi] for lo, hi in ranges
                     if lo < lo_bound or hi > hi_bound or lo > hi]
        if not ranges or offending:
            bad_ranges.append({"fact_text": (f.get("fact_text") or "")[:120],
                               "chunk_range": f.get("chunk_range") or [],
                               "offending": offending})
        else:
            in_range += 1
    cite_range = (in_range / len(facts)) if facts else None
    dims.append(Dimension(
        name="citation_in_range", status=_verdict(cite_range, th.get("citation_in_range")),
        value=round(cite_range, 4) if cite_range is not None else None,
        threshold=(th.get("citation_in_range") or {}).get("fail_below"),
        detail=f"{in_range} of {len(facts)} facts cite a chunk range inside the document "
               f"({lo_bound}..{hi_bound})",
        evidence={"out_of_range": bad_ranges[:10], "chunks_total": n_chunks},
    ))

    # ── 3. Citation support ─────────────────────────────────────────────────
    # Does the cited span actually contain the fact's subject/object language? This
    # is the dimension that makes padding counterproductive: an invented fact cites
    # a span that does not mention it, so adding it LOWERS the ratio.
    #
    # A fact the merge saw in several windows is supported if ANY of the spans it
    # cites supports it: each sighting was a separate claim that THIS span says
    # so, and one of them being right is enough. Testing the widened span instead
    # would credit the union of everything in between, which is the M1 defect
    # again from the other side.
    supported = 0
    unsupported = []
    measurable = 0
    for f in facts:
        ranges = _cited_ranges(f)
        if not ranges or n_chunks == 0:
            continue
        claim_tokens = (_content_tokens(f.get("subject"))
                        | _content_tokens(f.get("object"))
                        | _content_tokens(f.get("object_literal")))
        if not claim_tokens:
            continue
        best = None
        for lo, hi in ranges:
            lo, hi = max(lo_bound, lo), min(hi_bound, hi)
            if lo > hi:
                continue
            span = " ".join(chunks_by_index[c] for c in range(lo, hi + 1)
                            if c in chunks_by_index).lower()
            overlap = len(claim_tokens & _content_tokens(span)) / len(claim_tokens)
            best = overlap if best is None else max(best, overlap)
        if best is None:                 # no cited span lies inside the document
            continue
        measurable += 1
        # Half the distinctive endpoint language must appear in a span it cites.
        if best >= 0.5:
            supported += 1
        else:
            unsupported.append({
                "fact_text": (f.get("fact_text") or "")[:120],
                "chunk_ranges": [[lo, hi] for lo, hi in ranges],
                "token_overlap": round(best, 3),
            })
    cite_support = (supported / measurable) if measurable else None
    dims.append(Dimension(
        name="citation_support", status=_verdict(cite_support, th.get("citation_support")),
        value=round(cite_support, 4) if cite_support is not None else None,
        threshold=(th.get("citation_support") or {}).get("fail_below"),
        detail=f"{supported} of {measurable} facts have their endpoint language present in "
               f"the source span they cite",
        evidence={"unsupported_sample": unsupported[:10], "measurable_facts": measurable},
    ))

    # ── 4. Endpoint integrity ───────────────────────────────────────────────
    # Carried from the identity contract (#62) so the two layers report in one
    # place. Not re-derived here: re-deriving it would create a second opinion
    # about identity, and one of the two would eventually be wrong.
    ident_ok = None
    ident_detail = "identity contract not run for this extraction"
    ident_evidence = {"identity_mode": (identity_evidence or {}).get("mode")}
    if identity_evidence:
        ver = identity_evidence.get("verification") or {}
        check_only = identity_evidence.get("check_only")
        if "binding_verified" in ver:
            # A real run: what was persisted was checked against the frozen map.
            # This branch stays first — a run that wrote and verified always
            # outranks a dry resolution of the same payload.
            ident_ok = 1.0 if ver["binding_verified"] else 0.0
            ident_detail = (
                f"{ver.get('checked_facts', 0)} facts checked, "
                f"{ver.get('offending_facts', 0)} endpoint(s) outside the frozen map")
        elif isinstance(check_only, dict):
            # The operator gate resolves the payload's endpoints without writing
            # anything, so there is no persisted graph to verify and no
            # `verification` block — and it must not invent one. What it CAN
            # report is whether the resolution was a bijection (N distinct typed
            # endpoints -> N distinct URIs), which is the #62 acceptance
            # criterion the real run's verification also rests on. The scope is
            # named in the evidence so a reader never mistakes this for a
            # verified write.
            ident_ok = 1.0 if check_only.get("bijective") else 0.0
            ident_detail = (
                f"check-only: {int(check_only.get('resolved_endpoints') or 0)} resolved "
                f"endpoint(s) -> {int(check_only.get('distinct_uris') or 0)} distinct URI(s) "
                f"(bijective={bool(check_only.get('bijective'))}), "
                f"{int(check_only.get('would_preregister') or 0)} would be pre-registered; "
                f"no persisted graph was verified")
            ident_evidence["scope"] = "check_only"
        elif identity_evidence.get("warn_proceeded_unpinned"):
            ident_detail = "identity mode=warn: facts written UNPINNED, binding not verified"
    dims.append(Dimension(
        name="endpoint_integrity", status=_verdict(ident_ok, th.get("endpoint_integrity")),
        value=ident_ok, threshold=(th.get("endpoint_integrity") or {}).get("fail_below"),
        detail=ident_detail,
        evidence=ident_evidence,
    ))

    # ── 5. Predicate concentration ──────────────────────────────────────────
    # The share of facts carried by the single most common predicate. HIGH is the
    # bad news: one relation repeated for volume.
    #
    # This replaces a "distinct predicates / facts" ratio, which was wrong by
    # construction and not merely mis-thresholded. Any distinct-over-total ratio
    # FALLS as a thorough extraction legitimately adds facts, so it penalised
    # exactly the behaviour it was meant to reward. Measured on the real Buehler
    # fixtures it scored the thin 25-fact run 0.48 and the curated 213-fact run
    # 0.108 — backwards. Concentration is scale-free: the thin run is 0.40 and the
    # curated run 0.49, both far from a degenerate single-predicate dump.
    if facts:
        pred_counts: dict = {}
        for f in facts:
            k = f.get("predicate")
            pred_counts[k] = pred_counts.get(k, 0) + 1
        concentration = max(pred_counts.values()) / len(facts)
        top_pred = max(pred_counts, key=lambda k: pred_counts[k])
    else:
        concentration, top_pred, pred_counts = None, None, {}
    dims.append(Dimension(
        name="predicate_concentration",
        status=_verdict(concentration, th.get("predicate_concentration")),
        value=round(concentration, 4) if concentration is not None else None,
        threshold=(th.get("predicate_concentration") or {}).get("fail_above"),
        detail=f"most common predicate {top_pred!r} holds "
               f"{pred_counts.get(top_pred, 0)} of {len(facts)} facts "
               f"({len(pred_counts)} distinct predicates)",
        evidence={"distinct_predicates": len(pred_counts),
                  "top_predicate": top_pred,
                  "higher_is_worse": True},
    ))

    # ── 6. Discourse concentration (thorough only) ──────────────────────────
    # The share of moves carried by the single most common move_type. HIGH is the
    # bad news, exactly as for predicates — and for exactly the reason given
    # there, this replaces a "distinct move types / moves" ratio. The taxonomy
    # has eight types, so that ratio was capped at 8/N and FELL with every move a
    # long document legitimately produced: a 480-move book scored 8/480 = 0.017
    # and failed for being long. It was the distinct-over-total shape §5 removed
    # for predicates, and it survived because it lives only on the thorough tier
    # and no test had ever assessed that tier (review finding M1).
    #
    # Under `_MIN_MOVES_TO_MEASURE` the value is None, which `_verdict` routes to
    # REVIEW — never a pass and never a fail on a handful of moves.
    if "discourse_concentration" in th:
        move_counts: dict = {}
        for m in moves:
            k = m.get("move_type")
            move_counts[k] = move_counts.get(k, 0) + 1
        if len(moves) >= _MIN_MOVES_TO_MEASURE:
            top_move = max(move_counts, key=lambda k: move_counts[k])
            move_conc = move_counts[top_move] / len(moves)
            move_detail = (f"most common move type {top_move!r} holds "
                           f"{move_counts[top_move]} of {len(moves)} moves "
                           f"({len(move_counts)} distinct move types)")
        else:
            top_move, move_conc = None, None
            move_detail = (f"too few moves to measure concentration "
                           f"({len(moves)} < {_MIN_MOVES_TO_MEASURE})")
        dims.append(Dimension(
            name="discourse_concentration",
            status=_verdict(move_conc, th.get("discourse_concentration")),
            value=round(move_conc, 4) if move_conc is not None else None,
            threshold=th["discourse_concentration"]["fail_above"],
            detail=move_detail,
            evidence={"distinct_move_types": len(move_counts),
                      "moves_total": len(moves),
                      "top_move_type": top_move,
                      "higher_is_worse": True},
        ))

    # ── Roll-up ─────────────────────────────────────────────────────────────
    # Any FAIL fails. Otherwise any REVIEW routes to review. A waiver is recorded
    # ALONGSIDE the verdict and never overwrites it — the point of a waiver is that
    # someone decided to proceed anyway, which stays legible only if the thing they
    # decided about is still visible.
    if any(d.status == STATUS_FAIL for d in dims):
        status = STATUS_FAIL
    elif any(d.status == STATUS_REVIEW for d in dims):
        status = STATUS_REVIEW
    elif not th:
        status = STATUS_NOT_EVALUATED
    else:
        status = STATUS_PASS

    return QualityReport(status=status, dimensions=dims, tier=tier, waiver=waiver)
