"""Local review of cross-source discourse candidates (increment I1).

Read-only over KOI. It lists stored candidates for a person to judge and appends
that person's annotations to a local JSONL file they name. It never writes to the
database, never admits or declines a candidate, never changes a retrieval limit
and never hides a stored candidate: pairs from one work move to their own lane
and stay listed.

What it shows comes in three kinds, kept apart:
  facts      the same-work and author mappings an operator supplies in a file
  cues       a claim's text containing an author name the mapping lists for the
             other source, or two sources from one publication feed. String
             and identifier matches only: not verified citation, restatement
             or dependence of evidence
  judgments  only what a reviewer records. Nothing here infers restatement,
             evidence dependence, support or an answer to a question.

An annotation binds to the candidate (id and key) and to the exact text and
source version shown, and names its reviewer as declared. It is appended,
never edited: a correction is a new line that supersedes an earlier one. A is
the claim being studied. In a candidate review A is the stored query (its first
sighting's); in a sighting review (--seen-in-run) A is that sighting's query,
the annotation records the anchor and the sighting, and reviews of the two
directions stay separate.

Tiers belong to occurrences, not to candidates. An occurrence is one sighting
of the pair by one completed run: the run, the claim whose search found it and
its pick (position among that search's picks, 1-based), from the run's effect
rows. Picks 1-5 are the first five (what the default per-claim limit keeps);
6 and later are additional. A sighting recorded before picks were recorded is
first five when its run kept at most five per claim, and unknown otherwise:
never guessed. The same pair can be first five in one run and additional in
another.

A batch chosen by a filter or made of sightings is written as two files
(make_split_batch): a reviewer file, which is all a reviewer receives, and an
operator binding file, which holds the candidate ids, runs, anchors, tiers and
the selection. Annotation needs both and checks them against each other
(bind_split). A plain batch stays one file, exactly as before (make_batch).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from api import discourse_links as dl

QUESTION = "usefulness/v1"
QUESTION_TEXT = "Is B useful to someone studying A?"
ANSWERS = ("useful", "marginal", "not_useful", "needs_context")
ANNOTATION_SCHEMA = "koi-discourse-annotation/v1"
BATCH_SCHEMA = "koi-discourse-review-batch/v1"
REVIEWER_SCHEMA = "koi-discourse-review-batch/v2-reviewer"
BINDING_SCHEMA = "koi-discourse-review-binding/v1"
SPLIT_ANNOTATION_SCHEMA = "koi-discourse-annotation/v2"
REVIEWER_ORIENTATION = "A is the claim being studied; B is the claim to judge"
ORDERS = ("source_diverse", "fused")
LANES = ("cross_source", "same_work_mapped", "same_work_suggested")
MAX_REASON_CHARS = 2000

SHARED_AUTHOR_NOTE = ("The author mapping lists an author of both sources. Shared authorship alone does not "
                      "establish that the claims share observations or evaluations.")
SAME_PUBLICATION_NOTE = ("Both source identifiers name one publication feed. That is not evidence that the "
                         "claims share observations or evaluations.")
NAME_NOTE = ("The claim text contains a name the mapping lists as an author of the other source. A name match is "
             "not verified citation, restatement or dependence.")
SUGGESTED_LANE_NOTE = ("Suggested by text similarity between the two sources (dl.same_work_signals), not by a "
                       "mapping. Unverified.")
ADMISSION_NOTE = "Admission is disabled. Binding limitations are shown so the reviewer knows what a judgment could not rely on."
FIRST_PICKS = 5          # the default per-claim limit (api.discourse_discovery.PROFILE): picks 1-5 are what it keeps
TIERS = ("first_five", "additional", "unknown")
TIER_NOTE = ("A tier describes one retrieval occurrence: where one run's search for one claim placed this pair among "
             "its picks. It is not a judgment of the pair and not a lasting property of the candidate.")
_SIGHTINGS = ("candidate_inserted", "candidate_seen")
STORED_ORIENTATION = "A=query, B=target"     # the stored candidate: A is its first sighting's query
SIGHTING_ORIENTATION = "A=query of the selected sighting, B=the claim it picked"


class ReviewError(ValueError):
    """A review action this module refuses. Nothing has been written."""


# ---------------------------------------------------------------------------
# Reading candidates (read-only)
# ---------------------------------------------------------------------------

async def require_read_only(conn) -> None:
    if await conn.fetchval("SHOW transaction_read_only") != "on":
        raise ReviewError("review reads run only inside a READ ONLY transaction")


def _end(mv: Dict[str, Any], *, stored_repr: str, stored_bound: Optional[str]) -> Dict[str, Any]:
    ext, src = mv["extraction"], mv["source"]
    return {
        "move_id": mv["id"], "move_type": mv["move_type"], "title": mv["title"], "detail": mv["detail"],
        "status": mv["status"], "chunk_range": list(mv["chunk_range"]),
        "representation_sha256": mv["representation_sha256"],
        "representation_at_discovery": stored_repr,
        "text_changed_since_discovery": mv["representation_sha256"] != stored_repr,
        "source": {"rid": src["rid"], "title": src["title"], "url": src["source_url"],
                   "version": src["version"], "version_basis": src["version_basis"]},
        "binding": {"kind": ext["binding_kind"], "basis": ext["binding_basis"], "association": ext["association"],
                    "bound_version": ext["bound_version"], "bound_version_at_discovery": stored_bound,
                    "current": ext["current"], "limitations": list(mv["admission_blockers"])},
    }


def _json(v):
    return json.loads(v) if isinstance(v, str) else v


def occurrence_tier(pick: Optional[int], run_per_move_cap: Optional[int]) -> Tuple[str, str]:
    """One occurrence's tier and its basis: first_five (picks 1-5), additional (6 on) or unknown."""
    if isinstance(pick, int):
        return ("first_five" if pick <= FIRST_PICKS else "additional"), "recorded pick"
    if isinstance(run_per_move_cap, int) and run_per_move_cap <= FIRST_PICKS:
        return "first_five", f"pick not recorded; the run kept at most {run_per_move_cap} per claim"
    return "unknown", "pick not recorded"


async def occurrences(conn, candidates: Sequence[Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Each stored candidate's sightings by completed runs, in the order they were
    written: the run, which end's search found it, the pick and its tier. A
    rolled-back run's sightings stay in its history and are not occurrences.
    SELECTs only."""
    await require_read_only(conn)
    stored = {str(c["id"]): c for c in candidates}
    if not stored:
        return {}
    out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in await conn.fetch(
            """SELECT e.candidate_id, e.run_id, e.effect, e.after_state, r.approval_scope
                 FROM discourse_link_run_effects e JOIN discourse_link_runs r ON r.run_id = e.run_id
                WHERE e.candidate_id = ANY($1::uuid[]) AND e.effect = ANY($2::text[]) AND r.status = 'completed'
                ORDER BY e.id""", list(stored), list(_SIGHTINGS)):
        c = stored[str(e["candidate_id"])]
        occ = (_json(e["after_state"]) or {}).get("occurrence") or {}
        cap = (_json(e["approval_scope"]) or {}).get("per_move_cap")
        query = occ.get("query_move_id")
        if query is None and e["effect"] == "candidate_inserted":
            query = str(c["query_move_id"])          # the first sighting is the stored row's own query
        tier, basis = occurrence_tier(occ.get("pick"), cap)
        out[str(e["candidate_id"])].append({
            "run_id": e["run_id"], "sighting": "first" if e["effect"] == "candidate_inserted" else "again",
            "query_move_id": query, "query_end": {str(c["query_move_id"]): "a", str(c["target_move_id"]): "b"}.get(query),
            "pick": occ.get("pick"), "run_per_move_cap": cap, "tier": tier, "tier_basis": basis})
    return out


async def candidate_rows(conn, *, run_ids: Optional[Sequence[str]] = None,
                         candidate_ids: Optional[Sequence[str]] = None,
                         seen_in_runs: Optional[Sequence[str]] = None) -> List[Dict[str, Any]]:
    """Open candidates whose two claims the personal context may see, each with
    both claims' current text, source version and binding, and its occurrences
    (see occurrences()). run_ids: candidates first discovered by those runs (the
    stored run). seen_in_runs: candidates sighted by those runs, first or again.
    Rows are in stored orientation (A = the first sighting's query). SELECTs only,
    and refused outside a READ ONLY transaction."""
    await require_read_only(conn)
    rows = await conn.fetch(
        """SELECT * FROM discourse_link_candidates c
            WHERE c.status = 'candidate'
              AND ($1::text[] IS NULL OR c.run_id = ANY($1::text[]))
              AND ($2::uuid[] IS NULL OR c.id = ANY($2::uuid[]))
              AND ($3::text[] IS NULL OR EXISTS (
                     SELECT 1 FROM discourse_link_run_effects e JOIN discourse_link_runs r ON r.run_id = e.run_id
                      WHERE e.candidate_id = c.id AND e.run_id = ANY($3::text[]) AND r.status = 'completed'
                        AND e.effect IN ('candidate_inserted', 'candidate_seen')))
            ORDER BY c.id""",
        list(run_ids) if run_ids else None, list(candidate_ids) if candidate_ids else None,
        list(seen_in_runs) if seen_in_runs else None)
    ids = {str(r[k]) for r in rows for k in ("query_move_id", "target_move_id")}
    visible = await dl.visible_ids(conn, dl.personal_context(), ids)
    moves = await dl.fetch_moves(conn, visible)
    seen = await occurrences(conn, rows)
    out = []
    for r in rows:
        q, t = str(r["query_move_id"]), str(r["target_move_id"])
        if q not in moves or t not in moves:
            continue
        reasons = sorted(set(
            dl._end_problems(r["query_representation_sha256"], r["query_bound_version"], moves[q], for_admission=False)
            + dl._end_problems(r["target_representation_sha256"], r["target_bound_version"], moves[t], for_admission=False)))
        out.append({
            "candidate_id": str(r["id"]), "candidate_key": r["candidate_key"], "run_id": r["run_id"],
            "method": r["method"], "discovered_by": r["discovered_by"], "times_seen": r["times_seen"],
            "scores": {k: r[k] for k in ("fused_score", "lexical_rank", "lexical_score", "semantic_rank", "semantic_score")},
            "a": _end(moves[q], stored_repr=r["query_representation_sha256"], stored_bound=r["query_bound_version"]),
            "b": _end(moves[t], stored_repr=r["target_representation_sha256"], stored_bound=r["target_bound_version"]),
            "current": not reasons, "not_current_reasons": reasons,
            "occurrences": seen.get(str(r["id"]), []),
        })
    return out


async def suggested_same_work(conn) -> List[frozenset]:
    """Source pairs that look like two copies of one work by candidate similarity. Read-only."""
    await require_read_only(conn)
    return [frozenset((s["source_a"], s["source_b"])) for s in await dl.same_work_signals(conn, context=dl.personal_context())]


# ---------------------------------------------------------------------------
# Mappings, lanes, cues, ordering (pure)
# ---------------------------------------------------------------------------

def load_mappings(path: Optional[str]) -> Dict[str, Any]:
    """A mapping file: {"prepared_by": ..., "same_work": {work_id: [rid, ...]},
    "authors": {rid: [name, ...] or {"names": [...], "basis": ...}}}. A rid may belong to one
    work only. A source missing from "authors" has unknown authors, not "no shared author"."""
    if not path:
        return {"same_work": {}, "authors": {}, "path": None, "sha256": None, "prepared_by": None}
    raw = Path(path).read_bytes()
    data = json.loads(raw)
    rid_to_work: Dict[str, str] = {}
    for work, rids in (data.get("same_work") or {}).items():
        for rid in rids:
            if rid in rid_to_work and rid_to_work[rid] != work:
                raise ReviewError(f"{rid} is mapped to two works ({rid_to_work[rid]}, {work})")
            rid_to_work[rid] = work
    authors, author_basis = {}, {}
    for rid, entry in (data.get("authors") or {}).items():
        names = entry.get("names", []) if isinstance(entry, dict) else entry
        authors[rid] = [n.strip() for n in names if n and n.strip()]
        author_basis[rid] = entry.get("basis") if isinstance(entry, dict) else None
    return {"same_work": rid_to_work, "authors": authors, "author_basis": author_basis, "path": str(path),
            "sha256": hashlib.sha256(raw).hexdigest(), "prepared_by": data.get("prepared_by")}


def lane(row: Dict[str, Any], mappings: Dict[str, Any], suggested: Iterable[frozenset] = ()) -> str:
    a, b = row["a"]["source"]["rid"], row["b"]["source"]["rid"]
    work = mappings.get("same_work", {})
    if a in work and work.get(a) == work.get(b):
        return "same_work_mapped"
    if frozenset((a, b)) in set(suggested):
        return "same_work_suggested"
    return "cross_source"


def _publication(rid: str) -> Optional[str]:
    parts = rid.split(":")
    return parts[1] if len(parts) == 3 and parts[0] == "substack-corpus" else None


def _text(end: Dict[str, Any]) -> str:
    return " ".join(x for x in (end.get("title"), end.get("detail")) if x)


def listed_authors(row: Dict[str, Any], mappings: Dict[str, Any]) -> Dict[str, Optional[Dict[str, Any]]]:
    """Per end, the authors the mapping lists and its basis, or None when the mapping is silent."""
    out: Dict[str, Optional[Dict[str, Any]]] = {}
    for side in ("a", "b"):
        rid = row[side]["source"]["rid"]
        names = mappings.get("authors", {}).get(rid)
        out[side] = ({"names": names, "basis": mappings.get("author_basis", {}).get(rid)}
                     if names else None)
    return out


def cues(row: Dict[str, Any], mappings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Authorship and naming cues, each labelled with its basis. None is a dependence judgment."""
    authors = mappings.get("authors", {})
    a_src, b_src = row["a"]["source"]["rid"], row["b"]["source"]["rid"]
    a_auth, b_auth = authors.get(a_src, []), authors.get(b_src, [])
    out: List[Dict[str, Any]] = []
    shared = sorted({n.casefold() for n in a_auth} & {n.casefold() for n in b_auth})
    if shared:
        out.append({"kind": "shared_author_listed", "names": shared, "basis": "author mapping file",
                    "note": SHARED_AUTHOR_NOTE})
    pa, pb = _publication(a_src), _publication(b_src)
    if pa and pa == pb:
        out.append({"kind": "same_publication", "publication": pa, "basis": "source identifiers",
                    "note": SAME_PUBLICATION_NOTE})
    for side, end, other in (("a", row["a"], b_auth), ("b", row["b"], a_auth)):
        text = _text(end)
        for name in other:
            surname = name.split()[-1]
            # Case-sensitive on purpose: a capitalised surname, so "grant" does not match "Grant".
            if re.search(rf"(?<!\w){re.escape(surname)}(?!\w)", text):
                out.append({"kind": "names_other_source_author", "side": side, "name": name, "matched": surname,
                            "basis": "string match in claim text", "note": NAME_NOTE})
    return out


def order(rows: Sequence[Dict[str, Any]], strategy: str = "source_diverse") -> List[Dict[str, Any]]:
    """fused: by fused score. source_diverse: round-robin over unordered source pairs,
    each pair's candidates by fused score, pairs in order of their best candidate.
    Every row is kept; only the order changes."""
    if strategy not in ORDERS:
        raise ReviewError(f"order must be one of {ORDERS}")
    ranked = sorted(rows, key=lambda r: (-(r["scores"]["fused_score"] or 0.0), r["candidate_id"]))
    if strategy == "fused":
        return ranked
    groups: Dict[frozenset, List[Dict[str, Any]]] = defaultdict(list)
    for r in ranked:
        groups[frozenset((r["a"]["source"]["rid"], r["b"]["source"]["rid"]))].append(r)
    queues = list(groups.values())          # insertion order = order of each pair's best candidate
    out: List[Dict[str, Any]] = []
    while any(queues):
        for qu in queues:
            if qu:
                out.append(qu.pop(0))
    return out


def anchored(row: Dict[str, Any], occ: Dict[str, Any]) -> Dict[str, Any]:
    """The row as one sighting saw it: A is that sighting's query, B the claim it picked.
    When the query was the stored B, the ends swap, and each occurrence's query_end is
    restated for the swapped ends. The stored row is not changed."""
    swap = occ["query_end"] == "b"
    item = dict(row)
    if swap:
        item["a"], item["b"] = row["b"], row["a"]
        item["occurrences"] = [dict(o, query_end={"a": "b", "b": "a"}.get(o["query_end"]))
                               for o in row.get("occurrences", [])]
    item["anchor"] = {"move_id": item["a"]["move_id"], "stored_end": "b" if swap else "a"}
    item["sighting"] = {"run_id": occ["run_id"], "query_move_id": occ["query_move_id"]}
    item["tier"] = occ["tier"]
    return item


def review_items(rows: Sequence[Dict[str, Any]], *, seen_in_runs: Optional[Sequence[str]] = None,
                 tier: Optional[str] = None) -> Tuple[List[Dict[str, Any]], int]:
    """What to review, and how many sightings could not be anchored.

    Without seen_in_runs: each candidate once, in stored orientation, with its first
    sighting's tier. With seen_in_runs: each sighting by those runs, anchored at its
    own query (anchored()), so one pair met from both ends gives two items. A
    sighting whose query was not recorded is not listed and is counted. tier keeps
    only items of that tier."""
    if tier is not None and tier not in TIERS:
        raise ReviewError(f"tier must be one of {TIERS}")
    items: List[Dict[str, Any]] = []
    unanchored = 0
    for r in rows:
        if not seen_in_runs:
            first = next((o for o in r.get("occurrences", []) if o["sighting"] == "first"), None)
            items.append(dict(r, tier=first["tier"] if first else "unknown"))
            continue
        for o in r.get("occurrences", []):
            if o["run_id"] not in seen_in_runs:
                continue
            if o["query_end"] is None:
                unanchored += tier is None or o["tier"] == tier
                continue
            items.append(anchored(r, o))
    return [it for it in items if tier is None or it["tier"] == tier], unanchored


def tier_counts(items: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    """Review items by tier (review_items: one per candidate, or one per sighting)."""
    return {t: sum(it.get("tier") == t for it in items) for t in TIERS}


def queue(rows: Sequence[Dict[str, Any]], mappings: Dict[str, Any], suggested: Iterable[frozenset] = (),
          strategy: str = "source_diverse") -> Dict[str, List[Dict[str, Any]]]:
    """Rows split into lanes and ordered within each. Nothing is dropped."""
    suggested = set(suggested)
    lanes: Dict[str, List[Dict[str, Any]]] = {name: [] for name in LANES}
    for r in rows:
        name = lane(r, mappings, suggested)
        lanes[name].append({**r, "lane": name, "authors": listed_authors(r, mappings), "cues": cues(r, mappings)})
    return {name: order(items, strategy) for name, items in lanes.items()}


# ---------------------------------------------------------------------------
# Showing an item
# ---------------------------------------------------------------------------

_HIDDEN_WHEN_BLIND = ("scores", "run_id", "method", "discovered_by", "times_seen", "occurrences", "tier", "anchor",
                      "sighting")


def view(row: Dict[str, Any], *, blind: bool = True) -> Dict[str, Any]:
    """What a reviewer sees. Blind hides retrieval scores, ranks, runs, sightings, picks and tiers, and the discoverer."""
    v = {k: v for k, v in row.items() if not (blind and k in _HIDDEN_WHEN_BLIND)}
    v["admission_note"] = ADMISSION_NOTE
    return v


def _end_lines(tag: str, end: Dict[str, Any], authors: Optional[Dict[str, Any]] = None) -> List[str]:
    src, b = end["source"], end["binding"]
    lines = [f"{tag}  [{end['move_type']}] {end['title']}"]
    if end.get("detail"):
        lines.append(f"    {end['detail']}")
    lines.append(f"    source: {src['title'] or '(no title)'}  ({src['rid']})")
    lines.append(f"    authors: {', '.join(authors['names'])} (mapping: {authors['basis'] or 'basis not stated'})"
                 if authors else "    authors: unknown (not in the mapping)")
    if end.get("chunk_range") and end["chunk_range"][0] is not None:
        lines.append(f"    extracted from source chunks {end['chunk_range'][0]}-{end['chunk_range'][1]}")
    if src.get("url"):
        lines.append(f"    url: {src['url']}")
    lines.append(f"    source version: {src['version'] or 'unknown'} ({src['version_basis']})")
    lines.append(f"    binding: {b['kind']}" + (f", {b['association']}" if b.get("association") else ""))
    for lim in b["limitations"]:
        lines.append(f"    limitation: {lim}")
    if end.get("text_changed_since_discovery"):
        lines.append("    note: claim text changed since discovery")
    return lines


def render(item: Dict[str, Any], *, position: Optional[int] = None, total: Optional[int] = None,
           blind: bool = True) -> str:
    head = f"[{position}/{total}] " if position else ""
    label = f"item {item['item_id'][:8]}" if item.get("item_id") else f"candidate {item['candidate_id'][:8]}"
    lines = [f"{head}{label}  lane: {item.get('lane', 'cross_source')}"
             + ("  (blind: retrieval scores and run hidden)" if blind else "")]
    authors = item.get("authors") or {}
    lines += _end_lines("A", item["a"], authors.get("a")) + _end_lines("B", item["b"], authors.get("b"))
    for c in item.get("cues", []):
        where = {"a": "A's text", "b": "B's text"}.get(c.get("side"), "")
        detail = (f"{where} contains '{c['matched']}' ({c['name']})" if c["kind"] == "names_other_source_author"
                  else ", ".join(c.get("names", [])) or c.get("publication", ""))
        lines.append(f"cue (unverified, {c['basis']}): {c['kind']}: {detail}. {c['note']}")
    if item.get("lane") == "same_work_suggested":
        lines.append(f"lane note: {SUGGESTED_LANE_NOTE}")
    if not blind and "scores" in item:
        s = item["scores"]
        lines.append(f"retrieval: fused {s['fused_score']:.4f}  lexical rank {s['lexical_rank']}  semantic rank {s['semantic_rank']}"
                     f"  run {item.get('run_id')}")
    if not blind and item.get("sighting"):
        lines.append(f"anchor: A, the query of the selected sighting (run {item['sighting']['run_id']}, "
                     f"{'stored B' if item['anchor']['stored_end'] == 'b' else 'stored A'}): {item['tier']}")
    if not blind and item.get("occurrences"):
        for o in item["occurrences"]:
            search = {"a": "A's search", "b": "B's search"}.get(o["query_end"], "search not recorded")
            limit = f"limit {o['run_per_move_cap']} per claim" if o["run_per_move_cap"] else "limit not recorded"
            lines.append(f"occurrence ({'first sighting' if o['sighting'] == 'first' else 'seen again'}): run {o['run_id']}, "
                         f"{search}, pick {o['pick'] or '?'} ({limit}): {o['tier']} ({o['tier_basis']})")
        lines.append(f"tier note: {TIER_NOTE}")
    lines.append(ADMISSION_NOTE)
    lines.append(f"Question: {QUESTION_TEXT}  ({' | '.join(ANSWERS)})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Batches and annotations (local files only)
# ---------------------------------------------------------------------------

def _canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _sha(obj: Any) -> str:
    return hashlib.sha256(_canonical(obj)).hexdigest()


def _shown_end(end: Dict[str, Any]) -> Dict[str, Any]:
    keep = ("move_id", "move_type", "title", "detail", "status", "chunk_range", "representation_sha256")
    return {**{k: end[k] for k in keep}, "source": dict(end["source"]),
            "binding": {k: end["binding"][k] for k in ("kind", "association", "bound_version", "current", "limitations")}}


def make_batch(items: Sequence[Dict[str, Any]], *, database: str, blind: bool = True,
               mappings: Optional[Dict[str, Any]] = None, note: Optional[str] = None,
               created_at: Optional[str] = None, selection: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A self-contained batch: the exact text and versions shown, and the mapping file behind
    its lanes and cues. No retrieval scores, ranks, picks or tiers. An item made from one
    sighting (review_items with seen_in_runs) also carries its anchor and that sighting
    (run and query), which its annotations are bound to. `selection` (the filters the
    items were chosen by) is recorded for the operator and is part of batch_id. A batch
    made without either is exactly what it was before. A file with either is for the
    operator only: `batch` writes those as a reviewer file and a binding file
    (make_split_batch), and make_batch stays so existing batches rebuild to their ids."""
    entries = []
    for i, it in enumerate(items, start=1):
        entry = {"position": i, "candidate_id": it["candidate_id"], "candidate_key": it["candidate_key"],
                 "lane": it.get("lane", "cross_source"), "authors": it.get("authors", {}), "cues": it.get("cues", []),
                 "a": _shown_end(it["a"]), "b": _shown_end(it["b"])}
        if it.get("anchor"):
            entry["anchor"], entry["sighting"] = dict(it["anchor"]), dict(it["sighting"])
        entries.append(entry)
    body = {"schema": BATCH_SCHEMA, "question": QUESTION, "question_text": QUESTION_TEXT, "answers": list(ANSWERS),
            "blind": blind, "database": database, "note": note,
            "mappings": {k: (mappings or {}).get(k) for k in ("path", "sha256", "prepared_by")}, "items": entries}
    if selection is not None:
        body["selection"] = selection
    return {**body, "batch_id": _sha(body),
            "created_at": created_at or datetime.now(timezone.utc).isoformat(timespec="microseconds")}


def binding_problems(current: Optional[Dict[str, Any]], item: Dict[str, Any]) -> List[str]:
    """Why an annotation on `item` would no longer be bound to what the reviewer saw.
    `current` is the candidate as candidate_rows reads it now, in stored orientation;
    an item anchored at the stored B is compared end for end."""
    if current is None:
        return ["candidate is no longer open or visible"]
    problems = []
    if current["candidate_key"] != item["candidate_key"]:
        problems.append("candidate key changed")
    anchor = item.get("anchor")
    stored = {"a": "b", "b": "a"} if anchor and anchor["stored_end"] == "b" else {"a": "a", "b": "b"}
    for side in ("a", "b"):
        now = current[stored[side]]
        if now["move_id"] != item[side]["move_id"]:
            problems.append(f"{side}: not the claim the batch showed")
        if now["representation_sha256"] != item[side]["representation_sha256"]:
            problems.append(f"{side}: claim text changed since the batch was made")
        if now["source"]["version"] != item[side]["source"]["version"]:
            problems.append(f"{side}: source version changed since the batch was made")
    if anchor:
        s = item["sighting"]
        if s["query_move_id"] != item["a"]["move_id"] or not any(
                o["run_id"] == s["run_id"] and o["query_move_id"] == s["query_move_id"]
                for o in current.get("occurrences", [])):
            problems.append("the selected sighting is no longer current (its run was rolled back, or it never was)")
    return problems


# ---------------------------------------------------------------------------
# Split batches: a reviewer file and an operator binding file
# ---------------------------------------------------------------------------

def needs_split(items: Sequence[Dict[str, Any]], selection: Optional[Dict[str, Any]]) -> bool:
    """A batch chosen by a filter, or holding a sighting, carries retrieval metadata a
    reviewer must not see, so it is written as a reviewer file plus a binding file."""
    return selection is not None or any(it.get("anchor") for it in items)


def item_id(salt: str, *, candidate_id: str, candidate_key: str, a_move_id: str, b_move_id: str,
            anchor: Optional[Dict[str, Any]], sighting: Optional[Dict[str, Any]]) -> str:
    """The identity a reviewer file and its binding file share for one item. Salted, so
    a reviewer cannot recover the candidate, run or anchor from it."""
    return _sha({"salt": salt, "candidate_id": candidate_id, "candidate_key": candidate_key,
                 "a": a_move_id, "b": b_move_id, "anchor": anchor, "sighting": sighting})


def reviewer_file_bytes(reviewer: Dict[str, Any]) -> bytes:
    return (json.dumps(reviewer, indent=1, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def make_split_batch(items: Sequence[Dict[str, Any]], *, database: str,
                     mappings: Optional[Dict[str, Any]] = None, note: Optional[str] = None,
                     created_at: Optional[str] = None, selection: Optional[Dict[str, Any]] = None,
                     salt: Optional[str] = None) -> Tuple[bytes, Dict[str, Any]]:
    """The reviewer file's exact bytes, and the binding file that belongs to them.

    The reviewer file holds the question, the orientation and, per item, A and B as
    make_batch shows them (claim text, source and binding provenance), the lane, the
    listed authors and the cues. It holds no candidate id or key, score, rank, run,
    pick, tier, anchor, selection, note, database or time, and its positions follow
    the salted item ids, not the queue's (score) order. Everything else goes in the
    binding file: per item the candidate, its queue position, anchor, sighting and
    tier, and for the batch the salt, selection, note, mapping file and database. The
    reviewer file carries a salted commitment to all of that (binding_commitment)."""
    salt = salt or secrets.token_hex(32)
    entries = []
    for queue_position, it in enumerate(items, start=1):
        anchor = dict(it["anchor"]) if it.get("anchor") else None
        sighting = dict(it["sighting"]) if it.get("sighting") else None
        iid = item_id(salt, candidate_id=it["candidate_id"], candidate_key=it["candidate_key"],
                      a_move_id=it["a"]["move_id"], b_move_id=it["b"]["move_id"], anchor=anchor, sighting=sighting)
        entries.append((iid, queue_position, it, anchor, sighting))
    if len({e[0] for e in entries}) != len(entries):
        raise ReviewError("the same item is listed twice")
    shown_pairs = [(it["a"]["move_id"], it["b"]["move_id"]) for _, _, it, _, _ in entries]
    if len(set(shown_pairs)) != len(shown_pairs):
        raise ReviewError("two items would show the same A and B (one pair sighted from the same claim by two runs); "
                          "a reviewer would learn it was retrieved twice. Make one batch per run")
    entries.sort(key=lambda e: e[0])
    shown, bound = [], []
    for position, (iid, queue_position, it, anchor, sighting) in enumerate(entries, start=1):
        shown.append({"position": position, "item_id": iid, "lane": it.get("lane", "cross_source"),
                      "authors": it.get("authors", {}), "cues": it.get("cues", []),
                      "a": _shown_end(it["a"]), "b": _shown_end(it["b"])})
        bound.append({"position": position, "item_id": iid, "queue_position": queue_position,
                      "candidate_id": it["candidate_id"], "candidate_key": it["candidate_key"],
                      "anchor": anchor, "sighting": sighting, "tier": it.get("tier")})
    content = {"database": database, "note": note, "selection": selection,
               "mappings": {k: (mappings or {}).get(k) for k in ("path", "sha256", "prepared_by")},
               "created_at": created_at or datetime.now(timezone.utc).isoformat(timespec="microseconds"),
               "items": bound}
    body = {"schema": REVIEWER_SCHEMA, "question": QUESTION, "question_text": QUESTION_TEXT, "answers": list(ANSWERS),
            "blind": True, "orientation": REVIEWER_ORIENTATION, "items": shown,
            "binding_commitment": binding_commitment(salt, content)}
    raw = reviewer_file_bytes({**body, "reviewer_batch_id": _sha(body)})
    binding = {"schema": BINDING_SCHEMA, "reviewer_batch_id": _sha(body),
               "reviewer_file_sha256": hashlib.sha256(raw).hexdigest(), "salt": salt, **content}
    return raw, {**binding, "binding_id": _sha(binding)}


_BINDING_CONTENT = ("database", "note", "selection", "mappings", "created_at", "items")
_BINDING_KEYS = ("schema", "reviewer_batch_id", "reviewer_file_sha256", "salt", "binding_id") + _BINDING_CONTENT
_BOUND_ITEM_KEYS = ("position", "item_id", "queue_position", "candidate_id", "candidate_key", "anchor", "sighting", "tier")


def binding_commitment(salt: str, content: Dict[str, Any]) -> str:
    """The reviewer file's salted commitment to everything the binding file says about the
    batch and its items (tiers, queue positions, anchors, sightings, candidates, selection,
    note, mappings, database, time). Salted, so it reveals none of them."""
    return _sha({"salt": salt, **{k: content[k] for k in _BINDING_CONTENT}})


MIN_ITEM_ID_PREFIX = 8


def check_item_id(item: Dict[str, Any], given: Optional[str]) -> None:
    """An answer recorded from a reviewer file names the item id the reviewer was shown, so
    an answer cannot land on another item, or on another reviewer file's item at the same
    position."""
    given = (given or "").strip().lower()
    if len(given) < MIN_ITEM_ID_PREFIX:
        raise ReviewError(f"an answer to a reviewer file needs the item id it was given for "
                          f"(--item-id, at least {MIN_ITEM_ID_PREFIX} characters)")
    if not item["item_id"].startswith(given):
        raise ReviewError("the item id does not match the item at this position: "
                          "the answer was given for another item or another reviewer file")


def read_reviewer_file(raw: bytes) -> Dict[str, Any]:
    """A reviewer file, checked against its own id."""
    try:
        reviewer = json.loads(raw)
    except ValueError:
        raise ReviewError("the reviewer file is not JSON")
    if not isinstance(reviewer, dict) or reviewer.get("schema") != REVIEWER_SCHEMA:
        raise ReviewError(f"not a {REVIEWER_SCHEMA} file")
    if reviewer.get("reviewer_batch_id") != _sha({k: v for k, v in reviewer.items() if k != "reviewer_batch_id"}):
        raise ReviewError("the reviewer file does not match its own id: it was edited")
    if not isinstance(reviewer.get("items"), list) or not all(
            isinstance(i, dict) and {"position", "item_id", "a", "b"} <= set(i) for i in reviewer["items"]):
        raise ReviewError("the reviewer file does not have the fields a reviewer file has")
    return reviewer


def bind_split(raw: bytes, binding: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """The reviewer file and its binding file as one batch that annotation() and
    binding_problems() accept, after these checks, each of which refuses:
      1. the reviewer file matches its own id (reviewer_batch_id);
      2. a binding file is given, has the expected fields, and matches its own id (binding_id);
      3. it names this reviewer file, by id and by the sha256 of its exact bytes;
      4. its contents (items, tiers, selection, note, mappings, database, time) are the ones
         the reviewer file committed to (binding_commitment), so an edit is refused even
         when the binding file's own id was recomputed;
      5. both list the same positions, each with the same item id, and that id is the
         one the binding's salt and candidate give for the A and B the reviewer saw."""
    reviewer = read_reviewer_file(raw)
    if binding is None:
        raise ReviewError("this reviewer file needs its binding file (--binding); nothing is recorded without it")
    if not isinstance(binding, dict) or binding.get("schema") != BINDING_SCHEMA:
        raise ReviewError(f"not a {BINDING_SCHEMA} file")
    if set(binding) != set(_BINDING_KEYS) or not isinstance(binding["items"], list) or not all(
            isinstance(b, dict) and set(b) == set(_BOUND_ITEM_KEYS) for b in binding["items"]):
        raise ReviewError("the binding file does not have the fields a binding file has")
    if binding["binding_id"] != _sha({k: v for k, v in binding.items() if k != "binding_id"}):
        raise ReviewError("the binding file does not match its own id: it was edited")
    if binding["reviewer_batch_id"] != reviewer["reviewer_batch_id"]:
        raise ReviewError("the binding file belongs to another reviewer file")
    if binding["reviewer_file_sha256"] != hashlib.sha256(raw).hexdigest():
        raise ReviewError("the reviewer file's bytes are not the ones its binding file was made for")
    if reviewer.get("binding_commitment") != binding_commitment(binding["salt"], binding):
        raise ReviewError("the binding file's contents are not the ones its reviewer file was made with: it was edited")
    shown = {it["position"]: it for it in reviewer["items"]}
    bound = {it["position"]: it for it in binding["items"]}
    if len(shown) != len(reviewer["items"]) or len(bound) != len(binding["items"]) or set(shown) != set(bound):
        raise ReviewError("the reviewer file and the binding file list different items")
    items = []
    for position in sorted(shown):
        s, b = shown[position], bound[position]
        expected = item_id(binding["salt"], candidate_id=b["candidate_id"], candidate_key=b["candidate_key"],
                           a_move_id=s["a"]["move_id"], b_move_id=s["b"]["move_id"],
                           anchor=b["anchor"], sighting=b["sighting"])
        if not s["item_id"] == b["item_id"] == expected:
            raise ReviewError(f"item {position}: the two files do not describe the same item")
        merged = {**s, "candidate_id": b["candidate_id"], "candidate_key": b["candidate_key"]}
        if b["anchor"]:
            merged["anchor"], merged["sighting"] = dict(b["anchor"]), dict(b["sighting"])
        items.append(merged)
    return {"schema": REVIEWER_SCHEMA, "format": "split", "batch_id": reviewer["reviewer_batch_id"],
            "binding_id": binding["binding_id"], "blind": True, "database": binding["database"],
            "question_text": reviewer["question_text"], "items": items}


def annotation(batch: Dict[str, Any], position: int, *, reviewer: str, answer: str, reason: Optional[str] = None,
               supersedes: Optional[str] = None, os_user: Optional[str] = None, tool_version: Optional[str] = None,
               created_at: Optional[str] = None) -> Dict[str, Any]:
    reviewer = (reviewer or "").strip()
    if not reviewer:
        raise ReviewError("an annotation needs a reviewer")
    if answer not in ANSWERS:
        raise ReviewError(f"answer must be one of {ANSWERS}")
    reason = (reason or "").strip() or None
    if reason and len(reason) > MAX_REASON_CHARS:
        raise ReviewError(f"reason longer than {MAX_REASON_CHARS} characters")
    items = {it["position"]: it for it in batch["items"]}
    if position not in items:
        raise ReviewError(f"batch has no item {position}")
    it = items[position]

    def shown(end):
        return {"move_id": end["move_id"], "representation_sha256": end["representation_sha256"],
                "source_rid": end["source"]["rid"], "source_version": end["source"]["version"],
                "bound_version": end["binding"]["bound_version"], "binding_kind": end["binding"]["kind"]}
    split = batch.get("format") == "split"
    rec = {"schema": SPLIT_ANNOTATION_SCHEMA if split else ANNOTATION_SCHEMA, "question": QUESTION,
           "answer": answer, "reason": reason,
           "reviewer": reviewer, "os_user": os_user,
           "created_at": created_at or datetime.now(timezone.utc).isoformat(timespec="microseconds"),
           "batch_id": batch["batch_id"], "position": position, "blind": batch["blind"], "database": batch["database"],
           "candidate": {"id": it["candidate_id"], "candidate_key": it["candidate_key"]},
           "shown": {"orientation": SIGHTING_ORIENTATION if it.get("anchor") else STORED_ORIENTATION,
                     "a": shown(it["a"]), "b": shown(it["b"])},
           "tool": {"name": "koi-discourse review", "version": tool_version}, "supersedes": supersedes}
    if it.get("anchor"):
        rec["anchor"], rec["sighting"] = dict(it["anchor"]), dict(it["sighting"])
    if split:
        rec["binding_id"], rec["item_id"] = batch["binding_id"], it["item_id"]
    return {**rec, "annotation_id": _sha(rec)}


def read_annotations(path: str) -> List[Dict[str, Any]]:
    """Every line, each checked against its own id. A damaged line refuses the whole file."""
    p = Path(path)
    if not p.exists():
        return []
    out = []
    for n, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        rec = json.loads(line)
        body = {k: v for k, v in rec.items() if k != "annotation_id"}
        if rec.get("annotation_id") != _sha(body):
            raise ReviewError(f"{path}:{n}: annotation does not match its id; the file was edited or damaged")
        out.append(rec)
    return out


def append_annotation(path: str, rec: Dict[str, Any]) -> None:
    """Append one line. Refuses a damaged file, a duplicate, and a supersede of anything but
    the same reviewer's earlier annotation on the same candidate and question."""
    p = Path(path)
    if p.exists() and p.stat().st_size:
        with open(p, "rb") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                raise ReviewError(f"{path} does not end with a newline; refusing to append to a damaged file")
    prior = read_annotations(path)
    if any(a["annotation_id"] == rec["annotation_id"] for a in prior):
        raise ReviewError("this exact annotation is already recorded")
    if rec.get("supersedes"):
        old = next((a for a in prior if a["annotation_id"] == rec["supersedes"]), None)
        if old is None or (old["reviewer"], old["candidate"]["id"], old["question"], old["shown"]["a"]["move_id"]) != (
                rec["reviewer"], rec["candidate"]["id"], rec["question"], rec["shown"]["a"]["move_id"]):
            raise ReviewError("supersedes must name an earlier annotation by the same reviewer on the same "
                              "candidate and question, studying the same claim A")
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, sort_keys=True, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def latest(annotations: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Annotations no later annotation supersedes, in file order."""
    superseded = {a["supersedes"] for a in annotations if a.get("supersedes")}
    return [a for a in annotations if a["annotation_id"] not in superseded]


KEYS = {"u": "useful", "m": "marginal", "n": "not_useful", "c": "needs_context"}


def review_loop(batch: Dict[str, Any], *, reviewer: str, path: str, verify: Callable[[Dict[str, Any]], List[str]],
                ask: Callable[[str], str] = input, say: Callable[[str], None] = print,
                os_user: Optional[str] = None, tool_version: Optional[str] = None) -> int:
    """Walk a batch, skipping items this reviewer already answered in it. Returns the number recorded."""
    answered = {a["position"] for a in latest(read_annotations(path))
                if a["batch_id"] == batch["batch_id"] and a["reviewer"] == reviewer.strip()}
    total, recorded = len(batch["items"]), 0
    for it in batch["items"]:
        if it["position"] in answered:
            continue
        say(render(it, position=it["position"], total=total, blind=batch["blind"]))
        while True:
            key = ask("[u]seful  [m]arginal  [n]ot useful  needs [c]ontext  [s]kip  [q]uit: ").strip().lower()
            if key in KEYS or key in ("s", "q"):
                break
            say("Please answer u, m, n, c, s or q.")
        if key == "q":
            break
        if key == "s":
            continue
        reason = ask("Reason (optional, Enter to skip): ")
        problems = verify(it)
        if problems:
            say("Not recorded: " + "; ".join(problems))
            continue
        rec = annotation(batch, it["position"], reviewer=reviewer, answer=KEYS[key], reason=reason,
                         os_user=os_user, tool_version=tool_version)
        append_annotation(path, rec)
        recorded += 1
        say(f"Recorded {rec['annotation_id'][:12]} for item {it['position']}.")
    return recorded
