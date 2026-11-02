"""Cross-window merge for deep document extraction — the ONE production merge.

Lifted out of scripts/extract_deep_documents.py on 2026-09-14 so that the operator
gate (scripts/check_document_integrity.py) and the payload curator
(scripts/curate_cached_payload.py) run the SAME function the extractor runs,
rather than a re-implementation that de-duplicated by (name, type) while this one
coerces by name. Measured before the move: the gate BLOCKED 100 of 1,755 cached
documents that production would have ingested, and reported "0 cross-window
coercion(s)" structurally, because it never called the code that produces them
(independent review of PR #66, finding M7).

THE MERGE KEY IS THE IDENTITY KEY
---------------------------------
Entities are keyed on `merge_key()`, which IS `normalize_entity_text` — the same
function api/ingest_identity.py keys endpoint identity on, and the same one the
registry's `koi_normalize_entity_text()` mirrors (migration 124). A private
normalizer lived here before (lower + whitespace only) and disagreed with the
identity key on `-` and `_`: `omni-mapping` and `omni mapping` survived the merge
as two typed records, and the identity gate then raised `payload_type_conflict`
on a disagreement the merge existed to fold (review finding M6; 7 cached
documents). One key, or the two layers argue about what an entity is.

The extractor module re-exports everything here under its old names, so existing
callers and tests (`edd.merge_extractions`, `edd._norm`) still hit this code.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from api import document_extraction_contract as contract
from api.resolution_primitives import normalize_entity_text

logger = logging.getLogger(__name__)


def merge_key(s: Optional[str]) -> str:
    """The canonical entity key. Identical to the identity key by construction."""
    return normalize_entity_text(s or "")


def merge_extractions(per_window: List[dict], windows: Any = None) -> Dict[str, Any]:
    """Union entities (type-priority coercion) + dedup facts across windows.

    Entities are keyed on the NORMALIZED NAME ONLY, deliberately: the same entity
    typed differently by two windows must become one entity, which is what
    TYPE_PRIORITY resolves. Keying on (name, type) instead would turn every
    cross-window disagreement into a duplicate pair, which is worse.

    What that costs — and what `type_conflicts` buys back (issue #68 req. 4) — is
    that the coercion used to be INVISIBLE. Two genuinely different things sharing
    one normalized name (the `Freedom` app and the abstract `freedom`) get silently
    folded into whichever type ranks higher, and the caller sees a clean entity
    list with no sign a decision was made. Every coercion, and every off-contract
    type seen, is reported in the merged payload so the identity gate and the run
    receipt can show it. Nothing is suppressed; nothing is guessed.

    Facts keep BOTH the widened `chunk_range` (for callers that want one span) and
    `chunk_ranges`, the list of ranges the windows actually cited. The quality
    layer reads the list: the widened span made `chunk_coverage` report 456/456
    where 173 chunks were cited (review finding M1), because a fact seen in window
    1 and window 12 was credited with citing everything in between.

    `windows` is accepted for signature compatibility and unused.
    """
    ent: Dict[str, Dict[str, Any]] = {}
    type_conflicts: Dict[str, Dict[str, Any]] = {}
    unknown_types: Dict[str, int] = {}

    warned: set = set()

    def _note_if_off_contract(etype: Optional[str], name: str) -> None:
        """Count EVERY entity record carrying a type the contract does not admit.

        Separate from the priority lookup on purpose: folding it in made the count
        depend on how many comparisons a label happened to be involved in, which is
        an artefact of window layout rather than a fact about the payload. A cached
        payload from an older contract version, or a hand-corrected one, can
        legitimately carry such a type — it must not vanish into the floor without
        anyone being told, and the number reported must mean something.
        """
        if contract.is_extractable(etype):
            return
        key = str(etype)
        unknown_types[key] = unknown_types.get(key, 0) + 1
        if key not in warned:                     # once per distinct type, not per record
            warned.add(key)
            logger.warning(
                "entity %r carries type %r, which is not in %s — ranked below every "
                "admitted type for cross-window merging", name, etype,
                contract.DOCUMENT_TYPE_CONTRACT_VERSION)

    def _priority(etype: Optional[str]) -> int:
        pri, _known = contract.priority_for(etype)
        return pri

    for ex in per_window:
        for e in ex.get("entities", []):
            k = merge_key(e["name"])
            if not k:
                continue
            cur = ent.get(k)
            etype = e["type"]
            _note_if_off_contract(etype, e["name"])
            if cur is None:
                ent[k] = {"name": e["name"], "type": etype,
                          "first_seen_chunk": e["first_seen_chunk"], "mention_count": e["mention_count"]}
            else:
                if etype != cur["type"]:
                    incoming, incumbent = _priority(etype), _priority(cur["type"])
                    kept, dropped = ((etype, cur["type"]) if incoming > incumbent
                                     else (cur["type"], etype))
                    rec = type_conflicts.setdefault(
                        k, {"normalized": k, "name": cur["name"], "kept": kept,
                            "dropped": [], "occurrences": 0})
                    rec["kept"] = kept
                    if dropped not in rec["dropped"]:
                        rec["dropped"].append(dropped)
                    rec["occurrences"] += 1
                    if incoming > incumbent:
                        cur["type"] = etype
                cur["first_seen_chunk"] = min(cur["first_seen_chunk"], e["first_seen_chunk"])
                cur["mention_count"] += e["mention_count"]
                if len(e["name"]) > len(cur["name"]):      # prefer most-specific surface form
                    cur["name"] = e["name"]

    facts: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for ex in per_window:
        for f in ex.get("facts", []):
            obj_key = merge_key(f.get("object")) if f.get("object") else f"lit:{merge_key(f.get('object_literal'))}"
            key = (merge_key(f["subject"]), f["predicate"], obj_key)
            cr = list(f.get("chunk_range") or [0, 0])
            if key not in facts:
                facts[key] = {**f, "chunk_range": list(cr), "chunk_ranges": [list(cr)]}
            else:
                prev = facts[key]
                prev["chunk_range"] = [min(prev["chunk_range"][0], cr[0]), max(prev["chunk_range"][1], cr[1])]
                if list(cr) not in prev["chunk_ranges"]:
                    prev["chunk_ranges"].append(list(cr))
                if f.get("confidence") == "high":
                    prev["confidence"] = "high"

    type_map = {k: v["type"] for k, v in ent.items()}
    if type_conflicts:
        logger.warning(
            "cross-window type coercion on %d label(s): %s", len(type_conflicts),
            "; ".join(f"{c['name']!r} kept {c['kept']} over {'/'.join(c['dropped'])}"
                      for c in list(type_conflicts.values())[:6]))
    return {"entities": list(ent.values()), "facts": list(facts.values()),
            "type_map": type_map,
            "type_conflicts": sorted(type_conflicts.values(), key=lambda c: c["normalized"]),
            "unknown_types": dict(sorted(unknown_types.items())),
            "type_contract_version": contract.DOCUMENT_TYPE_CONTRACT_VERSION}


# ── Discourse merge (thorough tier only) ───────────────────────────────────────────

# Document ARGUMENT taxonomy (plan §Q2; enforced by migration 104's source-aware CHECK).
# Session discourse keeps its own enums (session_discourse_moves rows with
# source_type='session'); document moves use these.
VALID_MOVE_TYPES = {"thesis", "claim", "evidence", "premise",
                    "counterpoint", "open_question", "definition", "implication"}
VALID_MOVE_STATUS = {"asserted", "supported", "contested", "speculative", "open", "deferred"}


def merge_discourse(per_window: List[dict]) -> List[Dict[str, Any]]:
    """Dedup discourse moves across windows by (move_type, normalized title).
    Widen chunk_range; backfill a missing detail/status/supports from a later
    window. Deterministic order (insertion) so uuid5 ids are stable across re-runs."""
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ex in per_window:
        for m in ex.get("discourse", []) or []:
            title = (m.get("title") or "").strip()
            if not title:
                continue
            key = (m.get("move_type"), merge_key(title))
            cr = m.get("chunk_range") or [0, 0]
            if key not in out:
                out[key] = {**m, "title": title[:400], "chunk_range": list(cr)}
            else:
                prev = out[key]
                prev["chunk_range"] = [min(prev["chunk_range"][0], cr[0]),
                                       max(prev["chunk_range"][1], cr[1])]
                if not prev.get("detail") and m.get("detail"):
                    prev["detail"] = m["detail"]
                if not prev.get("status") and m.get("status"):
                    prev["status"] = m["status"]
                if not prev.get("supports") and m.get("supports"):
                    prev["supports"] = m["supports"]
    return list(out.values())
