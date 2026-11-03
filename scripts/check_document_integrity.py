#!/usr/bin/env python3
"""Run both document-ingest integrity gates against a document, read-only.

Gate 1 checks identity and gate 2 semantic quality. This exists so the gates are
something an operator can RUN before ingesting a paper, not something that was
demonstrated once. A check with no entry point is a check nothing reaches.

Nothing here writes. Every statement is a SELECT or a pure computation.

    GATE 1 — identity
        Collect the payload's complete distinct typed endpoint set, classify each
        against the live registry, and freeze the resolvable ones. Reports every
        blocker (counted BEFORE the display truncation), the bijection over the
        resolvable endpoints, every endpoint a real ingest would pre-register WITH
        THE TYPE it would be minted as, and which endpoints were only found by the
        compatibility read (their stored normalized_text is stale).

    THE PAYLOAD IS THE PRODUCTION MERGE. With no --payload, the stored window
    extractions are merged by `api.extraction_merge.merge_extractions` — the same
    function the extractor calls — so the gate reports what production would do.
    An earlier version de-duplicated entities by (name, type) here while the
    extractor coerces by name, and BLOCKED 100 of 1,755 cached documents that
    production ingests, while reporting "0 cross-window coercion(s)" because it
    never ran the code that produces them (found in an independent review).

    A --payload file is a VALIDATION ARTIFACT: the output of
    scripts/curate_cached_payload.py (already the production merge), checked
    against the live graph exactly as the stored windows would be. It is NOT a
    replay input — no ingest path consumes it, by design, until a sanctioned
    reconciliation mechanism exists. Its `curation.document_rid` must match
    --document-rid or the gate refuses (exit 3) before touching the database.

    GATE 2 — semantic quality
        Deterministic, no model call. Chunk coverage, citation range and support,
        predicate concentration, discourse variety.

Usage:

    # against a live document's stored extraction
    python scripts/check_document_integrity.py --document-rid document:<sha256>

    # against a curated payload file, checked against the live graph
    python scripts/check_document_integrity.py \
        --document-rid document:<sha256> \
        --payload /path/to/<slug>/curated-deep-extraction.json

    # machine-readable
    python scripts/check_document_integrity.py --document-rid ... --json

Exit codes
    0  both gates pass
    1  a gate failed (identity blocked, or semantic verdict is `fail`)
    2  semantic verdict is `review` and --strict-review was given, OR identity is
       `pass_after_preregistration` and --strict-preregistration was given
    3  misconfiguration (document not found, payload unreadable, no chunks)

`review` exits 0 by default. It means "a human should look", not "this is broken",
and making it fail by default would push operators toward running with the gate
off — which is worse than a verdict nobody blocks on.

`pass_after_preregistration` ALSO exits 0, and that is a correction.
The status was introduced precisely to say "the only unbound endpoints are ones
that do not exist yet, which is every first ingest" — and then the exit code
lumped it in with `blocked` anyway, so documents whose
sole finding was a not-yet-existing essay reported exit 1. An operator reading the
number rather than the word would conclude the gate had refused them. Use
--strict-preregistration when you genuinely mean "this payload must create
nothing".
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).parent.parent))
from api import document_extraction_contract as contract  # noqa: E402
from api import extraction_merge as merge  # noqa: E402
from api import ingest_identity as ident  # noqa: E402
from api.extraction_quality import assess_extraction  # noqa: E402
from api.resolution_primitives import (  # noqa: E402
    normalize_alias,
    normalize_entity_text,
)

POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")


class _Window:
    """Minimal stand-in for the extractor's Window, for the coverage denominator."""

    def __init__(self, index, chunk_indices):
        self.index = index
        self.chunk_indices = chunk_indices
        self.chunk_index_base = min(chunk_indices) if chunk_indices else 0


async def _load(conn, document_rid: str):
    chunks = {
        r["chunk_index"]: (r["text"] or "")
        for r in await conn.fetch(
            "SELECT chunk_index, content->>'text' AS text FROM koi_memory_chunks "
            "WHERE document_rid = $1 ORDER BY chunk_index", document_rid)
    }
    wins = await conn.fetch(
        "SELECT window_index, char_start, char_end, chunk_index_base, status, "
        "       raw_json, route_used, provider, model, transport "
        "  FROM document_window_extractions WHERE document_rid = $1 "
        " ORDER BY window_index", document_rid)
    return chunks, list(wins)


def _payload_from_windows(wins) -> dict:
    """The production merge over the cached windows the extractor would merge.

    Same status filter as `extract_deep_document` (only `extracted`/`imported`
    windows with a raw_json are merged; `failed`/`pending` are skipped), same
    `merge_extractions`, same `merge_discourse`. Returns the merged payload plus
    `windows_merged` / `windows_skipped` so the report can say what it covered.
    """
    raws = [json.loads(w["raw_json"]) if isinstance(w["raw_json"], str) else w["raw_json"]
            for w in wins
            if w["raw_json"] and w["status"] in ("extracted", "imported")]
    merged = merge.merge_extractions(raws)
    merged["discourse"] = merge.merge_discourse(raws)
    merged["windows_merged"] = len(raws)
    merged["windows_skipped"] = len(wins) - len(raws)
    return merged


def payload_provenance_problem(payload: dict, document_rid: str):
    """None when the payload may be checked against `document_rid`, else why not.

    A curated payload carries `curation.document_rid`. Running one document's
    curated payload against another document's chunks and windows produces a
    report that reads as that document's and is not; the check is cheap and the
    mistake is one wrong tab-completion away. A payload with no `curation` block
    is not refused — it is reported as unverified provenance.
    """
    cur = payload.get("curation") if isinstance(payload, dict) else None
    if not isinstance(cur, dict):
        return None
    claimed = cur.get("document_rid")
    if claimed and claimed != document_rid:
        return (f"curation.document_rid is {claimed!r} but the gate was run for "
                f"{document_rid!r}; refusing to report one document's payload as another's")
    return None


def _windows_for_coverage(wins, chunks) -> list:
    """Reconstruct each window's chunk band.

    `document_window_extractions` stores `chunk_index_base` but not the band, so
    the band is the half-open interval up to the NEXT window's base. Derived from
    the stored plan rather than recomputed from WINDOW_CHARS, because the plan is
    what the extraction actually used and the env var may have changed since.
    """
    if not wins or not chunks:
        return []
    hi = max(chunks)
    bases = [w["chunk_index_base"] for w in wins]
    out = []
    for i, w in enumerate(wins):
        start = bases[i]
        end = bases[i + 1] - 1 if i + 1 < len(bases) else hi
        out.append(_Window(w["window_index"],
                           [c for c in range(start, end + 1) if c in chunks]))
    return out


async def run_gate(conn, *, document_rid: str, payload=None, tier: str = "thorough",
                   alias_decisions=None, type_decisions=None,
                   strict_review: bool = False, strict_preregistration: bool = False):
    """Both gates, read-only. Returns (report, exit_code). Testable without the CLI."""
    alias_decisions = alias_decisions or {}
    type_decisions = type_decisions or {}

    chunks, wins = await _load(conn, document_rid)
    if not chunks:
        print(f"MISCONFIGURED: no chunks for {document_rid}", file=sys.stderr)
        return {"document_rid": document_rid, "error": "no chunks"}, 3

    if payload is None:
        payload = _payload_from_windows(wins)
        provenance = (f"stored windows, production merge "
                      f"({payload['windows_merged']} merged, {payload['windows_skipped']} skipped)")
    elif isinstance(payload.get("curation"), dict):
        provenance = "curated payload (validation artifact; curation.document_rid verified)"
    else:
        provenance = "payload file with no curation block — provenance NOT verified"

    report: dict = {"document_rid": document_rid, "tier": tier,
                    "payload_provenance": provenance}

    # ── GATE 1 — identity ────────────────────────────────────────────────
    identity_evidence: dict = {"mode": "check-only"}
    try:
        eps = ident.collect_endpoints(payload, normalize=normalize_entity_text,
                                      type_decisions=type_decisions)
        pre = await ident.preflight_endpoints(
            conn, eps, normalize=normalize_entity_text,
            normalize_alias_fn=normalize_alias, alias_decisions=alias_decisions,
            type_decisions=type_decisions)
        identity_evidence["preflight"] = pre.as_evidence()
        # Counted on the FULL list; truncated only for display.
        blockers = [f.as_evidence() for f in pre.blockers]
        # An endpoint the preflight classified MISSING is not unbindable — a real
        # ingest pre-registers it before freezing. Report it with the type it
        # would be minted as: the type is hashed into the URI, so this line is the
        # last moment it can be read before it becomes permanent.
        would_create = [{"name": f.endpoint.name, "type": f.endpoint.entity_type}
                        for f in pre.missing]
        if blockers:
            report["identity"] = {
                "status": "blocked",
                "reason": f"{len(blockers)} of {len(pre.findings)} endpoints cannot be bound",
                "blockers": blockers[:10],
                "blockers_total": len(blockers),
                "would_preregister": would_create,
                "state_counts": pre.counts(),
            }
            identity_evidence["check_only"] = {
                "bijective": False, "resolved_endpoints": 0, "distinct_uris": 0,
                "would_preregister": len(would_create), "blocked": len(blockers)}
        else:
            # Freeze the RESOLVABLE endpoints and assert their bijection. The
            # missing ones each become a fresh exact-only row (the preflight read
            # both normalizations and found none, so the server's Tier-1 exact
            # read finds none either), which cannot collide with a resolved URI or
            # with each other — so a bijection over the resolved set is a
            # bijection over the whole payload. Earlier versions skipped this
            # whenever anything was missing and still exited 0.
            resolved_eps = [f.endpoint for f in pre.resolved]
            frozen = await ident.freeze_endpoint_map(
                conn, resolved_eps, normalize=normalize_entity_text,
                alias_decisions=alias_decisions)
            # freeze_endpoint_map raises on any collapse an alias decision did not
            # sanction, so returning means the bijection holds modulo sanctioned
            # collapses — which are reported, not failed.
            by_uri: dict = {}
            for name, uri in frozen.by_name.items():
                by_uri.setdefault(uri, []).append(name)
            sanctioned = sum(1 for names in by_uri.values() if len(names) > 1)
            identity_evidence["frozen_map"] = frozen.as_evidence()
            identity_evidence["check_only"] = {
                "bijective": True,
                "resolved_endpoints": len(frozen.by_name),
                "distinct_uris": frozen.distinct_uris,
                "would_preregister": len(would_create),
                "sanctioned_alias_collapses": sanctioned,
            }
            report["identity"] = {
                "status": "pass" if not would_create else "pass_after_preregistration",
                "endpoints": len(frozen.by_name),
                "distinct_uris": frozen.distinct_uris,
                "bijective": True,
                "sanctioned_alias_collapses": sanctioned,
                "would_preregister": would_create,
                "state_counts": pre.counts(),
                "drift_recovered": pre.as_evidence()["drift_recovered"],
                "tombstones_recorded": [f.endpoint.name for f in pre.findings
                                        if f.tombstoned_uris],
            }
    except ident.IdentityError as e:
        # collect_endpoints (payload_type_conflict) or the freeze (endpoint_collapse,
        # alias_decision_*, registration_disagreement) refused.
        blockers = list(e.blockers)
        report["identity"] = {
            "status": "blocked",
            "reason": str(e)[:800],
            "blockers": blockers[:10],
            "blockers_total": len(blockers),
            "would_preregister": [],
        }
        identity_evidence["check_only"] = {
            "bijective": False, "resolved_endpoints": 0, "distinct_uris": 0,
            "would_preregister": 0, "blocked": len(blockers)}

    # ── GATE 2 — semantic quality ───────────────────────────────────────
    q = assess_extraction(
        merged=payload, chunks_by_index=chunks,
        windows=_windows_for_coverage(wins, chunks), tier=tier,
        discourse_moves=payload.get("discourse"),
        identity_evidence=identity_evidence)
    report["quality"] = q.as_dict()
    report["identity_evidence"] = identity_evidence

    # ── Type contract, reported but never gated ─────────────────────────
    # `type_conflicts` come from the production merge (stored windows or a
    # curated payload); a hand-made payload without them reports none, and its
    # provenance line above says it was not merged here.
    seen_types: dict = {}
    for e in payload.get("entities") or []:
        seen_types[e.get("type")] = seen_types.get(e.get("type"), 0) + 1
    report["type_contract"] = {
        "version": contract.DOCUMENT_TYPE_CONTRACT_VERSION,
        "admitted": len(contract.DOCUMENT_ENTITY_TYPE_NAMES),
        "types_present": dict(sorted((str(k), v) for k, v in seen_types.items())),
        "off_contract_types": sorted(
            str(k) for k in seen_types if not contract.is_extractable(k)),
        "conflicts": payload.get("type_conflicts") or [],
        "unknown_types": payload.get("unknown_types") or {},
        "audited_type_decisions": type_decisions,
    }

    # ── Provenance, reported but never gated ────────────────────────────
    report["provenance"] = {
        "windows": len(wins),
        "providers": sorted({w["provider"] for w in wins if w["provider"]}),
        "models": sorted({w["model"] for w in wins if w["model"]}),
        "routes": sorted({w["route_used"] for w in wins if w["route_used"]}),
        "windows_without_producer": sum(1 for w in wins if not w["provider"]),
    }

    identity_status = report["identity"]["status"]
    if identity_status not in ("pass", "pass_after_preregistration") or q.status == "fail":
        code = 1
    elif q.status == "review" and strict_review:
        code = 2
    elif identity_status == "pass_after_preregistration" and strict_preregistration:
        code = 2
    else:
        code = 0
    return report, code


def _print_report(report: dict) -> None:
    i = report["identity"]
    q = report["quality"]
    print(f"document: {report['document_rid']}")
    print(f"payload:  {report['payload_provenance']}")
    print(f"\nGATE 1 — identity: {i['status'].upper()}")
    if i["status"] == "blocked":
        print(f"  {i['reason'][:400]}")
        total = i.get("blockers_total", len(i.get("blockers") or []))
        for b in i.get("blockers") or []:
            print(f"  BLOCKER {b.get('state')}: {b.get('name')!r} ({b.get('type')}) "
                  f"existing={b.get('existing') or b.get('live_uris') or b.get('cross_type_uris')}")
        if total > len(i.get("blockers") or []):
            print(f"  … {total - len(i['blockers'])} more blocker(s) not shown "
                  f"({total} total)")
    else:
        print(f"  {i['endpoints']} resolved endpoints -> {i['distinct_uris']} distinct URIs "
              f"(bijective={i['bijective']}"
              + (f", {i['sanctioned_alias_collapses']} sanctioned alias collapse(s)"
                 if i.get("sanctioned_alias_collapses") else "") + ")")
        print(f"  states: {i['state_counts']}")
        if i.get("drift_recovered"):
            print(f"  {i['drift_recovered']} endpoint(s) found ONLY via the #61 "
                  f"compatibility read — their stored normalized_text is stale")
        if i.get("tombstones_recorded"):
            print(f"  tombstones alongside a live row (evidence, not blocks): "
                  f"{i['tombstones_recorded']}")
    pend = i.get("would_preregister") or []
    if pend:
        print(f"  {len(pend)} endpoint(s) a real ingest would pre-register — AS THIS TYPE:")
        for w in pend[:12]:
            print(f"    {w['name']!r} -> {w['type']}")
        if len(pend) > 12:
            print(f"    … {len(pend) - 12} more")
    print(f"\nGATE 2 — semantic quality: {q['status'].upper()}")
    for d in q["dimensions"]:
        print(f"  {d['name']:26} {d['status']:7} {str(d['value']):9} {d['detail'][:66]}")
    tc = report["type_contract"]
    print(f"\ntype contract: {tc['version']} | {tc['admitted']} admitted types"
          f" | {len(tc['conflicts'])} cross-window coercion(s)"
          f" | {len(tc['off_contract_types'])} off-contract type(s)")
    for c in tc["conflicts"][:6]:
        print(f"  coerced {c['name']!r} -> {c['kept']} over {'/'.join(c['dropped'])}")
    if tc["off_contract_types"]:
        print(f"  off-contract types present: {tc['off_contract_types']} — these "
              f"rank below every admitted type in a cross-window merge")
    p = report["provenance"]
    print(f"\nprovenance: {p['windows']} windows | providers={p['providers'] or '-'} "
          f"| models={p['models'] or '-'} | routes={p['routes']}")
    if p["windows_without_producer"]:
        print(f"  {p['windows_without_producer']} window(s) predate migration 125 "
              f"and carry no producer — route_used alone is unreliable (see #64)")


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--document-rid", required=True)
    ap.add_argument("--payload", help="curated extraction JSON (a validation artifact from "
                                      "curate_cached_payload.py); defaults to the production "
                                      "merge of the stored window extractions")
    ap.add_argument("--tier", default="thorough", choices=("rag", "standard", "thorough"))
    ap.add_argument("--json", action="store_true", help="machine-readable report")
    ap.add_argument("--strict-review", action="store_true",
                    help="exit 2 when the semantic verdict is `review`")
    ap.add_argument("--strict-preregistration", action="store_true",
                    help="exit 2 when identity is `pass_after_preregistration`, i.e. "
                         "the payload would create entities that do not exist yet")
    ap.add_argument("--alias-decisions", help="JSON file of audited {label: uri} — "
                                              "WHICH identity a label means")
    ap.add_argument("--type-decisions", help="JSON file of audited {label: entity_type} — "
                                            "WHAT a label is; the only thing that clears "
                                            "a cross_type_conflict or types an endpoint "
                                            "the extractor left untyped")
    args = ap.parse_args()

    alias_decisions = {}
    if args.alias_decisions:
        alias_decisions = json.loads(Path(args.alias_decisions).expanduser().read_text())

    # `preflight_endpoints` accepts type decisions, but nothing passed them: the
    # parameter existed and no caller supplied it, so the blocker's own remedy ("Supply an audited type decision")
    # was unreachable from a command line. A decision naming a type the extractor
    # cannot emit is refused here rather than silently binding nothing.
    type_decisions = {}
    if args.type_decisions:
        type_decisions = json.loads(Path(args.type_decisions).expanduser().read_text())
        bad = {k: v for k, v in type_decisions.items()
               if not contract.is_extractable(v)}
        if bad:
            print(f"MISCONFIGURED: --type-decisions names type(s) outside "
                  f"{contract.DOCUMENT_TYPE_CONTRACT_VERSION}: "
                  f"{json.dumps(bad, sort_keys=True)}. Admitted: "
                  f"{', '.join(contract.DOCUMENT_ENTITY_TYPE_NAMES)}", file=sys.stderr)
            return 3

    payload = None
    if args.payload:
        try:
            payload = json.loads(Path(args.payload).expanduser().read_text())
        except Exception as e:  # noqa: BLE001
            print(f"MISCONFIGURED: cannot read payload: {e}", file=sys.stderr)
            return 3
        problem = payload_provenance_problem(payload, args.document_rid)
        if problem:
            print(f"MISCONFIGURED: {problem}", file=sys.stderr)
            return 3

    conn = await asyncpg.connect(POSTGRES_URL)
    try:
        report, code = await run_gate(
            conn, document_rid=args.document_rid, payload=payload, tier=args.tier,
            alias_decisions=alias_decisions, type_decisions=type_decisions,
            strict_review=args.strict_review,
            strict_preregistration=args.strict_preregistration)
    finally:
        await conn.close()
    if "error" in report:
        return code
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report)
    return code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
