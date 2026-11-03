#!/usr/bin/env python3
"""Apply audited TYPE overrides to a document's cached extraction, without replaying.

Requirement: "Preserve support for cached historical payloads and audited
type overrides used during repair."

WHY THIS EXISTS
---------------
A cached extraction produced under an older type contract can carry a type the
contract now admits a better answer for. For example, two named essays —
`The End of Hoop Jumping` and `How To Think About Science` — were cached as
`Project` because no schema admitted `Document`. Widening the schema does not
rewrite what is already cached, and re-running the extractor to get a better type
means paying for the whole document again AND accepting a different extraction.

So: take the cached payload as-is, apply exactly the overrides an operator has
audited, validate the result against the live schema, run the PRODUCTION merge over
it, and write it out as a curated payload for `check_document_integrity.py --payload`.

THE OUTPUT IS A VALIDATION ARTIFACT, NOT A REPLAY INPUT
-------------------------------------------------------
Nothing consumes this file except the read-only gate. `extract_deep_documents.py`
has no `--payload` flag, on purpose: a replay reads the stored windows, and the only
way an audited override can reach a replay is by reaching the cache through a
mechanism (transactional rollback / replay reconciliation) that is yet to be
defined. Until then this file answers one question — "would the identity and quality
gates pass with these overrides applied?" — and asserts nothing about what a replay
would write. `curation.document_rid` is stamped so the gate can refuse to check it
against a different document.

Nothing here writes to the database. Every statement is a SELECT.

WHAT IT DELIBERATELY REFUSES
----------------------------
  * an override naming a type the contract does not admit
  * an override naming a label the cached payload does not contain (a typo that
    silently did nothing is exactly the failure to prevent)
  * an override whose label is ALREADY that type (a no-op dressed as a decision)

WHAT IT DOES NOT CHECK, AND WHY
-------------------------------
It does not ask whether the new type conflicts with a live entity of another type.
That is the identity gate's job and it is genuinely separate: retyping a payload
endpoint that is already bound to a live row of the old type converts a clean
binding into a `cross_type_conflict`, which is CORRECT — such a label needs an
operator `/entities/retype` on the graph, not a payload edit. Run
`check_document_integrity.py --payload <out>` afterwards; that is the check.

Usage:
    python scripts/curate_cached_payload.py \
        --document-rid substack-corpus:michaelgarfield:scenius \
        --retype "The End of Hoop Jumping=Document" \
        --out /path/to/scenius-curated.json

Exit codes
    0  curated payload written (or --dry-run reported cleanly)
    2  an override was rejected, or the document has no cached extraction
    3  the curated payload does not validate against the schema
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import document_extraction_contract as contract  # noqa: E402
from api import extraction_merge as merge  # noqa: E402
from api.resolution_primitives import normalize_entity_text  # noqa: E402

POSTGRES_URL = os.getenv(
    "POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")

SCHEMAS = {
    "standard": REPO_ROOT / "scripts/schemas/deep_extraction_doc_v1.schema.json",
    "thorough": REPO_ROOT / "scripts/schemas/deep_extraction_doc_v2.schema.json",
}


def parse_retype(values) -> dict:
    out: dict = {}
    for raw in values or []:
        if "=" not in raw:
            raise SystemExit(f"--retype expects LABEL=Type, got {raw!r}")
        label, _, etype = raw.partition("=")
        label, etype = label.strip(), etype.strip()
        if not label or not etype:
            raise SystemExit(f"--retype expects LABEL=Type, got {raw!r}")
        if not contract.is_extractable(etype):
            raise SystemExit(
                f"--retype {raw!r}: {etype!r} is not admitted by "
                f"{contract.DOCUMENT_TYPE_CONTRACT_VERSION}. Admitted: "
                f"{', '.join(contract.DOCUMENT_ENTITY_TYPE_NAMES)}")
        if label in out and out[label] != etype:
            raise SystemExit(f"--retype names {label!r} twice with different types")
        out[label] = etype
    return out


async def load_windows(conn, document_rid: str) -> list:
    rows = await conn.fetch(
        "SELECT window_index, raw_json FROM document_window_extractions "
        " WHERE document_rid = $1 AND raw_json IS NOT NULL ORDER BY window_index",
        document_rid)
    return [(r["window_index"], json.loads(r["raw_json"])) for r in rows]


def apply_overrides(windows: list, retype: dict) -> tuple:
    """Return (payload, applied) with every override applied by NORMALIZED label.

    Matching on the normalized label, not the raw string, because that is what the
    merge and the identity contract key on; an override that matched only the exact
    spelling would miss the very surface-form variation the merge exists to absorb.
    """
    want = {normalize_entity_text(k): v for k, v in retype.items()}
    applied: dict = {k: {"type": v, "entities": 0, "windows": []} for k, v in retype.items()}
    by_norm_label = {normalize_entity_text(k): k for k in retype}

    out_windows = []
    for idx, data in windows:
        d = json.loads(json.dumps(data))   # deep copy; never mutate the cached read
        for ent in d.get("entities") or []:
            norm = normalize_entity_text(ent.get("name") or "")
            if norm in want:
                rec = applied[by_norm_label[norm]]
                if ent.get("type") != want[norm]:
                    ent["type"] = want[norm]
                    rec["entities"] += 1
                    if idx not in rec["windows"]:
                        rec["windows"].append(idx)
                else:
                    rec.setdefault("already", 0)
                    rec["already"] = rec.get("already", 0) + 1
        out_windows.append((idx, d))
    return out_windows, applied


def merge_for_gate(windows: list) -> dict:
    """The payload shape `check_document_integrity.py --payload` consumes.

    This is the PRODUCTION merge (`api.extraction_merge`), not a re-implementation:
    entities coerced by normalized name with the coercions reported, facts
    de-duplicated with their per-window `chunk_ranges`, discourse merged. An earlier
    version de-duplicated by (normalized name, type) here — the same divergence the
    gate had (review finding M7) — so a curated payload could block on a type
    disagreement the extractor would have folded.
    """
    raws = [d for _idx, d in windows]
    payload = merge.merge_extractions(raws)
    payload["discourse"] = merge.merge_discourse(raws)
    return payload


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--document-rid", required=True)
    ap.add_argument("--retype", action="append", default=[],
                    help="LABEL=Type; repeatable. An audited type override.")
    ap.add_argument("--out", help="where to write the curated payload")
    ap.add_argument("--tier", default="thorough", choices=tuple(SCHEMAS))
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change and write nothing")
    args = ap.parse_args()

    retype = parse_retype(args.retype)
    if not retype:
        print("no --retype given: nothing to curate", file=sys.stderr)
        return 2
    if not args.out and not args.dry_run:
        print("--out is required unless --dry-run", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(POSTGRES_URL)
    try:
        windows = await load_windows(conn, args.document_rid)
    finally:
        await conn.close()
    if not windows:
        print(f"no cached window extractions for {args.document_rid}", file=sys.stderr)
        return 2

    curated_windows, applied = apply_overrides(windows, retype)

    # Refuse silently-inert overrides. A --retype that matched nothing is a typo
    # that looks like a decision; reporting success on it is the exact shape of
    # failure this check exists to prevent.
    inert = {k: v for k, v in applied.items() if not v["entities"]}
    if inert:
        for label, rec in sorted(inert.items()):
            reason = ("already that type in every window"
                      if rec.get("already") else "label not present in the cached payload")
            print(f"REJECTED --retype {label!r}={rec['type']}: {reason}", file=sys.stderr)
        print(f"\n{len(inert)} of {len(retype)} override(s) changed nothing; refusing to "
              f"write a curated payload that would read as audited but is not.",
              file=sys.stderr)
        return 2

    # Every window must still validate — a curated payload that a replay cannot
    # parse is worse than no curation.
    schema = json.loads(SCHEMAS[args.tier].read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    for idx, d in curated_windows:
        errors = sorted(validator.iter_errors(d), key=lambda e: list(e.path))
        if errors:
            print(f"curated window {idx} does not validate against "
                  f"{SCHEMAS[args.tier].name}:", file=sys.stderr)
            for e in errors[:5]:
                print(f"  {list(e.path)}: {e.message[:200]}", file=sys.stderr)
            return 3

    payload = merge_for_gate(curated_windows)
    payload["curation"] = {
        "source": "document_window_extractions",
        "document_rid": args.document_rid,
        "windows": [idx for idx, _ in curated_windows],
        "curated_at": datetime.now(timezone.utc).isoformat(),
        "type_contract_version": contract.DOCUMENT_TYPE_CONTRACT_VERSION,
        "schema_file": SCHEMAS[args.tier].name,
        "audited_type_overrides": {k: v["type"] for k, v in applied.items()},
        "override_detail": applied,
        "merge": "api.extraction_merge.merge_extractions (production)",
        "note": ("Audited type overrides applied to the CACHED extraction. No window "
                 "was re-extracted and no database row was written. VALIDATION "
                 "ARTIFACT for check_document_integrity.py --payload only; not a "
                 "replay input (see #69)."),
    }

    for label, rec in sorted(applied.items()):
        print(f"retyped {label!r} -> {rec['type']} in {rec['entities']} entity record(s) "
              f"across window(s) {rec['windows']}")
    print(f"payload: {len(payload['entities'])} entities, {len(payload['facts'])} facts, "
          f"{len(payload['discourse'])} discourse moves")

    if args.dry_run:
        print("--dry-run: nothing written")
        return 0

    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
