#!/usr/bin/env python3
"""Local review of cross-source discourse candidates.

Every database read runs in one READ ONLY transaction. Nothing here writes to the
database, admits, declines or hides a candidate, or changes a retrieval limit.
The only files written are the ones you name: a batch file, or for a filtered
batch a reviewer file and a binding file (batch), and an append-only annotation
file (annotate, review).

  queue        open candidates in lanes (cross_source, same_work_mapped,
               same_work_suggested), each ordered source_diverse (default) or by
               fused score. Text by default, --format json for JSON. --show-scores
               shows each sighting's run, search, pick and tier.
  batch        a self-contained review batch: the exact text and versions shown,
               no retrieval scores, ranks, picks or tiers. From --candidate-id, or
               the first --from-queue N of the cross_source lane. Without filters it
               is one file, as before. With --run-id, --seen-in-run or --tier it is
               two: --out, the reviewer file (all a reviewer receives: question,
               A and B with their sources, no run, rank, pick, tier, anchor or
               selection, in a salted order), and --binding-out (required, and
               outside the reviewer file's directory), the operator's binding
               file holding the candidates, runs, anchors, tiers and the
               selection. Never give a reviewer the binding file.

  Filters (queue and batch):
    --run-id RUN        candidates first discovered by RUN (the run stored on the
                        candidate). Reviewed as stored: A is that first sighting's
                        query. Same meaning as `koi-discourse candidates --run-id`.
    --seen-in-run RUN   every sighting by RUN, first or again (rediscovery). Each is
                        reviewed from its own query: A is the claim whose search
                        found B in that sighting, so a pair met from both ends is
                        two items. A sighting whose query was not recorded is
                        counted, not listed.
    --tier TIER         first_five (picks 1-5), additional (6 on) or unknown: the
                        tier of each --seen-in-run sighting, otherwise of each
                        candidate's first sighting.
  show         render a batch (blind). --verify re-checks it against the database
               (a reviewer file needs --binding for that).
  annotate     record one answer to "Is B useful to someone studying A?" for one
               batch item: useful | marginal | not_useful | needs_context, with an
               optional reason. Refused if the candidate's text or source version
               changed since the batch was made. A reviewer file needs its
               --binding file and the --item-id the answer was given for, and is
               refused if they do not match. Its annotation file must be outside
               the reviewer file's directory.
  review       walk a batch interactively and record answers as annotate does.
  annotations  summarise an annotation file (current answers per item).

Mappings (--mappings, or KOI_DISCOURSE_REVIEW_MAPPINGS): an operator JSON file
{"same_work": {work_id: [rid, ...]}, "authors": {rid: [name, ...]}}. Mapped facts
drive the same-work lane and authorship cues. Cues are never dependence judgments.
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import discourse_review as dr  # noqa: E402

DSN = "postgresql://koi:@localhost:5432/personal_koi?application_name=koi-discourse-review"


async def _readonly(dsn: str, fn):
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(readonly=True):
            return await fn(conn)
    finally:
        await conn.close()


def _tool_version() -> Optional[str]:
    try:
        out = subprocess.run(["git", "-C", str(REPO_ROOT), "describe", "--always", "--dirty"],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def _mappings(path: Optional[str]) -> Dict[str, Any]:
    return dr.load_mappings(path or os.environ.get("KOI_DISCOURSE_REVIEW_MAPPINGS"))


def _selection(args) -> Optional[Dict[str, Any]]:
    """The filters a batch was made with, or None when none was given."""
    if not (args.run_id or args.seen_in_run or args.tier != "all"):
        return None
    return {"run_ids": sorted(args.run_id) if args.run_id else None,
            "seen_in_runs": sorted(args.seen_in_run) if args.seen_in_run else None,
            "tier": None if args.tier == "all" else args.tier}


async def _items(conn, args, candidate_ids=None):
    rows = await dr.candidate_rows(conn, run_ids=args.run_id or None, candidate_ids=candidate_ids,
                                   seen_in_runs=args.seen_in_run or None)
    items, unanchored = dr.review_items(rows, seen_in_runs=args.seen_in_run or None)
    selected = [it for it in items if args.tier == "all" or it["tier"] == args.tier]
    return selected, dr.tier_counts(items), unanchored


async def _queue(conn, args, mappings):
    items, counts, unanchored = await _items(conn, args)
    suggested = await dr.suggested_same_work(conn)
    return (await conn.fetchval("SELECT current_database()"), dr.queue(items, mappings, suggested, args.order),
            counts, unanchored)


def cmd_queue(args) -> None:
    mappings = _mappings(args.mappings)
    database, lanes, tiers, unanchored = asyncio.run(_readonly(args.dsn, lambda c: _queue(c, args, mappings)))
    wanted = dr.LANES if args.lane == "all" else (args.lane,)
    blind = not args.show_scores
    unit = "sightings" if args.seen_in_run else "candidates"
    if args.format == "json":
        print(json.dumps({"database": database, "read_only": True, "order": args.order, "mappings": mappings["path"],
                          "label": "candidates (unreviewed): similarity proposals, not judgments",
                          "selection": _selection(args), "unit": unit, "tier_counts": tiers,
                          "sightings_without_a_recorded_query": unanchored, "tier_note": dr.TIER_NOTE,
                          "counts": {k: len(v) for k, v in lanes.items()},
                          "lanes": {k: [dr.view(r, blind=blind) for r in lanes[k][:args.limit]] for k in wanted}},
                         indent=1, default=str))
        return
    print(f"{database}  order={args.order}  tier={args.tier}  " + "  ".join(f"{k}={len(v)}" for k, v in lanes.items()))
    print(f"{unit} by tier: " + "  ".join(f"{k}={v}" for k, v in tiers.items())
          + (f"  (not listed: {unanchored} sightings without a recorded query)" if unanchored else ""))
    for k in wanted:
        print(f"\n== lane {k}: {len(lanes[k])} candidates" + (f" (showing {args.limit})" if args.limit < len(lanes[k]) else ""))
        for i, r in enumerate(lanes[k][:args.limit], start=1):
            print("\n" + dr.render(r, position=i, total=len(lanes[k]), blind=blind))


def _inside(path: Path, directory: Path) -> bool:
    """Whether path is in directory or below it. Compared by file identity, not by spelling,
    so a case-insensitive volume (macOS) or a symlink cannot make one folder look like two."""
    if not directory.exists():
        return False
    p = path.resolve()
    return any(a.exists() and os.path.samefile(a, directory) for a in (p, *p.parents))


def _write_new(files: List[tuple]) -> None:
    """Create each (path, bytes) exclusively, never overwriting. On any failure, remove every
    file this call created, partial ones included, and refuse."""
    created: List[Path] = []
    try:
        for path, data in files:
            with open(path, "xb") as f:
                created.append(path)
                f.write(data)
    except BaseException as e:
        for path in created:
            path.unlink(missing_ok=True)
        if isinstance(e, OSError):
            raise SystemExit(f"refused: cannot write {e.filename or ''}: {e.strerror or e}; nothing was kept")
        raise


def cmd_batch(args) -> None:
    out = Path(args.out)
    selection = _selection(args)
    binding_out = getattr(args, "binding_out", None)
    if selection is None and binding_out:
        raise SystemExit("--binding-out is only for a filtered batch (--run-id, --seen-in-run, --tier)")
    if selection is not None:
        if not binding_out:
            raise SystemExit("a filtered batch is a reviewer file and a binding file: name the binding file with "
                             "--binding-out, outside the reviewer file's directory")
        if _inside(Path(binding_out), out.parent):
            raise SystemExit("--binding-out must be outside the reviewer file's directory, which a reviewer may "
                             "read in full")
    for path in [out] + ([Path(binding_out)] if selection is not None else []):
        if path.exists():
            raise SystemExit(f"refusing to overwrite {path}")
    mappings = _mappings(args.mappings)

    async def body(conn):
        selected, _, _ = await _items(conn, args, candidate_ids=args.candidate_id or None)
        suggested = await dr.suggested_same_work(conn)
        lanes = dr.queue(selected, mappings, suggested, args.order)
        if args.candidate_id:
            listed = [r for k in dr.LANES for r in lanes[k]]
            missing = [c for c in args.candidate_id if not any(r["candidate_id"] == c for r in listed)]
            if missing:
                raise SystemExit(f"not open, not visible{' or outside the selection' if selection else ''}: "
                                 f"{', '.join(missing)}")
            items = [r for c in args.candidate_id for r in listed if r["candidate_id"] == c]
        else:
            items = lanes["cross_source"][:args.from_queue]
        return await conn.fetchval("SELECT current_database()"), items
    database, items = asyncio.run(_readonly(args.dsn, body))
    if dr.needs_split(items, selection):
        raw, binding = dr.make_split_batch(items, database=database, mappings=mappings, note=args.note,
                                           selection=selection)
        bpath = Path(binding_out)
        _write_new([(out, raw), (bpath, (json.dumps(binding, indent=1, sort_keys=True, ensure_ascii=False)
                                         + "\n").encode("utf-8"))])
        print(f"wrote {out}: {len(binding['items'])} items, reviewer_batch_id {binding['reviewer_batch_id'][:12]}. "
              "Give reviewers only this file, in a directory that holds nothing else. Their answers must name "
              "each item's id (shown as 'item <id>').")
        print(f"wrote {bpath}: the binding file (candidates, runs, anchors, tiers, selection), binding_id "
              f"{binding['binding_id'][:12]}. Operator only: annotate and review need it; reviewers must not see it.")
        return
    batch = dr.make_batch(items, database=database, blind=True, mappings=mappings, note=args.note, selection=selection)
    _write_new([(out, (json.dumps(batch, indent=1, ensure_ascii=False) + "\n").encode("utf-8"))])
    print(f"wrote {out}: {len(batch['items'])} items, batch_id {batch['batch_id'][:12]}")


def _load_batch(path: str, binding: Optional[str] = None, *, need_binding: bool = False) -> Dict[str, Any]:
    """A batch file. A one-file batch is returned as it is (a binding file is refused for it).
    A reviewer file is checked against its binding file when one is given or needed
    (dr.bind_split); without one it is returned for display only."""
    try:
        raw = Path(path).read_bytes()
    except OSError as e:
        raise SystemExit(f"refused: cannot read batch file {path}: {e.strerror or e}")
    try:
        schema = json.loads(raw).get("schema")
    except (ValueError, AttributeError):
        raise SystemExit(f"{path} is not a batch file")
    if schema == dr.BATCH_SCHEMA:
        if binding:
            raise SystemExit(f"{path} is a one-file batch; it has no binding file")
        return json.loads(raw)
    if schema != dr.REVIEWER_SCHEMA:
        raise SystemExit(f"{path} is neither a {dr.BATCH_SCHEMA} nor a {dr.REVIEWER_SCHEMA} file")
    if binding is None and not need_binding:
        reviewer = dr.read_reviewer_file(raw)
        return {"schema": dr.REVIEWER_SCHEMA, "format": "reviewer", "batch_id": reviewer["reviewer_batch_id"],
                "blind": True, "question_text": reviewer["question_text"], "items": reviewer["items"]}
    if binding is None:
        return dr.bind_split(raw, None)                     # refuses: the binding file is required
    if _inside(Path(binding), Path(path).parent):
        raise SystemExit("refused: the binding file is inside the reviewer file's directory, which a reviewer may "
                         "read in full. Move it out, and treat the batch as no longer blind if a reviewer had it")
    try:
        bound = json.loads(Path(binding).read_bytes())
    except FileNotFoundError:
        raise SystemExit(f"refused: binding file {binding} does not exist")
    except OSError as e:
        raise SystemExit(f"refused: cannot read binding file {binding}: {e.strerror or e}")
    except ValueError:
        raise SystemExit(f"refused: binding file {binding} is not JSON")
    return dr.bind_split(raw, bound)


def _annotations_outside(batch: Dict[str, Any], batch_path: str, annotations: str) -> None:
    """A reviewer file's annotations name candidates, anchors and runs: keep them out of the
    directory handed to reviewers."""
    if batch.get("format") == "split" and _inside(Path(annotations), Path(batch_path).parent):
        raise SystemExit("refused: keep the annotation file outside the reviewer file's directory; "
                         "annotations name candidates, anchors and runs")


def _carries_retrieval_metadata(batch: Dict[str, Any]) -> bool:
    return batch.get("schema") == dr.BATCH_SCHEMA and (
        batch.get("selection") is not None or any(i.get("anchor") for i in batch["items"]))


def _verify(dsn: str, items: List[Dict[str, Any]]) -> Dict[int, List[str]]:
    async def body(conn):
        rows = {r["candidate_id"]: r for r in await dr.candidate_rows(conn, candidate_ids=[i["candidate_id"] for i in items])}
        return {i["position"]: dr.binding_problems(rows.get(i["candidate_id"]), i) for i in items}
    return asyncio.run(_readonly(dsn, body))


def cmd_show(args) -> None:
    batch = _load_batch(args.batch, args.binding, need_binding=args.verify)
    items = [i for i in batch["items"] if args.item in (None, i["position"])]
    problems = _verify(args.dsn, items) if args.verify else {}
    print(f"batch {batch['batch_id'][:12]}  {len(batch['items'])} items  question: {batch['question_text']}")
    if _carries_retrieval_metadata(batch):
        print("warning: this one-file batch records a selection or sightings (runs, anchors). It is for the "
              "operator only; do not give it to a reviewer. New batches like it are written as a reviewer file "
              "and a binding file.")
    for it in items:
        print("\n" + dr.render(it, position=it["position"], total=len(batch["items"]), blind=batch["blind"]))
        if args.verify:
            print("verify: " + ("; ".join(problems[it["position"]]) or "unchanged since the batch was made"))


def cmd_annotate(args) -> None:
    batch = _load_batch(args.batch, args.binding, need_binding=True)
    _annotations_outside(batch, args.batch, args.annotations)
    item = next((i for i in batch["items"] if i["position"] == args.item), None)
    if item is None:
        raise SystemExit(f"batch has no item {args.item}")
    if batch.get("format") == "split":
        dr.check_item_id(item, getattr(args, "item_id", None))
    problems = _verify(args.dsn, [item])[item["position"]]
    if problems:
        raise SystemExit("not recorded: " + "; ".join(problems))
    rec = dr.annotation(batch, args.item, reviewer=args.reviewer, answer=args.answer, reason=args.reason,
                        supersedes=args.supersedes, os_user=getpass.getuser(), tool_version=_tool_version())
    dr.append_annotation(args.annotations, rec)
    print(f"recorded {rec['annotation_id'][:12]}: item {args.item} = {args.answer}")


def cmd_review(args) -> None:
    batch = _load_batch(args.batch, args.binding, need_binding=True)
    _annotations_outside(batch, args.batch, args.annotations)
    n = dr.review_loop(batch, reviewer=args.reviewer, path=args.annotations,
                       verify=lambda it: _verify(args.dsn, [it])[it["position"]],
                       os_user=getpass.getuser(), tool_version=_tool_version())
    print(f"{n} answer(s) recorded in {args.annotations}")


def cmd_annotations(args) -> None:
    current = dr.latest(dr.read_annotations(args.annotations))
    if args.batch:
        current = [a for a in current if a["batch_id"] == _load_batch(args.batch)["batch_id"]]
    for a in current:
        anchor = a.get("anchor") or {}
        print(f"{a['position']:>3}  {a['answer']:<13} {a['reviewer']:<12} {a['candidate']['id'][:8]}"
              + (f" (A = stored {anchor['stored_end'].upper()}, run {a['sighting']['run_id']})" if anchor else "")
              + f"  {a['created_at']}  {a['reason'] or ''}")
    print(f"{len(current)} current answer(s)")


def _filters(p) -> None:
    p.add_argument("--run-id", action="append",
                   help="candidates first discovered by this run (repeatable), reviewed as stored")
    p.add_argument("--seen-in-run", action="append",
                   help="every sighting by this run, first or again (repeatable), each reviewed from its own query")
    p.add_argument("--tier", choices=("all",) + dr.TIERS, default="all",
                   help="tier of each --seen-in-run sighting, otherwise of each candidate's first sighting")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    q = sub.add_parser("queue")
    q.add_argument("--dsn", default=DSN)
    _filters(q)
    q.add_argument("--order", choices=dr.ORDERS, default="source_diverse")
    q.add_argument("--lane", choices=("all",) + dr.LANES, default="all")
    q.add_argument("--limit", type=int, default=10)
    q.add_argument("--mappings")
    q.add_argument("--show-scores", action="store_true",
                   help="not blind: show retrieval scores and each sighting's run, search, pick and tier")
    q.add_argument("--format", choices=("text", "json"), default="text")
    b = sub.add_parser("batch")
    b.add_argument("--dsn", default=DSN)
    b.add_argument("--candidate-id", action="append")
    _filters(b)
    b.add_argument("--from-queue", type=int, default=10)
    b.add_argument("--order", choices=dr.ORDERS, default="source_diverse")
    b.add_argument("--mappings")
    b.add_argument("--note")
    b.add_argument("--out", required=True)
    b.add_argument("--binding-out", help="a filtered batch's binding file (required for one), outside the reviewer "
                                         "file's directory")
    s = sub.add_parser("show")
    s.add_argument("--dsn", default=DSN)
    s.add_argument("--batch", required=True)
    s.add_argument("--item", type=int)
    s.add_argument("--verify", action="store_true")
    s.add_argument("--binding", help="the reviewer file's binding file (needed with --verify)")
    a = sub.add_parser("annotate")
    a.add_argument("--dsn", default=DSN)
    a.add_argument("--batch", required=True)
    a.add_argument("--binding", help="the binding file a reviewer file needs")
    a.add_argument("--item", type=int, required=True)
    a.add_argument("--item-id", help="for a reviewer file: the item id the answer was given for (8+ characters)")
    a.add_argument("--answer", choices=dr.ANSWERS, required=True)
    a.add_argument("--reason")
    a.add_argument("--reviewer", required=True)
    a.add_argument("--annotations", required=True)
    a.add_argument("--supersedes")
    r = sub.add_parser("review")
    r.add_argument("--dsn", default=DSN)
    r.add_argument("--batch", required=True)
    r.add_argument("--binding", help="the binding file a reviewer file needs")
    r.add_argument("--reviewer", required=True)
    r.add_argument("--annotations", required=True)
    n = sub.add_parser("annotations")
    n.add_argument("--dsn", default=DSN)
    n.add_argument("--annotations", required=True)
    n.add_argument("--batch")
    args = ap.parse_args()
    try:
        {"queue": cmd_queue, "batch": cmd_batch, "show": cmd_show, "annotate": cmd_annotate,
         "review": cmd_review, "annotations": cmd_annotations}[args.cmd](args)
    except dr.ReviewError as e:
        raise SystemExit(f"refused: {e}")


if __name__ == "__main__":
    main()
