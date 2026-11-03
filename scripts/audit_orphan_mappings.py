#!/usr/bin/env python3
"""Audit orphaned entity_rid_mappings rows (vault_path → file missing).

Root cause: the backend stores entity_rid_mappings.vault_path at /register-entity
and returns it from /resolve-to-vault & /entities/mentioned-in WITHOUT any
os.path.exists check. Deleting / relocating / never-creating a note leaves an
orphan row that is still reported as an "exact match", producing dangling
wikilinks and phantom resolutions downstream (e.g. People/<name>,
Organizations/<org>).

This script is READ-ONLY by default. It classifies every orphan into:

  - relocated    : a file with the same basename exists elsewhere in the vault
                   → FIX by updating vault_path (don't delete the mapping/links)
  - test-junk    : Tests/ scratch paths → safe to delete the mapping
  - doc-orphan   : a Meetings/ or other document note that's simply gone
                   → delete mapping (and optionally its document_entity_links)
  - entity-orphan: a content entity (People/Orgs/Projects/Concepts/Locations/
                   SoftwareApplications) with no note anywhere → the resolution-
                   poisoning class; delete the mapping so it stops resolving to a
                   phantom path (registry entity row itself is left intact)

Pass --prune to ACTUALLY delete test-junk + doc-orphan + entity-orphan mappings
(relocated rows are repaired via vault_path UPDATE, never deleted). --prune
requires --confirm and prints every statement it runs. Nothing is written
without both flags.

Usage:
    python3 scripts/audit_orphan_mappings.py                 # report only
    python3 scripts/audit_orphan_mappings.py --json out.json # machine-readable
    python3 scripts/audit_orphan_mappings.py --prune --confirm   # apply
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

VAULT = Path(os.environ.get("OBSIDIAN_VAULT_PATH", str(Path.home() / "Documents" / "Notes"))).expanduser()
DB = os.environ.get("PERSONAL_KOI_DB", "personal_koi")

ENTITY_FOLDERS = ("People/", "Organizations/", "Projects/", "Concepts/",
                  "Locations/", "SoftwareApplications/", "Bioregions/")


def _psql(sql: str) -> str:
    """Run a SQL statement via psql, return stdout. Raises on error."""
    out = subprocess.run(
        ["psql", "-d", DB, "-tAF\t", "-c", sql],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"psql failed: {out.stderr.strip()}")
    return out.stdout


def _note_exists(rel: str) -> bool:
    rel = rel[:-3] if rel.endswith(".md") else rel
    return (VAULT / f"{rel}.md").exists() or (VAULT / rel).exists()


def _basename_index() -> dict[str, list[str]]:
    """Map note basename (stem) → list of vault-relative paths that have it."""
    idx: dict[str, list[str]] = defaultdict(list)
    for p in VAULT.rglob("*.md"):
        rel = str(p.relative_to(VAULT))[:-3]
        idx[p.stem].append(rel)
    return idx


def classify(rows: list[dict], basenames: dict[str, list[str]]) -> list[dict]:
    out = []
    for r in rows:
        vp = (r["vault_path"] or "").strip()
        if not vp or _note_exists(vp):
            continue  # not an orphan
        rel = vp[:-3] if vp.endswith(".md") else vp
        stem = Path(rel).name
        relocated_to = [p for p in basenames.get(stem, []) if p != rel]
        if relocated_to:
            cat = "relocated"
        elif rel.startswith("Tests/") or "/test" in rel.lower() or rel.lower().startswith("test"):
            cat = "test-junk"
        elif any(vp.startswith(f) for f in ENTITY_FOLDERS):
            cat = "entity-orphan"
        else:
            cat = "doc-orphan"
        out.append({**r, "category": cat, "relocated_to": relocated_to})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", metavar="PATH", help="write classified orphans to JSON")
    ap.add_argument("--prune", action="store_true", help="delete test-junk/doc-orphan/entity-orphan mappings + repair relocated")
    ap.add_argument("--confirm", action="store_true", help="required with --prune to actually write")
    args = ap.parse_args()

    print(f"vault: {VAULT}\ndb:    {DB}\n")
    raw = _psql("SELECT vault_rid, vault_path, canonical_uri, entity_type, name FROM entity_rid_mappings;")
    rows = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        rows.append(dict(zip(("vault_rid", "vault_path", "canonical_uri", "entity_type", "name"), parts)))

    orphans = classify(rows, _basename_index())
    by_cat: dict[str, list[dict]] = defaultdict(list)
    for o in orphans:
        by_cat[o["category"]].append(o)

    print(f"total mappings: {len(rows)} | orphans: {len(orphans)}\n")
    for cat in ("relocated", "test-junk", "doc-orphan", "entity-orphan"):
        items = by_cat.get(cat, [])
        print(f"=== {cat} ({len(items)}) ===")
        for o in items[:40]:
            extra = f"  → relocated to: {o['relocated_to'][0]}" if o["relocated_to"] else ""
            print(f"  [{o['entity_type'] or '?':<22}] {o['vault_path']}{extra}")
        if len(items) > 40:
            print(f"  … and {len(items) - 40} more")
        print()

    if args.json:
        Path(args.json).write_text(json.dumps(orphans, indent=2))
        print(f"wrote {args.json}")

    if args.prune:
        if not args.confirm:
            sys.exit("--prune requires --confirm; nothing written.")
        deleted = repaired = 0
        for o in orphans:
            rid = o["vault_rid"].replace("'", "''")
            if o["category"] == "relocated":
                newp = o["relocated_to"][0] + ".md"
                sql = f"UPDATE entity_rid_mappings SET vault_path = '{newp}' WHERE vault_rid = '{rid}';"
                print("REPAIR:", sql)
                _psql(sql); repaired += 1
            else:
                sql = f"DELETE FROM entity_rid_mappings WHERE vault_rid = '{rid}';"
                print("DELETE:", sql)
                _psql(sql); deleted += 1
        print(f"\nrepaired {repaired} relocated, deleted {deleted} orphan mappings")


if __name__ == "__main__":
    main()
