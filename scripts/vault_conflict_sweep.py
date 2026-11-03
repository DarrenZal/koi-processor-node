#!/usr/bin/env python3
"""
Vault Conflict Sweep

An Obsidian vault (--vault or OBSIDIAN_VAULT_PATH) synced through
a file-sync service such as iCloud Drive can have several concurrent writers
(agent sessions writing through vault_write_note/vault_register_entity, scheduled
jobs, Obsidian itself, and other devices on the account). A sync service plus
concurrent writers to the same file produces `NAME (conflict TIMESTAMP).md`
sibling copies rather than a clean merge.

Left alone, these are inert until a vault sync reads them and registers them as
duplicate Meeting/Project/Location entities -- exactly what the strict resolver
is meant to stop creating. Run this sweep on a schedule (a launchd agent or a
cron entry) instead of relying on someone to notice after the fact.

Safety method (the same rules a careful manual triage follows): frontmatter fields
are machine-managed and expected to differ between a pre-write snapshot and the
live file (last_synced, canonical_uri, mentionedIn, etc.) -- only the body
(everything after the closing `---`) is compared. A conflict copy whose body is
identical to, or a strict subset of, the live file's body is the stale
pre-write version and is safe to remove. Anything else is left in place and
flagged as a personal-koi task for manual review, never auto-deleted.

Without --apply it is a dry run: it reports what it finds and changes nothing.
With --apply it backs up and deletes the safe copies and posts a review task to
KOI_TASK_API_URL (default http://localhost:8351/tasks/ingest) for each of the others.
The vault has no default: name it with --vault or OBSIDIAN_VAULT_PATH.

Usage:
    OBSIDIAN_VAULT_PATH=... /path/to/venv/python3 scripts/vault_conflict_sweep.py [--dry-run]
    OBSIDIAN_VAULT_PATH=... /path/to/venv/python3 scripts/vault_conflict_sweep.py --apply
"""

import argparse
import datetime
import json
import os
import re
import shutil
import sys
import urllib.error
import urllib.request

VAULT_PATH = os.path.expanduser(os.getenv("OBSIDIAN_VAULT_PATH", ""))  # no default vault
BACKUP_ROOT = os.path.expanduser(
    os.getenv("KOI_VAULT_CONFLICT_BACKUP_ROOT", "~/.config/personal-koi/vault-conflict-backups")
)
TASK_API_URL = os.getenv("KOI_TASK_API_URL", "http://localhost:8351/tasks/ingest")

CONFLICT_RE = re.compile(r"^(.*) \(conflict \d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}\)\.md$")

# Real notes whose title happens to contain the literal substring "(conflict"
# -- not iCloud sync artifacts. Extend this set if another false positive shows up.
FALSE_POSITIVE_NAMES = {
    "CADAP (Conflict Aftermath Digital Archive Project).md",
}


def log(msg: str) -> None:
    print(f"[{datetime.datetime.now().isoformat()}] {msg}", flush=True)


def split_frontmatter(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return "", text
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            return "\n".join(lines[: i + 1]), "\n".join(lines[i + 1 :])
    return "", text  # no closing --- found; treat whole file as body


def find_conflict_files(vault_path: str) -> list[str]:
    found = []
    for root, _dirs, files in os.walk(vault_path):
        for fn in files:
            if not fn.endswith(".md") or fn in FALSE_POSITIVE_NAMES:
                continue
            if CONFLICT_RE.match(fn):
                found.append(os.path.join(root, fn))
    return sorted(found)


def triage(cpath: str) -> tuple[str, list[str] | None]:
    """Returns (verdict, detail). verdict in {"safe", "review", "no_live", "error"}."""
    m = CONFLICT_RE.match(os.path.basename(cpath))
    live_path = os.path.join(os.path.dirname(cpath), m.group(1) + ".md")
    if not os.path.exists(live_path):
        return "no_live", None
    try:
        conflict_text = open(cpath, encoding="utf-8", errors="replace").read()
        live_text = open(live_path, encoding="utf-8", errors="replace").read()
    except OSError as e:
        return "error", [str(e)]

    _, c_body = split_frontmatter(conflict_text)
    _, l_body = split_frontmatter(live_text)
    if c_body.strip() == l_body.strip():
        return "safe", None

    c_lines = {l.strip() for l in c_body.splitlines() if l.strip()}
    l_lines = {l.strip() for l in l_body.splitlines() if l.strip()}
    unique = c_lines - l_lines
    if not unique:
        return "safe", None
    return "review", sorted(unique)[:10]


def backup_and_delete(cpath: str, run_dir: str, vault_path: str) -> None:
    rel = os.path.relpath(cpath, vault_path)
    dest = os.path.join(run_dir, rel)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.copy2(cpath, dest)
    os.remove(cpath)


def flag_for_review(cpath: str, unique_lines: list[str], run_id: str, vault_path: str) -> bool:
    rel = os.path.relpath(cpath, vault_path)
    # task_key must not contain "/" -- PATCH /tasks/{task_key} uses the default
    # str path converter, which cannot match a literal slash in the segment
    # (a task_key built from a vault-relative path 404s on every PATCH and then
    # needs a direct SQL fix). The real path is still
    # carried in full on `vaultPath` below.
    key_safe_rel = rel.replace("/", "__")
    task_key = f"vault-conflict-review::{run_id}::{key_safe_rel}"
    payload = {
        "taskKey": task_key,
        "title": f"Vault conflict file needs review: {os.path.basename(cpath)}",
        "status": "open",
        "priority": "medium",
        "sourceType": "vault-conflict-sweep",
        "vaultPath": rel,
        "context": (
            "vault_conflict_sweep.py found body content in this conflict copy "
            "that is not present in the live note. Not auto-deleted. Unique "
            "lines (truncated): " + " | ".join(unique_lines)
        ),
    }
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        TASK_API_URL, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        return True
    except (urllib.error.URLError, OSError) as e:
        log(f"WARNING: failed to create review task for {cpath}: {e}")
        return False


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Remove stale sync-conflict copies from an Obsidian vault and flag the "
                    "rest for review. Without --apply it reports only.")
    parser.add_argument("--vault", help="vault directory (default: OBSIDIAN_VAULT_PATH; no default)")
    parser.add_argument("--apply", action="store_true",
                        help="back up and delete safe copies, and post review tasks to "
                             "KOI_TASK_API_URL")
    parser.add_argument("--dry-run", action="store_true",
                        help="report only, no delete/flag (the default; wins over --apply)")
    args = parser.parse_args(argv)
    dry_run = args.dry_run or not args.apply
    vault_path = os.path.expanduser(args.vault) if args.vault else VAULT_PATH

    if not vault_path:
        log("ERROR: no vault named: pass --vault or set OBSIDIAN_VAULT_PATH")
        return 2
    if not os.path.isdir(vault_path):
        log(f"ERROR: vault path {vault_path} does not exist or is not a directory")
        return 2
    if dry_run:
        log(f"dry run (pass --apply to change anything): vault {vault_path}")
    else:
        log(f"vault {vault_path}; backups under {BACKUP_ROOT}; review tasks to {TASK_API_URL}")

    conflicts = find_conflict_files(vault_path)
    if not conflicts:
        log("sweep: 0 conflict files found")
        return 0

    log(f"sweep: found {len(conflicts)} conflict file(s)")
    run_id = datetime.datetime.now().strftime("%Y%m%dT%H%M%SZ")
    run_dir = os.path.join(BACKUP_ROOT, run_id)

    safe_count = review_count = error_count = 0
    for cpath in conflicts:
        verdict, detail = triage(cpath)
        if verdict == "safe":
            safe_count += 1
            log(f"  SAFE   {cpath}")
            if not dry_run:
                backup_and_delete(cpath, run_dir, vault_path)
        elif verdict == "review":
            review_count += 1
            log(f"  REVIEW {cpath} -- unique body lines: {detail}")
            if not dry_run:
                flag_for_review(cpath, detail, run_id, vault_path)
        elif verdict == "no_live":
            review_count += 1
            log(f"  NO_LIVE {cpath} -- no matching live note, leaving in place")
            if not dry_run:
                flag_for_review(cpath, ["(no matching live note found)"], run_id, vault_path)
        else:
            error_count += 1
            log(f"  ERROR  {cpath}: {detail}")

    log(
        f"sweep complete: {safe_count} cleaned, {review_count} flagged for review, "
        f"{error_count} errors"
        + (" [dry-run, nothing changed]" if dry_run else f" (backups: {run_dir})")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
