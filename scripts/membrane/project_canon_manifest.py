#!/usr/bin/env python3
"""Project a canon note's dependency manifest into canon_dependencies.

The repo-side frontmatter manifest is authoritative; the table is an index.
Stdlib only — DB access via psql subprocess.

Expected frontmatter block (strict, line-based; between the leading '---' pair):

  canonDependencies:
    - canonAssertion: <slug>
      evidenceDependencies:
        - orn:koi-net.claim:<hash>

Usage:
  project_canon_manifest.py --note PATH --repo NAME --db DBNAME [--manifest-commit SHA]
"""
import argparse, re, subprocess, sys

RID_RE = re.compile(r"^orn:[a-z0-9_.:-]+$", re.IGNORECASE)
SLUG_RE = re.compile(r"^[a-z0-9_.:-]+$", re.IGNORECASE)


def parse_manifest(text: str):
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise SystemExit("ERROR: note has no frontmatter block")
    try:
        end = next(i for i in range(1, len(lines)) if lines[i].strip() == "---")
    except StopIteration:
        raise SystemExit("ERROR: unterminated frontmatter")
    fm = lines[1:end]
    deps, cur = [], None
    in_block = in_evidence = False
    for raw in fm:
        line = raw.rstrip()
        if line.strip() == "canonDependencies:":
            in_block = True
            continue
        if in_block and line and not line.startswith(" "):
            in_block = False  # left the block
        if not in_block:
            continue
        s = line.strip()
        if s.startswith("- canonAssertion:"):
            slug = s.split(":", 1)[1].strip()
            # slug itself contains ':'; re-split precisely
            slug = s[len("- canonAssertion:"):].strip()
            if not SLUG_RE.match(slug):
                raise SystemExit(f"ERROR: bad assertion slug {slug!r}")
            cur = {"assertion": slug, "evidence": []}
            deps.append(cur)
            in_evidence = False
        elif s == "evidenceDependencies:":
            in_evidence = True
        elif s.startswith("- ") and in_evidence and cur is not None:
            rid = s[2:].strip()
            if not RID_RE.match(rid):
                raise SystemExit(f"ERROR: bad RID {rid!r}")
            cur["evidence"].append(rid)
    return [d for d in deps if d["evidence"]]


def q(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--note", required=True)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--manifest-commit", default=None)
    args = ap.parse_args()

    text = open(args.note, encoding="utf-8").read()
    deps = parse_manifest(text)
    if not deps:
        raise SystemExit("ERROR: no canonDependencies with evidence found")

    stmts = []
    for d in deps:
        for rid in d["evidence"]:
            stmts.append(
                "INSERT INTO canon_dependencies "
                "(assertion_slug, claim_rid, repo, note_path, manifest_commit) VALUES ("
                + ", ".join([q(d["assertion"]), q(rid), q(args.repo), q(args.note),
                             q(args.manifest_commit) if args.manifest_commit else "NULL"])
                + ") ON CONFLICT (assertion_slug, claim_rid) DO NOTHING;"
            )
    sql = "BEGIN;\n" + "\n".join(stmts) + "\nCOMMIT;\n"
    r = subprocess.run(["psql", "-d", args.db, "-v", "ON_ERROR_STOP=1", "-q"],
                       input=sql, text=True, capture_output=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        raise SystemExit(r.returncode)
    print(f"projected {sum(len(d['evidence']) for d in deps)} dependencies "
          f"({len(deps)} assertions) from {args.note}")


if __name__ == "__main__":
    main()
