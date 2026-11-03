#!/usr/bin/env python3
"""CI guard: reject `emit_doclink_event(...)` calls whose `mention_delta`
argument is inferred from a SELECT or a post-insert read of mention_count.

`mention_delta` MUST be a publisher-supplied delta (a literal `1`, or `1`
gated on a rows-affected check) — NEVER the result of reading the current
mention_count back out of the database. Doing so re-derives a total the
publisher never moved and causes a publisher-side double-count on the
additive subscriber path (_apply_doclink).

Heuristic: scan each `emit_doclink_event(` call's argument text for tokens
that betray a DB read — SELECT, .fetchone()/.fetchval()/.fetchrow(),
.mention_count, RETURNING. Any hit fails the lint.

Usage:
    python scripts/lint_mention_delta.py            # scan api/ and scripts/
    python scripts/lint_mention_delta.py path ...   # scan specific paths

Exit 0 = clean, 1 = violations found.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCAN = ["api", "scripts"]

CALL_RE = re.compile(r"emit_doclink_event\s*\(", re.MULTILINE)
FORBIDDEN = (
    "SELECT",
    ".fetchone(",
    ".fetchval(",
    ".fetchrow(",
    ".mention_count",
    "RETURNING",
)


def _extract_call(text: str, open_paren_idx: int) -> str:
    """Return the substring from just after `emit_doclink_event(` to the
    matching close paren (paren-depth aware)."""
    depth = 1
    i = open_paren_idx + 1
    while i < len(text) and depth > 0:
        c = text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        i += 1
    return text[open_paren_idx + 1 : i - 1]


def lint_file(path: Path) -> list[str]:
    violations = []
    try:
        text = path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return violations
    if "emit_doclink_event" not in text:
        return violations
    for m in CALL_RE.finditer(text):
        # m.end() - 1 is the index of the `(`.
        args = _extract_call(text, m.end() - 1)
        line_no = text[: m.start()].count("\n") + 1
        for tok in FORBIDDEN:
            if tok in args:
                violations.append(
                    f"{path}:{line_no}: emit_doclink_event() argument contains "
                    f"{tok!r} — mention_delta must be a publisher-supplied delta, "
                    f"never inferred from a DB read"
                )
                break
    return violations


# This script itself documents the forbidden tokens in prose — exclude it so
# it does not flag its own docstring.
SELF = Path(__file__).resolve()


def iter_py_files(scan_paths: list[str]):
    for sp in scan_paths:
        root = (REPO_ROOT / sp) if not Path(sp).is_absolute() else Path(sp)
        if root.is_file() and root.suffix == ".py":
            candidates = [root]
        elif root.is_dir():
            candidates = sorted(root.rglob("*.py"))
        else:
            candidates = []
        for c in candidates:
            if c.resolve() != SELF:
                yield c


def main(argv: list[str]) -> int:
    scan_paths = argv[1:] or DEFAULT_SCAN
    all_violations = []
    for f in iter_py_files(scan_paths):
        all_violations.extend(lint_file(f))
    if all_violations:
        print("lint_mention_delta: FAIL", file=sys.stderr)
        for v in all_violations:
            print(f"  {v}", file=sys.stderr)
        return 1
    print("lint_mention_delta: OK (no forbidden mention_delta derivations)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
