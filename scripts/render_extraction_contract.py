#!/usr/bin/env python3
"""Derive every deep-document extraction contract surface from ONE source (issue #68).

The source is `api/document_extraction_contract.py`. The derived surfaces are:

    scripts/schemas/deep_extraction_doc_v1.schema.json   entities[].type.enum
    scripts/schemas/deep_extraction_doc_v2.schema.json   entities[].type.enum
    scripts/prompts/deep_extraction_doc_v1.md            typing rules, appendix
                                                         enum, HARD OUTPUT RULE 2
    scripts/prompts/deep_extraction_doc_v2.md            same three

Usage:
    python scripts/render_extraction_contract.py            # rewrite in place
    python scripts/render_extraction_contract.py --check    # exit 1 on drift

`--check` is what the test suite runs, so a hand-edit to any of the four files
fails CI rather than drifting. Six hand-maintained copies of one vocabulary is how
`Document` and `Event` stayed un-emittable for months while the live registry had
already admitted them.

EVERY REPLACEMENT IS ASSERTED, NEVER BEST-EFFORT. A missing marker or an enum line
that does not appear exactly once raises. A generator that silently skips a surface
it cannot find is indistinguishable from one that has nothing to do — and that
false pass is the failure mode this whole issue is about.

Exit codes
    0  in sync (--check), or rewritten successfully
    1  drift found (--check only)
    2  a surface could not be located — the generator itself is broken
"""

from __future__ import annotations

import argparse
import difflib
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api.document_extraction_contract import (  # noqa: E402
    DOCUMENT_TYPE_CONTRACT_VERSION,
    render_canonicalization_block,
    render_hard_rule_block,
    render_schema_enum,
)

SCHEMA_FILES = (
    REPO_ROOT / "scripts/schemas/deep_extraction_doc_v1.schema.json",
    REPO_ROOT / "scripts/schemas/deep_extraction_doc_v2.schema.json",
)
PROMPT_FILES = (
    REPO_ROOT / "scripts/prompts/deep_extraction_doc_v1.md",
    REPO_ROOT / "scripts/prompts/deep_extraction_doc_v2.md",
)

#: The `entities[].type` enum line. `"type": {"enum":` is unique in all four files
#: (`doc_kind`, `confidence` and `move_type` use their own key names), which the
#: exactly-once assertion below re-proves on every run rather than trusting.
_ENUM_RE = re.compile(r'("type":\s*\{"enum":\s*)\[[^\]]*\]')


class GeneratorError(RuntimeError):
    """A surface the generator is responsible for could not be located."""


def _marker(key: str, edge: str) -> str:
    return f"<!-- generated:{key}:{edge} — from api/document_extraction_contract.py; run scripts/render_extraction_contract.py -->"


def replace_marked(text: str, key: str, body: str, *, where: Path) -> str:
    """Replace the content between a begin/end marker pair. Raises if absent."""
    begin, end = _marker(key, "begin"), _marker(key, "end")
    i, j = text.find(begin), text.find(end)
    if i < 0 or j < 0:
        raise GeneratorError(
            f"{where}: missing {'begin' if i < 0 else 'end'} marker for {key!r}. "
            f"The generator is responsible for this region and cannot find it; "
            f"restore the markers rather than letting the region go unmanaged."
        )
    if j < i:
        raise GeneratorError(f"{where}: {key!r} end marker precedes its begin marker")
    return text[: i + len(begin)] + "\n" + body + "\n" + text[j:]


def replace_enum_line(text: str, *, spaced: bool, where: Path) -> str:
    """Rewrite the single `entities[].type` enum. Raises unless it matches once."""
    n = len(_ENUM_RE.findall(text))
    if n != 1:
        raise GeneratorError(
            f"{where}: expected exactly 1 `\"type\": {{\"enum\": [...]}}` occurrence, "
            f"found {n}. Refusing to guess which one is the entity-type enum."
        )
    return _ENUM_RE.sub(lambda m: m.group(1) + render_schema_enum(spaced=spaced), text)


def render_file(path: Path) -> str:
    """Return what `path` SHOULD contain, given the contract."""
    text = path.read_text(encoding="utf-8")
    if path in SCHEMA_FILES:
        return replace_enum_line(text, spaced=True, where=path)
    out = replace_marked(text, "entity-types", render_canonicalization_block(), where=path)
    out = replace_marked(out, "entity-type-hard-rule", render_hard_rule_block(), where=path)
    return replace_enum_line(out, spaced=False, where=path)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="report drift and exit 1; write nothing")
    args = ap.parse_args()

    targets = list(SCHEMA_FILES) + list(PROMPT_FILES)
    # A zero-length target list would make --check "pass" having compared nothing.
    if len(targets) != 4:
        print(f"generator misconfigured: {len(targets)} targets, expected 4",
              file=sys.stderr)
        return 2

    drifted, rewritten = [], []
    for path in targets:
        if not path.is_file():
            print(f"missing contract surface: {path}", file=sys.stderr)
            return 2
        try:
            current = path.read_text(encoding="utf-8")
            wanted = render_file(path)
        except GeneratorError as e:
            print(f"GENERATOR ERROR: {e}", file=sys.stderr)
            return 2
        if current == wanted:
            continue
        if args.check:
            drifted.append(path)
            sys.stdout.writelines(difflib.unified_diff(
                current.splitlines(keepends=True), wanted.splitlines(keepends=True),
                fromfile=f"{path.relative_to(REPO_ROOT)} (on disk)",
                tofile=f"{path.relative_to(REPO_ROOT)} (from contract)"))
        else:
            path.write_text(wanted, encoding="utf-8")
            rewritten.append(path)

    if args.check:
        if drifted:
            print(f"\n{len(drifted)} of {len(targets)} contract surface(s) drifted from "
                  f"{DOCUMENT_TYPE_CONTRACT_VERSION}. Run: "
                  f"python scripts/render_extraction_contract.py", file=sys.stderr)
            return 1
        print(f"all {len(targets)} contract surfaces match "
              f"{DOCUMENT_TYPE_CONTRACT_VERSION}")
        return 0

    print(f"{DOCUMENT_TYPE_CONTRACT_VERSION}: rewrote {len(rewritten)} of "
          f"{len(targets)} surface(s)"
          + ("".join(f"\n  {p.relative_to(REPO_ROOT)}" for p in rewritten)
             if rewritten else " (already in sync)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
