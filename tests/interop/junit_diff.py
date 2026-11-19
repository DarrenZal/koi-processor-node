"""Compare two JUnit XML files test by test (for example, a 3.11 suite run against a 3.13 one).

    python3 tests/interop/junit_diff.py BEFORE.xml AFTER.xml [--allow ID ...]

Prints every test whose status changed (pass, fail, skip, xfail), and tests present in only
one file. Exit 0 when no test that passed BEFORE fails, errors, skips, xfails or is missing
AFTER, except
the IDs given with --allow; exit 1 otherwise; exit 2 for unreadable input. Standard library
only.
"""
from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET


def statuses(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for tc in ET.parse(path).getroot().iter("testcase"):
        status = "pass"
        for child in tc:
            if child.tag in ("failure", "error"):
                status = "fail"
            elif child.tag == "skipped":
                status = "xfail" if child.get("type") == "pytest.xfail" else "skip"
        out[f"{tc.get('classname')}::{tc.get('name')}"] = status
    return out


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("--allow", nargs="*", default=[], help="test IDs whose regression is accepted")
    args = ap.parse_args(argv)
    try:
        a, b = statuses(args.before), statuses(args.after)
    except (OSError, ET.ParseError) as exc:
        print(f"junit_diff: {exc}", file=sys.stderr)
        return 2
    regressions = []
    for tid in sorted(set(a) | set(b)):
        sa, sb = a.get(tid, "absent"), b.get(tid, "absent")
        if sa != sb:
            print(f"{sa:>6} -> {sb:<6} {tid}")
            # A pass that now skips or xfails stopped proving anything: a regression too.
            if sa == "pass" and sb in ("fail", "absent", "skip", "xfail") and tid not in args.allow:
                regressions.append(tid)
    counts = lambda d: {s: sum(1 for v in d.values() if v == s) for s in ("pass", "fail", "skip", "xfail")}
    print(f"before {counts(a)}\nafter  {counts(b)}")
    if regressions:
        print(f"junit_diff: {len(regressions)} test(s) passed before and do not now", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
