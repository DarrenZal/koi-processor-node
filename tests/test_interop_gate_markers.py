"""The interop gate's xfail markers must name divergences the register still lists as open.

tests/interop/test_gate.py marks each blocked protocol step with the divergences that block
it. When a fix lands, its register entry changes to "fixed", and every marker still naming
it is stale. This test reads both files, so a stale marker fails here in the main suite
instead of surviving until someone reruns the gate.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REGISTER = REPO / "docs" / "architecture" / "koi-protocol-conformance.md"
GATE_TESTS = REPO / "tests" / "interop" / "test_gate.py"


def _summary() -> dict[int, str]:
    text = REGISTER.read_text()
    section = text.split("## Conformance summary", 1)[1].split("\n## ", 1)[0]
    rows = {}
    for line in section.splitlines():
        m = re.match(r"\|\s*(\d+)\s*\|[^|]*\|\s*(.+?)\s*\|\s*$", line)
        if m:
            rows[int(m.group(1))] = m.group(2)
    return rows


def _marker_divergences(text: str) -> set[int]:
    found = set()
    for call in re.findall(r"@_blocked_by\(([^)]*)\)", text):
        found.update(int(n) for n in re.findall(r'"(\d+)"', call))
    return found


def test_register_summary_parses():
    rows = _summary()
    # Positive control: the register has had entries 1-22 since 2026-10-31.
    assert set(range(1, 23)) <= set(rows), sorted(rows)


def test_gate_markers_name_open_divergences_only():
    rows = _summary()
    # Positive control on the parser. Zero markers in the gate is a valid state: every step passes.
    assert _marker_divergences('@_blocked_by("16" if KIND == "partial" else "", "24")\ndef t(): pass') == {16, 24}
    assert "def _blocked_by(" in GATE_TESTS.read_text(), "the gate's marker helper moved; update this test"
    named = _marker_divergences(GATE_TESTS.read_text())
    unknown = sorted(n for n in named if n not in rows)
    assert not unknown, f"markers name divergences the register does not list: {unknown}"
    closed = sorted(n for n in named if re.search(r"\bfixed\b", rows[n], re.I))
    assert not closed, f"markers name divergences the register lists as fixed: {closed}; remove them from test_gate.py"
