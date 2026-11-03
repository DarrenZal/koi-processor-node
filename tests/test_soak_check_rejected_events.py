"""A6 (silent-success sweep rank 6): scripts/federation/soak-check.sh's
rejected_events extraction fabricated a 0 for an unreachable/auth-failed
peer. `r = status.get('rejected_events', {})` then `sum(r.values())` cannot
distinguish "genuinely checked, found zero rejections" from "the whole
status response was {'error': 'unreachable'}, so rejected_events was never
present at all" -- both produce sum({}.values()) == 0. Every sibling field
(pending_events, scans_completed, watcher_enabled) already used
`.get(key, '?')`, so an unreachable peer correctly showed "?" for those
three but a fabricated "0" for rejected_total.

309 fabricated zeros were found across 289 soak-results records, including
inside a window CLAUDE.md's own history cites as "PASSED... zero rejected
events."

This test runs the ACTUAL python one-liner extracted from the script (not a
reimplementation) so it can't silently drift out of sync with the script
it's meant to guard.
"""

import re
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "federation" / "soak-check.sh"


def _extract_rejected_events_snippet() -> str:
    """Pull the python -c "..." snippet used for LOCAL_REJECTED out of the
    live script file, so this test exercises the real logic, not a copy."""
    text = SCRIPT.read_text()
    m = re.search(
        r"LOCAL_REJECTED=\$\(echo \"\$LOCAL_STATUS\" \| python3 -c \"(.*?)\" 2>/dev/null",
        text,
    )
    assert m, "could not locate the LOCAL_REJECTED extraction line in soak-check.sh"
    return m.group(1)


def _run_snippet(stdin_json: str) -> str:
    snippet = _extract_rejected_events_snippet()
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        input=stdin_json,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_unreachable_status_reports_unknown_not_zero():
    assert _run_snippet('{"error":"unreachable"}') == "?"


def test_genuinely_zero_rejected_events_reports_zero():
    assert _run_snippet('{"rejected_events":{}}') == "0"


def test_nonzero_rejected_events_sums_correctly():
    assert _run_snippet('{"rejected_events":{"peer1":2,"peer2":3}}') == "5"


@pytest.mark.parametrize("both_lines", ["LOCAL_REJECTED", "PEER_REJECTED"])
def test_both_local_and_peer_extraction_use_the_isinstance_guard(both_lines):
    """Both sites must share the identical fix -- one fixed and the other
    left on the old `.get(key, {})` pattern would silently reintroduce the
    bug for whichever side (local vs peer) still has it."""
    text = SCRIPT.read_text()
    line = next(l for l in text.splitlines() if l.startswith(f"{both_lines}="))
    assert "isinstance(r, dict)" in line, (
        f"{both_lines} no longer guards on isinstance(r, dict) -- "
        f"a bare .get('rejected_events', {{}}) fabricates 0 for an unreachable peer"
    )
