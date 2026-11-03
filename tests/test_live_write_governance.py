"""The live-write gate, expressed as a property rather than a list of filenames.

WHY THIS FILE EXISTS
--------------------
On 2026-08-22 four defects escaped the repo's gates in a single day, none going
red. One cause: every gate here ENUMERATES. The red baseline names eight files.
`live_graph_tripwire` named one table (so it reported "0 test-signature rows
written" while 135 task rows landed). `pytest.ini` names no `testpaths`, so four
`test_*.py` files outside `tests/` collect with no conftest, no DSN redirect and
no tripwire at all. Anything outside the enumeration fails silently, and silence
reads as safety.

This gate instead DERIVES its population from the filesystem and asserts a
property of it:

    every test that writes to an API over HTTP either cleans up after
    itself, or is named here with a reason.

A newly-added leaking test therefore fails on the day it is written, without
anyone remembering to update a list.

WHY THE DETECTOR IS DELIBERATELY OVER-INCLUSIVE
-----------------------------------------------
A false positive costs one line in KNOWN_UNGOVERNED. A false negative cost 627
leaked rows in `task_registry` and 1,310 in `intent_registry`. So the patterns
below match broadly and the allowlist absorbs the noise.

That bias was learned the hard way *while writing this file*: the first detector
matched only `.post(`/`.patch(` and missed `scripts/test_claims_api.py`, which
uses `urllib.request`. The second added `method="POST"` and missed it again,
because that file passes the verb positionally as `_req("POST", ...)`. Two
iterations, two different misses, same file — which is why
`test_detector_still_matches` exists: if these patterns silently stop matching,
the two assertions below pass trivially and this gate becomes decoration.

NO HTTP, NO DATABASE. This is a static scan. A gate that needs a live backend
skips when the backend is slow, and a skip that reads as a pass is the exact
failure that produced a bad red baseline on 2026-08-21.
"""

from __future__ import annotations

import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# A write verb, in any of the shapes this repo actually uses: an httpx/requests
# method call, a quoted verb passed positionally or by keyword, or curl -X.
WRITE_VERB = re.compile(
    r'\.(post|patch|put|delete)\s*\('
    r'|["\'](POST|PATCH|PUT|DELETE)["\']'
    r'|-X\s*(POST|PATCH|PUT|DELETE)',
    re.I,
)
# A client that leaves this process. TestClient/ASGITransport are deliberately
# NOT here: those drive the app in-process, against KOI_TEST_DATABASE_URL, with the
# writes and the teardown together.
OUT_OF_PROCESS_CLIENT = re.compile(
    r'httpx|requests\.|urllib|http\.client|aiohttp|curl\b'
)
# Advisory only — reported, never required. An earlier version REQUIRED a literal
# live URL, which a test could evade simply by assembling its base URL from a
# fixture, a helper or an env var. Measured on 2026-08-22: requiring it and
# ignoring it flag the identical 9 files, so the requirement bought no precision
# and cost a whole class of false negatives. An out-of-process HTTP client in a
# test is suspicious wherever it points.
LIVE_URL = re.compile(
    r'8351|KOI_API_URL|BASE_URL|PEER_URL|localhost:\d+|127\.0\.0\.1:\d+'
)
# Evidence the module DECLARES cleanup. This proves intent, not efficacy: a file
# with an unrelated `DELETE FROM` in a helper would satisfy it. That limit is
# deliberate and layered, not an oversight -- efficacy is proven at runtime by the
# fail-closed purges themselves (tests/test_task_registry.py fails when it deletes
# nothing or rows survive), which a static scan cannot do. This gate answers "did
# anyone even try?", which is the question that was going unasked while 627 rows
# accumulated.
CLEANUP = re.compile(
    r'purge_test|DELETE\s+FROM'
    r'|live_write_begin|governed_live_write_run'
)
# Drives the app in-process against KOI_TEST_DATABASE_URL, so its writes and its
# teardown share one database — not an out-of-process writer.
IN_PROCESS = re.compile(r'ASGITransport|TestClient|app\s*=\s*app')


def _candidate_files() -> list[pathlib.Path]:
    """Every test-shaped file in the repo, including ones pytest never collects.

    Shell scripts are included deliberately: five of them curl the live API and
    no conftest.py can ever govern them. A scanner is the only thing that can.
    Installed packages are skipped: the quickstart creates its venv inside the
    checkout, and third-party test files there are not this repo's writers.
    """
    seen: set[pathlib.Path] = set()
    for pattern in ("test_*.py", "test_*.sh", "*_test.py"):
        for path in REPO_ROOT.rglob(pattern):
            if ".git" in path.parts or "__pycache__" in path.parts or "site-packages" in path.parts:
                continue
            seen.add(path)
    return sorted(seen)


def _scan() -> dict[str, dict]:
    """Return {relative_path: facts} for every file that writes to a live API."""
    out: dict[str, dict] = {}
    for path in _candidate_files():
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        writes = len(WRITE_VERB.findall(text))
        if not writes or not OUT_OF_PROCESS_CLIENT.search(text):
            continue
        out[str(path.relative_to(REPO_ROOT))] = {
            "writes": writes,
            "cleanup": bool(CLEANUP.search(text)),
            "in_process": bool(IN_PROCESS.search(text)),
            # Advisory: recorded for triage, never used to include or exclude.
            "live_url": bool(LIVE_URL.search(text)),
        }
    return out


def _ungoverned(scan: dict[str, dict]) -> set[str]:
    return {
        p for p, f in scan.items() if not f["cleanup"] and not f["in_process"]
    }


# ---------------------------------------------------------------------------
# The allowlist. Every entry needs a reason someone can disagree with.
# ---------------------------------------------------------------------------
# Seeded 2026-08-22 from measurement, not from guesswork. Removing an entry is
# the point: when a file is fixed, test_no_stale_allowlist_entries fails until
# its line is deleted, so this list cannot quietly outlive the problems it
# describes.
KNOWN_UNGOVERNED: dict[str, str] = {
    # --- Safe: the endpoint does not persist -------------------------------
    "tests/test_contract.py": (
        "POSTs an empty body to each catalogued endpoint to prove it is registered "
        "(expects 2xx or 422), plus /entity/resolve without persist=true. It runs only "
        "against KOI_TEST_API_URL, which the guard keeps off the live API; an endpoint "
        "that accepts an empty body may write to that throwaway API's database."
    ),
    "tests/conformance/test_wire_conformance.py": (
        "Signs and POSTs koi-net requests to a throwaway conformance-* node on loopback at "
        "KOI_TEST_API_URL, which the guard keeps off the live API. That node's database is "
        "disposable (tests/conformance/README.md), so nothing is purged."
    ),
    "tests/test_cross_type_alias_dedup.py": (
        "Same shape: one POST site at :50-58, to /entity/resolve, non-persisting "
        "for the same reason as tests/test_contract.py."
    ),
}


def test_no_new_ungoverned_live_writers():
    """THE PROPERTY. A new test that writes to the live API without cleanup fails here."""
    offenders = _ungoverned(_scan()) - set(KNOWN_UNGOVERNED)
    assert not offenders, (
        "\n*** UNGOVERNED LIVE WRITER(S) ***\n"
        + "\n".join(f"  - {p}" for p in sorted(offenders))
        + "\n\nEach of these writes to an API over HTTP and shows no sign of "
        "removing what it wrote. Writes sent over HTTP land in whatever database that "
        "API uses, which no test variable can redirect.\n"
        "Either add a purge fixture (see tests/test_task_registry.py::purge_test_tasks) "
        "or add an entry to KNOWN_UNGOVERNED in this file WITH A REASON."
    )


def test_no_stale_allowlist_entries():
    """An allowlist that outlives its problems is how the last gate rotted."""
    scan = _scan()
    ungoverned = _ungoverned(scan)
    stale = []
    for path in KNOWN_UNGOVERNED:
        if not (REPO_ROOT / path).exists():
            stale.append(f"{path} — file no longer exists")
        elif path not in ungoverned:
            stale.append(f"{path} — now governed (or no longer writes); delete this entry")
    assert not stale, (
        "\n*** STALE KNOWN_UNGOVERNED ENTRIES ***\n"
        + "\n".join(f"  - {s}" for s in stale)
        + "\n\nRemove them so the allowlist keeps describing reality."
    )


def test_detector_still_matches():
    """Self-test: if the patterns stop matching, the assertions above pass trivially.

    Both controls are files whose behaviour is verified and documented, so a
    change here means the detector broke, not that the repo changed.
    """
    scan = _scan()

    # POSITIVE: writes via urllib with the verb passed positionally. Missed by
    # two earlier versions of this detector, for two different reasons. Checked
    # where the script is present (a public export may not carry it).
    if (REPO_ROOT / "scripts/test_claims_api.py").exists():
        assert "scripts/test_claims_api.py" in scan, (
            "Detector no longer flags scripts/test_claims_api.py, a known live writer. "
            "The patterns have regressed and this gate is now blind."
        )

    # POSITIVE: a governed writer must still be seen, and seen AS governed.
    assert scan.get("tests/test_task_registry.py", {}).get("cleanup") is True, (
        "Detector no longer recognises the purge fixture in tests/test_task_registry.py."
    )

    # NEGATIVE: GET-only traffic must not be flagged, or everything is an offender
    # and the gate gets ignored.
    assert "scripts/check_claims_regression.sh" not in scan, (
        "Detector flagged a GET-only script; it is over-matching and will train "
        "people to ignore this gate."
    )


def test_gate_needs_no_backend():
    """This module must never make a network or database call.

    A gate that needs the backend skips when the backend is slow, and a skip that
    reads as a pass is precisely what produced a bad red baseline on 2026-08-21.
    """
    source = pathlib.Path(__file__).read_text()
    body = source.split('KNOWN_UNGOVERNED: dict[str, str] = {', 1)[0]
    for forbidden in ("urlopen(", "httpx.", "requests.get", "requests.post",
                      "psycopg2.connect", "asyncpg.connect"):
        assert forbidden not in body, (
            f"{forbidden} appears in this gate's own logic; it must stay a static scan."
        )
