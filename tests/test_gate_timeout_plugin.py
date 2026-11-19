"""tests/interop/gate_timeout.py, loaded the way run_gate.py loads it: `-p
tests.interop.gate_timeout` in a child pytest, with the limit in KOI_GATE_TEST_TIMEOUT.

The child runs hanging tests in a temporary directory with its own ini, so this suite's
conftest and targets play no part. Each hang must fail its own phase within the limit, and
the run must go on to the next test. The gate's own runs show the plugin also loads under
pytest 9 in .venv-koinet: every case there loads it.
"""
from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

HANGS = textwrap.dedent('''
    import asyncio
    import time

    import pytest
    import pytest_asyncio


    @pytest.mark.asyncio
    async def test_awaits_forever():
        await asyncio.Event().wait()


    def test_sleeps_too_long():
        time.sleep(60)


    def test_swallows_exception():
        while True:
            try:
                time.sleep(60)
            except Exception:
                pass


    @pytest_asyncio.fixture
    async def held_connection():
        """The shape of the hang it guards against: a background task keeps a resource, and teardown waits for it."""
        lock = asyncio.Lock()
        release = asyncio.Event()

        async def holder():
            async with lock:
                await release.wait()

        task = asyncio.create_task(holder(), name="held-request")
        await asyncio.sleep(0)
        yield
        async with lock:  # never acquired: nothing sets release
            pass
        task.cancel()


    @pytest.mark.asyncio
    async def test_teardown_waits_on_a_held_task(held_connection):
        pass


    def test_after_the_hangs_still_runs():
        assert True
''')


def _run(tmp_path: Path, limit: str | None, source: str = HANGS) -> tuple[subprocess.CompletedProcess, dict, float]:
    (tmp_path / "pytest.ini").write_text("[pytest]\nasyncio_mode = strict\n")
    (tmp_path / "test_hangs.py").write_text(source)
    junit = tmp_path / "junit.xml"
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path), "PYTHONPATH": str(REPO),
           "PYTHONDONTWRITEBYTECODE": "1"}
    if limit is not None:
        env["KOI_GATE_TEST_TIMEOUT"] = limit
    started = time.monotonic()
    r = subprocess.run([sys.executable, "-B", "-m", "pytest", "-c", str(tmp_path / "pytest.ini"), "--rootdir", str(tmp_path),
                        "-p", "no:cacheprovider", "-p", "tests.interop.gate_timeout", "-rfE", f"--junitxml={junit}",
                        str(tmp_path / "test_hangs.py")],
                       cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)
    elapsed = time.monotonic() - started
    outcomes = {}
    if junit.exists():
        for case in ET.parse(junit).getroot().iter("testcase"):
            kinds = [child.tag for child in case if child.tag in ("failure", "error", "skipped")]
            text = " ".join((child.get("message") or "") + (child.text or "") for child in case)
            outcomes.setdefault(case.get("name"), []).append((kinds, text))
    return r, outcomes, elapsed


def test_each_hang_fails_its_own_phase_and_the_run_goes_on(tmp_path):
    r, outcomes, elapsed = _run(tmp_path, "1")
    out = r.stdout + r.stderr
    assert r.returncode == 1, out
    assert "gate per-test limit: 1s per phase" in r.stdout, out
    for name in ("test_awaits_forever", "test_sleeps_too_long", "test_swallows_exception"):
        kinds, text = outcomes[name][0]
        assert kinds == ["failure"], (name, outcomes[name])
        assert f"test_hangs.py::{name} call ran past the gate's per-test limit of 1s" in text, (name, text)
    # The async hang names the line it waits on.
    assert "test_hangs.py:11 test_awaits_forever" in outcomes["test_awaits_forever"][0][1], outcomes["test_awaits_forever"]
    # The teardown hang is an error of the teardown phase, and names the held task and the fixture's wait.
    teardown = [entry for entry in outcomes["test_teardown_waits_on_a_held_task"] if "error" in entry[0]]
    assert teardown, outcomes["test_teardown_waits_on_a_held_task"]
    text = teardown[0][1]
    assert "test_teardown_waits_on_a_held_task teardown ran past" in text, text
    assert "held-request: test_hangs.py:" in text and " holder" in text, text
    assert outcomes["test_after_the_hangs_still_runs"] == [([], "")], outcomes
    # Four limits of 1 s, plus interpreter start: far from the 60 s sleeps and the endless waits.
    assert elapsed < 30, elapsed


def test_without_the_variable_it_sets_no_limit(tmp_path):
    source = "import time\n\ndef test_slower_than_a_limit_would_allow():\n    time.sleep(2)\n"
    r, outcomes, _ = _run(tmp_path, None, source)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "gate per-test limit" not in r.stdout
    assert outcomes == {"test_slower_than_a_limit_would_allow": [([], "")]}, outcomes


def test_zero_sets_no_limit_and_a_bad_value_is_a_usage_error(tmp_path):
    source = "import time\n\ndef test_slower_than_a_limit_would_allow():\n    time.sleep(2)\n"
    r, outcomes, _ = _run(tmp_path, "0", source)
    assert r.returncode == 0, r.stdout + r.stderr
    assert outcomes == {"test_slower_than_a_limit_would_allow": [([], "")]}, outcomes
    for bad in ("soon", "-5"):
        r, _, _ = _run(tmp_path, bad, source)
        assert r.returncode == 4, r.stdout + r.stderr  # pytest's usage-error status
        assert "KOI_GATE_TEST_TIMEOUT" in r.stderr, r.stderr
