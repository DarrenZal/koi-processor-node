"""Entry points of the KOI-net interop gate, run the way an operator runs them.

tests/interop/run_gate.py is run as `tools/sandboxed.sh -- python3 tests/interop/run_gate.py`,
and tests/interop/setup_stock_nodes.sh directly. These tests run both that way and check
their refusals and usage errors, which start nothing and need no network. The passing path
of both is exercised by every gate run: run_gate.py calls `setup_stock_nodes.sh
--verify-only` on the pinned template before it starts any node.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SANDBOX = REPO / "tools" / "sandboxed.sh"
GATE = REPO / "tests" / "interop" / "run_gate.py"
SETUP = REPO / "tests" / "interop" / "setup_stock_nodes.sh"


def _gate(*args: str, env_extra: dict | None = None) -> subprocess.CompletedProcess:
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", "/tmp")}
    env.update(env_extra or {})
    return subprocess.run(["bash", str(SANDBOX), "--", sys.executable, str(GATE), *args],
                          cwd=REPO, env=env, capture_output=True, text=True, timeout=60)


def test_gate_requires_a_node_interpreter():
    r = _gate("--check")
    assert r.returncode == 2, r.stderr
    assert "--node-python" in r.stderr


def test_gate_refuses_an_unknown_case():
    r = _gate("--check", "--node-python", sys.executable, "--cases", "full-off,nonsense")
    assert r.returncode == 2, r.stderr
    assert "nonsense" in r.stderr


def test_gate_refuses_a_work_dir_in_the_home_documents_folder():
    home = Path(os.path.expanduser("~"))
    target = home / "Documents" / "koi-gate-must-not-exist"
    r = _gate("--check", "--node-python", sys.executable, "--work-dir", str(target),
              env_extra={"HOME": str(home)})
    assert r.returncode == 2, r.stderr
    assert "refusing work dir" in r.stderr
    assert not target.exists()


def test_gate_reports_a_missing_interpreter_as_missing_prerequisite(tmp_path):
    r = _gate("--check", "--node-python", str(tmp_path / "no-such-python"))
    assert r.returncode == 3, r.stderr
    assert "missing prerequisite" in r.stderr


def test_setup_verify_only_rejects_a_tampered_template(tmp_path):
    pkg = tmp_path / "src" / "koi_net_CHANGE_NAME_node"
    pkg.mkdir(parents=True)
    for name in ("__init__.py", "__main__.py", "config.py", "core.py", "custom_handler.py"):
        (pkg / name).write_text("# not the pinned file\n")
    r = subprocess.run(["bash", str(SETUP), "--verify-only", str(tmp_path)], capture_output=True, text=True, timeout=30)
    assert r.returncode == 1, r.stderr
    # __init__.py is empty upstream, so the first mismatch reported is the first file.
    assert "__init__.py has sha256" in r.stderr


def test_setup_verify_only_rejects_a_missing_template(tmp_path):
    r = subprocess.run(["bash", str(SETUP), "--verify-only", str(tmp_path / "absent")], capture_output=True, text=True, timeout=30)
    assert r.returncode == 1, r.stderr
    assert "is missing" in r.stderr


def test_setup_usage_error():
    r = subprocess.run(["bash", str(SETUP), "--verify-only"], capture_output=True, text=True, timeout=30)
    assert r.returncode == 2, r.stderr


def test_gate_refuses_an_existing_work_dir_and_leaves_it_alone(tmp_path):
    """The gate deletes its work dir after a passing run, so it must never adopt one that exists."""
    keep = tmp_path / "keep.txt"
    keep.write_text("not the gate's")
    r = _gate("--check", "--node-python", sys.executable, "--work-dir", str(tmp_path))
    assert r.returncode == 2, r.stderr
    assert "already exists" in r.stderr
    assert keep.read_text() == "not the gate's"


def test_gate_refusals_create_no_temporary_directory(tmp_path):
    for args in (["--check"], ["--check", "--node-python", str(tmp_path / "no-such-python")]):
        r = _gate(*args, env_extra={"TMPDIR": str(tmp_path)})
        assert r.returncode in (2, 3), r.stderr
    assert not list(tmp_path.glob("koi-gate-*")), "a refused or failed check left a scratch directory"


def test_gate_refuses_a_negative_time_limit():
    for flag in ("--test-timeout", "--wall-clock"):
        r = _gate("--check", "--node-python", sys.executable, flag, "-1")
        assert r.returncode == 2, r.stderr
        assert "0 or more" in r.stderr


def test_the_wall_clock_stops_a_hung_gate_as_a_failure(tmp_path):
    """--wall-clock, through the real main() under tools/sandboxed.sh. Only two steps are
    replaced: the prerequisite check (this test needs no stock node template) and the cluster
    start, which here never returns. The gate must end with exit 1, say why, stop what it
    started and keep the scratch directory. A pytest run it is waiting on is killed by
    subprocess.run as the exception passes; the gate's runs show the cluster stop."""
    driver = tmp_path / "drive_gate.py"
    driver.write_text(
        "import sys, time\n"
        f"sys.path.insert(0, {str(GATE.parent)!r})\n"
        "import run_gate\n"
        "run_gate.check_prerequisites = lambda node_python: run_gate.Path(node_python)\n"
        "def hang(self):\n"
        "    print('driver: cluster start hangs', flush=True)\n"
        "    time.sleep(600)\n"
        "run_gate.Gate.start_cluster = hang\n"
        "real_stop_all = run_gate.Gate.stop_all\n"
        "def stop_all(self):\n"
        "    print('driver: stop_all', flush=True)\n"
        "    real_stop_all(self)\n"
        "run_gate.Gate.stop_all = stop_all\n"
        "sys.exit(run_gate.main(sys.argv[1:]))\n")
    work = tmp_path / "work"
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path)}
    started = time.monotonic()
    r = subprocess.run(["bash", str(SANDBOX), "--", sys.executable, "-B", str(driver), "--node-python", sys.executable,
                        "--db-tests", "tests/interop/test_negotiation_db.py", "--work-dir", str(work), "--wall-clock", "2"],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=60)
    elapsed = time.monotonic() - started
    assert r.returncode == 1, r.stdout + r.stderr
    assert "driver: cluster start hangs" in r.stdout, r.stdout
    assert "gate: wall clock of 2s ran out; stopping every process it started" in r.stderr, r.stderr
    assert "driver: stop_all" in r.stdout and "gate: results wall-clock=1" in r.stdout, r.stdout
    assert f"gate: scratch directory kept: {work}" in r.stdout and work.is_dir(), r.stdout
    assert elapsed < 30, elapsed  # the hang was 600 s
