"""tests/interop/junit_diff.py, run as a user runs it."""
import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "tests" / "interop" / "junit_diff.py"


def _xml(path, cases):
    body = "".join(
        f'<testcase classname="m" name="{n}">' + {"pass": "", "fail": "<failure/>", "skip": '<skipped type="pytest.skip"/>',
                                                   "xfail": '<skipped type="pytest.xfail"/>'}[s] + "</testcase>"
        for n, s in cases.items())
    path.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
    return str(path)


def _run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True, timeout=30)


def test_no_regression_exits_0(tmp_path):
    a = _xml(tmp_path / "a.xml", {"t1": "pass", "t2": "fail", "t3": "xfail"})
    b = _xml(tmp_path / "b.xml", {"t1": "pass", "t2": "pass", "t3": "xfail", "t4": "pass"})
    r = _run(a, b)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "fail -> pass   m::t2" in r.stdout and "absent -> pass   m::t4" in r.stdout


def test_a_pass_that_fails_or_disappears_exits_1_unless_allowed(tmp_path):
    a = _xml(tmp_path / "a.xml", {"t1": "pass", "t2": "pass"})
    b = _xml(tmp_path / "b.xml", {"t1": "fail"})
    assert _run(a, b).returncode == 1
    assert _run(a, b, "--allow", "m::t1", "m::t2").returncode == 0


def test_unreadable_input_exits_2(tmp_path):
    assert _run(str(tmp_path / "missing.xml"), str(tmp_path / "missing.xml")).returncode == 2


def test_a_pass_that_now_skips_or_xfails_is_a_regression(tmp_path):
    a = _xml(tmp_path / "a.xml", {"t1": "pass", "t2": "pass"})
    b = _xml(tmp_path / "b.xml", {"t1": "skip", "t2": "xfail"})
    assert _run(a, b).returncode == 1
