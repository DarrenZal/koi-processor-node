"""Per-test time limit for the gate's pytest runs.

run_gate.py loads this with `-p tests.interop.gate_timeout` and sets the limit, in seconds, in
KOI_GATE_TEST_TIMEOUT (unset or 0: no limit). Each phase of a test (setup, call, teardown)
gets the limit. When it runs out, SIGALRM raises GateTestTimeout in the main thread. For an
async test that is the event loop's select, so the phase fails and the run goes on to the next
test instead of blocking.

The message names every pending asyncio task and where it waits. That is how a hang is found:
under a deliberately broken build, a failed assert in tests/interop/test_peer_rejection_db.py left a
held request waiting with its pool connection, and the fixture's pool.close() waited for it
for 80 minutes with the database idle.

GateTestTimeout is a BaseException, so code under test that catches Exception cannot swallow
it. If a task does take it (asyncio stores a BaseException other than KeyboardInterrupt or
SystemExit in the task), the alarm fires again every REPEAT_SECONDS until the phase ends.

POSIX and main thread only (SIGALRM). Standard library and pytest only, so it loads in the
node interpreter (pytest 7) and in .venv-koinet (pytest 9).
"""
from __future__ import annotations

import asyncio
import os
import signal
import sysconfig
import threading

import pytest

ENV = "KOI_GATE_TEST_TIMEOUT"
REPEAT_SECONDS = 5.0
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_LIBRARY_ROOTS = tuple(sorted({os.path.realpath(path) + os.sep for name, path in sysconfig.get_paths().items()
                               if name in ("stdlib", "platstdlib", "purelib", "platlib")}))


class GateTestTimeout(BaseException):
    """A test phase ran past the gate's per-test limit."""


class _Clock:
    def __init__(self, limit: float):
        self.limit = limit
        self.nodeid = ""
        self.phase = ""

    def on_alarm(self, _signum, _frame):
        raise GateTestTimeout(f"{self.nodeid} {self.phase} ran past the gate's per-test limit of "
                              f"{self.limit:g}s ({ENV}){_pending_tasks()}")


_CLOCK: _Clock | None = None


def _frame_label(frame) -> str:
    path = frame.f_code.co_filename
    shown = os.path.relpath(path, REPO_ROOT) if path.startswith(REPO_ROOT + os.sep) else os.path.basename(path)
    return f"{shown}:{frame.f_lineno} {frame.f_code.co_name}"


def _is_library(frame) -> bool:
    return os.path.realpath(frame.f_code.co_filename).startswith(_LIBRARY_ROOTS)


def _where(coro) -> str:
    """Where a suspended coroutine waits: its await chain, outermost first. Frames in the
    standard library and site-packages are dropped, except the innermost, so what is left is
    the tests' and the API's own frames and the line that is waiting."""
    chain = []
    while coro is not None:
        frame = getattr(coro, "cr_frame", None) or getattr(coro, "ag_frame", None) or getattr(coro, "gi_frame", None)
        if frame is not None:
            chain.append(frame)
        coro = getattr(coro, "cr_await", None) or getattr(coro, "ag_await", None) or getattr(coro, "gi_yieldfrom", None)
    if not chain:
        return "?"
    kept = [f for f in chain if not _is_library(f)]
    if not kept or kept[-1] is not chain[-1]:
        kept.append(chain[-1])
    return " > ".join(_frame_label(f) for f in kept)


def _pending_tasks() -> str:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # a sync test, or between loop runs
        return ""
    lines = sorted(f"  {task.get_name()}: {_where(task.get_coro())}"
                   for task in asyncio.all_tasks(loop) if not task.done())
    return ("\npending asyncio tasks:\n" + "\n".join(lines)) if lines else ""


def pytest_configure(config):
    global _CLOCK
    raw = os.environ.get(ENV, "").strip()
    if not raw:
        return
    try:
        limit = float(raw)
    except ValueError:
        raise pytest.UsageError(f"{ENV}={raw!r} is not a number of seconds") from None
    if limit < 0:
        raise pytest.UsageError(f"{ENV}={raw!r} is negative")
    if limit > 0:
        if not hasattr(signal, "SIGALRM"):
            raise pytest.UsageError(f"{ENV} needs SIGALRM, which this platform does not have")
        _CLOCK = _Clock(limit)


def pytest_report_header(config):
    if _CLOCK is not None:
        return f"gate per-test limit: {_CLOCK.limit:g}s per phase ({ENV})"
    return None


def _timed(item, phase):
    clock = _CLOCK
    if clock is None or threading.current_thread() is not threading.main_thread():
        yield
        return
    clock.nodeid, clock.phase = item.nodeid, phase
    previous = signal.signal(signal.SIGALRM, clock.on_alarm)
    signal.setitimer(signal.ITIMER_REAL, clock.limit, REPEAT_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item):
    yield from _timed(item, "setup")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    yield from _timed(item, "call")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item):
    yield from _timed(item, "teardown")
