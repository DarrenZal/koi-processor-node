"""Non-blocking subprocess helper for code running on the event loop.

Why this exists
---------------
A blocking call inside an `async def` freezes the WHOLE shared :8351 event loop — every
other request (search, ingest, tasks, morning brief) stalls for its duration. The symptom
is "the service is down" alongside a perfectly healthy Postgres, which sends you looking
in the wrong place. It has been the root cause three separate times in this codebase:

  * `claim_extractor._llm_complete` — `async def` calling the SYNCHRONOUS Anthropic SDK
    bare; SDK defaults gave up to 30 minutes of frozen loop (#36).
  * `ledger_anchor.broadcast_anchor` / `broadcast_attest` — `subprocess.run()` plus
    `time.sleep(5)` in a 6-attempt poll, up to 30s per anchor (#15).
  * `github_webhook.run_extraction` — `subprocess.run()` for `git pull` (60s) and a
    staging-load script (600s), i.e. up to TEN MINUTES.

`asyncio.to_thread` is a legitimate alternative, but it only relocates the problem to a
bounded ThreadPoolExecutor — which #36 showed can itself be exhausted by parked calls,
stalling every other `to_thread` caller. Prefer removing the blocking outright.

Contract note: this raises `subprocess.TimeoutExpired`, NOT `asyncio.TimeoutError`, so
existing `except subprocess.TimeoutExpired` clauses around converted call sites keep
matching. That is deliberate — silently changing the exception type is how a conversion
like this turns a handled timeout into an unhandled crash.
"""
from __future__ import annotations

import asyncio
import contextlib
import subprocess
from typing import Any, Mapping, Optional, Sequence


class CompletedProc:
    """Mirror of `subprocess.CompletedProcess` so call sites need no reshaping."""

    __slots__ = ("returncode", "stdout", "stderr", "args")

    def __init__(self, args, returncode: int, stdout: str, stderr: str):
        self.args, self.returncode, self.stdout, self.stderr = args, returncode, stdout, stderr

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"CompletedProc(returncode={self.returncode})"


async def run_async(
    cmd: Sequence[str],
    *,
    timeout: float,
    cwd: Optional[Any] = None,
    env: Optional[Mapping[str, str]] = None,
) -> CompletedProc:
    """Non-blocking equivalent of
    `subprocess.run(cmd, capture_output=True, text=True, timeout=…, cwd=…, env=…)`.

    Returns the same (returncode, stdout, stderr) shape. Raises
    `subprocess.TimeoutExpired` on timeout, after killing and reaping the child so a
    timed-out process is not left orphaned.
    """
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(cwd) if cwd is not None else None,
        env=dict(env) if env is not None else None,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        # Reap it, or the child lingers as a zombie for the process lifetime.
        with contextlib.suppress(Exception):
            await proc.wait()
        raise subprocess.TimeoutExpired(list(cmd), timeout)
    return CompletedProc(
        list(cmd),
        proc.returncode or 0,
        out.decode(errors="replace"),
        err.decode(errors="replace"),
    )
