"""Subscription LLM transport for code running inside the :8351 service.

Runs a prompt through the `claude -p` CLI so it bills the Claude Max SUBSCRIPTION,
not a per-token API key. Same approach as `_call_claude_p` in
scripts/extract_deep_documents.py, packaged for api/ callers (claim extraction, B8
chunk contexts) so they stop paying OpenAI for work the subscription already covers.

What makes it correct and cheap:
  * ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN are stripped from the child env. start.sh
    sources personal.env, so a naive child inherits the metered key and `claude` bills
    the API instead of the subscription.
  * `--tools ""` disables every tool. Callers pass untrusted text (wiki pages, email,
    GitHub docs), and a completion with no tools cannot act on an injected instruction.
  * `--strict-mcp-config --setting-sources ""` and cwd=/tmp shed MCP schemas and
    CLAUDE.md, and `--no-session-persistence` keeps each call out of ~/.claude/projects.
  * The prompt goes over stdin, never argv: a 400K-char document is near macOS ARG_MAX
    and argv is visible in `ps`.

Silent failures are errors here: a non-zero exit, non-JSON stdout, an envelope whose
subtype is not "success" (max turns, refusals), and an empty result all raise
ClaudePError. There is deliberately NO fallback to a paid API: a caller that gets
ClaudePError degrades (no claims, no context) and logs it, rather than quietly billing.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import shutil
import time
from typing import Optional

logger = logging.getLogger(__name__)

# The launchd job runs with a minimal PATH that may lack ~/.local/bin, so
# shutil.which() can miss it; fall back to the known install location.
CLAUDE_BIN = (os.getenv("CLAUDE_BIN") or shutil.which("claude")
              or os.path.expanduser("~/.local/bin/claude"))

# Bounds concurrent `claude` processes service-wide (each is a few hundred MB).
_MAX_CONCURRENCY = int(os.getenv("CLAUDE_P_MAX_CONCURRENCY", "4"))
_sem: Optional[asyncio.Semaphore] = None


class ClaudePError(RuntimeError):
    """`claude -p` did not return a usable completion."""


def _semaphore() -> asyncio.Semaphore:
    global _sem
    if _sem is None:
        _sem = asyncio.Semaphore(_MAX_CONCURRENCY)
    return _sem


def _child_env() -> dict:
    return {k: v for k, v in os.environ.items()
            if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")}


def _parse_envelope(stdout: str) -> str:
    """Return the completion text from `--output-format json` stdout, or raise."""
    try:
        env = json.loads(stdout)
    except json.JSONDecodeError:
        raise ClaudePError(f"non-JSON stdout: {stdout[:300]!r}")
    if not isinstance(env, dict):
        raise ClaudePError(f"unexpected envelope: {str(env)[:300]}")
    if env.get("is_error") or env.get("subtype") != "success":
        raise ClaudePError(f"claude -p reported {env.get('subtype')!r}: "
                           f"{str(env.get('result'))[:300]}")
    text = env.get("result") or ""
    if not text.strip():
        raise ClaudePError("claude -p returned an empty result")
    return text


async def complete(prompt: str, *, model: str, deadline_s: float = 200.0,
                   max_attempts: int = 2) -> str:
    """Return the completion for `prompt` via `claude -p` on the subscription.

    `deadline_s` bounds the whole call, including time spent waiting for a concurrency
    slot and any retry, so a caller with its own HTTP timeout can size this below it.
    Raises ClaudePError when no attempt succeeds within the deadline.
    """
    if not CLAUDE_BIN or not os.path.exists(CLAUDE_BIN):
        raise ClaudePError(f"claude CLI not found (CLAUDE_BIN={CLAUDE_BIN!r})")
    cmd = [CLAUDE_BIN, "-p", "--output-format", "json", "--model", model,
           "--tools", "", "--strict-mcp-config", "--setting-sources", "",
           "--no-session-persistence"]
    end = time.monotonic() + deadline_s
    last = "no attempt made"
    async with _semaphore():
        for attempt in range(max_attempts):
            remaining = end - time.monotonic()
            if remaining <= 1:
                break
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE, env=_child_env(), cwd="/tmp")
            except OSError as e:
                raise ClaudePError(f"failed to spawn {CLAUDE_BIN}: {e}")
            try:
                out, err = await asyncio.wait_for(proc.communicate(prompt.encode()),
                                                  timeout=remaining)
            except asyncio.TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                # Reap it, or the child lingers as a zombie for the process lifetime.
                with contextlib.suppress(Exception):
                    await proc.wait()
                last = f"timeout after {remaining:.0f}s"
                break
            if proc.returncode != 0:
                last = f"exit {proc.returncode}: {err.decode(errors='replace')[:300]}"
            else:
                try:
                    return _parse_envelope(out.decode(errors="replace"))
                except ClaudePError as e:
                    last = str(e)
            if attempt + 1 < max_attempts:
                await asyncio.sleep(min(2 ** attempt, max(0.0, end - time.monotonic())))
    raise ClaudePError(f"claude -p ({model}) failed: {last}")
