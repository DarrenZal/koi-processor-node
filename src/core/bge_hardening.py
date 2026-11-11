#!/usr/bin/env python3
"""Pure, offline-testable hardening logic for the embedding server (A8).

The embedding server (``bge_server.py``, :8090) fronts the OpenAI embeddings API
for both the ingest pipeline (the event bridge) and interactive search (the query
API). Unbounded concurrency lets an ingest burst open as many simultaneous
upstream calls as requests arrive, which both risks upstream rate-limit errors
and lets a bulk ingest starve interactive search latency.

This module holds the parts that need no web framework so they can be unit
tested without FastAPI/uvicorn:

- env-driven configuration (concurrency, timeouts, input cap, bind host),
- ``normalize_lane`` (which lane a request belongs to),
- ``input_too_large`` (the 413 guard),
- ``LaneLimiter``: a concurrency limiter that reserves capacity for queries.

bge serves no private data, so (per the plan's D8) it carries no service key; the
only protection it needs at the network layer is the loopback bind, gated on a
read-only check that nothing reaches it via the docker-bridge IP. That bind is
configurable here (``bind_host``) with the default left unchanged until that
check runs.

Only the standard library is imported here.
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

# Env var names (documented in one place).
MAX_CONCURRENCY_ENV = "KOI_BGE_MAX_CONCURRENCY"
INGEST_MAX_ENV = "KOI_BGE_INGEST_MAX_CONCURRENCY"
ACQUIRE_TIMEOUT_ENV = "KOI_BGE_ACQUIRE_TIMEOUT"
OPENAI_TIMEOUT_ENV = "KOI_BGE_OPENAI_TIMEOUT"
MAX_INPUT_CHARS_ENV = "KOI_BGE_MAX_INPUT_CHARS"
BIND_HOST_ENV = "KOI_BIND_HOST"

# Defaults. The input cap sits far above the observed p99 input (~1000 chars).
# The acquire timeout plus the upstream timeout stay below the smallest caller
# budget (the bridge's KOI_BGE_TIMEOUT, default 120s): 60 + 30 = 90 < 120.
DEFAULT_MAX_CONCURRENCY = 8
DEFAULT_INGEST_MAX = 5
DEFAULT_ACQUIRE_TIMEOUT = 60.0
DEFAULT_OPENAI_TIMEOUT = 30.0
DEFAULT_MAX_INPUT_CHARS = 8192
# Default bind is left unchanged (all interfaces) until a read-only check
# confirms nothing reaches bge over the docker-bridge IP; the deploy then sets
# KOI_BIND_HOST=127.0.0.1 in a drop-in. The code never hard-codes the bind.
DEFAULT_BIND_HOST = "0.0.0.0"


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value >= 1 else default


def _float_env(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def max_concurrency() -> int:
    return _int_env(MAX_CONCURRENCY_ENV, DEFAULT_MAX_CONCURRENCY)


def ingest_max() -> int:
    """Upper bound on how many global slots ingest may hold at once. Clamped to
    at most (total - 1) so at least one slot is always reserved for queries, and
    to at least 1."""
    total = max_concurrency()
    configured = _int_env(INGEST_MAX_ENV, DEFAULT_INGEST_MAX)
    return max(1, min(configured, total - 1)) if total > 1 else 1


def acquire_timeout() -> float:
    return _float_env(ACQUIRE_TIMEOUT_ENV, DEFAULT_ACQUIRE_TIMEOUT)


def openai_timeout() -> float:
    return _float_env(OPENAI_TIMEOUT_ENV, DEFAULT_OPENAI_TIMEOUT)


def max_input_chars() -> int:
    return _int_env(MAX_INPUT_CHARS_ENV, DEFAULT_MAX_INPUT_CHARS)


def bind_host() -> str:
    return os.getenv(BIND_HOST_ENV, DEFAULT_BIND_HOST)


def normalize_lane(value: Optional[str]) -> str:
    """Map a request's lane to 'ingest' or 'query'. Anything that is not exactly
    'ingest' (case/space-insensitive) is treated as 'query', so an un-updated
    caller that sends no lane gets the interactive, non-starvable lane."""
    if isinstance(value, str) and value.strip().lower() == "ingest":
        return "ingest"
    return "query"


def input_too_large(text: str, limit: Optional[int] = None) -> bool:
    cap = max_input_chars() if limit is None else limit
    return isinstance(text, str) and len(text) > cap


class LaneLimiter:
    """Bound concurrent upstream (OpenAI) calls and reserve capacity for queries.

    An ingest request must hold both an ingest token and a global token; a query
    request holds only a global token. Because ingest can hold at most
    ``ingest_limit`` ingest tokens, it can occupy at most ``ingest_limit`` of the
    ``total`` global slots, so queries always have ``total - ingest_limit`` slots
    available and a bulk ingest can never starve interactive search.

    Acquisition is bounded by a single wall-clock deadline; exceeding it raises
    ``asyncio.TimeoutError`` (the server turns that into a 503) rather than
    queueing unboundedly. Cache hits never enter the limiter.
    """

    def __init__(self, total: Optional[int] = None, ingest_limit: Optional[int] = None):
        self.total = total if total is not None else max_concurrency()
        if self.total < 1:
            self.total = 1
        ilim = ingest_limit if ingest_limit is not None else ingest_max()
        # Always leave at least one global slot for queries.
        self.ingest_limit = max(1, min(ilim, self.total - 1)) if self.total > 1 else 1
        self._global = asyncio.Semaphore(self.total)
        self._ingest = asyncio.Semaphore(self.ingest_limit)

    async def _acquire(self, sem: asyncio.Semaphore, deadline: float) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError()
        await asyncio.wait_for(sem.acquire(), timeout=remaining)

    @asynccontextmanager
    async def slot(self, lane: str, timeout: float):
        """Acquire a slot for ``lane`` within ``timeout`` seconds total, or raise
        asyncio.TimeoutError. Releases every token it took, on success or error."""
        lane = normalize_lane(lane)
        deadline = time.monotonic() + max(0.0, timeout)
        got_ingest = False
        got_global = False
        try:
            if lane == "ingest":
                await self._acquire(self._ingest, deadline)
                got_ingest = True
            await self._acquire(self._global, deadline)
            got_global = True
            yield
        finally:
            if got_global:
                self._global.release()
            if got_ingest:
                self._ingest.release()
