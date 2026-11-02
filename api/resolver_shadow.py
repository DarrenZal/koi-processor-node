"""Bounded, behavior-neutral shadow measurement for resolver policies.

The observer never queries the database, calls an embedding provider, or
changes a resolver return value.  Callers feed it candidates already present in
their active resolver loop.  Emission uses a bounded non-blocking queue so a
slow log sink can drop evidence but cannot delay ingestion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
import queue
import threading
import time
import uuid
from typing import Optional


LOG_PREFIX = "RESOLVER_SHADOW "
_logger = logging.getLogger("api.resolver_shadow.observation")
_STOP = object()
_queue: queue.Queue = queue.Queue(
    maxsize=max(1, int(os.getenv("KOI_RESOLVER_SHADOW_QUEUE_SIZE", "1024")))
)
_thread: Optional[threading.Thread] = None
_thread_lock = threading.Lock()
_stats_lock = threading.Lock()
_stats = {"emitted": 0, "dropped": 0}


def _enabled() -> bool:
    return os.getenv("KOI_RESOLVER_SHADOW_ENABLED", "false").lower() == "true"


def _sample_rate() -> float:
    try:
        value = float(os.getenv("KOI_RESOLVER_SHADOW_SAMPLE_RATE", "0.10"))
    except ValueError:
        value = 0.10
    return min(1.0, max(0.0, value))


def _is_sampled(attempt_id: str) -> bool:
    # Stable for a given attempt id, independent of process-global RNG state.
    bucket = int(attempt_id[:8], 16) / 0xFFFFFFFF
    return bucket < _sample_rate()


def _worker() -> None:
    while True:
        item = _queue.get()
        try:
            if item is _STOP:
                return
            _logger.info("%s%s", LOG_PREFIX, json.dumps(item, sort_keys=True))
            with _stats_lock:
                _stats["emitted"] += 1
        finally:
            _queue.task_done()


def _ensure_worker() -> None:
    global _thread
    if _thread and _thread.is_alive():
        return
    with _thread_lock:
        if _thread and _thread.is_alive():
            return
        _thread = threading.Thread(
            target=_worker,
            name="resolver-shadow-emitter",
            daemon=True,
        )
        _thread.start()


def emit_nonblocking(record: dict) -> bool:
    """Queue one observation without ever blocking the resolver."""
    _ensure_worker()
    try:
        _queue.put_nowait(record)
        return True
    except queue.Full:
        with _stats_lock:
            _stats["dropped"] += 1
        return False


def emitter_status() -> dict:
    with _stats_lock:
        stats = dict(_stats)
    return {
        "enabled": _enabled(),
        "sample_rate": _sample_rate(),
        "queue_size": _queue.qsize(),
        "queue_capacity": _queue.maxsize,
        **stats,
    }


def shutdown_emitter(timeout: float = 1.0) -> None:
    """Best-effort flush used by API shutdown and tests."""
    global _thread
    if not _thread or not _thread.is_alive():
        return
    try:
        _queue.put_nowait(_STOP)
    except queue.Full:
        return
    _thread.join(timeout=timeout)


@dataclass
class _PolicySelection:
    fuzzy_uri: Optional[str] = None
    fuzzy_score: float = 0.0
    semantic_uri: Optional[str] = None
    semantic_score: float = 0.0

    def observe(self, *, uri: str, score: float, tier: str, accepted: bool) -> None:
        if not accepted:
            return
        if tier == "fuzzy":
            if score > self.fuzzy_score:
                self.fuzzy_uri = uri
                self.fuzzy_score = score
            return
        if tier == "semantic":
            # A counterfactual policy may already have accepted a fuzzy
            # candidate even though the active policy continued to semantic.
            # Fuzzy wins by tier precedence; observing semantic is then a no-op,
            # not an invalid state.
            if self.fuzzy_uri is None and score > self.semantic_score:
                self.semantic_uri = uri
                self.semantic_score = score
            return
        raise ValueError(f"unsupported shadow tier: {tier}")

    def outcome(self, fallback: str) -> tuple[str, str]:
        if self.fuzzy_uri:
            return self.fuzzy_uri, "fuzzy"
        if self.semantic_uri:
            return self.semantic_uri, "semantic"
        return fallback, fallback


@dataclass
class ResolverShadowAttempt:
    attempt_id: str
    sampled: bool
    caller: str
    engine: str
    entity_type: str
    query_norm: str
    active_policy: str
    # Replayed attempts carry real counterfactual data but meaningless latency: in a
    # replay the shadow comparison IS the work, so shadow_overhead_ms/resolver_elapsed_ms
    # approaches 1.0. Untagged, a replay large enough to satisfy the attempt bar would
    # drag the p95 overhead ratio past its threshold and fail the gate for a reason that
    # has nothing to do with the policy being measured.
    replay: bool = False
    started_ns: int = field(default_factory=time.perf_counter_ns)
    shadow_ns: int = 0
    candidates_observed: int = 0
    candidate_divergences: int = 0
    legacy: _PolicySelection = field(default_factory=_PolicySelection)
    strict: _PolicySelection = field(default_factory=_PolicySelection)

    def observe_candidate(
        self,
        *,
        uri: str,
        score: float,
        tier: str,
        legacy_accepts: bool,
        strict_accepts: bool,
        elapsed_ns: int,
    ) -> None:
        if not self.sampled:
            return
        self.shadow_ns += elapsed_ns
        self.candidates_observed += 1
        if legacy_accepts != strict_accepts:
            self.candidate_divergences += 1
        self.legacy.observe(
            uri=uri, score=score, tier=tier, accepted=legacy_accepts
        )
        self.strict.observe(
            uri=uri, score=score, tier=tier, accepted=strict_accepts
        )

    def finish(
        self,
        *,
        active_uri: Optional[str],
        active_outcome: str,
        legacy_fallback: str,
        strict_fallback: str,
    ) -> Optional[dict]:
        if not self.sampled:
            return None
        legacy_uri, legacy_outcome = self.legacy.outcome(legacy_fallback)
        strict_uri, strict_outcome = self.strict.outcome(strict_fallback)
        record = {
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "replay": self.replay,
            "attempt_id": self.attempt_id,
            "caller": self.caller,
            "engine": self.engine,
            "entity_type": self.entity_type,
            "query_norm": self.query_norm,
            "active_policy": self.active_policy,
            "active_uri": active_uri,
            "active_outcome": active_outcome,
            "legacy_uri": legacy_uri,
            "legacy_outcome": legacy_outcome,
            "strict_uri": strict_uri,
            "strict_outcome": strict_outcome,
            "outcome_diverged": (legacy_uri, legacy_outcome)
            != (strict_uri, strict_outcome),
            "candidates_observed": self.candidates_observed,
            "candidate_divergences": self.candidate_divergences,
            "shadow_overhead_ms": round(self.shadow_ns / 1_000_000, 4),
            "resolver_elapsed_ms": round(
                (time.perf_counter_ns() - self.started_ns) / 1_000_000, 4
            ),
        }
        emit_nonblocking(record)
        return record


def start_attempt(
    *,
    caller: str,
    engine: str,
    entity_type: str,
    query_norm: str,
    active_policy: str,
    attempt_id: Optional[str] = None,
    sampled_override: Optional[bool] = None,
    replay: bool = False,
) -> ResolverShadowAttempt:
    attempt_id = attempt_id or uuid.uuid4().hex
    sampled = (
        sampled_override
        if sampled_override is not None
        else _enabled() and _is_sampled(attempt_id)
    )
    return ResolverShadowAttempt(
        attempt_id=attempt_id,
        sampled=sampled,
        caller=caller,
        engine=engine,
        entity_type=entity_type,
        query_norm=query_norm,
        active_policy=active_policy,
        replay=replay,
    )
