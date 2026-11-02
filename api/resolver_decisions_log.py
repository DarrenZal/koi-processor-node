"""Durable, unsampled log of resolve_entity() accept/reject/create decisions.

Writes to the `resolver_decisions` table (migration 115). Distinct from
resolver_shadow.py, which samples at 10% to A/B-compare legacy vs strict
resolution POLICIES. This module logs the FULL population of what the live
resolver actually decided, so rejections stop being destroyed into an
unrotated stderr.log — they are the only free negative labels this system
gets, per the 2026-08-31 pipeline hardening audit (Phase 0 #4).

Fire-and-forget by design: logging must NEVER affect entity resolution.
Each call spawns a background task on its OWN connection (never the
caller's `conn`/transaction), and every exception is caught and logged,
never raised.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger("api.resolver_decisions_log")

# Keep references to in-flight background tasks so they aren't garbage
# collected mid-write (a bare asyncio.create_task() result with no other
# reference can be GC'd before it completes).
_inflight: set = set()

_VALID_TIERS = {
    "tier1_exact", "tier1_1_alias", "tier1_1b_cross_type",
    "tier1_5_contextual", "tier2a_fuzzy", "tier2b_semantic", "tier3_create",
}
_VALID_DECISIONS = {"accepted", "rejected", "created"}


async def _write(
    pool: asyncpg.Pool,
    *,
    attempt_id: str,
    caller: str,
    entity_type: Optional[str],
    query_text: str,
    query_normalized: str,
    tier: str,
    decision: str,
    candidate_uri: Optional[str],
    candidate_text: Optional[str],
    score: Optional[float],
    reason: Optional[str],
) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO resolver_decisions
                    (attempt_id, caller, entity_type, query_text, query_normalized,
                     tier, decision, candidate_uri, candidate_text, score, reason)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                attempt_id, caller, entity_type, query_text, query_normalized,
                tier, decision, candidate_uri, candidate_text, score, reason,
            )
    except Exception:
        # Never let a logging failure surface anywhere near entity resolution.
        logger.warning(
            "resolver_decisions write failed (attempt=%s tier=%s decision=%s)",
            attempt_id, tier, decision, exc_info=True,
        )


def log_decision(
    pool: Optional[asyncpg.Pool],
    *,
    attempt_id: str,
    caller: str,
    entity_type: Optional[str],
    query_text: str,
    query_normalized: str,
    tier: str,
    decision: str,
    candidate_uri: Optional[str] = None,
    candidate_text: Optional[str] = None,
    score: Optional[float] = None,
    reason: Optional[str] = None,
) -> None:
    """Schedule a best-effort, non-blocking write. Safe to call unconditionally."""
    if pool is None:
        return
    if tier not in _VALID_TIERS or decision not in _VALID_DECISIONS:
        logger.warning("resolver_decisions: refusing malformed call tier=%r decision=%r", tier, decision)
        return
    try:
        task = asyncio.create_task(
            _write(
                pool,
                attempt_id=attempt_id,
                caller=caller,
                entity_type=entity_type,
                query_text=query_text,
                query_normalized=query_normalized,
                tier=tier,
                decision=decision,
                candidate_uri=candidate_uri,
                candidate_text=candidate_text,
                score=score,
                reason=reason,
            )
        )
        _inflight.add(task)
        task.add_done_callback(_inflight.discard)
    except RuntimeError:
        # No running event loop (e.g. called from sync test code) -- drop.
        logger.warning("resolver_decisions: no running event loop; decision dropped")
