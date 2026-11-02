"""Keep excluded Claude sessions out of the user-facing session readers (migration 136).

Excluding a session in the claude-sessions sensor only stops future ingestion. Its
session_ingestion_log row (summary, first prompt, files accessed) and its discourse moves stay,
because the exclusion and audit need them, and the session readers served them. The sensor
records each excluded session in session_exclusions from an exact-session manifest, and every
reader that returns per-session rows filters on it.

The readers fail closed. The migration is applied before this code, so a missing table means
the exclusion list was lost, not that nothing is excluded: they answer 503 instead of
serving everything. KOI_SESSION_READERS_DISABLED=true makes them answer 503 too, and is the
backout if the filtered readers misbehave. Rolling back to unfiltered code, dropping the table
or deleting marks would expose excluded sessions again.
"""
from __future__ import annotations

import os

from fastapi import HTTPException


def readers_disabled() -> bool:
    return os.getenv("KOI_SESSION_READERS_DISABLED", "false").lower() in ("true", "1", "yes")


async def _require_exclusion_list(conn) -> None:
    if readers_disabled():
        raise HTTPException(status_code=503, detail="Session readers are disabled (KOI_SESSION_READERS_DISABLED)")
    rows = await conn.fetch("SELECT to_regclass('session_exclusions') IS NOT NULL AS present")
    if not (rows and rows[0]["present"]):
        raise HTTPException(status_code=503, detail="Session exclusion list unavailable; session readers fail closed")


async def session_exclusion_filter(conn, column: str) -> str:
    """SQL predicate that drops rows of excluded sessions. Raises 503 when the readers are
    disabled or session_exclusions is missing. `column` is always a literal from the calling
    module, never request input."""
    await _require_exclusion_list(conn)
    return f"NOT EXISTS (SELECT 1 FROM session_exclusions sx WHERE sx.session_id = {column})"
