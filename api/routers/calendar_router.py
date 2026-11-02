"""Calendar events endpoint — returns email-ingested ICS invites by date range.

Reads from `koi_memories WHERE source_sensor='ics-event'`. Uses a PG helper
function `try_ts` to safely cast `metadata->>'dtstart'` to timestamptz; malformed
strings are silently skipped (logged at INFO).

V1: source is hard-coded to 'ics-event'. Google Calendar merge is a future
parking-lot item. Range semantics: dtstart must fall inside [from, to).
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class CalendarEvent(BaseModel):
    rid: str
    title: str = ""
    dtstart: Optional[str] = None
    dtend: Optional[str] = None
    location: Optional[str] = None
    organizer: Optional[str] = None
    attendees: List[str] = Field(default_factory=list)
    status: Optional[str] = None
    email_rid: Optional[str] = None
    source_sensor: str = "ics-event"


class CalendarEventsResponse(BaseModel):
    events: List[CalendarEvent]
    count: int
    skipped_malformed: int = 0


async def ensure_try_ts(pool) -> None:
    """Create the `try_ts(text) -> timestamptz` helper function if missing.

    Idempotent (CREATE OR REPLACE). Safe to call at every app startup.
    Uses a PL/pgSQL variant with EXCEPTION WHEN OTHERS to catch all cast errors,
    so regex-passing but invalid strings (e.g., 2026-13-99T...) yield NULL instead
    of raising.
    """
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE OR REPLACE FUNCTION try_ts(t text) RETURNS timestamptz AS $$
            BEGIN
                IF t IS NULL OR t !~ '^\\d{4}-\\d{2}-\\d{2}T' THEN
                    RETURN NULL;
                END IF;
                BEGIN
                    RETURN t::timestamptz;
                EXCEPTION WHEN OTHERS THEN
                    RETURN NULL;
                END;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
        """)


def create_router(pool, caps=None):
    router = APIRouter(prefix="/calendar", tags=["calendar"])

    @router.get("/events", response_model=CalendarEventsResponse)
    async def calendar_events(
        from_ts: datetime = Query(..., alias="from", description="Start timestamp (ISO 8601)"),
        to_ts: datetime = Query(..., alias="to", description="End timestamp, exclusive (ISO 8601)"),
        include_cancelled: bool = Query(False, description="Include cancelled events"),
    ) -> CalendarEventsResponse:
        if to_ts <= from_ts:
            raise HTTPException(status_code=400, detail="'to' must be after 'from'")
        if (to_ts - from_ts).days > 365:
            raise HTTPException(status_code=400, detail="range exceeds 365 days")

        cancelled_clause = (
            "" if include_cancelled
            else " AND (metadata->>'status' IS NULL OR metadata->>'status' != 'cancelled')"
        )
        query = f"""
            SELECT rid, content->>'title' AS title,
                   metadata->>'dtstart' AS dtstart,
                   metadata->>'dtend' AS dtend,
                   metadata->>'location' AS location,
                   metadata->>'organizer' AS organizer,
                   COALESCE(metadata->'attendees', '[]'::jsonb) AS attendees,
                   metadata->>'status' AS status,
                   metadata->>'email_rid' AS email_rid,
                   source_sensor
            FROM koi_memories
            WHERE source_sensor = 'ics-event'
              AND try_ts(metadata->>'dtstart') IS NOT NULL
              AND try_ts(metadata->>'dtstart') >= $1
              AND try_ts(metadata->>'dtstart') <  $2
              {cancelled_clause}
            ORDER BY try_ts(metadata->>'dtstart')
        """
        skipped_q = """
            SELECT COUNT(*) FROM koi_memories
            WHERE source_sensor = 'ics-event'
              AND metadata->>'dtstart' IS NOT NULL
              AND try_ts(metadata->>'dtstart') IS NULL
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, from_ts, to_ts)
            skipped = await conn.fetchval(skipped_q)

        events: List[CalendarEvent] = []
        for r in rows:
            attendees_raw = r["attendees"]
            if isinstance(attendees_raw, str):
                try:
                    attendees = json.loads(attendees_raw)
                except Exception:
                    attendees = []
            else:
                attendees = list(attendees_raw) if attendees_raw else []
            attendees = [a for a in attendees if isinstance(a, str)]
            events.append(CalendarEvent(
                rid=r["rid"],
                title=r["title"] or "",
                dtstart=r["dtstart"],
                dtend=r["dtend"],
                location=r["location"],
                organizer=r["organizer"],
                attendees=attendees,
                status=r["status"],
                email_rid=r["email_rid"],
                source_sensor=r["source_sensor"],
            ))

        if skipped:
            logger.info(f"/calendar/events: {skipped} rows skipped (malformed dtstart)")

        return CalendarEventsResponse(events=events, count=len(events), skipped_malformed=int(skipped or 0))

    return router
