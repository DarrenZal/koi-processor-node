"""Task registry endpoints (ingest, list, update, stats).

Provides dedicated task storage to replace the fragmented /register-entity approach.
All task writers (meeting-notes, task-agent, /tasks skill) converge here.

Routes are prefix-relative — prefix "/tasks" is applied at mount in personal_ingest_api.py.
"""

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator

from api.utils import parse_ts, validity_filter_clause
from api.optional_federation import optional_federation_attr

logger = logging.getLogger(__name__)

# Longest free-text note kept per PATCH. Longer text is truncated, not rejected,
# so callers that previously had `notes` silently dropped do not start to 422.
MAX_NOTE_CHARS = 4000

logger = logging.getLogger(__name__)

# Longest free-text note kept per PATCH. Longer text is truncated, not rejected,
# so callers that previously had `notes` silently dropped do not start to 422.
MAX_NOTE_CHARS = 4000


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

# Status/priority constraints — must match DB CHECK constraints on task_registry.
# Some upstream skills emit Python-enum-style "in_progress"; DB requires
# "in-progress". Normalize underscore→hyphen at input + reject unknown values
# with 422 instead of letting them 500 at INSERT.
ALLOWED_STATUSES = {"inbox", "open", "in-progress", "waiting", "done", "cancelled"}
ALLOWED_PRIORITIES = {"critical", "high", "medium", "low"}


def _normalize_status(v: Optional[str]) -> Optional[str]:
    if v is None:
        return v
    v2 = v.replace("_", "-").lower().strip()
    if v2 not in ALLOWED_STATUSES:
        raise ValueError(f"status must be one of {sorted(ALLOWED_STATUSES)}")
    return v2


def _normalize_priority(v: Optional[str]) -> Optional[str]:
    if v is None:
        return v
    v2 = v.lower().strip()
    if v2 not in ALLOWED_PRIORITIES:
        raise ValueError(f"priority must be one of {sorted(ALLOWED_PRIORITIES)}")
    return v2

class TaskIngestRequest(BaseModel):
    # Unknown keys are still ignored (they never reach the INSERT), but they are
    # kept on the model so ingest_task can log their NAMES. That is how callers
    # that send fields this API does not know are found before the model is
    # tightened to extra="forbid".
    model_config = ConfigDict(extra="allow")

    taskKey: str = Field(..., description="Idempotency key (e.g. 'meeting-2026-02-27-slug')")
    uuid: Optional[str] = None
    title: str
    # Fix 2: status/priority default to None so partial payloads don't regress
    # existing values. INSERT uses COALESCE($, 'inbox') for new rows.
    status: Optional[str] = None
    priority: Optional[str] = None
    dueDate: Optional[str] = None       # ISO date string YYYY-MM-DD
    startDate: Optional[str] = None
    waitUntil: Optional[str] = None
    validityStart: Optional[str] = None  # ISO 8601 -> TIMESTAMPTZ
    validityEnd: Optional[str] = None    # ISO 8601 -> TIMESTAMPTZ
    context: Optional[str] = None
    effort: Optional[str] = None
    ownerWikilink: Optional[str] = None        # "[[People/Name|alias]]" or "Name"
    projectWikilink: Optional[str] = None      # "[[Projects/Name]]" or plain text
    collaboratorWikilinks: Optional[List[str]] = []
    blockedBy: Optional[List[str]] = []
    sourceNote: Optional[str] = None
    sourceType: Optional[str] = None
    vaultPath: Optional[str] = None
    tags: Optional[List[str]] = []

    @field_validator("status")
    @classmethod
    def _norm_status(cls, v):
        return _normalize_status(v)

    @field_validator("priority")
    @classmethod
    def _norm_priority(cls, v):
        return _normalize_priority(v)


class TaskIngestResponse(BaseModel):
    task_key: str
    action: str    # "created" or "updated"
    id: int


class TaskRecord(BaseModel):
    id: int
    task_key: str
    uuid: Optional[str] = None
    title: str
    status: str
    priority: Optional[str] = None
    due_date: Optional[str] = None
    start_date: Optional[str] = None
    wait_until: Optional[str] = None
    context: Optional[str] = None
    effort: Optional[str] = None
    owner_uri: Optional[str] = None
    project_uri: Optional[str] = None
    collaborator_uris: List[str] = []
    blocked_by: List[str] = []
    source_note: Optional[str] = None
    source_type: Optional[str] = None
    vault_path: Optional[str] = None
    tags: List[str] = []
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    started_at: Optional[str] = None
    triaged_at: Optional[str] = None
    validity_start: Optional[str] = None
    validity_end: Optional[str] = None
    # Append-only log written by PATCH {"notes": ...}, stored in task_registry.metadata->'notes'.
    notes: List[Dict[str, Any]] = []


class TaskPatchRequest(BaseModel):
    # Unknown keys are tolerated for now but reported: patch_task logs their names
    # and returns them as `ignored_fields`, so a field that is silently dropped is
    # visible to the caller. Switch to extra="forbid" once the known offenders
    # (completedAt from triage_close.py, taskKey from gate_helpers.py) are fixed.
    model_config = ConfigDict(extra="allow")

    # Fix 3: all fields default to None; model_fields_set distinguishes
    # "not provided" from "explicitly null" for date clearing.
    title: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    dueDate: Optional[str] = None
    startDate: Optional[str] = None
    waitUntil: Optional[str] = None
    validityStart: Optional[str] = None
    validityEnd: Optional[str] = None
    context: Optional[str] = None
    effort: Optional[str] = None
    ownerWikilink: Optional[str] = None
    projectWikilink: Optional[str] = None
    collaboratorWikilinks: Optional[List[str]] = None
    blockedBy: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    # Free-text rationale for this change (for example why a task was closed).
    # Appended to metadata->'notes' as {at, status, text}; never overwrites.
    notes: Optional[str] = None

    @field_validator("status")
    @classmethod
    def _norm_status(cls, v):
        return _normalize_status(v)

    @field_validator("priority")
    @classmethod
    def _norm_priority(cls, v):
        return _normalize_priority(v)


class TaskStatsResponse(BaseModel):
    """Aggregate counts for the *personal task view*.

    Every field below excludes ``source_type`` of ``test`` and ``outbound-comm``,
    matching what ``GET /tasks/`` shows by default. That is deliberate — the comms
    outbox is not a to-do list — but it means these are NOT registry-wide totals,
    and reading ``total_done`` as "how many tasks are done" was wrong by 138 rows
    on 2026-08-13 (404 reported, 542 in the table).

    ``excluded`` makes the gap visible instead of silent: counts of what these
    numbers leave out, keyed by source_type.
    """

    total_open: int
    total_done: int
    by_status: Dict[str, int]
    overdue: int
    due_today: int
    due_this_week: int
    excluded: Dict[str, Dict[str, int]] = {}


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_router(pool, caps) -> APIRouter:
    """Return an APIRouter for task registry endpoints.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities object (unused for tasks, included for consistency).
    """
    router = APIRouter(tags=["tasks"])

    emit_domain_event = optional_federation_attr("api.federation_events", "emit_domain_event")

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _parse_wikilink_path(wikilink: str) -> Optional[str]:
        """Extract vault path from a wikilink like [[Folder/Name|alias]].

        Returns the path portion (e.g. "People/David Fortson"), or None if
        the input is not a wikilink format.
        """
        m = re.match(r'^\[\[([^\]|]+)(?:\|[^\]]+)?\]\]$', wikilink.strip())
        return m.group(1) if m else None

    async def _resolve_entity(conn, raw: str, entity_type: str) -> Optional[str]:
        """Resolve a wikilink or plain name to a canonical_uri.

        Attempts:
        1. Wikilink vault path lookup in entity_rid_mappings (exact, case-insensitive)
        2. Plain text name lookup in entity_rid_mappings (case-insensitive)
        3. Normalized text lookup in entity_registry (case-insensitive)

        entity_type hint prevents cross-type collisions (e.g. a Project named
        "RiverlinkAI" won't resolve as a Person).
        """
        if not raw or not raw.strip():
            return None

        raw = raw.strip()
        vault_path = _parse_wikilink_path(raw)

        if vault_path:
            # Tier 1: wikilink vault path lookup
            row = await conn.fetchrow(
                """
                SELECT canonical_uri FROM entity_rid_mappings
                WHERE LOWER(vault_path) = LOWER($1)
                  AND (entity_type = $2 OR entity_type IS NULL)
                LIMIT 1
                """,
                vault_path, entity_type
            )
            if row:
                return row["canonical_uri"]

        # Tier 2: plain name lookup (works for both unresolved wikilinks
        # and plain text names like "Orrin Tavish" or "RiverlinkAI")
        name = vault_path.split("/")[-1] if vault_path else raw
        row = await conn.fetchrow(
            """
            SELECT canonical_uri FROM entity_rid_mappings
            WHERE LOWER(name) = LOWER($1)
              AND (entity_type = $2 OR entity_type IS NULL)
            LIMIT 1
            """,
            name, entity_type
        )
        if row:
            return row["canonical_uri"]

        # Tier 3: entity_registry normalized_text fallback
        row = await conn.fetchrow(
            """
            SELECT fuseki_uri FROM entity_registry
            WHERE LOWER(normalized_text) = LOWER($1)
              AND (entity_type = $2 OR entity_type IS NULL)
            LIMIT 1
            """,
            name.lower().strip(), entity_type
        )
        if row:
            # Follow a merge: this is a RESOLUTION path (the uri is persisted as an
            # owner / subject), so excluding a tombstone would lose the binding entirely
            # while following it attaches to the entity the caller meant.
            from api.resolution_primitives import resolve_to_live_uri
            return await resolve_to_live_uri(conn, row["fuseki_uri"])

        return None

    def _row_to_dict(row) -> Dict[str, Any]:
        """Convert an asyncpg Record to a serialisable dict."""
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        # metadata is jsonb, which asyncpg returns as str here (no codec registered).
        # Surface only the notes log; the rest of metadata stays internal.
        meta = d.pop("metadata", None)
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except ValueError:
                meta = None
        notes = meta.get("notes") if isinstance(meta, dict) else None
        d["notes"] = notes if isinstance(notes, list) else []
        return d

    def _parse_date(s: Optional[str]) -> Optional[date]:
        """Parse an ISO date string; return None on failure or None input."""
        if not s:
            return None
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None

    def _parse_date_strict(name: str, s: Optional[str]) -> Optional[date]:
        """Like _parse_date, but a present-and-unparseable value 422s instead
        of silently dropping the filter. _parse_date conflates "absent" and
        "unparseable" into the same None — fine for the write path (a missing
        field is meant to leave the column untouched), wrong for the GET-list
        read path, where a caller filtering by ?due_before=<malformed value>
        (e.g. a full ISO datetime — only YYYY-MM-DD parses) silently got back
        every task with HTTP 200 instead of the filtered set it asked for.
        """
        if not s:
            return None
        try:
            return date.fromisoformat(s)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail=f"{name} must be an ISO date (YYYY-MM-DD), got {s!r}",
            )

    # -----------------------------------------------------------------------
    # POST /ingest — upsert by taskKey
    # -----------------------------------------------------------------------

    @router.post("/ingest", response_model=TaskIngestResponse)
    async def ingest_task(req: TaskIngestRequest):
        """Upsert a task by taskKey. Used by meeting-notes, task-agent, and /tasks add.

        Partial payloads are safe: existing status/priority/dates are preserved
        via COALESCE on conflict. Only title is always overwritten.
        """
        extras = sorted((req.model_extra or {}).keys())
        if extras:
            logger.warning("POST /tasks/ingest %s: ignored unknown field(s): %s", req.taskKey, ", ".join(extras))
        async with pool.acquire() as conn:
            owner_uri = await _resolve_entity(conn, req.ownerWikilink, "Person") if req.ownerWikilink else None
            project_uri = await _resolve_entity(conn, req.projectWikilink, "Project") if req.projectWikilink else None

            collab_uris: List[str] = []
            for c in (req.collaboratorWikilinks or []):
                uri = await _resolve_entity(conn, c, "Person")
                if uri:
                    collab_uris.append(uri)

            due_date = _parse_date(req.dueDate)
            start_date = _parse_date(req.startDate)
            wait_until = _parse_date(req.waitUntil)
            validity_start = parse_ts(req.validityStart)
            validity_end = parse_ts(req.validityEnd)

            row = await conn.fetchrow(
                """
                INSERT INTO task_registry (
                    task_key, uuid, title, status, priority,
                    due_date, start_date, wait_until, context, effort,
                    owner_uri, project_uri, collaborator_uris, blocked_by,
                    source_note, source_type, vault_path, tags,
                    validity_start, validity_end,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3,
                    COALESCE($4, 'inbox'), COALESCE($5, 'medium'),
                    $6, $7, $8, $9, $10,
                    $11, $12, $13, $14,
                    $15, COALESCE($16, 'meeting'), $17, $18,
                    $19, $20,
                    -- UTC, not NOW(). created_at/updated_at are `timestamp without time
                    -- zone`, so a bare NOW() is cast using the session TimeZone
                    -- (America/Vancouver) and lands in LOCAL time, while PATCH writes the
                    -- same columns from Python as naive UTC. One row could therefore carry
                    -- created_at 2026-08-13T18:44:13 and updated_at 2026-08-14T02:00:55 for
                    -- events 7 hours apart in wall-clock but simultaneous in fact, and the
                    -- updated_before/updated_after filters compared the two clocks directly.
                    NOW() AT TIME ZONE 'UTC', NOW() AT TIME ZONE 'UTC'
                )
                ON CONFLICT (task_key) DO UPDATE SET
                    uuid            = COALESCE(EXCLUDED.uuid, task_registry.uuid),
                    title           = EXCLUDED.title,
                    -- Fix 2: reference $4/$5 directly (EXCLUDED.status is never NULL
                    -- because VALUES applied COALESCE, so we can't use COALESCE(EXCLUDED...))
                    status          = CASE WHEN $4 IS NULL THEN task_registry.status ELSE $4 END,
                    priority        = CASE WHEN $5 IS NULL THEN task_registry.priority ELSE $5 END,
                    due_date        = COALESCE(EXCLUDED.due_date, task_registry.due_date),
                    start_date      = COALESCE(EXCLUDED.start_date, task_registry.start_date),
                    wait_until      = COALESCE(EXCLUDED.wait_until, task_registry.wait_until),
                    context         = COALESCE(EXCLUDED.context, task_registry.context),
                    effort          = COALESCE(EXCLUDED.effort, task_registry.effort),
                    owner_uri       = COALESCE(EXCLUDED.owner_uri, task_registry.owner_uri),
                    project_uri     = COALESCE(EXCLUDED.project_uri, task_registry.project_uri),
                    collaborator_uris = CASE WHEN array_length(EXCLUDED.collaborator_uris, 1) > 0
                                            THEN EXCLUDED.collaborator_uris
                                            ELSE task_registry.collaborator_uris END,
                    blocked_by      = CASE WHEN array_length(EXCLUDED.blocked_by, 1) > 0
                                          THEN EXCLUDED.blocked_by
                                          ELSE task_registry.blocked_by END,
                    source_note     = COALESCE(EXCLUDED.source_note, task_registry.source_note),
                    source_type     = COALESCE(EXCLUDED.source_type, task_registry.source_type),
                    vault_path      = COALESCE(EXCLUDED.vault_path, task_registry.vault_path),
                    tags            = CASE WHEN array_length(EXCLUDED.tags, 1) > 0
                                          THEN EXCLUDED.tags
                                          ELSE task_registry.tags END,
                    validity_start  = COALESCE(EXCLUDED.validity_start, task_registry.validity_start),
                    validity_end    = COALESCE(EXCLUDED.validity_end,   task_registry.validity_end),
                    updated_at      = NOW() AT TIME ZONE 'UTC'   -- see the INSERT branch above
                RETURNING id, task_key,
                    (xmax = 0) AS was_inserted
                """,
                req.taskKey, req.uuid, req.title,
                req.status, req.priority,
                due_date, start_date, wait_until, req.context, req.effort,
                owner_uri, project_uri, collab_uris, req.blockedBy or [],
                req.sourceNote, req.sourceType, req.vaultPath,
                req.tags or [],
                validity_start, validity_end,
            )

        action = "created" if row["was_inserted"] else "updated"
        await emit_domain_event("task", "NEW" if action == "created" else "UPDATE", req.taskKey, {
            "task_key": req.taskKey, "uuid": req.uuid, "title": req.title,
            "status": req.status, "priority": req.priority,
            "due_date": req.dueDate, "start_date": req.startDate,
            "wait_until": req.waitUntil, "context": req.context, "effort": req.effort,
            "owner_uri": owner_uri, "project_uri": project_uri,
            "collaborator_uris": collab_uris, "blocked_by": req.blockedBy or [],
            "source_note": req.sourceNote, "source_type": req.sourceType,
            "vault_path": req.vaultPath, "tags": req.tags or [],
            "validity_start": req.validityStart,
            "validity_end": req.validityEnd,
        })
        return TaskIngestResponse(task_key=row["task_key"], action=action, id=row["id"])

    # -----------------------------------------------------------------------
    # GET / — list tasks with filters
    # -----------------------------------------------------------------------

    @router.get("/", response_model=List[TaskRecord])
    async def list_tasks(
        response: Response,
        status: Optional[str] = Query(None, description="Comma-separated statuses; omit to exclude done,cancelled"),
        owner: Optional[str] = Query(None, description="Name or URI substring — matches entity_rid_mappings name or owner_uri"),
        project: Optional[str] = Query(None, description="Case-insensitive project name lookup → project_uri; fallback substring on project_uri"),
        project_uri: Optional[str] = Query(None, description="Exact project_uri match (preferred over ?project)"),
        due_before: Optional[str] = Query(None, description="ISO date — tasks due before this date"),
        due_after: Optional[str] = Query(None, description="ISO date — tasks due after (inclusive) this date"),
        updated_before: Optional[str] = Query(None, description="ISO date — tasks last updated before this date (stale-no-activity filter)"),
        updated_after: Optional[str] = Query(None, description="ISO date — tasks last updated on or after this date"),
        source_note: Optional[str] = Query(None, description="Substring match on source_note"),
        source_type: Optional[str] = Query(None, description="Exact match on source_type"),
        t_now: Optional[str] = Query(
            None,
            description=(
                "ISO 8601 timestamp. When set, filters out tasks whose "
                "[validity_start, validity_end] window does not contain t_now. "
                "Absent t_now = no filter (default-off, opt-in). Trailing 'Z' "
                "accepted; naive ISO treated as UTC."
            ),
        ),
        include_out_of_window: bool = Query(
            False, description="Bypass validity filter when t_now is set."
        ),
        limit: int = Query(100, ge=1, le=5000),
        offset: int = Query(0, ge=0),
    ):
        """List tasks with optional filters. Default excludes done and cancelled.

        **Also excluded by default: ``source_type='outbound-comm'``** (the comms
        outbox). This applies to every request that omits ``source_type``,
        *including* one that passes an explicit ``status`` — so ``?status=done``
        alone does NOT return every done row. Pass ``?source_type=outbound-comm``
        for the outbox alone, or ``?source_type=all`` to disable the exclusion
        and get everything.

        Sets an ``X-Total-Count`` response header with the number of rows
        matching the filters (before LIMIT/OFFSET) so paginating clients can
        size their loop without re-querying. Note that the header counts the
        rows the filters actually select, so under the default exclusion it
        counts the visible subset, not the whole registry.
        """
        async with pool.acquire() as conn:
            conditions = []
            params: List[Any] = []

            def add(clause: str, val: Any):
                params.append(val)
                conditions.append(clause.replace("?", f"${len(params)}"))

            # Status filter
            if status:
                status_list = [s.strip() for s in status.split(",")]
                params.append(status_list)
                conditions.append(f"status = ANY(${len(params)})")
            else:
                conditions.append("status NOT IN ('done', 'cancelled')")

            # Project filter (URI exact > name lookup > substring)
            if project_uri:
                add("project_uri = ?", project_uri)
            elif project:
                proj_row = await conn.fetchrow(
                    """
                    SELECT canonical_uri FROM entity_rid_mappings
                    WHERE LOWER(name) = LOWER($1)
                      AND (entity_type = 'Project' OR entity_type IS NULL)
                    LIMIT 1
                    """,
                    project
                )
                if proj_row:
                    add("project_uri = ?", proj_row["canonical_uri"])
                else:
                    add("LOWER(project_uri) LIKE LOWER(?)", f"%{project}%")

            # Fix 4: owner filter checks entity name (via entity_rid_mappings) OR
            # falls back to URI substring so opaque URIs still work.
            if owner:
                params.append(f"%{owner}%")
                n = len(params)
                conditions.append(
                    f"""(
                        LOWER(owner_uri) LIKE LOWER(${n})
                        OR owner_uri IN (
                            SELECT canonical_uri FROM entity_rid_mappings
                            WHERE LOWER(name) LIKE LOWER(${n})
                              AND (entity_type = 'Person' OR entity_type IS NULL)
                        )
                    )"""
                )

            # Date filters. Strict: a present-but-malformed value 422s rather
            # than silently widening the query to unfiltered (see
            # _parse_date_strict). Only the read path is strict — POST
            # /ingest and PATCH keep the lenient _parse_date, since a missing
            # or malformed date there is meant to leave the column untouched
            # for MCP callers, not reject the whole write.
            if due_before:
                d = _parse_date_strict("due_before", due_before)
                if d:
                    add("due_date < ?", d)
            if due_after:
                d = _parse_date_strict("due_after", due_after)
                if d:
                    add("due_date >= ?", d)
            if updated_before:
                d = _parse_date_strict("updated_before", updated_before)
                if d:
                    add("updated_at < ?", d)
            if updated_after:
                d = _parse_date_strict("updated_after", updated_after)
                if d:
                    add("updated_at >= ?", d)

            # Source filters
            if source_note:
                add("LOWER(source_note) LIKE LOWER(?)", f"%{source_note}%")
            if source_type and source_type != "all":
                add("source_type = ?", source_type)
            elif source_type != "all":
                # Comms outbox (source_type='outbound-comm') is a distinct category, not a
                # personal to-do — hidden from the default task view like done/cancelled are.
                # Query it explicitly with ?source_type=outbound-comm. (comms-outbox MVP 2026-07-04)
                #
                # `?source_type=all` opts out of the exclusion entirely (2026-08-13). Without it
                # there was NO way to retrieve every row: this predicate fired on any request that
                # merely omitted source_type, including an explicit `?status=done`, which returned
                # 404 of the 542 done rows and reported 404 in X-Total-Count. The exclusion is
                # intended and stays the default; being unable to see past it was not.
                conditions.append("source_type IS DISTINCT FROM 'outbound-comm'")

            # Opt-in validity filter — only emits SQL when t_now is set.
            # See plan AC1.3 / AC1.3a / AC1.7a.
            t_now_dt = parse_ts(t_now)
            validity_sql, validity_binds = validity_filter_clause(
                t_now=t_now_dt,
                include_out_of_window=include_out_of_window,
                start_col="validity_start",
                end_col="validity_end",
                value_cast="",  # TIMESTAMPTZ — datetime binds cleanly
                next_param_index=len(params) + 1,
            )
            params.extend(validity_binds)

            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            if validity_sql:
                # validity_sql already starts with ' AND '
                where = (where or "WHERE 1=1") + validity_sql

            # Total matching rows (same WHERE, before LIMIT/OFFSET) for the
            # X-Total-Count header. params here holds only the filter binds.
            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM task_registry {where}", *params
            )

            params += [limit, offset]
            query = f"""
                SELECT * FROM task_registry
                {where}
                ORDER BY
                    CASE WHEN due_date IS NOT NULL AND due_date < CURRENT_DATE
                              AND status NOT IN ('done', 'cancelled')
                         THEN 0 ELSE 1 END,
                    due_date ASC NULLS LAST,
                    CASE priority
                        WHEN 'critical' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 4
                        ELSE 5
                    END,
                    id ASC
                LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """
            rows = await conn.fetch(query, *params)

        response.headers["X-Total-Count"] = str(total or 0)
        return [TaskRecord(**_row_to_dict(r)) for r in rows]

    # -----------------------------------------------------------------------
    # PATCH /{task_key} — partial update
    # -----------------------------------------------------------------------

    @router.patch("/{task_key}")
    async def patch_task(task_key: str, req: TaskPatchRequest):
        """Partial update. Auto-sets timestamps based on status transitions.

        Fix 3: Date fields use model_fields_set to distinguish "not provided"
        from "explicitly null" (which clears the date).

        `notes` appends to metadata->'notes'. Unknown keys are ignored but returned
        as `ignored_fields` and logged by name. A non-empty owner/project link that
        does not resolve is left unchanged and returned as `unresolved`.
        """
        extras = sorted((req.model_extra or {}).keys())
        if extras:
            logger.warning("PATCH /tasks/%s: ignored unknown field(s): %s", task_key, ", ".join(extras))
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT * FROM task_registry WHERE task_key = $1", task_key
            )
            if not existing:
                raise HTTPException(status_code=404, detail=f"Task not found: {task_key}")

            old_status = existing["status"]
            new_status = req.status if req.status is not None else old_status

            # Use naive UTC datetimes — asyncpg rejects tz-aware into TIMESTAMP columns
            updates: Dict[str, Any] = {"updated_at": datetime.now(timezone.utc).replace(tzinfo=None)}
            explicitly_set = req.model_fields_set

            if req.title is not None:
                updates["title"] = req.title
            if req.status is not None:
                updates["status"] = req.status
            if req.priority is not None:
                updates["priority"] = req.priority
            if req.context is not None:
                updates["context"] = req.context
            if req.effort is not None:
                updates["effort"] = req.effort
            if req.tags is not None:
                updates["tags"] = req.tags
            if req.blockedBy is not None:
                updates["blocked_by"] = req.blockedBy

            # Fix 3: date fields — explicitly-null clears; valid string sets; absent skips
            for field, col in (("dueDate", "due_date"), ("startDate", "start_date"), ("waitUntil", "wait_until")):
                if field in explicitly_set:
                    raw = getattr(req, field)
                    if raw is None:
                        updates[col] = None          # explicit clear
                    else:
                        parsed = _parse_date(raw)
                        if parsed is not None:
                            updates[col] = parsed    # valid date
                        # else: malformed string — leave unchanged (no update)

            # Same explicit-null pattern for the new TIMESTAMPTZ validity columns.
            for field, col in (("validityStart", "validity_start"), ("validityEnd", "validity_end")):
                if field in explicitly_set:
                    raw = getattr(req, field)
                    if raw is None:
                        updates[col] = None          # explicit clear
                    else:
                        parsed = parse_ts(raw)
                        if parsed is not None:
                            updates[col] = parsed    # valid timestamp
                        # else: malformed string — leave unchanged

            # Resolve owner / project / collaborators.
            # A non-empty value that does not resolve leaves the stored link alone and is
            # reported in the response. It used to write NULL, which silently wiped a good
            # owner or project whenever a caller sent a spelling the resolver could not match.
            # An explicit empty string still clears the field.
            unresolved: List[str] = []
            for field, col, etype in (
                ("ownerWikilink", "owner_uri", "Person"),
                ("projectWikilink", "project_uri", "Project"),
            ):
                raw = getattr(req, field)
                if raw is None:
                    continue
                if not raw.strip():
                    updates[col] = None
                    continue
                uri = await _resolve_entity(conn, raw, etype)
                if uri is not None:
                    updates[col] = uri
                else:
                    unresolved.append(field)
            if req.collaboratorWikilinks is not None:
                uris = []
                for c in req.collaboratorWikilinks:
                    uri = await _resolve_entity(conn, c, "Person")
                    if uri:
                        uris.append(uri)
                updates["collaborator_uris"] = uris

            # Auto-timestamps based on status transition (naive UTC for TIMESTAMP columns)
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            if new_status == "done" and old_status != "done":
                updates["completed_at"] = now
            if new_status == "in-progress" and old_status != "in-progress":
                updates["started_at"] = now
            if old_status == "inbox" and new_status != "inbox":
                updates["triaged_at"] = now

            # Build SET clause
            set_clauses = []
            vals: List[Any] = []
            for col, val in updates.items():
                vals.append(val)
                set_clauses.append(f"{col} = ${len(vals)}")

            # Append a note to metadata->'notes' (a jsonb array; created if absent or not an array).
            note_text = (req.notes or "").strip()
            if note_text:
                entry = {"at": now.isoformat() + "Z", "status": new_status, "text": note_text[:MAX_NOTE_CHARS]}
                vals.append(json.dumps([entry]))
                set_clauses.append(
                    "metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{notes}', "
                    "CASE WHEN jsonb_typeof(metadata->'notes') = 'array' THEN metadata->'notes' "
                    f"ELSE '[]'::jsonb END || ${len(vals)}::jsonb)"
                )

            vals.append(task_key)
            await conn.execute(
                f"UPDATE task_registry SET {', '.join(set_clauses)} WHERE task_key = ${len(vals)}",
                *vals
            )

        await emit_domain_event("task", "UPDATE", task_key, {
            "task_key": task_key, "title": updates.get("title", existing["title"]),
            "status": new_status, "priority": updates.get("priority", existing["priority"]),
        })
        result: Dict[str, Any] = {"task_key": task_key, "action": "updated"}
        if unresolved:
            result["unresolved"] = unresolved
        if extras:
            result["ignored_fields"] = extras
        return result

    # -----------------------------------------------------------------------
    # GET /stats — aggregate counts
    # -----------------------------------------------------------------------

    @router.get("/stats", response_model=TaskStatsResponse)
    async def get_stats():
        """Return aggregate task counts."""
        async with pool.acquire() as conn:
            today = date.today()

            by_status_rows = await conn.fetch(
                """
                SELECT status, COUNT(*) AS cnt FROM task_registry
                WHERE source_type IS DISTINCT FROM 'test' AND source_type IS DISTINCT FROM 'outbound-comm'
                GROUP BY status
                """
            )
            by_status = {r["status"]: r["cnt"] for r in by_status_rows}

            total_open = sum(
                by_status.get(s, 0)
                for s in ("inbox", "open", "in-progress", "waiting")
            )
            total_done = by_status.get("done", 0)

            overdue = await conn.fetchval(
                """
                SELECT COUNT(*) FROM task_registry
                WHERE due_date < $1
                  AND status NOT IN ('done', 'cancelled')
                  AND source_type IS DISTINCT FROM 'test' AND source_type IS DISTINCT FROM 'outbound-comm'
                """,
                today
            )
            due_today = await conn.fetchval(
                """
                SELECT COUNT(*) FROM task_registry
                WHERE due_date = $1
                  AND status NOT IN ('done', 'cancelled')
                  AND source_type IS DISTINCT FROM 'test' AND source_type IS DISTINCT FROM 'outbound-comm'
                """,
                today
            )
            due_this_week = await conn.fetchval(
                """
                SELECT COUNT(*) FROM task_registry
                WHERE due_date BETWEEN $1 AND $1 + INTERVAL '7 days'
                  AND status NOT IN ('done', 'cancelled')
                  AND source_type IS DISTINCT FROM 'test' AND source_type IS DISTINCT FROM 'outbound-comm'
                """,
                today
            )

            # What the four aggregates above leave out. Reported rather than hidden:
            # the counts are filtered by design, but a caller reading total_done as a
            # registry total was silently wrong by every outbound-comm row.
            excluded_rows = await conn.fetch(
                """
                SELECT source_type, status, COUNT(*) AS cnt FROM task_registry
                WHERE source_type IN ('test', 'outbound-comm')
                GROUP BY source_type, status
                """
            )
            excluded: Dict[str, Dict[str, int]] = {}
            for r in excluded_rows:
                excluded.setdefault(r["source_type"], {})[r["status"]] = r["cnt"]

        return TaskStatsResponse(
            excluded=excluded,
            total_open=total_open,
            total_done=total_done,
            by_status=by_status,
            overdue=overdue,
            due_today=due_today,
            due_this_week=due_this_week,
        )

    # -----------------------------------------------------------------------
    # GET /{task_key} — fetch a single task by key
    #
    # MUST be declared AFTER /stats: FastAPI matches routes in declaration
    # order, so the literal /stats route has to win before this parametrized
    # route would bind task_key="stats". (Before this route existed, GET on a
    # bare key 405'd because only PATCH /{task_key} matched.)
    # -----------------------------------------------------------------------

    @router.get("/{task_key}", response_model=TaskRecord)
    async def get_task(task_key: str):
        """Return a single task by its taskKey, or 404 if not found."""
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM task_registry WHERE task_key = $1", task_key
            )
        if not row:
            raise HTTPException(status_code=404, detail=f"Task not found: {task_key}")
        return TaskRecord(**_row_to_dict(row))

    return router
