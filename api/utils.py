"""Shared utility helpers for the koi-processor API.

Currently hosts:
  * parse_ts() — public-renamed lift of the private _parse_ts() helper that
    was duplicated under api/domain_event_handlers.py:_parse_ts. Single
    source of truth for "ISO 8601 string -> aware datetime, returning None
    on failure" parsing across routers and federation handlers.
  * validity_filter_clause() — opt-in [t_start, t_end] freshness gate used
    by task_router, commitment_router, and intent_router list endpoints.
    Centralizes the symmetric two-sided predicate so the three call sites
    share one implementation.

Both helpers are kw-only on their downstream-facing parameters to make
call sites self-documenting and to prevent positional-argument drift as
new params are added (see plan §"Column-naming asymmetry across the three
tables" — three call sites, three column-name pairs, one helper).
"""

from datetime import datetime
from typing import Any, List, Optional, Tuple


def parse_ts(val: Any) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp string (or pass through a datetime).

    Accepts trailing 'Z' as UTC. Naive ISO strings without timezone are
    treated as UTC by callers that bind into TIMESTAMPTZ columns; this
    function does NOT inject tzinfo. Returns None on parse failure or
    None input (silent — same semantic as the original
    domain_event_handlers._parse_ts).
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def validity_filter_clause(
    *,
    t_now: Optional[Any],
    include_out_of_window: bool,
    start_col: str,
    end_col: str,
    value_cast: str = "",
    next_param_index: int,
) -> Tuple[str, List[Any]]:
    """Return (' AND <predicate>', [bound_values]) for an opt-in validity filter.

    Opt-in semantics:
      * If t_now is None  -> returns ('', []). No filter, no extra binds.
      * If include_out_of_window=True -> returns ('', []). Caller asked for
        explicit pass-through (the same byte-identical SQL as no-t_now).
      * Otherwise -> returns one AND-prefixed clause that keeps rows where:
            (start_col IS NULL OR start_col <= $N::cast)
          AND (end_col   IS NULL OR end_col   >= $N::cast)
        Both bounds reuse the single bound parameter $N. Rows with NULL
        bounds are always visible (matches the plan's "NULL bounds = -∞/+∞"
        semantic).

    Parameters
    ----------
    t_now : Optional[Any]
        The "now" anchor. If None, no filter is emitted. asyncpg accepts a
        Python datetime / date here; ISO-string callers should pre-parse
        via parse_ts() (TIMESTAMPTZ columns) or date.fromisoformat() (DATE
        columns) before passing in.
    include_out_of_window : bool
        Escape hatch — when True, the filter is short-circuited regardless
        of t_now. Mirrors the asymmetric flag from roadmap Phase 1.3.
    start_col, end_col : str
        Column names. The three call sites pass three distinct pairs:
          * task_registry      validity_start / validity_end (TIMESTAMPTZ)
          * commitments        c.validity_start / c.validity_end (TIMESTAMPTZ)
          * intent_registry    valid_from / expires_at (DATE)
    value_cast : str
        Optional ::cast suffix appended to the bind site. Empty string for
        TIMESTAMPTZ columns (asyncpg binds Python datetime cleanly).
        '::date' for DATE columns so a Python datetime/date binds correctly.
    next_param_index : int
        The asyncpg positional index ($N) the caller's WHERE-builder is
        about to consume. The helper appends ONE bind value (t_now) and
        returns the SQL fragment using $next_param_index.

    Returns
    -------
    (sql_fragment, bound_values)
        sql_fragment is either '' (no filter) or
        ' AND ((<start_col> IS NULL OR <start_col> <= $N<cast>)
                AND (<end_col>   IS NULL OR <end_col>   >= $N<cast>))'
        with leading ' AND ' so callers can append it after their existing
        WHERE-builder output.
        bound_values is either [] or [t_now]. Always exactly one bind.
    """
    if t_now is None or include_out_of_window:
        return "", []

    cast = value_cast or ""
    bind = f"${next_param_index}{cast}"
    fragment = (
        f" AND (({start_col} IS NULL OR {start_col} <= {bind})"
        f" AND ({end_col}   IS NULL OR {end_col}   >= {bind}))"
    )
    return fragment, [t_now]
