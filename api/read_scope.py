"""Caller-derived read scope and per-surface visibility predicates.

WHAT THIS IS
------------
A ``ReadScope`` is derived server-side from an authenticated credential and says
what a caller may read. Request parameters may only narrow it, never widen it.
The SQL builders below turn a scope into a WHERE predicate for each retrieval
surface, so visibility is applied *inside* the ranked query, before limiting,
enrichment and counting.

THE THREE FIXES (agreed with the discourse-graph session, 2026-10-29)
---------------------------------------------------------------------
1. Sessions have ``source_rid = NULL``. The generic source predicate would drop
   every session row under SQL NULL semantics, even for the operator. Moves get
   an explicit session branch: personal sees non-excluded sessions, non-personal
   is denied (there is no session grant in tranche 1).
2. Missing privacy metadata must never read as public. ``is_private`` defaults
   to false (migration 015), so "never set" is stored as false with
   ``access_source`` NULL; 35,933 live rows looked like that on 2026-10-29,
   including the mislabeled derived email. For a non-personal scope a source is
   visible only when its ``koi_memories`` row says ``is_private = false`` AND
   carries an affirmative ``access_source`` label (not NULL, not the
   ``default-public`` backfill marker). No row, NULL, or an unlabeled false
   fails closed. Which sources get labeled is a data decision made before any
   field or public token is issued.
3. Withheld sources (e.g. the mislabeled derived email) are a hard deny for
   every non-personal scope, evaluated before any grant, so an explicit source
   grant cannot bypass them. The personal scope still sees them.

PERSONAL REDUCES TO TODAY
-------------------------
When ``scope.view == 'personal'`` every predicate collapses to ``TRUE`` (for the
source surfaces) or to the route's existing filter (entities already exclude
``node_private``; that is preserved, not widened). So a personal-token request
returns exactly what the route returns today. The new restriction is added for
field and public scopes only.

This module builds SQL and maps a raw token to a scope. It never opens a
connection and never enforces authentication; that is ``api.read_auth`` (the
per-route dependency that validates the credential and denies in enforce mode).

CONFIGURATION IS VALIDATED STRICTLY
-----------------------------------
``KOI_READ_SCOPES_FILE`` entries are parsed by ``parse_scope_entry``, which
accepts exact types and values only. Any invalid entry, an unreadable or
malformed file, or a configured path that does not exist raises
``ReadScopeConfigError``; the caller turns that into an explicit 503. Nothing
in the file can ever resolve to the personal scope: personal is reserved for
the operator's env tokens.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import secrets
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from api.auth import read_admin_token

_log = logging.getLogger("koi.readauth")

# Held corpora excluded from discourse/source surfaces in every view, matching
# api.discourse_links.EXCLUDED_SOURCE_PATTERNS. Kept here to avoid importing the
# discourse module into every retrieval path.
EXCLUDED_SOURCE_PATTERNS: Tuple[str, ...] = ("mediawiki:%",)


def _withheld_sources() -> FrozenSet[str]:
    """Source rids hard-denied to every non-personal scope.

    Seeded with the known mislabeled derived email and
    extendable by the ``KOI_WITHHELD_SOURCE_RIDS`` env var (comma-separated).
    This withholds by SOURCE IDENTITY, never by a list of candidate ids, so it
    covers existing and future rows alike.
    """
    seed = {
        "document:2f6c912bb900dcd324473b8a4228118526a3a4104342c14553b9d3bc4de5be3f",
    }
    extra = os.getenv("KOI_WITHHELD_SOURCE_RIDS", "")
    for rid in (x.strip() for x in extra.split(",")):
        if rid:
            seed.add(rid)
    return frozenset(seed)


@dataclass(frozen=True)
class ReadScope:
    """What a caller may read. Derived from the credential, never from the body."""
    view: str                                    # personal | field | public
    fields: FrozenSet[str] = frozenset()         # granted learning fields
    sources: FrozenSet[str] = frozenset()        # explicitly granted source rids
    include_unassigned: bool = False             # sources in no field at all
    include_private: bool = False                # is_private sources (personal only, today)
    principal: Optional[str] = None              # audit identity (email / service name)
    withheld: FrozenSet[str] = field(default_factory=_withheld_sources)

    @property
    def is_personal(self) -> bool:
        return self.view == "personal"

    def describe(self) -> dict:
        return {
            "view": self.view,
            "fields": sorted(self.fields),
            "sources": sorted(self.sources),
            "include_unassigned": self.include_unassigned,
            "include_private": self.include_private,
            "principal": self.principal,
            "withheld_count": len(self.withheld),
        }


def personal_scope(principal: Optional[str] = None) -> ReadScope:
    return ReadScope(view="personal", include_unassigned=True, include_private=True,
                     principal=principal)


def field_scope(fields, *, sources=(), include_unassigned: bool = False,
                principal: Optional[str] = None) -> ReadScope:
    return ReadScope(view="field", fields=frozenset(fields), sources=frozenset(sources),
                     include_unassigned=include_unassigned, include_private=False,
                     principal=principal)


def public_scope(*, sources=(), principal: Optional[str] = None) -> ReadScope:
    return ReadScope(view="public", sources=frozenset(sources), principal=principal)


# ---------------------------------------------------------------------------
# Source-keyed visibility: the heart of chunks, facts and document moves.
# ---------------------------------------------------------------------------

def source_visible_sql(scope: ReadScope, source_col: str, start: int, *,
                       exclude_held: bool = True) -> Tuple[str, List[Any]]:
    """Return ``(sql, params)`` true iff a row whose source rid is ``source_col``
    is visible to ``scope``. ``source_col`` is a trusted SQL identifier supplied
    by the calling module, never request input. Params bind from ``$start``.

    Personal collapses to ``NOT (source LIKE excluded)`` only (i.e. today). Non
    personal adds: withheld hard-deny, fail-closed privacy, and field/grant
    scoping. ``exclude_held=False`` drops the held-corpus patterns, which are a
    discourse-surface rule, not a chunk-visibility rule (the wiki surface reads
    ``mediawiki:%`` chunks by design).
    """
    n = start
    sql = (
        f"(NOT ({source_col} LIKE ANY(${n}::text[]))"
        f" AND (${n + 1}::bool"                                  # personal → true
        f"   OR (NOT ({source_col} = ANY(${n + 2}::text[]))"      # withheld hard-deny
        f"       AND EXISTS (SELECT 1 FROM koi_memories kp"
        f"                   WHERE kp.rid = {source_col} AND kp.is_private = false"
        f"                     AND kp.access_source IS NOT NULL"
        f"                     AND kp.access_source <> 'default-public')"  # affirmatively public
        f"       AND ({source_col} = ANY(${n + 3}::text[])"       # granted source
        f"            OR EXISTS (SELECT 1 FROM document_field_membership fm"
        f"                       WHERE fm.document_rid = {source_col}"
        f"                         AND fm.field_id = ANY(${n + 4}::text[]))"   # granted field
        f"            OR (${n + 5}::bool AND NOT EXISTS (SELECT 1 FROM document_field_membership fu"
        f"                       WHERE fu.document_rid = {source_col}))))))"    # unassigned
    )
    params: List[Any] = [
        list(EXCLUDED_SOURCE_PATTERNS) if exclude_held else [],
        scope.is_personal,
        sorted(scope.withheld),
        sorted(scope.sources),
        sorted(scope.fields),
        scope.include_unassigned,
    ]
    return sql, params


def move_visible_sql(scope: ReadScope, move_alias: str, start: int
                     ) -> Tuple[str, List[Any]]:
    """Visibility for ``session_discourse_moves``. Document moves use the source
    predicate on ``source_rid``; session moves (``source_rid`` NULL) use the
    explicit session policy: personal only, and never an excluded session."""
    src_sql, params = source_visible_sql(scope, f"{move_alias}.source_rid", start)
    n_session = start + len(params)
    sql = (
        f"(CASE WHEN {move_alias}.source_type = 'session'"
        f"  THEN (${n_session}::bool"                             # personal only
        f"        AND NOT EXISTS (SELECT 1 FROM session_exclusions sx"
        f"                        WHERE sx.session_id = {move_alias}.session_id))"
        f"  ELSE {src_sql} END)"
    )
    params = params + [scope.is_personal]
    return sql, params


def chunk_visible_sql(scope: ReadScope, source_col: str, start: int
                      ) -> Tuple[str, List[Any]]:
    """Visibility for a chunk, document or fact row keyed by its source rid,
    for the vector and text routes (``/search`` family, unified-search).

    Personal returns the literal ``TRUE`` with no params, so the ranked query is
    today's query and the planner folds the clause away; the HNSW plan cannot
    change for the operator. Non-personal is the source predicate without the
    held-corpus patterns, placed inside the ranked WHERE so filtering happens
    before ranking and limiting. A NULL source rid fails closed.
    """
    if scope.is_personal:
        return "TRUE", []
    return source_visible_sql(scope, source_col, start, exclude_held=False)


def entity_visible_sql(scope: ReadScope, entity_alias: str, start: int = 1
                       ) -> Tuple[str, List[Any]]:
    """Visibility for ``entity_registry``.

    Every scope excludes ``node_private`` (today's behavior; personal is
    unchanged and gets no params). A non-personal scope additionally requires at
    least one mention in a source it may see, so an entity supported only by
    private or out-of-scope documents (1,568 such entities at the 2026-10-29
    audit) does not surface through a field or public token (AT11)."""
    base = f"(NOT COALESCE({entity_alias}.node_private, false))"
    if scope.is_personal:
        return base, []
    src_sql, params = chunk_visible_sql(scope, "del_v.document_rid", start)
    return (f"({base} AND EXISTS (SELECT 1 FROM document_entity_links del_v"
            f" WHERE del_v.entity_uri = {entity_alias}.fuseki_uri AND {src_sql}))", params)


# ---------------------------------------------------------------------------
# Scope resolution from a credential (server-side; the body never widens it).
# ---------------------------------------------------------------------------

_PERSONAL_TOKEN_ENVS = ("KOI_ADMIN_TOKEN", "KOI_CLAIMS_SERVICE_TOKEN", "KOI_MCP_TOKEN")

# Views a KOI_READ_SCOPES_FILE entry may name. "personal" is deliberately absent:
# personal is reserved for the operator's env tokens, so no file entry, typo or
# default can mint it.
_FILE_VIEWS = ("field", "public")
_ENTRY_KEYS = {
    "field": frozenset({"view", "fields", "sources", "include_unassigned", "principal"}),
    "public": frozenset({"view", "sources", "principal"}),
}
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


class ReadScopeConfigError(Exception):
    """The read-scope configuration is invalid. Callers must deny (503) the
    request it affects; it never degrades to a broader scope."""


def _sha256(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _personal_env_tokens() -> List[Tuple[str, str]]:
    """``(env name, value)`` for each configured operator token. The admin token
    is read the same way the observation middleware reads it (env, else
    ``$KOI_STATE_DIR/admin_token``), so the log's verdicts predict enforcement."""
    out: List[Tuple[str, str]] = []
    for env in _PERSONAL_TOKEN_ENVS:
        if env == "KOI_ADMIN_TOKEN":
            try:
                v = read_admin_token()
            except OSError as e:
                # An unreadable admin_token file disables the admin token only;
                # the other operator tokens keep working. Never a 500, never broader.
                _log.error("admin token unreadable (%s); admin token ignored", type(e).__name__)
                v = None
        else:
            v = os.getenv(env, "")
        if v:
            out.append((env, v))
    return out


def _str_list(entry: dict, key: str, where: str) -> FrozenSet[str]:
    value = entry.get(key, [])
    # A bare string is rejected: frozenset("bio") would grant {"b","i","o"}.
    if not isinstance(value, list) or not all(isinstance(x, str) and x.strip() for x in value):
        raise ReadScopeConfigError(f"{where}: '{key}' must be a list of non-empty strings")
    return frozenset(value)


def parse_scope_entry(key: Any, entry: Any) -> ReadScope:
    """Strictly parse one scopes-file entry into a ReadScope.

    Exact types and values only: ``view`` must be the string ``"field"`` or
    ``"public"``; ``include_unassigned`` must be a JSON boolean; ``fields`` and
    ``sources`` must be lists of strings; unknown keys are rejected. Anything
    else raises ReadScopeConfigError. Errors name the entry by an 8-hex prefix
    of its key hash, never by a token.
    """
    if not isinstance(key, str) or not _SHA256_HEX.match(key):
        raise ReadScopeConfigError("scopes file: every key must be a lowercase sha256 hex digest")
    where = f"scopes entry {key[:8]}"
    if not isinstance(entry, dict):
        raise ReadScopeConfigError(f"{where}: entry must be an object")
    view = entry.get("view")
    if not isinstance(view, str) or view not in _FILE_VIEWS:
        raise ReadScopeConfigError(
            f"{where}: 'view' must be one of {list(_FILE_VIEWS)}, got {view!r}")
    unknown = set(entry) - _ENTRY_KEYS[view]
    if unknown:
        raise ReadScopeConfigError(f"{where}: keys not allowed for view {view!r}: {sorted(unknown)}")
    principal = entry.get("principal")
    if principal is not None and not (isinstance(principal, str) and principal.strip()):
        raise ReadScopeConfigError(f"{where}: 'principal' must be a non-empty string")
    sources = _str_list(entry, "sources", where)
    if view == "public":
        return public_scope(sources=sources, principal=principal)
    include_unassigned = entry.get("include_unassigned", False)
    if not isinstance(include_unassigned, bool):
        # bool("false") is True; only a real JSON boolean is accepted.
        raise ReadScopeConfigError(
            f"{where}: 'include_unassigned' must be a JSON boolean, got {include_unassigned!r}")
    return field_scope(_str_list(entry, "fields", where), sources=sources,
                       include_unassigned=include_unassigned, principal=principal)


def load_scope_map() -> Dict[str, ReadScope]:
    """``{sha256(token): ReadScope}`` from ``KOI_READ_SCOPES_FILE``, validated as
    a whole. Read every call (the file is small) so edits take effect on the next
    request and tests need no cache reset.

    Unset variable: no field/public tokens exist (empty map). Set variable but a
    missing, unreadable or malformed file, any invalid entry, or an entry for an
    operator env token: ReadScopeConfigError. A broken file never resolves a
    token to some other scope, and never to personal.
    """
    path = os.getenv("KOI_READ_SCOPES_FILE", "").strip()
    if not path:
        return {}
    try:
        with open(path) as fh:
            data = json.load(fh)
    except FileNotFoundError:
        raise ReadScopeConfigError("KOI_READ_SCOPES_FILE is set but the file does not exist")
    except (OSError, ValueError) as e:
        raise ReadScopeConfigError(f"KOI_READ_SCOPES_FILE unreadable or not JSON ({type(e).__name__})")
    if not isinstance(data, dict):
        raise ReadScopeConfigError("KOI_READ_SCOPES_FILE must contain a JSON object")
    out = {k: parse_scope_entry(k, v) for k, v in data.items()}
    for env, value in _personal_env_tokens():
        if _sha256(value) in out:
            raise ReadScopeConfigError(
                f"KOI_READ_SCOPES_FILE has an entry for the {env} operator token")
    return out


def scope_from_token_value(token: Optional[str]) -> Optional[ReadScope]:
    """Map a raw token to a ReadScope, or None when no configuration knows it.

    Order: the operator's env tokens (admin/claims/mcp) → personal; then a
    ``KOI_READ_SCOPES_FILE`` entry → its field/public scope. Session tokens are
    not handled here: ``api.read_auth`` validates them against
    ``session_tokens`` and binds scope to the validated principal. Raises
    ReadScopeConfigError when the scopes file is invalid and the token is not
    an operator token.
    """
    if not token:
        return None
    presented = token.encode("utf-8")
    for env, value in _personal_env_tokens():
        # bytes, not str: compare_digest raises TypeError on non-ASCII str, and a
        # header like "Bearer \xe9" must read as unrecognized, not as a 500.
        if secrets.compare_digest(presented, value.encode("utf-8")):
            return personal_scope(principal=f"token:{env}")
    return load_scope_map().get(_sha256(token))
