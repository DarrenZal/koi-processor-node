"""Dynamic read-only query endpoint for the personal knowledge graph.

Allows Claude (via MCP) to execute arbitrary SELECT queries against a
whitelisted subset of tables.  Defence-in-depth: client-side pre-validation
in the MCP handler, plus server-side guardrails here.

Routes are prefix-relative — prefix "/sql" is applied at mount time.
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Optional AST layer (defense in depth; the read-only role is the real boundary).
# sqlglot is not yet a dependency — when absent, this layer is inactive and the
# regex guards plus the restricted role still apply.
try:  # pragma: no cover - import guard
    import sqlglot
    from sqlglot import exp as _sqlglot_exp
    _HAVE_SQLGLOT = True
except Exception:  # pragma: no cover
    _HAVE_SQLGLOT = False
    logging.getLogger(__name__).info(
        "query_router: sqlglot not installed; AST validation inactive, "
        "relying on regex guards and the least-privilege role")

_PLACEHOLDER = re.compile(r"\$(\d+)")

# Functions a read query must never call, even though they parse as SELECTs.
_DENYLISTED_FUNCS = ("query_to_xml", "query_to_xmlschema", "query_to_xml_and_xmlschema",
                     "pg_read_file", "pg_read_binary_file", "pg_ls_dir", "pg_stat_file",
                     "lo_import", "lo_export", "dblink")


def _ast_reject_reason(sql: str):
    """Return a rejection string if the AST is unsafe, else None. No-op without sqlglot."""
    if not _HAVE_SQLGLOT:
        return None
    # sqlglot 30.x mis-tokenizes adjacent placeholders ("IN ($1,$2)" raises a
    # TokenError), which would reject legitimate parameterized queries that the
    # regex guards pass. Padding each $n with spaces parses identically. The
    # padded text is used for analysis only; the caller's SQL is what executes.
    parse_text = _PLACEHOLDER.sub(r" $\1 ", sql)
    try:
        statements = sqlglot.parse(parse_text, read="postgres")
    except Exception as e:  # conservative: unparseable -> reject
        return f"unparseable SQL ({type(e).__name__})"
    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        return "exactly one statement required"
    root = statements[0]
    if not isinstance(root, (_sqlglot_exp.Select, _sqlglot_exp.With, _sqlglot_exp.Union,
                             _sqlglot_exp.Subquery)):
        return f"root is {type(root).__name__}, only SELECT/WITH allowed"
    for node in root.walk():
        n = node[0] if isinstance(node, tuple) else node
        if isinstance(n, (_sqlglot_exp.Insert, _sqlglot_exp.Update, _sqlglot_exp.Delete,
                          _sqlglot_exp.Drop, _sqlglot_exp.Alter, _sqlglot_exp.Create,
                          _sqlglot_exp.Command, _sqlglot_exp.Copy)):
            return f"disallowed statement node {type(n).__name__}"
        if isinstance(n, _sqlglot_exp.Table):
            name = (n.name or "").lower()
            if name and name not in ALLOWED_TABLES and name != "_q":
                return f"table not allowed: {name}"
        if isinstance(n, (_sqlglot_exp.Anonymous, _sqlglot_exp.Func)):
            fname = (getattr(n, "name", "") or "").lower()
            if any(fname == d or fname.startswith(d) for d in _DENYLISTED_FUNCS):
                return f"disallowed function: {fname}"
    return None

# ---------------------------------------------------------------------------
# Table whitelist — only these tables can appear after FROM/JOIN/INTO
# ---------------------------------------------------------------------------
ALLOWED_TABLES = frozenset({
    # Core data
    "entity_registry",
    "entity_relationships",
    "task_registry",
    # Derived knowledge graph. Omitted until 2026-08-26, which meant the /sql
    # escape hatch could find an entity but never its facts — 59,343 rows that
    # every other surface (search_facts, /knowledge/facts/search) already
    # served. Read-only exposure here is the same data, joinable.
    "knowledge_facts",
    "knowledge_episodes",
    "document_entity_links",
    "claims",
    "claim_attestations",
    # Content
    "koi_memories",
    "koi_memory_chunks",
    "commitments",
    "commitment_pools",
    # External
    "email_metadata",
    # MediaWiki (p2pfoundation wiki live-sync + any future wikis)
    "mediawiki_wikis",
    "mediawiki_page_state",
    "mediawiki_page_links",
    "mediawiki_import_runs",
    # MediaWiki views
    "v_mediawiki_page_resolved",
})

# Dangerous SQL keywords (word-boundary matched, case-insensitive)
_DANGEROUS_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|COPY)\b",
    re.IGNORECASE,
)

# Catalog / information_schema access, and server-side file/large-object
# functions.
#
# The trailing \b was a bug (found 2026-08-26): \bpg_\b requires a non-word
# character after the underscore, so it matched only a bare `pg_` token and
# NOT `pg_class`, `pg_read_file`, or `pg_ls_dir`. Table position was still
# covered by the allowlist guard, but a bare function call has no FROM clause
# to catch — `SELECT pg_read_file($1)` passed every guard. Matching `pg_` as a
# PREFIX closes that. Verified no allowlisted table has a pg_-prefixed column,
# so this costs no legitimate query.
#
# This is a blocklist and blocklists leak. The durable fix is to stop
# connecting as a superuser; see the read-only-role task.
_CATALOG_ACCESS = re.compile(
    r"\b(pg_|information_schema\b|lo_import\b|lo_export\b|dblink\b)",
    re.IGNORECASE,
)

# Extract table identifiers after FROM / JOIN / INTO
_TABLE_REF = re.compile(
    r"\b(?:FROM|JOIN|INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)

# Single-quoted string literals (not dollar-quotes)
_INLINE_LITERAL = re.compile(r"'[^']*'")

# Strip leading SQL line comments
_LEADING_COMMENTS = re.compile(r"^(\s*--[^\n]*\n)*", re.MULTILINE)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    sql: str = Field(..., description="SELECT or WITH query with $1,$2 param placeholders")
    params: List[Any] = Field(default_factory=list, description="Parameter values")
    limit: int = Field(default=200, ge=1, le=1000, description="Max rows returned")


class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    truncated: bool


# ---------------------------------------------------------------------------
# Read-pool privilege check
# ---------------------------------------------------------------------------

# True when the pool's login role could read beyond its grants. A KOI_SQL_RO_DSN
# copied from the service's own DSN would otherwise reopen the bypass silently.
_PRIVILEGED_ROLE_SQL = (
    "SELECT r.rolsuper OR r.rolbypassrls OR r.rolcreaterole OR r.rolcreatedb"
    " OR pg_has_role(current_user, 'pg_read_all_data', 'MEMBER')"
    " OR pg_has_role(current_user, 'pg_read_server_files', 'MEMBER')"
    " OR pg_has_role(current_user, 'pg_execute_server_program', 'MEMBER')"
    " FROM pg_roles r WHERE r.rolname = current_user"
)


async def read_pool_is_least_privilege(pool) -> bool:
    """True only when the pool's role is verifiably unprivileged. Any error,
    or a role that cannot be looked up, counts as privileged."""
    try:
        async with pool.acquire() as conn:
            privileged = await conn.fetchval(_PRIVILEGED_ROLE_SQL)
    except Exception as e:
        logger.warning("/sql read-pool privilege check failed: %s", type(e).__name__)
        return False
    return privileged is False


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_router(pool, caps=None, *, log_pool=None) -> APIRouter:
    """Return an APIRouter for the dynamic query endpoint.

    Parameters
    ----------
    pool : asyncpg.Pool | None
        The connection pool that EXECUTES the caller's query. For the /sql hardening
        this must be the least-privilege ``koi_sql_ro`` pool, never the
        service's superuser pool. **If it is None, every ``/sql`` call returns
        503 and nothing runs — the route fails closed and never falls back to a
        privileged connection** (a silent fallback would re-open the bypass).
    caps : Capabilities (optional)
        Runtime capabilities object.
    log_pool : asyncpg.Pool | None
        A writable pool used only for the best-effort ``koi_query_log`` insert.
        The read-only role cannot write, so logging uses this separate pool when
        provided and is skipped otherwise. It is also the pool the read-auth
        dependency uses to validate session tokens.

    Read auth: every call passes the tranche-1 ``sql``
    dependency, which accepts the personal scope only. A field or public token
    gets 403, so no restricted scope can escape through arbitrary SQL. It is
    always enforced (no credential -> 401) even while the other routes are in
    log mode: the early token gate D7 asked for.
    """
    from api.read_auth import make_read_scope_dependency

    router = APIRouter(tags=["query"])
    # Only a positive result is cached; a privileged or failed check is re-run
    # on the next call, so /sql stays 503 until the pool is genuinely safe.
    _pool_verified = {"ok": False}
    _sql_scope = make_read_scope_dependency("sql", lambda: log_pool, personal_only=True,
                                            always_enforce=True)

    @router.post("", response_model=QueryResponse, dependencies=[Depends(_sql_scope)])
    async def execute_query(req: QueryRequest):
        # --- Guard 0: fail closed. No read role -> no /sql, no privileged fallback. ---
        if pool is None:
            raise HTTPException(
                503,
                "/sql is unavailable: the least-privilege read role is not "
                "configured. It does not fall back to a privileged connection.",
            )
        sql = req.sql

        # --- Guard 1: Single-statement enforcement ---
        sql = sql.rstrip().rstrip(";").rstrip()
        if ";" in sql:
            raise HTTPException(400, "Only single statements allowed")

        # --- Guard 2: Must start with SELECT or WITH ---
        cleaned = _LEADING_COMMENTS.sub("", sql).strip()
        if not re.match(r"^(SELECT|WITH)\s", cleaned, re.IGNORECASE):
            raise HTTPException(400, "Only SELECT or WITH queries allowed")

        # --- Guard 3: Table whitelist ---
        referenced_tables = {m.group(1).lower() for m in _TABLE_REF.finditer(sql)}
        # Also allow subquery aliases (single-letter or _q patterns) — they aren't real tables
        disallowed = referenced_tables - ALLOWED_TABLES - {"_q"}
        if disallowed:
            raise HTTPException(
                400,
                f"Table(s) not allowed: {', '.join(sorted(disallowed))}. "
                f"Allowed: {', '.join(sorted(ALLOWED_TABLES))}",
            )

        # --- Guard 4: Reject dangerous keywords ---
        if _DANGEROUS_KEYWORDS.search(sql):
            raise HTTPException(400, "Query contains disallowed keyword")

        # --- Guard 5: Reject catalog access ---
        if _CATALOG_ACCESS.search(sql):
            raise HTTPException(400, "Catalog/system table access not allowed")

        # --- Guard 6: Parameterization enforcement ---
        if _INLINE_LITERAL.search(sql):
            raise HTTPException(
                400,
                "Use $1, $2 parameters instead of inline string literals",
            )

        # --- Guard 7: AST validation (defense in depth; inactive without sqlglot) ---
        ast_reason = _ast_reject_reason(sql)
        if ast_reason:
            raise HTTPException(400, f"Query rejected by AST validation: {ast_reason}")

        # --- Guard 8: the read pool must be an unprivileged role ---
        if not _pool_verified["ok"]:
            _pool_verified["ok"] = await read_pool_is_least_privilege(pool)
            if not _pool_verified["ok"]:
                raise HTTPException(
                    503,
                    "/sql is unavailable: its read pool logs in as a privileged "
                    "role. It does not run caller SQL with elevated privileges.",
                )

        # --- Execute with safety constraints ---
        limit = min(req.limit, 1000)
        wrapped_sql = f"SELECT * FROM ({sql}) AS _q LIMIT {limit}"

        start_ms = time.monotonic()
        try:
            async with pool.acquire() as conn:
                async with conn.transaction(readonly=True):
                    await conn.execute("SET LOCAL statement_timeout = '5s'")
                    stmt = await conn.prepare(wrapped_sql)
                    records = await stmt.fetch(*req.params)
                    columns = [attr.name for attr in stmt.get_attributes()]
        except Exception as e:
            error_msg = str(e)
            logger.warning("Query execution failed: %s | SQL: %s", error_msg, sql[:200])
            raise HTTPException(400, f"Query execution error: {error_msg}")

        elapsed_ms = (time.monotonic() - start_ms) * 1000

        # Convert records to lists
        rows = [list(r.values()) for r in records]
        row_count = len(rows)

        # --- Guard 9: Max result size (100KB) ---
        truncated = False
        result = QueryResponse(
            columns=columns,
            rows=rows,
            row_count=row_count,
            truncated=False,
        )
        serialized = result.model_dump_json()
        if len(serialized) > 100_000:
            # Trim rows until under 100KB. The model holds its own copy of the list,
            # so trim that one: popping `rows` would never shrink what is measured.
            while result.rows and len(result.model_dump_json()) > 100_000:
                result.rows.pop()
            truncated = True
            result.row_count = len(result.rows)
            result.truncated = True

        # --- Query logging (zero PII — only SQL template, not params) ---
        # Logging needs a WRITABLE pool; the read-only query pool cannot insert.
        # Skip quietly when no writable pool was provided.
        extracted_tables = sorted(referenced_tables & ALLOWED_TABLES)
        if log_pool is None:
            return result
        try:
            async with log_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO koi_query_log
                       (query_text, agent_id, confidence_score,
                        total_results, execution_time_ms, metadata)
                       VALUES ($1, $2, $3, $4, $5, $6)""",
                    sql,
                    "dobby",
                    1.0,
                    row_count,
                    elapsed_ms,
                    json.dumps({
                        "source_type": "mcp",
                        "params_count": len(req.params),
                        "tables_accessed": extracted_tables,
                    }),
                )
        except Exception as e:
            logger.warning("Query logging failed (non-fatal): %s", e)

        return result

    return router
