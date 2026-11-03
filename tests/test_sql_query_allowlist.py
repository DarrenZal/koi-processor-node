"""
Unit tests for the /sql table allowlist (api/routers/query_router.py).

The allowlist had no test at all until 2026-08-26, which is how the derived
knowledge graph — knowledge_facts (59k rows), knowledge_episodes,
document_entity_links — stayed unreachable from the /sql escape hatch while
every other surface served it. A caller could find an entity and never its
facts.

These tests pin two things:
  1. The knowledge-graph tables are reachable (the regression that motivated
     this file).
  2. The guards that make read-only exposure safe still hold — writes,
     multi-statement, catalog access, and inline literals are all refused.
"""

from __future__ import annotations

import pytest

from api.routers.query_router import (
    ALLOWED_TABLES,
    _CATALOG_ACCESS,
    _DANGEROUS_KEYWORDS,
    _INLINE_LITERAL,
    _TABLE_REF,
)


# --------------------------------------------------------------------------
# 1. The regression: the derived knowledge graph must be queryable.
# --------------------------------------------------------------------------

KNOWLEDGE_GRAPH_TABLES = (
    "knowledge_facts",
    "knowledge_episodes",
    "document_entity_links",
)


@pytest.mark.parametrize("table", KNOWLEDGE_GRAPH_TABLES)
def test_knowledge_graph_tables_are_allowlisted(table):
    """A fact is reachable from /sql, not only from search_facts."""
    assert table in ALLOWED_TABLES


def test_entity_to_fact_join_passes_the_table_guard():
    """The join that answers 'what do we know about X' is not rejected."""
    sql = (
        "SELECT f.predicate, f.object_literal "
        "FROM knowledge_facts f "
        "JOIN entity_registry e ON e.fuseki_uri = f.subject_uri "
        "WHERE e.normalized_text = $1"
    )
    referenced = {m.group(1).lower() for m in _TABLE_REF.finditer(sql)}
    assert referenced - ALLOWED_TABLES - {"_q"} == set()


def test_unknown_table_is_still_rejected():
    """Widening the allowlist did not turn it into an allow-anything."""
    sql = "SELECT * FROM pg_shadow_copy"
    referenced = {m.group(1).lower() for m in _TABLE_REF.finditer(sql)}
    assert referenced - ALLOWED_TABLES - {"_q"} == {"pg_shadow_copy"}


def test_allowlist_is_immutable():
    """frozenset, so a handler cannot widen it at runtime."""
    assert isinstance(ALLOWED_TABLES, frozenset)


# --------------------------------------------------------------------------
# 2. The guards that make this safe are unchanged.
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO knowledge_facts VALUES ($1)",
        "UPDATE knowledge_facts SET predicate = $1",
        "DELETE FROM knowledge_facts",
        "DROP TABLE knowledge_facts",
        "ALTER TABLE knowledge_facts ADD COLUMN x INT",
        "TRUNCATE knowledge_facts",
        "COPY knowledge_facts TO $1",
        "GRANT ALL ON knowledge_facts TO public",
    ],
)
def test_writes_to_the_newly_allowed_tables_are_refused(sql):
    """Allowlisting a table grants READ. It must not grant write."""
    assert _DANGEROUS_KEYWORDS.search(sql) is not None


def test_catalog_access_still_refused():
    assert _CATALOG_ACCESS.search("SELECT * FROM information_schema.columns")
    assert _CATALOG_ACCESS.search("SELECT * FROM pg_class")


@pytest.mark.parametrize(
    "sql",
    [
        # The bypass, found 2026-08-26. `\bpg_\b` matched only a bare `pg_`
        # token, so every real catalog object slipped through. A bare function
        # call has no FROM clause, so the table allowlist could not catch it
        # either — this passed all six guards against a SUPERUSER connection.
        "SELECT pg_read_file($1)",
        "SELECT pg_read_binary_file($1)",
        "SELECT pg_ls_dir($1)",
        "SELECT pg_stat_file($1)",
        "SELECT lo_import($1)",
        "SELECT lo_export($1, $2)",
        "SELECT dblink($1, $2)",
    ],
)
def test_server_side_file_functions_are_refused(sql):
    """A read-only endpoint must not be able to read the server's disk."""
    assert _CATALOG_ACCESS.search(sql) is not None, f"bypass: {sql}"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM knowledge_facts",
        "SELECT lower(fact_text) FROM knowledge_facts WHERE group_id = $1",
        "SELECT now()",
        "SELECT e.normalized_text FROM entity_registry e JOIN knowledge_facts f"
        " ON f.subject_uri = e.fuseki_uri",
    ],
)
def test_legitimate_queries_are_not_caught_by_the_catalog_guard(sql):
    """Broadening pg_ to a prefix must not cost real queries. Verified against
    the live schema: no allowlisted table has a pg_-prefixed column."""
    assert _CATALOG_ACCESS.search(sql) is None, f"false positive: {sql}"


def test_inline_literals_still_refused():
    """Parameterization is enforced, so the new tables cannot be probed by
    string concatenation."""
    assert _INLINE_LITERAL.search("SELECT * FROM knowledge_facts WHERE group_id = 'spore'")
    assert _INLINE_LITERAL.search("SELECT * FROM knowledge_facts WHERE group_id = $1") is None
