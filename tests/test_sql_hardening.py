"""Tests for the /sql hardening (D7).

No database needed. Covers: fail-closed when the read pool is absent, the AST
layer's inactive default, the denylist, and a regression-documenting test that
the regex guards alone do NOT catch the all-parameter query_to_xml form (which
is why the least-privilege role and the AST layer exist).

Run: venvs/koi-server/bin/python -m pytest tests/test_sql_hardening.py -q
"""
import asyncio
import re
import pytest
from fastapi import HTTPException

import api.routers.query_router as qr


def _call(router, sql, params=None):
    ep = [rt.endpoint for rt in router.routes if getattr(rt, "path", "") == ""][0]
    req = qr.QueryRequest(sql=sql, params=params or [])
    return asyncio.new_event_loop().run_until_complete(ep(req))


def test_fail_closed_without_read_pool():
    router = qr.create_router(None, None, log_pool=None)
    with pytest.raises(HTTPException) as ei:
        _call(router, "SELECT 1")
    assert ei.value.status_code == 503
    assert "fall back" in ei.value.detail.lower()


def test_ast_layer_inactive_default_returns_none():
    if qr._HAVE_SQLGLOT:
        pytest.skip("sqlglot installed; inactive-default case not applicable")
    # Without sqlglot the AST helper is a no-op for any input.
    assert qr._ast_reject_reason("SELECT query_to_xml($1,$2,$3,$4)") is None


def test_denylist_covers_query_to_xml_and_file_functions():
    for f in ("query_to_xml", "query_to_xmlschema", "query_to_xml_and_xmlschema",
              "pg_read_file", "lo_import", "dblink"):
        assert f in qr._DENYLISTED_FUNCS


# --- Regression: the regex guards alone miss the all-parameter query_to_xml form.
# This is the verified bypass from audit file 09; it is why /sql needs the
# least-privilege role (so the inner query cannot run as superuser) and the AST
# layer (so the call is rejected outright). If a future guard change starts
# catching it at the regex layer, that is a welcome improvement, not a failure —
# update this test then.

def _passes_regex_guards(sql: str) -> bool:
    s = sql.rstrip().rstrip(";").rstrip()
    if ";" in s:
        return False
    cleaned = qr._LEADING_COMMENTS.sub("", s).strip()
    if not re.match(r"^(SELECT|WITH)\s", cleaned, re.I):
        return False
    refs = {m.group(1).lower() for m in qr._TABLE_REF.finditer(s)}
    if refs - qr.ALLOWED_TABLES - {"_q"}:
        return False
    if qr._DANGEROUS_KEYWORDS.search(s):
        return False
    if qr._CATALOG_ACCESS.search(s):
        return False
    if qr._INLINE_LITERAL.search(s):
        return False
    return True


def test_regex_guards_miss_all_param_query_to_xml():
    # The bound-parameter form slips past every regex guard.
    assert _passes_regex_guards("SELECT query_to_xml($1,$2,$3,$4)") is True


def test_regex_guards_catch_inline_literal_form():
    # The inline form is caught only incidentally, by the no-literal guard.
    assert _passes_regex_guards("SELECT query_to_xml('SELECT 1',false,false,'')") is False


def test_regex_guards_block_direct_catalog_and_allow_allowlisted():
    assert _passes_regex_guards("SELECT * FROM pg_authid") is False
    assert _passes_regex_guards("SELECT * FROM knowledge_facts") is True


# --- AST layer with sqlglot active (requirements pin sqlglot==30.20.0). -------
# Skipped, with that reason, when sqlglot is not importable; the offline
# rehearsal runs this module with the pinned wheel on PYTHONPATH.

_needs_sqlglot = pytest.mark.skipif(not qr._HAVE_SQLGLOT, reason="sqlglot not installed")


@_needs_sqlglot
@pytest.mark.parametrize("sql", [
    "SELECT entity_text FROM entity_registry WHERE entity_type = $1",
    "SELECT entity_text FROM entity_registry WHERE entity_type IN ($1,$2)",   # adjacent params
    "SELECT $1,$2",
    "SELECT e.entity_text FROM entity_registry e JOIN document_entity_links d"
    " ON d.entity_uri = e.fuseki_uri",
    "SELECT rid FROM koi_memories UNION SELECT document_rid FROM koi_memory_chunks",
    "SELECT count(*) FROM entity_registry",
])
def test_ast_accepts_allowlisted_reads(sql):
    assert qr._ast_reject_reason(sql) is None


@_needs_sqlglot
@pytest.mark.parametrize("sql,why", [
    ("SELECT query_to_xml($1,$2,$3,$4)", "query_to_xml"),
    ("SELECT query_to_xml($1, $2, $3, $4)", "query_to_xml"),
    ("SELECT QUERY_TO_XMLSCHEMA($1,$2,$3,$4)", "query_to_xml"),
    ("SELECT pg_read_file($1)", "pg_read_file"),
    ("SELECT lo_export($1,$2)", "lo_export"),
    ("SELECT * FROM dblink($1,$2) AS t(x text)", "dblink"),
    ("SELECT * FROM session_tokens", "session_tokens"),
    ("SELECT (SELECT count(*) FROM session_tokens) AS n", "session_tokens"),
    ("SELECT * FROM pg_catalog.pg_authid", "pg_authid"),
    ("INSERT INTO entity_registry(entity_text) VALUES ($1)", "Insert"),
    ("SELECT 1; SELECT 2", "one statement"),
])
def test_ast_rejects_unsafe_reads(sql, why):
    reason = qr._ast_reject_reason(sql)
    assert reason is not None and why.lower() in reason.lower(), reason


@_needs_sqlglot
def test_ast_layer_catches_the_param_form_the_regex_guards_miss():
    router = qr.create_router(object(), None, log_pool=None)   # any non-None pool
    with pytest.raises(HTTPException) as ei:
        _call(router, "SELECT query_to_xml($1,$2,$3,$4)", ["select 1", True, False, ""])
    assert ei.value.status_code == 400 and "AST" in ei.value.detail
