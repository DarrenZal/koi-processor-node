"""The optional TerminusDB schema module: present, the outbox works as before; absent, it
is skipped with one warning instead of an ImportError inside the vault write.

Offline. terminusdb_client is replaced by minimal stand-ins so the real
scripts/terminusdb/schema.py imports without the package.
"""
import asyncio
import importlib
import importlib.util
import logging
import sys
import types

import pytest

ROW = {"subject_uri": "orn:entity:a", "predicate": "knows", "object_uri": "orn:entity:b",
       "confidence": 0.9, "source": "personal-vault", "source_rid": "People/A.md",
       "source_field": "knows", "raw_value": "[[B]]"}


class FakeConn:
    def __init__(self, rows):
        self.rows, self.executed = rows, []

    async def fetch(self, sql, *args):
        return self.rows

    async def execute(self, sql, *args):
        self.executed.append(args[0] if args else sql)
        return "INSERT 0 1"


@pytest.fixture
def api(monkeypatch):
    import api.personal_ingest_api as mod
    monkeypatch.setattr(mod, "TERMINUSDB_ENABLED", True)
    monkeypatch.setattr(mod, "_terminusdb_schema_warned", False, raising=False)
    return mod


@pytest.fixture
def schema_present(monkeypatch):
    client = types.ModuleType("terminusdb_client")
    client.WOQLClient = type("WOQLClient", (), {})
    woql = types.ModuleType("terminusdb_client.woqlschema")
    woql.DocumentTemplate = type("DocumentTemplate", (), {})
    woql.LexicalKey = lambda *a, **k: None
    woql.Schema = type("Schema", (), {})
    monkeypatch.setitem(sys.modules, "terminusdb_client", client)
    monkeypatch.setitem(sys.modules, "terminusdb_client.woqlschema", woql)
    monkeypatch.delitem(sys.modules, "scripts.terminusdb.schema", raising=False)
    try:
        return importlib.import_module("scripts.terminusdb.schema")
    except ImportError:
        # The public export does not ship scripts/terminusdb; there only the absent cases run.
        if importlib.util.find_spec("scripts.terminusdb") is None:
            pytest.skip("scripts/terminusdb is not in this tree")
        raise


@pytest.fixture
def schema_absent(monkeypatch):
    monkeypatch.setitem(sys.modules, "scripts.terminusdb.schema", None)  # import raises ImportError


def test_present_payload_matches_the_schema_functions(api, schema_present):
    payload = api._build_assertion_payload(dict(ROW))
    expected = schema_present.compute_assertion_hash(
        subject_uri=ROW["subject_uri"], predicate=ROW["predicate"], object_kind="entity",
        object_uri=ROW["object_uri"], literal_value="", literal_datatype="", literal_lang="",
        source=ROW["source"], source_rid=ROW["source_rid"], source_field=ROW["source_field"],
        asserted_by=payload["asserted_by"])
    assert payload["assertion_hash"] == expected
    assert payload["normalized_object_key"] == schema_present.serialize_object_key(
        schema_present.canonical_object_key({"object_kind": "entity", "object_uri": ROW["object_uri"],
                                             "literal_value": "", "literal_datatype": "",
                                             "literal_lang": ""}))


def test_present_outbox_enqueues_retract_and_upserts(api, schema_present):
    conn = FakeConn([ROW, {**ROW, "object_uri": "orn:entity:c"}])
    asyncio.run(api._enqueue_relationship_outbox(conn, "orn:entity:a", "People/A.md"))
    assert conn.executed == ["assertion_retract", "assertion_upsert", "assertion_upsert"]


def test_absent_outbox_enqueues_nothing_and_warns_once(api, schema_absent, caplog):
    conn = FakeConn([ROW])
    with caplog.at_level(logging.WARNING, logger=api.logger.name):
        for _ in range(3):
            asyncio.run(api._enqueue_relationship_outbox(conn, "orn:entity:a", "People/A.md"))
    assert conn.executed == []
    warnings = [r for r in caplog.records if "scripts.terminusdb.schema" in r.getMessage()]
    assert len(warnings) == 1


def test_absent_build_payload_still_raises(api, schema_absent):
    with pytest.raises(ImportError):
        api._build_assertion_payload(dict(ROW))
