"""The paid embedding and enrichment backfills are capped dry runs unless --apply (D13).

Covers scripts/reembed_chunks.py, backfill_entity_embeddings.py, b8a_enrich_entities.py
and backfill_contextual_retrieval.py. Offline: asyncpg, the embedding provider and the
LLM calls are fakes, and every socket connect fails the test.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import socket
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FAKE_URL = "postgresql://tester:changeme@127.0.0.1:1/koi_fake"
READ_ONLY = {"default_transaction_read_only": "on"}

ROW = {
    "source_sensor": "doc-scanner", "cnt": 1, "chunk_rid": "chunk:1", "text": "chunk text",
    "title": "T", "entity_type": "Concept", "fuseki_uri": "urn:e:1", "entity_text": "Kelp",
    "description": None, "context": None, "metadata": {}, "doc_links": 2, "excerpt": "x",
    "document_rid": "doc:1", "chunk_index": 0, "chunk_content": json.dumps({"text": "chunk text"}),
    "doc_text": "document text", "doc_title": "D", "ctx": "c",
}


def _load(stem):
    spec = importlib.util.spec_from_file_location(f"d13_{stem}", REPO / "scripts" / f"{stem}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeConn:
    def __init__(self, log):
        self.log = log

    async def fetchval(self, sql, *args):
        self.log.append(" ".join(sql.split()))
        return 1

    async def fetch(self, sql, *args):
        self.log.append(" ".join(sql.split()))
        return [dict(ROW)]

    async def execute(self, sql, *args):
        self.log.append(" ".join(sql.split()))
        return "UPDATE 1"

    def transaction(self):
        return _AsyncNull()

    async def close(self):
        pass


class _AsyncNull:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class FakePool:
    def __init__(self, log):
        self.conn = FakeConn(log)

    def acquire(self):
        conn = self.conn

        class _Acq:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False
        return _Acq()

    async def close(self):
        pass


class FakeProvider:
    model_name = "fake-embed"
    dimension = 2

    def __init__(self, calls):
        self.calls = calls

    async def embed_batch_or_none(self, texts, prompt_type=None):
        self.calls.append(list(texts))
        return [[0.25, 0.5] for _ in texts]


@pytest.fixture
def harness(monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("network call attempted")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)

    async def no_sleep(*_a, **_k):
        return None
    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    state = types.SimpleNamespace(sql=[], pools=[], provider_made=0, embed_calls=[], llm_calls=[])

    async def create_pool(url, **kwargs):
        state.pools.append(kwargs)
        return FakePool(state.sql)

    state.asyncpg = types.SimpleNamespace(create_pool=create_pool)

    def make_provider():
        state.provider_made += 1
        return FakeProvider(state.embed_calls)

    state.make_provider = make_provider
    return state


def _setup(monkeypatch, stem, state):
    mod = _load(stem)
    monkeypatch.setattr(mod, "asyncpg", state.asyncpg)
    monkeypatch.setenv("POSTGRES_URL", FAKE_URL)
    if hasattr(mod, "create_embedding_provider"):
        monkeypatch.setattr(mod, "create_embedding_provider", state.make_provider)
    if stem == "b8a_enrich_entities":
        async def describe(name, etype, excerpts):
            state.llm_calls.append(name)
            return "A kelp forest."
        monkeypatch.setattr(mod, "generate_entity_description", describe)
    if stem == "backfill_contextual_retrieval":
        async def contexts(document_text, chunks, document_title="", concurrency=5):
            state.llm_calls.append(document_title)
            return state.context_reply(chunks)
        state.context_reply = lambda chunks: ["Situating context."] * len(chunks)
        monkeypatch.setattr(mod, "generate_contexts_for_document", contexts)
    return mod


def _run(mod, stem, argv):
    if stem in ("reembed_chunks", "backfill_entity_embeddings"):
        return mod.cli(argv)
    return mod.main(argv)


def _writes(state):
    return [s for s in state.sql if not s.upper().startswith("SELECT")]


STEMS = ["reembed_chunks", "backfill_entity_embeddings", "b8a_enrich_entities",
         "backfill_contextual_retrieval"]
WRITE_TABLE = {
    "reembed_chunks": "UPDATE koi_memory_chunks SET embedding",
    "backfill_entity_embeddings": "UPDATE entity_registry SET embedding_3072",
    "b8a_enrich_entities": "UPDATE entity_registry SET metadata",
    "backfill_contextual_retrieval": "UPDATE koi_memory_chunks SET content",
}


@pytest.mark.parametrize("stem", STEMS)
def test_help_exits_zero_without_side_effects(stem, monkeypatch, harness, capsys):
    mod = _setup(monkeypatch, stem, harness)
    with pytest.raises(SystemExit) as exc:
        _run(mod, stem, ["--help"])
    assert exc.value.code == 0
    assert "--apply" in capsys.readouterr().out
    assert harness.pools == [] and harness.sql == []
    assert harness.provider_made == 0 and harness.llm_calls == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--apply"]])
@pytest.mark.parametrize("stem", STEMS)
def test_default_is_read_only_dry_run(stem, argv, monkeypatch, harness, caplog):
    caplog.set_level("INFO")
    mod = _setup(monkeypatch, stem, harness)
    _run(mod, stem, argv)
    assert harness.pools == [{"min_size": 1, "max_size": harness.pools[0]["max_size"],
                              "server_settings": READ_ONLY}]
    assert _writes(harness) == []
    assert harness.provider_made == 0 and harness.embed_calls == [] and harness.llm_calls == []
    text = caplog.text
    assert "DRY RUN" in text
    assert "changeme" not in text and "tester:***@127.0.0.1:1/koi_fake" in text


@pytest.mark.parametrize("stem", STEMS)
def test_apply_reaches_the_write_path(stem, monkeypatch, harness):
    mod = _setup(monkeypatch, stem, harness)
    _run(mod, stem, ["--apply"])
    assert "server_settings" not in harness.pools[0]
    assert any(w.startswith(WRITE_TABLE[stem]) for w in _writes(harness)), _writes(harness)
    if stem == "b8a_enrich_entities":
        assert harness.llm_calls
    else:
        assert harness.provider_made == 1 and harness.embed_calls


@pytest.mark.parametrize("stem,default", [("reembed_chunks", 500), ("backfill_entity_embeddings", 500)])
def test_apply_is_capped_by_default_and_zero_lifts_the_cap(stem, default, monkeypatch, harness):
    mod = _setup(monkeypatch, stem, harness)
    mod.cli(["--apply"])
    selects = [s for s in harness.sql if s.startswith("SELECT") and "ORDER BY" in s
               and "GROUP BY" not in s]
    assert selects and selects[-1].endswith(f"LIMIT {default}")
    harness.sql.clear()
    mod.cli(["--apply", "--limit", "0"])
    selects = [s for s in harness.sql if s.startswith("SELECT") and "ORDER BY" in s
               and "GROUP BY" not in s]
    assert selects and "LIMIT" not in selects[-1]


@pytest.mark.parametrize("stem,runner,default", [("b8a_enrich_entities", "run_enrichment", 50),
                                                 ("backfill_contextual_retrieval", "run_backfill", 10)])
def test_llm_scripts_have_a_finite_default_limit(stem, runner, default, monkeypatch, harness):
    mod = _setup(monkeypatch, stem, harness)
    seen = []

    async def record(args):
        seen.append(args)
    monkeypatch.setattr(mod, runner, record)
    mod.main(["--apply"])
    mod.main(["--apply", "--limit", "0"])
    assert [a.limit for a in seen] == [default, 0]
    assert all(a.apply and not a.dry_run for a in seen)


def test_contextual_failed_generation_writes_nothing(monkeypatch, harness, caplog):
    caplog.set_level("INFO")
    mod = _setup(monkeypatch, "backfill_contextual_retrieval", harness)
    harness.context_reply = lambda chunks: [""] * len(chunks)
    mod.main(["--apply"])
    assert harness.llm_calls, "context generation was attempted"
    assert _writes(harness) == []
    assert harness.embed_calls == []
    assert "left unchanged" in caplog.text


def test_contextual_partial_failure_writes_only_chunks_with_context(monkeypatch, harness):
    mod = _setup(monkeypatch, "backfill_contextual_retrieval", harness)
    rows = [dict(ROW, chunk_rid=f"chunk:{i}", chunk_index=i) for i in range(3)]

    async def fetch(sql, *args):
        harness.sql.append(" ".join(sql.split()))
        return rows
    harness.context_reply = lambda chunks: ["ctx one", "", "ctx three"]
    orig = FakeConn.fetch
    monkeypatch.setattr(FakeConn, "fetch", lambda self, sql, *a: fetch(sql, *a))
    mod.main(["--apply"])
    monkeypatch.setattr(FakeConn, "fetch", orig)
    updates = [w for w in _writes(harness) if w.startswith("UPDATE koi_memory_chunks")]
    assert len(updates) == 2
    assert [len(c) for c in harness.embed_calls] == [2]
