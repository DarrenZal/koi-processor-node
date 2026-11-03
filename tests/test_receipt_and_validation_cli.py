"""Receipt demos, provenance demo and validation scripts are dry runs unless --apply.

Covers src/cat/cat_receipt_chain.py, src/cat/coordinator_receipt_integration.py,
src/cat/cat_receipt_chain_v2.py, src/core/provenance_to_rdf.py,
src/utils/complete_validation.py and src/utils/ultimate_verification.py. Every test runs
offline: sockets and database drivers are replaced, and the opt-in tests use fakes.
"""

import asyncio
import importlib.util
import json
import socket
import sys
import types
from pathlib import Path

import httpx
import pytest

REPO = Path(__file__).resolve().parents[1]


def _load(rel, name=None):
    name = name or "rv_cli_" + Path(rel).stem
    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def blocked(monkeypatch, tmp_path):
    """Refuse and record every socket connect and database connect; run in tmp_path."""
    calls = []

    def deny(name):
        def f(*a, **k):
            calls.append(name)
            raise ConnectionRefusedError(f"test blocked {name}")
        return f

    monkeypatch.setattr(socket.socket, "connect", deny("socket.connect"))
    monkeypatch.setattr(socket.socket, "connect_ex", deny("socket.connect_ex"))
    monkeypatch.setattr(socket, "create_connection", deny("socket.create_connection"))
    import asyncpg
    import psycopg2
    monkeypatch.setattr(asyncpg, "connect", deny("asyncpg.connect"))
    monkeypatch.setattr(asyncpg, "create_pool", deny("asyncpg.create_pool"))
    monkeypatch.setattr(psycopg2, "connect", deny("psycopg2.connect"))
    monkeypatch.chdir(tmp_path)
    return calls


class FakeConn:
    def __init__(self, log, rows=()):
        self.log = log
        self.rows = list(rows)

    async def execute(self, sql, *args):
        self.log.append(" ".join(sql.split()))
        return "OK"

    async def fetchrow(self, sql, *args):
        self.log.append(" ".join(sql.split()))
        return None

    async def fetch(self, sql, *args):
        self.log.append(" ".join(sql.split()))
        return self.rows

    async def close(self):
        pass


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False
        return _Ctx()

    async def close(self):
        pass


def _stub_jena(monkeypatch, record):
    class JenaProvenanceManager:
        def __init__(self, query_endpoint=None, **kw):
            self.query_endpoint = query_endpoint

        async def store_artifact(self, **kw):
            record.append(("artifact", kw.get("rid")))

        async def store_transformation(self, **kw):
            record.append(("transformation", kw.get("input_rid")))
            return "urn:activity", "urn:receipt"

    pkg = types.ModuleType("provenance")
    pkg.__path__ = []
    sub = types.ModuleType("provenance.jena_integration")
    sub.JenaProvenanceManager = JenaProvenanceManager
    monkeypatch.setitem(sys.modules, "provenance", pkg)
    monkeypatch.setitem(sys.modules, "provenance.jena_integration", sub)


def _v2(monkeypatch, record=None):
    _stub_jena(monkeypatch, record if record is not None else [])
    return _load("src/cat/cat_receipt_chain_v2.py")


CLIS = [
    ("src/cat/cat_receipt_chain.py", "cli_main"),
    ("src/cat/coordinator_receipt_integration.py", "cli_main"),
    ("src/core/provenance_to_rdf.py", "cli_main"),
    ("src/utils/complete_validation.py", "cli_main"),
    ("src/utils/ultimate_verification.py", "cli_main"),
    ("src/cat/cat_receipt_chain_v2.py", "cli_main"),
]


def _module(rel, monkeypatch):
    if rel.endswith("cat_receipt_chain_v2.py"):
        return _v2(monkeypatch)
    return _load(rel)


@pytest.mark.parametrize("rel,entry", CLIS)
def test_help_does_nothing(rel, entry, blocked, monkeypatch, tmp_path, capsys):
    mod = _module(rel, monkeypatch)
    with pytest.raises(SystemExit) as exc:
        getattr(mod, entry)(["--help"])
    assert exc.value.code == 0
    assert "--apply" in capsys.readouterr().out
    assert blocked == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--apply"]])
@pytest.mark.parametrize("rel,entry", CLIS)
def test_default_and_dry_run_do_nothing(rel, entry, argv, blocked, monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("POSTGRES_URL", "postgresql://u:changeme@127.0.0.1:1/x")
    mod = _module(rel, monkeypatch)
    ran = []
    for do_it in ("main", "migrate_existing_receipts", "test_provenance", "_example_receipt"):
        if hasattr(mod, do_it):
            monkeypatch.setattr(mod, do_it, lambda *a, _n=do_it, **k: ran.append(_n))
    assert getattr(mod, entry)(argv) == 0
    out = capsys.readouterr().out
    assert "Dry run" in out
    assert "changeme" not in out
    assert ran == []
    assert blocked == []
    assert list(tmp_path.iterdir()) == []


def test_cat_receipt_chain_apply_creates_table_and_inserts(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POSTGRES_URL", "postgresql://u@127.0.0.1:1/throwaway")
    mod = _load("src/cat/cat_receipt_chain.py")
    log = []

    async def create_pool(**cfg):
        assert cfg == {"dsn": "postgresql://u@127.0.0.1:1/throwaway"}
        return FakePool(FakeConn(log))
    monkeypatch.setattr(mod.asyncpg, "create_pool", create_pool)
    assert mod.cli_main(["--apply"]) == 0
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS cat_receipts") for s in log)
    assert sum(s.startswith("INSERT INTO cat_receipts") for s in log) == 3


def test_coordinator_receipt_apply_inserts(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POSTGRES_URL", "postgresql://u@127.0.0.1:1/throwaway")
    mod = _load("src/cat/coordinator_receipt_integration.py")
    log = []

    async def connect(**cfg):
        return FakeConn(log)
    monkeypatch.setattr(mod.asyncpg, "connect", connect)
    assert mod.cli_main(["--apply"]) == 0
    assert any(s.startswith("INSERT INTO koi_transformation_receipts") for s in log)


def _mock_async_client(monkeypatch, handler):
    real = httpx.AsyncClient

    class _Client(real):
        def __init__(self, *a, **k):
            k["transport"] = httpx.MockTransport(handler)
            super().__init__(*a, **k)
    monkeypatch.setattr(httpx, "AsyncClient", _Client)


def test_provenance_apply_inserts_example(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load("src/core/provenance_to_rdf.py")
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path, request.content.decode()))
        if request.url.path.endswith("/update"):
            return httpx.Response(204)
        if request.url.path.endswith("/sparql"):
            return httpx.Response(200, json={"results": {"bindings": []}})
        return httpx.Response(200, text="ok")
    _mock_async_client(monkeypatch, handler)
    assert mod.cli_main(["--apply"]) == 0
    assert ("GET", "/$/ping") in [(m, p) for m, p, _ in seen]
    inserts = [b for m, p, b in seen if p == "/koi/update"]
    assert inserts and "INSERT DATA" in inserts[0] and "README_md_v1" in inserts[0]


def test_complete_validation_apply_posts_event_and_writes_output(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load("src/utils/complete_validation.py")
    seen = []

    def handler(request):
        seen.append((request.method, request.url.port, request.url.path))
        if request.url.path == "/encode":
            return httpx.Response(200, json={"embedding": [0.0] * 1024})
        if request.url.path == "/search":
            return httpx.Response(200, json={"memories": []})
        return httpx.Response(200, json={"rid": "test", "status": "ok"})
    _mock_async_client(monkeypatch, handler)
    out = tmp_path / "results.json"
    assert mod.cli_main(["--apply", "--output", str(out)]) == 0
    assert ("POST", 8005, "/api/event") in seen
    assert json.loads(out.read_text())["summary"]["tests"] == "4/4"


def test_ultimate_verification_apply_runs_main(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load("src/utils/ultimate_verification.py")
    ran = []
    monkeypatch.setattr(mod, "main", lambda: ran.append("main") or True)
    assert mod.cli_main(["--apply"]) == 0
    monkeypatch.setattr(mod, "main", lambda: ran.append("main") or False)
    assert mod.cli_main(["--apply"]) == 1
    assert ran == ["main", "main"]


def test_ultimate_verification_trace_posts_production_test_event(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _load("src/utils/ultimate_verification.py")
    posts = []

    class _Resp:
        status_code = 500

        def json(self):
            return {"success": False}

    class _Requests:
        def post(self, url, json=None, **k):
            posts.append((url, json))
            return _Resp()
    monkeypatch.setattr(mod, "requests", _Requests())
    ok, _ = mod.trace_unique_content()
    assert ok is False
    assert posts[0][0] == "http://localhost:8100/process-koi-event"
    assert posts[0][1]["bundle"]["metadata"]["source"] == "production_test"


def test_cat_receipt_chain_v2_apply_migrates(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    jena = []
    mod = _v2(monkeypatch, jena)
    log = []
    from datetime import datetime, timezone
    row = {"receipt_id": "r" * 64, "transformation_type": "chunking", "input_rid": "in",
           "output_rid": "out", "parent_receipt_id": None, "processor_name": "p",
           "source_sensor": "s", "created_at": datetime.now(timezone.utc), "metadata": "{}"}
    dsns = []

    async def create_pool(dsn):
        dsns.append(dsn)
        return FakePool(FakeConn(log, rows=[row]))
    monkeypatch.setattr(mod.asyncpg, "create_pool", create_pool)
    assert mod.cli_main(["--apply", "--db-url", "postgresql://u@127.0.0.1:1/throwaway"]) == 0
    assert dsns == ["postgresql://u@127.0.0.1:1/throwaway"]
    assert ("transformation", "in") in jena
    assert any(s.startswith("UPDATE koi_transformation_receipts") for s in log)


def test_cat_receipt_chain_v2_default_db_is_unchanged(monkeypatch):
    mod = _v2(monkeypatch)
    seen = []

    async def fake_migrate(db_url=None):
        seen.append(db_url)
    monkeypatch.setattr(mod, "migrate_existing_receipts", fake_migrate)
    assert mod.cli_main(["--apply"]) == 0
    assert seen == [mod.DEFAULT_MIGRATION_DB_URL]
    assert mod.DEFAULT_MIGRATION_DB_URL.endswith("@localhost:5433/eliza")
