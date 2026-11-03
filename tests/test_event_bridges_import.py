"""The two src/core event bridges are servers: they act only when started.

Importing either module opens no connection (the semantic bridge used to connect to
PostgreSQL through its knowledge-graph integrator at import), and `--help` prints usage
and exits 0 instead of starting the server. Run with no arguments they still start as
before. Every test runs offline.
"""

import asyncio
import importlib.util
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
V2 = REPO / "src" / "core" / "koi_event_bridge_v2.py"
SEMANTIC = REPO / "src" / "core" / "koi_event_bridge_semantic.py"


@pytest.fixture
def blocked(monkeypatch, tmp_path):
    calls = []

    def deny(name):
        def f(*a, **k):
            calls.append(name)
            raise ConnectionRefusedError(f"test blocked {name}")
        return f

    monkeypatch.setattr(socket.socket, "connect", deny("socket.connect"))
    monkeypatch.setattr(socket.socket, "connect_ex", deny("socket.connect_ex"))
    monkeypatch.setattr(socket.socket, "bind", deny("socket.bind"))
    monkeypatch.setattr(socket, "create_connection", deny("socket.create_connection"))
    import asyncpg
    import psycopg2
    monkeypatch.setattr(asyncpg, "connect", deny("asyncpg.connect"))
    monkeypatch.setattr(asyncpg, "create_pool", deny("asyncpg.create_pool"))
    monkeypatch.setattr(psycopg2, "connect", deny("psycopg2.connect"))
    monkeypatch.chdir(tmp_path)
    return calls


@pytest.fixture
def bridge_path(monkeypatch):
    """src and src/core on a copy of sys.path, restored after the test."""
    monkeypatch.setattr(sys, "path", [str(REPO / "src" / "core"), str(REPO / "src")] + list(sys.path))


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _help(path, tmp_path):
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
           "PYTHONPATH": os.pathsep.join(p for p in sys.path if p),
           "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run([sys.executable, str(path), "--help"], cwd=tmp_path, env=env,
                          capture_output=True, text=True, timeout=60)


def test_bridge_v2_import_does_nothing(bridge_path, blocked):
    mod = _load(V2, "bridge_v2_under_test")
    assert mod.db_pool is None
    assert blocked == []


def test_bridge_v2_help_exits_without_starting(tmp_path):
    p = _help(V2, tmp_path)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "usage:" in p.stdout and "8100" in p.stdout
    assert "Uvicorn running" not in p.stderr


def test_bridge_v2_masks_the_database_password_in_its_log(bridge_path, blocked):
    mod = _load(V2, "bridge_v2_mask_under_test")
    assert mod._masked_dsn("postgresql://u:secret@h:1/d") == "postgresql://u:***@h:1/d"
    assert mod._masked_dsn("postgresql://h:1/d") == "postgresql://h:1/d"


def test_semantic_bridge_import_does_nothing(bridge_path, blocked, monkeypatch):
    pytest.importorskip("rdflib")  # knowledge_graph.graph_integration needs it to import
    monkeypatch.delenv("POSTGRES_URL", raising=False)  # used to raise at import
    mod = _load(SEMANTIC, "bridge_semantic_under_test")
    assert (mod.llm_extractor, mod.metadata_resolver, mod.smart_chunker,
            mod.kg_integrator, mod.cat_chain) == (None, None, None, None, None)
    assert blocked == []
    with pytest.raises(RuntimeError, match="POSTGRES_URL is not set"):
        mod.init_components()


def test_semantic_bridge_startup_creates_components(bridge_path, blocked, monkeypatch):
    pytest.importorskip("rdflib")
    mod = _load(SEMANTIC, "bridge_semantic_startup_under_test")
    made = []

    def fake(name):
        class _C:
            model = "m"
            store_type = "sparql"

            def __init__(self, *a, **k):
                made.append((name, k))

            async def initialize(self):
                made.append((name, "initialize"))
        return _C

    for name in ("OpenAIExtractor", "MetadataResolver", "SmartChunker",
                 "KnowledgeGraphIntegrator", "CATReceiptChain"):
        monkeypatch.setattr(mod, name, fake(name))
    monkeypatch.setattr(mod, "DB_URL", "postgresql://u@127.0.0.1:1/throwaway")
    asyncio.run(mod.startup_event())
    names = [m[0] for m in made]
    assert names == ["OpenAIExtractor", "MetadataResolver", "SmartChunker",
                     "KnowledgeGraphIntegrator", "CATReceiptChain", "CATReceiptChain"]
    assert made[3][1]["store_config"]["update_endpoint"] == "http://localhost:3030/koi/update"
    assert made[4][1] == {"db_config": {"dsn": "postgresql://u@127.0.0.1:1/throwaway"}}
    assert made[5][1] == "initialize"
    # a second startup does not create them again
    asyncio.run(mod.startup_event())
    assert [m[0] for m in made].count("OpenAIExtractor") == 1


def test_semantic_bridge_help_exits_without_starting(tmp_path):
    pytest.importorskip("rdflib")
    p = _help(SEMANTIC, tmp_path)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "usage:" in p.stdout and "8004" in p.stdout
