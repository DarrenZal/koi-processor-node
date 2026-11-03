"""The Ollama re-embed scripts are dry runs unless --apply is given (D13).

Covers scripts/fast_chunk_reembed.py, fast_entity_reembed.py, fast_reembed_all.py and
simple_chunk_reembed.py. Offline: the database driver is a fake and every socket
connect fails the test.
"""

from __future__ import annotations

import importlib.util
import socket
import types
import urllib.request
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FAKE_URL = "postgresql://tester:changeme@127.0.0.1:1/koi_fake"

SCRIPTS = {
    "fast_chunk_reembed": ["koi_memory_chunks"],
    "fast_entity_reembed": ["entity_registry"],
    "fast_reembed_all": ["entity_registry", "koi_memory_chunks"],
    "simple_chunk_reembed": ["koi_memory_chunks"],
}


def _load(stem):
    spec = importlib.util.spec_from_file_location(f"d13_{stem}", REPO / "scripts" / f"{stem}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.last = ""

    def execute(self, sql, params=None):
        self.conn.sql.append(" ".join(sql.split()))
        self.last = sql

    def fetchone(self):
        return (1,)

    def fetchall(self):
        if "FROM entity_registry" in self.last:
            return [{"fuseki_uri": "urn:e:1", "entity_text": "Kelp", "description": None,
                     "context": None}]
        return [{"chunk_rid": "chunk:1", "text": "some text", "title": "T"}]

    def close(self):
        pass


class FakeConn:
    def __init__(self):
        self.sql = []
        self.readonly = None
        self.commits = 0
        self.autocommit = True

    def set_session(self, readonly=None, **_kw):
        self.readonly = readonly

    def cursor(self, cursor_factory=None):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


@pytest.fixture
def no_network(monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("network call attempted")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(urllib.request, "urlopen", refuse)


@pytest.fixture
def fake_db(monkeypatch):
    conns = []

    def connect(**kwargs):
        conn = FakeConn()
        conn.kwargs = kwargs
        conns.append(conn)
        return conn

    return conns, types.SimpleNamespace(connect=connect)


def _prepare(monkeypatch, stem, fake_db):
    mod = _load(stem)
    conns, fake_psycopg2 = fake_db
    monkeypatch.setattr(mod, "psycopg2", fake_psycopg2)
    monkeypatch.setattr(mod, "POSTGRES_URL", FAKE_URL)
    embedded = []

    def fake_embed(text):
        embedded.append(text)
        return [0.25, 0.5]

    monkeypatch.setattr(mod, "embed_one", fake_embed)
    return mod, conns, embedded


def _writes(conns):
    return [s for c in conns for s in c.sql if not s.upper().startswith("SELECT")]


@pytest.mark.parametrize("stem", sorted(SCRIPTS))
def test_help_exits_zero_without_side_effects(stem, monkeypatch, no_network, fake_db, capsys):
    mod, conns, embedded = _prepare(monkeypatch, stem, fake_db)
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    assert conns == [] and embedded == []
    assert "--apply" in capsys.readouterr().out


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--apply"]])
@pytest.mark.parametrize("stem", sorted(SCRIPTS))
def test_default_is_read_only_dry_run(stem, argv, monkeypatch, no_network, fake_db, capsys):
    mod, conns, embedded = _prepare(monkeypatch, stem, fake_db)
    assert mod.main(argv) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "changeme" not in out and "tester:***@127.0.0.1:1/koi_fake" in out
    assert embedded == []
    assert conns and all(c.readonly is True and c.commits == 0 for c in conns)
    assert _writes(conns) == []
    counted = " ".join(s for c in conns for s in c.sql)
    for table in SCRIPTS[stem]:
        assert f"FROM {table} WHERE embedding IS NULL" in counted


@pytest.mark.parametrize("stem", sorted(SCRIPTS))
def test_apply_reaches_the_write_path(stem, monkeypatch, no_network, fake_db, capsys):
    mod, conns, embedded = _prepare(monkeypatch, stem, fake_db)
    assert mod.main(["--apply"]) in (0, None)
    writes = _writes(conns)
    for table in SCRIPTS[stem]:
        assert any(w.startswith(f"UPDATE {table} SET embedding") for w in writes), writes
    assert embedded
    assert all(c.readonly is None for c in conns)
    assert sum(c.commits for c in conns) >= 1


@pytest.mark.parametrize("stem", sorted(SCRIPTS))
def test_default_ollama_url_is_localhost(stem, monkeypatch):
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    mod = _load(stem)
    assert mod.OLLAMA_URL == "http://localhost:11434"


@pytest.mark.parametrize("stem", sorted(SCRIPTS))
def test_unparseable_url_stops_before_connecting(stem, monkeypatch, no_network, fake_db, capsys):
    mod, conns, embedded = _prepare(monkeypatch, stem, fake_db)
    monkeypatch.setattr(mod, "POSTGRES_URL", "not-a-url")
    if stem == "fast_entity_reembed":
        # This one already exited 1 on an unparseable URL before D13.
        with pytest.raises(SystemExit) as exc:
            mod.main(["--apply"])
        assert exc.value.code == 1
    else:
        assert mod.main(["--apply"]) == 1
    assert conns == [] and embedded == []
