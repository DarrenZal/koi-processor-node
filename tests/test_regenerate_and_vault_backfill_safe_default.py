"""regenerate_embeddings.py, api/scripts/backfill_vault_notes.py and
scripts/ops/build_null_embed_indexes.sh act only with --apply (D13).

Offline: the database drivers and HTTP client are fakes, psql is a recording stub, and
every socket connect fails the test.
"""

from __future__ import annotations

import importlib.util
import os
import socket
import subprocess
import time
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, REPO / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def no_network(monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("network call attempted")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


# ---------------------------------------------------------------- regenerate_embeddings


class PgCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        self.conn.sql.append(" ".join(sql.split()))

    def fetchall(self):
        return [{"memory_id": "m1", "rid": "rid:website:1", "text": "a document text body",
                 "source": "website", "url": None, "embedding_id": None}]

    def close(self):
        pass


class PgConn:
    def __init__(self, kwargs):
        self.kwargs = kwargs
        self.sql = []
        self.readonly = None
        self.commits = 0

    def set_session(self, readonly=None, **_kw):
        self.readonly = readonly

    def cursor(self, cursor_factory=None):
        return PgCursor(self)

    def commit(self):
        self.commits += 1

    def close(self):
        pass


@pytest.fixture
def regen(monkeypatch, no_network):
    mod = _load("scripts/regenerate_embeddings.py", "d13_regenerate_embeddings")
    state = types.SimpleNamespace(conns=[], http=[])

    def connect(**kwargs):
        conn = PgConn(kwargs)
        state.conns.append(conn)
        return conn

    class Resp:
        def __init__(self, data):
            self.data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self.data

    def get(url, **_k):
        state.http.append(("GET", url))
        return Resp({"model": "BAAI/bge-large-en-v1.5", "device": "cpu"})

    def post(url, **_k):
        state.http.append(("POST", url))
        return Resp({"embedding": [0.1] * 4})

    monkeypatch.setattr(mod, "psycopg2", types.SimpleNamespace(connect=connect))
    monkeypatch.setattr(mod, "requests", types.SimpleNamespace(get=get, post=post))
    monkeypatch.setattr(mod, "BGE_SERVER_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(mod, "DB_CONFIG", {"host": "127.0.0.1", "port": 1, "dbname": "koi_fake",
                                           "user": "tester", "password": "changeme"})
    monkeypatch.setattr(time, "sleep", lambda _s: None)
    return mod, state


def _exit_code(fn, argv):
    with pytest.raises(SystemExit) as exc:
        fn(argv)
    return exc.value.code


def test_regenerate_help_has_no_side_effects(regen, capsys):
    mod, state = regen
    assert _exit_code(mod.main, ["--help"]) == 0
    assert "--apply" in capsys.readouterr().out
    assert state.conns == [] and state.http == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--apply"], ["--incremental"]])
def test_regenerate_default_is_read_only(regen, argv, caplog):
    caplog.set_level("INFO")
    mod, state = regen
    assert _exit_code(mod.main, argv) == 0
    assert state.http == [], "a dry run must not contact the BGE server"
    assert len(state.conns) == 1 and state.conns[0].readonly is True
    assert state.conns[0].commits == 0
    assert all(s.startswith("SELECT") for s in state.conns[0].sql)
    assert "DRY RUN" in caplog.text
    assert "tester@127.0.0.1:1/koi_fake" in caplog.text and "changeme" not in caplog.text


def test_regenerate_apply_writes(regen):
    mod, state = regen
    assert _exit_code(mod.main, ["--apply"]) == 0
    assert ("GET", "http://127.0.0.1:1/health") in state.http
    assert ("POST", "http://127.0.0.1:1/encode") in state.http
    conn = state.conns[0]
    assert conn.readonly is None and conn.commits == 1
    assert any(s.startswith("INSERT INTO koi_embeddings") for s in conn.sql)


def test_regenerate_defaults_are_localhost(monkeypatch):
    for var in ("BGE_SERVER_URL", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB"):
        monkeypatch.delenv(var, raising=False)
    mod = _load("scripts/regenerate_embeddings.py", "d13_regenerate_embeddings_defaults")
    assert mod.BGE_SERVER_URL == "http://localhost:8090"
    assert (mod.DB_CONFIG["host"], mod.DB_CONFIG["port"], mod.DB_CONFIG["dbname"]) == (
        "localhost", 5433, "eliza")


# ---------------------------------------------------------------- backfill_vault_notes


class VaultConn:
    def __init__(self, state):
        self.state = state

    async def fetch(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        return [{"fuseki_uri": "urn:koi:concept:kelp", "entity_text": "Kelp Forest",
                 "entity_type": "Concept"}]

    async def execute(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        return "INSERT 0 1"

    async def close(self):
        pass


@pytest.fixture
def vault(monkeypatch, tmp_path, no_network):
    mod = _load("api/scripts/backfill_vault_notes.py", "d13_backfill_vault_notes")
    state = types.SimpleNamespace(connects=[], sql=[])

    async def connect(url, **kwargs):
        state.connects.append((url, kwargs))
        return VaultConn(state)

    monkeypatch.setattr(mod, "asyncpg", types.SimpleNamespace(connect=connect))
    root = tmp_path / "vault"
    root.mkdir()
    monkeypatch.setenv("VAULT_PATH", str(root))
    monkeypatch.setenv("POSTGRES_URL", "postgresql://tester:changeme@127.0.0.1:1/koi_fake")
    return mod, state, root


def _files(root):
    return sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())


def test_vault_help_has_no_side_effects(vault, capsys):
    mod, state, root = vault
    assert _exit_code(mod.main, ["--help"]) == 0
    assert "--apply" in capsys.readouterr().out
    assert state.connects == [] and _files(root) == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--apply"]])
def test_vault_default_writes_nothing(vault, argv, caplog):
    caplog.set_level("INFO")
    mod, state, root = vault
    mod.main(argv)
    assert _files(root) == []
    assert state.connects[0][1] == {"server_settings": {"default_transaction_read_only": "on"}}
    assert all(s.startswith("SELECT") for s in state.sql)
    assert "WOULD CREATE" in caplog.text
    assert "changeme" not in caplog.text and "tester:***@127.0.0.1:1/koi_fake" in caplog.text


def test_vault_apply_writes_note_and_mapping(vault):
    mod, state, root = vault
    mod.main(["--apply"])
    assert state.connects[0][1] == {}
    assert len(_files(root)) == 1 and _files(root)[0].endswith("Kelp Forest.md")
    assert any(s.startswith("INSERT INTO entity_rid_mappings") for s in state.sql)


# ---------------------------------------------------------------- build_null_embed_indexes.sh

SCRIPT = REPO / "scripts" / "ops" / "build_null_embed_indexes.sh"


@pytest.fixture
def fake_psql(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log = tmp_path / "psql.log"
    stub = bindir / "psql"
    stub.write_text(f'#!/bin/sh\nprintf "%s|" "$@" >> "{log}"\necho >> "{log}"\n')
    stub.chmod(0o755)
    env = {"PATH": f"{bindir}:/usr/bin:/bin", "HOME": str(tmp_path), "LANG": "C"}
    return env, log


def _sh(args, env):
    return subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True,
                          timeout=20)


def _calls(log):
    return log.read_text().splitlines() if log.exists() else []


@pytest.mark.parametrize("args", [["--help"], ["-h"]])
def test_indexes_help(fake_psql, args):
    env, log = fake_psql
    r = _sh(args, env)
    assert r.returncode == 0 and "--apply" in r.stdout
    assert _calls(log) == []


@pytest.mark.parametrize("args", [[], ["--dry-run"], ["--apply", "--dry-run"], ["mydb"]])
def test_indexes_default_runs_nothing(fake_psql, args):
    env, log = fake_psql
    r = _sh(args, env)
    assert r.returncode == 0, r.stderr
    assert "DRY RUN" in r.stdout and "CREATE INDEX CONCURRENTLY" in r.stdout
    assert ("database mydb" if args == ["mydb"] else "database personal_koi") in r.stdout
    assert _calls(log) == []


def test_indexes_rejects_unknown_option(fake_psql):
    env, log = fake_psql
    r = _sh(["--force"], env)
    assert r.returncode == 2 and _calls(log) == []


@pytest.mark.parametrize("args,db", [(["--apply"], "personal_koi"), (["--apply", "mydb"], "mydb"),
                                     (["mydb", "--apply"], "mydb")])
def test_indexes_apply_runs_the_old_statements(fake_psql, args, db):
    env, log = fake_psql
    r = _sh(args, env)
    assert r.returncode == 0, r.stderr
    calls = _calls(log)
    assert len(calls) == 3
    assert all(c.startswith(f"-d|{db}|-X|") for c in calls)
    assert "idx_kmc_null_embed3072 ON koi_memory_chunks" in calls[0]
    assert "idx_sc_null_embed3072  ON session_chunks" in calls[1]
    assert "indisvalid" in calls[2]
