"""scripts/test_claims_api.py writes only with --apply, and its purge deletes only from
the database the API wrote to (D13).

Offline: the script's HTTP helper is replaced by a recorder and psycopg2 by a fake, so
no test reaches a network or a database.
"""

from __future__ import annotations

import importlib
import sys
import types

import pytest


@pytest.fixture
def smoke(monkeypatch):
    mod = importlib.import_module("scripts.test_claims_api")
    mod = importlib.reload(mod)  # fresh module globals (CREATED_RIDS, PURGE_DSN, BASE_URL)
    calls: list[str] = []

    def fake_req(method, path, body=None, expected_status=None):
        calls.append(path)
        if path == "/health":
            return 200, {"status": "healthy"}
        return 500, {"detail": "fake API"}

    monkeypatch.setattr(mod, "_req", fake_req)
    for var in ("KOI_LIVE_POSTGRES_URL", "POSTGRES_URL"):
        monkeypatch.delenv(var, raising=False)
    return mod, calls


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self.rowcount = 0
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.db.executed.append(sql)
        if sql.startswith("SELECT current_database()"):
            self._rows = [(self.db.name,)]
        elif sql.startswith("SELECT claim_rid FROM claims WHERE claim_rid = ANY(%s) AND"):
            self._rows = []  # nothing anchored
        elif sql.startswith("SELECT claim_rid FROM claims"):
            self._rows = [(r,) for r in params[0] if r in self.db.claims]
        elif sql.startswith("SELECT count(*)"):
            self._rows = [(0,)]
        else:
            self.rowcount = len(params[0]) if params else 0

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self.db)


@pytest.fixture
def fake_pg(monkeypatch):
    db = types.SimpleNamespace(name="koi_test_api", claims=set(), executed=[], dsns=[])
    fake = types.ModuleType("psycopg2")

    def connect(dsn):
        db.dsns.append(dsn)
        return FakeConn(db)

    fake.connect = connect
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    return db


def _deletes(db):
    return [s for s in db.executed if "DELETE FROM claims" in s or "DELETE FROM claim_attestations" in s]


def test_help(smoke, fake_pg):
    mod, calls = smoke
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    assert calls == [] and fake_pg.dsns == []


def test_no_arguments_is_a_usage_error(smoke, fake_pg):
    mod, calls = smoke
    with pytest.raises(SystemExit) as exc:
        mod.main([])
    assert exc.value.code == 2
    assert calls == [] and fake_pg.dsns == []


@pytest.mark.parametrize("extra", [[], ["--dry-run"], ["--dry-run", "--apply"],
                                   ["--db-url", "postgresql://u:pw@127.0.0.1:1/koi_test_x"]])
def test_without_apply_nothing_is_sent(smoke, fake_pg, capsys, extra):
    mod, calls = smoke
    mod.main(["--base-url", "http://127.0.0.1:1", *extra])
    assert calls == [] and fake_pg.dsns == []
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "pw@" not in out


def test_apply_without_a_purge_database_refuses_before_writing(smoke, fake_pg):
    mod, calls = smoke
    with pytest.raises(SystemExit) as exc:
        mod.main(["--base-url", "http://127.0.0.1:1", "--apply"])
    assert exc.value.code == 1
    assert calls == []


def test_apply_reaches_the_write_path(smoke, fake_pg):
    mod, calls = smoke
    with pytest.raises(SystemExit):
        mod.main(["--base-url", "http://127.0.0.1:1", "--apply",
                  "--db-url", "postgresql://u@127.0.0.1:1/koi_test_x"])
    assert calls[0] == "/health"
    assert "/entity/resolve" in calls and "/ingest" in calls


def test_apply_with_no_cleanup_needs_no_purge_database(smoke, fake_pg):
    mod, calls = smoke
    with pytest.raises(SystemExit):
        mod.main(["--base-url", "http://127.0.0.1:1", "--apply", "--no-cleanup"])
    assert "/ingest" in calls


def test_purge_deletes_when_the_database_holds_the_run_s_claims(smoke, fake_pg):
    mod, _ = smoke
    mod.PURGE_DSN = "postgresql://u@127.0.0.1:1/koi_test_api"
    mod.BASE_URL = "http://127.0.0.1:1"
    mod.CREATED_RIDS[:] = ["orn:test:a", "orn:test:b"]
    fake_pg.claims = {"orn:test:a", "orn:test:b"}
    mod.purge_created_claims()
    assert fake_pg.dsns == [mod.PURGE_DSN]
    assert len(_deletes(fake_pg)) == 2


def test_purge_refuses_a_database_without_the_run_s_claims(smoke, fake_pg):
    mod, _ = smoke
    mod.PURGE_DSN = "postgresql://u@127.0.0.1:1/some_other_db"
    mod.BASE_URL = "http://127.0.0.1:1"
    mod.CREATED_RIDS[:] = ["orn:test:a", "orn:test:b"]
    fake_pg.claims = {"orn:test:a"}
    with pytest.raises(SystemExit) as exc:
        mod.purge_created_claims()
    assert exc.value.code == 1
    assert _deletes(fake_pg) == []
