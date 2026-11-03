"""backfill_phonetic_codes.py and tier-3/reembed-null-facts.py act only with --apply (D13).

Offline: asyncpg, the schema loader, the /health pre-flight and the embedding provider
are fakes, and every socket connect fails the test.
"""

from __future__ import annotations

import importlib.util
import socket
import sys
import types
from pathlib import Path
from urllib.parse import urlparse

import pytest

REPO = Path(__file__).resolve().parents[1]
FAKE_URL = "postgresql://tester:changeme@127.0.0.1:1/koi_fake"
READ_ONLY = {"default_transaction_read_only": "on"}


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


class _AsyncNull:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *exc):
        return False


class Conn:
    """Fake asyncpg connection: answers by the shape of the SQL."""

    def __init__(self, state):
        self.state = state

    async def fetchval(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        if "pg_indexes" in sql:
            return False
        return self.state.count

    async def fetch(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        if "with_phonetic" in sql:
            return [{"entity_type": "Concept", "total": 2, "with_phonetic": 1, "without_phonetic": 1}]
        if "RANDOM()" in sql:
            return [{"entity_type": "Concept", "entity_text": "Kelp", "normalized_text": "kelp",
                     "phonetic_code": "KLP"}]
        if "normalized_text" in sql:
            return [{"id": 1, "normalized_text": "kelp forest"}]
        rows = [{"id": i, "fact_text": f"fact {i}"} for i in range(self.state.count)]
        return rows[:3] if sql.rstrip().endswith("LIMIT 3") else rows

    async def execute(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        return "UPDATE 1"

    def transaction(self):
        return _AsyncNull()

    async def close(self):
        pass


def _fake_asyncpg(state):
    async def connect(url, **kwargs):
        state.connects.append((url, kwargs))
        return Conn(state)
    return types.SimpleNamespace(connect=connect)


def _writes(state):
    return [s for s in state.sql if not s.upper().startswith("SELECT")]


# ---------------------------------------------------------------- backfill_phonetic_codes


@pytest.fixture
def phonetic(monkeypatch, no_network):
    mod = _load("scripts/backfill_phonetic_codes.py", "d13_backfill_phonetic_codes")
    state = types.SimpleNamespace(sql=[], connects=[], count=2)
    monkeypatch.setattr(mod, "asyncpg", _fake_asyncpg(state))
    monkeypatch.setattr(mod, "DB_URL", FAKE_URL)
    monkeypatch.setattr(mod, "reload_entity_schemas", lambda _p: {"Concept": 1, "Location": 2})
    monkeypatch.setattr(mod, "get_phonetic_enabled_types", lambda: ["Concept"])
    monkeypatch.setattr(mod, "get_schema_for_type",
                        lambda _t: types.SimpleNamespace(phonetic_stopwords=[]))
    monkeypatch.setattr(mod, "get_first_significant_token", lambda text, _sw: text.split()[0])
    return mod, state


def test_phonetic_help_has_no_side_effects(phonetic, capsys):
    mod, state = phonetic
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0 and "--apply" in capsys.readouterr().out
    assert state.connects == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--clear-stale"], ["--apply", "--dry-run"],
                                  ["--stats"]])
def test_phonetic_default_is_read_only(phonetic, argv, capsys):
    mod, state = phonetic
    mod.main(argv)
    assert state.connects == [(FAKE_URL, {"server_settings": READ_ONLY})]
    assert _writes(state) == []
    out = capsys.readouterr().out
    assert "changeme" not in out and "tester:***@127.0.0.1:1/koi_fake" in out


def test_phonetic_apply_writes(phonetic):
    mod, state = phonetic
    mod.main(["--apply"])
    assert state.connects == [(FAKE_URL, {})]
    writes = _writes(state)
    assert any(w.startswith("UPDATE entity_registry SET phonetic_code = $1") for w in writes)
    assert any(w.startswith("CREATE INDEX idx_entity_phonetic_code") for w in writes)
    assert not any("SET phonetic_code = NULL" in w for w in writes)


def test_phonetic_apply_clear_stale(phonetic):
    mod, state = phonetic
    mod.main(["--apply", "--clear-stale"])
    assert any("SET phonetic_code = NULL" in w for w in _writes(state))


def test_phonetic_default_dsn_names_no_user(monkeypatch):
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    mod = _load("scripts/backfill_phonetic_codes.py", "d13_backfill_phonetic_codes_default")
    parts = urlparse(mod.DB_URL)
    # A local default with no role in it (the driver then uses the current OS user).
    assert parts.scheme == "postgresql" and parts.hostname == "localhost"
    assert parts.username is None and "@" not in mod.DB_URL


# ---------------------------------------------------------------- reembed-null-facts

FACTS = "scripts/tier-3/reembed-null-facts.py"


class Provider:
    def __init__(self, state):
        self.state = state

    async def embed_or_none(self, text, prompt_type=None):
        self.state.embeds += 1
        return [0.5, 0.25]


@pytest.fixture
def facts(monkeypatch, no_network):
    mod = _load(FACTS, "d13_reembed_null_facts")
    state = types.SimpleNamespace(sql=[], connects=[], count=3, health=0, providers=0, embeds=0)
    monkeypatch.setattr(mod, "asyncpg", _fake_asyncpg(state))

    def health():
        state.health += 1
        return True

    async def provider():
        state.providers += 1
        return Provider(state)

    monkeypatch.setattr(mod, "preflight_health", health)
    monkeypatch.setattr(mod, "get_provider", provider)
    return mod, state


def test_facts_help_has_no_side_effects(facts, capsys):
    mod, state = facts
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0 and "--apply" in capsys.readouterr().out
    assert state.connects == [] and state.health == 0


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--apply", "--dry-run"]])
def test_facts_default_is_read_only(facts, argv, capsys):
    mod, state = facts
    assert mod.main(["--db-url", FAKE_URL, *argv]) == 0
    assert state.connects == [(FAKE_URL, {"server_settings": READ_ONLY})]
    assert state.health == 0, "a dry run must not poll the KOI API"
    assert state.providers == 0 and state.embeds == 0
    assert _writes(state) == []
    out = capsys.readouterr().out
    assert "would-attempt=3" in out
    assert "changeme" not in out and "tester:***@127.0.0.1:1/koi_fake" in out


def test_facts_apply_writes_and_is_capped(facts):
    mod, state = facts
    state.count = 600
    assert mod.main(["--db-url", FAKE_URL, "--apply"]) == 0
    assert state.health == 1 and state.connects == [(FAKE_URL, {})]
    updates = [w for w in _writes(state) if w.startswith("UPDATE knowledge_facts")]
    assert len(updates) == 500 and state.embeds == 500


def test_facts_apply_max_records_zero_is_unbounded(facts):
    mod, state = facts
    state.count = 600
    assert mod.main(["--db-url", FAKE_URL, "--apply", "--max-records", "0"]) == 0
    assert len([w for w in _writes(state) if w.startswith("UPDATE knowledge_facts")]) == 600


def test_facts_missing_dependency_does_not_exit_at_import(monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "asyncpg", None)
    mod = _load(FACTS, "d13_reembed_null_facts_nodeps")
    assert mod.asyncpg is None
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    with pytest.raises(SystemExit) as exc:
        mod.main(["--db-url", FAKE_URL])
    assert exc.value.code == 1
    assert "asyncpg not available" in capsys.readouterr().err
