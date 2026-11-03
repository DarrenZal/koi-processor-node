"""reembed_oversize_chunks.py does nothing at import and acts only with --apply (D13).

Offline: asyncpg, the tokenizer and the OpenAI client are fakes, and every socket
connect fails the test.
"""

from __future__ import annotations

import importlib.util
import socket
import types
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FAKE_URL = "postgresql://tester:changeme@127.0.0.1:1/koi_fake"
READ_ONLY = {"default_transaction_read_only": "on"}


def _load(name="d13_reembed_oversize_chunks"):
    spec = importlib.util.spec_from_file_location(name, REPO / "scripts" / "reembed_oversize_chunks.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Conn:
    def __init__(self, state):
        self.state = state

    async def fetch(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        if "GROUP BY" in sql:
            return [{"source_sensor": "doc-scanner", "total": 3, "n": 1}]
        return self.state.rows

    async def execute(self, sql, *args):
        self.state.sql.append(" ".join(sql.split()))
        return "UPDATE 1"

    async def close(self):
        pass


class Encoder:
    """Three characters per token."""

    def encode(self, text, disallowed_special=()):
        return list(range(len(text) // 3))

    def decode(self, tokens):
        return "x" * (3 * len(tokens))


@pytest.fixture
def oversize(monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("network call attempted")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    mod = _load()
    state = types.SimpleNamespace(sql=[], connects=[], embeds=[], rows=[
        {"id": 1, "text": "x" * 30000}, {"id": 2, "text": "x" * 27000}, {"id": 3, "text": "short"}])

    async def connect(url, **kwargs):
        state.connects.append((url, kwargs))
        return Conn(state)

    def no_tokenizer(*_a, **_k):
        raise AssertionError("tokenizer loaded")

    def embed(texts):
        state.embeds.append(len(texts))
        return [[0.5, 0.25] for _ in texts]

    monkeypatch.setattr(mod, "asyncpg", types.SimpleNamespace(connect=connect))
    monkeypatch.setattr(mod, "DB_URL", FAKE_URL)
    monkeypatch.setattr(mod.tiktoken, "get_encoding", no_tokenizer)
    monkeypatch.setattr(mod, "embed_batch", embed)
    return mod, state


def test_import_creates_no_client_or_tokenizer(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    mod = _load("d13_reembed_oversize_chunks_import")
    assert mod.enc is None and mod.client is None


def test_help_has_no_side_effects(oversize, capsys):
    mod, state = oversize
    with pytest.raises(SystemExit) as exc:
        mod.cli(["--help"])
    assert exc.value.code == 0 and "--apply" in capsys.readouterr().out
    assert state.connects == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--apply", "--dry-run"]])
def test_default_is_read_only_and_offline(oversize, argv, capsys):
    mod, state = oversize
    assert mod.cli(argv) == 0
    assert state.connects == [(FAKE_URL, {"server_settings": READ_ONLY})]
    assert [s for s in state.sql if not s.startswith("SELECT")] == []
    assert state.embeds == []
    out = capsys.readouterr().out
    assert "Candidates: 3" in out and "DRY RUN" in out
    assert "changeme" not in out and "tester:***@127.0.0.1:1/koi_fake" in out


def test_apply_without_key_stops_before_connecting(oversize, capsys):
    mod, state = oversize
    assert mod.cli(["--apply"]) == 1
    assert state.connects == [] and "OPENAI_API_KEY" in capsys.readouterr().err


@pytest.mark.parametrize("argv,expected", [(["--apply"], 2), (["--apply", "--limit", "1"], 1),
                                           (["--apply", "--limit", "0"], 2)])
def test_apply_embeds_and_updates(oversize, monkeypatch, argv, expected):
    mod, state = oversize
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-used")
    monkeypatch.setattr(mod, "_encoder", lambda: Encoder())
    assert mod.cli(argv) == 0
    assert state.connects == [(FAKE_URL, {})]
    updates = [s for s in state.sql if s.startswith("UPDATE koi_memory_chunks SET embedding_3072")]
    assert len(updates) == expected and len(state.embeds) == expected


def test_default_limit_is_finite():
    mod = _load("d13_reembed_oversize_chunks_args")
    assert mod.parse_args([]).limit == 100
