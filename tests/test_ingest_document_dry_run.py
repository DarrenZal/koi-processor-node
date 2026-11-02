"""scripts/ingest_document.py --dry-run chunks and reports, and nothing else.

A dry run must not embed (which sends the document's text to the embedding provider),
open a database pool, or make any network call. Each test replaces the embedder, the
pool factory and socket connects with traps that record a call, and fails on any.
"""
import asyncio
import socket
import sys

import pytest

from scripts import ingest_document as ingest

ARGS = dict(slug=None, name=None, source_url=None, retrieval_method=None, group_id="personal")
TEXT = "# A private note\n\n" + "\n\n".join(
    f"Paragraph {i}: text that must stay on this machine during a dry run." * 4 for i in range(40))


class TrapEmbedder:
    def __init__(self, *args, **kwargs):
        self.calls = 0

    async def embed(self, text):
        self.calls += 1
        raise AssertionError("dry run called the embedder")

    async def embed_batch(self, texts):
        self.calls += 1
        raise AssertionError("dry run called the embedder")


@pytest.fixture
def traps(monkeypatch, tmp_path):
    calls = {"embedder": 0, "pool": 0, "connect": 0}

    class CountingEmbedder(TrapEmbedder):
        def __init__(self, *args, **kwargs):
            calls["embedder"] += 1
            super().__init__()

    async def no_pool(*args, **kwargs):
        calls["pool"] += 1
        raise AssertionError("dry run opened a database pool")

    real_connect = socket.socket.connect

    def no_connect(self, address):
        if self.family in (socket.AF_INET, socket.AF_INET6):
            calls["connect"] += 1
            raise AssertionError(f"dry run connected to {address}")
        return real_connect(self, address)

    monkeypatch.setattr(ingest, "OpenAIEmbeddingProvider", CountingEmbedder)
    monkeypatch.setattr(ingest.asyncpg, "create_pool", no_pool)
    monkeypatch.setattr(socket.socket, "connect", no_connect)
    monkeypatch.setenv("INGEST_SOURCE_ROOT", str(tmp_path))
    source = tmp_path / "note.md"
    source.write_text(TEXT)
    return calls, source


def test_rag_core_dry_run_never_embeds():
    embedder = TrapEmbedder()
    chunker = ingest.TextChunker(chunk_size=ingest.CHUNK_SIZE, chunk_overlap=ingest.CHUNK_OVERLAP)
    for fn, first in ((ingest.ingest_document_rag, None), (ingest.ingest_document_rag_conn, None)):
        out = asyncio.run(fn(first, document_rid="document:x", markdown=TEXT, source_meta={},
                             embedder=embedder, chunker=chunker, dry_run=True))
        assert out["chunks_total"] > 1 and out["chunks_written"] == 0
    assert embedder.calls == 0


@pytest.mark.parametrize("tier", ["rag", "standard", "thorough"])
def test_ingest_path_dry_run_makes_no_call(traps, monkeypatch, tier):
    calls, source = traps
    monkeypatch.setattr(ingest, "OPENAI_API_KEY", "")   # a dry run needs no key either
    out = asyncio.run(ingest.ingest_path(source_path=str(source), tier=tier, dry_run=True, **ARGS))
    assert out["rag"]["chunks_total"] > 1 and out["rag"]["chunks_written"] == 0
    assert "extract" not in out and "claims" not in out
    assert out["gate_evidence"]["chunks_total"] == out["rag"]["chunks_total"]
    assert calls == {"embedder": 0, "pool": 0, "connect": 0}


def test_cli_dry_run_makes_no_call(traps, monkeypatch, capsys):
    calls, source = traps
    monkeypatch.setattr(ingest, "OPENAI_API_KEY", "sk-test-not-used")
    monkeypatch.setattr(sys, "argv", ["ingest_document.py", "--source-path", str(source),
                                      "--tier", "rag", "--dry-run"])
    ingest.main()
    assert "dry run: nothing embedded or written" in capsys.readouterr().out
    assert calls == {"embedder": 0, "pool": 0, "connect": 0}


def test_traps_fire_without_dry_run(traps, monkeypatch):
    """Positive control: the same traps catch a real (non-dry) run."""
    calls, source = traps
    monkeypatch.setattr(ingest, "OPENAI_API_KEY", "sk-test-not-used")
    with pytest.raises(AssertionError, match="database pool"):
        asyncio.run(ingest.ingest_path(source_path=str(source), tier="rag", dry_run=False, **ARGS))
    assert calls["embedder"] == 1 and calls["pool"] == 1
