"""Tests for OpenAI embedding provider + embed_jsonl_via_openai.py."""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root to sys.path so we can import api/ and scripts/
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests import koi_test_targets as _targets

# dry_run counts tokens with tiktoken's cl100k_base; skip rather than download it.
_needs_cl100k = _targets.requires_tiktoken(encodings=("cl100k_base",))


# ---------- OpenAIEmbeddingProvider tests ----------

def _mock_openai_response(dim: int):
    response = MagicMock()
    data = MagicMock()
    data.embedding = [0.1] * dim
    response.data = [data]
    response.usage = MagicMock(total_tokens=10, prompt_tokens=10)
    return response


def test_openai_v3_large_passes_dimensions():
    """text-embedding-3-large with dim=1024 must pass dimensions=1024 to OpenAI."""
    from api.embedding_provider import OpenAIEmbeddingProvider

    mock_client = MagicMock()
    mock_client.embeddings.create.return_value = _mock_openai_response(1024)

    with patch("openai.OpenAI", return_value=mock_client):
        p = OpenAIEmbeddingProvider(api_key="fake", model="text-embedding-3-large", dimension=1024)
        result = asyncio.run(p.embed("hello"))

    assert len(result) == 1024
    call_kwargs = mock_client.embeddings.create.call_args.kwargs
    assert call_kwargs["model"] == "text-embedding-3-large"
    assert call_kwargs["input"] == "hello"
    assert call_kwargs["dimensions"] == 1024


def test_openai_ada_does_not_pass_dimensions():
    """text-embedding-ada-002 must NOT pass dimensions (not supported by that model)."""
    from api.embedding_provider import OpenAIEmbeddingProvider

    mock_client = MagicMock()
    mock_client.embeddings.create.return_value = _mock_openai_response(1536)

    with patch("openai.OpenAI", return_value=mock_client):
        p = OpenAIEmbeddingProvider(api_key="fake", model="text-embedding-ada-002")
        asyncio.run(p.embed("hello"))

    call_kwargs = mock_client.embeddings.create.call_args.kwargs
    assert "dimensions" not in call_kwargs


def test_openai_batch_passes_dimensions_for_v3():
    """embed_batch must also pass dimensions for text-embedding-3-* models."""
    from api.embedding_provider import OpenAIEmbeddingProvider

    # Build a response object with 2 data entries
    response = MagicMock()
    entry1, entry2 = MagicMock(), MagicMock()
    entry1.embedding = [0.1] * 1024
    entry2.embedding = [0.2] * 1024
    response.data = [entry1, entry2]
    response.usage = MagicMock(total_tokens=20, prompt_tokens=20)

    mock_client = MagicMock()
    mock_client.embeddings.create.return_value = response

    with patch("openai.OpenAI", return_value=mock_client):
        p = OpenAIEmbeddingProvider(api_key="fake", model="text-embedding-3-small", dimension=1024)
        results = asyncio.run(p.embed_batch(["a", "b"]))

    assert len(results) == 2
    assert all(len(r) == 1024 for r in results)
    call_kwargs = mock_client.embeddings.create.call_args.kwargs
    assert call_kwargs["dimensions"] == 1024
    assert call_kwargs["input"] == ["a", "b"]


# ---------- embed_jsonl_via_openai.py tests ----------

@_needs_cl100k
def test_dry_run_estimates_cost_without_api_calls(tmp_path):
    """--dry-run must count tokens locally via tiktoken without hitting OpenAI."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "embed_jsonl_via_openai",
        ROOT / "scripts" / "embed_jsonl_via_openai.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Build input file
    inp = tmp_path / "in.jsonl"
    with open(inp, "w") as f:
        for i in range(3):
            f.write(json.dumps({"id": i, "text": "hello world" * 10}) + "\n")

    records = mod.read_jsonl(str(inp))
    assert len(records) == 3

    total_tokens, cost = mod.dry_run(records)
    assert total_tokens > 0
    # $0.13/1M tokens * small count = tiny cost
    assert cost < 0.01


def test_resume_skips_already_embedded_ids(tmp_path, monkeypatch):
    """Apply mode must skip ids already present in the output file."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "embed_jsonl_via_openai",
        ROOT / "scripts" / "embed_jsonl_via_openai.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    out = tmp_path / "out.jsonl"
    # Pre-populate output with ids 0, 1
    with open(out, "w") as f:
        f.write(json.dumps({"id": "0", "embedding": [0.0] * 1024}) + "\n")
        f.write(json.dumps({"id": "1", "embedding": [0.0] * 1024}) + "\n")

    existing = mod.read_existing_ids(str(out))
    assert existing == {"0", "1"}


def test_apply_batches_and_writes_output(tmp_path, monkeypatch):
    """Apply mode batches correctly and writes {id, embedding} lines."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "embed_jsonl_via_openai",
        ROOT / "scripts" / "embed_jsonl_via_openai.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Force smaller batch size for test
    monkeypatch.setattr(mod, "BATCH_SIZE", 2)

    # Build 5 records → 3 batches (2+2+1)
    records = [{"id": i, "text": f"text-{i}"} for i in range(5)]

    # Mock OpenAI client (apply_embed imports OpenAI from openai internally)
    def fake_create(*, model, input, dimensions):
        assert model == "text-embedding-3-large"
        assert dimensions == 1024
        response = MagicMock()
        response.data = [MagicMock(embedding=[float(j)] * 1024) for j in range(len(input))]
        response.usage = MagicMock(total_tokens=len(input) * 5)
        return response

    fake_client = MagicMock()
    fake_client.embeddings.create.side_effect = fake_create

    with patch("openai.OpenAI", return_value=fake_client):
        out = tmp_path / "out.jsonl"
        processed, tokens, cost = asyncio.run(
            mod.apply_embed(records, str(out), "fake-key", None, None)
        )

    assert processed == 5
    assert tokens == 25  # 5 records * 5 tokens each
    # Verify output file
    with open(out) as f:
        lines = [json.loads(l) for l in f if l.strip()]
    assert len(lines) == 5
    assert all(len(r["embedding"]) == 1024 for r in lines)
    assert {r["id"] for r in lines} == {"0", "1", "2", "3", "4"}
    # Batches: 5 records / 2 per batch = 3 API calls
    assert fake_client.embeddings.create.call_count == 3
