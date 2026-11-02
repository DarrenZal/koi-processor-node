"""Subscription transport for claim extraction and B8 contexts (api/claude_p.py).

Every test drives a FAKE `claude` binary written to tmp_path, so nothing here touches
the network, the subscription, or a database.
"""
import json
import os
import stat
import sys

import pytest

import api.claude_p as claude_p
import api.claim_extractor as claim_extractor
import api.contextual_retriever as cr


def _fake_claude(tmp_path, body: str):
    """Write an executable fake `claude` whose behaviour is the Python `body`.

    `body` sees `prompt` (stdin) and `args` (argv[1:]) and prints the envelope.
    """
    script = tmp_path / "claude"
    script.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys, time\n"
        "args = sys.argv[1:]\n"
        "prompt = sys.stdin.read()\n"
        f"{body}\n")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return str(script)


@pytest.fixture(autouse=True)
def _fresh_semaphore(monkeypatch):
    # The module semaphore binds to the first event loop that contends on it, and
    # pytest-asyncio gives each test its own loop.
    monkeypatch.setattr(claude_p, "_sem", None)


def _envelope(result, subtype="success", is_error=False):
    return json.dumps({"type": "result", "subtype": subtype, "is_error": is_error,
                       "result": result})


# ── envelope parsing ──────────────────────────────────────────────────────────

def test_parse_envelope_success():
    assert claude_p._parse_envelope(_envelope("hello")) == "hello"


@pytest.mark.parametrize("stdout", [
    "not json",
    _envelope("partial", subtype="error_max_turns"),
    _envelope("x", is_error=True),
    _envelope(""),
    _envelope("   "),
    json.dumps(["not", "a", "dict"]),
])
def test_parse_envelope_rejects_silent_failures(stdout):
    with pytest.raises(claude_p.ClaudePError):
        claude_p._parse_envelope(stdout)


# ── complete(): subprocess contract ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_complete_passes_prompt_on_stdin_and_strips_api_key(tmp_path, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-not-real")
    monkeypatch.setattr(claude_p, "CLAUDE_BIN", _fake_claude(tmp_path, (
        "leaked = 'ANTHROPIC_API_KEY' in os.environ\n"
        "print(json.dumps({'subtype': 'success', 'is_error': False, 'result': json.dumps({\n"
        "    'prompt': prompt, 'args': args, 'leaked': leaked, 'cwd': os.getcwd()})}))")))
    seen = json.loads(await claude_p.complete("the prompt", model="m-1", deadline_s=30))
    assert seen["prompt"] == "the prompt"
    assert seen["leaked"] is False
    assert seen["args"][seen["args"].index("--model") + 1] == "m-1"
    assert seen["args"][seen["args"].index("--tools") + 1] == ""
    assert "--no-session-persistence" in seen["args"]
    assert "the prompt" not in seen["args"]
    assert os.path.realpath(seen["cwd"]) == os.path.realpath("/tmp")


@pytest.mark.asyncio
async def test_complete_raises_after_nonzero_exits(tmp_path, monkeypatch):
    monkeypatch.setattr(claude_p, "CLAUDE_BIN", _fake_claude(tmp_path, (
        "sys.stderr.write('rate limited'); sys.exit(1)")))
    with pytest.raises(claude_p.ClaudePError, match="rate limited"):
        await claude_p.complete("p", model="m", deadline_s=30, max_attempts=2)


@pytest.mark.asyncio
async def test_complete_honours_deadline(tmp_path, monkeypatch):
    monkeypatch.setattr(claude_p, "CLAUDE_BIN", _fake_claude(tmp_path, "time.sleep(30)"))
    with pytest.raises(claude_p.ClaudePError, match="timeout"):
        await claude_p.complete("p", model="m", deadline_s=2)


@pytest.mark.asyncio
async def test_complete_missing_binary(monkeypatch):
    monkeypatch.setattr(claude_p, "CLAUDE_BIN", "/nonexistent/claude")
    with pytest.raises(claude_p.ClaudePError, match="not found"):
        await claude_p.complete("p", model="m")


# ── claim extractor: one transport, no paid fallback ─────────────────────────

@pytest.mark.asyncio
async def test_claim_extractor_claude_p_failure_does_not_fall_back(monkeypatch):
    async def boom(*a, **k):
        raise claude_p.ClaudePError("down")

    async def paid(*a, **k):
        raise AssertionError("a paid transport was called")

    monkeypatch.setattr(claim_extractor, "CLAIM_EXTRACTOR_TRANSPORT", "claude_p")
    monkeypatch.setattr(claude_p, "complete", boom)
    monkeypatch.setattr(claim_extractor, "_openai_complete", paid)
    monkeypatch.setattr(claim_extractor, "_anthropic_api_complete", paid)
    assert await claim_extractor._llm_complete("p") is None


@pytest.mark.asyncio
async def test_claim_extractor_claude_p_success(monkeypatch):
    async def ok(prompt, *, model, deadline_s):
        assert model == claim_extractor.CLAIM_EXTRACTOR_MODEL
        return "[]"

    monkeypatch.setattr(claim_extractor, "CLAIM_EXTRACTOR_TRANSPORT", "claude_p")
    monkeypatch.setattr(claude_p, "complete", ok)
    assert await claim_extractor._llm_complete("p") == "[]"


@pytest.mark.asyncio
async def test_claim_extractor_openai_is_opt_in(monkeypatch):
    # Control for the no-fallback test: the openai path is reachable when chosen.
    async def paid(prompt, *, max_tokens):
        return "from-openai"

    monkeypatch.setattr(claim_extractor, "CLAIM_EXTRACTOR_TRANSPORT", "openai")
    monkeypatch.setattr(claim_extractor, "_openai_complete", paid)
    assert await claim_extractor._llm_complete("p") == "from-openai"


# ── B8 batch contexts ────────────────────────────────────────────────────────

def test_parse_batch_contexts_maps_ids_and_tolerates_fences():
    text = '```json\n[{"id": 2, "context": " second "}, {"id": 1, "context": "first"}]\n```'
    assert cr._parse_batch_contexts(text, 3) == ["first", "second", ""]


@pytest.mark.parametrize("text", [
    "no array here",
    "[not json]",
    '[{"id": "x", "context": "bad id"}, {"id": 9, "context": "out of range"}, "str"]',
])
def test_parse_batch_contexts_degrades_to_empty(text):
    assert cr._parse_batch_contexts(text, 2) == ["", ""]


@pytest.mark.asyncio
async def test_b8_claude_p_batches_document_once_per_batch(tmp_path, monkeypatch):
    calls = tmp_path / "calls.log"
    monkeypatch.setattr(claude_p, "CLAUDE_BIN", _fake_claude(tmp_path, (
        "import re\n"
        f"open({str(calls)!r}, 'a').write('call\\n')\n"
        "assert prompt.count('<document>') == 1\n"
        "ids = [int(i) for i in re.findall(r'<chunk id=\"(\\d+)\">', prompt)]\n"
        "out = [{'id': i, 'context': 'ctx-' + str(i)} for i in ids]\n"
        "print(json.dumps({'subtype': 'success', 'is_error': False, 'result': json.dumps(out)}))")))
    monkeypatch.setattr(cr, "B8_TRANSPORT", "claude_p")
    monkeypatch.setattr(cr, "B8_BATCH_SIZE", 2)
    chunks = [{"text": f"chunk {i}"} for i in range(5)]
    contexts = await cr.generate_contexts_for_document("doc text", chunks, "Title")
    assert contexts == ["ctx-1", "ctx-2", "ctx-1", "ctx-2", "ctx-1"]
    assert calls.read_text().count("call") == 3


@pytest.mark.asyncio
async def test_b8_claude_p_failure_yields_empty_contexts(tmp_path, monkeypatch):
    monkeypatch.setattr(claude_p, "CLAUDE_BIN", _fake_claude(tmp_path, "sys.exit(2)"))
    monkeypatch.setattr(cr, "B8_TRANSPORT", "claude_p")
    contexts = await cr.generate_contexts_for_document("doc", [{"text": "a"}, {"text": "b"}])
    assert contexts == ["", ""]
