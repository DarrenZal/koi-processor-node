#!/usr/bin/env python3
"""
Document deep-extraction, chunk-wise + cross-window merge.
  standard tier (Phase 1): entities + facts (v1 prompt/schema).
  thorough tier (Phase 3): + discourse moves (v2 prompt/schema) written to the
    generalized session_discourse_moves table (source_type='document', migration 103).

Part of the unified thorough content-ingestion plan
(an internal planning document).

Reads a document's RAG chunks (koi_memory_chunks, written by ingest_document.py),
packs them into windows (well under the claude -p char cap), runs the document
extractor prompt per window (transport + model are BOTH env-tunable — see
DOC_EXTRACTOR_TRANSPORT / DOC_EXTRACTOR_MODEL below; the old "Sonnet, forced for
extraction quality" wording was an unexamined default, and Haiku matched
fact-density at ~3.6x the speed in measurement — issue #37), then MERGES entities
(type-priority coercion) + facts (dedup) across
windows deterministically. Facts are written through POST /knowledge/episodes —
NOT the session raw-INSERT — so they land in fact_embedding_3072 and get 3-tier
resolution + cosine>0.95 dedup + type-mismatch detection. Type-mismatches are
recorded in document_extraction_item_errors and filed as /tasks/ingest cleanups
(report-and-task, never auto-merge across the resolution guard).

Idempotent + resumable via document_ingestion_log + document_window_extractions
(migration 102). Re-running skips already-extracted windows (cached raw_json).

Usage:
    set -a; source config/personal.env; set +a
    python scripts/extract_deep_documents.py --slug biohubs-whitepaper --tier standard
    python scripts/extract_deep_documents.py --document-rid document:<sha> --group-id <field>
"""

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import httpx
from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).parent.parent))
from api.provider_http import provider_async_client  # noqa: E402
from api import ingest_identity as ident  # noqa: E402
from api import document_extraction_contract as contract  # noqa: E402
from api import extraction_merge as _merge  # noqa: E402
from api.extraction_quality import assess_extraction  # noqa: E402
from api.resolution_primitives import (  # noqa: E402
    normalize_entity_text, normalize_alias,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent.parent

# ── Config ───────────────────────────────────────────────────────────────────────
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
# Pin deterministically to the CO-LOCATED KOI backend (:8351 on whatever host the
# pipeline runs). Use a dedicated var so it does NOT inherit personal.env's general
# KOI_BASE_URL, which may point at another node that the ingest host cannot reach.
KOI_BASE_URL = os.getenv("DOC_INGEST_KOI_URL", "http://localhost:8351")
CLAUDE_P_MODEL = os.getenv("CLAUDE_P_MODEL", "claude-sonnet-4-6")
# /knowledge/episodes authenticates with the CLAIMS service token (server-side
# require_service_auth checks KOI_CLAIMS_SERVICE_TOKEN). Prefer it; fall back to the
# legacy INGEST var only for envs that haven't split the two tokens. (:8351 episodes-
# gate readiness — sending the token is a harmless no-op until the gate lands.)
KOI_EPISODES_SERVICE_TOKEN = os.getenv("KOI_CLAIMS_SERVICE_TOKEN") or os.getenv("KOI_INGEST_SERVICE_TOKEN")
if not os.getenv("KOI_CLAIMS_SERVICE_TOKEN") and os.getenv("KOI_INGEST_SERVICE_TOKEN"):
    logger.warning(
        "KOI_CLAIMS_SERVICE_TOKEN unset — /knowledge/episodes will be sent with the legacy "
        "INGEST token, which the :8351 auth gate REJECTS (401). Set KOI_CLAIMS_SERVICE_TOKEN."
    )

# Tier selects the extractor contract: standard = entities+facts (v1); thorough =
# entities+facts+discourse (v2). Env overrides win if set.
PROMPT_V1 = Path(os.getenv(
    "DOC_EXTRACTOR_PROMPT_FILE", str(REPO_ROOT / "scripts/prompts/deep_extraction_doc_v1.md")))
SCHEMA_V1 = Path(os.getenv(
    "DOC_EXTRACTOR_SCHEMA_FILE", str(REPO_ROOT / "scripts/schemas/deep_extraction_doc_v1.schema.json")))
PROMPT_V2 = Path(os.getenv(
    "DOC_EXTRACTOR_PROMPT_V2_FILE", str(REPO_ROOT / "scripts/prompts/deep_extraction_doc_v2.md")))
SCHEMA_V2 = Path(os.getenv(
    "DOC_EXTRACTOR_SCHEMA_V2_FILE", str(REPO_ROOT / "scripts/schemas/deep_extraction_doc_v2.schema.json")))

# Deterministic namespace for document discourse-move ids (uuid5 → idempotent upsert).
DISCOURSE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "discourse.document.koi")


def prompt_schema_for_tier(tier: str) -> Tuple[Path, Path]:
    """thorough → v2 (adds discourse); standard → v1 (entities+facts)."""
    return (PROMPT_V2, SCHEMA_V2) if tier == "thorough" else (PROMPT_V1, SCHEMA_V1)

WINDOW_CHARS = int(os.getenv("DOC_WINDOW_CHARS", "45000"))   # proper window size — the Anthropic API is fast,
                                                             # so no need to shrink windows to beat a CLI timeout
WINDOW_OVERLAP_CHUNKS = int(os.getenv("DOC_WINDOW_OVERLAP_CHUNKS", "2"))
MAX_WINDOWS = int(os.getenv("DOC_MAX_WINDOWS", "12"))        # per-invocation budget cap (plan §Q3)
# Was the cap CHOSEN by the caller, or is it just the default? A default 12 x 45k = 540k
# chars, which SILENTLY truncated every book-length source (2026-07-31: the 227-post blog
# lost 8 of 86 windows and still passed every gate floor). An explicit cap is a real cost
# guard and is honoured; an unset one now auto-raises instead of quietly dropping the tail.
MAX_WINDOWS_EXPLICIT = "DOC_MAX_WINDOWS" in os.environ
# Semantic dedup threshold: the exact-triple sweep misses PARAPHRASES (a re-extraction's
# fresh phrasings resolve to distinct triples). After it, retract the later of any
# same-subject + same-predicate fact pair whose fact_text embeddings exceed this cosine
# — conservative (predicate match + high bar) so re-extractions self-converge without
# false-retracting genuinely-distinct facts.
SEMANTIC_DEDUP_THRESHOLD = float(os.getenv("DOC_SEMANTIC_DEDUP_THRESHOLD", "0.95"))

# Flag 2 doc->entity links (see the call site). Each uri is resolved to its live
# survivor BEFORE the GROUP BY, with the same rule as migration 132's trigger (follow
# merged_into up to 10 hops; a broken or over-long chain keeps the original). Otherwise
# two fact URIs that share a survivor become two rows the trigger folds onto one key, and
# Postgres rejects the statement ("ON CONFLICT DO UPDATE command cannot affect row a
# second time"). Episodes are reused, so a re-extraction re-reads old tombstone facts.
# $1 episode_id, $2 document_rid, $3 context.
FLAG2_DOC_ENTITY_LINKS_SQL = """
WITH RECURSIVE u AS (
  SELECT subject_uri AS uri, count(*) n FROM knowledge_facts
    WHERE episode_id = $1 AND valid_to IS NULL GROUP BY subject_uri
  UNION ALL
  SELECT object_uri, count(*) FROM knowledge_facts
    WHERE episode_id = $1 AND object_uri IS NOT NULL AND valid_to IS NULL GROUP BY object_uri
), chain AS (
  SELECT d.uri AS orig, d.uri AS cur, er.merged_into AS nxt, 0 AS hops
    FROM (SELECT DISTINCT uri FROM u WHERE uri IS NOT NULL) d
    JOIN entity_registry er ON er.fuseki_uri = d.uri
  UNION ALL
  SELECT c.orig, c.nxt, er.merged_into, c.hops + 1
    FROM chain c JOIN entity_registry er ON er.fuseki_uri = c.nxt
    WHERE c.nxt IS NOT NULL AND c.hops < 10
), live AS (
  SELECT orig, cur FROM chain WHERE nxt IS NULL
)
INSERT INTO document_entity_links (document_rid, entity_uri, mention_count, context)
SELECT $2, COALESCE(live.cur, u.uri), sum(u.n)::int, $3
  FROM u LEFT JOIN live ON live.orig = u.uri
 WHERE u.uri IS NOT NULL GROUP BY COALESCE(live.cur, u.uri)
ON CONFLICT (document_rid, entity_uri) DO UPDATE SET mention_count = EXCLUDED.mention_count
"""

# Per-window extraction transport — selected by DOC_EXTRACTOR_TRANSPORT:
#   'claude_p' (DEFAULT): run each window through the `claude -p` CLI on the Claude Code
#       SUBSCRIPTION — zero pay-per-token cost. Slower (harness startup, no server-side
#       batching) but $0 marginal. We strip ANTHROPIC_API_KEY from the child env so the
#       CLI uses OAuth, and pass --strict-mcp-config + --setting-sources '' to shed the
#       MCP/CLAUDE.md context (~7x fewer overhead tokens per call in measurement).
#   'api': the direct Anthropic Messages API — ~4-5x faster, but BILLS per-token against
#       ANTHROPIC_API_KEY.
#   'openai': ANY OpenAI-compatible /v1/chat/completions endpoint — bring your own model
#       (public OpenAI, a self-hosted vLLM/Ollama, a provider-hosted open model, etc.).
#       Configure it entirely via DOC_EXTRACTOR_OPENAI_* env vars (below); no endpoint or
#       key is baked in, so forks point it at their own infra.
# The default is 'claude_p' and should stay that way — 'openai'/'api' are opt-in per run.
DOC_EXTRACTOR_TRANSPORT = os.getenv("DOC_EXTRACTOR_TRANSPORT", "claude_p").strip().lower()
# Resolve the claude binary — the launchd job runs with a minimal PATH that may lack
# ~/.local/bin, so shutil.which() can miss it; fall back to the known install location.
CLAUDE_BIN = (os.getenv("CLAUDE_BIN") or shutil.which("claude")
              or os.path.expanduser("~/.local/bin/claude"))
ROUTE_USED = {"api": "anthropic_api", "openai": "openai_compat"}.get(
    DOC_EXTRACTOR_TRANSPORT, "claude_p_cli")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = os.getenv("DOC_EXTRACTOR_MODEL", CLAUDE_P_MODEL)
ANTHROPIC_MAX_TOKENS = int(os.getenv("DOC_EXTRACTOR_MAX_TOKENS", "24000"))
ANTHROPIC_TIMEOUT = int(os.getenv("DOC_EXTRACTOR_TIMEOUT", "300"))
# 504/524 added after a live document (`f641b77d...`, a run behind a hosted Qwen endpoint) hit a
# mix of 503/504/524 across repeated attempts at every window size tried — 504 fell
# through this set entirely and crashed the whole extraction with an unhandled
# HTTPStatusError instead of retrying; 524 is Cloudflare's upstream-timeout code, seen
# fronting the same class of gateway.
ANTHROPIC_RETRYABLE = {429, 500, 502, 503, 504, 524, 529}

# 'openai' transport config — bring-your-own OpenAI-compatible model. Defaults are
# GENERIC (public OpenAI) so a fork works with just an OPENAI_API_KEY; point BASE_URL
# at any compatible server (vLLM/Ollama/provider) to use a different model. Nothing
# operator-specific is committed here — set these in your (gitignored) config/env.
#   DOC_EXTRACTOR_OPENAI_NO_THINK: for reasoning models (Qwen3, etc.) served by vLLM,
#     set =1 to send chat_template_kwargs.enable_thinking=false — otherwise the model
#     spends its budget reasoning and returns content=null. Default 0 because the param
#     is a vLLM extension that the public OpenAI API rejects. Enable it for reasoning
#     models on a compatible server.
OPENAI_BASE_URL = os.getenv("DOC_EXTRACTOR_OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = os.getenv("DOC_EXTRACTOR_OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("DOC_EXTRACTOR_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY", "")
OPENAI_MAX_TOKENS = int(os.getenv("DOC_EXTRACTOR_OPENAI_MAX_TOKENS", "12000"))
OPENAI_NO_THINK = os.getenv("DOC_EXTRACTOR_OPENAI_NO_THINK", "0").strip().lower() in ("1", "true", "yes")

# Type-priority coercion (plan §23): highest wins on cross-window conflict.
#
# NO LONGER WRITTEN HERE. Until 2026-09-14 this dict was one of six hand-maintained
# copies of the extraction type vocabulary, and it was missing `Document` and
# `Event` — so `.get(etype, 0)` silently ranked them BELOW `Concept`, the floor.
# Issue #68. The vocabulary is single-sourced and drift-tested now; see
# api/document_extraction_contract.py + tests/test_document_extraction_type_contract.py.
TYPE_PRIORITY = contract.TYPE_PRIORITY


class ExtractionError(RuntimeError):
    def __init__(self, reason: str, detail: str, terminal: bool = False):
        super().__init__(f"{reason}: {detail}")
        self.reason, self.detail, self.terminal = reason, detail, terminal


# The merge key IS the identity key. A private normalizer used to live here
# (lower + whitespace only) and disagreed with `normalize_entity_text` on `-`/`_`,
# so `omni-mapping` / `omni mapping` survived the merge as two typed records and the
# identity gate then raised on a conflict the merge existed to fold (review M6).
# Single-sourced in api/extraction_merge.py; re-exported here under the old name so
# every existing caller and test still reaches the one production function.
_norm = _merge.merge_key


def discourse_move_id_key(title: Optional[str]) -> str:
    """The title key hashed into a discourse move's uuid5 id — and NOTHING else.

    lower + strip + collapse whitespace: byte-for-byte the normalizer the extractor
    used from migration 103 until `cdc445c`, when `_norm` above was rebound to the
    entity normalizer and this hash input silently changed with it. `normalize_entity_text`
    also folds `-`/`_` to a space and strips a leading `@`, so every stored move whose
    title carried one — 5,578 rows on 1,169 documents, 12 of them on the three
    michaelgarfield replay targets — stopped matching its own id, and a replay would
    have INSERTed a duplicate beside each row it was meant to repair (ON CONFLICT (id)
    only ever fires on an equal id). A persisted-id derivation is a contract with the
    rows already written; it does not follow the identity key. Entity merging and
    endpoint identity keep `_norm` / `normalize_entity_text` (M6). Pinned by literal
    UUIDs in tests/test_discourse_move_ids.py.
    """
    return re.sub(r"\s+", " ", (title or "").strip().lower())


def discourse_move_id(document_rid: str, move_type: str, title: str, chunk_start: int) -> uuid.UUID:
    """Deterministic id of one document discourse move. chunk_start (global RAG-chunk
    index) keeps two same-type/same-title moves at different locations distinct
    (plan §163). Idempotent upsert depends on this never changing for a stored row."""
    return uuid.uuid5(DISCOURSE_NAMESPACE,
                      f"{document_rid}:{move_type}:{discourse_move_id_key(title)}:{chunk_start}")


def compute_next_retry(attempts: int) -> Optional[datetime]:
    """Exponential backoff with a terminal cap at 3 attempts (mirrors the session path)."""
    if attempts >= 3:
        return None
    return datetime.now(timezone.utc) + timedelta(minutes=15 * (2 ** attempts))


# ── claude -p + validation (cloned from extract_deep_sessions.py) ─────────────────

async def _call_anthropic(prompt: str, http: httpx.AsyncClient, *, model: str,
                          max_tokens: int = ANTHROPIC_MAX_TOKENS, timeout: int = ANTHROPIC_TIMEOUT,
                          max_retries: int = 3) -> str:
    """Direct Anthropic Messages API call — the document-extraction transport.

    Same model/prompt/schema as the session path's claude -p; just a faster, headless
    transport. Retries transient errors (429/5xx/connection) with backoff so a blip
    never silently drops a window — on exhaustion (or max_tokens truncation) it RAISES,
    marking the window failed/resumable rather than producing a partial merge.
    """
    if not ANTHROPIC_API_KEY:
        raise ExtractionError("no_api_key", "ANTHROPIC_API_KEY not set (document extraction transport)",
                              terminal=True)
    headers = {"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
               "content-type": "application/json"}
    body = {"model": model, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}
    last = None
    for attempt in range(max_retries):
        try:
            r = await http.post("https://api.anthropic.com/v1/messages",
                                headers=headers, json=body, timeout=timeout)
            if r.status_code in ANTHROPIC_RETRYABLE:
                last = f"http {r.status_code}: {r.text[:400]}"
                await asyncio.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("stop_reason") == "max_tokens":
                raise ExtractionError("extract_truncated",
                                      f"hit max_tokens={max_tokens}; raise DOC_EXTRACTOR_MAX_TOKENS "
                                      f"or lower DOC_WINDOW_CHARS")
            text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
            if not text.strip():
                raise ExtractionError("empty_completion", f"no text content: {str(data)[:400]}")
            return text
        except httpx.HTTPStatusError as e:
            # A non-retryable provider status (400/401/403/404/413/422) reaches here
            # from raise_for_status(). It is NOT an ExtractionError, so before this
            # arm existed it escaped EVERY layer above: no transport fallback
            # (_extract_window catches only ExtractionError), no repair loop, and —
            # the expensive one — no per-window dead-lettering, so a single bad key
            # or oversized window aborted the WHOLE document. That defeats the
            # per-window isolation this file's own #40 comment promises.
            # Re-raised as extract_http_error, which IS in TRANSPORT_FALLBACK_REASONS,
            # so a transport-specific rejection can still try the next transport.
            raise ExtractionError(
                "extract_http_error",
                f"anthropic api returned HTTP {e.response.status_code}: "
                f"{e.response.text[:400]}") from e
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.WriteError) as e:
            last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2 ** attempt)
    raise ExtractionError("extract_http_error", f"anthropic api failed after {max_retries} attempts: {last}")


async def _call_claude_p(prompt: str, *, model: str, timeout: int = ANTHROPIC_TIMEOUT,
                         max_retries: int = 3) -> str:
    """Subscription transport: run the extractor prompt through the `claude -p` CLI.

    Bills the Claude Code SUBSCRIPTION, not ANTHROPIC_API_KEY. Same model/prompt/schema
    as the API path — just a headless CLI transport with $0 marginal per-token cost.

    Two things make this correct-and-cheap: (1) we strip ANTHROPIC_API_KEY /
    ANTHROPIC_AUTH_TOKEN from the child env so `claude` authenticates via the OAuth
    subscription instead of falling back to pay-per-token API billing (the shell wrapper
    exports the key via `source personal.env`, so a naive child would inherit it); and
    (2) --strict-mcp-config (no MCP servers) + --setting-sources '' + a neutral cwd shed
    the MCP tool schemas and CLAUDE.md, cutting per-call overhead tokens ~7x. Retries
    transient CLI/API-status errors with backoff; raises (resumable) on exhaustion.
    """
    if not CLAUDE_BIN or not os.path.exists(CLAUDE_BIN):
        raise ExtractionError("no_claude_bin",
                              f"claude CLI not found (CLAUDE_BIN={CLAUDE_BIN!r}); set CLAUDE_BIN "
                              f"or add ~/.local/bin to PATH", terminal=True)
    # Force subscription auth — a set ANTHROPIC_API_KEY would make `claude` bill the API.
    child_env = {k: v for k, v in os.environ.items()
                 if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")}
    cmd = [CLAUDE_BIN, "-p", "--output-format", "json", "--model", model,
           "--strict-mcp-config", "--setting-sources", ""]
    last = None
    for attempt in range(max_retries):
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, env=child_env, cwd="/tmp")
        except OSError as e:
            raise ExtractionError("no_claude_bin", f"failed to spawn {CLAUDE_BIN}: {e}", terminal=True)
        try:
            out, err = await asyncio.wait_for(proc.communicate(prompt.encode()), timeout=timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            last = f"timeout after {timeout}s"
            await asyncio.sleep(2 ** attempt)
            continue
        if proc.returncode != 0:
            last = f"exit {proc.returncode}: {err.decode(errors='replace')[:400]}"
            await asyncio.sleep(2 ** attempt)
            continue
        try:
            env = json.loads(out.decode())
        except json.JSONDecodeError:
            last = f"non-JSON stdout: {out.decode(errors='replace')[:400]}"
            await asyncio.sleep(2 ** attempt)
            continue
        if env.get("is_error") or env.get("subtype") != "success":
            last = f"claude -p reported error: {str(env)[:400]}"
            await asyncio.sleep(2 ** attempt)
            continue
        text = env.get("result") or ""
        if not text.strip():
            raise ExtractionError("empty_completion", f"claude -p empty result: {str(env)[:400]}")
        return text
    raise ExtractionError("claude_p_error", f"claude -p failed after {max_retries} attempts: {last}")


async def _call_openai(prompt: str, http: httpx.AsyncClient, *,
                       timeout: int = ANTHROPIC_TIMEOUT, max_retries: int = 3,
                       temperature: float = 0.0) -> str:
    """OpenAI-compatible /v1/chat/completions transport (bring-your-own model).

    Same prompt/schema as the other transports. For reasoning models we disable the
    think phase (OPENAI_NO_THINK) — otherwise the model spends its budget in a
    `reasoning`/`<think>` channel and returns `content: null`. parse_and_validate
    downstream strips any residual <think> block and fences. Retries transient
    errors with backoff; raises (resumable) on exhaustion. `temperature` is bumped
    by the repair loop so a re-ask is not deterministically identical.
    """
    headers = {"content-type": "application/json"}
    if OPENAI_API_KEY:
        headers["authorization"] = f"Bearer {OPENAI_API_KEY}"
    body: Dict[str, Any] = {
        "model": OPENAI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": OPENAI_MAX_TOKENS,
        "temperature": temperature,
        "stream": False,
    }
    if OPENAI_NO_THINK:
        body["chat_template_kwargs"] = {"enable_thinking": False}
    url = OPENAI_BASE_URL.rstrip("/") + "/chat/completions"
    last = None
    for attempt in range(max_retries):
        try:
            r = await http.post(url, headers=headers, json=body, timeout=timeout)
            if r.status_code in ANTHROPIC_RETRYABLE:
                last = f"http {r.status_code}: {r.text[:400]}"
                await asyncio.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            data = r.json()
            choice = (data.get("choices") or [{}])[0]
            msg = choice.get("message") or {}
            text = msg.get("content") or msg.get("reasoning") or ""
            # Real truncation almost always returns PARTIAL text, not empty — the old
            # `and not text.strip()` guard only caught the rare empty case and let
            # truncated-mid-object JSON fall through to parse_and_validate as a
            # confusing generic extract_parse_error. Matches _call_anthropic's
            # stop_reason == "max_tokens" check above, which has no such gate.
            if choice.get("finish_reason") == "length":
                raise ExtractionError("extract_truncated",
                                      f"hit max_tokens={OPENAI_MAX_TOKENS}; raise "
                                      f"DOC_EXTRACTOR_OPENAI_MAX_TOKENS or lower DOC_WINDOW_CHARS")
            if not text.strip():
                raise ExtractionError("empty_completion", f"no content: {str(data)[:400]}")
            return text
        except httpx.HTTPStatusError as e:
            # Same escape as the anthropic transport above — see that comment.
            raise ExtractionError(
                "extract_http_error",
                f"openai-compatible endpoint returned HTTP {e.response.status_code}: "
                f"{e.response.text[:400]}") from e
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError, httpx.WriteError) as e:
            last = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2 ** attempt)
    raise ExtractionError("extract_http_error", f"openai api failed after {max_retries} attempts: {last}")


# Transport fallback chain (#34). A TRANSPORT-level failure — the CLI hung past its
# timeout, the provider 5xx'd, the key is missing — is precisely when you want the next
# transport, not an aborted run. #34 describes a fallback loop that excluded
# `extract_timeout`; that loop no longer exists (there was NO fallback at all), so this
# implements the behaviour the issue actually asked for.
#
# Empty by default: single-transport behaviour is unchanged unless an operator opts in,
# because the transports are not interchangeable in cost or quality (claude_p is the $0
# subscription, `api` bills per token, `openai` may point at a different model entirely).
#   e.g. DOC_EXTRACTOR_TRANSPORT_FALLBACK=openai
DOC_EXTRACTOR_TRANSPORT_FALLBACK = [
    x.strip().lower() for x in os.getenv("DOC_EXTRACTOR_TRANSPORT_FALLBACK", "").split(",")
    if x.strip()
]

# Reasons that mean "this TRANSPORT is unusable right now" — worth trying the next one.
# Deliberately EXCLUDES the content-level failures (extract_parse_error, empty_completion,
# extract_truncated): those mean the model answered and the answer was wrong, so another
# transport would likely answer wrong too, and the repair loop + per-window dead-lettering
# already handle them. Including them would turn one bad window into N provider calls.
TRANSPORT_FALLBACK_REASONS = {
    "claude_p_error",     # includes the timeout path — the exact case #34 was filed for
    "extract_http_error",
    "no_api_key",
    "no_claude_bin",
}


# Producer identity per transport (issue #64). The model a transport ACTUALLY uses is
# not always the one the caller passed: `_call_openai` ignores its `model` argument and
# sends OPENAI_MODEL. Deriving the receipt from the same constants the call bodies read
# is what keeps the recorded producer honest — a hand-maintained mapping would drift
# from the call sites the first time one of them changed.
TRANSPORT_PRODUCERS = {
    "openai": {"provider": "openai_compatible", "model": lambda m: OPENAI_MODEL},
    "api": {"provider": "anthropic", "model": lambda m: m},
    "claude_p": {"provider": "claude_subscription", "model": lambda m: m},
}


def _producer_for(transport: str, model: str) -> dict:
    spec = TRANSPORT_PRODUCERS.get(transport, TRANSPORT_PRODUCERS["claude_p"])
    out = {
        "transport": transport,
        "provider": spec["provider"],
        "model": spec["model"](model),
        "route": {"api": "anthropic_api", "openai": "openai_compat"}.get(transport, "claude_p_cli"),
    }
    if transport == "openai":
        # The HOST only. The key is never recorded, and the full URL can carry one in
        # a query string on some gateways.
        try:
            from urllib.parse import urlsplit
            out["endpoint_host"] = urlsplit(OPENAI_BASE_URL).netloc
        except Exception:  # noqa: BLE001 — provenance must never break extraction
            out["endpoint_host"] = None
    return out


async def _dispatch_transport(name: str, prompt: str, http: httpx.AsyncClient, *,
                              model: str, temperature: float) -> str:
    if name == "openai":
        return await _call_openai(prompt, http, temperature=temperature)
    if name == "api":
        return await _call_anthropic(prompt, http, model=model)
    return await _call_claude_p(prompt, model=model)


async def _extract_window(prompt: str, http: httpx.AsyncClient, *, model: str,
                          temperature: float = 0.0) -> Tuple[str, dict]:
    """Dispatch a window extraction, degrading through the configured fallback chain.

    Returns (completion_text, producer_receipt). Returning the receipt is the whole
    point of the change (issue #64): this function is the ONLY place that knows which
    transport actually served the window, and it used to throw that away. `ROUTE_USED`
    — the module constant the caller persisted instead — is derived from the
    CONFIGURED transport, so with DOC_EXTRACTOR_TRANSPORT_FALLBACK set, a window
    served by the fallback was recorded as having used the primary. That is a wrong
    value, not a missing one, and nothing downstream could tell.
    """
    chain = [DOC_EXTRACTOR_TRANSPORT] + [t for t in DOC_EXTRACTOR_TRANSPORT_FALLBACK
                                         if t != DOC_EXTRACTOR_TRANSPORT]
    last_err: Optional[ExtractionError] = None
    started = time.monotonic()
    for i, name in enumerate(chain):
        try:
            text = await _dispatch_transport(name, prompt, http, model=model,
                                             temperature=temperature)
            receipt = _producer_for(name, model)
            receipt.update({
                "configured_transport": DOC_EXTRACTOR_TRANSPORT,
                "fell_back_from": chain[:i] or None,
                "transport_attempts": i + 1,
                "temperature": temperature,
                "latency_ms": int((time.monotonic() - started) * 1000),
            })
            return text, receipt
        except ExtractionError as e:
            last_err = e
            if i + 1 >= len(chain) or e.reason not in TRANSPORT_FALLBACK_REASONS:
                raise
            logger.warning("transport %r failed (%s) — falling back to %r",
                           name, e.reason, chain[i + 1])
    assert last_err is not None
    raise last_err


# How many repair passes to attempt when the model returns malformed / schema-invalid
# JSON. Smaller / open models occasionally slip on strict JSON — a
# missing comma, or a bare string where the schema wants an object. Re-asking with the
# exact error + a bumped temperature recovers most of these (a plain retry at temp 0
# would reproduce the same broken output deterministically). 0 disables the loop.
DOC_EXTRACTOR_REPAIR_PASSES = int(os.getenv("DOC_EXTRACTOR_REPAIR_PASSES", "2"))


async def extract_window_validated(prompt: str, http: httpx.AsyncClient, schema: dict,
                                   *, model: str) -> Tuple[dict, dict]:
    """Extract one window and parse+validate it, with a repair loop on EVERY transport.

    On a parse/schema failure, re-ask the model with the broken output and the exact
    validator error, nudging temperature up each pass so the retry isn't identical.
    Set DOC_EXTRACTOR_REPAIR_PASSES=0 for single-shot behaviour.

    If repair is still exhausted, the caller dead-letters this WINDOW and continues over
    the rest of the document (#40) — a bad window no longer costs the whole book."""
    raw, receipt = await _extract_window(prompt, http, model=model)
    receipt["repair_passes"] = 0
    try:
        return parse_and_validate(raw, schema), receipt
    except ExtractionError as first_err:
        repairable = first_err.reason in ("extract_parse_error", "empty_completion")
        # #40: the repair loop used to be gated to the 'openai' transport, on the reasoning
        # that api/claude_p "rarely emit invalid JSON" and re-prompting claude -p is slow.
        # Measured otherwise: on the Kurtz corpus the DEFAULT claude_p transport hit four
        # separate schema failures, each of which discarded a whole document (30-50 min of
        # extraction) because it had no repair path at all. One extra ~2 min repair call is
        # obviously cheaper than that. Repair now runs on every transport; set
        # DOC_EXTRACTOR_REPAIR_PASSES=0 to restore single-shot behaviour.
        if not repairable or DOC_EXTRACTOR_REPAIR_PASSES <= 0:
            raise
        last_err = first_err
        for i in range(DOC_EXTRACTOR_REPAIR_PASSES):
            repair_prompt = (
                f"{prompt}\n\n---\nYour previous response was NOT accepted. Error:\n"
                f"{last_err.detail}\n\nPrevious response (fix it):\n{raw[:6000]}\n\n"
                "Return the CORRECTED result as ONE valid JSON object matching the schema "
                "exactly — no prose, no markdown fences, every required field present, and "
                "every entities[] item an object with name+type (never a bare string). "
                "Start with { and end with }."
            )
            temp = 0.2 + 0.3 * i
            logger.info("  repair pass %d/%d (temp=%.1f): %s",
                        i + 1, DOC_EXTRACTOR_REPAIR_PASSES, temp, last_err.detail[:80])
            try:
                # 2026-08-28: was `_call_openai(...)`, which contradicted this function's own
                # docstring ("a repair loop on EVERY transport") and the comment above. The
                # gating comment was updated when repair was generalised; the CALL was not.
                # Effect: a claude_p/api run's schema-invalid window was repaired by
                # gpt-4o-mini on api.openai.com, billed to the embeddings key, then recorded
                # with the original route_used — so the logs attributed OpenAI output to
                # Claude. Route through the same dispatcher as the first attempt.
                raw, receipt = await _extract_window(repair_prompt, http, model=model,
                                                     temperature=temp)
                # The receipt is REPLACED, not merged: a repair pass may have been
                # served by a different transport than the first attempt, and the
                # producer of the ACCEPTED output is the one that matters. The
                # earlier misattribution here (repair hardcoded to _call_openai
                # while route_used said claude_p) is exactly this confusion.
                receipt["repair_passes"] = i + 1
                return parse_and_validate(raw, schema), receipt
            except ExtractionError as e:
                last_err = e
                if e.reason not in ("extract_parse_error", "empty_completion"):
                    raise
        raise last_err


def parse_and_validate(raw: str, schema: dict) -> dict:
    first, last = raw.find("{"), raw.rfind("}")
    if first < 0 or last < 0 or last < first:
        raise ExtractionError("extract_parse_error", "no JSON object found in output")
    try:
        data = json.loads(raw[first:last + 1])
    except json.JSONDecodeError as e:
        raise ExtractionError("extract_parse_error", f"json decode: {e}") from e
    # Robustness: the extractor occasionally emits a fact object missing the required
    # fact_text. Rather than fail (and discard) the whole window, synthesize fact_text
    # from the triple (subject / predicate / object|object_literal) so no fact is lost
    # and schema validation passes. Only fires when fact_text is absent/blank — the
    # normal path (model-written fact_text) is untouched.
    if isinstance(data.get("facts"), list):
        for f in data["facts"]:
            if isinstance(f, dict) and not str(f.get("fact_text") or "").strip():
                obj = f.get("object") or f.get("object_literal") or ""
                synth = " ".join(str(x).strip() for x in (f.get("subject"), f.get("predicate"), obj)
                                 if x and str(x).strip())
                if synth:
                    f["fact_text"] = synth
    # Same spirit as the fact_text repair above. `chunk_range` must be EXACTLY two ints —
    # the cross-window merge indexes cr[0] and cr[1] — but when a fact's evidence sits in a
    # single chunk the extractor reasonably answers `[990]`, and that one slip discarded a
    # whole document (2026-07-31: the 227-post blog died at window 78 of 86 on
    # `['facts', 2, 'chunk_range']: [990] is too short`). Normalise BEFORE validating so the
    # len==2 invariant the merge relies on stays enforced, rather than relaxing minItems
    # (which would turn a clean validation error into an IndexError downstream).
    for _key in ("facts", "discourse"):
        if isinstance(data.get(_key), list):
            for _item in data[_key]:
                if not isinstance(_item, dict):
                    continue
                _cr = _item.get("chunk_range")
                if isinstance(_cr, list) and all(isinstance(n, int) for n in _cr):
                    if len(_cr) == 1:
                        _item["chunk_range"] = [_cr[0], _cr[0]]
                    elif len(_cr) > 2:
                        _item["chunk_range"] = [min(_cr), max(_cr)]
    errors = sorted(Draft202012Validator(schema).iter_errors(data), key=lambda e: list(e.path))
    if errors:
        msgs = [f"{list(e.path)}: {e.message}" for e in errors[:5]]
        raise ExtractionError("extract_parse_error", f"schema: {' | '.join(msgs)}")
    return data


# ── Windowing over the persisted RAG chunks ───────────────────────────────────────

class Window:
    __slots__ = ("index", "char_start", "char_end", "chunk_index_base", "chunk_indices", "text")

    def __init__(self, index, char_start, char_end, chunk_index_base, chunk_indices, text):
        self.index = index
        self.char_start = char_start
        self.char_end = char_end
        self.chunk_index_base = chunk_index_base
        self.chunk_indices = chunk_indices
        self.text = text


async def fetch_rag_chunks(conn: asyncpg.Connection, document_rid: str) -> List[Tuple[int, str]]:
    rows = await conn.fetch(
        "SELECT chunk_index, content->>'text' AS text FROM koi_memory_chunks "
        "WHERE document_rid = $1 ORDER BY chunk_index ASC", document_rid)
    return [(r["chunk_index"], r["text"] or "") for r in rows]


def build_windows(chunks: List[Tuple[int, str]], target_chars: int, overlap_chunks: int) -> List[Window]:
    """Pack consecutive global RAG chunks into windows; label each chunk with its
    GLOBAL index [N] (no offset — the model emits global coordinates directly)."""
    if not chunks:
        return []
    # Running char offsets per chunk for char_start/char_end bookkeeping.
    offsets, run = [], 0
    for _, txt in chunks:
        offsets.append(run)
        run += len(txt)

    windows: List[Window] = []
    i, n, w = 0, len(chunks), 0
    while i < n:
        j, size = i, 0
        while j < n and (size == 0 or size + len(chunks[j][1]) <= target_chars):
            size += len(chunks[j][1])
            j += 1
        sel = chunks[i:j]
        text = "\n\n".join(f"[{ci}] {txt}" for ci, txt in sel)
        windows.append(Window(
            index=w, char_start=offsets[i], char_end=offsets[j - 1] + len(chunks[j - 1][1]),
            chunk_index_base=sel[0][0], chunk_indices=[ci for ci, _ in sel], text=text))
        w += 1
        if j >= n:
            break
        i = max(i + 1, j - overlap_chunks)   # advance with overlap
    return windows


def assert_contract_surfaces(template: str, schema: dict,
                             prompt_path: Path, schema_path: Path) -> None:
    """Refuse to run against a prompt or schema that disagrees with the contract.

    THE COMMITTED FILES ARE NOT NECESSARILY THE FILES THIS RUN LOADS. All four
    paths are env-overridable (DOC_EXTRACTOR_PROMPT_FILE, _SCHEMA_FILE,
    _PROMPT_V2_FILE, _SCHEMA_V2_FILE), and a scheduled job may run from a separate
    deploy checkout that is refreshed by `git pull` — so it can sit several commits
    behind this one indefinitely. A test that reads the files
    in THIS repository proves the repository is consistent; it proves nothing about
    what an unattended run actually sent to a model.

    Issue #68 requirement 5 asks for drift detection "at startup/tests". This is
    the startup half, and it is the half that covers the deployment gap that let
    `Document` and `Event` stay un-emittable while the registry admitted them.

    Fails loudly and terminally: an extraction that cannot emit the right type
    writes a wrong one, and under PR #66's pinning that wrong type is permanent.
    """
    want = list(contract.DOCUMENT_ENTITY_TYPE_NAMES)
    try:
        got = schema["properties"]["entities"]["items"]["properties"]["type"]["enum"]
    except (KeyError, TypeError) as e:
        raise ExtractionError(
            "contract_drift",
            f"{schema_path}: no entities[].type enum to check against "
            f"{contract.DOCUMENT_TYPE_CONTRACT_VERSION} ({e})", terminal=True) from e
    if list(got) != want:
        raise ExtractionError(
            "contract_drift",
            f"{schema_path} admits {got}, but {contract.DOCUMENT_TYPE_CONTRACT_VERSION} "
            f"admits {want}. Refusing to extract: a type the schema forbids cannot be "
            f"emitted, and a wrong type written under a pinned identity is permanent. "
            f"Refresh this checkout, or run scripts/render_extraction_contract.py.",
            terminal=True)

    # The prompt is what the model actually reads. A schema-conformant run whose
    # PROMPT still lists seven types produces seven-type output that validates
    # perfectly — the drift would be invisible to every downstream check.
    missing = [n for n in want if f"`{n}`" not in template]
    if missing:
        raise ExtractionError(
            "contract_drift",
            f"{prompt_path} never mentions {missing} — the model would not be told "
            f"those types exist, and its output would still validate. Refresh this "
            f"checkout, or run scripts/render_extraction_contract.py.", terminal=True)


def build_prompt(template: str, window: Window, window_count: int) -> str:
    placeholder = "<!-- The pipeline appends the concatenated window chunks here at call time -->"
    if placeholder not in template:
        raise RuntimeError("doc prompt template missing window placeholder")
    return (template
            .replace("{WINDOW_INDEX}", str(window.index + 1))
            .replace("{WINDOW_COUNT}", str(window_count))
            .replace(placeholder, window.text))


# ── Cross-window merge (deterministic) ────────────────────────────────────────────

merge_extractions = _merge.merge_extractions   # see api/extraction_merge.py


# ── Discourse merge + write (thorough tier only) ───────────────────────────────────

# Document ARGUMENT taxonomy + cross-window discourse merge live in
# api/extraction_merge.py (one production merge for extractor, gate and curator).
VALID_MOVE_TYPES = _merge.VALID_MOVE_TYPES
VALID_MOVE_STATUS = _merge.VALID_MOVE_STATUS
merge_discourse = _merge.merge_discourse


async def write_discourse_moves(conn, *, document_rid: str, episode_id,
                                moves: List[Dict[str, Any]],
                                written_ids: Optional[List[Any]] = None) -> int:
    """Write document discourse moves into the generalized session_discourse_moves
    table (source_type='document', migration 103). Deterministic uuid5 ids →
    idempotent upsert. Two passes: insert all moves, then link argument edges
    (supports → the claim/thesis's id, via resolves_move_id). Document moves carry no
    embedding in v1 (the column is 1024-dim/session-scoped); the chunk range lands in
    the turn_range_* columns per migration 103's documented dual semantics. Invalid
    moves are dead-lettered, never silently dropped."""
    if not moves:
        return 0

    def move_id(mt: str, title: str, chunk_start: int):
        # The hash input is the WHITESPACE-ONLY key, not `_norm` — see discourse_move_id_key.
        return discourse_move_id(document_rid, mt, title, chunk_start)

    title_to_id: Dict[str, Any] = {}
    inserted = 0
    for m in moves:
        mt = m.get("move_type")
        title = (m.get("title") or "").strip()[:400]
        if mt not in VALID_MOVE_TYPES or not title:
            with contextlib.suppress(Exception):
                await conn.execute(
                    "INSERT INTO document_extraction_item_errors (document_rid, item_type, payload, error) "
                    "VALUES ($1,'discourse_move',$2::jsonb,$3)",
                    document_rid, json.dumps(m), f"invalid move_type/title: move_type={mt!r}")
            continue
        status = m.get("status") if m.get("status") in VALID_MOVE_STATUS else None
        cr = m.get("chunk_range") or [None, None]
        a = cr[0] if isinstance(cr[0], int) else None
        b = cr[1] if isinstance(cr[1], int) else None
        mid = move_id(mt, title, a if a is not None else 0)
        await conn.execute(
            """INSERT INTO session_discourse_moves
                 (id, episode_id, source_type, source_rid, session_id, move_type, title,
                  detail, status, turn_range_start, turn_range_end, embedding)
               VALUES ($1,$2,'document',$3,NULL,$4,$5,$6,$7,$8,$9,NULL)
               ON CONFLICT (id) DO UPDATE SET
                 detail=EXCLUDED.detail, status=EXCLUDED.status,
                 turn_range_start=EXCLUDED.turn_range_start,
                 turn_range_end=EXCLUDED.turn_range_end,
                 episode_id=EXCLUDED.episode_id""",
            mid, episode_id, document_rid, mt, title, m.get("detail"), status, a, b)
        title_to_id[_norm(title)] = mid
        inserted += 1
        if written_ids is not None:
            written_ids.append(mid)

    # Second pass: link argument edges — a premise/evidence/counterpoint `supports`
    # the claim/thesis whose title it names → resolves_move_id self-FK.
    for m in moves:
        sup = m.get("supports")
        title = (m.get("title") or "").strip()[:400]
        mt = m.get("move_type")
        if not sup or mt not in VALID_MOVE_TYPES or not title:
            continue
        target = title_to_id.get(_norm(sup))
        if not target:
            continue
        cr = m.get("chunk_range") or [None, None]
        cs = cr[0] if isinstance(cr[0], int) else 0
        with contextlib.suppress(Exception):
            await conn.execute(
                "UPDATE session_discourse_moves SET resolves_move_id=$2 "
                "WHERE id=$1 AND source_rid=$3",
                move_id(mt, title, cs), target, document_rid)
    return inserted


async def read_rag_input(conn, document_rid: str):
    """fetch_rag_chunks, plus (only when KOI_DISCOURSE_BINDINGS=1) the source
    version observed before and after the read, for the discourse-link binding
    (issue #60, A2). Returns (chunks, extraction_input or None)."""
    if os.getenv("KOI_DISCOURSE_BINDINGS") != "1":
        return await fetch_rag_chunks(conn, document_rid), None
    from api.discourse_links import capture_extraction_input
    return await capture_extraction_input(conn, source_rid=document_rid,
                                          read_chunks=lambda: fetch_rag_chunks(conn, document_rid))


async def record_discourse_bindings(conn, *, document_rid: str, extraction_input, move_ids: List[Any],
                                    run_id: str) -> int:
    """Record which source version and exact chunk input this run's moves came
    from, using the association captured when the input was read. A source that
    changed since then is recorded as such and cannot be admitted. Off unless
    KOI_DISCOURSE_BINDINGS=1 (read_rag_input then returns no captured input).
    Without the draft table it logs and records nothing: a move without a
    recorded binding cannot be admitted, so skipping is the conservative side."""
    if extraction_input is None or not move_ids:
        return 0
    if not await conn.fetchval("SELECT to_regclass('discourse_move_extraction_bindings') IS NOT NULL"):
        logger.error("KOI_DISCOURSE_BINDINGS=1 but discourse_move_extraction_bindings does not exist; "
                     "%d move(s) of %s left without a recorded binding", len(move_ids), document_rid)
        return 0
    from api.discourse_links import record_extraction_bindings
    recorded = await record_extraction_bindings(
        conn, source_rid=document_rid, move_ids=move_ids, extraction_input=extraction_input,
        run_id=run_id, recorded_by="extract_deep_documents")
    logger.info("discourse: recorded extraction bindings for %d move(s) (%s)", recorded,
                extraction_input.association)
    return recorded


async def schedule_discourse_discovery(conn, *, document_rid: str, move_ids: List[Any], run_id: str):
    """After a COMPLETE extraction whose bindings were recorded, queue cross-source
    discourse discovery for this run's moves (issue #60). Off unless
    KOI_DISCOURSE_INGEST_DISCOVERY=1, and then a no-op for moves this run did not
    bind with its input (KOI_DISCOURSE_BINDINGS=1). The job's per-claim limit is
    the default unless KOI_DISCOURSE_DISCOVERY_PER_MOVE_CAP holds one of the
    allowed values (ingest_document.py --discourse-move-cap). It only inserts a
    job row: scripts/discourse_discovery_worker.py does the retrieval later. It
    never fails the extraction; an error (an invalid limit included) is logged and
    visible as an unscheduled extraction in `discourse_discovery_worker.py status`."""
    if os.getenv("KOI_DISCOURSE_INGEST_DISCOVERY") != "1" or not move_ids:
        return None
    try:
        from api.discourse_discovery import enqueue_for_extraction, requested_per_move_cap
        async with conn.transaction():
            result = await enqueue_for_extraction(conn, source_rid=document_rid, extraction_run_id=run_id,
                                                  move_ids=move_ids, per_move_cap=requested_per_move_cap())
    except Exception as exc:  # noqa: BLE001 - discovery must never fail an ingested document
        logger.error("discourse discovery for %s not scheduled: %s: %s", document_rid, type(exc).__name__, exc)
        return {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
    logger.info("discourse discovery for %s: %s", document_rid, result)
    return result


def facts_to_episode_payload(merged: dict, *, name: str, summary: str, source_document: str,
                             group_id: str, frozen=None) -> dict:
    """Build the /knowledge/episodes request.

    When `frozen` (an api.ingest_identity.FrozenMap) is supplied, every entity-valued
    endpoint carries its PINNED URI and `create_entities` is False. Those two go
    together and neither is optional:

    - The pinned URI is what makes the binding order-independent; without it the
      server resolves by name and an entity created earlier in the same request is a
      candidate for a later, different endpoint.
    - `create_entities: False` is the belt to that braces. Every endpoint was
      pre-registered during the freeze, so nothing legitimate remains to create. If
      the server finds itself wanting to create an entity anyway, something is
      referenced that the freeze did not cover, and the right outcome is a refusal
      rather than a quietly-minted row the frozen map does not know about.

    Passing an incomplete map is treated as a programming error, not a partial
    improvement: a payload where some endpoints are pinned and others are resolved
    is exactly as order-dependent as one where none are, but it LOOKS hardened.
    """
    type_map = merged["type_map"]
    fact_inputs = []
    for f in merged["facts"]:
        subj_t = type_map.get(_norm(f["subject"]))
        obj_t = type_map.get(_norm(f["object"])) if f.get("object") else None
        item = {
            "subject": f["subject"], "subject_type": subj_t,
            "predicate": f["predicate"],
            "object": f.get("object"), "object_type": obj_t,
            "object_literal": f.get("object_literal"),
            "fact_text": f["fact_text"],
        }
        if f.get("confidence"):
            item["confidence"] = f["confidence"]
        if frozen is not None:
            subj_uri = frozen.uri_for(f["subject"])
            if subj_uri is None:
                raise ExtractionError(
                    "identity_map_incomplete",
                    f"frozen endpoint map has no binding for subject {f['subject']!r}; "
                    f"refusing to write a partially-pinned payload")
            item["subject_uri"] = subj_uri
            # subject_type must agree with the pinned entity's type or the server
            # 422s. The freeze bound on (label, type), so send the type it bound.
            item["subject_type"] = frozen.type_for(f["subject"]) or subj_t
            if f.get("object"):
                obj_uri = frozen.uri_for(f["object"])
                if obj_uri is None:
                    raise ExtractionError(
                        "identity_map_incomplete",
                        f"frozen endpoint map has no binding for object {f['object']!r}; "
                        f"refusing to write a partially-pinned payload")
                item["object_uri"] = obj_uri
                item["object_type"] = frozen.type_for(f["object"]) or obj_t
        fact_inputs.append(item)
    return {
        "name": name, "content": summary[:2000] if summary else None,
        "source_description": "document", "source_document": source_document,
        "group_id": group_id,
        "create_entities": frozen is None,
        "facts": fact_inputs,
    }


# ── HTTP: /knowledge/episodes + /tasks/ingest ──────────────────────────────────────

async def post_episode(http: httpx.AsyncClient, payload: dict) -> dict:
    headers = {}
    if KOI_EPISODES_SERVICE_TOKEN:
        headers["Authorization"] = f"Bearer {KOI_EPISODES_SERVICE_TOKEN}"
    # 180s was hardcoded and is a hard SCALE CEILING: a book-length document merges into a
    # single episode (WWS 4th edition = 296 entities + 299 facts), and 3-tier entity
    # resolution with embeddings for that many entities exceeds 180s. The client then
    # raises ReadTimeout and the whole ingest aborts AFTER all windows extracted — while
    # uvicorn keeps working server-side, leaving a partially-written episode. Env-tunable,
    # default unchanged at 180 so nothing else shifts behaviour.
    # (Added 2026-07-31 during the Kurtz corpus drain — separate from the uncommitted
    # transport work already in this file.)
    _ep_timeout = float(os.getenv("DOC_EPISODE_TIMEOUT", "180"))
    r = await http.post(f"{KOI_BASE_URL}/knowledge/episodes", json=payload, headers=headers,
                        timeout=_ep_timeout)
    r.raise_for_status()
    return r.json()


# Episode request-size cap (#41). `EpisodeCreateRequest.facts` has no max_items and the
# write path has no chunking anywhere, so request duration scaled with DOCUMENT size,
# unbounded: a book merges into ONE call (WWS 4th edition 296 entities/299 facts; the
# blog 1,502/1,323). Two coupled failures followed. (a) The request outran the client
# timeout. (b) Worse, a client disconnect does NOT cancel the server request —
# create_episode runs to completion and COMMITS — so the client's retry then lock-waited
# on the ORIGINAL request's own uncommitted tuples, blew asyncpg's 60s per-statement
# command_timeout, and surfaced as a bare TimeoutError + HTTP 500. A request colliding
# with itself.
#
# Batching converts request duration from a function of document size (unbounded) into a
# function of batch size (bounded, tunable). Batches share (source_document, group_id), and
# the server's episode-reuse path keys on exactly that pair (knowledge_router ~line 813),
# so they collapse into ONE episode — same episode_id, so the post-episode dedup sweeps
# (which key on episode_id) still see the whole document and now also catch CROSS-BATCH
# duplicates that a single mega-request would have merged internally.
#
# ATOMICITY TRADE, stated plainly: P4's "every write commits together or not at all"
# narrows from per-DOCUMENT to per-BATCH. Acceptable because it still holds for every
# individual request; resume state lives in document_window_extractions, so a failure
# costs a re-POST and never a re-extraction; episode reuse plus the dedup sweeps make a
# re-POST convergent; and deep_extracted_at is only set after the LAST batch, so a
# partially-written document is retried rather than marked done. The status quo was
# strictly worse — the retry collision above already produced 489 facts from a ~299-fact
# payload.
DOC_EPISODE_BATCH_SIZE = int(os.getenv("DOC_EPISODE_BATCH_SIZE", "100"))

# ── Identity contract mode (issue #62) ────────────────────────────────────────
# strict (DEFAULT): an unbindable endpoint blocks BEFORE any fact is written, and a
#   persisted endpoint outside the frozen map blocks before finalization.
# warn: report the blockers and proceed UNPINNED. Honest about what it is — the run
#   is order-dependent, exactly as before this shipped — and it is here for one real
#   case: a bounded backlog where a pre-existing graph-wide duplicate set would
#   otherwise block every document over a defect that has nothing to do with the
#   document. It is NOT a soak default. `warn_proceeded_unpinned` lands in the
#   evidence so a warn-mode graph can be found again.
# off: skip the contract entirely (pre-#62 behaviour).
#
# The default is strict because the alternative was measured: the audited Buehler
# import passed the structural gate with 11 fact endpoints pointing at the wrong
# entity, and nothing surfaced it for weeks.
DOC_IDENTITY_MODE = os.getenv("DOC_IDENTITY_MODE", "strict").strip().lower()
if DOC_IDENTITY_MODE not in ("strict", "warn", "off"):
    raise SystemExit(
        f"DOC_IDENTITY_MODE must be strict|warn|off, got {DOC_IDENTITY_MODE!r}. "
        f"Refusing to start: an unrecognised value must not silently mean 'off'.")

# Audited operator decisions. Two kinds, deliberately separate files and separate
# env vars, because they answer different questions and must not be conflatable:
#
#   DOC_IDENTITY_ALIAS_DECISIONS  {payload label -> canonical URI}   WHICH thing
#       (issue #62 requirement 6) The ONLY thing that can clear an alias-only
#       block, and the only sanctioned way two distinct payload labels may share
#       one URI.
#
#   DOC_IDENTITY_TYPE_DECISIONS   {payload label -> entity_type}     WHAT it IS
#       (issue #68 requirement 7) The ONLY thing that can clear a cross-type
#       conflict. `preflight_endpoints` has accepted these since #66 and NOTHING
#       PASSED THEM — the parameter existed, no caller supplied it, so the block's
#       own suggested remedy was unreachable from the command line. Wired here and
#       in scripts/check_document_integrity.py.
#
# Both are recorded in the run's evidence.
def _load_decisions(env_var: str, what: str, *,
                    valid: Optional[set] = None) -> Dict[str, str]:
    path = os.getenv(env_var)
    if not path:
        return {}
    p = Path(path).expanduser()
    if not p.is_file():
        raise SystemExit(f"{env_var} points at a missing file: {p}")
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in data.items()):
        raise SystemExit(f"{p}: {what} must be a flat object of label -> value")
    if valid is not None:
        bad = {k: v for k, v in data.items() if v not in valid}
        if bad:
            # A decision naming a type the extractor cannot emit would bind an
            # endpoint to a type no payload can declare — it reads as resolved
            # while resolving nothing. Refuse at load, not at use.
            raise SystemExit(
                f"{p}: {what} names type(s) outside {contract.DOCUMENT_TYPE_CONTRACT_VERSION}: "
                f"{json.dumps(bad, sort_keys=True)}. Admitted: "
                f"{', '.join(sorted(valid))}")
    return data


alias_decisions: Dict[str, str] = _load_decisions(
    "DOC_IDENTITY_ALIAS_DECISIONS", "alias decisions")
type_decisions: Dict[str, str] = _load_decisions(
    "DOC_IDENTITY_TYPE_DECISIONS", "type decisions",
    valid=set(contract.DOCUMENT_ENTITY_TYPE_NAMES))


async def post_episode_batched(http: httpx.AsyncClient, payload: dict) -> dict:
    """POST an episode, splitting oversized fact lists into sequential same-episode calls.

    Returns one aggregated response shaped like a single post_episode() result.
    A payload at or under the cap takes the single-POST path, byte-identical to before;
    set DOC_EPISODE_BATCH_SIZE=0 to force that path always.
    """
    facts = payload.get("facts") or []
    if DOC_EPISODE_BATCH_SIZE <= 0 or len(facts) <= DOC_EPISODE_BATCH_SIZE:
        return await post_episode(http, payload)

    n = DOC_EPISODE_BATCH_SIZE
    chunks = [facts[i:i + n] for i in range(0, len(facts), n)]
    logger.info("episode: %d facts > cap %d → %d sequential batches sharing one episode",
                len(facts), n, len(chunks))

    agg = {"facts_created": 0, "facts_skipped": 0, "facts_null_embed": 0,
           "entities_created": 0, "entities_resolved": 0, "endpoints_pinned": 0}
    mismatches: List[dict] = []
    fact_ids: List[str] = []
    saw_fact_ids = False
    episode_id = None
    for i, batch in enumerate(chunks):
        sub = {**payload, "facts": batch}
        ep = await post_episode(http, sub)
        for k in agg:
            agg[k] += int(ep.get(k) or 0)
        mismatches.extend(ep.get("type_mismatches") or [])
        if "fact_ids" in ep:
            saw_fact_ids = True
            fact_ids.extend(ep.get("fact_ids") or [])
        eid = ep.get("episode_id")
        if episode_id is None:
            episode_id = eid
        elif eid and eid != episode_id:
            # The whole design rests on episode reuse collapsing these. If it did not,
            # the document is now split across episodes and the dedup sweeps (which key on
            # a single episode_id) would silently only clean the first one. Fail loud.
            raise ExtractionError(
                "episode_split",
                f"batch {i + 1}/{len(chunks)} landed in episode {eid}, not {episode_id} — "
                f"episode reuse by (source_document, group_id) did not hold; refusing to "
                f"continue with a document split across episodes")
        logger.info("  episode batch %d/%d: %d facts (created=%s skipped=%s)",
                    i + 1, len(chunks), len(batch), ep.get("facts_created"), ep.get("facts_skipped"))
    out = {**agg, "episode_id": episode_id, "type_mismatches": mismatches}
    if saw_fact_ids:
        out["fact_ids"] = fact_ids     # absent, not empty, when the server has none
    return out


async def file_type_mismatch_task(http: httpx.AsyncClient, conn, document_rid: str, tm: dict) -> None:
    """Record the mismatch once, and keep ONE rolling task per mismatch CLASS.

    WHAT WAS WRONG WITH THE OLD SHAPE (fixed 2026-09-02)
    ----------------------------------------------------
    This filed ONE TASK PER (document, entity). Over 95 days that produced
    **1,023 open tasks, every one owner=None, never triaged, still accruing** --
    the extraction pipeline reporting its own failures into a void. And the same
    finding was written to TWO places: this task AND the dead-letter row below,
    so one failure filled two voids.

    1,023 undifferentiated tasks are unreadable. Their AGGREGATE is not:

        Protocol     -> Concept              339
        Project      -> Concept              179
        Organization <-> Project             165  (both directions)
        Organization -> Person                60
        Location     -> Place                 22  } dead types, cancelled
        Project      -> SoftwareApplication   19  } 2026-09-02

    Six classes, not 1,023 decisions -- and 51% is a single story (Concept
    absorbing specific types). The per-document granularity never carried
    information the aggregate lacks, because nobody was ever going to read
    1,023 rows one at a time.

    THE NEW SHAPE. document_extraction_item_errors is the SINGLE HOME for the
    detail -- every occurrence still lands there, with the full payload, so
    nothing is lost and per-document forensics stay possible. The task layer
    now READS that table rather than duplicating it: one rolling task per
    class, carrying an occurrence count and last-seen, updated in place.

    WHY THIS MATTERS BEYOND TIDINESS: the count makes a prediction testable.
    Organization->Person (60) is the email From-name class, whose guard is
    committed but deploy-parked in koi-sensors-runtime. If that guard works,
    THIS class stops accruing on deploy while the other five continue. A
    per-document task pile could not show that; a counter can.
    """
    requested = tm.get("requested_type") or "?"
    resolved = tm.get("resolved_type") or "?"
    error_sig = f"requested={requested} resolved={resolved}"

    # 1. The single durable home. Deliberately NOT silently suppressed: if the
    #    dead-letter write fails, the finding is gone and the aggregate below
    #    under-counts. Failing loudly here is the point -- but it must not kill
    #    the extraction that produced it, so it warns rather than raises.
    try:
        await conn.execute(
            "INSERT INTO document_extraction_item_errors (document_rid, item_type, payload, error) "
            "VALUES ($1, 'type_mismatch', $2::jsonb, $3)",
            document_rid, json.dumps(tm), error_sig)
    except Exception as e:
        logger.warning(
            "dead-letter write FAILED for type_mismatch %s on %s (%s) -- this occurrence "
            "is lost and the class counter will under-report", error_sig, document_rid, e)
        return

    # 2. One rolling task per class, counted FROM the dead-letter table so the
    #    two can never disagree.
    try:
        n = await conn.fetchval(
            "SELECT count(*) FROM document_extraction_item_errors "
            "WHERE item_type = 'type_mismatch' AND error = $1", error_sig)
        first_seen = await conn.fetchval(
            "SELECT min(created_at)::date FROM document_extraction_item_errors "
            "WHERE item_type = 'type_mismatch' AND error = $1", error_sig)
    except Exception as e:
        logger.warning("could not read type-mismatch aggregate for %s: %s", error_sig, e)
        return

    slug = f"{requested}-{resolved}".lower().replace(" ", "-")
    payload = {
        "taskKey": f"doc-ingest-typemismatch-class-{slug}",
        "title": f"Type-mismatch class: {requested} -> {resolved} ({n} occurrences)",
        "status": "open", "priority": "low", "sourceType": "document-ingest",
        "context": (
            f"Extraction requested type {requested!r} but resolution returned {resolved!r}. "
            f"{n} occurrences since {first_seen}; most recent from document {document_rid}. "
            f"Full per-occurrence detail lives in document_extraction_item_errors "
            f"(item_type='type_mismatch', error='{error_sig}') -- this task is a rolling "
            f"summary of that table, not a second copy of it.\n\n"
            f"On …->Concept classes: an earlier version of this text said they were the "
            f"extractor enum and allowed_entity_types disagreeing, and would resolve as a "
            f"side effect of generating the prompt enum (audit D4b). That was WRONG and is "
            f"corrected here (2026-09-03). The extractor already emits {requested!r} and "
            f"does so freely -- the enum is not the constraint. What happens is that a typed "
            f"Tier-1 miss falls through to an UNTYPED lookup which accepts any type "
            f"(api/routers/knowledge_router.py:1253-1288), and the cross-type refusal there "
            f"fires only when the HINT is Concept, never when the existing ROW is. So a "
            f"{requested!r} hint binds to whatever Concept row already holds the name. "
            f"Regenerating the enum resolves none of these.\n\n"
            f"Nor are they uniformly non-triage. Measured across the whole …->Concept set: "
            f"621 occurrences over 215 distinct entities -- ~46 of them "
            f"(Person/Organization/Location stored as Concept, e.g. people and universities) "
            f"are real defects worth retyping, ~176 (Protocol/Project) are a genuine "
            f"modelling question rather than a bug, and the remainder want a type neither "
            f"side proposed."),
        "tags": ["document-ingest", "type-mismatch", "aggregate",
                 f"class:{requested}->{resolved}"],
    }
    with contextlib.suppress(Exception):
        r = await http.post(f"{KOI_BASE_URL}/tasks/ingest", json=payload, timeout=30.0)
        if r.status_code >= 300:
            logger.warning("task_ingest non-2xx (%s) for class %s", r.status_code, error_sig)


# ── Orchestration ──────────────────────────────────────────────────────────────────

# ── Deep-extract lease: liveness + identity around the per-document advisory lock (#35) ──
# The advisory lock is still the mutex. What it cannot express is "is the holder ALIVE or
# WEDGED": a process that hangs rather than crashes keeps its session-level lock forever
# (the `finally` never runs), and the contended caller just gets `skipped_locked`, which
# naive callers read as "nothing to do". These helpers add a heartbeat + holder identity
# so contention is diagnosable and a reclaim can be identity-checked.
#
# NOTE the GLOBAL lock #35 describes no longer exists — only `deep-extract-doc:<rid>` is
# taken, so distinct documents already extract concurrently (verified 2026-08-05). Scope
# of a wedged holder is one document, not the fleet.
DOC_EXTRACT_LEASE_TTL = float(os.getenv("DOC_EXTRACT_LEASE_TTL", "300"))
DOC_EXTRACT_HEARTBEAT_INTERVAL = float(os.getenv("DOC_EXTRACT_HEARTBEAT_INTERVAL", "30"))


async def _lease_acquire(conn, document_rid: str, run_id: Optional[str]) -> None:
    """Record who holds this document's lock. Call right after the advisory lock is taken."""
    await conn.execute(
        """
        INSERT INTO deep_extract_lease
            (document_rid, holder_pid, holder_backend_start, run_id, acquired_at, last_heartbeat)
        SELECT $1, pg_backend_pid(), a.backend_start, $2, now(), now()
          FROM pg_stat_activity a WHERE a.pid = pg_backend_pid()
        ON CONFLICT (document_rid) DO UPDATE SET
            holder_pid = EXCLUDED.holder_pid,
            holder_backend_start = EXCLUDED.holder_backend_start,
            run_id = EXCLUDED.run_id,
            acquired_at = now(),
            last_heartbeat = now()
        """, document_rid, run_id)


async def _lease_heartbeat(pool, document_rid: str) -> None:
    """Background task: prove liveness until cancelled.

    Uses its OWN pooled connection — the run's connection is busy inside the extract, so
    a heartbeat sharing it would only ever tick between statements and would go silent
    during exactly the long provider call we most need to distinguish from a hang.
    """
    try:
        while True:
            await asyncio.sleep(DOC_EXTRACT_HEARTBEAT_INTERVAL)
            try:
                async with pool.acquire() as hb:
                    await hb.execute(
                        "UPDATE deep_extract_lease SET last_heartbeat = now() WHERE document_rid = $1",
                        document_rid)
            except Exception as e:  # noqa: BLE001
                # A failed heartbeat must not kill the extraction it is only observing.
                logger.warning("lease heartbeat failed for %s: %s", document_rid, e)
    except asyncio.CancelledError:
        raise


async def _lease_release(conn, document_rid: str) -> None:
    with contextlib.suppress(Exception):
        await conn.execute("DELETE FROM deep_extract_lease WHERE document_rid = $1", document_rid)


async def lease_status(conn, document_rid: str) -> dict:
    """Who holds this document, for how long, and does it look wedged?"""
    row = await conn.fetchrow(
        """
        SELECT l.holder_pid, l.holder_backend_start, l.run_id, l.acquired_at, l.last_heartbeat,
               EXTRACT(EPOCH FROM (now() - l.last_heartbeat))::float AS heartbeat_age_s,
               EXTRACT(EPOCH FROM (now() - l.acquired_at))::float    AS held_for_s,
               (a.pid IS NOT NULL) AS holder_alive,
               a.state AS holder_state, a.wait_event_type, a.wait_event
          FROM deep_extract_lease l
          LEFT JOIN pg_stat_activity a
                 ON a.pid = l.holder_pid AND a.backend_start = l.holder_backend_start
         WHERE l.document_rid = $1
        """, document_rid)
    if row is None:
        return {"lease": None,
                "note": "advisory lock is held but no lease row — holder predates this "
                        "feature, or crashed between lock and lease insert"}
    d = dict(row)
    d["stale"] = bool(d["heartbeat_age_s"] is not None
                      and d["heartbeat_age_s"] > DOC_EXTRACT_LEASE_TTL)
    d["verdict"] = ("holder gone (lock will clear when its session ends)" if not d["holder_alive"]
                    else "WEDGED — no heartbeat past TTL" if d["stale"]
                    else "healthy, actively extracting")
    return d


async def reclaim_stale_lease(conn, document_rid: str, *, ttl: Optional[float] = None) -> dict:
    """Terminate a provably-wedged holder so its lock clears. Explicit, never automatic.

    #35 warns that reclaiming by PID off a stale pg_stat_activity snapshot can kill a
    HEALTHY backend that has since reused the PID. This does the staleness check and the
    identity check (pid AND backend_start) inside ONE transaction against
    pg_stat_activity, so the process being terminated is provably the same one recorded in
    the lease and provably still stale at the moment of the kill.
    """
    ttl = DOC_EXTRACT_LEASE_TTL if ttl is None else ttl
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT l.holder_pid, EXTRACT(EPOCH FROM (now() - l.last_heartbeat))::float AS age,
                   (a.pid IS NOT NULL) AS alive
              FROM deep_extract_lease l
              LEFT JOIN pg_stat_activity a
                     ON a.pid = l.holder_pid AND a.backend_start = l.holder_backend_start
             WHERE l.document_rid = $1
             FOR UPDATE OF l
            """, document_rid)
        if row is None:
            return {"reclaimed": False, "reason": "no lease row"}
        if not row["alive"]:
            await conn.execute("DELETE FROM deep_extract_lease WHERE document_rid = $1", document_rid)
            return {"reclaimed": True, "reason": "holder already gone; cleared orphan lease row"}
        if row["age"] is None or row["age"] <= ttl:
            return {"reclaimed": False, "reason": f"holder is live (heartbeat {row['age']:.0f}s "
                                                  f"<= ttl {ttl:.0f}s) — refusing to terminate"}
        killed = await conn.fetchval("SELECT pg_terminate_backend($1)", row["holder_pid"])
        await conn.execute("DELETE FROM deep_extract_lease WHERE document_rid = $1", document_rid)
        return {"reclaimed": bool(killed), "terminated_pid": row["holder_pid"],
                "heartbeat_age_s": row["age"]}



async def establish_identity(conn, http, merged: dict, identity_evidence: Dict[str, Any], *,
                             document_rid: str, mode: Optional[str] = None,
                             alias_decisions_override: Optional[Dict[str, str]] = None,
                             type_decisions_override: Optional[Dict[str, str]] = None):
    """Run the identity contract (issue #62) over a complete merged payload.

    Returns the FrozenMap to write with, or None when the mode is `off` or the
    mode is `warn` and the preflight blocked (the caller then writes UNPINNED and
    the evidence says so). Raises IdentityError in `strict` mode on any blocker.

    Lifted out of `extract_deep_document` so the REAL call path — the same client
    the extractor uses, the same decisions, the same freeze inputs — can be driven
    in a test. Every defect the independent review found in this seam (B1 relative
    URL, B2 payload-wide cross-type, B3 raw-spelling lookup, M3 Concept default,
    M4 unvalidated alias decision) was invisible to tests that exercised
    api/ingest_identity.py through stand-ins instead of through this function.

    On failure the document's `deep_extraction_last_error` is stamped before the
    error propagates, so an unattended run leaves a reason behind instead of a
    bare traceback in a log nobody reads.
    """
    mode = mode or DOC_IDENTITY_MODE
    a_dec = alias_decisions if alias_decisions_override is None else alias_decisions_override
    t_dec = type_decisions if type_decisions_override is None else type_decisions_override
    if mode == "off":
        return None
    try:
        # Audited type decisions reach `collect_endpoints` too: they are what types
        # a fact endpoint the extractor left out of entities[]. There is no default.
        endpoints = ident.collect_endpoints(merged, normalize=normalize_entity_text,
                                            type_decisions=t_dec)
        preflight = await ident.preflight_endpoints(
            conn, endpoints,
            normalize=normalize_entity_text,
            normalize_alias_fn=normalize_alias,
            alias_decisions=a_dec,
            type_decisions=t_dec)
        identity_evidence["preflight"] = preflight.as_evidence()
        logger.info("identity preflight: %d endpoints %s (drift-recovered=%d)",
                    len(endpoints), preflight.counts(),
                    identity_evidence["preflight"]["drift_recovered"])

        if preflight.blockers:
            msg = (f"identity preflight found {len(preflight.blockers)} unbindable "
                   f"endpoint(s): {sorted({f.state for f in preflight.blockers})}")
            if mode == "strict":
                ident.require_no_blockers(preflight)   # raises
            logger.warning("%s — MODE=warn, proceeding UNPINNED. The facts written "
                           "by this run are order-dependent.", msg)
            identity_evidence["warn_proceeded_unpinned"] = True
            return None

        # Exact-only pre-registration of the truly missing identities, then
        # freeze. The freeze RE-READS rather than trusting the registration
        # responses, and asserts the bijection. The registration URL is ABSOLUTE:
        # the client has no base_url (review B1).
        prereg = await ident.preregister_missing(http, preflight, base_url=KOI_BASE_URL)
        identity_evidence["preregistration"] = {
            "registered": len(prereg["uri_map"]),
            "receipts": prereg["receipts"],
        }
        frozen = await ident.freeze_endpoint_map(
            conn, preflight.endpoints,
            normalize=normalize_entity_text,
            expected_uris=prereg["uri_map"],
            alias_decisions=a_dec)
        identity_evidence["frozen_map"] = frozen.as_evidence()
        logger.info("identity frozen: %d endpoints → %d distinct URIs (%d pre-registered)",
                    len(frozen.by_name), frozen.distinct_uris, len(prereg["uri_map"]))
        return frozen
    except ident.IdentityError as e:
        identity_evidence["error"] = {"message": str(e)[:1500], "blockers": e.blockers[:20]}
        await conn.execute(
            """UPDATE document_ingestion_log
               SET deep_extraction_last_error = $2
             WHERE document_rid = $1""",
            document_rid, ("identity_blocked:" + str(e))[:2000])
        raise


async def extract_deep_document(pool: asyncpg.Pool, http: httpx.AsyncClient, *,
                                document_rid: str, tier: str, group_id: Optional[str],
                                run_id: str, force: bool,
                                source_sensor: str = "document-ingest") -> Dict[str, Any]:
    prompt_path, schema_path = prompt_schema_for_tier(tier)
    template = prompt_path.read_text(encoding="utf-8")
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert_contract_surfaces(template, schema, prompt_path, schema_path)
    want_discourse = (tier == "thorough")

    async with pool.acquire() as conn:
        locked = await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtext('deep-extract-doc:' || $1));", document_rid)
        if not locked:
            # #35: a bare `skipped_locked` is indistinguishable from "nothing to do", which
            # is how a wedged holder used to go unnoticed. Say WHO holds it and whether it
            # still has a pulse.
            status = await lease_status(conn, document_rid)
            logger.warning("document %s is locked by another run — %s",
                           document_rid, status.get("verdict") or status.get("note"))
            return {"status": "skipped_locked", "document_rid": document_rid,
                    "lock_holder": status}
        heartbeat_task = None
        try:
            await _lease_acquire(conn, document_rid, run_id)
            heartbeat_task = asyncio.create_task(_lease_heartbeat(pool, document_rid))
            mem = await conn.fetchrow(
                "SELECT content->>'title' AS title, "
                "COALESCE(metadata->>'source_url', metadata->>'url') AS url, "
                "metadata->>'slug' AS slug, metadata->>'group_id' AS group_id "
                "FROM koi_memories WHERE rid = $1 AND source_sensor = $2", document_rid, source_sensor)
            if mem is None:
                raise ExtractionError("no_rag", f"no koi_memories row for {document_rid} "
                                                f"with source_sensor={source_sensor!r} (run the RAG step first)",
                                      terminal=True)
            group_id = group_id or mem["group_id"] or "personal"
            source_document = mem["url"] or document_rid
            doc_title = mem["title"] or mem["slug"] or document_rid

            chunks, extraction_input = await read_rag_input(conn, document_rid)
            windows = build_windows(chunks, WINDOW_CHARS, WINDOW_OVERLAP_CHUNKS)
            budget_exhausted = len(windows) > MAX_WINDOWS
            if budget_exhausted and not MAX_WINDOWS_EXPLICIT:
                # No caller-chosen budget → the default must not silently discard the
                # tail of a long document. Process all windows and do NOT flag truncation.
                logger.warning("window budget: %d windows > default MAX_WINDOWS=%d → "
                               "auto-raising (DOC_MAX_WINDOWS unset; set it to cap cost)",
                               len(windows), MAX_WINDOWS)
                budget_exhausted = False
            elif budget_exhausted:
                logger.warning("window budget: %d windows > MAX_WINDOWS=%d → truncating "
                               "(explicit cap; document tail NOT extracted)",
                               len(windows), MAX_WINDOWS)
                windows = windows[:MAX_WINDOWS]

            await conn.execute(
                """INSERT INTO document_ingestion_log
                   (document_rid, source_path, source_url, title, content_hash, chunk_count,
                    window_count, group_id, tier, rag_chunked_at, last_run_id, last_ingested_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, NOW(), $10, NOW())
                   ON CONFLICT (document_rid) DO UPDATE SET
                     chunk_count=EXCLUDED.chunk_count, window_count=EXCLUDED.window_count,
                     tier=EXCLUDED.tier, group_id=EXCLUDED.group_id,
                     last_run_id=EXCLUDED.last_run_id, last_ingested_at=NOW()""",
                document_rid, None, source_document, doc_title,
                document_rid.split(":", 1)[-1], len(chunks), len(windows), group_id, tier, run_id)

            # Plan windows (idempotent); load cached extractions for resume.
            per_window: List[Optional[dict]] = [None] * len(windows)
            cached_windows = 0
            _cached_window_indices: set = set()
            for w in windows:
                await conn.execute(
                    """INSERT INTO document_window_extractions
                       (document_rid, window_index, char_start, char_end, chunk_index_base, status, run_id)
                       VALUES ($1,$2,$3,$4,$5,'pending',$6)
                       ON CONFLICT (document_rid, window_index) DO NOTHING""",
                    document_rid, w.index, w.char_start, w.char_end, w.chunk_index_base, run_id)
                cached = await conn.fetchrow(
                    "SELECT status, raw_json FROM document_window_extractions "
                    "WHERE document_rid=$1 AND window_index=$2", document_rid, w.index)
                if cached and cached["status"] in ("extracted", "imported") and cached["raw_json"] and not force:
                    per_window[w.index] = json.loads(cached["raw_json"])
                    cached_windows += 1
                    # Tracked so the producer-coverage check does not demand a
                    # receipt for a window THIS run never produced.
                    _cached_window_indices.add(w.index)

            # Extract any window not already cached.
            #
            # PER-WINDOW ISOLATION (#40): a window that cannot be extracted is DEAD-LETTERED
            # and the run continues over the windows that succeeded. Previously any single
            # window error propagated out and aborted the whole document, discarding every
            # window already extracted — during the Kurtz corpus drain that cost four
            # separate 30-50 minute passes, each losing 7-78 successfully extracted windows
            # to one stray key or a 1-element chunk_range. The schema already had
            # status='failed' + last_error for exactly this; nothing used them.
            #
            # Prefer "N-1 of N windows, loudly reported" over "0 of N, silently re-run
            # tomorrow". windows_failed is surfaced in the result and the gate evidence, and
            # deep_extracted_at is left NULL when any window failed, so a partial document
            # is retried rather than marked complete. Failed windows stay status='failed'
            # with their error, so a later run retries only those.
            windows_failed: List[int] = []
            # window_index -> producer receipt for the ACCEPTED output (#64). Only
            # windows extracted in THIS run appear here; cache-resumed windows keep
            # the producer recorded when they were first extracted, which is the
            # correct attribution — re-stamping them with this run's transport would
            # claim this run produced output it merely read back.
            window_receipts: Dict[int, dict] = {}
            for w in windows:
                if per_window[w.index] is not None:
                    logger.info("window %d/%d cached — skip", w.index + 1, len(windows))
                    continue
                # Log the model the SELECTED transport will actually use, not the
                # Anthropic variable. An `openai` run reads OPENAI_MODEL inside
                # _call_openai and ignores this argument entirely, so logging
                # ANTHROPIC_MODEL here printed a model that did no work (#64).
                _intended = _producer_for(DOC_EXTRACTOR_TRANSPORT, ANTHROPIC_MODEL)
                logger.info("window %d/%d: extracting (%d chunks, %d chars) via %s/%s",
                            w.index + 1, len(windows), len(w.chunk_indices), len(w.text),
                            _intended["provider"], _intended["model"])
                prompt = build_prompt(template, w, len(windows))
                try:
                    data, receipt = await extract_window_validated(
                        prompt, http, schema, model=ANTHROPIC_MODEL)
                except ExtractionError as e:
                    windows_failed.append(w.index)
                    logger.error("window %d/%d FAILED (%s) — dead-lettering the WINDOW and "
                                 "continuing; the document is not lost: %s",
                                 w.index + 1, len(windows), e.reason, str(e)[:400])
                    # A FAILED window has no accepted producer, so the configured
                    # route is all there is to record — and it is labelled as
                    # intended, not actual, so a reader cannot mistake it.
                    await conn.execute(
                        """UPDATE document_window_extractions
                           SET status='failed', last_error=$3, route_used=$4,
                               producer=$5::jsonb, updated_at=NOW()
                           WHERE document_rid=$1 AND window_index=$2""",
                        document_rid, w.index, str(e)[:2000], ROUTE_USED,
                        json.dumps({**_intended, "outcome": "failed",
                                    "intended_only": True, "reason": e.reason}))
                    continue
                per_window[w.index] = data
                window_receipts[w.index] = receipt
                if receipt["route"] != ROUTE_USED:
                    logger.warning(
                        "window %d: served by %s (%s/%s), NOT the configured %s — recording "
                        "the actual producer",
                        w.index, receipt["transport"], receipt["provider"],
                        receipt["model"], DOC_EXTRACTOR_TRANSPORT)
                await conn.execute(
                    """UPDATE document_window_extractions
                       SET status='extracted', route_used=$4, raw_json=$3::jsonb,
                           provider=$5, model=$6, transport=$7, producer=$8::jsonb,
                           last_error=NULL, updated_at=NOW()
                       WHERE document_rid=$1 AND window_index=$2""",
                    document_rid, w.index, json.dumps(data),
                    receipt["route"], receipt["provider"], receipt["model"],
                    receipt["transport"], json.dumps(receipt))

            # Only a TOTAL loss is fatal — there is nothing to merge, and silently writing an
            # empty episode would look like success.
            if windows_failed and not any(d for d in per_window):
                raise ExtractionError(
                    "all_windows_failed",
                    f"all {len(windows)} window(s) failed extraction; nothing to merge "
                    f"(see document_window_extractions.last_error for {document_rid})")
            if windows_failed:
                logger.warning("proceeding with %d/%d windows — %d dead-lettered: %s",
                               len(windows) - len(windows_failed), len(windows),
                               len(windows_failed), windows_failed)

            # Merge + write facts through /episodes.
            merged = merge_extractions([d for d in per_window if d], windows)

            # POISONED-CACHE GUARD (#33). A resume that extracts NOTHING new because every
            # window was cache-skipped, and whose cached content yields zero facts AND zero
            # entities, is not a successful no-op — it is a prior failed pass having left
            # empty rows behind, and it used to report success with exit 0. Note a single
            # empty window is perfectly legitimate (a table of contents extracts nothing),
            # so the guard requires ALL THREE conditions before firing, which is what keeps
            # it off genuine resumes.
            newly_extracted = len(windows) - cached_windows - len(windows_failed)
            if (windows and newly_extracted == 0 and cached_windows > 0
                    and not merged["facts"] and not merged["entities"]):
                raise ExtractionError(
                    "cache_poisoned",
                    f"resume skipped all {cached_windows} window(s) from cache and they "
                    f"contain zero facts and zero entities — a previous pass cached empty "
                    f"results. Re-run with --force to re-extract "
                    f"(document_rid={document_rid})")
            summary = next((d["document"].get("summary") for d in per_window if d and d.get("document")), "")

            # ── Identity contract (issue #62) ────────────────────────────────
            # THIS is the seam. `merged` is the complete payload: every window has
            # been extracted, nothing has been written yet, and the whole typed
            # endpoint set is known. Everything below happens before the first fact
            # POST, because after the first POST the graph has already changed under
            # the very resolution the rest of the payload depends on.
            frozen = None
            identity_evidence: Dict[str, Any] = {"mode": DOC_IDENTITY_MODE}
            # The type contract this run was produced under, plus every decision
            # the merge made on the caller's behalf. Recorded unconditionally —
            # including when there are none — so a stored run distinguishes
            # "no coercion happened" from "this build did not look" (issue #68).
            identity_evidence["type_contract"] = {
                "version": merged.get("type_contract_version"),
                "conflicts": merged.get("type_conflicts") or [],
                "unknown_types": merged.get("unknown_types") or {},
                "audited_type_decisions": {k: v for k, v in type_decisions.items()},
                "audited_alias_decisions": sorted(alias_decisions),
            }
            if merged.get("unknown_types"):
                logger.warning(
                    "extraction carried %d off-contract type(s) %s — they rank below "
                    "every admitted type in the cross-window merge",
                    len(merged["unknown_types"]), sorted(merged["unknown_types"]))
            frozen = await establish_identity(
                conn, http, merged, identity_evidence, document_rid=document_rid)

            try:
                payload = facts_to_episode_payload(merged, name=doc_title, summary=summary,
                                                   source_document=source_document,
                                                   group_id=group_id, frozen=frozen)
            except ExtractionError as e:
                await conn.execute(
                    """UPDATE document_ingestion_log
                       SET deep_extraction_last_error = $2
                     WHERE document_rid = $1""",
                    document_rid, f"{e.reason}:{e.detail}"[:2000])
                raise
            logger.info("merged: %d entities, %d facts → POST /knowledge/episodes "
                        "(group=%s, pinned=%s)",
                        len(merged["entities"]), len(merged["facts"]), group_id,
                        frozen is not None)
            ep = await post_episode_batched(http, payload)

            # ── Verify the persisted graph against the frozen map (issue #62) ──
            # BEFORE relational/discourse finalization, deliberately. A document that
            # fails here must not receive document_entity_links, discourse moves, or
            # a deep_extracted_at stamp — otherwise it reads as a finished document
            # that points at the wrong entities, which is exactly what the audited
            # Buehler import was.
            if frozen is not None:
                pinned = int(ep.get("endpoints_pinned") or 0)
                if "fact_ids" not in ep:
                    # A server that pins but does not say WHICH rows it wrote
                    # leaves verification episode-scoped, and episodes are shared
                    # across documents (review M5). Refuse rather than verify the
                    # wrong population.
                    raise ExtractionError(
                        "identity_verification_unscoped",
                        "the episode response carries no fact_ids; verification cannot "
                        "be scoped to this run's rows (server predates PR #66's M5 fix?)")
                verify = await ident.verify_persisted_graph(
                    conn, ep.get("episode_id"), frozen, fact_ids=ep.get("fact_ids") or [])
                identity_evidence["verification"] = {
                    **verify.as_evidence(), "endpoints_pinned_reported": pinned}
                if not verify.ok:
                    # Rollback receipt: name what is wrong, where, and what state the
                    # document is being left in, before raising. `deep_extracted_at`
                    # is already NULL at this point (it is only stamped at the end),
                    # so the document is retried rather than reported done — but say
                    # so explicitly rather than leaving it to be inferred.
                    receipt = {
                        "document_rid": document_rid,
                        "episode_id": str(ep.get("episode_id")),
                        "run_id": run_id,
                        "reason": "frozen_map_violation",
                        "offending_facts": verify.offending,
                        "left_state": "facts written; NOT finalized "
                                      "(no entity links, no discourse, deep_extracted_at NULL) "
                                      "— re-run to retry",
                        "identity": identity_evidence,
                    }
                    logger.error("IDENTITY VERIFICATION FAILED — %s", json.dumps(receipt)[:2000])
                    await conn.execute(
                        """UPDATE document_ingestion_log
                           SET deep_extracted_at = NULL,
                               deep_extraction_last_error = $2
                           WHERE document_rid = $1""",
                        document_rid,
                        f"identity_verification_failed:{len(verify.offending)} endpoint(s) "
                        f"outside the frozen map")
                    ident.require_binding_verified(verify)  # raises
                # A write that reported fewer pinned endpoints than the payload
                # carried means some endpoint went through the resolver after all.
                # The verification above would usually catch the consequence, but not
                # always — a resolver that happens to land on the right URI passes the
                # check while proving nothing about determinism.
                expected_pins = sum(
                    1 + (1 if f.get("object") else 0) for f in merged["facts"])
                if pinned != expected_pins:
                    raise ExtractionError(
                        "identity_pins_incomplete",
                        f"the server bound {pinned} endpoint(s) from pinned URIs but the "
                        f"payload carried {expected_pins}; {expected_pins - pinned} went "
                        f"through name resolution and are order-dependent")
                logger.info("identity verified: %d facts, %d/%d endpoints pinned, "
                            "every persisted endpoint in the frozen map",
                            verify.checked_facts, pinned, expected_pins)

            # Type-mismatches → dead-letter + cleanup task.
            mismatches = ep.get("type_mismatches") or []
            for tm in mismatches:
                await file_type_mismatch_task(http, conn, document_rid, tm)

            # Post-resolution fact dedup (merge-core correctness). The name-based
            # pre-merge AND /episodes' (predicate, object_uri) dedup both miss facts
            # that resolve to the same triple: distinct extracted names resolving to
            # one URI, and literal-object facts entirely (object_uri is NULL, so the
            # (predicate, object_uri) check never fires). Collapse by the RESOLVED key,
            # keeping the earliest row per (subject_uri, predicate, object|literal).
            episode_id = ep.get("episode_id")
            dedup_tag = await conn.execute(
                """
                WITH ranked AS (
                  SELECT id, row_number() OVER (
                    PARTITION BY subject_uri, predicate, COALESCE(object_uri, object_literal)
                    ORDER BY created_at ASC, id ASC) AS rn
                  FROM knowledge_facts WHERE episode_id = $1)
                DELETE FROM knowledge_facts f USING ranked r
                WHERE f.id = r.id AND r.rn > 1
                """, episode_id)
            dup_deleted = int(dedup_tag.split()[-1]) if dedup_tag.startswith("DELETE") else 0
            if dup_deleted:
                logger.info("post-resolution dedup: removed %d duplicate triple(s)", dup_deleted)

            # Semantic dedup (paraphrase tail) — the exact-triple sweep above misses
            # re-extraction paraphrases (fresh phrasing → distinct triple). Soft-retract
            # the LATER of a same-subject + same-predicate + same-OBJECT pair whose
            # fact_text embeddings exceed the threshold (keep earliest). OBJECT-AWARE so it
            # never merges genuinely-distinct facts (PR-#28 review): entity objects must
            # share object_uri (so "uses solar" vs "uses wind" never collapse), and literal
            # objects must be containment-related — one a more-detailed form of the other
            # ("152 initiatives" ⊂ "152 initiatives across 44 countries") — NOT merely
            # text-similar (so "$30 fee" vs "$40 fee" never collapse). Reversible (valid_to).
            sem_tag = await conn.execute(
                """
                WITH pairs AS (
                  SELECT a.id a_id, b.id b_id, a.created_at a_ct, b.created_at b_ct
                  FROM knowledge_facts a JOIN knowledge_facts b
                    ON a.subject_uri = b.subject_uri AND a.predicate = b.predicate
                   AND a.object_uri IS NOT DISTINCT FROM b.object_uri
                   AND (a.object_literal IS NULL OR b.object_literal IS NULL
                        OR position(b.object_literal IN a.object_literal) > 0
                        OR position(a.object_literal IN b.object_literal) > 0)
                   AND a.id < b.id
                   AND a.episode_id = $1 AND b.episode_id = $1
                   AND a.valid_to IS NULL AND b.valid_to IS NULL
                   AND a.fact_embedding_3072 IS NOT NULL AND b.fact_embedding_3072 IS NOT NULL
                   AND (1 - (a.fact_embedding_3072::halfvec(3072) <=> b.fact_embedding_3072::halfvec(3072))) > $2
                )
                UPDATE knowledge_facts SET valid_to = NOW()
                WHERE id IN (SELECT CASE WHEN a_ct <= b_ct THEN b_id ELSE a_id END FROM pairs)
                  AND valid_to IS NULL
                """, episode_id, SEMANTIC_DEDUP_THRESHOLD)
            sem_retracted = int(sem_tag.split()[-1]) if sem_tag.startswith("UPDATE") else 0
            if sem_retracted:
                logger.info("semantic dedup: retracted %d paraphrase-duplicate fact(s) (>%.2f)",
                            sem_retracted, SEMANTIC_DEDUP_THRESHOLD)

            # Flag 3 (plan §25): stamp source_node_rid = document_rid so a document purge
            # can key on it, not only episode_id.
            await conn.execute(
                "UPDATE knowledge_facts SET source_node_rid = $2 "
                "WHERE episode_id = $1 AND source_node_rid IS DISTINCT FROM $2",
                episode_id, document_rid)

            # Flag 2: populate the doc→entity bridge from the episode's resolved entity
            # URIs — /episodes writes facts but never document_entity_links, so
            # mentioned-in / get_entity_documents wouldn't surface this doc's entities.
            # Live-survivor resolution happens inside the statement; see FLAG2_DOC_ENTITY_LINKS_SQL.
            await conn.execute(FLAG2_DOC_ENTITY_LINKS_SQL, episode_id, document_rid, doc_title)
            entity_links = await conn.fetchval(
                "SELECT count(*) FROM document_entity_links WHERE document_rid = $1", document_rid)

            # no_residual_dups (gate regression guard): after both sweeps, assert 0 dup
            # triples REMAIN. Always 1 on a healthy run (the sweeps guarantee it); 0 only
            # if a sweep was skipped/broken — the gate floors this (the right "0 residual
            # dups" invariant, vs the old dups_ok which wrongly failed on healthy dedup).
            residual = await conn.fetchval(
                """SELECT count(*) FROM (
                     SELECT 1 FROM knowledge_facts WHERE episode_id = $1 AND valid_to IS NULL
                     GROUP BY subject_uri, predicate, COALESCE(object_uri, object_literal)
                     HAVING count(*) > 1) d""", episode_id)
            no_residual_dups = 1 if (residual or 0) == 0 else 0

            # Discourse (argument) layer — thorough tier only. Writes moves into the
            # generalized session_discourse_moves table (source_type='document').
            discourse_created = 0
            discourse_discovery = None
            if want_discourse:
                merged_moves = merge_discourse([d for d in per_window if d])
                written_move_ids: List[Any] = []
                discourse_created = await write_discourse_moves(
                    conn, document_rid=document_rid, episode_id=episode_id, moves=merged_moves,
                    written_ids=written_move_ids)
                await record_discourse_bindings(conn, document_rid=document_rid,
                                                extraction_input=extraction_input,
                                                move_ids=written_move_ids, run_id=run_id)
                logger.info("discourse: merged %d move(s) → wrote %d (source_type=document)",
                            len(merged_moves), discourse_created)

            # ── Semantic quality (#64), assessed and recorded SEPARATELY ──────
            # Deliberately does NOT gate the write. Structural completion and
            # semantic quality are different questions with different remedies: a
            # structurally broken import must not be finalized, while a thin-but-
            # sound one is a promotion decision for an operator. Collapsing them
            # would mean either blocking on a judgement call or, worse, letting a
            # green structural result keep standing in for a quality one — which is
            # the state #64 was filed about.
            quality = None
            try:
                quality = assess_extraction(
                    merged=merged,
                    chunks_by_index={idx: txt for idx, txt in chunks},
                    windows=windows,
                    tier=tier,
                    discourse_moves=(merged_moves if want_discourse else None),
                    identity_evidence=identity_evidence,
                )
                logger.info("semantic quality: %s (%s)", quality.status,
                            ", ".join(f"{d.name}={d.value}" for d in quality.dimensions))
            except Exception as e:  # noqa: BLE001
                # A measurement failure must not destroy a sound extraction, but it
                # must not read as a pass either — semantic_status stays
                # 'not_evaluated' and the reason is recorded.
                logger.warning("semantic quality assessment failed (recorded as "
                               "not_evaluated, NOT as a pass): %s", e)

            # Per-run provenance + both verdicts (migration 125). One row per
            # (document, run): re-extracting with another model creates a NEW row, so
            # runs stay separately attributable instead of overwriting each other.
            await conn.execute(
                """INSERT INTO document_extraction_runs
                     (document_rid, run_id, tier, ended_at, providers, models, transports,
                      extraction_params, structural_status, semantic_status, semantic_report,
                      identity_contract, identity_evidence)
                   VALUES ($1,$2,$3,NOW(),$4,$5,$6,$7::jsonb,$8,$9,$10::jsonb,$11,$12::jsonb)
                   ON CONFLICT (document_rid, run_id) DO UPDATE SET
                     ended_at=NOW(), providers=EXCLUDED.providers, models=EXCLUDED.models,
                     transports=EXCLUDED.transports, extraction_params=EXCLUDED.extraction_params,
                     structural_status=EXCLUDED.structural_status,
                     semantic_status=EXCLUDED.semantic_status,
                     semantic_report=EXCLUDED.semantic_report,
                     identity_contract=EXCLUDED.identity_contract,
                     identity_evidence=EXCLUDED.identity_evidence""",
                document_rid, run_id, tier,
                sorted({r["provider"] for r in window_receipts.values()}),
                sorted({r["model"] for r in window_receipts.values()}),
                sorted({r["transport"] for r in window_receipts.values()}),
                json.dumps({
                    "window_chars": WINDOW_CHARS,
                    "max_windows": MAX_WINDOWS,
                    "episode_batch_size": DOC_EPISODE_BATCH_SIZE,
                    "repair_passes_allowed": DOC_EXTRACTOR_REPAIR_PASSES,
                    "configured_transport": DOC_EXTRACTOR_TRANSPORT,
                    "schema_file": str(schema_path.name),
                    "type_contract_version": contract.DOCUMENT_TYPE_CONTRACT_VERSION,
                }),
                "fail" if (budget_exhausted or windows_failed) else "pass",
                quality.status if quality else "not_evaluated",
                json.dumps(quality.as_dict()) if quality else None,
                ident.IDENTITY_CONTRACT_VERSION if frozen else None,
                json.dumps(identity_evidence, default=str),
            )

            # Only promote windows that actually extracted. A dead-lettered window must KEEP
            # status='failed' (with its last_error) so a later run retries just that window.
            for w in windows:
                if w.index in windows_failed:
                    continue
                await conn.execute(
                    "UPDATE document_window_extractions SET status='imported', updated_at=NOW() "
                    "WHERE document_rid=$1 AND window_index=$2", document_rid, w.index)
            # deep_extracted_at stays NULL when the document is INCOMPLETE for any reason —
            # a truncated tail OR a dead-lettered window — so a partial document is retried
            # rather than reported done.
            incomplete = bool(budget_exhausted) or bool(windows_failed)
            _err_parts = []
            if budget_exhausted:
                _err_parts.append(f"budget_truncated:{MAX_WINDOWS}/{len(chunks)}")
            if windows_failed:
                _err_parts.append(f"windows_failed:{len(windows_failed)}/{len(windows)}"
                                  f":{windows_failed[:20]}")
            await conn.execute(
                """UPDATE document_ingestion_log
                   SET deep_extracted_at = CASE WHEN $2 THEN NULL ELSE NOW() END,
                       deep_extraction_attempts = 0, deep_extraction_last_error = $3
                   WHERE document_rid = $1""",
                document_rid, incomplete, ("; ".join(_err_parts) if _err_parts else None))
            if want_discourse and not incomplete:
                discourse_discovery = await schedule_discourse_discovery(
                    conn, document_rid=document_rid, move_ids=written_move_ids, run_id=run_id)

            return {
                "status": "ok", "document_rid": document_rid, "group_id": group_id,
                "windows_total": len(windows), "windows_processed": sum(1 for d in per_window if d),
                "merged_entities": len(merged["entities"]), "merged_facts": len(merged["facts"]),
                "facts_created": ep.get("facts_created"), "facts_skipped": ep.get("facts_skipped"),
                "facts_dup_removed": dup_deleted, "facts_null_embed": ep.get("facts_null_embed"),
                "semantic_dups_retracted": sem_retracted, "no_residual_dups": no_residual_dups,
                "entity_links": entity_links,
                "entities_created": ep.get("entities_created"), "entities_resolved": ep.get("entities_resolved"),
                "discourse_moves_created": discourse_created, "discourse_discovery": discourse_discovery,
                "type_mismatches": len(mismatches), "budget_exhausted": budget_exhausted,
                "windows_failed": len(windows_failed), "windows_failed_idx": windows_failed,
                "episode_id": ep.get("episode_id"),
                # ── Identity contract (#62) ──────────────────────────────────
                "identity_mode": DOC_IDENTITY_MODE,
                "identity_endpoints": len(frozen.by_name) if frozen else 0,
                "identity_distinct_uris": frozen.distinct_uris if frozen else 0,
                "endpoints_pinned": int(ep.get("endpoints_pinned") or 0),
                # 1 only when a map was frozen AND every persisted endpoint was
                # verified against it. warn/off runs report 0 — they did not verify,
                # and "did not verify" must not read the same as "verified clean".
                "identity_verified": 1 if (
                    frozen is not None
                    and identity_evidence.get("verification", {}).get("binding_verified")
                ) else 0,
                "identity_evidence": identity_evidence,
                "quality": quality.as_dict() if quality else {"status": "not_evaluated"},
                # ── Producer provenance (#64) ────────────────────────────────
                "providers": sorted({r["provider"] for r in window_receipts.values()}),
                "models": sorted({r["model"] for r in window_receipts.values()}),
                "transports": sorted({r["transport"] for r in window_receipts.values()}),
                # 1 when every window that ran in THIS run recorded a producer.
                # Cache-resumed windows are excluded from the denominator on
                # purpose: their producer was recorded when they were extracted,
                # and claiming this run produced them would be the misattribution
                # #64 is about.
                "producers_recorded": 1 if all(
                    w.index in window_receipts
                    for w in windows
                    if w.index not in windows_failed and per_window[w.index] is not None
                    and w.index not in _cached_window_indices
                ) else 0,
            }
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat_task
            await _lease_release(conn, document_rid)
            await conn.execute("SELECT pg_advisory_unlock(hashtext('deep-extract-doc:' || $1));", document_rid)


async def amain(args) -> int:
    pp, sp = prompt_schema_for_tier(args.tier)
    if not pp.exists() or not sp.exists():
        print(f"Error: prompt/schema missing ({pp}, {sp})", file=sys.stderr)
        return 1

    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
    try:
        document_rid = args.document_rid
        if not document_rid and args.slug:
            async with pool.acquire() as conn:
                document_rid = await conn.fetchval(
                    "SELECT rid FROM koi_memories WHERE source_sensor=$1 "
                    "AND metadata->>'slug' = $2 ORDER BY updated_at DESC LIMIT 1",
                    args.source_sensor, args.slug)
        if not document_rid:
            print("Error: provide --document-rid or a --slug that resolves to one", file=sys.stderr)
            return 1

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
        # #36: the pool settings that actually fix CLOSE_WAIT reuse (keepalive_expiry
        # + TCP keepalive) can only come from the CLIENT — a per-request `timeout=`
        # scalar does not reliably fire on a half-closed pooled socket (observed: a
        # 40-minute hang against timeout=300). Per-request overrides below still
        # apply on top of these per-phase ceilings.
        async with provider_async_client(read=ANTHROPIC_TIMEOUT) as http:
            result = await extract_deep_document(
                pool, http, document_rid=document_rid, tier=args.tier,
                group_id=args.group_id, run_id=run_id, force=args.force,
                source_sensor=args.source_sensor)
    finally:
        await pool.close()

    print("\nDocument deep-extraction result:")
    for k, v in result.items():
        print(f"  {k}: {v}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--document-rid", help="document:<sha256> RID")
    g.add_argument("--slug", help="resolve the RID from koi_memories metadata slug")
    parser.add_argument("--tier", default="standard", choices=["standard", "thorough"],
                        help="standard = entities+facts (v1 prompt); thorough = +discourse (v2 prompt)")
    parser.add_argument("--group-id", help="override learning field (default: the document's group_id)")
    parser.add_argument("--force", action="store_true", help="re-extract all windows (ignore cache)")
    parser.add_argument("--source-sensor", default="document-ingest",
                        help="koi_memories.source_sensor to read RAG rows from "
                             "(default: document-ingest; e.g. substack-corpus-backfill for Substack posts)")
    args = parser.parse_args()
    try:
        return asyncio.run(amain(args))
    except ExtractionError as e:
        print(f"ExtractionError: {e}", file=sys.stderr)
        return 1
    except ident.IdentityError as e:
        # A blocked identity contract is an expected, documented outcome of a
        # strict run — not a crash. Say what blocked, in the same shape the gate
        # prints, and exit 1 like every other refusal.
        print(f"IdentityError: {e}", file=sys.stderr)
        for b in (e.blockers or [])[:10]:
            print(f"  BLOCKER {b.get('state')}: {b.get('name')!r} ({b.get('type')})",
                  file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
