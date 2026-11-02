"""documents_router.py — unified document ingestion over HTTP.

  POST /documents/ingest      — trigger an ingest (background job); 202 + document_rid
  GET  /documents/{rid}       — poll status (document_ingestion_log) + gate evidence

This is the HTTP front door for `scripts/ingest_document.py`'s depth-dial
orchestrator (rag | standard | thorough). standard/thorough are ~15-minute jobs
(per-window LLM extraction), so the endpoint runs the pipeline as a BACKGROUND task
and returns immediately with the content-addressed document_rid; callers poll the
status endpoint. The pipeline is idempotent + resumable (document_window_extractions
cache), so a worker restart mid-run is recovered by re-POSTing the same source.

Security (plan §16): service-token gated (KOI_INGEST_SERVICE_TOKEN) and the endpoint
REQUIRES INGEST_SOURCE_ROOT to be set — it will not ingest an arbitrary server-side
path over HTTP without an allowlist root. The source path is canonicalised +
allowlist-checked (resolve_allowed_source_path) before the job starts, so a bad path
returns 400 synchronously rather than failing silently in the background.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import asyncpg
from asyncpg.pool import Pool
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from api.auth_deps import make_service_token_auth

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
STATE_DIR = Path(os.path.expanduser("~/.config/personal-koi/koi-state/doc-ingest"))
VALID_TIERS = ("rag", "standard", "thorough")

# Keep strong refs to in-flight background tasks so they are not GC'd mid-run.
_BG_TASKS: set = set()


def _load_ingest_module():
    """Load scripts/ingest_document.py (not an api-package module) by path.
    Its own module-level sys.path.insert puts the repo root on the path so its
    `from api.* import ...` and sibling-extractor load resolve."""
    p = REPO_ROOT / "scripts" / "ingest_document.py"
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    spec = importlib.util.spec_from_file_location("ingest_document", str(p))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class IngestRequest(BaseModel):
    source_path: str
    tier: str = "standard"
    slug: Optional[str] = None
    name: Optional[str] = None
    source_url: Optional[str] = None
    retrieval_method: Optional[str] = None
    group_id: str = "personal"
    no_claims: bool = False
    force: bool = False


def _state_path(document_rid: str) -> Path:
    safe = document_rid.replace(":", "_").replace("/", "_")
    return STATE_DIR / f"{safe}.json"


def _write_state(document_rid: str, payload: dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    _state_path(document_rid).write_text(json.dumps(payload, default=str, indent=2))


async def _run_ingest(mod, req: IngestRequest, document_rid: str) -> None:
    """Background runner: execute the pipeline, persist result + gate evidence."""
    try:
        result = await mod.ingest_path(
            source_path=req.source_path, tier=req.tier, slug=req.slug, name=req.name,
            source_url=req.source_url, retrieval_method=req.retrieval_method,
            group_id=req.group_id, claims=(req.tier != "rag") and not req.no_claims,
            force=req.force, dry_run=False)
        _write_state(document_rid, {"status": "complete", "tier": req.tier,
                                    "gate_evidence": result.get("gate_evidence"), "result": result})
        logger.info("document ingest complete: %s (tier=%s)", document_rid, req.tier)
    except Exception as e:  # noqa: BLE001 — surface into the status file, never crash the worker
        logger.exception("document ingest failed: %s", document_rid)
        _write_state(document_rid, {"status": "failed", "tier": req.tier, "error": str(e)})


def create_router(db_pool: Pool) -> APIRouter:
    router = APIRouter()
    require_service = make_service_token_auth(
        db_pool, env_var="KOI_INGEST_SERVICE_TOKEN", service_identity="service:document-ingest")

    @router.post("/ingest", status_code=202)
    async def ingest(req: IngestRequest, identity: str = Depends(require_service)):
        if req.tier not in VALID_TIERS:
            raise HTTPException(400, f"invalid tier {req.tier!r}; must be one of {VALID_TIERS}")
        if not os.getenv("INGEST_SOURCE_ROOT"):
            raise HTTPException(
                400, "INGEST_SOURCE_ROOT must be set for the HTTP ingest endpoint (path-safety, plan §16)")
        mod = _load_ingest_module()
        # Validate + content-address the source eagerly so a bad path is a synchronous
        # 400, and the caller gets the document_rid to poll.
        try:
            path = mod.resolve_allowed_source_path(req.source_path)
            markdown = path.read_text(encoding="utf-8", errors="replace")
            if not markdown.strip():
                raise ValueError(f"source file is empty: {path}")
            document_rid = mod.compute_document_rid(markdown)[1]
        except ValueError as e:
            raise HTTPException(400, str(e))

        _write_state(document_rid, {"status": "running", "tier": req.tier})
        task = asyncio.create_task(_run_ingest(mod, req, document_rid))
        _BG_TASKS.add(task)
        task.add_done_callback(_BG_TASKS.discard)
        return {"document_rid": document_rid, "status": "running", "tier": req.tier,
                "status_url": f"/documents/{document_rid}"}

    @router.get("/{document_rid}")
    async def status(document_rid: str, identity: str = Depends(require_service)):
        sp = _state_path(document_rid)
        state = json.loads(sp.read_text()) if sp.exists() else {"status": "unknown"}
        try:
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT tier, chunk_count, window_count, rag_chunked_at, deep_extracted_at, "
                    "claims_extracted_at, deep_extraction_last_error, last_ingested_at "
                    "FROM document_ingestion_log WHERE document_rid = $1", document_rid)
        except asyncpg.exceptions.UndefinedTableError:
            # document-ingest migrations (102+) not applied on this DB → the feature is
            # unavailable here. Degrade to 503 rather than 500-ing the recovery-polling path.
            raise HTTPException(
                503, "document-ingest unavailable: tracking tables absent (apply migrations 102-104)")
        return {"document_rid": document_rid, "state": state, "log": dict(row) if row else None}

    return router
