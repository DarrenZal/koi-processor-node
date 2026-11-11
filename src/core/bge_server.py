#!/usr/bin/env python3
"""
Embedding Server - OpenAI API Edition
Provides embeddings via HTTP API for the KOI event bridge (ingest) and query API
(search). Uses OpenAI text-embedding-3-large.

Hardening (A8): concurrency is bounded and queries are reserved capacity so a
bulk ingest cannot starve interactive search; slot acquisition is deadline-bound
(503 on exhaustion, below the smallest caller timeout); inputs over a cap are
rejected (413); the interactive docs and wildcard CORS are off; the bind host is
configurable; the unused cache-admin routes are removed. bge serves no private
data, so it carries no service key (the loopback bind, gated on a read-only
docker-bridge check, is its network protection). The pure logic lives in
``bge_hardening`` so it can be unit tested without the web framework.
"""

import asyncio
import hashlib
import logging
import os
import sys
from typing import Dict, List, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import bge_hardening

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Interactive docs disabled: this is an internal service, not a public API.
app = FastAPI(
    title="Embedding Server",
    version="2.1.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# OpenAI API configuration
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 1024  # Configurable dimension (OpenAI supports 256-3072)

# In-memory cache for embeddings
embedding_cache: Dict[str, List[float]] = {}
MAX_CACHE_ENTRIES = 10000

# Concurrency limiter: bounds upstream calls and reserves query capacity. Created
# on startup (and lazily on first request) so its semaphores bind to the running
# event loop, not to import time.
limiter: Optional[bge_hardening.LaneLimiter] = None


@app.on_event("startup")
async def _init_limiter():
    global limiter
    if limiter is None:
        limiter = bge_hardening.LaneLimiter()

# Wildcard CORS removed: no browser calls this internal service.


class EmbeddingRequest(BaseModel):
    text: Optional[str] = None
    input: Optional[str] = None  # Alternative field name
    # "ingest" marks a bulk/background request; anything else (incl. unset) is
    # treated as an interactive "query" so it is never starved by ingest.
    lane: Optional[str] = None


class EmbeddingResponse(BaseModel):
    embedding: List[float]
    dim: int = 1024


async def get_openai_embedding(text: str) -> List[float]:
    """Get embedding from OpenAI API. Caller holds a limiter slot."""
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY not configured")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "input": text,
                    "model": OPENAI_MODEL,
                    "dimensions": EMBEDDING_DIM,
                },
                timeout=bge_hardening.openai_timeout(),
            )
            response.raise_for_status()
            data = response.json()
            return data["data"][0]["embedding"]
        except httpx.HTTPError as e:
            logger.error(f"OpenAI API error: {e}")
            raise HTTPException(status_code=502, detail=f"OpenAI API error: {str(e)}")


@app.post("/encode", response_model=EmbeddingResponse)
async def generate_embedding(request: EmbeddingRequest):
    """Generate an embedding for text (with caching and bounded concurrency)."""
    text = request.text or request.input

    if not text:
        raise HTTPException(status_code=400, detail="No text provided")

    if bge_hardening.input_too_large(text):
        raise HTTPException(
            status_code=413,
            detail=f"input exceeds {bge_hardening.max_input_chars()} characters",
        )

    # Cache hits never enter the limiter.
    cache_key = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if cache_key in embedding_cache:
        logger.debug(f"Cache hit for text of length {len(text)}")
        return EmbeddingResponse(embedding=embedding_cache[cache_key], dim=EMBEDDING_DIM)

    global limiter
    if limiter is None:
        limiter = bge_hardening.LaneLimiter()
    lane = bge_hardening.normalize_lane(request.lane)
    try:
        async with limiter.slot(lane, bge_hardening.acquire_timeout()):
            logger.info(f"Calling OpenAI API (lane={lane}, len={len(text)})")
            embedding_list = await get_openai_embedding(text)
    except asyncio.TimeoutError:
        logger.warning(f"Embedding server busy; rejecting {lane} request (len={len(text)})")
        raise HTTPException(status_code=503, detail="embedding server busy, retry later")

    if len(embedding_cache) < MAX_CACHE_ENTRIES:
        embedding_cache[cache_key] = embedding_list

    return EmbeddingResponse(embedding=embedding_list, dim=EMBEDDING_DIM)


@app.get("/health")
async def health_check():
    """Health check endpoint (public; no upstream call, no secret)."""
    return {
        "status": "healthy",
        "service": "Embedding Server (OpenAI)",
        "model": OPENAI_MODEL,
        "embedding_dim": EMBEDDING_DIM,
        "api_configured": OPENAI_API_KEY is not None,
        "cache_size": len(embedding_cache),
        "max_concurrency": bge_hardening.max_concurrency(),
        "ingest_max_concurrency": bge_hardening.ingest_max(),
    }


def _preflight() -> int:
    """Validate configuration without starting the server. Exit 0 if ready."""
    logger.info(
        "bge preflight: bind=%s max_concurrency=%d ingest_max=%d "
        "acquire_timeout=%.1fs openai_timeout=%.1fs max_input_chars=%d",
        bge_hardening.bind_host(),
        bge_hardening.max_concurrency(),
        bge_hardening.ingest_max(),
        bge_hardening.acquire_timeout(),
        bge_hardening.openai_timeout(),
        bge_hardening.max_input_chars(),
    )
    budget = bge_hardening.acquire_timeout() + bge_hardening.openai_timeout()
    logger.info(
        "bge preflight: worst-case per-request budget %.1fs (acquire + upstream); "
        "keep it below the smallest caller timeout (the bridge's KOI_BGE_TIMEOUT, "
        "default 120s), or that caller will time out before bge answers",
        budget,
    )
    if not OPENAI_API_KEY:
        logger.error("bge preflight: FAIL - OPENAI_API_KEY not configured")
        return 1
    logger.info("bge preflight: OK")
    return 0


if __name__ == "__main__":
    if "--preflight" in sys.argv:
        sys.exit(_preflight())

    import uvicorn

    bind = bge_hardening.bind_host()
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY not found in environment; /encode will 500")
    logger.info(f"[Embedding Server] Starting on http://{bind}:8090")
    logger.info(f"[Embedding Server] Model: {OPENAI_MODEL}, dim: {EMBEDDING_DIM}")
    logger.info(
        f"[Embedding Server] max_concurrency={bge_hardening.max_concurrency()} "
        f"ingest_max={bge_hardening.ingest_max()} "
        f"worst_case_budget={bge_hardening.acquire_timeout() + bge_hardening.openai_timeout():.1f}s "
        f"(keep below the smallest caller timeout, e.g. bridge KOI_BGE_TIMEOUT=120s)"
    )
    uvicorn.run(app, host=bind, port=8090, log_level="info")
