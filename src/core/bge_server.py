#!/usr/bin/env python3
"""
Embedding Server - OpenAI API Edition
Provides embeddings via HTTP API for KOI Event Bridge
Uses OpenAI text-embedding-3-large for high-quality, fast embeddings
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
import uvicorn
import logging
import hashlib
import os
import httpx
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Embedding Server", version="2.0.0")

# OpenAI API configuration
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = "text-embedding-3-large"
EMBEDDING_DIM = 1024  # Configurable dimension (OpenAI supports 256-3072)

# In-memory cache for embeddings
embedding_cache: Dict[str, List[float]] = {}

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class EmbeddingRequest(BaseModel):
    text: Optional[str] = None
    input: Optional[str] = None  # Alternative field name

class EmbeddingResponse(BaseModel):
    embedding: List[float]
    dim: int = 1024

async def get_openai_embedding(text: str) -> List[float]:
    """Get embedding from OpenAI API"""
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY not configured")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "input": text,
                    "model": OPENAI_MODEL,
                    "dimensions": EMBEDDING_DIM
                },
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
            return data["data"][0]["embedding"]
        except httpx.HTTPError as e:
            logger.error(f"OpenAI API error: {e}")
            raise HTTPException(status_code=502, detail=f"OpenAI API error: {str(e)}")

@app.post("/encode", response_model=EmbeddingResponse)
async def generate_embedding(request: EmbeddingRequest):
    """Generate embedding for text (with caching)"""
    # Get text from either field
    text = request.text or request.input

    if not text:
        raise HTTPException(status_code=400, detail="No text provided")

    # Check cache first (use hash as key)
    cache_key = hashlib.sha256(text.encode('utf-8')).hexdigest()

    if cache_key in embedding_cache:
        logger.debug(f"✓ Cache hit for text of length {len(text)}")
        return EmbeddingResponse(
            embedding=embedding_cache[cache_key],
            dim=EMBEDDING_DIM
        )

    # Get embedding from OpenAI API
    logger.info(f"→ Calling OpenAI API for text of length {len(text)}")
    embedding_list = await get_openai_embedding(text)

    # Cache the result (limit cache size to 10000 entries)
    if len(embedding_cache) < 10000:
        embedding_cache[cache_key] = embedding_list
        logger.debug(f"✓ Cached embedding (cache size: {len(embedding_cache)})")

    logger.info(f"✓ Generated embedding via OpenAI (cache size: {len(embedding_cache)})")

    return EmbeddingResponse(
        embedding=embedding_list,
        dim=EMBEDDING_DIM
    )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    api_configured = OPENAI_API_KEY is not None
    return {
        "status": "healthy",
        "service": "Embedding Server (OpenAI)",
        "model": OPENAI_MODEL,
        "embedding_dim": EMBEDDING_DIM,
        "api_configured": api_configured,
        "cache_size": len(embedding_cache),
        "cache_hit_ratio": "tracked in logs"
    }

@app.get("/cache/stats")
async def cache_stats():
    """Get cache statistics"""
    return {
        "cache_size": len(embedding_cache),
        "max_cache_size": 10000,
        "memory_usage_mb": len(embedding_cache) * 1024 * 4 / 1024 / 1024  # Approximate
    }

@app.post("/cache/clear")
async def clear_cache():
    """Clear the embedding cache"""
    global embedding_cache
    old_size = len(embedding_cache)
    embedding_cache = {}
    return {
        "status": "cleared",
        "entries_removed": old_size
    }

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        print("⚠️  WARNING: OPENAI_API_KEY not found in environment")
        print("Set it with: export OPENAI_API_KEY='your-key-here'")
    print(f"[Embedding Server] Starting on http://localhost:8090")
    print(f"[Embedding Server] Model: {OPENAI_MODEL}")
    print(f"[Embedding Server] Dimensions: {EMBEDDING_DIM}")
    print(f"[Embedding Server] API Key: {'✓ Configured' if OPENAI_API_KEY else '✗ Missing'}")
    uvicorn.run(app, host="0.0.0.0", port=8090, log_level="info")