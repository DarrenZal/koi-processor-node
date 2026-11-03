#!/usr/bin/env python3
"""
Local BGE Embedding Server
Runs BAAI/bge-large-en-v1.5 locally for privacy-preserving embeddings.
No data leaves your machine.

Usage:
    python bge_server_local.py  # Runs on port 8091
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
import uvicorn
import logging
import hashlib
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Local BGE Embedding Server", version="1.0.0")

# Model configuration
MODEL_NAME = "BAAI/bge-large-en-v1.5"
EMBEDDING_DIM = 1024

# Global model reference (lazy loaded)
_model = None
_model_load_time = None

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


def get_model():
    """Lazy load the model on first use."""
    global _model, _model_load_time

    if _model is None:
        logger.info(f"Loading model: {MODEL_NAME}")
        start = time.time()

        try:
            from sentence_transformers import SentenceTransformer
            _model = SentenceTransformer(MODEL_NAME)
            _model_load_time = time.time() - start
            logger.info(f"Model loaded in {_model_load_time:.2f}s")
        except ImportError:
            raise RuntimeError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )

    return _model


class EmbeddingRequest(BaseModel):
    text: Optional[str] = None
    input: Optional[str] = None  # Alternative field name


class EmbeddingResponse(BaseModel):
    embedding: List[float]
    dim: int = EMBEDDING_DIM
    model: str = MODEL_NAME
    cached: bool = False


class BatchEmbeddingRequest(BaseModel):
    texts: List[str]


class BatchEmbeddingResponse(BaseModel):
    embeddings: List[List[float]]
    dim: int = EMBEDDING_DIM
    model: str = MODEL_NAME


@app.on_event("startup")
async def startup_event():
    """Pre-load model on startup for faster first request."""
    logger.info("Pre-loading BGE model...")
    try:
        get_model()
        logger.info("Model ready!")
    except Exception as e:
        logger.warning(f"Model pre-load failed (will retry on first request): {e}")


@app.post("/encode", response_model=EmbeddingResponse)
async def generate_embedding(request: EmbeddingRequest):
    """Generate embedding for a single text (with caching)."""
    text = request.text or request.input

    if not text:
        raise HTTPException(status_code=400, detail="No text provided")

    # Check cache first
    cache_key = hashlib.sha256(text.encode('utf-8')).hexdigest()

    if cache_key in embedding_cache:
        logger.debug(f"Cache hit for text of length {len(text)}")
        return EmbeddingResponse(
            embedding=embedding_cache[cache_key],
            cached=True
        )

    # Generate embedding
    model = get_model()
    start = time.time()
    embedding = model.encode(text, normalize_embeddings=True)
    elapsed = time.time() - start

    embedding_list = embedding.tolist()

    # Cache the result (limit cache size)
    if len(embedding_cache) < 10000:
        embedding_cache[cache_key] = embedding_list

    logger.info(f"Generated embedding in {elapsed*1000:.1f}ms (len={len(text)}, cache={len(embedding_cache)})")

    return EmbeddingResponse(
        embedding=embedding_list,
        cached=False
    )


@app.post("/encode_batch", response_model=BatchEmbeddingResponse)
async def generate_embeddings_batch(request: BatchEmbeddingRequest):
    """Generate embeddings for multiple texts at once (more efficient)."""
    if not request.texts:
        raise HTTPException(status_code=400, detail="No texts provided")

    model = get_model()
    start = time.time()

    # Batch encode is much more efficient
    embeddings = model.encode(request.texts, normalize_embeddings=True, show_progress_bar=False)
    elapsed = time.time() - start

    embeddings_list = [e.tolist() for e in embeddings]

    logger.info(f"Batch encoded {len(request.texts)} texts in {elapsed*1000:.1f}ms ({elapsed*1000/len(request.texts):.1f}ms/text)")

    return BatchEmbeddingResponse(embeddings=embeddings_list)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    model_loaded = _model is not None
    return {
        "status": "healthy",
        "service": "Local BGE Embedding Server",
        "model": MODEL_NAME,
        "model_loaded": model_loaded,
        "model_load_time": f"{_model_load_time:.2f}s" if _model_load_time else None,
        "embedding_dim": EMBEDDING_DIM,
        "cache_size": len(embedding_cache),
        "privacy": "All processing is local - no data leaves your machine"
    }


@app.get("/cache/stats")
async def cache_stats():
    """Get cache statistics."""
    return {
        "cache_size": len(embedding_cache),
        "max_size": 10000
    }


@app.post("/cache/clear")
async def clear_cache():
    """Clear the embedding cache."""
    global embedding_cache
    old_size = len(embedding_cache)
    embedding_cache = {}
    return {"cleared": old_size}


if __name__ == "__main__":
    print(f"[Local BGE Server] Model: {MODEL_NAME}")
    print(f"[Local BGE Server] Embedding dimension: {EMBEDDING_DIM}")
    print(f"[Local BGE Server] Starting on http://localhost:8091")
    print(f"[Local BGE Server] Privacy: All data stays on your machine")
    uvicorn.run(app, host="0.0.0.0", port=8091, log_level="info")
