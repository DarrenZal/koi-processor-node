#!/usr/bin/env python3
"""
Adaptive Extraction API Service
Exposes the Python adaptive extractor via HTTP API
"""

import asyncio
import os
from typing import List, Dict, Any, Optional
import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
from loguru import logger

from src.core.adaptive_extractor import AdaptiveExtractor

# Request/Response models
class ExtractionRequest(BaseModel):
    query: str
    search_results: List[Dict[str, Any]]
    user_id: Optional[str] = "web-user"
    agent_id: Optional[str] = "koi-interface"

class ExtractionResponse(BaseModel):
    success: bool
    extraction_triggered: bool
    confidence: float
    receipt_rid: Optional[str] = None
    facts_extracted: int = 0
    entities_extracted: int = 0
    relationships_extracted: int = 0
    confidence_improvement: float = 0.0
    cost_usd: float = 0.0
    message: str = ""

# FastAPI app
app = FastAPI(
    title="KOI Adaptive Extraction API",
    description="Adaptive knowledge extraction service for KOI pipeline",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global extractor instance
extractor = None
db_pool = None

async def init_services():
    """Initialize database and extractor"""
    global extractor, db_pool
    
    try:
        # Connect to database
        POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5433/eliza")
        db_pool = await asyncpg.create_pool(POSTGRES_URL)
        logger.info("✅ Database connection established")
        
        # Initialize extractor
        openai_key = os.getenv("OPENAI_API_KEY")
        if not openai_key:
            logger.warning("⚠️ OPENAI_API_KEY not set - extraction will fail")
            
        extractor = AdaptiveExtractor(
            db_pool=db_pool,
            llm_api_key=openai_key
        )
        logger.info("✅ Adaptive extractor initialized")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize services: {e}")
        raise

@app.on_event("startup")
async def startup_event():
    await init_services()

@app.on_event("shutdown")
async def shutdown_event():
    if db_pool:
        await db_pool.close()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "adaptive-extraction-api",
        "extractor_ready": extractor is not None,
        "database_ready": db_pool is not None
    }

@app.post("/extract", response_model=ExtractionResponse)
async def extract_knowledge(request: ExtractionRequest):
    """
    Trigger adaptive knowledge extraction for a query
    """
    if not extractor:
        raise HTTPException(status_code=503, detail="Extractor not initialized")
    
    try:
        logger.info(f"🔧 Processing extraction request for query: '{request.query[:50]}...'")
        
        # Process query through adaptive extractor
        enhanced_results, extraction_result = await extractor.process_query(
            query=request.query,
            search_results=request.search_results,
            user_id=request.user_id,
            agent_id=request.agent_id
        )
        
        # Build response
        if extraction_result:
            response = ExtractionResponse(
                success=True,
                extraction_triggered=True,
                confidence=extractor.calculate_confidence(request.search_results),
                receipt_rid=extraction_result.receipt_rid,
                facts_extracted=len(extraction_result.extracted_facts),
                entities_extracted=len(extraction_result.extracted_entities),
                relationships_extracted=len(extraction_result.extracted_relationships),
                confidence_improvement=extraction_result.confidence_improvement,
                cost_usd=extraction_result.cost_usd,
                message=f"Extraction completed successfully. {len(extraction_result.extracted_facts)} facts extracted."
            )
            logger.info(f"✅ Extraction completed: {len(extraction_result.extracted_facts)} facts, {len(extraction_result.extracted_entities)} entities")
        else:
            confidence = extractor.calculate_confidence(request.search_results)
            response = ExtractionResponse(
                success=True,
                extraction_triggered=False,
                confidence=confidence,
                message=f"Extraction not triggered (confidence {confidence:.3f} >= {extractor.CONFIDENCE_THRESHOLD})"
            )
            logger.info(f"ℹ️ Extraction not triggered - confidence {confidence:.3f} above threshold")
        
        return response
        
    except Exception as e:
        logger.error(f"❌ Extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")

@app.get("/stats")
async def get_extraction_stats():
    """Get extraction statistics"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    try:
        async with db_pool.acquire() as conn:
            # Recent extractions
            recent_extractions = await conn.fetch("""
                SELECT 
                    COUNT(*) as total_extractions,
                    AVG(confidence_improvement) as avg_improvement,
                    SUM(extraction_cost_usd) as total_cost,
                    MAX(extraction_timestamp) as last_extraction
                FROM koi_adaptive_extractions 
                WHERE extraction_timestamp > NOW() - INTERVAL '24 hours'
            """)
            
            # Query stats
            query_stats = await conn.fetch("""
                SELECT 
                    COUNT(*) as total_queries,
                    AVG(confidence_score) as avg_confidence,
                    COUNT(*) FILTER (WHERE triggered_extraction) as triggered_count
                FROM koi_query_log 
                WHERE timestamp > NOW() - INTERVAL '24 hours'
            """)
            
            return {
                "extraction_stats": recent_extractions[0] if recent_extractions else {},
                "query_stats": query_stats[0] if query_stats else {},
                "timestamp": "recent_24h"
            }
            
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=f"Stats query failed: {str(e)}")

if __name__ == "__main__":
    # Run the server
    uvicorn.run(
        "adaptive_extraction_api:app",
        host="0.0.0.0",
        port=8351,
        reload=True,
        log_level="info"
    )