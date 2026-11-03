#!/usr/bin/env python3
"""
Code Graph Service - Standalone listener for processing GitHub events
and populating Apache AGE graph database with code entities.

Runs parallel to Event Bridge v2:
- Listens to same KOI events from coordinator
- Filters for GitHub source code events
- Extracts code entities and loads into Apache AGE

Usage:
    python code_graph_service.py
"""

import os
from dotenv import load_dotenv

# Load .env file from koi-processor directory
load_dotenv("/opt/projects/koi-processor/.env")
import asyncio
import logging
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

from code_graph_processor import CodeGraphProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Code Graph Service",
    description="Processes GitHub events to extract code entities into Apache AGE graph",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration from environment
POSTGRES_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza')
GRAPH_NAME = os.getenv('GRAPH_NAME', 'regen_graph')
CODE_GRAPH_ENABLED = os.getenv('CODE_GRAPH_ENABLED', 'true').lower() == 'true'
CODE_GRAPH_REPOS = os.getenv('CODE_GRAPH_REPOS', 'regen-ledger,regen-web,regen-data-standards,koi-sensors,koi-processor,koi-research,regen-koi-mcp').split(',')

# Parse PostgreSQL URL into psycopg2 config
def parse_postgres_url(url: str) -> Dict[str, str]:
    """Parse PostgreSQL URL into psycopg2 connection parameters"""
    # postgresql://user:password@host:port/database
    import re
    match = re.match(r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)', url)
    if match:
        user, password, host, port, database = match.groups()
        return {
            'user': user,
            'password': password,
            'host': host,
            'port': int(port),
            'database': database
        }
    # Fallback defaults
    return {
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': 5433,
        'database': 'eliza'
    }

DB_CONFIG = parse_postgres_url(POSTGRES_URL)

# Global processor instance (reused for efficiency)
processor = None

# Pydantic models - KOI Protocol compliant
class KOIManifest(BaseModel):
    rid: str
    timestamp: str
    content_hash: str
    size_bytes: int
    content_type: str
    version: str = "1.0"
    metadata: Dict[str, Any] = None

class KOIBundle(BaseModel):
    rid: str
    manifest: KOIManifest
    contents: Dict[str, Any]

class KOIEvent(BaseModel):
    event_type: str  # NEW, UPDATE, FORGET
    rid: str
    source_node: str
    timestamp: str
    bundle: KOIBundle = None
    reason: str = None

class ProcessingResult(BaseModel):
    success: bool
    rid: str
    entities_extracted: int
    entities_loaded: int
    repo: str = None
    file_path: str = None
    error: str = None

# Statistics
stats = {
    'events_received': 0,
    'events_processed': 0,
    'events_skipped': 0,
    'entities_extracted': 0,
    'entities_loaded': 0,
    'errors': 0,
    'by_repo': {},
    'by_language': {},
    'start_time': datetime.now()
}


@app.on_event("startup")
async def startup_event():
    """Initialize processor on startup"""
    global processor
    
    if not CODE_GRAPH_ENABLED:
        logger.warning("Code graph processing is DISABLED (set CODE_GRAPH_ENABLED=true to enable)")
        return
    
    logger.info("=" * 60)
    logger.info("Code Graph Service Starting")
    logger.info("=" * 60)
    logger.info(f"PostgreSQL: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    logger.info(f"Graph Name: {GRAPH_NAME}")
    logger.info(f"Enabled Repos: {', '.join(CODE_GRAPH_REPOS)}")
    logger.info("=" * 60)
    
    # Create processor instance
    processor = CodeGraphProcessor(DB_CONFIG, GRAPH_NAME)
    processor.graph_enabled_repos = CODE_GRAPH_REPOS
    
    logger.info("✓ Code Graph Service ready")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global processor
    if processor:
        processor.close()
        logger.info("✓ Processor closed")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Code Graph Service",
        "version": "1.0.0",
        "status": "running" if CODE_GRAPH_ENABLED else "disabled",
        "graph_name": GRAPH_NAME,
        "enabled_repos": CODE_GRAPH_REPOS
    }


@app.get("/stats")
async def get_stats():
    """Get processing statistics"""
    uptime = (datetime.now() - stats['start_time']).total_seconds()
    return {
        **stats,
        'uptime_seconds': uptime,
        'events_per_minute': (stats['events_received'] / uptime * 60) if uptime > 0 else 0
    }


@app.post("/process-koi-event")
async def process_koi_event(event: KOIEvent) -> ProcessingResult:
    """Process a KOI event for code graph extraction"""
    global stats
    stats['events_received'] += 1
    
    if not CODE_GRAPH_ENABLED:
        stats['events_skipped'] += 1
        return ProcessingResult(
            success=False,
            rid=event.rid,
            entities_extracted=0,
            entities_loaded=0,
            error="Code graph processing is disabled"
        )
    
    try:
        # Convert Pydantic model to dict for processor
        event_dict = event.dict()
        
        # Process the event
        result = await processor.process_event(event_dict)
        
        if result['processed']:
            stats['events_processed'] += 1
            stats['entities_loaded'] += result['entities_loaded']
            
            # Extract file info for response
            bundle = event_dict.get('bundle', {})
            manifest = bundle.get('manifest', {})
            metadata = manifest.get('metadata', {})
            file_path = metadata.get('file_path', '')
            repo = processor.extract_repo_from_path(file_path)
            
            # Update repo stats
            if repo not in stats['by_repo']:
                stats['by_repo'][repo] = {'events': 0, 'entities': 0}
            stats['by_repo'][repo]['events'] += 1
            stats['by_repo'][repo]['entities'] += result['entities_loaded']
            
            logger.info(f"✓ Processed {file_path}: {result['entities_loaded']} entities loaded")
            
            return ProcessingResult(
                success=True,
                rid=event.rid,
                entities_extracted=result['entities_loaded'],  # We don't track separately yet
                entities_loaded=result['entities_loaded'],
                repo=repo,
                file_path=file_path
            )
        else:
            stats['events_skipped'] += 1
            return ProcessingResult(
                success=True,
                rid=event.rid,
                entities_extracted=0,
                entities_loaded=0,
                error="Event skipped (not a code file or repo not enabled)"
            )
    
    except Exception as e:
        stats['errors'] += 1
        logger.error(f"Error processing event {event.rid}: {e}", exc_info=True)
        return ProcessingResult(
            success=False,
            rid=event.rid,
            entities_extracted=0,
            entities_loaded=0,
            error=str(e)
        )


@app.post("/test-extraction")
async def test_extraction(source_code: str, language: str, file_path: str = "test.py", repo: str = "test-repo"):
    """Test code entity extraction without loading to graph"""
    if not CODE_GRAPH_ENABLED:
        raise HTTPException(status_code=503, detail="Code graph processing is disabled")
    
    try:
        entities = processor.extractor.extract_entities(source_code, file_path, language, repo)
        return {
            "entities_extracted": len(entities),
            "entities": [e.to_dict() for e in entities]
        }
    except Exception as e:
        logger.error(f"Error in test extraction: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """Health check for monitoring"""
    if not CODE_GRAPH_ENABLED:
        return {"status": "disabled"}
    
    try:
        # Test database connection
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        
        return {
            "status": "healthy",
            "database": "connected",
            "graph_name": GRAPH_NAME
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


def main():
    """Main entry point"""
    import uvicorn
    
    port = int(os.getenv('CODE_GRAPH_PORT', 8300))
    
    logger.info(f"Starting Code Graph Service on port {port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning"
    )


if __name__ == "__main__":
    main()
