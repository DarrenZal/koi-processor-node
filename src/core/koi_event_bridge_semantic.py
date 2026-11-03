#!/usr/bin/env python3
"""
KOI Event Bridge with Semantic Extraction
Extends v2 with LLM extraction, knowledge graph, and CAT receipt chain
"""

import os
import sys
import json
import asyncio
import asyncpg
import httpx
import hashlib
import logging
import uuid
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

if __name__ == "__main__":
    import argparse

    # A server: run with no arguments it starts on 0.0.0.0:8004, as before. --help prints
    # this and exits, before the optional packages below (rdflib) are imported; other
    # arguments are ignored, as before.
    argparse.ArgumentParser(
        description="KOI Event Bridge (semantic): serves /events/process on 0.0.0.0:8004, "
                    "extracts with OpenAI and writes to the database POSTGRES_URL names and to "
                    "Fuseki on localhost:3030. Run with no arguments to start it."
    ).parse_known_args()

# Load environment variables from .env file
load_dotenv()

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import semantic extraction components
# from extraction.llm_extractor import OntologyLLMExtractor  # Ollama version
from extraction.openai_extractor import OpenAIExtractor  # OpenAI GPT-4o-mini version
from extraction.metadata_resolver import MetadataResolver
from extraction.smart_chunker import SmartChunker
from knowledge_graph.graph_integration import KnowledgeGraphIntegrator
from cat.cat_receipt_chain import CATReceiptChain

# Import from existing event bridge
from core.koi_event_bridge_v2 import (
    KOIManifest, KOIBundle, KOIEvent, ProcessingResult,
    generate_embedding_bge, check_existing_memory
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Helper function to serialize objects to JSON with datetime handling
def json_serialize(obj):
    """Convert an object to JSON-serializable format, handling datetime objects"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: json_serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [json_serialize(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(json_serialize(item) for item in obj)
    else:
        return obj

# FastAPI app
app = FastAPI(title="KOI Event Bridge Semantic", version="3.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
DB_URL = os.getenv('POSTGRES_URL')
BGE_API_URL = os.getenv('BGE_API_URL', 'http://localhost:8090/encode')
USE_ISOLATED_TABLES = os.getenv('USE_ISOLATED_TABLES', 'true').lower() == 'true'
ENABLE_LLM_EXTRACTION = os.getenv('ENABLE_LLM_EXTRACTION', 'true').lower() == 'true'
OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434')

# Semantic extraction components. init_components() creates them when the server starts
# (or before uvicorn.run when this file is run as a script), not at import: the knowledge
# graph integrator opens a PostgreSQL connection when it is created.
llm_extractor = None
metadata_resolver = None
smart_chunker = None
kg_integrator = None
cat_chain = None


def init_components() -> None:
    """Create the extraction, knowledge-graph and CAT receipt components, once."""
    global llm_extractor, metadata_resolver, smart_chunker, kg_integrator, cat_chain
    if cat_chain is not None:
        return
    if not DB_URL:
        raise RuntimeError("POSTGRES_URL is not set; the semantic event bridge reads its database connection from the environment")

    # Initialize semantic extraction components
    # Use OpenAI GPT-4o-mini for extraction instead of Ollama
    llm_extractor = OpenAIExtractor(
        model=os.getenv('OPENAI_EXTRACT_MODEL', 'gpt-4o-mini'),
        use_batch_api=False  # Set to True for batch processing
    )

    metadata_resolver = MetadataResolver()

    smart_chunker = SmartChunker()

    kg_integrator = KnowledgeGraphIntegrator(
        store_type="sparql",  # Use SPARQL endpoint (Apache Jena Fuseki)
        store_config={
            "query_endpoint": "http://localhost:3030/koi/sparql",
            "update_endpoint": "http://localhost:3030/koi/update"
        }
    )

    cat_chain = CATReceiptChain(db_config={"dsn": DB_URL})

# Initialize CAT chain on startup
@app.on_event("startup")
async def startup_event():
    """Create the components and initialize the CAT receipt chain on startup"""
    init_components()
    await cat_chain.initialize()
    logger.info("Semantic Event Bridge initialized with CAT receipt chain")

def get_source_type(source_node: str) -> str:
    """Map KOI source to ontology source type"""
    source_mapping = {
        'discourse': 'discourse',
        'twitter': 'twitter',
        'medium': 'medium',
        'github': 'github',
        'telegram': 'telegram',
        'website': 'website',
        'notion': 'website',
        'gitlab': 'github',
        'podcast': 'medium'
    }
    
    source_lower = source_node.lower()
    for key, value in source_mapping.items():
        if key in source_lower:
            return value
    
    return 'website'  # Default to website ontology

async def extract_and_chunk_content(
    event: KOIEvent,
    text_content: str,
    metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract semantic metadata and chunk content with CAT receipts
    
    Returns:
        Dictionary containing chunks, metadata, and CAT receipt RID
    """
    result = {
        "chunks": [],
        "metadata": metadata,
        "cat_receipt_rid": None,
        "extraction_report": None,
        "kg_report": None
    }
    
    try:
        # Create initial CAT receipt for sensor collection
        sensor_receipt = await cat_chain.create_sensor_receipt(
            sensor_name=event.source_node,
            source_url=metadata.get('url', 'unknown'),
            content_cid=event.bundle.rid,
            document_count=1,
            metadata=metadata
        )
        result["cat_receipt_rid"] = sensor_receipt.rid
        
        # Apply LLM extraction if enabled
        enhanced_metadata = metadata
        extraction_receipt = None
        resolved_metadata = metadata  # Default to sensor metadata

        if ENABLE_LLM_EXTRACTION:
            try:
                # Determine source type
                source_type = get_source_type(event.source_node)

                # Extract semantic metadata with confidence scores
                llm_extraction = await llm_extractor.extract_metadata(
                    content=text_content,
                    source_type=source_type,
                    existing_metadata=metadata
                )

                # Resolve metadata conflicts between sensor and LLM
                if llm_extraction.get('llm_extracted_metadata'):
                    resolved_metadata = metadata_resolver.resolve_metadata(
                        sensor_metadata=metadata,
                        llm_metadata=llm_extraction.get('llm_extracted_metadata', {}),
                        llm_confidence=llm_extraction.get('llm_metadata_confidence', {})
                    )
                    # Merge resolved metadata with extraction results
                    enhanced_metadata = {**llm_extraction, **resolved_metadata}
                else:
                    enhanced_metadata = llm_extraction
                
                # Create extraction CAT receipt
                entities_count = len(enhanced_metadata.get('extracted_entities', []))
                relationships_count = len(enhanced_metadata.get('extracted_relationships', []))
                
                extraction_receipt = await cat_chain.create_extraction_receipt(
                    parent_rid=sensor_receipt.rid,
                    content_cid=f"{event.bundle.rid}:extracted",
                    model=llm_extractor.model,
                    ontology=source_type,
                    entities_count=entities_count,
                    relationships_count=relationships_count
                )
                
                result["extraction_report"] = {
                    "entities": entities_count,
                    "relationships": relationships_count,
                    "source_type": source_type
                }
                
                logger.info(f"LLM extraction: {entities_count} entities, {relationships_count} relationships")
                
            except Exception as e:
                logger.warning(f"LLM extraction failed: {e}")
                enhanced_metadata = metadata
        
        # Update result metadata
        result["metadata"] = enhanced_metadata
        
        # Integrate into knowledge graph if extraction succeeded
        graph_receipt = None
        if enhanced_metadata.get('extracted_entities') or enhanced_metadata.get('extracted_relationships'):
            try:
                # Create document for graph integration
                document = {
                    'rid': event.bundle.rid,
                    'url': metadata.get('url', ''),
                    'title': enhanced_metadata.get('title', ''),
                    'content': text_content,
                    'source_type': get_source_type(event.source_node),
                    'metadata': enhanced_metadata
                }
                
                # Integrate into knowledge graph
                kg_report = kg_integrator.integrate_document(
                    document=document,
                    extraction_metadata=enhanced_metadata
                )
                
                # Create graph integration receipt
                parent_rid = extraction_receipt.rid if extraction_receipt else sensor_receipt.rid
                graph_receipt = await cat_chain.create_graph_receipt(
                    parent_rid=parent_rid,
                    content_cid=f"{event.bundle.rid}:graph",
                    triples_added=kg_report.get('triples_added', 0),
                    store_type=kg_integrator.store_type
                )
                
                result["kg_report"] = kg_report
                result["cat_receipt_rid"] = graph_receipt.rid
                
                logger.info(f"Graph integration: {kg_report.get('triples_added', 0)} triples added")
                
            except Exception as e:
                logger.warning(f"Knowledge graph integration failed: {e}")
        
        # Use intelligent chunking based on source type and extracted entities
        chunk_results = smart_chunker.chunk_content(
            content=text_content,
            source_type=get_source_type(event.source_node),
            extracted_entities=enhanced_metadata.get('extracted_entities', []),
            metadata=resolved_metadata
        )

        # Extract just the text from chunk results
        chunks = [chunk['text'] for chunk in chunk_results]
        
        # Create chunking receipt
        parent_rid = graph_receipt.rid if graph_receipt else (
            extraction_receipt.rid if extraction_receipt else sensor_receipt.rid
        )
        
        chunk_receipt = await cat_chain.create_chunking_receipt(
            parent_rid=parent_rid,
            content_cid=f"{event.bundle.rid}:chunks",
            chunk_count=len(chunks),
            chunk_size=1000,
            overlap=200
        )
        
        result["chunks"] = chunks
        result["cat_receipt_rid"] = chunk_receipt.rid
        
    except Exception as e:
        logger.error(f"Error in semantic extraction pipeline: {e}")
        # Mark extraction as failed - let caller decide how to handle
        # Don't create chunks on failure to prevent orphan issues
        result["extraction_failed"] = True
        result["extraction_error"] = str(e)
        result["chunks"] = []  # Don't create chunks automatically on failure

    return result

def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """Chunk text into smaller pieces with overlap"""
    if not text:
        return []
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        # Try to end at a sentence boundary
        if end < len(text):
            last_period = chunk.rfind('.')
            last_newline = chunk.rfind('\n')
            break_point = max(last_period, last_newline)
            
            if break_point > chunk_size * 0.5:
                chunk = chunk[:break_point + 1]
                end = start + break_point + 1
        
        chunks.append(chunk.strip())
        start = end - overlap
    
    return chunks

async def process_koi_event_semantic(event: KOIEvent) -> ProcessingResult:
    """Process a KOI event with semantic extraction"""
    start_time = time.time()

    # Validate event has bundle
    if not event.bundle:
        logger.error(f"Event missing bundle: {event}")
        return ProcessingResult(
            success=False,
            rid=event.rid if hasattr(event, 'rid') else "",
            cid="",
            chunks_created=0,
            embeddings_created=0,
            error="Event missing bundle"
        )

    try:
        async with asyncpg.create_pool(DB_URL) as pool:
            async with pool.acquire() as conn:
                # Check for existing memory
                existing = await check_existing_memory(conn, event.bundle.rid)
                
                # Handle FORGET events
                if event.event_type == "FORGET":
                    if existing and USE_ISOLATED_TABLES:
                        await conn.execute("""
                            UPDATE koi_memories 
                            SET superseded_at = $1 
                            WHERE rid = $2 AND superseded_at IS NULL
                        """, datetime.now(tz=timezone.utc), event.bundle.rid)
                    
                    return ProcessingResult(
                        success=True,
                        rid=event.bundle.rid,
                        cid="",
                        chunks_created=0,
                        embeddings_created=0
                    )
                
                # Skip if NEW and already exists
                if event.event_type == "NEW" and existing:
                    logger.info(f"Content {event.bundle.rid} already exists, skipping")
                    return ProcessingResult(
                        success=False,
                        rid=event.bundle.rid,
                        cid="",
                        chunks_created=0,
                        embeddings_created=0,
                        error="Content already exists"
                    )
                
                # Extract text content
                text_content = extract_text_from_bundle(event.bundle)
                if not text_content:
                    return ProcessingResult(
                        success=False,
                        rid=event.bundle.rid,
                        cid="",
                        chunks_created=0,
                        embeddings_created=0,
                        error="No text content found"
                    )
                
                # Apply semantic extraction and chunking
                extraction_result = await extract_and_chunk_content(
                    event,
                    text_content,
                    event.bundle.manifest.metadata or {}
                )

                # Handle extraction failures gracefully
                if extraction_result.get("extraction_failed"):
                    logger.warning(
                        f"LLM extraction failed for {event.bundle.rid}: "
                        f"{extraction_result.get('extraction_error')}, using basic chunking"
                    )
                    chunks = chunk_text(text_content, chunk_size=1000, overlap=200)
                    enhanced_metadata = extraction_result["metadata"]
                else:
                    chunks = extraction_result["chunks"]
                    enhanced_metadata = extraction_result["metadata"]

                # Validate we have chunks to process
                if not chunks:
                    return ProcessingResult(
                        success=False,
                        rid=event.bundle.rid,
                        cid="",
                        chunks_created=0,
                        embeddings_created=0,
                        error="No chunks created - document processing failed"
                    )

                cat_receipt_rid = extraction_result["cat_receipt_rid"]
                
                # Implement three-layer storage architecture with transaction wrapping
                # This ensures atomicity: either ALL storage succeeds or NONE does
                chunks_created = 0
                embeddings_created = 0

                # Get original metadata from the bundle
                original_metadata = event.bundle.manifest.metadata or {}

                # Also merge metadata from bundle contents if available
                content_metadata = {}
                if event.bundle and event.bundle.contents:
                    if isinstance(event.bundle.contents, dict):
                        # Check for metadata field in contents
                        if 'metadata' in event.bundle.contents:
                            content_metadata = event.bundle.contents['metadata']
                            logger.info(f"Found metadata in bundle contents with keys: {list(content_metadata.keys())}")
                            if 'published_at' in content_metadata:
                                logger.info(f"Found published_at in content metadata: {content_metadata['published_at']}")

                # Layer 1: Store raw content in koi_content
                raw_content = json.dumps(event.bundle.contents)  # Store original bundle contents
                content_type = "json"  # Could be enhanced to detect HTML/text
                content_rid = f"content:{event.bundle.rid}"

                # Layer 2: Store processed document in koi_memories
                document_rid = event.bundle.rid

                # Extract URL and title from bundle contents
                url = None
                title = None
                if event.bundle and event.bundle.contents:
                    if isinstance(event.bundle.contents, dict):
                        # Try to get URL from document field
                        if 'document' in event.bundle.contents:
                            doc = event.bundle.contents['document']
                            if isinstance(doc, dict):
                                url = doc.get('url', '')
                                title = doc.get('title', '')
                        # Also check top-level URL
                        if not url:
                            url = event.bundle.contents.get('url', '')
                        if not title:
                            title = event.bundle.contents.get('title', '')

                # Merge original and enhanced metadata for document
                document_metadata = {
                    **original_metadata,
                    **content_metadata,
                    **enhanced_metadata,
                    "url": url or original_metadata.get("url") or enhanced_metadata.get("url"),
                    "title": title or original_metadata.get("title") or enhanced_metadata.get("title"),
                    "cat_receipt_rid": cat_receipt_rid,
                    "extraction_report": extraction_result.get("extraction_report"),
                    "kg_report": extraction_result.get("kg_report")
                }

                # Promote published date from various sources to root metadata fields
                published_at = document_metadata.get("published_at")

                # First check sensor data (most reliable)
                if not published_at and "sources" in document_metadata:
                    if "sensor" in document_metadata["sources"]:
                        sensor_data = document_metadata["sources"]["sensor"]
                        if "published_at" in sensor_data and sensor_data["published_at"]:
                            published_at = sensor_data["published_at"]
                            logger.info(f"Using sensor date for published_at: {sensor_data['published_at']}")

                    # Fall back to LLM extracted date if no sensor date
                    if not published_at and "llm" in document_metadata["sources"]:
                        llm_data = document_metadata["sources"]["llm"]
                        if "published_date" in llm_data and llm_data["published_date"]:
                            published_at = llm_data["published_date"]
                            logger.info(f"Using LLM date for published_at: {llm_data['published_date']}")

                # Also check top-level sensor fields
                if not published_at:
                    if "sensor_published_at" in document_metadata:
                        published_at = document_metadata["sensor_published_at"]
                        logger.info(f"Using sensor_published_at: {published_at}")
                    elif "published_date" in document_metadata:
                        published_at = document_metadata["published_date"]
                        logger.info(f"Using published_date: {published_at}")

                # Set the final published_at value
                if published_at and not document_metadata.get("published_at"):
                    document_metadata["published_at"] = published_at

                # Promote confidence score if available
                if "confidence_scores" in document_metadata:
                    date_confidence = document_metadata["confidence_scores"].get("published_date")
                    if date_confidence and not document_metadata.get("published_confidence"):
                        document_metadata["published_confidence"] = date_confidence
                        logger.info(f"Promoted date confidence: {date_confidence}")

                # Collect chunk data for embedding generation (done outside transaction)
                chunk_embedding_data = []

                # ============================================================
                # TRANSACTION BLOCK: Ensures atomicity of all storage operations
                # Either ALL of Layer 1 + Layer 2 + Layer 3 succeed, or NONE do
                # This prevents orphaned chunks when parent document creation fails
                # ============================================================
                async with conn.transaction():
                    if USE_ISOLATED_TABLES:
                        await store_raw_content(
                            conn,
                            content_rid,
                            raw_content,
                            content_type,
                            {**original_metadata, **content_metadata},
                            event
                        )

                    if USE_ISOLATED_TABLES:
                        await store_processed_document(
                            conn,
                            document_rid,
                            content_rid,
                            text_content,
                            document_metadata,
                            event
                        )

                    # Layer 3: Store chunks in koi_memory_chunks
                    # First, clean up old chunks if this is an UPDATE (prevents orphaned chunks)
                    if event.event_type == "UPDATE" and USE_ISOLATED_TABLES:
                        # Delete old chunks that won't be replaced
                        # (e.g., if old content had 10 chunks but new has only 5)
                        await conn.execute("""
                            DELETE FROM koi_memory_chunks
                            WHERE document_rid = $1
                              AND chunk_index >= $2
                        """, document_rid, len(chunks))
                        logger.info(f"Cleaned up old chunks for {document_rid} (keeping {len(chunks)} chunks)")

                    for i, chunk in enumerate(chunks):
                        chunk_rid = f"{event.bundle.rid}:chunk_{i}"

                        # Preserve key metadata in chunks for temporal filtering
                        chunk_metadata = {
                            **document_metadata,
                            "chunk_index": i,
                            "total_chunks": len(chunks)
                        }

                        if USE_ISOLATED_TABLES:
                            chunk_id = await store_memory_chunk(
                                conn,
                                chunk_rid,
                                document_rid,
                                content_rid,
                                i,
                                len(chunks),
                                chunk,
                                chunk_metadata
                            )
                        else:
                            # Legacy storage for non-isolated tables
                            chunk_id = str(uuid.uuid4())
                            await store_legacy_memory_chunk(
                                conn,
                                chunk_id,
                                event.bundle.rid,
                                f"chunk_{i}",
                                chunk,
                                chunk_metadata,
                                event
                            )

                        chunks_created += 1
                        # Collect chunk data for embedding generation outside transaction
                        chunk_embedding_data.append((chunk_rid, chunk_id, chunk, i))

                # ============================================================
                # EMBEDDING GENERATION: Done OUTSIDE transaction
                # Embedding generation involves external API calls which:
                # 1. Can be slow and would block the transaction
                # 2. Should not cause storage rollback if they fail
                # ============================================================
                for chunk_rid, chunk_id, chunk_text, chunk_index in chunk_embedding_data:
                    embedding = await generate_embedding_bge(chunk_text)
                    if embedding:
                        # Store embedding linked to chunk
                        if USE_ISOLATED_TABLES:
                            # Update chunk table with embedding
                            embedding_str = '[' + ','.join(map(str, embedding)) + ']'
                            await conn.execute("""
                                UPDATE koi_memory_chunks
                                SET embedding = $1::vector(1024)
                                WHERE chunk_rid = $2
                            """, embedding_str, chunk_rid)
                        else:
                            await store_embedding(conn, chunk_id, embedding)

                        # Create embedding CAT receipt
                        await cat_chain.create_embedding_receipt(
                            parent_rid=cat_receipt_rid,
                            content_cid=f"{event.bundle.rid}:chunk_{chunk_index}:embedding",
                            model="bge-m3",
                            dimension=len(embedding)
                        )
                        embeddings_created += 1
                
                # Calculate CID
                cid = f"cid:sha256:{event.bundle.manifest.content_hash}"
                
                elapsed = time.time() - start_time
                logger.info(f"Processed {event.bundle.rid} with semantic extraction in {elapsed:.2f}s")
                
                return ProcessingResult(
                    success=True,
                    rid=event.bundle.rid,
                    cid=cid,
                    chunks_created=chunks_created,
                    embeddings_created=embeddings_created,
                    version=existing['version'] + 1 if existing else 1
                )
                
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        return ProcessingResult(
            success=False,
            rid=event.bundle.rid if event.bundle else "",
            cid="",
            chunks_created=0,
            embeddings_created=0,
            error=str(e)
        )

def extract_text_from_bundle(bundle: KOIBundle) -> str:
    """Extract text content from KOI bundle"""
    text_parts = []
    
    # Handle different content structures
    if isinstance(bundle.contents, dict):
        # Look for text fields
        if 'text' in bundle.contents:
            text_parts.append(str(bundle.contents['text']))
        if 'content' in bundle.contents:
            text_parts.append(str(bundle.contents['content']))
        if 'document' in bundle.contents:
            doc = bundle.contents['document']
            if isinstance(doc, dict):
                if 'content' in doc:
                    text_parts.append(str(doc['content']))
                if 'text' in doc:
                    text_parts.append(str(doc['text']))
        
        # Look for title/description
        if 'title' in bundle.contents:
            text_parts.append(f"Title: {bundle.contents['title']}")
        if 'description' in bundle.contents:
            text_parts.append(f"Description: {bundle.contents['description']}")
    
    elif isinstance(bundle.contents, str):
        text_parts.append(bundle.contents)
    
    elif isinstance(bundle.contents, list):
        for item in bundle.contents:
            if isinstance(item, str):
                text_parts.append(item)
            elif isinstance(item, dict) and 'text' in item:
                text_parts.append(str(item['text']))
    
    return '\n\n'.join(text_parts)

async def store_raw_content(
    conn: asyncpg.Connection,
    content_rid: str,
    raw_content: str,
    content_type: str,
    metadata: Dict[str, Any],
    event: KOIEvent
) -> None:
    """Store raw content in koi_content table (Layer 1)"""
    content_hash = hashlib.sha256(raw_content.encode('utf-8')).hexdigest()

    # Check if content already exists with same hash
    existing = await conn.fetchrow("""
        SELECT rid FROM koi_content WHERE content_hash = $1
    """, content_hash)

    if existing:
        logger.info(f"Content already exists with same hash, skipping raw storage: {existing['rid']}")
        return

    # Extract URL from bundle contents if available
    url = None
    title = None
    if event.bundle and event.bundle.contents:
        if isinstance(event.bundle.contents, dict):
            # Try to get URL from document field
            if 'document' in event.bundle.contents:
                doc = event.bundle.contents['document']
                if isinstance(doc, dict):
                    url = doc.get('url', '')
                    title = doc.get('title', '')
            # Also check top-level URL
            if not url:
                url = event.bundle.contents.get('url', '')
            if not title:
                title = event.bundle.contents.get('title', '')

    # Fall back to metadata if no URL in bundle
    if not url:
        url = metadata.get('url', '')
    if not title:
        title = metadata.get('title', '')

    # Store raw content with URL and title
    await conn.execute("""
        INSERT INTO koi_content (
            rid, raw_content, content_type, content_hash, scraped_at, metadata, url, title
        ) VALUES ($1, $2, $3, $4, NOW(), $5, $6, $7)
        ON CONFLICT (rid) DO UPDATE SET
            raw_content = $2,
            content_type = $3,
            content_hash = $4,
            scraped_at = NOW(),
            metadata = $5,
            url = $6,
            title = $7
    """, content_rid, raw_content, content_type, content_hash, json.dumps(json_serialize(metadata)), url, title)

    logger.info(f"Stored raw content in koi_content: {content_rid} with URL: {url}")


async def store_processed_document(
    conn: asyncpg.Connection,
    document_rid: str,
    source_content_rid: str,
    text_content: str,
    metadata: Dict[str, Any],
    event: KOIEvent
) -> None:
    """Store processed document in koi_memories (Layer 2)"""
    # Extract publication date from metadata
    published_at = None
    published_confidence = 0.0

    logger.debug(f"Processing metadata for document {document_rid}: keys={list(metadata.keys())}")

    # Try published_date first (from sensor/LLM extraction)
    if 'published_date' in metadata:
        date_str = metadata['published_date']
        if isinstance(date_str, str):
            try:
                # Handle various date formats
                if 'T' in date_str or ' ' in date_str:
                    # ISO format with time
                    published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                else:
                    # Date only format (YYYY-MM-DD)
                    published_at = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                published_confidence = metadata.get('published_confidence', 0.8)
            except Exception as e:
                logger.debug(f"Could not parse published_date: {date_str}, error: {e}")

    # Fall back to published_at if no published_date
    elif 'published_at' in metadata:
        date_str = metadata['published_at']
        if isinstance(date_str, str):
            try:
                published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                published_confidence = metadata.get('published_confidence', 0.9)
            except Exception as e:
                logger.debug(f"Could not parse published_at: {date_str}, error: {e}")

    # Calculate content hash for document
    content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

    # Privacy: promote metadata is_private/access_source to dedicated columns.
    # Sticky-OR on ON CONFLICT — once-private-stays-private (Phase 1 / tech-backlog #23).
    is_private = bool((metadata or {}).get('is_private', False))
    access_source = (metadata or {}).get('access_source')

    # Store document in koi_memories (not chunks)
    await conn.execute("""
        INSERT INTO koi_memories (
            id, rid, cid, version, event_type, source_sensor,
            content, metadata, published_at, published_confidence,
            content_hash, source_content_rid, is_chunk,
            is_private, access_source
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, FALSE, $13, $14)
        ON CONFLICT (rid) DO UPDATE SET
            content = $7,
            metadata = $8,
            published_at = $9,
            published_confidence = $10,
            content_hash = $11,
            source_content_rid = $12,
            is_private = (koi_memories.is_private OR EXCLUDED.is_private),
            access_source = COALESCE(koi_memories.access_source, EXCLUDED.access_source),
            updated_at = NOW()
    """,
        str(uuid.uuid4()),
        document_rid,
        f"cid:sha256:{content_hash}",
        1,
        event.event_type,
        event.source_node,
        json.dumps({
            "text": text_content,
            "title": metadata.get('title', ''),
            "url": metadata.get('url', ''),
            "source_type": metadata.get('source_type', ''),
            "source_id": metadata.get('source_id', '')
        }),
        json.dumps(json_serialize(metadata)),
        published_at,
        published_confidence,
        content_hash,
        source_content_rid,
        is_private,
        access_source
    )

    logger.info(f"Stored processed document in koi_memories: {document_rid}")


async def store_memory_chunk(
    conn: asyncpg.Connection,
    chunk_rid: str,
    document_rid: str,
    source_content_rid: str,
    chunk_index: int,
    total_chunks: int,
    content: str,
    metadata: Dict[str, Any]
) -> str:
    """Store a memory chunk in koi_memory_chunks table (Layer 3)

    Note: This function should be called within a transaction that has already
    created the parent document. The parent verification guard ensures data
    integrity even if called outside a transaction context.
    """
    # Verify parent document exists before creating chunk (defense-in-depth)
    # This guard prevents orphaned chunks even if transaction wrapping fails
    parent_exists = await conn.fetchval(
        "SELECT 1 FROM koi_memories WHERE rid = $1",
        document_rid
    )
    if not parent_exists:
        raise ValueError(
            f"Cannot create chunk: parent document {document_rid} does not exist. "
            "This indicates a transaction isolation issue or storage order problem."
        )

    chunk_id = str(uuid.uuid4())

    # Preserve key metadata fields for temporal filtering
    chunk_metadata = {
        "url": metadata.get("url"),
        "title": metadata.get("title"),
        "published_at": metadata.get("published_at") or metadata.get("published_date"),
        "published_confidence": metadata.get("published_confidence", 0.0),
        "chunk_index": chunk_index,
        "total_chunks": total_chunks,
        **{k: v for k, v in metadata.items() if k not in ['published_at', 'published_date']}
    }

    # Store chunk in new koi_memory_chunks table
    await conn.execute("""
        INSERT INTO koi_memory_chunks (
            chunk_rid, document_rid, source_content_rid, chunk_index, total_chunks,
            content, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (chunk_rid) DO UPDATE SET
            content = $6,
            metadata = $7,
            created_at = NOW()
    """,
        chunk_rid,
        document_rid,
        source_content_rid,
        chunk_index,
        total_chunks,
        json.dumps({"text": content}),
        json.dumps(json_serialize(chunk_metadata))
    )

    logger.info(f"Stored chunk {chunk_index}/{total_chunks} in koi_memory_chunks: {chunk_rid}")
    return chunk_id


async def store_legacy_memory_chunk(
    conn: asyncpg.Connection,
    memory_id: str,
    rid: str,
    chunk_id: str,
    content: str,
    metadata: Dict[str, Any],
    event: KOIEvent
) -> None:
    """Store a memory chunk with metadata (legacy format)"""
    if USE_ISOLATED_TABLES:
        # Extract publication date - check both published_date and published_at
        published_at = None
        published_confidence = 0.0

        # Try published_date first (from sensor/LLM extraction)
        if 'published_date' in metadata:
            date_str = metadata['published_date']
            if isinstance(date_str, str):
                try:
                    # Handle various date formats
                    if 'T' in date_str or ' ' in date_str:
                        # ISO format with time
                        published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    else:
                        # Date only format (YYYY-MM-DD)
                        from datetime import datetime, timezone
                        published_at = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    published_confidence = metadata.get('published_confidence', 0.8)
                except Exception as e:
                    logger.debug(f"Could not parse published_date: {date_str}, error: {e}")

        # Fall back to published_at if no published_date
        elif 'published_at' in metadata:
            date_str = metadata['published_at']
            if isinstance(date_str, str):
                try:
                    published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    published_confidence = metadata.get('published_confidence', 0.9)
                except Exception as e:
                    logger.debug(f"Could not parse published_at: {date_str}, error: {e}")

        # Calculate content hash
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()

        # Privacy: promote metadata is_private/access_source to dedicated columns
        # (Phase 1 / tech-backlog #23). ON CONFLICT (id) DO NOTHING means
        # initial-INSERT is the only write path here; sticky-OR is moot.
        chunk_is_private = bool((metadata or {}).get('is_private', False))
        chunk_access_source = (metadata or {}).get('access_source')

        # Store in koi_memories
        await conn.execute("""
            INSERT INTO koi_memories (
                id, rid, cid, version, event_type, source_sensor,
                content, metadata, published_at, published_confidence,
                content_hash, is_private, access_source
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (id) DO NOTHING
        """,
            memory_id,
            f"{rid}:{chunk_id}",
            f"{rid}:{chunk_id}",
            1,
            event.event_type,
            event.source_node,
            json.dumps({"text": content}),
            json.dumps(json_serialize(metadata)),
            published_at,
            published_confidence,
            content_hash,
            chunk_is_private,
            chunk_access_source
        )
    else:
        # Store in legacy memories table
        agent_id = "8e1e4498-b3c8-0fae-ad1f-e90d1c1a4331"
        await conn.execute("""
            INSERT INTO memories (id, type, content, "agentId", "createdAt")
            VALUES ($1::uuid, 'koi_chunk', $2::jsonb, $3::uuid, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO NOTHING
        """,
            memory_id,
            json.dumps({
                "text": content,
                "rid": f"{rid}:{chunk_id}",
                "metadata": metadata
            }),
            agent_id
        )

async def store_embedding(conn: asyncpg.Connection, memory_id: str, embedding: List[float]) -> bool:
    """Store embedding in database (legacy format)"""
    if not embedding:
        return False

    embedding_str = '[' + ','.join(map(str, embedding)) + ']'
    embedding_dim = len(embedding)

    try:
        if USE_ISOLATED_TABLES:
            if embedding_dim == 768:
                await conn.execute("""
                    INSERT INTO koi_embeddings (memory_id, dim_768)
                    VALUES ($1, $2::vector(768))
                    ON CONFLICT (memory_id) DO UPDATE SET dim_768 = $2::vector(768)
                """, memory_id, embedding_str)
            elif embedding_dim == 1024:
                await conn.execute("""
                    INSERT INTO koi_embeddings (memory_id, dim_1024)
                    VALUES ($1, $2::vector(1024))
                    ON CONFLICT (memory_id) DO UPDATE SET dim_1024 = $2::vector(1024)
                """, memory_id, embedding_str)
            else:
                logger.warning(f"Unsupported embedding dimension: {embedding_dim}")
                return False
        return True
    except Exception as e:
        logger.error(f"Error storing embedding: {e}")
        return False

# API Endpoints
@app.post("/events/process")
async def process_event(event: KOIEvent):
    """Process a single KOI event with semantic extraction"""

    # Log the received event for debugging
    logger.info(f"Received event: {event}")
    logger.info(f"Event type: {event.event_type}")
    logger.info(f"Event has bundle: {event.bundle is not None}")

    # If event doesn't have a bundle but has data, create one
    if not event.bundle and hasattr(event, 'data') and event.data:
        from core.koi_event_bridge_v2 import KOIBundle, KOIManifest

        # Create bundle from data
        event.bundle = KOIBundle(
            rid=event.rid or event.data.get('rid', ''),
            cid=event.data.get('cid', ''),
            manifest=KOIManifest(
                title=event.data.get('title', ''),
                description=event.data.get('description', ''),
                content=event.data.get('content', ''),
                url=event.data.get('url', ''),
                metadata=event.data
            )
        )
        logger.info(f"Created bundle from event data with RID: {event.bundle.rid}")

    result = await process_koi_event_semantic(event)
    status_code = 200 if result.success else 500
    return JSONResponse(status_code=status_code, content=result.dict())

@app.post("/events/batch")
async def process_batch(events: List[KOIEvent]):
    """Process a batch of KOI events"""
    results = []
    for event in events:
        result = await process_koi_event_semantic(event)
        results.append(result.dict())
    return {"processed": len(results), "results": results}

@app.get("/cat/receipt/{rid}")
async def get_cat_receipt(rid: str):
    """Get a CAT receipt by RID"""
    receipt = await cat_chain.get_receipt(rid)
    if receipt:
        return receipt.to_dict()
    raise HTTPException(status_code=404, detail="Receipt not found")

@app.get("/cat/chain/{rid}")
async def get_cat_chain(rid: str):
    """Get complete CAT receipt chain"""
    chain = await cat_chain.get_chain(rid)
    return {"chain": [r.to_dict() for r in chain]}

@app.get("/cat/provenance/{rid}")
async def get_provenance(rid: str):
    """Get complete provenance report"""
    report = await cat_chain.get_provenance_report(rid)
    return report

@app.get("/kg/statistics")
async def get_kg_statistics():
    """Get knowledge graph statistics"""
    stats = kg_integrator.get_statistics()
    return stats

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "3.0.0",
        "semantic_extraction": ENABLE_LLM_EXTRACTION,
        "kg_store": kg_integrator.store_type,
        "llm_model": llm_extractor.model
    }

if __name__ == "__main__":
    import uvicorn

    # Arguments were parsed at the top of the file (--help exits there).
    init_components()
    uvicorn.run(app, host="0.0.0.0", port=8004)