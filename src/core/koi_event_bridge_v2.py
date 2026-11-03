#!/usr/bin/env python3
"""
KOI Event Bridge v2 - With Deduplication and Versioning
Processes KOI events and generates embeddings with proper version control
"""

import os
import json
import asyncio
import asyncpg
import httpx
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging
import uuid
import time

# Import CAT receipt creation
from create_cat_receipt import create_cat_receipt, create_embedding_receipt

# Import event filter
from koi_event_filter import filter_koi_event

# Import provenance to RDF
from provenance_to_rdf import ProvenanceToRDF

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="KOI Event Bridge v2", version="2.0.0")

# Add CORS middleware to allow dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
DB_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza')
BGE_API_URL = os.getenv('BGE_API_URL', 'http://localhost:8090/encode')
USE_ISOLATED_TABLES = os.getenv('USE_ISOLATED_TABLES', 'true').lower() == 'true'
KG_EXTRACTION_ENABLED = os.getenv('KG_EXTRACTION_ENABLED', 'false').lower() == 'true'
LEDGER_ENTITY_INDEXING_ENABLED = os.getenv('LEDGER_ENTITY_INDEXING_ENABLED', 'true').lower() == 'true'

# Ledger entity RID prefixes for entity_registry indexing
LEDGER_ENTITY_PREFIXES = [
    'orn:regen.credit_class:',
    'orn:regen.project:',
    'orn:regen.organization:',
]

# Code file extensions that should skip KG extraction (still get embeddings for search)
CODE_FILE_EXTENSIONS = {
    '.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs',  # JavaScript/TypeScript
    '.py', '.pyi', '.pyx',  # Python
    '.go', '.rs', '.rb', '.php',  # Other languages
    '.java', '.kt', '.scala', '.groovy',  # JVM languages
    '.c', '.cpp', '.cc', '.h', '.hpp',  # C/C++
    '.cs', '.fs',  # .NET
    '.swift', '.m', '.mm',  # Apple
    '.sol',  # Solidity
    '.sh', '.bash', '.zsh',  # Shell
    '.sql', '.graphql',  # Query languages
    '.proto',  # Protocol buffers
}

def should_skip_kg_extraction(metadata: Optional[Dict[str, Any]], rid: str = '') -> bool:
    """Check if KG extraction should be skipped for this content.
    
    Returns True for code files where semantic entity extraction
    provides little value. These files are still chunked and embedded
    for code search - just not processed for KG entities/statements.
    """
    metadata = metadata or {}

    source_type = metadata.get('source_type', '')
    file_type = metadata.get('file_type', '')
    
    # Also detect file extension from RID (more reliable)
    rid_extension = ''
    if rid:
        # Extract extension from RID like regen.github:github_repo_path_file.py
        import re
        ext_match = re.search(r'\.(\w+)(?:#|$)', rid)
        if ext_match:
            rid_extension = '.' + ext_match.group(1)
    
    # Check if it's a github source (from metadata or RID pattern)
    is_github = source_type == 'github' or 'github' in rid.lower()

    logger.info(f"[KG Filter] rid={rid[-50:]}, source_type={source_type}, file_type={file_type}, rid_ext={rid_extension}, is_github={is_github}")

    if is_github:
        # Use file_type from metadata, or fall back to RID extension
        ext = file_type or rid_extension

        # Allow markdown and documentation files
        if ext in {'.md', '.mdx', '.rst', '.txt'}:
            logger.info(f"[KG Filter] ALLOW doc file: {ext}")
            return False
        # Skip code files
        if ext in CODE_FILE_EXTENSIONS:
            logger.info(f"[KG Filter] SKIP code file: {ext}")
            return True

    return False

# Global connection pool (shared across all requests)
db_pool: Optional[asyncpg.Pool] = None

# Pydantic models - KOI Protocol compliant
class KOIManifest(BaseModel):
    rid: str
    timestamp: str
    content_hash: str
    size_bytes: int
    content_type: str
    version: str = "1.0"
    metadata: Optional[Dict[str, Any]] = None

class KOIBundle(BaseModel):
    rid: str
    manifest: KOIManifest
    contents: Dict[str, Any]  # KOI protocol uses 'contents' not 'content'

class KOIEvent(BaseModel):
    event_type: str  # NEW, UPDATE, FORGET
    rid: str
    source_node: str  # KOI protocol uses 'source_node' not 'source_sensor'
    timestamp: str
    bundle: Optional[KOIBundle] = None  # Bundle is optional for FORGET events
    reason: Optional[str] = None  # For FORGET events

class ProcessingResult(BaseModel):
    success: bool
    rid: str
    cid: str
    chunks_created: int
    embeddings_created: int
    version: Optional[int] = None
    previous_version_id: Optional[str] = None
    error: Optional[str] = None

# Helper functions
async def generate_embedding_bge(text: str) -> List[float]:
    """Generate BGE embedding via API"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                BGE_API_URL,
                json={"text": text}  # Some servers use "text"
            )
            if response.status_code != 200:
                # Try with "input" field
                response = await client.post(
                    BGE_API_URL,
                    json={"input": text}
                )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("embedding", [])
            else:
                logger.warning(f"BGE API error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error calling BGE API: {e}")
            return []

def chunk_text(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """Split text into overlapping chunks"""
    if not text:
        return []
    
    chunks = []
    start = 0
    text_length = len(text)
    
    while start < text_length:
        end = min(start + chunk_size, text_length)
        chunk = text[start:end]
        chunks.append(chunk)
        start += (chunk_size - overlap)
    
    return chunks

def is_ledger_entity_rid(rid: str) -> bool:
    """Check if a RID is a ledger entity that should go to entity_registry"""
    return any(rid.startswith(prefix) for prefix in LEDGER_ENTITY_PREFIXES)

async def handle_ledger_entity_event(conn: asyncpg.Connection, event) -> bool:
    """
    Handle ledger entity events by upserting to entity_registry.

    Ledger entities (credit classes, projects, organizations) are indexed
    for automated entity resolution by the MCP server.

    Args:
        conn: Database connection
        event: KOI event with bundle containing entity data

    Returns:
        True if entity was successfully indexed, False otherwise
    """
    if not LEDGER_ENTITY_INDEXING_ENABLED:
        return False

    if not event.bundle:
        return False

    rid = event.bundle.rid
    if not is_ledger_entity_rid(rid):
        return False

    try:
        contents = event.bundle.contents
        metadata = event.bundle.manifest.metadata or {}

        # Extract entity data from bundle
        entity_type = contents.get('entity_type') or metadata.get('entity_type')
        entity_text = contents.get('name') or contents.get('id') or rid.split(':')[-1]
        ledger_id = contents.get('id') or metadata.get('ledger_id')
        metadata_iri = contents.get('metadata_iri') or metadata.get('metadata_iri')
        admin_address = contents.get('admin') or metadata.get('admin_address')
        aliases = contents.get('aliases', [])
        jurisdiction = contents.get('jurisdiction') or metadata.get('jurisdiction')
        class_id = contents.get('class_id') or metadata.get('class_id')

        # Normalize text for matching
        normalized_text = entity_text.lower().strip() if entity_text else rid.split(':')[-1].lower()

        # Create fuseki URI for this entity
        fuseki_uri = f"https://regen.network/entity/{rid.replace(':', '/')}"

        # Upsert to entity_registry
        # We need to handle the embedding - for now, use a placeholder embedding
        # The entity resolution API will use exact/fuzzy matching, not semantic
        await conn.execute("""
            INSERT INTO entity_registry (
                fuseki_uri, entity_text, entity_type, normalized_text,
                ledger_id, metadata_iri, admin_address, aliases,
                jurisdiction, class_id, source, metadata, embedding
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'regen_ledger',
                $11::jsonb, (SELECT embedding FROM entity_registry LIMIT 1)
            )
            ON CONFLICT (normalized_text, entity_type) DO UPDATE SET
                entity_text = EXCLUDED.entity_text,
                ledger_id = EXCLUDED.ledger_id,
                metadata_iri = EXCLUDED.metadata_iri,
                admin_address = EXCLUDED.admin_address,
                aliases = EXCLUDED.aliases,
                jurisdiction = EXCLUDED.jurisdiction,
                class_id = EXCLUDED.class_id,
                source = 'regen_ledger',
                last_seen_at = NOW(),
                occurrence_count = entity_registry.occurrence_count + 1,
                metadata = entity_registry.metadata || EXCLUDED.metadata
        """,
            fuseki_uri,
            entity_text,
            entity_type,
            normalized_text,
            ledger_id,
            metadata_iri,
            admin_address,
            aliases if isinstance(aliases, list) else [],
            jurisdiction,
            class_id,
            json.dumps({
                'rid': rid,
                'source_node': event.source_node,
                'timestamp': event.timestamp,
                'description': contents.get('description'),
            })
        )

        logger.info(f"Indexed ledger entity to entity_registry: {rid} ({entity_type}: {entity_text})")
        return True

    except Exception as e:
        logger.error(f"Error indexing ledger entity {rid}: {e}", exc_info=True)
        return False

async def extract_text_from_bundle(bundle: KOIBundle) -> str:
    """Extract text content from KOI bundle"""
    content = bundle.contents
    
    logger.info(f"Bundle content type: {type(content)}")
    logger.info(f"Bundle content keys: {content.keys() if isinstance(content, dict) else 'Not a dict'}")
    
    if isinstance(content, dict):
        # Check if content is wrapped in a document structure (from koi-sensors)
        if 'document' in content and isinstance(content['document'], dict):
            doc = content['document']
            logger.info(f"Found document structure, keys: {doc.keys()}")
            # Extract content from the document
            if 'content' in doc:
                extracted = str(doc['content'])
                logger.info(f"Extracting content from document.content")
                return extracted
            # Fallback to other fields in document
            for key in ['text', 'body', 'description']:
                if key in doc:
                    return str(doc[key])
        
        # Try common content keys at root level
        for key in ['text', 'content', 'body', 'description']:
            if key in content:
                return str(content[key])
        
        # Try HTML content
        if 'html' in content:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content['html'], 'html.parser')
            return soup.get_text(strip=True)
        
        # Concatenate all string values
        text_parts = []
        for value in content.values():
            if isinstance(value, str):
                text_parts.append(value)
        return ' '.join(text_parts)
    
    elif isinstance(content, str):
        return content
    
    return json.dumps(content)

async def check_existing_memory(conn: asyncpg.Connection, rid: str, content_hash: Optional[str] = None, url: Optional[str] = None) -> Optional[Dict]:
    """Check if a memory with this RID already exists, optionally with same content hash or URL"""
    if USE_ISOLATED_TABLES:
        if url:
            # For web pages, check by URL instead of RID (URLs can generate different RIDs)
            query = """
                SELECT id, version, superseded_at, content_hash, metadata, rid
                FROM koi_memories
                WHERE metadata->>'source_url' = $1
                AND superseded_at IS NULL
                ORDER BY version DESC
                LIMIT 1
            """
            result = await conn.fetchrow(query, url)
        elif content_hash:
            # Check for exact content match
            query = """
                SELECT id, version, superseded_at, content_hash, metadata
                FROM koi_memories
                WHERE rid = $1 AND content_hash = $2
                AND superseded_at IS NULL
                ORDER BY version DESC
                LIMIT 1
            """
            result = await conn.fetchrow(query, rid, content_hash)
        else:
            # Check for any version of this RID
            query = """
                SELECT id, version, superseded_at, content_hash, metadata
                FROM koi_memories
                WHERE rid = $1
                AND superseded_at IS NULL
                ORDER BY version DESC
                LIMIT 1
            """
            result = await conn.fetchrow(query, rid)
    else:
        # Legacy table structure
        query = """
            SELECT id, content
            FROM memories
            WHERE content->>'rid' = $1
            ORDER BY "createdAt" DESC
            LIMIT 1
        """
        result = await conn.fetchrow(query, rid)

    return dict(result) if result else None

async def create_new_version(conn: asyncpg.Connection, event: KOIEvent, 
                           previous: Optional[Dict], text_content: str) -> str:
    """Create a new version of a memory"""
    memory_id = str(uuid.uuid4())
    
    # Generate CID from manifest content_hash
    cid = f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else None
    
    if USE_ISOLATED_TABLES:
        # Determine version number
        version = (previous['version'] + 1) if previous else 1
        previous_id = previous['id'] if previous else None
        
        # If updating, mark previous version as superseded
        if previous and event.event_type == 'UPDATE':
            await conn.execute("""
                UPDATE koi_memories 
                SET superseded_at = $1 
                WHERE id = $2
            """, datetime.now(tz=timezone.utc), previous['id'])
        
        # Extract publication date from metadata or content
        published_at = None
        published_confidence = 0.0
        content_hash = None
        
        # Try to extract from metadata first
        # DEBUG: Log metadata to see what's coming through
        logger.info(f"DEBUG: Processing RID {event.bundle.rid}, metadata keys: {list(event.bundle.manifest.metadata.keys())}")
        if 'published_at' in event.bundle.manifest.metadata:
            logger.info(f"DEBUG: Found published_at: {event.bundle.manifest.metadata['published_at']}")

        if 'published_at' in event.bundle.manifest.metadata:
            # Convert string date to datetime object if needed
            date_str = event.bundle.manifest.metadata['published_at']
            if isinstance(date_str, str):
                try:
                    # Parse ISO format datetime string
                    published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    logger.info(f"Converted published_at string to datetime: {date_str} -> {published_at}")
                except Exception as e:
                    logger.warning(f"Failed to parse published_at date '{date_str}': {e}")
                    published_at = None
            else:
                published_at = date_str
            published_confidence = event.bundle.manifest.metadata.get('published_confidence', 0.9)
        elif 'created_at' in event.bundle.manifest.metadata:
            # Convert string date to datetime object if needed
            date_str = event.bundle.manifest.metadata['created_at']
            if isinstance(date_str, str):
                try:
                    published_at = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except Exception as e:
                    logger.warning(f"Failed to parse created_at date '{date_str}': {e}")
                    published_at = None
            else:
                published_at = date_str
            published_confidence = 0.8
        
        # Calculate content hash for deduplication
        import hashlib
        content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()
        
        # Include source_url in metadata if available
        metadata = {
            **event.bundle.manifest.metadata,
            "koi_timestamp": event.timestamp,
            "koi_manifest": event.bundle.manifest.dict()
        }

        # Ensure source_url is captured for web pages
        if 'url' in event.bundle.manifest.metadata and 'source_url' not in metadata:
            metadata['source_url'] = event.bundle.manifest.metadata['url']

        # Privacy: promote bundle-metadata is_private/access_source to dedicated columns.
        # Sticky-OR on ON CONFLICT — once-private-stays-private (Phase 1 / tech-backlog #23).
        bundle_meta = event.bundle.manifest.metadata or {}
        is_private = bool(bundle_meta.get('is_private', False))
        access_source = bundle_meta.get('access_source')

        # Insert new version with publication tracking
        await conn.execute("""
            INSERT INTO koi_memories (
                id, rid, cid, version, previous_version_id,
                event_type, source_sensor, content, metadata,
                published_at, published_confidence, content_hash,
                is_private, access_source
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (rid) DO UPDATE SET
                content = EXCLUDED.content,
                metadata = EXCLUDED.metadata,
                published_at = EXCLUDED.published_at,
                content_hash = EXCLUDED.content_hash,
                is_private = (koi_memories.is_private OR EXCLUDED.is_private),
                access_source = COALESCE(koi_memories.access_source, EXCLUDED.access_source),
                superseded_at = NULL,
                updated_at = CURRENT_TIMESTAMP
        """,
            memory_id,
            event.bundle.rid,
            cid or event.bundle.rid,
            version,
            previous_id,
            event.event_type,
            event.source_node,
            json.dumps({
                "text": text_content,
                **event.bundle.contents
            }),
            json.dumps(metadata),
            published_at,
            published_confidence,
            content_hash,
            is_private,
            access_source
        )

        # Fetch the actual memory_id from database (in case ON CONFLICT kept the old UUID)
        actual_memory_id = await conn.fetchval("""
            SELECT id FROM koi_memories WHERE rid = $1
        """, event.bundle.rid)

        # Update memory_id to use the actual one from database
        if actual_memory_id:
            memory_id = str(actual_memory_id)
    else:
        # Legacy table structure - just insert without version control
        # TODO: Update legacy structure to support versioning
        agent_id = "8e1e4498-b3c8-0fae-ad1f-e90d1c1a4331"  # RegenAI agent
        
        await conn.execute("""
            INSERT INTO memories (id, type, content, "agentId", "createdAt")
            VALUES ($1::uuid, 'koi_document', $2::jsonb, $3::uuid, CURRENT_TIMESTAMP)
        """, 
            memory_id,
            json.dumps({
                "text": text_content,
                "rid": event.bundle.rid,
                "cid": event.bundle.rid,
                "source_sensor": event.source_node,
                "event_type": event.event_type,
                **event.bundle.manifest.metadata
            }),
            agent_id
        )
    
    return memory_id

async def store_embedding(conn: asyncpg.Connection, memory_id: str, 
                         embedding: List[float]) -> bool:
    """Store embedding in appropriate table"""
    if not embedding:
        return False
    
    embedding_str = '[' + ','.join(map(str, embedding)) + ']'
    embedding_dim = len(embedding)
    
    try:
        if USE_ISOLATED_TABLES:
            # Store in isolated koi_embeddings table
            if embedding_dim == 768:
                await conn.execute("""
                    INSERT INTO koi_embeddings (memory_id, dim_768)
                    VALUES ($1, $2::vector(768))
                    ON CONFLICT (memory_id) 
                    DO UPDATE SET dim_768 = $2::vector(768)
                """, memory_id, embedding_str)
            elif embedding_dim == 1024:
                await conn.execute("""
                    INSERT INTO koi_embeddings (memory_id, dim_1024)
                    VALUES ($1, $2::vector(1024))
                    ON CONFLICT (memory_id)
                    DO UPDATE SET dim_1024 = $2::vector(1024)
                """, memory_id, embedding_str)
            else:
                logger.warning(f"Unsupported embedding dimension: {embedding_dim}")
                return False
        else:
            # Store in legacy embeddings table
            existing = await conn.fetchval("""
                SELECT memory_id FROM embeddings WHERE memory_id = $1
            """, memory_id)
            
            if embedding_dim == 1024:
                if existing:
                    await conn.execute("""
                        UPDATE embeddings SET dim_1024 = $2::vector(1024)
                        WHERE memory_id = $1
                    """, memory_id, embedding_str)
                else:
                    await conn.execute("""
                        INSERT INTO embeddings (memory_id, dim_1024)
                        VALUES ($1, $2::vector(1024))
                    """, memory_id, embedding_str)
            else:
                logger.warning(f"Legacy table only supports 1024-dim embeddings")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error storing embedding: {e}")
        return False

async def trigger_kg_extraction(memory_rid: str, content: str, metadata: dict) -> Optional[str]:
    """Trigger KG extraction for a memory using Pass A extractor

    Args:
        memory_rid: RID of the memory to extract from
        content: Text content to extract entities/statements from
        metadata: Metadata dict containing source_url and other info

    Returns:
        extraction_rid if successful, None if failed
    """
    if not KG_EXTRACTION_ENABLED:
        return None

    try:
        # Import Pass A extractor from koi-sensors
        import sys
        sys.path.insert(0, '/opt/projects/koi-sensors')
        from knowledge_graph.extractors.pass_a_extractor import PassAExtractor

        # Create extractor instance
        extractor = PassAExtractor(db_url=DB_URL)

        # Run extraction with full provenance tracking
        extraction_rid, receipt_id = await extractor.extract_and_track(
            memory_rid=memory_rid,
            content=content,
            metadata=metadata
        )

        logger.info(f"KG extraction complete for {memory_rid}: {extraction_rid} (receipt: {receipt_id})")
        return extraction_rid

    except Exception as e:
        logger.error(f"Error during KG extraction for {memory_rid}: {e}", exc_info=True)
        return None

async def process_koi_event(event: KOIEvent) -> ProcessingResult:
    """Process a KOI event with deduplication and versioning"""
    start_time = time.time()
    try:
        # Use global connection pool instead of creating new one
        async with db_pool.acquire() as conn:
            # Handle ledger entity events (credit classes, projects, organizations)
            # These are indexed to entity_registry for automated entity resolution
            if event.bundle and is_ledger_entity_rid(event.bundle.rid):
                entity_indexed = await handle_ledger_entity_event(conn, event)
                if entity_indexed:
                    # Ledger entities are lightweight - return success after indexing
                    # They don't need full content processing/chunking/embedding
                    return ProcessingResult(
                        success=True,
                        rid=event.bundle.rid,
                        cid=f"cid:sha256:{event.bundle.manifest.content_hash}",
                        chunks_created=0,
                        embeddings_created=0,
                        version=1,
                        error="Ledger entity indexed to entity_registry"
                    )

            # Extract URL if this is a web page
            source_url = None
            if event.bundle and event.bundle.manifest.metadata:
                # Check for URL in metadata
                source_url = event.bundle.manifest.metadata.get('url') or event.bundle.manifest.metadata.get('source_url')

            # For web pages, check by URL to handle re-crawls properly
            if source_url and source_url.startswith('http'):
                existing = await check_existing_memory(conn, event.bundle.rid, url=source_url)
            else:
                # Check for existing memory with this RID
                existing = await check_existing_memory(conn, event.bundle.rid)
            
            # Handle based on event type
            if event.event_type == "FORGET":
                if existing and USE_ISOLATED_TABLES:
                    # Mark as superseded without creating new version
                    await conn.execute("""
                        UPDATE koi_memories 
                        SET superseded_at = $1 
                        WHERE rid = $2 AND superseded_at IS NULL
                    """, datetime.now(tz=timezone.utc), event.bundle.rid)
                    
                    return ProcessingResult(
                        success=True,
                        rid=event.bundle.rid,
                        cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
                        chunks_created=0,
                        embeddings_created=0
                    )
                else:
                    # TODO: Implement deletion for legacy tables
                    return ProcessingResult(
                        success=True,
                        rid=event.bundle.rid,
                        cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
                        chunks_created=0,
                        embeddings_created=0
                    )
            
            elif event.event_type == "NEW" and existing:
                # For web pages with URLs, we'll handle this later with content hash checking
                if not source_url:
                    # Only skip if not a web page (no URL)
                    logger.info(f"RID {event.bundle.rid} already exists (non-web content), skipping NEW event")
                    return ProcessingResult(
                        success=True,
                        rid=event.bundle.rid,
                        cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
                        chunks_created=0,
                        embeddings_created=0,
                        error="Already exists"
                    )
                # For web pages, continue to content hash checking below
            
            elif event.event_type == "UPDATE" and not existing:
                # No previous version to update, treat as NEW
                logger.info(f"No existing version for RID {event.bundle.rid}, treating UPDATE as NEW")
                event.event_type = "NEW"
            
            # Extract text content
            text_content = await extract_text_from_bundle(event.bundle)

            # Debug logging
            logger.info(f"Extracted text content length: {len(text_content) if text_content else 0}")
            if text_content:
                logger.info(f"Content preview: {text_content[:200]}")

            if not text_content or len(text_content.strip()) < 50:
                return ProcessingResult(
                    success=False,
                    rid=event.bundle.rid,
                    cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
                    chunks_created=0,
                    embeddings_created=0,
                    error="Content too short or empty"
                )

            # Calculate content hash for deduplication
            content_hash = hashlib.sha256(text_content.encode('utf-8')).hexdigest()

            # GLOBAL DEDUP: Check if ANY memory has this exact content (catches cross-RID duplicates)
            # This fixes the issue where GitLab sensor creates new RIDs per run for unchanged files
            if USE_ISOLATED_TABLES:
                global_duplicate = await conn.fetchrow("""
                    SELECT id, rid, version FROM koi_memories
                    WHERE content_hash = $1
                      AND superseded_at IS NULL
                    LIMIT 1
                """, content_hash)

                if global_duplicate:
                    if global_duplicate['rid'] == event.bundle.rid:
                        # Same RID, same content - skip (no change)
                        logger.debug(f"Skipping unchanged content for RID: {event.bundle.rid}")
                    else:
                        # Different RID, same content - this is a cross-RID duplicate
                        logger.info(
                            f"Cross-RID duplicate detected: {event.bundle.rid} "
                            f"has same content as existing {global_duplicate['rid']} "
                            f"(hash: {content_hash[:8]}...)"
                        )
                    return ProcessingResult(
                        success=True,
                        rid=event.bundle.rid,
                        cid=f"cid:sha256:{event.bundle.manifest.content_hash}",
                        chunks_created=0,
                        embeddings_created=0,
                        version=global_duplicate['version'],
                        error=f"Duplicate content, matches RID: {global_duplicate['rid']}"
                    )

            # Check if we have this exact content already (deduplication)
            if USE_ISOLATED_TABLES:
                # For web pages, re-check by URL with the content hash
                if source_url:
                    existing_by_url = await check_existing_memory(conn, event.bundle.rid, url=source_url)
                    if existing_by_url:
                        # Check if content changed
                        if existing_by_url.get('content_hash') == content_hash:
                            # Content hasn't changed, skip processing
                            logger.info(f"URL {source_url} has unchanged content (hash: {content_hash[:8]}...), skipping")
                            return ProcessingResult(
                                success=True,
                                rid=event.bundle.rid,
                                cid=f"cid:sha256:{event.bundle.manifest.content_hash}",
                                chunks_created=0,
                                embeddings_created=0,
                                version=existing_by_url.get('version'),
                                error="Content unchanged, skipped processing"
                            )
                        else:
                            # Content changed, treat as UPDATE
                            logger.info(f"URL {source_url} has new content, converting NEW to UPDATE")
                            event.event_type = "UPDATE"
                            existing = existing_by_url  # Use the URL-matched entry as existing
                else:
                    # Non-web content, check by RID and hash
                    existing_with_same_hash = await check_existing_memory(conn, event.bundle.rid, content_hash)

                    if existing_with_same_hash:
                        # Content hasn't changed, skip processing
                        logger.info(f"RID {event.bundle.rid} has unchanged content (hash: {content_hash[:8]}...), skipping")
                        return ProcessingResult(
                            success=True,
                            rid=event.bundle.rid,
                            cid=f"cid:sha256:{event.bundle.manifest.content_hash}",
                            chunks_created=0,
                            embeddings_created=0,
                            version=existing_with_same_hash.get('version'),
                            error="Content unchanged, skipped processing"
                        )

                    # If we have a different version, this should be an UPDATE
                    if existing and event.event_type == "NEW":
                        logger.info(f"RID {event.bundle.rid} exists with different content, converting NEW to UPDATE")
                        event.event_type = "UPDATE"
            
            # Chunk the text
            chunks = chunk_text(text_content)
            
            if not chunks:
                return ProcessingResult(
                    success=False,
                    rid=event.bundle.rid,
                    cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
                    chunks_created=0,
                    embeddings_created=0,
                    error="No chunks created"
                )
            
            # Process each chunk
            embeddings_created = 0
            memory_ids = []

            # Extract document metadata if it exists
            doc_metadata = {}
            if isinstance(event.bundle.contents, dict):
                if 'document' in event.bundle.contents and isinstance(event.bundle.contents['document'], dict):
                    doc = event.bundle.contents['document']
                    if 'metadata' in doc and isinstance(doc['metadata'], dict):
                        doc_metadata = doc['metadata']

            for i, chunk in enumerate(chunks):
                # Create memory for chunk
                chunk_rid = f"{event.bundle.rid}#chunk{i}"
                # Calculate chunk content hash for CID
                chunk_hash = hashlib.sha256(chunk.encode()).hexdigest()
                chunk_event = KOIEvent(
                    event_type=event.event_type,
                    rid=chunk_rid,
                    source_node=event.source_node,
                    timestamp=event.timestamp,
                    bundle=KOIBundle(
                        rid=chunk_rid,
                        manifest=KOIManifest(
                            rid=chunk_rid,
                            timestamp=event.timestamp,
                            content_hash=chunk_hash,
                            size_bytes=len(chunk.encode()),
                            content_type="text/plain",
                            version="1.0",
                            metadata={
                                **event.bundle.manifest.metadata,
                                **doc_metadata,  # Include document metadata (post details, etc.)
                                "chunk_index": i,
                                "chunk_total": len(chunks),
                                "parent_rid": event.bundle.rid
                            }
                        ),
                        contents={"text": chunk}
                    )
                )
                
                # Check if chunk already exists
                chunk_existing = await check_existing_memory(conn, chunk_rid)
                
                # Create new version if needed
                if event.event_type == "NEW" and not chunk_existing:
                    memory_id = await create_new_version(conn, chunk_event, None, chunk)
                elif event.event_type == "UPDATE":
                    memory_id = await create_new_version(conn, chunk_event, chunk_existing, chunk)
                else:
                    continue  # Skip if already exists
                
                memory_ids.append(memory_id)

                # Create CAT receipt for memory creation (only if transformation occurred)
                # Skip if this is just a forwarding operation where input equals output
                if event.bundle.rid != chunk_rid:  # Only create receipt if we actually transformed
                    await create_cat_receipt(
                        conn=conn,
                        transformation_type="koi_to_memory",
                        input_rid=event.bundle.rid,
                        output_rid=chunk_rid,
                        input_cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else None,
                        output_cid=f"cid:sha256:{chunk_hash}",
                        chunks_created=1,
                        source_sensor=event.source_node,
                        event_type=event.event_type,
                        metadata={"chunk_index": i, "chunk_total": len(chunks)}
                    )

                # Generate and store embedding
                embedding_start = time.time()
                embedding = await generate_embedding_bge(chunk)
                if embedding and await store_embedding(conn, memory_id, embedding):
                    embeddings_created += 1
                    embedding_time_ms = int((time.time() - embedding_start) * 1000)

                    # Create CAT receipt for embedding generation
                    await create_embedding_receipt(
                        conn=conn,
                        memory_id=memory_id,
                        rid=chunk_rid,
                        embedding_model="bge-large-en-v1.5",
                        embedding_dim=len(embedding),
                        source_sensor=event.source_node,
                        processing_time_ms=embedding_time_ms
                    )

                # Delay removed for maximum throughput - OpenAI can handle the load
                # await asyncio.sleep(0.05)

            # Trigger KG extraction on the full document (not chunks)
            # Only extract from NEW or UPDATE events with sufficient content
            if KG_EXTRACTION_ENABLED and text_content and len(text_content) > 100 and not should_skip_kg_extraction(event.bundle.manifest.metadata, event.bundle.rid):
                # Build metadata for KG extraction
                kg_metadata = {
                    'source_url': source_url or event.bundle.manifest.metadata.get('url'),
                    'source_sensor': event.source_node,
                    'event_type': event.event_type,
                    **event.bundle.manifest.metadata
                }

                # Extract from the original document RID, not chunk RIDs
                await trigger_kg_extraction(
                    memory_rid=event.bundle.rid,
                    content=text_content,
                    metadata=kg_metadata
                )

            # Get version info for response
            version = None
            previous_version_id = None
            if USE_ISOLATED_TABLES and memory_ids:
                result = await conn.fetchrow("""
                    SELECT version, previous_version_id 
                    FROM koi_memories 
                    WHERE id = $1
                """, memory_ids[0])
                if result:
                    version = result['version']
                    previous_version_id = str(result['previous_version_id']) if result['previous_version_id'] else None
            
            # Calculate total processing time
            processing_time_ms = int((time.time() - start_time) * 1000)

            # Create overall transformation receipt (only if we created chunks)
            cat_receipt_id = None
            if memory_ids and USE_ISOLATED_TABLES and len(chunks) > 1:
                # Only create overall receipt if we actually chunked the content
                cat_receipt_id = await create_cat_receipt(
                    conn=conn,
                    transformation_type="koi_event_processing",
                    input_rid=event.bundle.rid,
                    output_rid=event.bundle.rid,
                    chunks_created=len(memory_ids),
                    embeddings_created=embeddings_created,
                    source_sensor=event.source_node,
                    event_type=event.event_type,
                    processing_duration_ms=processing_time_ms,
                    metadata={
                        "version": version,
                        "chunks_processed": len(memory_ids),
                        "content_hash": content_hash[:8] + "..."  # Store abbreviated hash for tracking
                    }
                )

            # Write provenance to RDF knowledge graph
            try:
                prdf = ProvenanceToRDF()
                if await prdf.check_fuseki_connection():
                    # Extract sensor ID from source_node
                    sensor_id = event.source_node.split(":")[-1] if ":" in event.source_node else event.source_node

                    # Write document provenance
                    await prdf.write_document_provenance(
                        rid=event.bundle.rid,
                        sensor_id=sensor_id,
                        event_type=event.event_type,
                        timestamp=event.timestamp,
                        title=event.bundle.contents.get("title", event.bundle.rid),
                        content_hash=event.bundle.manifest.content_hash,
                        processors=["event-bridge", "bge-embeddings"],
                        storage_locations=["postgresql"],
                        cat_receipt_id=cat_receipt_id
                    )
                    logger.info(f"Wrote provenance to RDF for {event.bundle.rid}")
                else:
                    logger.warning("Apache Jena Fuseki not available for provenance writing")
            except Exception as e:
                logger.error(f"Failed to write provenance to RDF: {e}")
                # Don't fail the whole process if RDF writing fails

            return ProcessingResult(
                success=True,
                rid=event.bundle.rid,
                cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
                chunks_created=len(memory_ids),
                embeddings_created=embeddings_created,
                version=version,
                previous_version_id=previous_version_id
            )
            
    except Exception as e:
        logger.error(f"Error processing event: {e}", exc_info=True)
        return ProcessingResult(
            success=False,
            rid=event.bundle.rid,
            cid=f"cid:sha256:{event.bundle.manifest.content_hash}" if event.bundle else "",
            chunks_created=0,
            embeddings_created=0,
            error=str(e)
        )


# Lightweight health endpoint that can respond even when main loop is busy
import asyncio
from concurrent.futures import ThreadPoolExecutor

_health_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="health")

def _sync_health_check():
    """Synchronous health check that runs in a separate thread"""
    return {"status": "ok", "service": "koi-event-bridge", "version": "2.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint - runs in thread pool to avoid event loop blocking"""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_health_executor, _sync_health_check)
    return result

# API Endpoints
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "KOI Event Bridge v2",
        "status": "operational",
        "version": "2.0.0",
        "features": [
            "RID-based deduplication",
            "Version control for updates",
            "Isolated KOI tables",
            "BGE embedding generation"
        ],
        "isolated_tables": USE_ISOLATED_TABLES
    }

@app.post("/process-koi-event", response_model=ProcessingResult)
async def process_event_endpoint(event: KOIEvent):
    """Process a KOI event from the coordinator"""
    rid = event.bundle.rid if event.bundle else event.rid
    logger.info(f"[KOI Bridge v2] Received {event.event_type} event for RID: {rid}")

    # Filter out non-content events (heartbeats, test data, etc.)
    event_dict = event.dict()
    if not filter_koi_event(event_dict):
        logger.info(f"[KOI Bridge v2] Filtered out non-content event: {rid}")
        return ProcessingResult(
            success=True,
            rid=rid,
            cid="filtered",
            chunks_created=0,
            embeddings_created=0,
            error="Event filtered: non-content (heartbeat/test/monitoring)"
        )

    # Process the event
    result = await process_koi_event(event)
    
    if result.success:
        logger.info(f"[KOI Bridge v2] Successfully processed: {result.chunks_created} chunks, "
                   f"{result.embeddings_created} embeddings, version: {result.version}")
    else:
        logger.error(f"[KOI Bridge v2] Processing failed: {result.error}")
    
    status_code = 200 if result.success else 500
    return JSONResponse(status_code=status_code, content=result.dict())

@app.get("/stats")
async def get_stats():
    """Get pipeline statistics"""
    try:
        # Use global connection pool instead of creating new one
        async with db_pool.acquire() as conn:
            if USE_ISOLATED_TABLES:
                result = await conn.fetchrow("""
                    SELECT 
                        COUNT(DISTINCT rid) as unique_documents,
                        COUNT(*) as total_versions,
                        COUNT(CASE WHEN event_type = 'NEW' THEN 1 END) as new_events,
                        COUNT(CASE WHEN event_type = 'UPDATE' THEN 1 END) as update_events,
                        COUNT(DISTINCT source_sensor) as active_sensors,
                        MAX(created_at) as latest_event
                    FROM koi_memories
                """)
                
                embeddings = await conn.fetchrow("""
                    SELECT 
                        COUNT(dim_768) as gemma_embeddings,
                        COUNT(dim_1024) as bge_embeddings
                    FROM koi_embeddings
                """)
                
                return {
                    "unique_documents": result['unique_documents'],
                    "total_versions": result['total_versions'],
                    "new_events": result['new_events'],
                    "update_events": result['update_events'],
                    "active_sensors": result['active_sensors'],
                    "latest_event": result['latest_event'].isoformat() if result['latest_event'] else None,
                    "embeddings": {
                        "bge": embeddings['bge_embeddings'],
                        "gemma": embeddings['gemma_embeddings']
                    }
                }
            else:
                # Legacy stats
                result = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_memories,
                        COUNT(DISTINCT content->>'rid') as unique_rids,
                        COUNT(CASE WHEN type = 'koi_document' THEN 1 END) as koi_documents
                    FROM memories
                """)
                
                return {
                    "total_memories": result['total_memories'],
                    "unique_rids": result['unique_rids'],
                    "koi_documents": result['koi_documents'],
                    "isolated_tables": False
                }
                
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Application lifecycle - manage connection pool
@app.on_event("startup")
async def startup():
    """Create database connection pool on startup"""
    global db_pool
    logger.info("Creating database connection pool...")
    try:
        db_pool = await asyncpg.create_pool(
            DB_URL,
            min_size=10,   # Keep 10 connections ready
            max_size=20,   # Never exceed 20 connections (prevents exhaustion)
            command_timeout=60
        )
        logger.info(f"✓ Database pool created (10-20 connections)")
    except Exception as e:
        logger.error(f"✗ Failed to create database pool: {e}")
        raise

@app.on_event("shutdown")
async def shutdown():
    """Close database connection pool on shutdown"""
    global db_pool
    if db_pool:
        logger.info("Closing database connection pool...")
        await db_pool.close()
        logger.info("✓ Database pool closed")

def _masked_dsn(dsn: str) -> str:
    """The DSN with any password replaced by ***, for logs."""
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(dsn)
    if parts.password is None:
        return dsn
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit(parts._replace(netloc=f"{parts.username}:***@{host}"))


if __name__ == "__main__":
    import argparse
    import uvicorn

    # A server: run with no arguments it starts on 0.0.0.0:8100 and creates its database
    # pool, as before. --help prints this and exits; other arguments are ignored, as before.
    argparse.ArgumentParser(
        description="KOI Event Bridge v2: serves /process-koi-event on 0.0.0.0:8100 and stores "
                    "events in the database POSTGRES_URL names (default eliza on localhost:5433). "
                    "Run with no arguments to start it."
    ).parse_known_args()

    logger.info("="*70)
    logger.info("KOI EVENT BRIDGE v2 STARTING")
    logger.info(f"Database: {_masked_dsn(DB_URL)}")
    logger.info(f"BGE API: {BGE_API_URL}")
    logger.info(f"Using isolated tables: {USE_ISOLATED_TABLES}")
    logger.info(f"Features: Deduplication, Versioning, Isolated Tables")
    logger.info("="*70)
    
    uvicorn.run(app, host="0.0.0.0", port=8100, log_level="info", access_log=True)
