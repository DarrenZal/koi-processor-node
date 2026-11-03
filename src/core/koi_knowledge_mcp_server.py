#!/usr/bin/env python3
"""
KOI Knowledge MCP Server
Provides agents with access to KOI pipeline knowledge via MCP protocol
"""

import asyncio
import json
import asyncpg
import httpx
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import logging
import os
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="KOI Knowledge MCP Server",
    version="3.0.0",
    description="MCP Streamable HTTP Server (2025-03-26) for KOI Knowledge Graph"
)

# Configuration
DB_URL = os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza')
BGE_API_URL = os.getenv('BGE_API_URL', 'http://localhost:8090/encode')
HYBRID_RAG_API_URL = os.getenv('HYBRID_RAG_API_URL', 'http://localhost:8301/api/koi/query')

# Pydantic models
class KnowledgeQuery(BaseModel):
    query: str
    agent_id: Optional[str] = None
    limit: int = 10
    similarity_threshold: float = 0.7
    source_filter: Optional[str] = None  # Filter by source_sensor (e.g., 'podcast')
    include_metadata: Optional[bool] = True  # MCP client compatibility
    filters: Optional[Dict[str, Any]] = None  # Date range and other filters for hybrid search

    class Config:
        extra = "ignore"  # Ignore any additional fields from clients

class KnowledgeResponse(BaseModel):
    success: bool
    memories: List[Dict[str, Any]]
    count: int
    query_embedding_generated: bool
    confidence: Optional[float] = None
    triggered_extraction: Optional[bool] = None
    execution_time: Optional[float] = None
    search_method: Optional[str] = None  # 'hybrid_rag' or 'fallback'

# Database connection pool
db_pool: Optional[asyncpg.Pool] = None

async def init_db():
    """Initialize database connection pool"""
    global db_pool
    db_pool = await asyncpg.create_pool(
        DB_URL,
        min_size=2,
        max_size=10
    )
    logger.info("Database connection pool initialized")

async def generate_embedding(text: str) -> List[float]:
    """Generate BGE embedding via API"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                BGE_API_URL,
                json={"text": text}
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

@app.on_event("startup")
async def startup():
    """Initialize database on startup"""
    await init_db()
    
    # Get KOI memory stats
    async with db_pool.acquire() as conn:
        koi_count = await conn.fetchval(
            "SELECT COUNT(*) FROM koi_memories"
        )
        agent_count = await conn.fetchval(
            "SELECT COUNT(*) FROM memories"
        )
        embedding_count = await conn.fetchval(
            "SELECT COUNT(*) FROM embeddings WHERE dim_1024 IS NOT NULL"
        )
        
        logger.info(f"KOI Memories: {koi_count}")
        logger.info(f"Agent Memories: {agent_count}")
        logger.info(f"Embeddings: {embedding_count}")

@app.on_event("shutdown")
async def shutdown():
    """Clean up on shutdown"""
    if db_pool:
        await db_pool.close()

@app.get("/")
async def root():
    """Health check and stats"""
    async with db_pool.acquire() as conn:
        koi_count = await conn.fetchval("SELECT COUNT(*) FROM koi_memories")

    return {
        "service": "KOI Knowledge MCP Server",
        "status": "operational",
        "version": "3.0.0",
        "protocol": "MCP Streamable HTTP",
        "protocolVersion": "2025-03-26",
        "specification": "https://modelcontextprotocol.io/specification/2025-03-26",
        "koi_memories": koi_count,
        "endpoints": {
            "mcp": {
                "path": "/mcp",
                "methods": ["POST", "GET"],
                "post": "JSON-RPC 2.0 messages (client→server)",
                "get": "SSE stream (server→client, optional)"
            },
            "legacy_search": "/search (POST)",
            "legacy_memory": "/memory/{rid} (GET)",
            "stats": "/stats (GET)"
        },
        "mcp_tools": [
            {
                "name": "search_knowledge",
                "description": "Hybrid RAG search with confidence scores"
            },
            {
                "name": "get_memory",
                "description": "Retrieve specific document by RID"
            },
            {
                "name": "get_stats",
                "description": "Knowledge base statistics"
            }
        ],
        "features": [
            "MCP Streamable HTTP (2025-03-26)",
            "JSON-RPC 2.0 protocol",
            "Bidirectional communication",
            "SSE streaming support",
            "Hybrid RAG search (RRF + BGE + Keyword)",
            "Adaptive knowledge extraction",
            "Confidence-based query routing",
            "Agent-specific filtering",
            "Real-time knowledge access",
            "Automatic fallback to text search"
        ],
        "search_capabilities": {
            "primary": "Hybrid RAG API (port 8301)",
            "fallback": "Text search (ILIKE)",
            "confidence_threshold": 0.7,
            "adaptive_extraction": True
        }
    }

async def call_hybrid_rag_api(query_text: str, agent_id: str, limit: int, source_filter: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Call the Hybrid RAG API (RRF + BGE + Adaptive Extraction)
    Returns API response or None if unavailable
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            logger.info(f"🔍 Calling Hybrid RAG API for: {query_text[:50]}...")
            payload = {
                "question": query_text,
                "agent_id": agent_id or "mcp-agent",
                "user_id": "mcp-user"
            }
            if source_filter:
                payload["source_filter"] = source_filter
                logger.info(f"🎯 Filtering by source: {source_filter}")

            response = await client.post(
                HYBRID_RAG_API_URL,
                json=payload
            )

            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ Hybrid RAG: {result.get('total_results', 0)} results, confidence: {result.get('confidence', 0):.3f}")
                return result
            else:
                logger.warning(f"⚠️ Hybrid RAG API error: {response.status_code}")
                return None

        except Exception as e:
            logger.warning(f"⚠️ Hybrid RAG API unavailable: {e}")
            return None

async def fallback_text_search(query_text: str, limit: int, source_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fallback to simple text search when Hybrid RAG API is unavailable
    """
    logger.info("📝 Using fallback text search")
    async with db_pool.acquire() as conn:
        if source_filter:
            logger.info(f"🎯 Filtering by source: {source_filter}")
            results = await conn.fetch("""
                SELECT
                    rid,
                    cid,
                    content,
                    metadata,
                    created_at,
                    source_sensor,
                    version
                FROM koi_memories
                WHERE (content::text ILIKE $1
                   OR metadata::text ILIKE $1
                   OR source_sensor ILIKE $1)
                  AND source_sensor ILIKE $3
                ORDER BY created_at DESC
                LIMIT $2
            """, f'%{query_text}%', limit, f'%{source_filter}%')
        else:
            results = await conn.fetch("""
                SELECT
                    rid,
                    cid,
                    content,
                    metadata,
                    created_at,
                    source_sensor,
                    version
                FROM koi_memories
                WHERE content::text ILIKE $1
                   OR metadata::text ILIKE $1
                   OR source_sensor ILIKE $1
                ORDER BY created_at DESC
                LIMIT $2
            """, f'%{query_text}%', limit)

        memories = []
        for row in results:
            memory = {
                "rid": row['rid'],
                "cid": row['cid'],
                "content": row['content'],
                "metadata": row['metadata'],
                "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                "source_sensor": row['source_sensor'],
                "version": row['version'],
                "similarity": 0.5  # Lower score for text search
            }
            memories.append(memory)

        return memories

async def enrich_with_kg_data(memory_rid: str, conn: asyncpg.Connection) -> Dict[str, Any]:
    """
    Enrich a memory with Knowledge Graph extraction data

    Returns KG data including:
    - Extracted entities
    - Extracted statements
    - Extraction confidence
    - Provenance chain (transformation receipts)
    """
    # Get KG extractions for this memory
    kg_rows = await conn.fetch("""
        SELECT
            extraction_rid,
            entities,
            statements,
            relations,
            confidence_score,
            ontology_version,
            extractor_version,
            created_at
        FROM koi_kg_extractions
        WHERE memory_rid = $1
        ORDER BY created_at DESC
    """, memory_rid)

    if not kg_rows:
        return None

    # Get the most recent extraction
    kg_data = dict(kg_rows[0])

    # Parse JSON fields if they're strings
    import json
    if isinstance(kg_data.get('entities'), str):
        kg_data['entities'] = json.loads(kg_data['entities']) if kg_data['entities'] else []
    if isinstance(kg_data.get('statements'), str):
        kg_data['statements'] = json.loads(kg_data['statements']) if kg_data['statements'] else []
    if isinstance(kg_data.get('relations'), str):
        kg_data['relations'] = json.loads(kg_data['relations']) if kg_data['relations'] else []

    # Resolve entities to canonical RIDs
    resolved_entities = []
    if kg_data.get('entities'):
        for entity in kg_data['entities']:
            entity_rid = entity.get('rid')
            if entity_rid:
                # Check if this entity was resolved to a canonical RID
                canonical_rid = await conn.fetchval("""
                    SELECT output_rid FROM koi_transformation_receipts
                    WHERE input_rid = $1 AND transformation_type = 'kg_entity_resolution'
                    ORDER BY created_at DESC
                    LIMIT 1
                """, entity_rid)

                if canonical_rid:
                    entity['canonical_rid'] = canonical_rid
                    entity['was_resolved'] = True
                else:
                    entity['canonical_rid'] = entity_rid
                    entity['was_resolved'] = False

            resolved_entities.append(entity)

    # Get provenance chain for this extraction
    provenance = await conn.fetch("""
        SELECT
            receipt_id,
            transformation_type,
            input_rid,
            output_rid,
            processor_name,
            metadata,
            created_at
        FROM koi_transformation_receipts
        WHERE output_rid = $1
        ORDER BY created_at ASC
    """, kg_data['extraction_rid'])

    return {
        'extraction_rid': kg_data['extraction_rid'],
        'entities': resolved_entities,
        'statements': kg_data.get('statements', []),
        'relations': kg_data.get('relations', []),
        'confidence': kg_data.get('confidence_score', 0.0),
        'ontology_version': kg_data.get('ontology_version'),
        'extractor_version': kg_data.get('extractor_version'),
        'provenance_chain': [
            {
                'receipt_id': p['receipt_id'],
                'type': p['transformation_type'],
                'processor': p['processor_name'],
                'timestamp': p['created_at'].isoformat() if p['created_at'] else None
            }
            for p in provenance
        ]
    }

async def search_with_kg_enrichment(query_text: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search memories and enrich results with Knowledge Graph data

    This provides:
    1. Regular memory search results
    2. Extracted entities and statements for each result
    3. Resolved canonical entity RIDs
    4. Complete provenance chains
    """
    logger.info(f"🔍 KG-enriched search for: {query_text[:50]}...")

    async with db_pool.acquire() as conn:
        # First, do text search on memories
        memories = await fallback_text_search(query_text, limit)

        # Enrich each memory with KG data
        enriched_memories = []
        for memory in memories:
            kg_data = await enrich_with_kg_data(memory['rid'], conn)

            if kg_data:
                memory['kg'] = kg_data
                logger.info(f"  ✓ Enriched {memory['rid'][:30]}... with {len(kg_data['entities'])} entities")
            else:
                memory['kg'] = None

            enriched_memories.append(memory)

        # Also search directly in KG entities and statements
        entity_matches = await conn.fetch("""
            SELECT DISTINCT
                kg.memory_rid,
                kg.extraction_rid,
                e->>'name' as entity_name,
                e->>'type' as entity_type,
                e->>'confidence' as confidence
            FROM koi_kg_extractions kg,
                 jsonb_array_elements(kg.entities) AS e
            WHERE e->>'name' ILIKE $1
            ORDER BY CAST(e->>'confidence' AS FLOAT) DESC
            LIMIT $2
        """, f'%{query_text}%', limit)

        statement_matches = await conn.fetch("""
            SELECT DISTINCT
                kg.memory_rid,
                kg.extraction_rid,
                s->>'subject' as subject,
                s->>'predicate' as predicate,
                s->>'object' as object,
                s->>'confidence' as confidence
            FROM koi_kg_extractions kg,
                 jsonb_array_elements(kg.statements) AS s
            WHERE s->>'subject' ILIKE $1
               OR s->>'predicate' ILIKE $1
               OR s->>'object' ILIKE $1
            ORDER BY CAST(s->>'confidence' AS FLOAT) DESC
            LIMIT $2
        """, f'%{query_text}%', limit)

        # Add KG match metadata
        kg_matches = {
            'entity_matches': [dict(e) for e in entity_matches],
            'statement_matches': [dict(s) for s in statement_matches]
        }

        return enriched_memories, kg_matches

@app.post("/search", response_model=KnowledgeResponse)
async def search_knowledge(query: KnowledgeQuery):
    """
    Search KOI knowledge base using Hybrid RAG API
    Falls back to simple text search if API unavailable
    """
    logger.info(f"Searching for: {query.query[:100]}...")

    try:
        # Try Hybrid RAG API first (RRF + BGE + Adaptive)
        hybrid_result = await call_hybrid_rag_api(
            query.query,
            query.agent_id,
            query.limit,
            query.source_filter
        )

        if hybrid_result:
            # Transform Hybrid RAG response to MCP format
            memories = []
            for result in hybrid_result.get('results', [])[:query.limit]:
                memory = {
                    "rid": result.get('rid', 'unknown'),
                    "cid": None,  # Not provided by Hybrid RAG
                    "content": result.get('content', ''),
                    "metadata": {"source": result.get('source', 'unknown')},
                    "created_at": None,
                    "source_sensor": result.get('source', 'unknown'),
                    "version": 1,
                    "similarity": result.get('score', 0.0)
                }
                memories.append(memory)

            return KnowledgeResponse(
                success=True,
                memories=memories,
                count=len(memories),
                query_embedding_generated=True,
                confidence=hybrid_result.get('confidence'),
                triggered_extraction=hybrid_result.get('triggered_extraction'),
                execution_time=hybrid_result.get('execution_time'),
                search_method='hybrid_rag'
            )

        # Fallback to simple text search
        logger.info("⚠️ Hybrid RAG unavailable, using fallback")
        memories = await fallback_text_search(query.query, query.limit, query.source_filter)

        return KnowledgeResponse(
            success=True,
            memories=memories,
            count=len(memories),
            query_embedding_generated=False,
            search_method='fallback'
        )

    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/memory/{rid}")
async def get_memory(rid: str):
    """Get specific memory by RID"""
    async with db_pool.acquire() as conn:
        result = await conn.fetchrow("""
            SELECT 
                rid,
                cid,
                content,
                metadata,
                created_at,
                version
            FROM koi_memories
            WHERE rid = $1
        """, rid)
        
        if not result:
            raise HTTPException(status_code=404, detail="Memory not found")
        
        return {
            "rid": result['rid'],
            "cid": result['cid'],
            "content": result['content'],
            "metadata": result['metadata'],
            "created_at": result['created_at'].isoformat() if result['created_at'] else None,
            "version": result['version']
        }

@app.get("/stats")
async def get_stats():
    """Get KOI knowledge statistics"""
    async with db_pool.acquire() as conn:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_memories,
                COUNT(DISTINCT source_sensor) as unique_sensors,
                COUNT(CASE WHEN embedding IS NOT NULL THEN 1 END) as memories_with_embeddings,
                MIN(created_at) as oldest_memory,
                MAX(created_at) as newest_memory
            FROM koi_memories
        """)
        
        # Get top sensors
        top_sensors = await conn.fetch("""
            SELECT 
                source_sensor,
                COUNT(*) as count
            FROM koi_memories
            GROUP BY source_sensor
            ORDER BY count DESC
            LIMIT 5
        """)
        
        return {
            "total_memories": stats['total_memories'],
            "unique_sensors": stats['unique_sensors'],
            "memories_with_embeddings": stats['memories_with_embeddings'],
            "oldest_memory": stats['oldest_memory'].isoformat() if stats['oldest_memory'] else None,
            "newest_memory": stats['newest_memory'].isoformat() if stats['newest_memory'] else None,
            "top_sensors": [
                {"sensor": row['source_sensor'], "count": row['count']}
                for row in top_sensors
            ]
        }

# ============================================================================
# MCP Protocol Implementation (JSON-RPC 2.0 with Streamable HTTP)
# ============================================================================

class MCPMessage(BaseModel):
    """Base MCP JSON-RPC 2.0 message"""
    jsonrpc: str = "2.0"
    id: Optional[Union[str, int]] = None

class MCPRequest(MCPMessage):
    """MCP request message"""
    method: str
    params: Optional[Dict[str, Any]] = None

class MCPResponse(MCPMessage):
    """MCP response message"""
    result: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

class MCPCapabilities(BaseModel):
    """MCP server capabilities"""
    tools: Dict[str, bool] = {"listChanged": False}

class MCPServerInfo(BaseModel):
    """MCP server information"""
    name: str = "koi-knowledge"
    version: str = "3.0.0"

class MCPImplementation(BaseModel):
    """MCP implementation details"""
    name: str = "koi-knowledge-mcp"
    version: str = "3.0.0"

class MCPInitializeResult(BaseModel):
    """Result of initialize request"""
    protocolVersion: str = "2025-03-26"
    capabilities: MCPCapabilities
    serverInfo: MCPServerInfo
    instructions: Optional[str] = None

# MCP Tool definitions
MCP_TOOLS = [
    {
        "name": "search_knowledge",
        "description": "Search the KOI knowledge graph using hybrid RAG (RRF + BGE embeddings + keyword search). Returns relevant documents with confidence scores.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query text"
                },
                "agent_id": {
                    "type": "string",
                    "description": "Agent identifier (optional)"
                },
                "limit": {
                    "type": "number",
                    "description": "Maximum number of results (default: 10)",
                    "default": 10
                },
                "source_filter": {
                    "type": "string",
                    "description": "Filter by source sensor (e.g., 'podcast' to only search podcast content)"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "search_with_kg",
        "description": "Search memories with Knowledge Graph enrichment. Returns documents with extracted entities, statements, canonical entity RIDs, and complete provenance chains. Use this to find structured knowledge about entities, relationships, and facts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query text (searches memory content, entities, and statements)"
                },
                "limit": {
                    "type": "number",
                    "description": "Maximum number of results (default: 10)",
                    "default": 10
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_memory",
        "description": "Retrieve a specific memory by its RID (Resource Identifier)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "rid": {
                    "type": "string",
                    "description": "The Resource ID (RID) of the memory to retrieve"
                }
            },
            "required": ["rid"]
        }
    },
    {
        "name": "get_stats",
        "description": "Get statistics about the KOI knowledge base (total memories, sensors, embedding coverage, etc.)",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]

async def handle_mcp_initialize(params: Dict[str, Any]) -> MCPInitializeResult:
    """Handle MCP initialize request"""
    client_info = params.get('clientInfo', {})
    client_version = params.get('protocolVersion', 'unknown')

    logger.info(f"MCP initialize - Client: {client_info.get('name', 'unknown')} {client_info.get('version', '')}")
    logger.info(f"MCP initialize - Client protocol version: {client_version}")
    logger.info(f"MCP initialize - Client capabilities: {params.get('capabilities', {})}")

    return MCPInitializeResult(
        protocolVersion="2025-03-26",
        capabilities=MCPCapabilities(
            tools={"listChanged": False}
        ),
        serverInfo=MCPServerInfo(
            name="koi-knowledge",
            version="3.0.0"
        ),
        instructions="KOI Knowledge Graph - Search 6,400+ documents about regenerative agriculture, Regen Network, and ecological credits using hybrid RAG (RRF + BGE embeddings + keyword search) with confidence-based results."
    )

async def handle_mcp_list_tools(params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle MCP tools/list request"""
    return {"tools": MCP_TOOLS}

async def handle_mcp_call_tool(params: Dict[str, Any]) -> Dict[str, Any]:
    """Handle MCP tools/call request"""
    tool_name = params.get("name")
    arguments = params.get("arguments", {})

    logger.info(f"MCP tool call: {tool_name} with args: {arguments}")

    if tool_name == "search_knowledge":
        # Call hybrid RAG search
        query_text = arguments.get("query", "")
        agent_id = arguments.get("agent_id")
        limit = arguments.get("limit", 10)
        source_filter = arguments.get("source_filter")

        hybrid_result = await call_hybrid_rag_api(query_text, agent_id, limit, source_filter)

        if hybrid_result:
            # Format results for MCP response
            results = []
            for r in hybrid_result.get('results', [])[:limit]:
                results.append({
                    "rid": r.get('rid', 'unknown'),
                    "content": r.get('content', ''),
                    "source": r.get('source', 'unknown'),
                    "score": r.get('score', 0.0)
                })

            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "query": query_text,
                        "total_results": len(results),
                        "confidence": hybrid_result.get('confidence', 0.0),
                        "search_method": "hybrid_rag",
                        "results": results
                    }, indent=2)
                }],
                "isError": False
            }
        else:
            # Fallback search
            memories = await fallback_text_search(query_text, limit, source_filter)
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "query": query_text,
                        "total_results": len(memories),
                        "search_method": "fallback",
                        "results": memories
                    }, indent=2)
                }],
                "isError": False
            }

    elif tool_name == "search_with_kg":
        # KG-enriched search
        query_text = arguments.get("query", "")
        limit = arguments.get("limit", 10)

        enriched_memories, kg_matches = await search_with_kg_enrichment(query_text, limit)

        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "success": True,
                    "query": query_text,
                    "total_results": len(enriched_memories),
                    "search_method": "kg_enriched",
                    "kg_matches": {
                        "entity_matches": len(kg_matches['entity_matches']),
                        "statement_matches": len(kg_matches['statement_matches'])
                    },
                    "results": enriched_memories,
                    "direct_kg_matches": kg_matches
                }, indent=2, default=str)
            }],
            "isError": False
        }

    elif tool_name == "get_memory":
        rid = arguments.get("rid")
        async with db_pool.acquire() as conn:
            result = await conn.fetchrow("""
                SELECT rid, cid, content, metadata, created_at, version
                FROM koi_memories
                WHERE rid = $1
            """, rid)

            if result:
                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "success": True,
                            "memory": {
                                "rid": result['rid'],
                                "cid": result['cid'],
                                "content": result['content'],
                                "metadata": result['metadata'],
                                "created_at": result['created_at'].isoformat() if result['created_at'] else None,
                                "version": result['version']
                            }
                        }, indent=2)
                    }],
                    "isError": False
                }
            else:
                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "success": False,
                            "error": f"Memory not found: {rid}"
                        })
                    }],
                    "isError": True
                }

    elif tool_name == "get_stats":
        async with db_pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_memories,
                    COUNT(DISTINCT source_sensor) as unique_sensors,
                    COUNT(CASE WHEN embedding IS NOT NULL THEN 1 END) as memories_with_embeddings,
                    MIN(created_at) as oldest_memory,
                    MAX(created_at) as newest_memory
                FROM koi_memories
            """)

            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "success": True,
                        "stats": {
                            "total_memories": stats['total_memories'],
                            "unique_sensors": stats['unique_sensors'],
                            "memories_with_embeddings": stats['memories_with_embeddings'],
                            "oldest_memory": stats['oldest_memory'].isoformat() if stats['oldest_memory'] else None,
                            "newest_memory": stats['newest_memory'].isoformat() if stats['newest_memory'] else None
                        }
                    }, indent=2)
                }],
                "isError": False
            }

    else:
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({
                    "success": False,
                    "error": f"Unknown tool: {tool_name}"
                })
            }],
            "isError": True
        }

async def handle_mcp_request(request: MCPRequest) -> MCPResponse:
    """Handle MCP JSON-RPC 2.0 request"""
    method = request.method
    params = request.params or {}

    logger.info(f"MCP request: {method}")

    try:
        if method == "initialize":
            result = await handle_mcp_initialize(params)
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                result=result.dict()
            )

        elif method == "tools/list":
            result = await handle_mcp_list_tools(params)
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                result=result
            )

        elif method == "tools/call":
            result = await handle_mcp_call_tool(params)
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                result=result
            )

        elif method == "ping":
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                result={}
            )

        else:
            return MCPResponse(
                jsonrpc="2.0",
                id=request.id,
                error={
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            )

    except Exception as e:
        logger.error(f"MCP request error: {e}")
        return MCPResponse(
            jsonrpc="2.0",
            id=request.id,
            error={
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        )

@app.post("/mcp")
async def mcp_post_endpoint(request: Request):
    """
    MCP Streamable HTTP POST endpoint (2025-03-26 specification)
    Handles JSON-RPC 2.0 messages from clients

    Client MUST:
    - Use POST method
    - Include Accept header with both application/json and text/event-stream
    - Send JSON-RPC 2.0 message(s) in body

    Server responds with:
    - Content-Type: application/json for simple responses
    - Content-Type: text/event-stream for streaming (multiple messages)
    """
    accept_header = request.headers.get("accept", "application/json")
    logger.info(f"POST /mcp - Accept header: {accept_header}")

    try:
        body = await request.json()
        logger.info(f"POST /mcp - Request body type: {type(body)}")

        # Handle batch requests
        if isinstance(body, list):
            responses = []
            for req_data in body:
                mcp_req = MCPRequest(**req_data)
                mcp_res = await handle_mcp_request(mcp_req)
                responses.append(mcp_res.dict(exclude_none=True))

            # For batch requests, could use SSE if client prefers
            if "text/event-stream" in accept_header and len(responses) > 1:
                async def sse_stream():
                    for response in responses:
                        yield f"data: {json.dumps(response)}\n\n"
                    yield "event: done\ndata: {}\n\n"

                return StreamingResponse(
                    sse_stream(),
                    media_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache",
                        "Connection": "keep-alive",
                        "X-Accel-Buffering": "no"
                    }
                )
            else:
                return Response(
                    content=json.dumps(responses),
                    media_type="application/json"
                )

        # Handle single request
        else:
            mcp_req = MCPRequest(**body)
            mcp_res = await handle_mcp_request(mcp_req)

            return Response(
                content=json.dumps(mcp_res.dict(exclude_none=True)),
                media_type="application/json"
            )

    except Exception as e:
        logger.error(f"MCP POST endpoint error: {e}", exc_info=True)
        error_response = MCPResponse(
            jsonrpc="2.0",
            error={
                "code": -32700,
                "message": f"Parse error: {str(e)}"
            }
        )
        return Response(
            content=json.dumps(error_response.dict(exclude_none=True)),
            media_type="application/json",
            status_code=400
        )

@app.get("/mcp")
async def mcp_get_endpoint(request: Request):
    """
    MCP Streamable HTTP GET endpoint (optional SSE stream)
    Allows server to send notifications to client

    This is OPTIONAL per spec - servers MAY support it
    Returns SSE stream or 405 Method Not Allowed
    """
    logger.info("GET /mcp - Opening SSE stream for server-to-client messages")

    async def server_event_stream():
        # Per MCP spec, GET SSE stream should only send JSON-RPC messages or keepalive comments
        # Do NOT send custom events - they confuse clients
        # Keep connection alive with pings (SSE comment format)
        while True:
            await asyncio.sleep(30)
            yield ": keepalive\n\n"

    return StreamingResponse(
        server_event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

if __name__ == "__main__":
    import uvicorn

    # Run server
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8200,
        log_level="info"
    )