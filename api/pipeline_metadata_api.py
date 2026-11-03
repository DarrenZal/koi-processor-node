#!/usr/bin/env python3
"""
Pipeline Metadata API
Provides access to KOI pipeline structure and component metadata
"""

import os
import time
import json
import hashlib
import asyncio
import httpx
import asyncpg
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging
from datetime import datetime
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection: read from the environment (.env via the systemd unit)
POSTGRES_URL = os.environ.get("POSTGRES_URL")
if not POSTGRES_URL:
    raise RuntimeError("POSTGRES_URL is not set; pipeline_metadata_api reads its database connection from the environment")

# FastAPI app
app = FastAPI(title="Pipeline Metadata API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define namespaces
KOI = Namespace("http://koi.regen.network/ontology#")
INFRA = Namespace("http://koi.regen.network/infrastructure/")
SENSOR = Namespace("http://koi.regen.network/sensor/")

# Pydantic models
class Component(BaseModel):
    id: str
    rid: str
    type: str  # sensor, processor, storage, service
    label: str
    status: str
    endpoint: Optional[str] = None
    port: Optional[int] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = {}

class Connection(BaseModel):
    source: str
    target: str
    type: str  # data, control, monitoring, query
    label: Optional[str] = None  # Label for the edge based on predicate

class PipelineStructure(BaseModel):
    components: List[Component]
    connections: List[Connection]
    metadata: Dict[str, Any]

class ComponentStatus(BaseModel):
    id: str
    status: str
    last_updated: str
    metrics: Optional[Dict[str, Any]] = {}

# URL Reconstruction Functions

def reconstruct_url_from_metadata(rid: str, metadata: dict) -> Optional[str]:
    """
    Reconstruct source URL from RID and metadata
    Handles different source types: discourse, github, notion, web, etc.
    """

    # Direct URL in metadata (best case)
    if metadata.get('url'):
        return metadata['url']

    if metadata.get('post_url'):
        return metadata['post_url']

    # Check for discourse topic_url
    if metadata.get('topic_url'):
        # If we have post_number, append it to topic URL
        post_num = metadata.get('post_number')
        topic_url = metadata['topic_url']
        if post_num and post_num > 1:
            return f"{topic_url}/{post_num}"
        return topic_url

    source = metadata.get('source', '')
    source_type = metadata.get('source_type', '')
    original_id = metadata.get('original_id', '')

    # Discourse forum posts
    if 'discourse' in source or source_type == 'forum-post':
        # Parse original_id like "forum.regen.network_136_post_3"
        if '_post_' in original_id:
            parts = original_id.split('_post_')
            if len(parts) == 2:
                topic_parts = parts[0].split('_')
                topic_id = topic_parts[-1]
                post_number = parts[1]
                forum_domain = source.replace('discourse:', '')
                if topic_id and forum_domain:
                    return f"https://{forum_domain}/t/{topic_id}/{post_number}"

        # Try parsing from RID
        # Format: regen.forum-topic:forum.regen.network_topic_136#chunk0
        if 'forum-topic:' in rid or 'forum-post:' in rid:
            # Extract domain and topic ID from RID
            if '_topic_' in rid or '_post_' in rid:
                parts = rid.split(':')
                if len(parts) >= 2:
                    id_part = parts[1].split('#')[0]  # Remove chunk suffix

                    # Parse "forum.regen.network_topic_136" or similar
                    if '_topic_' in id_part:
                        domain_and_topic = id_part.split('_topic_')
                        if len(domain_and_topic) == 2:
                            forum_domain = domain_and_topic[0]
                            topic_id = domain_and_topic[1]
                            return f"https://{forum_domain}/t/{topic_id}"

                    elif '_post_' in id_part:
                        # Parse "forum.regen.network_136_post_3"
                        post_match = id_part.split('_post_')
                        if len(post_match) == 2:
                            topic_parts = post_match[0].split('_')
                            topic_id = topic_parts[-1]
                            post_number = post_match[1]
                            # Get domain from first part
                            forum_domain = '_'.join(topic_parts[:-1])
                            return f"https://{forum_domain}/t/{topic_id}/{post_number}"

    # GitHub repositories
    elif 'github' in source or source_type == 'github':
        repo = metadata.get('repository', '')
        if repo:
            return f"https://github.com/{repo}"

        # Try parsing from RID
        if 'github:' in rid:
            parts = rid.split('github:')
            if len(parts) == 2:
                repo_path = parts[1].split('#')[0]
                return f"https://github.com/{repo_path}"

    # Notion pages
    elif 'notion' in source or source_type == 'notion':
        page_id = metadata.get('page_id', '')
        if page_id:
            return f"https://www.notion.so/{page_id}"

    # Web pages
    elif source_type in ['web', 'webpage', 'website']:
        domain = metadata.get('domain', '')
        path = metadata.get('path', '')
        if domain:
            return f"https://{domain}{path}" if path else f"https://{domain}"

    return None

async def fetch_source_url(conn, rid: str) -> Optional[str]:
    """
    Fetch source URL for a given RID from database
    Tries multiple strategies:
    1. koi_content.url (new pipeline)
    2. koi_memory_chunks -> koi_content.url (via link)
    3. koi_memories.metadata->>'url' (old pipeline with URL)
    4. Reconstruct from koi_memories.metadata (old pipeline)
    """

    # Strategy 1: Check koi_content directly
    result = await conn.fetchrow("""
        SELECT metadata->>'url' as url, metadata
        FROM koi_content
        WHERE rid = $1
    """, rid)

    if result:
        if result['url']:
            return result['url']
        # Try reconstruction from koi_content metadata
        if result['metadata']:
            # metadata is returned as JSON, convert to dict if needed
            metadata = result['metadata'] if isinstance(result['metadata'], dict) else json.loads(result['metadata'])
            reconstructed = reconstruct_url_from_metadata(rid, metadata)
            if reconstructed:
                return reconstructed

    # Strategy 2: Check via koi_memory_chunks
    result = await conn.fetchrow("""
        SELECT c.metadata->>'url' as url, c.metadata
        FROM koi_memory_chunks mc
        JOIN koi_content c ON mc.source_content_rid = c.rid
        WHERE mc.chunk_rid = $1
    """, rid)

    if result:
        if result['url']:
            return result['url']
        # Try reconstruction from linked koi_content metadata
        if result['metadata']:
            metadata = result['metadata'] if isinstance(result['metadata'], dict) else json.loads(result['metadata'])
            reconstructed = reconstruct_url_from_metadata(rid, metadata)
            if reconstructed:
                return reconstructed

    # Strategy 3 & 4: Check koi_memories (old pipeline)
    result = await conn.fetchrow("""
        SELECT metadata->>'url' as url, metadata
        FROM koi_memories
        WHERE rid = $1
    """, rid)

    if result:
        if result['url']:
            return result['url']

        # Try reconstruction from metadata
        if result['metadata']:
            metadata = result['metadata'] if isinstance(result['metadata'], dict) else json.loads(result['metadata'])
            reconstructed = reconstruct_url_from_metadata(rid, metadata)
            if reconstructed:
                return reconstructed

    # Strategy 5: Check transformation receipts (sensor_collection has source URL)
    result = await conn.fetchrow("""
        SELECT metadata->>'source_url' as source_url, metadata->>'url' as url
        FROM koi_transformation_receipts
        WHERE output_rid = $1
          AND transformation_type = 'sensor_collection'
        ORDER BY created_at DESC
        LIMIT 1
    """, rid)

    if result:
        # Try source_url first, then url
        return result['source_url'] or result['url']

    # Last attempt: Try reconstruction from RID alone with empty metadata
    return reconstruct_url_from_metadata(rid, {})

# Load and parse RDF data
def load_pipeline_metadata() -> Graph:
    """Load pipeline metadata from TTL files"""
    g = Graph()

    # Bind namespaces
    g.bind("koi", KOI)
    g.bind("", INFRA)
    g.bind("sensor", SENSOR)
    g.bind("rdfs", RDFS)

    # Load ontology
    ontology_path = "/opt/projects/koi-processor/koi-ontology.ttl"
    if os.path.exists(ontology_path):
        g.parse(ontology_path, format="turtle")
        logger.info(f"Loaded ontology from {ontology_path}")

    # Load pipeline metadata
    metadata_path = "/opt/projects/koi-processor/pipeline-metadata.ttl"
    if os.path.exists(metadata_path):
        g.parse(metadata_path, format="turtle")
        logger.info(f"Loaded pipeline metadata from {metadata_path}")

    return g

def extract_components(graph: Graph) -> List[Component]:
    """Extract components from RDF graph"""
    components = []

    # Query for all components (sensors, processors, storage, services)
    query = """
    PREFIX koi: <http://koi.regen.network/ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>

    SELECT ?component ?type ?label ?rid ?status ?endpoint ?port ?description
    WHERE {
        ?component a ?class .
        ?class rdfs:subClassOf* koi:Component .
        OPTIONAL { ?component rdfs:label ?label }
        OPTIONAL { ?component koi:rid ?rid }
        OPTIONAL { ?component koi:status ?status }
        OPTIONAL { ?component koi:endpoint ?endpoint }
        OPTIONAL { ?component koi:port ?port }
        OPTIONAL { ?component dc:description ?description }
        OPTIONAL { ?component koi:sensorType ?sensorType }
        BIND(
            IF(EXISTS { ?component a koi:Sensor }, "sensor",
            IF(EXISTS { ?component a koi:Processor }, "processor",
            IF(EXISTS { ?component a koi:Coordinator }, "processor",
            IF(EXISTS { ?component a koi:Storage }, "storage",
            IF(EXISTS { ?component a koi:Service }, "service", "unknown")))))
            AS ?type
        )
    }
    """

    results = graph.query(query)

    for row in results:
        # Extract component ID from URI
        component_uri = str(row.component)
        component_id = component_uri.split("/")[-1].split("#")[-1]

        components.append(Component(
            id=component_id,
            rid=str(row.rid) if row.rid else f"koi.infrastructure:{component_id}",
            type=str(row.type) if row.type else "unknown",
            label=str(row.label) if row.label else component_id,
            status=str(row.status) if row.status else "unknown",
            endpoint=str(row.endpoint) if row.endpoint else None,
            port=int(row.port) if row.port else None,
            description=str(row.description) if row.description else None
        ))

    return components

def extract_connections(graph: Graph) -> List[Connection]:
    """Extract connections from RDF graph"""
    connections = []

    # Query for all connections
    query = """
    PREFIX koi: <http://koi.regen.network/ontology#>

    SELECT ?source ?target ?predicate
    WHERE {
        ?source ?predicate ?target .
        FILTER(?predicate IN (koi:sendsTo, koi:receivesFrom, koi:forwardsTo, koi:queriedBy, koi:queriesFrom, koi:writesTo))
    }
    """

    results = graph.query(query)

    for row in results:
        # Extract IDs from URIs
        source_id = str(row.source).split("/")[-1].split("#")[-1]
        target_id = str(row.target).split("/")[-1].split("#")[-1]
        predicate = str(row.predicate).split("#")[-1]

        # Map predicates to connection types and labels
        connection_type = "data"
        label = predicate.replace("_", " ").title()  # Default label

        if predicate == "queriedBy":
            connection_type = "query"
            label = "queries"
        elif predicate == "queriesFrom":
            connection_type = "query"
            label = "queries"
        elif predicate == "monitors":
            connection_type = "monitoring"
            label = "monitors"
        elif predicate == "sendsTo":
            label = "sends to"
        elif predicate == "receivesFrom":
            label = "receives"
        elif predicate == "forwardsTo":
            label = "forwards"
        elif predicate == "writesTo":
            label = "writes"
            connection_type = "data"

        connections.append(Connection(
            source=source_id,
            target=target_id,
            type=connection_type,
            label=label
        ))

    return connections

async def get_live_sensor_status() -> Dict[str, Any]:
    """Fetch live sensor status from coordinator"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get("http://localhost:8005/sensors")
            if response.status_code == 200:
                return response.json()
    except Exception as e:
        logger.warning(f"Could not fetch live sensor status: {e}")

    return {"sensors": []}

async def get_service_health(endpoint: str) -> str:
    """Check health of a service endpoint"""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{endpoint}/health")
            if response.status_code == 200:
                return "active"
    except:
        pass
    return "offline"

# =============================================================================
# Optional Bearer auth for privacy filtering
# Mirrors the SHA-256 hash + session_tokens lookup in src/services/auth_service.py
# Returns user_email when a valid @regen.network token is presented, else None.
# Endpoints use the return value to splice an is_private filter into their WHERE clause.
# =============================================================================

async def get_optional_user_email(authorization: Optional[str] = Header(None)) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token_hash = hashlib.sha256(parts[1].encode()).hexdigest()
    try:
        conn = await asyncpg.connect(POSTGRES_URL)
        try:
            row = await conn.fetchrow(
                """
                SELECT user_email, expires_at, revoked_at
                FROM session_tokens
                WHERE token_hash = $1
                """,
                token_hash,
            )
        finally:
            await conn.close()
    except Exception as e:
        logger.warning(f"session_tokens lookup failed: {e}")
        return None
    if not row or row["revoked_at"] is not None:
        return None
    if row["expires_at"] and row["expires_at"].timestamp() < time.time():
        return None
    user_email = row["user_email"] or ""
    if not user_email.endswith("@regen.network"):
        return None
    return user_email


# API Endpoints

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Pipeline Metadata API"}

@app.get("/api/koi/graph/pipeline", response_model=PipelineStructure)
async def get_pipeline_structure():
    """Get complete pipeline structure with components and connections"""

    # Load RDF data
    graph = load_pipeline_metadata()

    # Extract components and connections
    components = extract_components(graph)
    connections = extract_connections(graph)

    # Get live sensor status
    sensor_data = await get_live_sensor_status()

    # Update component status with live data
    sensor_map = {s["id"]: s for s in sensor_data.get("sensors", [])}

    for component in components:
        if component.type == "sensor" and component.id in sensor_map:
            sensor_info = sensor_map[component.id]
            component.status = sensor_info.get("status", "unknown")
            component.metadata = {
                "lastActivity": sensor_info.get("lastActivity"),
                "eventsProcessed": sensor_info.get("eventsProcessed"),
                "monitoring": sensor_info.get("monitoring", [])
            }

    return PipelineStructure(
        components=components,
        connections=connections,
        metadata={
            "timestamp": datetime.utcnow().isoformat(),
            "version": "2.0.0",
            "graph_size": len(graph)
        }
    )

@app.get("/api/koi/graph/components")
async def get_components(component_type: Optional[str] = None):
    """Get all components, optionally filtered by type"""

    graph = load_pipeline_metadata()
    components = extract_components(graph)

    if component_type:
        components = [c for c in components if c.type == component_type]

    return {"components": components, "count": len(components)}

@app.get("/api/koi/graph/connections")
async def get_connections(component_id: Optional[str] = None):
    """Get all connections, optionally filtered by component"""

    graph = load_pipeline_metadata()
    connections = extract_connections(graph)

    if component_id:
        connections = [
            c for c in connections
            if c.source == component_id or c.target == component_id
        ]

    return {"connections": connections, "count": len(connections)}

@app.get("/api/koi/graph/component/{component_id}")
async def get_component_detail(component_id: str):
    """Get detailed information about a specific component"""

    graph = load_pipeline_metadata()
    components = extract_components(graph)

    # Find the component
    component = next((c for c in components if c.id == component_id), None)

    if not component:
        raise HTTPException(status_code=404, detail="Component not found")

    # Get connections for this component
    connections = extract_connections(graph)
    incoming = [c for c in connections if c.target == component_id]
    outgoing = [c for c in connections if c.source == component_id]

    # Check live status if it's a service
    if component.endpoint:
        component.status = await get_service_health(component.endpoint)

    return {
        "component": component,
        "incoming_connections": incoming,
        "outgoing_connections": outgoing
    }

@app.get("/api/koi/graph/provenance/{rid:path}")
async def get_document_provenance(rid: str):
    """Get provenance information for a document through the pipeline"""

    import asyncpg
    from urllib.parse import unquote

    # Decode the RID if it was URL encoded
    rid = unquote(rid)

    try:
        # Connect to database
        conn = await asyncpg.connect(POSTGRES_URL)

        try:
            # Fetch source URL using our reconstruction logic
            source_url = await fetch_source_url(conn, rid)

            # Strip content: prefix if present for transformation receipt queries
            # koi_content uses content:orn:... but transformation_receipts use orn:...
            receipt_rid = rid.replace("content:", "") if rid.startswith("content:") else rid

            # Extract base RID (without #chunkN suffix) for chunks
            base_rid = receipt_rid.split('#')[0] if '#' in receipt_rid else receipt_rid

            # Query transformation receipts for this RID
            # For chunks, include base document transformations but exclude other chunks
            receipts = await conn.fetch("""
                WITH RECURSIVE chain AS (
                    -- Base case: Find all transformations directly involving this RID
                    SELECT * FROM koi_transformation_receipts
                    WHERE input_rid = $1 OR output_rid = $1

                    -- For chunks, also include transformations on the base document
                    -- BUT exclude koi_to_memory transformations that create other chunks
                    UNION
                    SELECT * FROM koi_transformation_receipts
                    WHERE ($1 != $2) AND (input_rid = $2 OR output_rid = $2)
                    AND NOT (
                        -- Exclude other chunks' creation receipts
                        transformation_type = 'koi_to_memory'
                        AND output_rid LIKE $2 || '#chunk%'
                        AND output_rid != $1
                    )

                    UNION

                    -- Recursive case: Trace backwards through input_rid
                    SELECT t.* FROM koi_transformation_receipts t
                    INNER JOIN chain c ON t.output_rid = c.input_rid
                )
                SELECT
                    receipt_id,
                    transformation_type,
                    input_rid,
                    output_rid,
                    source_sensor,
                    event_type,
                    chunks_created,
                    embeddings_created,
                    entities_extracted,
                    processor_name,
                    created_at,
                    metadata
                FROM chain
                ORDER BY created_at ASC
            """, receipt_rid, base_rid)

            # Build provenance timeline with logical ordering
            timeline = []
            sensors = set()
            processors = set()
            storage = set()
            first_sensor = None

            for receipt in receipts:
                receipt_data = {
                    "timestamp": receipt['created_at'].isoformat() if receipt['created_at'] else None,
                    "type": receipt['transformation_type'],
                    "receipt_id": receipt['receipt_id'],
                    "input_rid": receipt['input_rid'],
                    "output_rid": receipt['output_rid'],
                    "details": {
                        "processor": receipt['processor_name'],
                        "chunks_created": receipt['chunks_created'],
                        "embeddings_created": receipt['embeddings_created'],
                        "entities_extracted": receipt['entities_extracted']
                    }
                }

                # For sensor collection, use the source URL as input if available
                if ('collection' in receipt['transformation_type'].lower() or
                    'sensor' in receipt['transformation_type'].lower()) and not receipt['input_rid']:
                    # We'll add the source_url after we fetch it
                    receipt_data['_needs_source_url'] = True

                timeline.append(receipt_data)

                # Track components (only first sensor)
                if receipt['source_sensor'] and not first_sensor:
                    first_sensor = receipt['source_sensor']
                    sensors.add(receipt['source_sensor'])
                if receipt['processor_name']:
                    processors.add(receipt['processor_name'])

                # Infer storage
                if 'embedding' in receipt['transformation_type']:
                    storage.add('postgresql-pgvector')
                if 'graph' in receipt['transformation_type']:
                    storage.add('apache-jena')
                if 'memory' in receipt['transformation_type']:
                    storage.add('postgresql')

            # Sort timeline by logical provenance flow:
            # 1. Sensor collection first (no input or collection type)
            # 2. Then by RID length (base RID before chunks/derivatives)
            # 3. Then by timestamp
            # 4. Then by transformation type (for stable sort when timestamps match)
            def sort_key(receipt):
                # Priority 1: Collection events first
                is_collection = (
                    not receipt['input_rid'] or
                    'collection' in receipt['type'].lower() or
                    'sensor' in receipt['type'].lower()
                )
                collection_priority = 0 if is_collection else 1

                # Priority 2: Shorter RIDs first (base before derived)
                rid_length = len(receipt['output_rid']) if receipt['output_rid'] else 999

                # Priority 3: Earlier timestamps first
                timestamp = receipt['timestamp'] or '9999'

                # Priority 4: Transformation type order for same timestamps
                # Order: collection → forwarding → processing → memory → embedding
                type_order = {
                    'sensor_collection': 0,
                    'coordinator_forwarding': 1,
                    'koi_event_processing': 2,
                    'koi_to_memory': 3,
                    'memory_to_bge_embedding': 4
                }
                type_priority = type_order.get(receipt['type'], 99)

                return (collection_priority, rid_length, timestamp, type_priority)

            timeline.sort(key=sort_key)

            # If querying a chunk and there's no koi_to_memory receipt for it,
            # add a synthetic entry to show it was created during initial chunking
            if '#chunk' in receipt_rid:
                has_chunk_creation = any(
                    r['output_rid'] == receipt_rid and 'memory' in r['type'].lower()
                    for r in timeline
                )

                if not has_chunk_creation:
                    # Find the earliest embedding timestamp for this chunk to estimate creation time
                    chunk_embedding = next(
                        (r for r in timeline if r['input_rid'] == receipt_rid and 'embedding' in r['type'].lower()),
                        None
                    )

                    if chunk_embedding:
                        synthetic_chunk = {
                            "timestamp": chunk_embedding['timestamp'],
                            "type": "koi_to_memory",
                            "receipt_id": "synthetic-chunk-creation",
                            "input_rid": base_rid,
                            "output_rid": receipt_rid,
                            "details": {
                                "processor": "KOI Event Bridge v2",
                                "chunks_created": 1,
                                "embeddings_created": 0,
                                "entities_extracted": 0,
                                "note": "Created during initial chunking (receipt not recorded)"
                            }
                        }
                        # Insert before the embedding step
                        embedding_idx = timeline.index(chunk_embedding)
                        timeline.insert(embedding_idx, synthetic_chunk)
                        processors.add("KOI Event Bridge v2")

            # If we don't have a source URL yet, try to get it from sensor_collection receipt metadata
            if not source_url:
                sensor_receipt = next((r for r in receipts if r['transformation_type'] == 'sensor_collection'), None)
                if sensor_receipt and sensor_receipt['metadata']:
                    metadata = sensor_receipt['metadata'] if isinstance(sensor_receipt['metadata'], dict) else json.loads(sensor_receipt['metadata'])
                    source_url = metadata.get('source_url') or metadata.get('url')

            # Add source URL to sensor collection receipts that need it
            for receipt in timeline:
                if receipt.get('_needs_source_url') and source_url:
                    receipt['input_rid'] = source_url
                    del receipt['_needs_source_url']  # Clean up the marker

            # Also check if document exists in koi_content
            doc = await conn.fetchrow("""
                SELECT rid as id, title, metadata->>'source' as source_sensor, created_at, content_hash
                FROM koi_content
                WHERE rid = $1
            """, rid)

            # Only add doc sensor if we don't have one from receipts (first_sensor takes priority)
            if doc and doc['source_sensor'] and not first_sensor:
                sensors.add(doc['source_sensor'])

            return {
                "rid": rid,
                "found": len(receipts) > 0 or doc is not None,
                "document": {
                    "title": doc['title'] if doc else None,
                    "source_sensor": doc['source_sensor'] if doc else None,
                    "source_url": source_url,  # Add reconstructed URL
                    "created_at": doc['created_at'].isoformat() if doc and doc['created_at'] else None,
                    "content_hash": doc['content_hash'] if doc else None
                } if doc else {"source_url": source_url},  # Include URL even if no doc found
                "provenance": {
                    "sensed_by": list(sensors),
                    "processed_by": list(processors),
                    "stored_in": list(storage),
                    "transformation_count": len(receipts),
                    "first_seen": timeline[0]["timestamp"] if timeline else None,
                    "last_updated": timeline[-1]["timestamp"] if timeline else None
                },
                "timeline": timeline
            }

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"Error fetching provenance for {rid}: {e}")
        return {
            "rid": rid,
            "error": str(e),
            "found": False
        }

@app.get("/api/koi/rids")
async def list_available_rids(
    limit: int = Query(default=50, le=500),
    source: Optional[str] = Query(default=None, description="Filter by source (e.g., 'notion', 'github', 'discourse')"),
    context: Optional[str] = Query(default=None, description="Filter by RID context pattern"),
    offset: int = Query(default=0, ge=0),
    indexed_after: Optional[str] = Query(default=None, description="Only RIDs indexed after this date (YYYY-MM-DD)"),
    indexed_before: Optional[str] = Query(default=None, description="Only RIDs indexed before this date (YYYY-MM-DD)"),
    user_email: Optional[str] = Depends(get_optional_user_email),
):
    """List available RIDs from indexed documents (koi_memories)"""

    try:
        # Connect to database
        conn = await asyncpg.connect(POSTGRES_URL)

        try:
            # Build WHERE clause for filters
            where_conditions = [
                "rid IS NOT NULL",
                "rid NOT LIKE '%test_%'",
                "rid NOT LIKE '%heartbeat%'",
                "rid NOT LIKE '%:demo:%'",
                "(is_chunk = FALSE OR is_chunk IS NULL)"  # Only parent documents, not chunks
            ]
            # Privacy filter: unauthenticated callers see only public rows
            if not user_email:
                where_conditions.append("(is_private = FALSE OR is_private IS NULL)")
            params = []
            param_idx = 1

            if source:
                where_conditions.append(f"source_sensor ILIKE ${param_idx}")
                params.append(f"%{source}%")
                param_idx += 1

            if context:
                where_conditions.append(f"rid LIKE ${param_idx}")
                params.append(f"{context}%")
                param_idx += 1

            if indexed_after:
                try:
                    after_date = datetime.strptime(indexed_after, "%Y-%m-%d").date()
                    where_conditions.append(f"created_at >= ${param_idx}")
                    params.append(after_date)
                    param_idx += 1
                except ValueError:
                    pass  # Invalid date format, skip filter

            if indexed_before:
                try:
                    before_date = datetime.strptime(indexed_before, "%Y-%m-%d").date()
                    # Add 1 day to include the full day
                    from datetime import timedelta
                    before_date = before_date + timedelta(days=1)
                    where_conditions.append(f"created_at < ${param_idx}")
                    params.append(before_date)
                    param_idx += 1
                except ValueError:
                    pass  # Invalid date format, skip filter

            where_clause = " AND ".join(where_conditions)

            # Get total count for pagination
            count_query = f"""
                SELECT COUNT(*) FROM koi_memories
                WHERE {where_clause}
            """
            total_count = await conn.fetchval(count_query, *params)

            # Get RIDs from koi_memories (the main indexed documents table)
            params_with_limit = params + [limit, offset]
            doc_rids = await conn.fetch(f"""
                SELECT DISTINCT
                    rid,
                    content->>'title' as title,
                    source_sensor,
                    created_at,
                    metadata->>'url' as url
                FROM koi_memories
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT ${param_idx} OFFSET ${param_idx + 1}
            """, *params_with_limit)

            # Format results
            rids = []
            by_context = {}
            by_source = {}

            for doc in doc_rids:
                rid = doc['rid']

                # Extract context from RID (e.g., "orn:regen.document" from "orn:regen.document:notion/...")
                context_part = rid.split(':')[0] if ':' in rid else 'unknown'
                if len(rid.split(':')) > 1:
                    context_part = ':'.join(rid.split(':')[:2])

                by_context[context_part] = by_context.get(context_part, 0) + 1

                # Track by source
                src = doc['source_sensor'] or 'unknown'
                # Simplify source (e.g., "notion-sensor-123" -> "notion")
                src_simple = src.split('-')[0] if '-' in src else src
                by_source[src_simple] = by_source.get(src_simple, 0) + 1

                # Infer rid_type from context
                rid_type = None
                if 'notion' in rid.lower():
                    rid_type = 'NotionDocument'
                elif 'github' in rid.lower():
                    rid_type = 'GitHubDocument'
                elif 'discourse' in rid.lower() or 'forum' in rid.lower():
                    rid_type = 'DiscoursePost'
                elif 'youtube' in rid.lower():
                    rid_type = 'YouTubeVideo'

                rids.append({
                    "rid": rid,
                    "context": context_part,
                    "rid_type": rid_type,
                    "source": src_simple,
                    "title": doc['title'] or f"Document {rid[:50]}...",
                    "url": doc['url'],
                    "indexed_at": doc['created_at'].isoformat() if doc['created_at'] else None
                })

            return {
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "has_more": offset + len(rids) < total_count
                },
                "by_context": by_context,
                "by_source": by_source,
                "rids": rids
            }

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"Error listing RIDs: {e}")
        return {
            "error": str(e),
            "rids": [],
            "pagination": {"total": 0, "limit": limit, "offset": offset, "has_more": False}
        }

@app.get("/api/koi/rid-lookup/{rid:path}")
async def rid_lookup(
    rid: str,
    include_chunks: bool = Query(default=True, description="Include chunk documents"),
    limit: int = Query(default=50, le=200),
    user_email: Optional[str] = Depends(get_optional_user_email),
):
    """Look up documents by exact RID match from koi_memories.

    This queries the database directly by RID, unlike /query which uses semantic search.
    Returns the base document and optionally all chunks.
    """
    from urllib.parse import unquote

    # URL decode the RID
    rid = unquote(rid)

    try:
        conn = await asyncpg.connect(POSTGRES_URL)

        try:
            # Build query - search for exact RID or RID with chunk suffix
            base_rid = rid.split('#')[0]  # Remove chunk suffix if present

            # Unauthenticated callers see only public rows
            privacy_clause = "" if user_email else " AND (is_private = FALSE OR is_private IS NULL)"

            if include_chunks:
                # Get base doc and all chunks
                query = f"""
                    SELECT
                        rid,
                        content->>'title' as title,
                        content->>'text' as text,
                        source_sensor,
                        created_at,
                        metadata->>'url' as url,
                        is_chunk,
                        CASE
                            WHEN rid LIKE '%#chunk%' THEN
                                CAST(SUBSTRING(rid FROM '#chunk([0-9]+)') AS INTEGER)
                            ELSE -1
                        END as chunk_index
                    FROM koi_memories
                    WHERE (rid = $1 OR rid LIKE $2){privacy_clause}
                    ORDER BY chunk_index ASC
                    LIMIT $3
                """
                params = [base_rid, f"{base_rid}#chunk%", limit]
            else:
                # Get only exact RID match
                query = f"""
                    SELECT
                        rid,
                        content->>'title' as title,
                        content->>'text' as text,
                        source_sensor,
                        created_at,
                        metadata->>'url' as url,
                        is_chunk,
                        -1 as chunk_index
                    FROM koi_memories
                    WHERE rid = $1{privacy_clause}
                    LIMIT $2
                """
                params = [rid, limit]

            docs = await conn.fetch(query, *params)

            results = []
            for doc in docs:
                results.append({
                    "rid": doc['rid'],
                    "title": doc['title'],
                    "content": doc['text'],
                    "source": doc['source_sensor'],
                    "url": doc['url'],
                    "indexed_at": doc['created_at'].isoformat() if doc['created_at'] else None,
                    "is_chunk": doc['is_chunk'],
                    "chunk_index": doc['chunk_index'] if doc['chunk_index'] >= 0 else None
                })

            return {
                "rid": rid,
                "base_rid": base_rid,
                "found": len(results) > 0,
                "count": len(results),
                "documents": results
            }

        finally:
            await conn.close()

    except Exception as e:
        logger.error(f"Error looking up RID {rid}: {e}")
        return {
            "rid": rid,
            "found": False,
            "error": str(e),
            "documents": []
        }

@app.get("/api/koi/cat/chain/{rid:path}")
async def get_cat_receipt_chain(rid: str):
    """Get complete CAT receipt chain for a RID using the CATReceiptChain class"""

    from urllib.parse import unquote
    import sys
    import os

    # Add parent directory to path to import CAT module
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'cat'))

    try:
        from cat_receipt_chain import CATReceiptChain
    except ImportError:
        logger.error("Could not import CATReceiptChain")
        return {
            "rid": rid,
            "error": "CAT Receipt Chain module not available",
            "found": False
        }

    # Decode the RID if it was URL encoded
    rid = unquote(rid)

    try:
        # Initialize CAT receipt chain
        chain = CATReceiptChain()
        await chain.initialize()

        # Get provenance report
        report = await chain.get_provenance_report(rid)

        # Verify the chain
        verification = await chain.verify_chain(rid)

        # Get the complete chain
        receipts = await chain.get_chain(rid)

        # Format the response
        formatted_chain = []
        for receipt in receipts:
            formatted_chain.append({
                "rid": receipt.rid,
                "type": receipt.type,
                "timestamp": receipt.timestamp,
                "parent_rid": receipt.parent_rid,
                "content_cid": receipt.content_cid,
                "transformation": receipt.transformation,
                "metadata": receipt.metadata,
                "hash": receipt.hash,
                "hash_valid": True  # Will be updated from verification
            })

        # Add verification status to each receipt
        for i, receipt_info in enumerate(formatted_chain):
            if i < len(verification.get("receipts", [])):
                receipt_info["hash_valid"] = verification["receipts"][i].get("hash_valid", True)
                receipt_info["parent_valid"] = verification["receipts"][i].get("parent_valid", True)

        return {
            "rid": rid,
            "found": len(receipts) > 0,
            "chain": {
                "length": len(receipts),
                "valid": verification.get("valid", False),
                "receipts": formatted_chain,
                "errors": verification.get("errors", [])
            },
            "report": report,
            "verification": verification
        }

    except Exception as e:
        logger.error(f"Error fetching CAT chain for {rid}: {e}")
        return {
            "rid": rid,
            "error": str(e),
            "found": False
        }

@app.get("/api/koi/graph/status")
async def get_pipeline_status():
    """Get current status of all pipeline components"""

    graph = load_pipeline_metadata()
    components = extract_components(graph)

    # Check status of each component
    status_list = []

    for component in components:
        status = ComponentStatus(
            id=component.id,
            status=component.status,
            last_updated=datetime.utcnow().isoformat()
        )

        # Add specific metrics based on component type
        if component.type == "sensor":
            sensor_data = await get_live_sensor_status()
            sensor_info = next(
                (s for s in sensor_data.get("sensors", []) if s["id"] == component.id),
                None
            )
            if sensor_info:
                status.metrics = {
                    "events_processed": sensor_info.get("eventsProcessed", 0),
                    "last_activity": sensor_info.get("lastActivity")
                }

        status_list.append(status)

    return {
        "components": status_list,
        "overall_status": "operational",
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)