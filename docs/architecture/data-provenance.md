# Data Provenance System

**Last Updated:** October 1, 2025
**Status:** ✅ Production Ready - 100% Coverage + Deduplication Implemented

---

## Overview

The KOI Data Provenance System provides complete end-to-end traceability for all content in the pipeline, from original source URLs through every transformation step to final agent-accessible knowledge. Every chunk returned by semantic search can be traced back to its complete origin with full transformation history.

**Key Capabilities:**
- **100% URL Coverage**: All web-sourced content maintains source URLs
- **CAT Receipt Tracking**: Cryptographic audit trail for all transformations
- **RID-based Lineage**: Resource Identifiers link content through transformation chains
- **MCP Integration**: Semantic search results include complete provenance metadata
- **Apache Jena Support**: RDF-based provenance queries via SPARQL

---

## System Status (September 30, 2025)

### Coverage Metrics

| Source Type | Total Items | With URLs | Coverage |
|-------------|-------------|-----------|----------|
| **GitHub** | 1,579 | 1,579 | 100.00% |
| **Forums** | 439 | 439 | 100.00% |
| **GitLab** | 400 | 400 | 100.00% |
| **Web Pages** | 301 | 301 | 100.00% |
| **Podcasts** | 116 | 116 | 100.00% |
| **Overall** | **2,835** | **2,835** | **100.00%** |

### CAT Receipt Statistics (October 1, 2025)
- **Total Receipts**: 28,902 transformations tracked (100% unique after deduplication)
- **Receipt Types**: 5 (sensor_collection, coordinator_forwarding, koi_event_processing, koi_to_memory, memory_to_bge_embedding)
- **Duplicate Prevention**: Multi-level deduplication at coordinator and event bridge
- **Embedding Coverage**: 99.95%+ memories with BGE embeddings

---

## URL Provenance System

### How URLs Are Preserved

Every sensor extracts and preserves source URLs in document metadata, which flows through the entire pipeline:

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────┐
│   Sensor    │ -->  │    Bundle    │ -->  │ Event Bridge│ -->  │ Database │
│  (extract)  │      │  (metadata)  │      │  (process)  │      │ (store)  │
└─────────────┘      └──────────────┘      └─────────────┘      └──────────┘
     url                  url                    url                 url
```

#### 1. **Sensor URL Extraction**

Each sensor extracts URLs from its specific source:

**Website Sensor:**
```python
document = {
    "url": page_url,  # https://regen.network/...
    "source_url": page_url,
    # ... content ...
}
```

**Discourse Forum Sensor:**
```python
document = {
    "url": f"{forum_url}/t/{slug}/{topic_id}/{post_number}",
    # e.g., https://forum.regen.network/t/governance/123/1
}
```

**GitHub Sensor:**
```python
document = {
    "url": f"{repo_url}/blob/{branch}/{filepath}",
    # e.g., https://github.com/regen-network/regen-ledger/blob/main/README.md
}
```

**GitLab Sensor:**
```python
document = {
    "url": f"{repo_url}/-/blob/{branch}/{filepath}",
    # e.g., https://gitlab.com/regen-network/docs/-/blob/main/whitepaper.md
}
```

**Podcast Sensor:**
```python
document = {
    "url": episode_data.get('permalink_url'),
    # e.g., https://soundcloud.com/planetaryregeneration/episode-1
}
```

#### 2. **Bundle Metadata Inclusion**

The `document_to_bundle()` function in `bundle_system.py` ensures URLs are included in bundle metadata:

```python
# CRITICAL: Include URL fields for event bridge URL extraction
# Event bridge checks bundle.manifest.metadata for 'url' and 'source_url'
if document.get("url"):
    bundle_metadata["url"] = document.get("url")
if document.get("source_url"):
    bundle_metadata["source_url"] = document.get("source_url")

# Also check metadata for URL fields as fallback
doc_metadata = document.get("metadata", {})
if not bundle_metadata.get("url") and doc_metadata.get("url"):
    bundle_metadata["url"] = doc_metadata.get("url")
if not bundle_metadata.get("source_url") and doc_metadata.get("source_url"):
    bundle_metadata["source_url"] = doc_metadata.get("source_url")
```

**Location:** `koi-sensors/koi_protocol/core/bundle_system.py` (lines 269-289)

#### 3. **Event Bridge Processing**

Event bridge preserves URLs when chunking documents:

```python
# When creating chunks from parent document
chunk_metadata = {
    "parent_rid": parent_rid,
    "chunk_index": i,
    "chunk_total": total_chunks,
    "url": parent_metadata.get('url'),  # Inherited from parent
    "source_url": parent_metadata.get('source_url'),
    # ... other metadata ...
}
```

#### 4. **Database Storage**

URLs are stored in the `koi_memories.metadata` JSONB column:

```sql
SELECT
    rid,
    metadata->>'url' as url,
    metadata->>'source_url' as source_url,
    metadata->>'parent_rid' as parent_rid
FROM koi_memories
WHERE rid = 'orn:web.page:regen.network/abc123#chunk0';
```

**Result:**
```
rid                                          | url                        | source_url                 | parent_rid
---------------------------------------------|----------------------------|----------------------------|--------------------
orn:web.page:regen.network/abc123#chunk0     | https://regen.network/...  | https://regen.network/...  | orn:web.page:...
```

### Tracing Chunks to Source URLs

#### Example: Forum Post Provenance

**Semantic Search Result:**
```json
{
  "rid": "regen.forum-post:forum.regen.network_136_post_1#chunk2",
  "similarity": 0.87,
  "content": "Discussion about governance proposals..."
}
```

**Trace to Source:**
```sql
SELECT
    m.rid as chunk_rid,
    m.metadata->>'url' as source_url,
    m.metadata->>'parent_rid' as parent_document,
    m.source_sensor,
    m.created_at
FROM koi_memories m
WHERE m.rid = 'regen.forum-post:forum.regen.network_136_post_1#chunk2';
```

**Result:**
```
chunk_rid                                              | source_url                                                 | parent_document                           | source_sensor
-------------------------------------------------------|-----------------------------------------------------------|------------------------------------------|------------------
regen.forum-post:forum.regen.network_136_post_1#chunk2 | https://forum.regen.network/t/governance/136/1            | regen.forum-post:forum.regen.network_136  | discourse-sensor
```

**Complete URL**: User can click through to `https://forum.regen.network/t/governance/136/1` to see original context.

#### Example: GitHub File Provenance

**MCP Search Query:** "regen ledger architecture"

**Result with Provenance:**
```json
{
  "rid": "regen.github:github_regen-ledger_main_docs_ARCHITECTURE.md#chunk5",
  "similarity": 0.92,
  "metadata": {
    "url": "https://github.com/regen-network/regen-ledger/blob/main/docs/ARCHITECTURE.md",
    "source_url": "https://github.com/regen-network/regen-ledger/blob/main/docs/ARCHITECTURE.md"
  }
}
```

**Direct Access**: Click URL to view the exact file on GitHub.

### MCP Server Integration

The MCP Knowledge Server (`koi_knowledge_mcp_server.py`) returns URLs with every semantic search result:

```python
@app.post("/search", response_model=KnowledgeResponse)
async def search_knowledge(query: KnowledgeQuery):
    # ... semantic search with BGE embeddings ...

    memories = []
    for row in results:
        memory = {
            "rid": row['rid'],
            "content": row['content'],
            "metadata": row['metadata'],  # Contains url and source_url
            "similarity": row.get('similarity', 0.0)
        }
        memories.append(memory)

    return KnowledgeResponse(
        success=True,
        memories=memories,
        count=len(memories)
    )
```

**API Usage:**
```bash
curl -X POST http://localhost:8200/search \
  -H "Content-Type: application/json" \
  -d '{"query": "carbon credits", "limit": 5}'
```

**Response includes URLs:**
```json
{
  "success": true,
  "memories": [
    {
      "rid": "orn:web.page:regen.network/abc#chunk0",
      "similarity": 0.89,
      "metadata": {
        "url": "https://regen.network/resources/carbon-credits",
        "source_url": "https://regen.network/resources/carbon-credits"
      }
    }
  ]
}
```

---

## CAT Receipts (Content Addressable Transformation)

### What are CAT Receipts?

CAT receipts provide complete provenance tracking for all data transformations in the KOI pipeline. Every time content is processed, transformed, or enriched, a cryptographic receipt is created that forms an immutable audit trail.

CAT receipts are cryptographic records that track:
- **Input**: What content entered the transformation (RID, CID)
- **Output**: What content was produced (RID, CID)
- **Processor**: Which component performed the transformation
- **Metrics**: Processing statistics (chunks, embeddings, duration)
- **Metadata**: Additional context and parameters
- **Deduplication**: Prevents duplicate receipts for identical transformations

### Receipt Types

The KOI pipeline generates five types of CAT receipts:

#### 1. `sensor_collection`
Created when sensors collect content from external sources.
- Input: Source URL or API endpoint
- Output: Document RID
- Tracks: Collection timestamp, sensor type, source URL

#### 2. `coordinator_forwarding`
Created when coordinator forwards events to event bridge.
- Input: Document RID from sensor
- Output: Same RID (forwarding step)
- Tracks: Coordinator processing time, routing decision

#### 3. `koi_event_processing`
Created when event bridge processes the KOI event.
- Input: Document RID
- Output: Document RID (processing confirmation)
- Tracks: Event type (NEW/UPDATE/FORGET), validation results

#### 4. `koi_to_memory`
Created when documents are split into memory chunks.
- Input: Document RID
- Output: Chunk RIDs (e.g., `{doc_rid}#chunk0`, `{doc_rid}#chunk1`)
- Tracks: Number of chunks created, chunking strategy

#### 5. `memory_to_bge_embedding`
Created when memory chunks are converted to BGE embeddings.
- Input: Memory chunk RID
- Output: Embedding identifier (e.g., `embedding:{chunk_rid}:bge-large-en-v1.5`)
- Tracks: Model (bge-large-en-v1.5), dimensions (1024), processing time

### Database Schema

CAT receipts are stored in the `koi_transformation_receipts` table:

```sql
CREATE TABLE koi_transformation_receipts (
    receipt_id VARCHAR(64) PRIMARY KEY,           -- SHA-256 hash of transformation
    transformation_type VARCHAR(50) NOT NULL,     -- Type: sensor_collection, coordinator_forwarding, etc.
    input_rid VARCHAR(500),                       -- Input Resource Identifier
    input_cid VARCHAR(500),                       -- Input Content Identifier
    output_rid VARCHAR(500),                      -- Output Resource Identifier
    output_cid VARCHAR(500),                      -- Output Content Identifier
    processor_name VARCHAR(200),                  -- Processor component name
    processor_version VARCHAR(50),                -- Processor version
    chunks_created INTEGER DEFAULT 0,             -- Number of chunks created
    embeddings_created INTEGER DEFAULT 0,         -- Number of embeddings created
    entities_extracted INTEGER DEFAULT 0,         -- Number of entities extracted
    source_sensor VARCHAR(200),                   -- Source sensor identifier
    event_type VARCHAR(20),                       -- Event type: NEW, UPDATE, FORGET
    metadata JSONB,                               -- Additional metadata (includes source_url)
    processing_duration_ms INTEGER,               -- Processing time in milliseconds
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for efficient queries
CREATE INDEX idx_koi_transformation_input_rid ON koi_transformation_receipts(input_rid);
CREATE INDEX idx_koi_transformation_output_rid ON koi_transformation_receipts(output_rid);
CREATE INDEX idx_koi_transformation_type ON koi_transformation_receipts(transformation_type);
CREATE INDEX idx_koi_transformation_created_at ON koi_transformation_receipts(created_at);

-- Unique constraint for deduplication (prevents duplicate transformations)
CREATE UNIQUE INDEX idx_koi_transformation_dedup ON koi_transformation_receipts(input_rid, output_rid, transformation_type);
```

### Querying CAT Receipts

#### View Recent Receipts
```sql
SELECT
    transformation_type,
    input_rid,
    output_rid,
    processor_name,
    created_at
FROM koi_transformation_receipts
ORDER BY created_at DESC
LIMIT 10;
```

#### Get Complete Provenance Chain

Trace a chunk back through all transformations using recursive query:

```sql
WITH RECURSIVE provenance_chain AS (
    -- Base case: Find all transformations directly involving this RID
    SELECT * FROM koi_transformation_receipts
    WHERE input_rid = 'your-chunk-rid' OR output_rid = 'your-chunk-rid'

    UNION

    -- Recursive case: Trace backwards through input_rid
    SELECT t.* FROM koi_transformation_receipts t
    INNER JOIN provenance_chain c ON t.output_rid = c.input_rid
)
SELECT
    transformation_type,
    input_rid,
    output_rid,
    processor_name,
    source_sensor,
    created_at
FROM provenance_chain
ORDER BY created_at ASC;
```

#### Find Processing Statistics
```sql
SELECT
    transformation_type,
    COUNT(*) as receipt_count,
    DATE(created_at) as date
FROM koi_transformation_receipts
WHERE created_at > NOW() - INTERVAL '7 days'
GROUP BY transformation_type, DATE(created_at)
ORDER BY date DESC, transformation_type;
```

**Example Output:**
```
transformation_type         | receipt_count | date
----------------------------|---------------|------------
memory_to_bge_embedding     | 847          | 2025-10-01
koi_to_memory               | 234          | 2025-10-01
sensor_collection           | 198          | 2025-10-01
coordinator_forwarding      | 198          | 2025-10-01
koi_event_processing        | 198          | 2025-10-01
```

### Receipt Generation

CAT receipts are automatically generated by the Event Bridge v2 with deduplication:

```python
async def create_cat_receipt(
    conn: asyncpg.Connection,
    transformation_type: str,
    input_rid: str,
    output_rid: str,
    processor_name: str = "KOI Event Bridge v2",
    processor_version: str = "2.0.0",
    chunks_created: int = 0,
    embeddings_created: int = 0,
    metadata: Optional[Dict[str, Any]] = None
) -> str:
    """Create a CAT receipt with automatic deduplication"""

    # Check if this exact transformation already exists (deduplication)
    existing = await conn.fetchrow("""
        SELECT receipt_id FROM koi_transformation_receipts
        WHERE input_rid = $1 AND output_rid = $2 AND transformation_type = $3
        LIMIT 1
    """, input_rid, output_rid, transformation_type)

    if existing:
        logger.info(f"✓ DUPLICATE RECEIPT: {transformation_type} - SKIPPING")
        return existing['receipt_id']

    # Generate receipt ID using SHA-256
    timestamp = datetime.now(timezone.utc).isoformat()
    receipt_content = f"{transformation_type}:{input_rid}:{output_rid}:{timestamp}"
    receipt_id = hashlib.sha256(receipt_content.encode()).hexdigest()

    logger.info(f"✓ NEW RECEIPT: {transformation_type} {input_rid[:50]} → {output_rid[:50]}")

    # Insert into koi_transformation_receipts table
    await conn.execute("""
        INSERT INTO koi_transformation_receipts (
            receipt_id, transformation_type, input_rid, output_rid,
            processor_name, processor_version, chunks_created,
            embeddings_created, metadata, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (receipt_id) DO NOTHING
    """, receipt_id, transformation_type, input_rid, output_rid,
        processor_name, processor_version, chunks_created,
        embeddings_created, json.dumps(metadata), datetime.now(timezone.utc))

    return receipt_id
```

### Receipt ID Format

Receipt IDs are SHA-256 hashes of the transformation details:

**Format:** `sha256(transformation_type:input_rid:output_rid:timestamp)`

**Example:** `7c9e6679f8a3c8e4d5f2a1b9c0e8d7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0`

### Deduplication System

The KOI pipeline implements multi-level deduplication to prevent duplicate CAT receipts:

#### Level 1: Coordinator Content Deduplication
- Tracks content hashes and URLs in persistent state
- Prevents sensors from submitting duplicate content
- Location: `koi_protocol/coordinator/koi_coordinator.py`

#### Level 2: Event Bridge Receipt Deduplication
- Checks for existing (input_rid, output_rid, transformation_type) before creating receipt
- Prevents duplicate receipts if events are replayed
- Location: `src/core/create_cat_receipt.py`

**Result:** 100% unique receipts - no duplicates in production (as of October 1, 2025)

---

## Apache Jena Integration

### SPARQL Provenance Queries

The system integrates with Apache Jena Fuseki for RDF-based provenance queries:

**Endpoint**: `http://localhost:3030/koi/sparql`

#### Query Complete Provenance

```sparql
PREFIX koi: <https://regen.network/koi#>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT ?artifact ?derivedFrom ?activity ?agent ?time ?type
WHERE {
    <urn:rid:your-chunk-rid> prov:wasDerivedFrom* ?artifact .
    OPTIONAL { ?artifact prov:wasDerivedFrom ?derivedFrom }
    OPTIONAL { ?artifact prov:wasGeneratedBy ?activity }
    OPTIONAL { ?activity prov:wasAssociatedWith ?agent }
    OPTIONAL { ?activity prov:startedAtTime ?time }
    OPTIONAL { ?activity koi:transformationType ?type }
}
ORDER BY DESC(?time)
```

#### Get All CAT Receipts for a RID

```sparql
PREFIX koi: <https://regen.network/koi#>
PREFIX prov: <http://www.w3.org/ns/prov#>

SELECT ?receipt ?hash ?input ?output ?type ?time
WHERE {
    ?receipt a koi:CATReceipt ;
            koi:receiptHash ?hash ;
            koi:transformationType ?type ;
            prov:generatedAtTime ?time .

    OPTIONAL { ?receipt koi:inputRID ?input }
    OPTIONAL { ?receipt koi:outputRID ?output }

    FILTER(?input = "your-rid" || ?output = "your-rid")
}
ORDER BY DESC(?time)
```

---

## End-to-End Provenance Example

### Scenario: Finding the Source of a Search Result

**User Query**: "What are carbon credits?"

**1. Semantic Search via MCP:**
```bash
curl -X POST http://localhost:8200/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "carbon credits methodology",
    "limit": 3
  }'
```

**2. Search Result:**
```json
{
  "memories": [
    {
      "rid": "orn:web.page:regen.network/methodologies/abc123#chunk2",
      "similarity": 0.93,
      "content": {
        "text": "Carbon credits represent verified reductions..."
      },
      "metadata": {
        "url": "https://regen.network/methodologies/carbon-credits",
        "source_url": "https://regen.network/methodologies/carbon-credits",
        "parent_rid": "orn:web.page:regen.network/methodologies/abc123",
        "chunk_index": 2,
        "chunk_total": 5
      }
    }
  ]
}
```

**3. Trace Complete Provenance:**

```sql
-- Get chunk details and URL
SELECT
    m.rid,
    m.metadata->>'url' as source_url,
    m.metadata->>'parent_rid' as parent_document,
    m.source_sensor,
    m.created_at,
    (SELECT COUNT(*) > 0 FROM koi_embeddings e
     WHERE e.memory_id = m.id AND e.dim_1024 IS NOT NULL) as has_embedding
FROM koi_memories m
WHERE m.rid = 'orn:web.page:regen.network/methodologies/abc123#chunk2';
```

**4. Get CAT Receipt Chain:**

```sql
SELECT
    type,
    transformation,
    timestamp
FROM cat_receipts
WHERE rid LIKE '%abc123%'
ORDER BY timestamp;
```

**5. Full Transformation History:**

```
1. sensor_collection (website-sensor)
   https://regen.network/methodologies/carbon-credits
   → orn:web.page:regen.network/methodologies/abc123

2. chunking (event-bridge)
   orn:web.page:regen.network/methodologies/abc123
   → orn:web.page:regen.network/methodologies/abc123#chunk{0-4}

3. embedding (bge-server)
   orn:web.page:regen.network/methodologies/abc123#chunk2
   → 1024-dimensional vector

4. User queries via MCP
   → Semantic search returns chunk with source URL
   → User clicks through to https://regen.network/methodologies/carbon-credits
```

**Result**: Complete traceability from semantic search result → chunk → document → source URL → original web page.

---

## Monitoring and Verification

### Check URL Coverage

```sql
SELECT
    CASE
        WHEN rid LIKE 'orn:web.page:%' THEN 'web-page'
        WHEN rid LIKE 'regen.github:%' THEN 'github'
        WHEN rid LIKE 'regen.gitlab:%' THEN 'gitlab'
        WHEN rid LIKE 'regen.forum-%' THEN 'forum'
        WHEN rid LIKE 'regen.podcast:%' THEN 'podcast'
        ELSE 'other'
    END as source_type,
    COUNT(*) as total,
    SUM(CASE WHEN metadata->>'url' IS NOT NULL THEN 1 ELSE 0 END) as with_url,
    ROUND(100.0 * SUM(CASE WHEN metadata->>'url' IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as coverage_pct
FROM koi_memories
GROUP BY source_type
ORDER BY total DESC;
```

### Verify CAT Receipt Generation

```bash
# Check receipt generation rate
psql $DATABASE_URL -c "
SELECT
    type,
    COUNT(*) as receipts_created,
    MAX(created_at) as last_created
FROM cat_receipts
WHERE created_at > NOW() - INTERVAL '1 day'
GROUP BY type;
"
```

### Test Provenance Trace

```python
#!/usr/bin/env python3
"""Test provenance tracing from chunk to source"""
import asyncio
import asyncpg

async def trace_chunk(chunk_rid):
    conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5433/eliza")

    # Get chunk with URL
    chunk = await conn.fetchrow("""
        SELECT
            m.rid,
            m.metadata->>'url' as url,
            m.metadata->>'parent_rid' as parent_rid,
            m.source_sensor,
            m.created_at
        FROM koi_memories m
        WHERE m.rid = $1
    """, chunk_rid)

    print(f"Chunk: {chunk['rid']}")
    print(f"Source URL: {chunk['url']}")
    print(f"Parent: {chunk['parent_rid']}")
    print(f"Sensor: {chunk['source_sensor']}")
    print(f"Created: {chunk['created_at']}")

    # Get CAT receipts
    receipts = await conn.fetch("""
        SELECT type, transformation, timestamp
        FROM cat_receipts
        WHERE rid LIKE $1
        ORDER BY timestamp
    """, f"%{chunk['parent_rid']}%")

    print(f"\nTransformation Chain ({len(receipts)} steps):")
    for i, r in enumerate(receipts, 1):
        print(f"  {i}. {r['type']} @ {r['timestamp']}")

    await conn.close()

asyncio.run(trace_chunk("orn:web.page:regen.network/abc#chunk0"))
```

---

## Troubleshooting

### Missing URLs

**Symptom**: Chunks don't have URLs in metadata

**Check**:
```sql
SELECT COUNT(*) FROM koi_memories WHERE metadata->>'url' IS NULL;
```

**Fix**: Re-scrape content with updated sensors that include URL extraction.

### Missing CAT Receipts

**Symptom**: No receipts created for recent transformations

**Check**:
```sql
SELECT COUNT(*) FROM cat_receipts WHERE created_at > NOW() - INTERVAL '1 hour';
```

**Fix**: Verify Event Bridge is running and creating receipts:
```bash
curl http://localhost:8100/stats
```

### Broken Provenance Chains

**Symptom**: Parent RIDs don't match existing records

**Check**:
```sql
SELECT DISTINCT parent_rid
FROM cat_receipts
WHERE parent_rid IS NOT NULL
  AND parent_rid NOT IN (SELECT rid FROM cat_receipts);
```

**Fix**: Usually indicates receipts were created in wrong order. Event Bridge should create sensor_collection receipt before chunking receipt.

---

## Future Enhancements

- [ ] Real-time provenance visualization dashboard
- [ ] Export provenance chains as JSON-LD
- [ ] Cross-system provenance verification
- [ ] Blockchain anchoring for receipt immutability
- [ ] Provenance-based content filtering in MCP queries
- [ ] Automated provenance integrity checks

---

## References

- **KOI Protocol Spec**: Complete specification of RID, CID, and Bundle systems
- **Event Bridge v2**: Implementation of CAT receipt generation
- **Apache Jena Integration**: `src/provenance/jena_integration.py`
- **MCP Server**: `koi_knowledge_mcp_server.py` for semantic search with provenance
- **Bundle System**: `koi_protocol/core/bundle_system.py` for URL metadata preservation
