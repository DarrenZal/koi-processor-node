# KOI Storage Architecture

## Overview

The KOI pipeline uses a **dual-table storage pattern** to optimize for both data integrity and agent accessibility. This document explains how data flows through the system and where it's stored.

## Storage Tables

### 1. `koi_memories` Table
**Purpose**: Source document tracking and versioning

- Stores **original documents** with their RIDs (Resource Identifiers)
- Maintains version history and deduplication
- Tracks source sensors and metadata
- **Current count**: ~26 documents (as of Sept 2025)

**What goes here**:
- One entry per unique document/RID
- Full content as received from sensors
- Version tracking for updates

### 2. `memories` Table  
**Purpose**: Agent-accessible chunked content

- Stores **chunked documents** for RAG (Retrieval Augmented Generation)
- Each document may produce multiple chunks
- Optimized for agent queries and semantic search
- **Current count**: 40,000+ chunks (as of Sept 2025)

**What goes here**:
- Multiple chunks per document
- Each chunk is a searchable unit
- Linked to agent IDs for access control

### 3. `koi_embeddings` Table
**Purpose**: Vector embeddings for semantic search

- Stores BGE embeddings (1024-dimensional vectors)
- Links to `koi_memories` via `memory_id`
- Enables similarity search via pgvector
- Supports multiple embedding dimensions (768, 1024, 1536)

## Data Flow

```
┌──────────────┐      ┌──────────────────┐      ┌─────────────────────────┐
│   Sensors    │─────▶│  Event Bridge    │─────▶│  Dual Storage Pattern   │
│              │      │                  │      │                         │
│ • Websites   │      │ • Deduplication  │      │ ┌───────────────────┐   │
│ • GitHub     │      │ • Chunking       │      │ │  koi_memories     │   │
│ • Medium     │      │ • Embedding      │      │ │  (26 docs)        │   │
│ • Telegram   │      │                  │      │ └───────────────────┘   │
└──────────────┘      └──────────────────┘      │                         │
                                                │ ┌───────────────────┐   │
                                                │ │  memories         │   │
                                                │ │  (40K+ chunks)    │   │
                                                │ └───────────────────┘   │
                                                │                         │
                                                │ ┌───────────────────┐   │
                                                │ │  koi_embeddings   │   │
                                                │ │  (26 embeddings)  │   │
                                                │ └───────────────────┘   │
                                                └─────────────────────────┘
```

## Processing Example

### Input: GitHub README.md (1 document)
```
Sensor collects: 1 README.md file
  ↓
Event Bridge processes:
  - Checks RID for deduplication
  - Chunks into 5 parts (based on size/paragraphs)
  - Generates embeddings for each chunk
  ↓
Storage:
  - koi_memories: 1 entry (full document with RID)
  - memories: 5 entries (one per chunk)
  - koi_embeddings: 1 entry (embedding for search)
```

## Why Dual Storage?

### Benefits:
1. **Data Integrity**: Original documents preserved in `koi_memories`
2. **Performance**: Chunked content in `memories` optimizes agent retrieval
3. **Deduplication**: RID-based tracking prevents duplicate processing
4. **Versioning**: Track document changes over time
5. **Flexibility**: Different storage strategies for different needs

### Trade-offs:
- More complex than single-table storage
- Requires coordination between tables
- Storage overhead (same content in multiple forms)

## Deduplication Process

The Event Bridge checks for existing RIDs before processing:

```python
# Simplified deduplication logic
existing = await check_rid_exists(rid)
if existing and not force_update:
    return {"chunks": 0, "embeddings": 0}  # Skip duplicate
```

This is why you often see "0 chunks, 0 embeddings" - the content already exists!

## Querying the Data

### For KOI source documents:
```sql
SELECT * FROM koi_memories WHERE source_sensor = 'github-sensor';
```

### For agent-accessible chunks:
```sql
SELECT * FROM memories 
WHERE content::text LIKE '%carbon credits%'
AND "createdAt" > NOW() - INTERVAL '24 hours';
```

### For similarity search (via MCP server):
```bash
curl -X POST http://localhost:8200/search \
  -H "Content-Type: application/json" \
  -d '{"query": "regenerative agriculture", "limit": 5}'
```

## Storage Statistics (Sept 2025)

| Table | Document Count | Purpose | Growth Rate |
|-------|---------------|---------|-------------|
| koi_memories | 26 | Source documents | Slow (deduplication) |
| memories | 40,479 | Agent chunks | Fast (1,500+/day) |
| koi_embeddings | 26 | Vector search | Matches koi_memories |

## Common Misconceptions

❌ **Wrong**: "Only 26 documents from KOI pipeline"
✅ **Right**: 26 source documents → 40,000+ searchable chunks

❌ **Wrong**: "Event Bridge isn't working (0 chunks)"  
✅ **Right**: Deduplication is preventing reprocessing

❌ **Wrong**: "Data isn't being stored"
✅ **Right**: Check both tables - chunks go to `memories`

## Monitoring Commands

```bash
# Check KOI pipeline stats
psql -d eliza -c "
  SELECT 
    (SELECT COUNT(*) FROM koi_memories) as koi_docs,
    (SELECT COUNT(*) FROM memories WHERE \"createdAt\" > NOW() - INTERVAL '24 hours') as recent_chunks,
    (SELECT COUNT(*) FROM koi_embeddings) as embeddings;"

# Check deduplication effectiveness  
tail -f logs/event_bridge_new.log | grep "Successfully processed"

# Verify MCP server access
curl http://localhost:8200/
```

## Future Improvements

1. **Unified view**: Create views joining both tables
2. **Better metrics**: Dashboard showing pipeline throughput
3. **Configurable chunking**: Adjust chunk size per document type
4. **Retention policies**: Auto-cleanup of old versions

---

*Last Updated: September 2025*
*Pipeline Version: KOI Event Bridge v2*