# KOI Pipeline API Documentation

> ⚠️ **SCOPE:** This documents the **RegenAI
> public-production event-bridge API** (branch `stable`, `eliza` DB). Its BGE-1024 embedding
> (`BAAI/bge-large-en-v1.5`) IS live there via `src/core/bge_server.py`. **personal-KOI is a
> different surface**: OpenAI `text-embedding-3-large` (**3072-dim**, since 2026-04-23),
> served from `api/personal_ingest_api.py` (port 8351). Don't apply the BGE examples below to
> personal-KOI.

## Overview
The KOI Pipeline provides several REST APIs for content processing, knowledge search, and system monitoring.

## Base URLs

- **KOI Coordinator**: `http://localhost:8005`
- **Event Bridge**: `http://localhost:8100`
- **BGE Server**: `http://localhost:8090`
- **MCP Knowledge Server**: `http://localhost:8200`

---

## KOI Coordinator API (Port 8005)

### POST /api/event
Submit a new event to the KOI pipeline.

**Request:**
```json
{
  "source_sensor": "sensor.type.hash",
  "content": {
    "text": "Content to process",
    "metadata": {}
  },
  "metadata": {
    "type": "content_type",
    "source": "source_name"
  }
}
```

**Response:**
```json
{
  "success": true,
  "rid": "sensor.type.hash.timestamp",
  "message": "Event forwarded to Event Bridge"
}
```

### GET /health
Check coordinator health status.

**Response:**
```json
{
  "service": "KOI Coordinator (Fixed)",
  "endpoints": ["/api/event", "/health"]
}
```

---

## Event Bridge API (Port 8100)

### GET /
Health check and service information.

**Response:**
```json
{
  "service": "KOI Event Bridge v2",
  "status": "operational",
  "version": "2.0.0",
  "features": [
    "RID-based deduplication",
    "Version control for updates",
    "Isolated KOI tables",
    "BGE embedding generation"
  ],
  "isolated_tables": true
}
```

### POST /process-koi-event
Process a KOI event (internal use).

**Request:**
```json
{
  "event_type": "NEW|UPDATE|FORGET",
  "source_sensor": "sensor_id",
  "timestamp": "ISO-8601 timestamp",
  "bundle": {
    "rid": "resource_id",
    "cid": "content_id",
    "content": {},
    "metadata": {},
    "manifest": {}
  }
}
```

**Response:**
```json
{
  "success": true,
  "rid": "resource_id",
  "cid": "content_id",
  "chunks_created": 5,
  "embeddings_created": 5,
  "version": 1,
  "previous_version_id": null
}
```

### GET /stats
Get pipeline statistics.

**Response:**
```json
{
  "unique_documents": 25,
  "total_versions": 25,
  "new_events": 25,
  "update_events": 0,
  "active_sensors": 18,
  "latest_event": "2025-09-12T21:52:10.889311+00:00",
  "embeddings": {
    "bge": 25,
    "gemma": 0
  }
}
```

---

## BGE Server API (Port 8090)

### POST /encode
Generate BGE embeddings for text.

**Request:**
```json
{
  "text": "Text to encode",
  "input": "Alternative field for text"
}
```

**Response:**
```json
{
  "embedding": [0.123, -0.456, ...],  // 1024-dimensional vector
  "dim": 1024,
  "model": "BAAI/bge-large-en-v1.5"
}
```

### GET /health
Check BGE server health.

**Response:**
```json
{
  "status": "healthy",
  "model": "BAAI/bge-large-en-v1.5",
  "device": "cuda|cpu|mps"
}
```

---

## MCP Knowledge Server API (Port 8200)

### GET /
Service information and statistics.

**Response:**
```json
{
  "service": "KOI Knowledge MCP Server",
  "status": "operational",
  "version": "1.0.0",
  "koi_memories": 24,
  "features": [
    "KOI memory search",
    "BGE embedding similarity",
    "Agent-specific filtering",
    "Real-time knowledge access"
  ]
}
```

### POST /search
Search KOI knowledge base.

**Request:**
```json
{
  "query": "search query",
  "agent_id": "optional_agent_uuid",
  "limit": 10,
  "similarity_threshold": 0.7
}
```

**Response:**
```json
{
  "success": true,
  "memories": [
    {
      "rid": "resource_id",
      "cid": "content_id",
      "content": {},
      "metadata": {},
      "created_at": "2025-09-12T21:29:56.168510+00:00",
      "source_sensor": "sensor_name",
      "version": 1,
      "similarity": 0.8
    }
  ],
  "count": 10,
  "query_embedding_generated": true
}
```

### GET /memory/{rid}
Get specific memory by Resource ID.

**Response:**
```json
{
  "rid": "resource_id",
  "cid": "content_id",
  "content": {},
  "metadata": {},
  "created_at": "2025-09-12T21:29:56.168510+00:00",
  "version": 1
}
```

### GET /stats
Get knowledge base statistics.

**Response:**
```json
{
  "total_memories": 24,
  "unique_sensors": 5,
  "memories_with_embeddings": 24,
  "oldest_memory": "2025-09-12T21:28:11.226907",
  "newest_memory": "2025-09-12T23:29:56.168510",
  "top_sensors": [
    {"sensor": "content.pusher", "count": 10},
    {"sensor": "web_content", "count": 5}
  ]
}
```

---

## Authentication

Currently, all APIs are open without authentication. For production deployment, consider:

1. **API Keys**: Add `X-API-Key` header validation
2. **JWT Tokens**: Implement OAuth2/JWT for agent authentication
3. **Rate Limiting**: Prevent abuse with rate limits per IP/key
4. **HTTPS**: Use SSL/TLS for all production endpoints

---

## Error Responses

All APIs return consistent error responses:

```json
{
  "error": "Error message",
  "detail": "Detailed error information",
  "status_code": 400
}
```

Common HTTP status codes:
- `200`: Success
- `400`: Bad Request
- `404`: Not Found
- `500`: Internal Server Error
- `503`: Service Unavailable

---

## Rate Limits

Default rate limits (configurable):
- Coordinator: 100 requests/minute
- Event Bridge: 50 events/minute
- BGE Server: 30 embeddings/minute
- MCP Server: 100 searches/minute

---

## Monitoring Endpoints

All services support health checks at:
- `GET /health` or `GET /`

For production monitoring, use:
```bash
curl -f http://localhost:8005/health || alert "Coordinator down"
curl -f http://localhost:8100/ || alert "Event Bridge down"
curl -f http://localhost:8090/health || alert "BGE Server down"
curl -f http://localhost:8200/ || alert "MCP Server down"
```