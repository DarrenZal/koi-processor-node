# KOI Processor Architecture

> ⚠️ **Embedding layer — two surfaces (verified 2026-07-16):** This doc's **BGE-1024
> embedding server** (`bge_server.py`, now at `src/core/`) describes **RegenAI public
> production** (branch `stable`, `eliza` DB) — confirmed live there. **personal-KOI** uses
> **OpenAI `text-embedding-3-large` (3072-dim)** since 2026-04-23 (no BGE server). Read the
> embedding sections below as the RegenAI-prod design; see
> [the personal-KOI quickstart](../operations/quickstart-personal-koi.md) for personal-KOI.

## System Overview

The KOI (Knowledge Organization Infrastructure) Processor is a distributed system for processing, embedding, and storing sensor-generated content with semantic search capabilities. It implements deduplication, versioning, and permission-based access control.

## Core Components

### 1. Event Bridge v2 (`src/core/koi_event_bridge_v2.py`)

The central processing hub that:
- Receives events from KOI sensors
- Implements RID-based deduplication
- Manages content versioning
- Chunks text for optimal embedding
- Stores data in PostgreSQL with pgvector

**Key Features:**
- Asynchronous processing with FastAPI
- Connection pooling for database efficiency
- Graceful error handling and recovery
- Support for both isolated and legacy tables

### 2. BGE Embedding Server (`bge_server.py`)

Generates semantic embeddings using BGE-large-en-v1.5:
- 1024-dimensional dense vectors
- REST API interface
- Mock mode for testing
- Batch processing support

**Architecture:**
```
Sensor Event → Event Bridge → BGE Server → Embeddings
                    ↓              ↓
                PostgreSQL ← Vector Storage
                    ↓
                MCP Server ←→ Eliza Agents
                    ↓
                Apache Jena Fuseki
                (SPARQL Triplestore)
```

### 3. MCP Server (`bge-mcp-ts/bge-server.ts`)

Provides Model Context Protocol interface for agents:
- Routes semantic queries to PostgreSQL pgvector
- Routes ontological/SPARQL queries to Apache Jena
- Implements `bge_search` and `bge_stats` tools
- TypeScript implementation for ElizaOS compatibility
- Stdio transport for agent communication

**Query Routing:**
```
Eliza Agent Query → MCP Server
                        ↓
            [Semantic?] → PostgreSQL
            [Ontological?] → Apache Jena
                        ↓
                  Unified Response
```

### 4. Apache Jena Fuseki

Semantic reasoning and ontological storage:
- SPARQL endpoint on port 3030
- RDF triplestore for knowledge graph
- OWL ontologies for inference
- Separate from embedding pipeline
- Used for complex semantic queries

**Data Sources:**
- Unified metabolic ontology (36 classes)
- Entity relationships and hierarchies
- Provenance tracking via CAT receipts
- Registry Framework integration

### 5. Database Schema

#### Isolated Tables (v2)
```sql
koi_memories
├── id (UUID, primary key)
├── rid (resource identifier)
├── version (integer)
├── previous_version_id (UUID reference)
├── content (JSONB)
├── created_at
└── superseded_at

koi_embeddings  
├── id (UUID)
├── memory_id (foreign key)
├── dim_768 (vector[768])
├── dim_1024 (vector[1024])
└── dim_1536 (vector[1536])
```

#### Legacy Tables (v1)
```sql
memories
├── id (UUID)
├── type (memory type)
├── content (JSONB with embedded RID)
└── created_at

embeddings
├── id (UUID)
├── memory_id (foreign key)
└── embedding vectors
```

## Data Flow

### 1. Event Ingestion
```
KOI Sensor → HTTP POST → /process-koi-event
    ↓
Event Validation (Pydantic)
    ↓
RID Extraction & Deduplication Check
    ↓
[NEW Event] → Process if RID not exists
[UPDATE Event] → Create new version
[DELETE Event] → Mark as superseded
```

### 2. Agent Query Flow
```
Eliza Agent → MCP Server (stdio)
    ↓
Query Analysis
    ↓
[Embedding Search] → PostgreSQL pgvector
[SPARQL Query] → Apache Jena Fuseki
[Hybrid Query] → Both systems
    ↓
Results Aggregation
    ↓
Response to Agent
```

### 3. Content Processing
```
Raw Content → Text Extraction
    ↓
Chunking (1000 chars, 200 overlap)
    ↓
Chunk RID Generation (parent#chunk_N)
    ↓
Parallel Embedding Generation
    ↓
Batch Database Insert
```

### 4. Embedding Pipeline
```
Text Chunk → BGE API Request
    ↓
1024-dim Vector Response
    ↓
Vector Normalization
    ↓
PostgreSQL pgvector Storage
    ↓
HNSW Index for Fast Search
```

## Deduplication Strategy

### RID-Based Deduplication
- Every piece of content has a unique Resource Identifier (RID)
- Format: `source.type.identifier.timestamp`
- Example: `sensor.document.abc123.20250909`

### Deduplication Flow
1. Extract RID from incoming event
2. Query database for existing RID
3. If exists and event_type == "NEW": Skip processing
4. If exists and event_type == "UPDATE": Create new version
5. If not exists: Process normally

### Version Control
- Each update creates a new version
- Previous version linked via `previous_version_id`
- Old version marked with `superseded_at` timestamp
- Complete audit trail maintained

## API Design

### REST Endpoints

#### POST /process-koi-event
Processes incoming KOI events with deduplication and versioning.

**Request:**
```json
{
  "event_type": "NEW|UPDATE|DELETE",
  "source_sensor": "sensor_id",
  "timestamp": "ISO-8601",
  "bundle": {
    "rid": "unique.resource.id",
    "content": {
      "text": "content to process"
    }
  }
}
```

**Response:**
```json
{
  "status": "processed",
  "rid": "unique.resource.id",
  "version": 2,
  "previous_version_id": "uuid",
  "chunks_created": 5,
  "embeddings_created": 5
}
```

#### GET /search
Semantic search across embedded content.

**Query Parameters:**
- `query`: Search text
- `limit`: Max results (default: 10)
- `agent_id`: Filter by agent permissions

### WebSocket Support (Planned)
- Real-time event streaming
- Live deduplication notifications
- Processing status updates

## Performance Optimizations

### 1. Database
- Connection pooling (min: 10, max: 50)
- Prepared statements for common queries
- HNSW indexes on vector columns
- Partitioning for time-series data

### 2. Embedding Generation
- Batch processing for multiple chunks
- Async/await for concurrent requests
- Caching for frequently accessed embeddings
- GPU acceleration (when available)

### 3. Deduplication
- Bloom filters for quick existence checks
- In-memory RID cache (LRU, 10000 entries)
- Database indexes on RID columns

## Security Considerations

### 1. Input Validation
- Pydantic models for all inputs
- SQL injection prevention via parameterized queries
- Content size limits (10MB max)
- Rate limiting per source sensor

### 2. Access Control
- Agent-based permissions
- Row-level security in PostgreSQL
- API key authentication (production)
- CORS configuration for web clients

### 3. Data Privacy
- No PII in embeddings
- Encrypted database connections
- Audit logging for all operations
- GDPR-compliant data retention

## Monitoring & Observability

### 1. Metrics
- Events processed per second
- Embedding generation latency
- Database query performance
- Deduplication hit rate

### 2. Logging
- Structured JSON logging
- Log levels: DEBUG, INFO, WARNING, ERROR
- Correlation IDs for request tracing
- Error aggregation with Sentry

### 3. Health Checks
- `/health` endpoint for liveness
- `/ready` endpoint for readiness
- Database connection validation
- BGE server availability check

## Scaling Architecture

### Horizontal Scaling
```
Load Balancer (nginx/HAProxy)
       ↓
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│Event Bridge │  │Event Bridge │  │Event Bridge │
│  Instance 1 │  │  Instance 2 │  │  Instance 3 │
└─────────────┘  └─────────────┘  └─────────────┘
       ↓                ↓                ↓
    PostgreSQL Primary → Read Replicas
              ↓
    ┌─────────────────────────┐
    │     MCP Server Pool     │
    │  (Load Balanced stdio)  │
    └─────────────────────────┘
         ↓              ↓
    PostgreSQL    Apache Jena
                  (Clustered)
         ↓              ↓
    ┌─────────────────────────┐
    │    Eliza Agent Fleet    │
    │  (5+ agents concurrent) │
    └─────────────────────────┘
```

### Vertical Scaling
- Increase worker processes
- Tune PostgreSQL shared_buffers
- GPU instances for BGE server
- SSD storage for vector indexes

## Development Workflow

### 1. Local Development
```bash
# Setup environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with local settings

# Run services (from the repository root; each script imports its siblings in src/core)
python src/core/bge_server.py &
python src/core/koi_event_bridge_v2.py &

# Test
pytest -rs tests/
```

### 2. Testing Strategy
- Unit tests for deduplication logic
- Integration tests for full pipeline
- Load tests for performance validation
- Mock BGE server for CI/CD

### 3. Deployment Pipeline
```
Git Push → GitHub Actions → Build → Test → Deploy
    ↓           ↓             ↓       ↓        ↓
Trigger    Lint/Format   Docker   E2E    Kubernetes
```

## Common Patterns

### 1. Event Sourcing
All changes captured as events with complete history.

### 2. CQRS (Command Query Responsibility Segregation)
- Write path: Event Bridge → PostgreSQL
- Read path: MCP Server → Vector Search

### 3. Saga Pattern
Multi-step processing with compensating transactions.

### 4. Circuit Breaker
Automatic fallback when BGE server unavailable.

## Troubleshooting Guide

### Common Issues

#### 1. Duplicate Content
**Symptom:** Same content appears multiple times
**Cause:** RID not properly extracted
**Solution:** Check RID generation logic in sensor

#### 2. Missing Embeddings
**Symptom:** Content stored but not searchable
**Cause:** BGE server timeout or error
**Solution:** Check BGE server logs, retry failed embeddings

#### 3. Slow Queries
**Symptom:** Search takes >1 second
**Cause:** Missing vector indexes
**Solution:** Run index creation migration

#### 4. Memory Leaks
**Symptom:** RAM usage grows over time
**Cause:** Connection pool exhaustion
**Solution:** Ensure proper connection cleanup

## Future Enhancements

### Short Term (Q1 2025)
- [ ] Batch event processing
- [ ] WebSocket support
- [ ] Prometheus metrics
- [ ] GraphQL API

### Medium Term (Q2 2025)
- [ ] Multi-model embeddings
- [ ] Distributed caching
- [ ] Event replay capability
- [ ] A/B testing framework

### Long Term (Q3-Q4 2025)
- [ ] Kubernetes operators
- [ ] Multi-region deployment
- [ ] Real-time analytics
- [ ] ML-based deduplication

## Contributing

### Code Style
- Black for Python formatting
- Type hints for all functions
- Docstrings in Google style
- 100% test coverage for new features

### Pull Request Process
1. Create feature branch
2. Write tests first (TDD)
3. Implement feature
4. Update documentation
5. Submit PR with description
6. Address review feedback
7. Merge after approval

## References

- [KOI-net node protocol](https://github.com/DynamicalSystemsGroup/koi-net)
- [BGE Model Paper](https://arxiv.org/abs/2309.07597)
- [pgvector Documentation](https://github.com/pgvector/pgvector)
- [FastAPI Best Practices](https://fastapi.tiangolo.com/tutorial/)
- [PostgreSQL Performance Tuning](https://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server)

---

For implementation details, see the [README](../../README.md). Deployment instructions are maintainer-only and not part of the public export.