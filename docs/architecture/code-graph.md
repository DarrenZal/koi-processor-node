# Code Graph Service

## Overview

The Code Graph Service is a standalone FastAPI service that processes GitHub sensor events to extract code entities (functions, classes, interfaces, etc.) and populate an Apache AGE graph database. It runs in parallel with the Event Bridge v2, providing specialized code analysis alongside document embeddings.

## Architecture

```
GitHub Sensor → Coordinator (port 8005)
                     │
                     ├─► Event Bridge v2 (port 8100) → pgvector (document embeddings)
                     │
                     └─► Code Graph Service (port 8350) → Apache AGE (code entities)
```

### Design Principles

1. **Separation of Concerns**: Code Graph Service runs independently from Event Bridge v2
2. **Parallel Processing**: Events are forwarded to both services simultaneously
3. **Non-blocking**: Failures in Code Graph don't affect Event Bridge operations
4. **Selective Processing**: Only processes source code files from configured repositories

## Components

### 1. Code Graph Processor (`src/core/code_graph_processor.py`)

Multi-language entity extraction engine supporting:

**Languages:**
- Python (`.py`)
- Go (`.go`)
- TypeScript/JavaScript (`.ts`, `.tsx`, `.js`, `.jsx`)
- Rust (`.rs`) - partial support

**Entity Types:**
- **Go**: `Keeper`, `Message`, `Event`, `Query`, `Function`, `Interface`, `Class`
- **Python**: `Function`, `Class`, `Sensor`, `Handler`, `Processor`, `API`, `Config`
- **TypeScript**: `Function`, `Class`, `Interface`, `Type`, `Sensor`, `Handler`

**Features:**
- Regex-based extraction (works without tree-sitter)
- Repository-aware extraction
- Automatic deduplication via MERGE
- Docstring/comment extraction

### 2. Code Graph Service (`src/core/code_graph_service.py`)

Standalone FastAPI service with endpoints:

**Endpoints:**
- `GET /` - Service status and configuration
- `GET /stats` - Processing statistics
- `GET /health` - Health check
- `POST /process-koi-event` - Main event processing endpoint
- `POST /test-extraction` - Test entity extraction

**Service Configuration:**
```env
CODE_GRAPH_ENABLED=true
CODE_GRAPH_PORT=8350
GRAPH_NAME=regen_graph
CODE_GRAPH_REPOS=regen-ledger,regen-web,regen-data-standards,koi-sensors,koi-processor,koi-research,regen-koi-mcp
```

### 3. Event Forwarder (`scripts/coordinator_to_eventbridge_forwarder.py`)

Updated to forward events to both services in parallel:

```python
# Send to both Event Bridge AND Code Graph Service
eb_task = client.post(f"{EVENT_BRIDGE_URL}/process-koi-event", json=event)
cg_task = client.post(f"{CODE_GRAPH_URL}/process-koi-event", json=event)

eb_response, cg_response = await asyncio.gather(eb_task, cg_task, return_exceptions=True)
```

## Database Schema

### Apache AGE Graph Structure

**Node Types:**
- `Repository` - GitHub repositories
- `Function` - Functions/methods
- `Class` - Classes/structs
- `Interface` - Interfaces
- `Type` - Type definitions
- `Keeper` - Go Keepers (Cosmos SDK)
- `Message` - Go Messages (Cosmos SDK)
- `Event` - Go Events (Cosmos SDK)
- `Query` - Go Query types
- `Sensor` - KOI Sensors
- `Handler` - Event handlers

**Node Properties:**
- `name` - Entity name
- `file_path` - Source file path (relative to repo)
- `line_number` - Line number in source
- `language` - Programming language
- `repo` - Repository name
- `docstring` - Documentation/comments
- `fields/methods/properties` - Entity members (JSON)

**Relationships:**
- `Repository -[:CONTAINS]-> Entity` - Repo contains entities

## Setup

### Prerequisites

```bash
# PostgreSQL with Apache AGE extension
sudo apt install postgresql-14 postgresql-14-age

# Python dependencies
pip install psycopg2-binary fastapi uvicorn python-dotenv
```

### Configuration

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Configure Code Graph settings:
```env
CODE_GRAPH_ENABLED=true
CODE_GRAPH_PORT=8350
GRAPH_NAME=regen_graph
CODE_GRAPH_REPOS=regen-ledger,regen-web,regen-data-standards,koi-sensors,koi-processor,koi-research,regen-koi-mcp
POSTGRES_URL=postgresql://postgres:postgres@localhost:5433/eliza
```

3. Ensure Apache AGE graph exists:
```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
SELECT create_graph('regen_graph');
```

### Running the Service

**Development:**
```bash
cd src/core
CODE_GRAPH_PORT=8350 python3 code_graph_service.py
```

**Production (with nohup):**
```bash
cd src/core
CODE_GRAPH_PORT=8350 nohup python3 code_graph_service.py >> code_graph_service.log 2>&1 &
```

**Check status:**
```bash
curl http://localhost:8350/
```

## Usage

### Processing Events

The service automatically processes events forwarded by the coordinator:

1. GitHub sensor detects code changes
2. Coordinator receives events
3. Forwarder sends events to both Event Bridge and Code Graph Service
4. Code Graph Service:
   - Filters for code files from configured repos
   - Extracts entities using language-specific parsers
   - Loads entities into Apache AGE graph
   - Returns processing results

### Testing Extraction

Test entity extraction manually:

```bash
curl -X POST "http://localhost:8350/test-extraction" \
  -G \
  --data-urlencode "source_code=class MyClass:
    def method(self):
        pass" \
  --data-urlencode "language=python" \
  --data-urlencode "file_path=test.py" \
  --data-urlencode "repo=test-repo"
```

### Monitoring

**Service Stats:**
```bash
curl http://localhost:8350/stats | jq
```

**Response:**
```json
{
  "events_received": 150,
  "events_processed": 45,
  "events_skipped": 105,
  "entities_extracted": 0,
  "entities_loaded": 892,
  "errors": 0,
  "by_repo": {
    "regen-ledger": {"events": 30, "entities": 450},
    "regen-web": {"events": 15, "entities": 442}
  },
  "by_language": {},
  "start_time": "2025-11-26T20:33:26.677235",
  "uptime_seconds": 1234.56,
  "events_per_minute": 7.3
}
```

**View Logs:**
```bash
tail -f src/core/code_graph_service.log
```

## Querying the Graph

### Via PostgreSQL

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Count entities by type
SELECT * FROM cypher('regen_graph', $$
    MATCH (n)
    RETURN labels(n)[0] as type, count(*) as count
    ORDER BY count DESC
$$) as (type agtype, count agtype);

-- Find all Keepers
SELECT * FROM cypher('regen_graph', $$
    MATCH (k:Keeper)
    RETURN k.name, k.file_path, k.line_number
$$) as (name agtype, file_path agtype, line_number agtype);

-- Find Messages handled by a Keeper
SELECT * FROM cypher('regen_graph', $$
    MATCH (k:Keeper {name: 'Keeper'})<-[:HANDLED_BY]-(m:Message)
    RETURN m.name, m.file_path
$$) as (name agtype, file_path agtype);
```

### Via MCP Server

The `regen-koi-mcp` server provides `query_code_graph` tool:

```python
# Query via MCP
query_code_graph(
    query_type="find_by_type",
    entity_type="Keeper",
    repo_name="regen-ledger"
)
```

### Via API

Direct HTTP access via koi-query-api:

```bash
curl -X POST http://localhost:3001/api/koi/graph \
  -H "Content-Type: application/json" \
  -d {query_type: list_repos}
```

## Troubleshooting

### Service Won't Start

**Check port availability:**
```bash
lsof -i :8350
```

**Check logs:**
```bash
tail -50 src/core/code_graph_service.log
```

**Common issues:**
- Port already in use: Change `CODE_GRAPH_PORT` in `.env`
- Database connection failed: Check `POSTGRES_URL`
- AGE extension not loaded: Verify Apache AGE installation

### No Entities Extracted

**Check if repo is enabled:**
```bash
curl http://localhost:8350/ | jq .enabled_repos
```

**Check file filtering:**
- Only processes code files: `.py`, `.go`, `.ts`, `.tsx`, `.js`, `.jsx`, `.rs`
- Skips test files: `*_test.go`, `*_test.py`, `*.spec.ts`, `*.test.ts`
- Skips generated files: `*.d.ts`, `node_modules/`, `dist/`, `build/`

**Check stats:**
```bash
curl http://localhost:8350/stats | jq
```

### Database Errors

**AGE extension not loaded:**
```sql
CREATE EXTENSION IF NOT EXISTS age;
```

**Graph doesn't exist:**
```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
SELECT create_graph('regen_graph');
```

**Connection errors:**
- Verify PostgreSQL is running: `systemctl status postgresql`
- Check connection string in `.env`
- Verify port: default 5433 (Docker) or 5432 (system)

## Performance

### Processing Throughput

- **Average**: 5-10 events/minute
- **Entity extraction**: ~10-50ms per file
- **Graph loading**: ~5-20ms per entity
- **Memory usage**: ~50-100MB per process

### Optimization Tips

1. **Batch processing**: Events are processed sequentially (by design for DB safety)
2. **Connection pooling**: Service reuses single processor instance
3. **Selective processing**: Only configured repos are processed
4. **Deduplication**: MERGE queries prevent duplicates

## Development

### Adding New Languages

1. Add file extension to `code_extensions` dict in `CodeGraphProcessor`
2. Implement `_extract_{language}_entities` method
3. Add language-specific patterns for entity types
4. Test extraction with sample code

### Adding New Entity Types

1. Add type to `_classify_{language}_entity` method
2. Update graph queries in MCP server
3. Document in this file

### Testing

```bash
# Test extraction
curl -X POST "http://localhost:8350/test-extraction?source_code=..."

# Test full pipeline
# 1. Trigger GitHub sensor refresh
# 2. Check coordinator received event
# 3. Check forwarder logs
# 4. Check Code Graph Service logs
# 5. Query graph for new entities
```

## Roadmap

- [ ] Tree-sitter integration for better parsing
- [ ] Relationship extraction (function calls, imports, etc.)
- [ ] Module/package hierarchy
- [ ] Cross-repository references
- [ ] Type inference and relationships
- [ ] Call graph generation
- [ ] Dependency analysis
- [ ] Code metrics (complexity, size, etc.)

## Related Documentation

- [Architecture](./overview.md)
- [API Documentation](../operations/api.md)
- Deployment guide: maintainer-only, not part of the public export
