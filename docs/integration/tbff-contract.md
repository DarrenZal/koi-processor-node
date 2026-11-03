# TBFF Integration Contract — KOI API for Flow Funding

**Version:** 1.0 (2026-02-26)
**Base URLs:** each node serves this API on its own host (`http://<node-host>:8351` in the examples below). The web dashboard BFF proxies every node at `http://<bff-host>:3100/commons/api/nodes/{nodeId}/...`.

## Authentication

- **Read endpoints** (`GET`): No authentication required.
- **Write endpoints** (`POST /ingest` direct): No authentication required for localhost. Remote callers need `KOI_COMMONS_SERVICE_TOKEN` header (ask the node operator to provision one).
- **BFF ingest** (`POST /commons/api/nodes/{nodeId}/ingest`): Requires `x-ingest-token` header matching server's `BFF_INGEST_TOKEN`. Also requires `source` field in body. Returns 401 without valid token, 503 if token not configured in production.
- **BFF proxy** (web dashboard): All endpoints available through `http://<bff-host>:3100/commons/api/nodes/{nodeId}/...`

> **Note:** Direct node IPs (e.g., `:8351`) are behind nginx and may return 404 from external networks. Always use BFF paths for external access.

---

## 1. Search Entities by Relevance

Find entities matching a text query with fuzzy + containment scoring.

```
GET /entity-search?query={text}&limit={n}&entity_type={type}
```

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `query` | yes | — | Search text (min 1 char) |
| `limit` | no | 20 | Max results (1-100) |
| `entity_type` | no | — | Filter by type (e.g. `Evidence`, `CaseStudy`, `Project`) |

**Response:**
```json
{
  "results": [
    {
      "fuseki_uri": "orn:personal-koi.entity:project-garry-oak-restoration-abc123",
      "name": "Garry Oak Ecosystem Restoration",
      "entity_type": "Project",
      "similarity": 0.9,
      "aliases": ["Garry Oak Recovery"],
      "relationship_count": 5
    }
  ],
  "count": 1
}
```

**Example:**
```bash
# Search for evidence about restoration
curl -s "http://<node-host>:8351/entity-search?query=restoration&entity_type=Evidence&limit=10"

# Search across all types
curl -s "http://<node-host>:8351/entity-search?query=garry+oak"
```

Also supports POST with JSON body:
```bash
curl -s -X POST http://<node-host>:8351/entity-search \
  -H "Content-Type: application/json" \
  -d '{"query": "garry oak restoration", "limit": 10, "entity_type": "Project"}'
```

---

## 2. List Entities by Type

Retrieve all entities, optionally filtered by type.

```
GET /entities?entity_type={type}&limit={n}&offset={n}
```

| Param | Required | Default | Description |
|-------|----------|---------|-------------|
| `entity_type` | no | — | Filter by type |
| `limit` | no | 50 | Max results |
| `offset` | no | 0 | Pagination offset |

**Response:**
```json
{
  "entities": [
    {
      "fuseki_uri": "orn:personal-koi.entity:evidence-garry-oak-fire-regime-abc123",
      "entity_text": "Garry Oak Fire Regime Evidence",
      "entity_type": "Evidence",
      "source": "demo-seed",
      "created_at": "2026-02-26T10:00:00Z",
      "metadata": {}
    }
  ],
  "count": 15,
  "total": 32
}
```

**Example:**
```bash
# All evidence entities on Greater Victoria
curl -s "http://<node-host>:8351/entities?entity_type=Evidence"

# All case studies on Octo
curl -s "http://<node-host>:8351/entities?entity_type=CaseStudy"
```

---

## 3. Get Relationships for an Entity

Retrieve all relationships where the entity appears as subject or object.

```
GET /relationships/{entity_uri}
```

**Response:**
```json
{
  "relationships": [
    {
      "subject_uri": "orn:personal-koi.entity:project-garry-oak-restoration-abc123",
      "predicate": "located_in",
      "object_uri": "orn:personal-koi.entity:location-saanich-peninsula-def456",
      "source": "demo-seed",
      "created_at": "2026-02-26T10:00:00Z"
    },
    {
      "subject_uri": "orn:personal-koi.entity:organization-habitat-acquisition-trust-ghi789",
      "predicate": "has_project",
      "object_uri": "orn:personal-koi.entity:project-garry-oak-restoration-abc123",
      "source": "demo-seed",
      "created_at": "2026-02-26T10:00:00Z"
    }
  ],
  "count": 2
}
```

**Example:**
```bash
# Get all relationships for a specific entity
curl -s "http://<node-host>:8351/relationships/orn:personal-koi.entity:project-garry-oak-restoration-abc123"
```

---

## 4. Ingest Entities + Relationships (Write-Back)

Log a funding decision, evidence, or any structured data back into the knowledge graph.

```
POST /ingest
Content-Type: application/json
```

**Request body:**
```json
{
  "document_rid": "tbff:decision-2026-03-01-garry-oak",
  "entities": [
    {"name": "Garry Oak Restoration Grant Q1-2026", "type": "Evidence"},
    {"name": "Victoria Landscape Group", "type": "Organization"}
  ],
  "relationships": [
    {
      "subject": "Garry Oak Restoration Grant Q1-2026",
      "predicate": "documents",
      "object": "Victoria Landscape Group"
    }
  ],
  "source": "tbff-sandbox"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `document_rid` | yes | Unique document identifier (use `tbff:` prefix) |
| `entities` | yes | Array of `{name, type}` objects |
| `relationships` | no | Array of `{subject, predicate, object}` triples |
| `source` | no | Tag for provenance tracking (default: `personal-vault`) |

**Response:**
```json
{
  "status": "success",
  "document_rid": "tbff:decision-2026-03-01-garry-oak",
  "entities_processed": 2,
  "relationships_processed": 1,
  "canonical_entities": [
    {
      "uri": "orn:personal-koi.entity:evidence-garry-oak-restoration-grant-q1-2026-abc123",
      "name": "Garry Oak Restoration Grant Q1-2026",
      "type": "Evidence",
      "resolution_tier": "tier3_new"
    },
    {
      "uri": "orn:personal-koi.entity:organization-victoria-landscape-group-def456",
      "name": "Victoria Landscape Group",
      "type": "Organization",
      "resolution_tier": "tier1_exact"
    }
  ]
}
```

**Key behaviors:**
- Entity resolution runs automatically (exact match → fuzzy → semantic → create new)
- Relationships reference entities by name — resolution maps them to URIs
- `source` tag enables rollback: `DELETE FROM entity_registry WHERE source = 'tbff-sandbox'`
- Duplicate entities are merged, not duplicated (idempotent on name+type)

**Example — log a funding decision:**
```bash
curl -s -X POST http://<node-host>:8351/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "document_rid": "tbff:decision-2026-03-01-garry-oak",
    "entities": [
      {"name": "Garry Oak Restoration Grant Q1-2026", "type": "Evidence"},
      {"name": "Victoria Landscape Group", "type": "Organization"},
      {"name": "Saanich Peninsula", "type": "Location"}
    ],
    "relationships": [
      {"subject": "Garry Oak Restoration Grant Q1-2026", "predicate": "documents", "object": "Victoria Landscape Group"},
      {"subject": "Victoria Landscape Group", "predicate": "located_in", "object": "Saanich Peninsula"}
    ],
    "source": "tbff-sandbox"
  }'
```

---

## 5. Chat / RAG Query

Ask natural-language questions against the knowledge graph (requires OpenAI key on node).

```
POST /chat
Content-Type: application/json
```

**Request:**
```json
{
  "query": "What evidence exists for Garry Oak ecosystem restoration in Saanich?",
  "max_context_entities": 10
}
```

**Response:**
```json
{
  "response": "Based on the knowledge graph, there are several relevant entities...",
  "context_entities": [
    {"uri": "orn:...", "name": "Garry Oak Ecosystem Restoration", "type": "Project", "similarity": 0.92}
  ],
  "model": "gpt-4o-mini"
}
```

**Availability:** nodes with an OpenAI key configured. A node without one returns `503`. Falls back to text search if embedding tables are missing.

---

## Entity Types (BKC Ontology)

15 types available: `Person`, `Organization`, `Project`, `Location`, `Concept`, `Meeting`, `Practice`, `Pattern`, `CaseStudy`, `Bioregion`, `Protocol`, `Playbook`, `Question`, `Claim`, `Evidence`

**TBFF-relevant types:**
- `Evidence` — Documented evidence backing a claim or decision
- `CaseStudy` — Detailed case study of a practice or project
- `Claim` — An assertion that can be supported or opposed
- `Project` — Active restoration or stewardship project
- `Practice` — A method or approach (e.g., "watershed stewardship")

## Predicates (Relationship Types)

27 predicates across 4 categories. TBFF-relevant subset:
- `documents` — X documents Y (evidence → subject)
- `supports` — X supports claim Y
- `opposes` — X opposes claim Y
- `informs` — X informs decision Y
- `located_in` — X is located in Y
- `has_project` — Organization X has project Y
- `affiliated_with` — X is affiliated with Y

---

## Content-Addressed Identifiers (KOI RIDs)

Every entity has a stable URI: `orn:personal-koi.entity:{type}-{normalized-name}-{hash}`

These can be referenced in on-chain attestations, hypercerts, or any external system as content-addressed evidence identifiers.

---

## BFF (Web Dashboard) Endpoints

All the above endpoints are also available through the web dashboard BFF, which fans out across all nodes:

```bash
# Global search across all nodes (merged, deduplicated, scored)
curl -s "http://<bff-host>:3100/commons/api/search?q=garry+oak&limit=20"

# Per-node search
curl -s "http://<bff-host>:3100/commons/api/nodes/greater-victoria/search?q=restoration"

# Per-node entities
curl -s "http://<bff-host>:3100/commons/api/nodes/greater-victoria/entities?entity_type=Evidence"

# Per-node chat
curl -s -X POST "http://<bff-host>:3100/commons/api/nodes/octo-salish-sea/chat" \
  -H "Content-Type: application/json" \
  -d '{"message": "What do you know about flow funding?"}'
```

**Node IDs:** `octo-salish-sea`, `front-range`, `greater-victoria`, `cowichan-valley`

---

## Quick Start: One Full Loop (via BFF)

All steps use the externally reachable BFF. No direct node IPs needed.

```bash
# 1. Query evidence for a bioregion
curl -s "http://<bff-host>:3100/commons/api/search?q=garry+oak"

# 2. Chat for context
curl -s -X POST "http://<bff-host>:3100/commons/api/nodes/octo-salish-sea/chat" \
  -H "Content-Type: application/json" \
  -d '{"message": "What evidence exists for Garry Oak restoration?"}'

# 3. TBFF evaluates and decides...

# 4. Log the decision back via BFF ingest (requires x-ingest-token + source)
curl -s -X POST "http://<bff-host>:3100/commons/api/nodes/octo-salish-sea/ingest" \
  -H "Content-Type: application/json" \
  -H "x-ingest-token: <BFF_INGEST_TOKEN>" \
  -d '{
    "document_rid": "tbff:decision-2026-03-garry-oak-grant",
    "entities": [{"name": "Garry Oak Q1 Grant Decision", "type": "Evidence"}],
    "relationships": [{"subject": "Garry Oak Q1 Grant Decision", "predicate": "informs", "object": "Garry Oak Ecosystem Restoration"}],
    "source": "tbff-sandbox"
  }'

# 5. Verify the decision is searchable
curl -s "http://<bff-host>:3100/commons/api/search?q=Garry+Oak+Q1+Grant"
```
