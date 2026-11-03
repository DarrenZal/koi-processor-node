# Obsidian Vault ↔ Knowledge Graph Sync

## Project Overview

Enable bidirectional synchronization between a local Obsidian vault (YAML frontmatter) and the Regen Knowledge Graph. The Obsidian vault serves as the **source of truth** for personal/organizational knowledge, with the KG providing semantic querying and relationship inference.

## Current Status

### Completed Work

#### 1. Obsidian MCP Vault Tools (regen-koi-mcp)
- **Branch**: `feature/obsidian-support`
- **Status**: Ready for testing
- Local vault tools for Claude Code integration:
  - `vault_read_note` - Read note content with parsed YAML frontmatter
  - `vault_write_note` - Create/update notes with frontmatter
  - `vault_list_notes` - List notes by folder or entity type
  - `vault_search_notes` - Search notes by query and content
  - `vault_get_entity` - Get entity by type and name
  - `vault_prep_meeting` - Meeting prep with attendee context
- Tool filtering via `MCP_ENABLED_TOOLS` environment variable

#### 2. Obsidian Sensor (koi-sensors)
- **Branch**: `feature/obsidian-sensor`
- **Status**: Ready for testing
- Parses Obsidian vault markdown files with YAML frontmatter
- Extracts wikilinks (`[[link]]`) for relationship mapping
- Handles `@type` and `@id` schema.org-style entity typing
- Fixed YAML parsing for `@` characters (reserved in YAML)

#### 3. Vault Cleanup & Standardization
- **Status**: Complete
- Standardized YAML frontmatter (`"@type"`) across the vault's entity folders (People, Organizations, Meetings, Projects)
- Created schema documentation notes (`schema-person.md`, `schema-organization.md`, `schema-meeting.md`, `schema-project.md`)

#### 4. Enhanced Contextual Entity Resolution (2026-02-02)
- **Branch**: `feature/obsidian-kg-sync-plan`
- **Status**: ✅ Complete
- Multi-hop relationship-aware entity resolution:
  - **Tier 1.5 Contextual**: Uses org/project relationships for disambiguation
  - **Phonetic Matching**: Double Metaphone for name variants (Alyce → Alice)
  - **2-hop Paths**: Person → affiliated_with/founded/has_founder → Org → has_project → Project
- Per-entity context fields:
  - `associated_people` - People mentioned alongside entity
  - `associated_organizations` - Orgs mentioned alongside entity
- Relationship sync from vault frontmatter:
  - `founders` → `has_founder` (Person → Org)
  - `affiliation` → `affiliated_with` (Person → Org)
  - `projects` → `has_project` (Org → Project)
  - `members` → `involves_person` (Project → Person)
  - `parentOrg` → `involves_organization` (Project → Org)
- **Metrics**:
  - Relationships in DB: 3 → 112
  - Test result: "Alyce Rivera" + Example Labs → "Alice Rivera" @ 93.4%

### Infrastructure Status

| Component | Status | Notes |
|-----------|--------|-------|
| PostgreSQL + Apache AGE | ✅ Running | Graph database |
| Apache Jena Fuseki | ✅ Running | SPARQL endpoint |
| Embedding Models | ✅ Running | BGE-M3 for vectors |
| KOI Coordinator | ✅ Running | Event orchestration |
| KOI Processor | ✅ Running | Quality pipeline |
| Obsidian Sensor | 🔄 Ready | Needs deployment |

---

## Architecture

### Current KOI Pipeline

```
Sensors → Coordinator → Processor → PostgreSQL/AGE + Jena
                                   (Dedup → Normalize → Store)
```

### Proposed Obsidian Integration

```
                    ┌─────────────────────────┐
                    │    Obsidian Vault       │
                    │  (YAML = Source of      │
                    │        Truth)           │
                    └───────────┬─────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
              ▼                 │                 ▼
    ┌─────────────────┐         │       ┌─────────────────┐
    │ Obsidian Sensor │         │       │   MCP Vault     │
    │ (Batch Ingest)  │         │       │   Tools         │
    └────────┬────────┘         │       └────────┬────────┘
             │                  │                │
             ▼                  │                ▼
    ┌─────────────────┐         │       ┌─────────────────┐
    │ KOI Coordinator │         │       │  Claude Code    │
    │  (Events)       │         │       │  (Interactive)  │
    └────────┬────────┘         │       └─────────────────┘
             │                  │
             ▼                  │
    ┌─────────────────┐         │
    │ YAML→KG Loader  │◄────────┘ (Sync State)
    │ (New Component) │
    └────────┬────────┘
             │
             ▼
    ┌─────────────────────────────────────────┐
    │           Knowledge Graph               │
    │  ┌─────────────┐    ┌─────────────┐    │
    │  │ PostgreSQL  │    │   Jena      │    │
    │  │ + AGE       │    │   Fuseki    │    │
    │  └─────────────┘    └─────────────┘    │
    └─────────────────────────────────────────┘
```

### Key Design Decisions

1. **YAML as Source of Truth**: All structured data lives in Obsidian frontmatter
2. **Unidirectional Primary Flow**: Vault → KG (not KG → Vault for now)
3. **Sync State Tracking**: Hash-based change detection per file
4. **Direct YAML → KG Loading**: Bypass NLP extraction for typed entities
5. **Wikilinks → Relationships**: `[[Person Name]]` becomes graph edges

---

## Implementation Roadmap

### Phase 1: Sensor Deployment (Current)
- [ ] Deploy Obsidian sensor to production
- [ ] Validate vault scan and entity extraction
- [ ] Test full pipeline: Sensor → Coordinator → Processor → KG
- [ ] Verify entities appear in KG with correct typing

### Phase 2: YAML-to-KG Direct Loader
Create a new loader module that:
- Reads YAML frontmatter from vault files
- Maps `@type` to ontology classes
- Creates triples directly without NLP extraction
- Handles wikilinks as relationship predicates

```python
# Conceptual structure
class YAMLToKGLoader:
    def load_entity(self, file_path: str, frontmatter: dict):
        entity_type = frontmatter.get("@type")
        entity_id = frontmatter.get("@id") or self.generate_id(file_path)

        # Map to ontology
        triples = self.map_to_triples(entity_type, entity_id, frontmatter)

        # Store in graph
        self.kg_client.insert_triples(triples)
```

### Phase 3: Sync State Management
Track what's been synced to detect changes:

```sql
CREATE TABLE obsidian_sync_state (
    file_path TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    last_synced TIMESTAMP NOT NULL,
    entity_uri TEXT,
    sync_status TEXT  -- 'synced', 'pending', 'error'
);
```

### Phase 4: Incremental Sync
- Watch for file changes (file watcher or periodic scan)
- Compute hash diff against sync state
- Process only changed files
- Update or create entities in KG

### Phase 5: (Future) Reverse Sync
- KG → YAML for enrichment (optional)
- AI-generated summaries written back to vault
- Relationship inference written as wikilinks

---

## Entity Type Mappings

| Vault @type | Ontology Class | Predicates |
|-------------|----------------|------------|
| Person | schema:Person | name, role, organization, expertise |
| Organization | schema:Organization | name, type, description, members |
| Meeting | schema:Event | date, attendees, project, topics, decisions |
| Project | schema:Project | name, status, organization, participants |

---

## Technical Details

### Current Processor Pipeline

The koi-processor already has a sophisticated pipeline:

1. **ConfidenceFilter** - Filters low-confidence extractions
2. **Deduplicator** - 3-tier deduplication (Exact → Fuzzy → Vector)
3. **CanonicalResolver** - Resolves to canonical entity URIs
4. **OntologyNormalizer** - Normalizes predicates to ontology
5. **EntityQualityFilter** - Quality scoring and filtering
6. **ListSplitter** - Splits list-valued properties

For YAML entities, we can:
- Skip ConfidenceFilter (already validated)
- Use Deduplicator for wikilink resolution
- Apply OntologyNormalizer for predicate mapping
- Skip NLP-specific modules

### File Hashing Strategy

```python
def compute_sync_hash(file_path: str) -> str:
    """Hash based on frontmatter only (content changes don't affect KG)."""
    content = Path(file_path).read_text()
    frontmatter = extract_frontmatter(content)
    return hashlib.sha256(json.dumps(frontmatter, sort_keys=True).encode()).hexdigest()
```

---

## Repository References

| Repository | Branch | Purpose |
|------------|--------|---------|
| regen-koi-mcp | `feature/obsidian-support` | MCP vault tools |
| koi-sensors | `feature/obsidian-sensor` | Vault sensor |
| koi-processor | `regen-prod` | Quality pipeline |
| koi-coordinator | `main` | Event orchestration |

---

## Configuration

### Obsidian Sensor Config

```yaml
# sensor_config.yaml
obsidian:
  vault_path: /path/to/your/vault
  entity_folders:
    - People
    - Organizations
    - Meetings
    - Projects
  schema_folder: Ontology
  output_format: koi_manifest
```

### MCP Tool Filtering

```bash
# .zshrc or Claude Code config
export MCP_ENABLED_TOOLS="vault_read_note,vault_write_note,vault_search_notes,vault_list_notes,vault_get_entity,vault_prep_meeting"
```

---

## Success Criteria

1. **Vault entities appear in KG** - Query `SELECT ?s ?p ?o WHERE { ?s a schema:Person }` returns vault people
2. **Wikilinks become edges** - `[[Alice Rivera]]` in a meeting creates `attendee` relationship
3. **Changes sync within 5 minutes** - File modification triggers KG update
4. **No data loss** - YAML remains authoritative, KG is derived view
5. **MCP tools work** - Claude Code can read/write vault via MCP

---

## Open Questions

1. **Conflict resolution**: If same entity exists from multiple sources, which wins?
2. **Deletion handling**: What happens when a vault file is deleted?
3. **Schema evolution**: How to handle frontmatter schema changes over time?
4. **Access control**: Different visibility levels for vault subsets?

---

## Next Steps

1. Merge feature branches after testing
2. Deploy Obsidian sensor to production coordinator
3. Run full vault ingestion
4. Verify KG population
5. Begin Phase 2: YAML-to-KG direct loader

---

*Last updated: 2025-01-16*
