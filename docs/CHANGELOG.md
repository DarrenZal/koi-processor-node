# Changelog

All notable changes to the KOI Processor project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.4.0] - 2026-03-11

### Added
- **Steel Thread Phase B — Evidence-from-artifacts endpoint**:
  - `POST /claims/evidence-from-artifacts` creates Evidence entities from published interview artifacts (Pattern, Protocol, CaseStudy, Practice)
  - Visibility scope inherited from source artifacts (most-restrictive wins: any `node_private` source → `node_private` Evidence)
  - Bioregion-aware naming and URI generation (distinct canonical URIs per bioregion)
  - Delegates to existing `/register-entity` path for full entity resolution behavior
  - Creates `derived_from` graph edges linking Evidence to source artifacts
  - Idempotent via vault_rid
- **Migration 067**: `derived_from` predicate in `allowed_predicates`
- **Consent leakage smoke test** (`tests/test_consent_leakage.sh`): 18 checks across all 15 public endpoints verifying `node_private` entity invisibility
- **Steel Thread Phase B test** (`tests/test_steel_thread_phase_b.sh`): 25 checks — full pipeline from published artifacts → Evidence → claim → attestations → anchor → proof pack
- **Data Classification Matrix v0.1**: 34 query sites across 15 public endpoints reconciled. Consent leakage verified. T2 enforcement deferred to Horizon 3.

### Changed
- Claims Engine V1 doc updated with Phase B endpoints, predicates, build history, and key files

### Verified
- Steel Thread Phase A: 22/22 on Local, Octo, FR
- Steel Thread Phase B: 25/25 on Local + Octo (on published interview artifacts)
- Consent leakage: 18/18 on Local
- On-chain anchor TX: `84BCCDA873F39E65C43D885F6808A24107E1F85FD4A32A81CB182D47A35D6FEB`

## [3.3.2] - 2026-02-25

### Added
- **KOI-net document sharing API** with rich-share metadata:
  - `POST /koi-net/share` now supports:
    - `share_mode`: `root_only` | `root_plus_required` | `context_pack`
    - `context_depth` (1-4)
    - `recipient_type`: `peer` | `commons`
  - Share manifests now store:
    - `references_summary`
    - `dependency_graph_summary`
    - `missing_references`
    - `recipient_type`
- **Commons intake workflow endpoints**:
  - `GET /koi-net/commons/intake`
  - `POST /koi-net/commons/intake/decide` (localhost + admin token)

### Changed
- **`/koi-net/shared-with-me` response expanded** to include:
  - `recipient_type`
  - `intake_status`
  - `reviewed_at`, `reviewed_by`, `review_notes`
  - `context_depth` and dependency summary fields
- **Inbound share persistence now retains manifest/context metadata** for receiver-side visibility.

### Security
- Added reusable localhost/admin-token gate helpers for admin-only actions:
  - `_read_admin_token()`
  - `_enforce_local_admin()`
- Edge approval and commons intake decisions both require local admin authorization.

### Migrations
- Added `047_shared_documents_intake.sql`:
  - `koi_shared_documents.recipient_type`
  - `koi_shared_documents.intake_status`
  - `koi_shared_documents.reviewed_at`
  - `koi_shared_documents.reviewed_by`
  - `koi_shared_documents.review_notes`

### Validation
- Python compile checks pass for `koi_net_router.py` and `koi_poller.py`.
- Live smoke checks run against local node on port `8351`:
  - `recipient_type=commons` share request accepted
  - commons intake listing/approve/reject flow verified
  - invalid UUID input now returns `INVALID_EVENT_ID` (400) instead of 500

## [3.3.1] - 2026-02-19

### Fixed
- **extractor_version column too narrow** - Widened `koi_kg_extractions.extractor_version` from varchar(20) to varchar(50) to accommodate model names like `docstring-gpt-4.1-mini`

### Production Run
- **Docstring extraction deployed** on koi-processor (290 files) and regen-ledger (500 files)
  - 1,167 batches, 10,508 entities persisted, 235 relationships
  - Top types: API_MESSAGE (5,565), CONCEPT (3,463), TECHNOLOGY (960)
  - Zero failures, idempotency verified on re-run

## [3.3.0] - 2026-02-18

### Added
- **Docstring-to-KG Semantic Extraction** - Route tree-sitter-extracted docstrings through LLM semantic extractor
  - New `src/core/docstring_filter.py`: Filters trivial/synthetic docstrings, aggregates into 3000-char batches
  - New `scripts/extract_docstring_semantics.py` (not in this export): Batch coordinator with idempotency, quality pipeline, entity dedup
  - New `migrations/039_code_docstring_extractions.sql`: Provenance table with FK cascade and deterministic idempotency key
  - Modified `src/extraction/prompt_builder.py`: Domain-tuned prompt for `code_docstring` source type
    - Prioritizes: API_MESSAGE, KEEPER, MODULE, STANDARD, TECHNOLOGY, CONCEPT
    - Blocks: variable names, generic programming words, implementation details
  - Shadow `koi_memories` rows with `source_sensor='code_docstring'` for analytics visibility
  - Full quality pipeline: ConfidenceFilter → CanonicalResolver → EntityQualityFilter → ListSplitter → OntologyNormalizer
  - EntityResolver deduplication against existing doc-sourced entities
  - Deterministic RIDs: `code_docstring:{repo}:{path_hash}:{file_hash}:{batch}:{prompt_ver}:{model}`

## [3.2.1] - 2026-02-03

### Added
- **Tier 1.1 Alias Resolution** - Entity aliases stored in vault frontmatter now resolve to canonical entities
  - New `normalize_alias()` helper strips wikilinks and normalizes for matching
  - Aliases stored in `entity_registry.aliases` TEXT[] column with GIN index
  - Resolution: "Acme" (mentioned) → "Acme Labs" (canonical) via alias match @ 100% confidence
  - `/register-entity` now syncs aliases from frontmatter to backend
  - Migration 036: Enforces TEXT[] type for aliases column (aligns with production DB)

- **Vault Parser Predicate Mappings** - Added support for `creator` and `lead` fields
  - `creator` → `has_founder` (incoming, Person) - same semantics as `founders`
  - `lead` → `involves_person` (outgoing, Person)

- **Schema Loading Diagnostics** - Enhanced logging for vault schema loading
  - Logs vault path, individual schema names, and phonetic_matching status
  - Helps debug when schemas fail to load or have unexpected settings

### Fixed
- **Entity Resolution False Positives** - "Acme" no longer creates duplicate entity when "Acme Labs" exists with alias
  - Root cause: Aliases in frontmatter weren't being read or matched during resolution
  - Fix: Tier 1.1 alias matching inserted after exact match, before contextual matching

## [3.2.0] - 2026-02-02

### Added
- **Enhanced Contextual Entity Resolution** - Multi-hop relationship-aware resolution
  - Per-entity `associated_people` and `associated_organizations` context fields
  - Organization and project context for disambiguation
  - Phonetic matching (Double Metaphone) for name variants (e.g., "Alyce" → "Alice")
  - 2-hop resolution paths: Person → Org → Project
  - Predicates: `affiliated_with`, `founded`, `has_founder`, `involves_person`
  - Result: "Alyce Rivera" + Example Labs context → "Alice Rivera" @ 93.4% confidence

- **Proto Support for Code Graph** - Cosmos SDK cross-language linking
  - ProtoMessage, ProtoService, ProtoRPC, ProtoEnum entity types
  - Enables proto → Go struct → TypeScript type linking
  - Module hierarchy with BELONGS_TO and CONTAINS edges

- **Local BGE Embedding Server** - `src/core/bge_server_local.py`
  - Privacy-preserving embeddings using BAAI/bge-large-en-v1.5
  - No data leaves the machine - suitable for personal KOI deployment

- **GitHub Webhook Handler** - `api/github_webhook.py`
  - Receives push events and triggers code extraction
  - Environment-configurable paths (CODE_GRAPH_BASE_PATH, REGEN_REPOS_PATH)

- **Phonetic Code Backfill** - `scripts/backfill_phonetic_codes.py` (not in this export)
  - Utility to populate phonetic_code for schema-enabled entity types
  - Supports per-type stopwords and idempotent execution

### Changed
- **Vault Parser** - Added `parentorg` predicate mapping for Project → Organization
- **Entity Resolution** - Relationships synced from vault: 3 → 112

### Fixed
- **has_founder Direction** - Corrected to Person (subject) → Org (object)
- **Frontmatter Sync** - Backend accepts both `frontmatter` and `properties` fields

## [3.1.5] - 2026-01-02

### Fixed
- **Weekly Digest Stale Cache** - Date-range aware caching for `/api/koi/weekly-digest`
  - Root cause: Cache lookup ignored `start_date`/`end_date` params, returning 6+ day old digests
  - Fix: Cache filename now includes date range (`weekly_digest_{start}_to_{end}.md`)
  - Affected files: `content_dashboard.py` (not in this export), `weekly_curator_llm.py` (not in this export)

- **Event Bridge Routing** - Forwarder now targets correct endpoint
  - Root cause: Forwarder posted to semantic bridge (port 8004) instead of v2 bridge (port 8100)
  - Fix: `EVENT_BRIDGE_URL=http://localhost:8100`, `EVENT_BRIDGE_ENDPOINT=/process-koi-event`

- **Fuseki Provenance 401** - Added Basic Auth support for provenance writes
  - Root cause: `provenance_to_rdf.py` (not in this export) didn't authenticate with Fuseki
  - Fix: Added `FUSEKI_USER`/`FUSEKI_PASSWORD` env var support
  - Runbook: `docs/runbook-fuseki-provenance.md` (not in this export)

### Changed
- **Pipeline Health** - All event flow components verified working:
  - Sensors → Coordinator → Forwarder → Event Bridge v2 → BGE → koi_embeddings ✅
  - Provenance → Fuseki ✅

## [3.1.4] - 2025-12-29

### Added
- **FIX-020: Alias Audit & Merge Scripts** - Entity deduplication tooling
  - `scripts/alias_audit.py` (not in this export) - Generates audit report of alias entities matching canonical_entities.json
  - `scripts/apply_alias_merges.py` (not in this export) - Applies safe merges with backup tables
  - Merged 8 alias entities into their canonical counterparts
  - Copies the affected rows into backup tables before merging
- **GraphRAG 3D Export** - `scripts/export_graph_hierarchy.py` (not in this export)
  - Exports entity_registry + koi_relationships to 3D visualization format
  - Uses UMAP for 3D positioning from pgvector embeddings
  - Outputs JSON for `GAIA/graph/GraphRAG3D_EmbeddingView.html`
- **Post-Extraction Audit Runbook** - `docs/runbooks/post_extraction_audit.md` (not in this export)
  - Standardized checklist for validating extraction quality
  - Companion script: `scripts/post_extraction_audit.sh` (not in this export)

## [3.1.3] - 2025-12-28

### Fixed
- **Stage6 Predicate Guard Bypass** - Fixed `stage6_full_reextract_gemini.py` (not in this export) not applying predicate normalization
  - Root cause: Script used `normalize_predicate()` (snake_case only) instead of `validate_predicate()` (applies PREDICATE_MAPPINGS)
  - Fix: Import and use `validate_predicate()` from `predicate_guard.py` after basic normalization
  - Result: Non-canonical predicates like `exploring` now correctly map to `discusses`

### Added
- **Week 20 Predicate Mappings** - 8 new mappings in `predicate_guard.py`:
  - `used_in` → `uses`, `handles` → `processes`, `powers` → `powered_by`
  - `managed_by` → `manages`, `implemented_in` → `implements`, `co_founded` → `founded`
  - `developed` → `creates`, `funded_by` → `funds`

### Changed
- **Week 20 QA Documentation** - Updated `docs/archive/knowledge-graph-review-2026-01.md` (not in this export)
  - Phase A: Predicate regrowth analysis (66 non-canonical from Week 19 → fixed)
  - Phase B: Quality spot-check (94% acceptable, threshold 85%)
  - Phase C: GraphRAG eval (100% resolution, threshold 70%)

## [3.1.2] - 2025-12-24

### Fixed
- **E401: Entity Documents Privacy Filter Order** - Fixed `/entity/documents` endpoint returning 0 docs for entities like "ethereum"
  - Root cause: Privacy filter applied AFTER `LIMIT` in CTE; for entities where first docs in index order were private, all results filtered out
  - Fix: Moved `JOIN koi_memories` + privacy filter INTO the `entity_docs` CTE before `LIMIT`
  - Verified: ethereum, regen ledger, regen commons, cosmos sdk, polygon all now return docs correctly

### Changed
- **Week 10 Closeout Documentation** - Updated `docs/archive/knowledge-graph-review-2026-01.md` (not in this export)
  - Added E401 fix details and verification results
  - Added Week 11 placeholder with backlog items
  - Expanded Curated Entity Audit table with closeout checks

## [3.1.1] - 2025-12-24

### Added
- **Graph Expansion PoC (Log-Only)** - 1-hop relationship traversal analysis
  - `get1HopNeighbors()` function finds related entities via `koi_relationships`
  - Quality filters: confidence >= 0.5, occurrence_count >= 2
  - Logs potential recall gains without affecting search ranking
  - Enable with `DEBUG_GRAPH_EXPANSION=true`

### Changed
- **Normalized Text Index** - Entity lookup now uses `normalized_text` B-Tree index
  - Was: `LOWER(entity_text)` (computed per query)
  - Now: `normalized_text` (indexed column)
  - Includes `entity_type` in results for debugging
- **Multi-Token Seed Filter** - Expansion seeds filtered to >= 2 words OR >= 8 chars
  - Prevents single-token names ("alice") from exploding to 1000+ docs
  - Reduces noise in graph expansion analysis
- **High-Degree Entity Guard** - Skips expensive COUNT when neighbors > 10
  - Avoids slow queries on high-connectivity entities
- **Removed `entity_uris` Propagation** - Cleaned up unused field from entity search CTEs
  - Was aggregating URIs but never using them (switched to name-based lookup)

### Fixed
- **Duplicate API process** - Two instances of the query API ran at once on the same port
  - The older instance was answering every request
  - Resolution: stop the older instance

## [3.1.0] - 2025-12-24

### Fixed
- **Hybrid Search Keyword Scoring** - Fixed `keyword_score: 0` in search results
  - Root cause: RID mismatch between entity chunks (`UUID#chunk14`) and keyword base docs (`UUID`)
  - Added `normalizeRidForFusion()` to strip chunk suffix before fusion merge
  - Keyword scores now properly contribute (0.59, 0.31, 0.30 instead of 0)

### Added
- **Full-Text Search Migration** - `migrations/025_add_content_tsv_fts.sql`
  - `content_tsv` tsvector column with weighted title (A) + text (B)
  - Trigger for auto-update on INSERT/UPDATE
  - GIN index for fast FTS queries
- **FTS Backfill Script** - `scripts/backfill-fts.sql`
  - Batch processing (5K rows) to avoid locks
  - CONCURRENTLY index creation
  - Verification queries
- **Unit Tests** - `tests/adaptive-features.test.ts`
  - RID normalization tests
  - Fusion merge tests
- **Integration Tests** - `tests/test_keyword_search_fts.py`
  - FTS trigger validation
  - Prefix vs strict matching

### Changed
- **Strict-First Query Ordering** - AND matches now prioritized over OR matches
- **Lexeme-Aware OR Filtering** - Replaced substring filter with prefix tsquery
- **Debug Logging Gated** - All per-request logs now behind `DEBUG_*` env flags
  - `DEBUG_AUTH`, `DEBUG_EXTRACTION`, `DEBUG_FUSION`, `DEBUG_KEYWORD_SEARCH`, `DEBUG_GRAPH_EXPANSION`
- **Documentation Updated** - Fixed migration references (012→025) in:
  - `docs/KOI_PIPELINE_COMPLETE.md` (not in this export)
  - `docs/ADAPTIVE_KNOWLEDGE_MCP_IMPLEMENTATION.md` (not in this export)
  - `docs/ADAPTIVE_KNOWLEDGE_IMPLEMENTATION_STATUS.md` (not in this export)

## [3.0.0] - 2025-12-23

### Added
- **Stage 6 Full Re-Extraction** - Complete docs-only corpus re-extraction using Gemini Flash
  - 12,002 documents processed
  - 88,322 entities extracted
  - 17,329 relationships extracted
  - 30,041 unique entities in entity_registry
- **FIX-007 Predicate Consolidation** - Reduced predicate sprawl from 3,303 to 1,501 distinct predicates (-54.6%)
- **Code↔Docs Bridge** - Full integration between documentation and code artifacts
  - 16,820 code artifacts exported
  - 6,453 doc→code links created
  - 241 entity→code links (API_MESSAGE, MODULE types)
- **AGE Graph Sync** - Apache AGE integration for graph database
  - 5,464 stub nodes (Person, Organization, CodeArtifact, Doc)
  - 6,463 edges (MENTIONS, CODE_REF)
- **PostgreSQL-based Fuseki Rebuild** - New rebuild pipeline from authoritative PostgreSQL data
- **Quality Gates** - Automated verification for production deployments
  - No http://regen.network/ URIs
  - No ontology# types/predicates
  - No self-referential triples

### Fixed
- **Predicate Format Violations** - All predicates now conform to `^[a-z0-9_]+$` pattern
- **HTTP URI Issues** - Eliminated all http:// URIs in favor of https://
- **Self-Referential Relationships** - Removed relationships where subject = object
- **AGE Sync Compatibility** - Fixed multi-label MERGE syntax and batch execution issues

### Changed
- **Extraction Model** - Switched from GPT-4.1-mini to Gemini Flash (99.4% vs 98.4% pass rate)
- **PostgreSQL as Authoritative Store** - Fuseki now rebuilt from PostgreSQL rather than dual-write
- **Entity Deduplication** - Improved from 70% to higher dedup rates with semantic matching

### Deployment
- **Production Fuseki**: 165,619 triples deployed to /koi endpoint
- **Staging Fuseki**: 165,619 triples deployed to /koi-staging endpoint
- **All quality gates passing**

## [2.3.0] - 2025-09-28

### Added
- **Content-Based Deduplication** - Web pages are now deduplicated based on content hash to prevent reprocessing unchanged content
- **Event Filtering** - Heartbeats and test data are now filtered at the Event Bridge entry point
- **GitHub Activity Sensor** - Added github_activity sensor to pipeline metadata for comprehensive GitHub tracking
- **URL-Based Versioning** - Web pages tracked by URL with superseded_at timestamps for version control

### Fixed
- **White Text on White Background** - Fixed text visibility issues in Transformation Provenance UI
- **Test Data in Production** - Cleaned up 1,683 test/heartbeat entries from koi_content table
- **UI Flash Issue** - Removed hardcoded examples that briefly appeared before real data loaded
- **Duplicate Processing** - Fixed unnecessary reprocessing of unchanged web page content

### Changed
- **Removed Sensor Status Tab** - Consolidated sensor information into Overview tab
- **Pipeline Metadata API** - Enhanced filtering to exclude test/heartbeat/demo data from RID listings
- **UI Loading States** - Added proper loading spinners instead of placeholder examples

### Cleaned
- **Database Cleanup** - Removed 1,794 heartbeat memories from koi_memories table
- **Test Files** - Removed temporary test scripts from koi-processor directory

## [2.2.0] - 2025-09-25

### Added
- **Source Tracking for Daily Posts** - All daily posts now include source provenance to prevent AI hallucination
- **Podcast Generation** - Full audio podcast generation using OpenAI TTS with automatic chunking for long content
- **Markdown Export for NotebookLM** - Weekly digests can be exported as markdown for NotebookLM integration
- **Audio Player in Dashboard** - Integrated HTML5 audio player with download capabilities
- **Loading Feedback** - Spinner and status messages during podcast generation
- **Nginx Configuration** - Added podcast_audio location block for proper file serving

### Fixed
- **Fake Username Generation** - LLM no longer invents fake usernames like "@EcoWarrior123"
- **502 Bad Gateway Error** - Fixed nginx proxy configuration for public URL access
- **Audio File Serving** - Properly encoded URLs and added Accept-Ranges header for streaming
- **Markdown Download Links** - Fixed base path issues for downloads through public URL

### Changed
- **Podcast Duration** - Expanded from 1 minute to 8+ minutes with comprehensive content
- **Daily Curator Prompts** - Added strict rules to prevent fake data generation
- **Script Generation** - Now uses full digest content including brief_content and podcast_script fields

## [2.1.0] - 2025-09-24

### Added
- **Comprehensive Regen Ledger Integration** - Full on-chain data fetching from all Cosmos SDK modules
- **LLM-Enhanced Content Curation** - AI-powered daily and weekly digest generation
- **On-Chain Activity Tracking** - Weekly digests now include ledger statistics and network metrics
- **Discourse Sensor Integration** - Forum posts properly indexed with published_at dates

### Fixed
- **Daily Generation 502 Error** - Dashboard now properly loads environment variables
- **Weekly Generation Timeout** - Increased timeout to 5 minutes for comprehensive data fetching
- **GitHub URL Malformation** - Cleaned temporary directory paths from sensor URLs
- **Forum Post Dating** - Fixed published_at extraction for all discourse content
- **Post Truncation** - Daily posts now display full content in dashboard

### Changed
- **Daily Curator** - Now uses `daily_curator_llm.py` with OpenAI integration
- **Weekly Curator** - Now uses `weekly_curator_llm.py` (not in this export) with comprehensive ledger data
- **Dashboard Timeout** - Increased subprocess timeout from 180s to 300s

## [2.0.0] - 2025-09-09

### Added
- **RID-based Deduplication System** - Prevents duplicate content ingestion using Resource Identifiers
- **Version Control for Updates** - UPDATE events create new versions with full audit trail
- **Isolated KOI Tables** - New `koi_memories` and `koi_embeddings` tables separate sensor data from scraped content
- **Event Bridge v2** (`koi_event_bridge_v2.py` (not in this export)) - Complete rewrite with deduplication and versioning
- **Database Migration 003** - Creates isolated tables with version tracking
- **Comprehensive Documentation** - Complete README with installation, configuration, and troubleshooting
- **Setup Script** (`scripts/setup.sh`) - Automated environment setup
- **Integration Test Suite** (`scripts/test_pipeline.py` (not in this export)) - End-to-end pipeline testing
- **Environment Configuration** (`.env.example`) - Template for all configuration options
- **BGE-large-en-v1.5 Support** - Production-grade 1024-dimensional embeddings
- **MCP Server Integration** - TypeScript implementation for semantic search
- **CAT Receipt System** - Complete provenance tracking for transformations
- **Permission Filtering** - Agent-specific content access control

### Changed
- **Event Processing Logic** - Now checks for existing RIDs before processing
- **Database Schema** - Added version tracking and superseded_at timestamps
- **Embedding Storage** - Support for multiple embedding dimensions (768, 1024, 1536)
- **Error Handling** - More robust with graceful fallbacks
- **Logging** - Enhanced with color output and structured messages
- **API Responses** - Include version information and previous_version_id

### Fixed
- **Duplicate Content Issue** - RID-based deduplication prevents duplicate ingestion
- **Update Event Handling** - Properly creates new versions instead of duplicating
- **Memory Leaks** - Improved connection pooling and resource cleanup
- **BGE Server Compatibility** - Handles both "text" and "input" field formats
- **Database Connection Issues** - Better error handling and retry logic

### Security
- **Environment Variables** - All sensitive configuration moved to .env file
- **SQL Injection Prevention** - Parameterized queries throughout
- **Input Validation** - Pydantic models for all API inputs

## [1.0.0] - 2025-09-07

### Added
- Initial KOI Event Bridge implementation
- BGE mock server for testing
- Basic PostgreSQL integration
- Simple chunking algorithm
- FastAPI-based REST API
- Agent memory format support

### Known Issues (Fixed in v2)
- No deduplication mechanism
- UPDATE events not properly handled
- Mixed data sources in same tables
- No version tracking

## [0.1.0] - 2025-09-01

### Added
- Project initialization
- Basic project structure
- Research documentation
- Initial prototypes

---

## Migration Guide

### From v1.0.0 to v2.0.0

1. **Database Migration Required**
   ```bash
   psql -U postgres -d eliza < migrations/003_create_isolated_koi_tables.sql
   ```

2. **Environment Variable Changes**
   - Add `USE_ISOLATED_TABLES=true` to your .env file
   - Review new options in .env.example

3. **Code Changes**
   - Replace `koi_event_bridge.py` with `koi_event_bridge_v2.py` (not in this export)
   - Update any custom integrations to handle version fields in responses

4. **Testing**
   - Run `scripts/test_pipeline.py` (not in this export) to verify the upgrade
   - Check deduplication is working with duplicate RID tests

### Rollback Instructions

If you need to rollback to v1.0.0:
1. Set `USE_ISOLATED_TABLES=false` in .env
2. Use `koi_event_bridge.py` instead of v2
3. The isolated tables can remain - they won't be used

---

## Upcoming Features (Planned)

- [ ] Batch processing for bulk imports
- [ ] Automatic sensor health monitoring
- [ ] Content change detection with diff generation
- [ ] Multi-model embedding support
- [ ] GraphQL API for complex queries
- [ ] Distributed processing with message queues
- [ ] Real-time WebSocket event streaming
- [ ] Prometheus metrics and Grafana dashboards
---

## [2.0.0] - 2025-12-20

### Added - Hybrid Graph-Boosted RAG

**Major Feature: Entity-Aware Knowledge Retrieval**

The KOI Query API now implements a sophisticated Hybrid Graph-Boosted RAG system that combines:
- **Vector Search**: Semantic similarity using BGE-1024 embeddings
- **Entity/Graph Search**: Knowledge graph traversal via entity-chunk links
- **Keyword Search**: Full-text search with PostgreSQL tsvector

**Key Components:**

1. **Entity Registry** (koi_entity_registry)
   - Canonical entity storage with type information
   - Semantic deduplication (Tier 1: Exact, Tier 2: Embedding similarity)
   - 12,985 unique entities, 43,430 mentions, 70.10% dedup rate

2. **Entity-Chunk Links** (koi_entity_chunk_links)
   - 614,021 entity-memory associations
   - Enables graph-based document retrieval
   - Source: PostgreSQL entity extraction pipeline

3. **Weighted Average Fusion** (adaptive-features.ts)
   - Formula: 0.6*vector + 0.2*entity + 0.2*keyword + 0.15*entity_boost 
   - Entity boost applied when document appears in both vector and entity results

4. **Source-Diversity Sampling**
   - Prevents high-volume sources (GitHub) from drowning out high-value sources (Homepage)
   - Partitions entity results by source type: web, github, gitlab, other
   - Domain-level diversity for web: regen.network, forum, registry, guides
   - Top 25 from each non-web source, top 10 from each web domain

**Performance:**
- Quality: 99.7% (up from 62%)
- Entity extraction: 3,497 documents processed
- Zero type collisions in entity registry

### Changed
- Entity search now uses source-diversity sampling instead of simple LIMIT
- Fusion algorithm changed from RRF to Weighted Average for better score discrimination
- Added superseded_at IS NULL filter to all search queries

### Technical Details
See docs/HYBRID_RAG_ARCHITECTURE.md (not in this export) for complete technical documentation.
