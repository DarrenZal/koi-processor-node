# Hybrid Graph-Boosted RAG Architecture

## Overview

The KOI Query API implements a Hybrid Graph-Boosted Retrieval-Augmented Generation (RAG) 
system that combines three search modalities:

1. **Vector Search** - Semantic similarity using BGE-1024 embeddings
2. **Entity/Graph Search** - Knowledge graph traversal via entity-chunk links
3. **Keyword Search** - Full-text search using PostgreSQL tsvector

## Key Components

### 0. Content Deduplication (3-Layer Protection)

Prevents duplicate content from appearing in search results:

**Query-Level (koi-query-api.ts):**
- `md5(content)` PARTITION BY in all search CTEs
- Groups identical content, keeps highest-scoring instance
- Safety net for historical duplicates

**Storage-Level (`src/core/koi_event_bridge_v2.py` (not in this export)):**
- `content_hash` column (SHA-256) on koi_memories
- Global check: rejects new entries if content_hash already exists
- Catches cross-RID duplicates from sensors

**Sensor-Level (gitlab/github sensors):**
- Canonical RIDs without run-specific info (no temp dir names)
- Format: `gitlab_regen-public-docs_WhitePaper.tex` (not `gitlab_sensor_xyz123/...`)
- Prevents duplicates at the source

### 1. Entity Registry (koi_entity_registry)

Stores canonical entities with deduplication:
- Tier 1 (Exact): B-Tree index matching on entity_name_lower
- Tier 2 (Semantic): pgvector cosine similarity (0.92 threshold)
- Tier 3: Insert new entity if no match

Current stats: 12,985 unique entities, 43,430 mentions, 70.10% dedup rate

### 2. Entity-Chunk Links (koi_entity_chunk_links)

Maps entities to document chunks:
- 614,021 entity-memory associations
- B-Tree index for fast entity matching
- Enables graph-based document retrieval

### 3. Full-Text Search (PostgreSQL tsvector)

**Migration:** `migrations/025_add_content_tsv_fts.sql`
- `content_tsv` tsvector column with weighted fields
- Weight A: title (highest priority)
- Weight B: text body
- GIN index for fast full-text queries
- Trigger auto-updates tsvector on INSERT/UPDATE

**Query Strategy (koi-query-api.ts):**
- Strict AND query: `to_tsquery('english', 'claims & engine')`
- Relaxed OR query: `to_tsquery('english', 'claims | engine')`
- Strict results prioritized via `ORDER BY match_type, rank DESC`
- Lexeme-aware prefix matching filters OR results

**Backfill:** `scripts/backfill-fts.sql` (batch 5K rows, CONCURRENTLY index)

### 4. RID Normalization for Fusion

**Problem:** Entity search returns chunks (`UUID#chunk14`), keyword search returns base docs (`UUID`). Without normalization, they don't merge in fusion.

**Solution:** `normalizeRidForFusion()` strips `#chunk\d+` suffix before merging:
```typescript
function normalizeRidForFusion(rid: string): string {
  return rid.replace(/#chunk\d+$/, '');
}
```

### 5. Weighted Average Fusion (adaptive-features.ts)

Fusion weights:
- VECTOR_WEIGHT = 0.6 (semantic relevance)
- ENTITY_WEIGHT = 0.2 (graph/entity match)
- KEYWORD_WEIGHT = 0.2 (exact text match)
- ENTITY_BOOST = 0.15 (bonus for entity overlap)

Formula: score = (vectorScore * 0.6) + (entityScore * 0.2) + (keywordScore * 0.2) + entityBoost

### 6. Source-Diversity Sampling

Prevents any single source from dominating entity search results:

**Source Types:**
- web (orn:web.page:*)
- github (regen.github:*)
- gitlab (regen.gitlab:*)
- other

**Web Domains (for diversity within web):**
- main (regen.network)
- forum (forum.regen.network)
- registry (registry.regen.network)
- guides (guides.regen.network)

**Sampling Strategy:**
- Non-web sources: Top 25 from each source type
- Web sources: Top 10 from each domain (up to 50 total)
- Guarantees homepage (regen.network) is included in boost candidates

## Query Flow

1. Parse Query: Extract words >= 3 chars, build n-gram patterns
2. Parallel Search: Execute vector, entity, and keyword searches concurrently
3. Merge Results: Combine by document ID, track scores from each source
4. Apply Boost: Add 0.15 entity boost for overlapping documents
5. Sort and Return: Order by final weighted score

## Design Rationale

### Why Source-Diversity Sampling?

Without diversity sampling, high-volume sources like GitHub (976 matches) 
dominate entity search results, pushing out high-value sources like the 
homepage (8 matches). This prevents the homepage from getting entity boost
even though it contains relevant entities.

### Why Weighted Average over RRF?

Reciprocal Rank Fusion (RRF) compresses scores, making it hard to distinguish
between results. Weighted Average preserves score discrimination while still
combining multiple signals.

### Why Entity Boost?

Documents that appear in BOTH vector search (semantically relevant) AND 
entity search (explicitly mentions query entities) are highly likely to be
the best answers. The 0.15 boost rewards this overlap.

## 7. Graph Expansion PoC (Log-Only)

**Status:** Deployed as log-only analysis (no ranking impact)

Analyzes potential recall gains from 1-hop relationship traversal in `koi_relationships`.

### How It Works

1. Extract matched entity names from entity search results
2. Filter to multi-token names (>= 2 words OR >= 8 chars) to reduce noise
3. Look up entities in `entity_registry` using `normalized_text` index
4. Find 1-hop neighbors via `koi_relationships` (confidence >= 0.5, occurrence_count >= 2)
5. Count how many new docs the neighbors would add
6. Log analysis (no ranking change)

### Key Function

```typescript
async function get1HopNeighbors(
  matchedEntityNames: string[],  // normalized (lowercased)
  maxPerEntity: number = 5,
  totalLimit: number = 15
): Promise<{ neighbor_uri, neighbor_name, neighbor_type, via_predicate, confidence }[]>
```

### Filters and Guards

- **Multi-token filter**: Only entities with >= 2 words or >= 8 chars are used as expansion seeds
  - Prevents single-token names like "gregory" from exploding to 1000+ docs
- **High-degree guard**: Skips doc count query if neighbors > 10
  - Avoids expensive COUNT on high-connectivity entities
- **Quality thresholds**: Only relationships with confidence >= 0.5 and occurrence_count >= 2

### Sample Output

```
[GraphExpansion] Query: "Gregory Landua"
[GraphExpansion] Matched 1 entities: gregory landua
[GraphExpansion] Expanded to 5: Regen Network (ORGANIZATION), RND PBC (ORGANIZATION)
[GraphExpansion] Predicates: represents, associated_with, mentions, attended
[GraphExpansion] Would add 1667/1682 new docs (60 direct)
```

### Enabling

Set `DEBUG_GRAPH_EXPANSION=true` in `ecosystem.hybrid.config.js`

---

## Future Improvements

### Near-Term (if expansion affects ranking)

1. **URI Alignment** - Reconcile `entity_uri` (gaia.ai format) with `fuseki_uri` (regen.network format)
   - Add mapping table: `entity_uri_map(gaia_uri, fuseki_uri, confidence, source)`
   - Enables deterministic FK joins instead of string matching
   - Reduces false positives from name collisions across entity types

2. **Predicate Filtering** - Filter expansion by predicate type
   - High-value: `represents`, `founded`, `member_of`
   - Lower-value: `mentions`, `associated_with`

### Medium-Term

3. **Graph Proximity Scoring** - Add `graph_proximity` score to fusion weights
   - Weight neighbors by relationship confidence and hop distance
   - Formula: `graph_score = sum(confidence * decay^hop_distance)`

4. **Multi-Hop Traversal** - Extend beyond 1-hop
   - 2-hop for high-confidence paths only
   - Cap total expansion to prevent explosion

5. **Predicate Consolidation** - Further reduce predicates (1,501 → ~100-200)
   - Group synonymous predicates under canonical forms

---

## Files Reference

- koi-query-api.ts - Main API with entity search, keyword search, content dedup, graph expansion
- adaptive-features.ts - Fusion algorithms, RID normalization
- tests/adaptive-features.test.ts - Unit tests for fusion
- src/core/koi_event_bridge_v2.py (not in this export) - Storage-level dedup
- scripts/archive/entity_chunk_linker.py (not in this export) - Batch entity linking
- scripts/backfill_entity_registry.py (not in this export) - Registry population
- scripts/backfill-fts.sql - FTS backfill script
- migrations/023_content_hash_dedup_index.sql - Content hash schema
- migrations/025_add_content_tsv_fts.sql - Full-text search schema
- tests/test_keyword_search_fts.py - FTS integration tests

---

*Last Updated: 2025-12-24*
