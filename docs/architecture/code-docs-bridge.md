# Code ↔ Docs Bridge (AGE + Postgres/RDF)

## Purpose

RegenAI maintains two complementary representations:

- **Semantic KG (PostgreSQL → Fuseki/RDF):** people, orgs, claims, domain concepts, governance artifacts, and *semantic* relationships.
- **Code graph (Apache AGE + tree-sitter):** symbols, files, AST-derived relationships and structural connections.

The bridge makes these systems **joinable**, so documentation can resolve to concrete code artifacts and the query layer can traverse both.

The bridge scripts under `scripts/code_bridge/` (not in this export) and the docstring
pipeline's batch coordinator are left out of this export. This page describes them as they
run in the private repository; the tables, migrations and alias file it names are published.

## Bridge Contract

### Canonical identifiers

All joins flow through **stable string identifiers** shared across systems:

- `repo_key`: `<host>/<org>/<repo>` (e.g. `github.com/regen-network/regen-ledger`)
- `file_path`: repo-relative path (e.g. `x/ecocredit/keeper/keeper.go`)
- `symbol`: best-effort symbol name (e.g. `MsgCreateBatch`, `BasketKeeper`)
- `code_uri`: canonical artifact ID (stored as `TEXT`)

Recommended `code_uri`:
- Initial (available now): `rid://code/...` from `code_entity_provenance.entity_rid`
- Future (optional): `https://regen.network/koi/code/...` if you want web-safe URIs

### Tables

Two Postgres tables form the bridge:

1) `koi_code_artifacts`
- One row per canonical code artifact.
- Can optionally include `(age_graph, age_id)` for direct AGE mapping.

2) `koi_doc_code_links`
- Many-to-many links from documents (`koi_memories.rid`) to `code_uri`.
- Populated by a “doc→code linker” job (regex/symbol resolution, or LLM assisted).

## Migrations

- `migrations/024_code_docs_bridge.sql`

## Initial Population (Code → Bridge)

Use the provenance view created by `migrations/add_code_graph_provenance.sql`:

- `code_entity_provenance` is built from CAT receipts (`koi_transformation_receipts`).

Script:

`scripts/code_bridge/export_code_artifacts.py` (not in this export).

This upserts `code_entity_provenance` into `koi_code_artifacts`.

## Linking (Docs → Code)

Recommended minimal linker behavior (future job):

- For each doc (`koi_memories`), find mentions of:
  - Cosmos modules (`x/<name>`)
  - protobuf message types (`Msg[A-Za-z0-9]+`)
  - keeper names (`<Something>Keeper`)
  - full import paths (`github.com/<org>/<repo>/...`)
- Resolve to `koi_code_artifacts.code_uri` by matching `(repo_key, file_path)` or `(repo_key, symbol)`.
- Insert into `koi_doc_code_links`.
- Optionally write `entity_registry.metadata.code_uri` for FIX-005 types (MODULE/API_MESSAGE/KEEPER) when a confident match is found.

### Initial linker implementation

Script (regex-based, high-precision):

`scripts/code_bridge/link_docs_to_code.py` (not in this export), with `--dry-run` first.

Behavior:
- Links **symbol mentions** when the symbol is globally unique (or uniquely resolvable by repo context).
- Links **file paths** only when a full org/repo path is present *and* maps to a single artifact.
- Uses the same docs-only corpus filter as Stage 6.

## Entity-Level Linking (Semantic → Code)

After Stage 6, link semantic entities (MODULE / KEEPER / API_MESSAGE) to code artifacts:

`scripts/code_bridge/link_entities_to_code.py` (not in this export), with
`--types MODULE,KEEPER,API_MESSAGE --alias-file data/code_bridge_module_aliases.json`.

This writes `metadata.code_uri` + `link_confidence` + `link_method` into `entity_registry`.

## Stub Sync (AGE ← Postgres)

Stub nodes and edges are synchronized into AGE for single-query performance:

`scripts/code_bridge/sync_stubs_to_age.py` (not in this export), with `--dry-run` first.

Stub edges:
- `(:Stub:Doc)-[:MENTIONS]->(:Stub:CodeArtifact)`
- `(:Stub:*)-[:CODE_REF]->(:Stub:CodeArtifact)`

The sync is **mark/sweep** using `sync_run_id`.

## Query Federation Pattern

1) Semantic KG answers the “what/why” and yields `code_uri` values (directly on entities or via `koi_doc_code_links`).
2) Query layer fetches code details:
   - via Postgres `koi_code_artifacts` (fast; no AGE call)
   - optionally via AGE by `(age_graph, age_id)` when available

## Docstring Semantic Extraction (v3.3.0)

In addition to the structural bridge above, code docstrings are now routed through the LLM semantic extraction pipeline. This closes the gap where domain knowledge in comments (references to standards, protocols, architectural concepts) was invisible to the semantic KG.

**Pipeline:** `scripts/extract_docstring_semantics.py` (not in this export)
1. Tree-sitter extracts entities per file (same extension filter as `scripts/load_to_staging.py` (not in this export))
2. `src/core/docstring_filter.py` filters trivial/synthetic docstrings, aggregates into 3000-char batches
3. OpenAI extractor with domain-tuned prompt (`source_type="code_docstring"`)
4. Quality pipeline + EntityResolver deduplication
5. Provenance stored in `koi_code_docstring_extractions` (FK cascade to `koi_kg_extractions`)
6. Shadow `koi_memories` rows (`source_sensor='code_docstring'`) for analytics visibility

**Idempotency:** Deterministic RID from `(repo, file_path, file_hash, batch_index, prompt_version, model)`. Re-running on unchanged files is a no-op.

**Production Results (2026-02-19):**

| Repo | Files | Batches | Entities | Relationships |
|------|-------|---------|----------|---------------|
| koi-processor | 213 | 232 | 1,112 | 66 |
| regen-ledger | 306 | 935 | 9,396 | 169 |
| **Total** | **519** | **1,167** | **10,508** | **235** |

Top entity types: API_MESSAGE (5,565), CONCEPT (3,463), TECHNOLOGY (960), PROCESS (190), MODULE (92), KEEPER (36), CREDIT_CLASS (27).

## Notes

- Stage 6 semantic re-extraction can remain **docs-only** while still linking into the code graph through this bridge.
- Keep the bridge table schema stable; enrich it incrementally (AGE ids, commit-aware versioning) when needed.
