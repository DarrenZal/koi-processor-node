# Personal Sensor Integration

This document defines the contract between local personal sensors (`koi-sensors`) and the personal ingest/query API (`koi-processor`).

## Repositories in the Local Stack

- `koi-sensors`: Collects personal data (`email`, `claude_sessions`) and sends/ingests it.
- `koi-processor`: Stores/query personal data (`koi_memories`, `email_metadata`, `session_*` tables).
- `personal-koi-mcp`: MCP tool surface used by Claude Code.

## Canonical Sensor Identifiers

Use these source IDs consistently:

- `email-sensor`
- `claude-sessions-sensor`
- `substack-corpus-backfill` — Substack corpus (Johar/Ruddick/Bauwens); RID `substack-corpus:<feed_slug>:<post_slug>`.
- RSS/Atom feed sensor (`rss-<slug>` access sources)
- `research-paper-sensor` — arXiv/author paper corpus
- `proton-email-sensor`

Notes:

- Email documents are stored in `koi_memories` with `source_sensor='email-sensor'`.
- Claude sessions are stored in dedicated `session_ingestion_log`, `session_chunks`, and `session_tool_usage` tables.
- **Deployment topology:** run the scheduled sensor jobs (substack, research-author and the others) from a dedicated checkout pinned to the deployed branch, not from a checkout you develop in. To update sensor code, commit to that branch and pull in the dedicated checkout.

## Required Migration for Email Metadata

The email sensor needs `migrations/033_email_sensor_tables.sql`. It is part of the chain
`scripts/run_migrations.sh` applies to a new database
([fresh-install-migrations.md](../operations/fresh-install-migrations.md)). On an existing
database, apply that one file and record it in the database's ledger, as
[Existing databases](../operations/fresh-install-migrations.md#existing-databases)
describes. It creates:

- `email_metadata`
- supporting indexes and constraints

## Personal Session Endpoints

These endpoints are served by `api/personal_ingest_api.py`:

- `POST /search-sessions` — semantic search over session chunks
- `GET /session-stats` — index statistics
- `GET /session-tools` — query by tool/MCP usage
- `GET /session-files` — query by files accessed

No route lists the sessions that mention an entity. `GET /entity/{uri}/mentioned-in` and
`POST /entities/mentioned-in` return vault documents only: they keep
`orn:obsidian.entity:`, `vault:` and bare vault-path RIDs (the single-entity route also
`orn:obsidian.document:`) and leave out `claude-session:` and other sensor RIDs.
Session links are `document_entity_links` rows whose `document_rid` starts with
`claude-session:`. Query them with `psql`:

```sql
SELECT document_rid, mention_count FROM document_entity_links
WHERE entity_uri = '<entity uri>' AND document_rid LIKE 'claude-session:%';
```

or through `POST /sql`, which takes the same `SELECT` without the `;` as
`{"sql": "...", "params": [...]}` with `$1` placeholders and runs it on a read-only role
(set up as in the quickstart's
[Access control](../operations/quickstart-personal-koi.md#7-access-control) section).

## Session-Entity Knowledge Graph Integration

The Claude sessions sensor extracts entities from session transcripts and links them to the personal knowledge graph via the `/ingest` endpoint.

**Flow**: Session transcript → chunk + embed → extract entities (OpenAI gpt-4o-mini) → `POST /ingest` → entity resolution (4-tier) → `document_entity_links` rows with `claude-session:` RIDs. Entity notes do not gain `mentionedIn` links to sessions: the mentioned-in routes return vault documents only (see above).

**Fields `/ingest` ignores**: the sensor also sends `replace_existing` and
`link_existing_only`. `/ingest` has never implemented either: its request model
(`IngestRequest` in `api/personal_ingest_api.py`) does not declare them, so the API drops
both and logs a warning. Reprocessing a session does not clear its old links, and Tier 3
still creates new entities when the sensor asks to link existing ones only.

**Config** (`koi-sensors/sensors/claude_sessions/config.personal.yaml`):
- `entity_extraction.enabled` — gate all extraction
- `entity_extraction.link_existing` — gate `/ingest` call
- `entity_extraction.extract_new` — sets the `link_existing_only` field (inverted), which `/ingest` ignores
- `entity_extraction.model` — LLM for extraction (default: `gpt-4o-mini`)
- `entity_extraction.max_chunks` — chunks sent to LLM (default: 5)

**Privacy**: Text is redacted before LLM call (`_redact_for_extraction()`) — strips env vars, API keys, connection strings, private keys, base64 blobs.

**Failure safety**: Extraction returns `(entities, success)`. On failure (`success=False`), the sensor makes no `/ingest` call, so existing links stay. Validated results (including a valid empty `[]`) are sent to `/ingest`, which adds links and never removes old ones.

**Known limitation**: Redaction regex does not handle escaped quotes inside quoted env values. Accepted as impractical edge case.

**Required migration**: `migrations/055_session_schema_governance.sql`, applied the same
way as 033 above.

## Local Verification Checklist

1. Email sensor inserts rows with `source_sensor='email-sensor'`.
2. `email_metadata` rows exist and link to `koi_memories.id`.
3. Claude sessions sensor populates `session_ingestion_log` and `session_chunks`.
4. Session endpoints return results for indexed sessions.
5. After entity extraction: `document_entity_links` contains rows with `claude-session:` RIDs.
6. `GET /entity/{uri}/mentioned-in` lists vault documents only; the session links from item 5 do not appear there.
