# Fresh install: migrations

The migration chain grew alongside live databases and was not written to build a new
one from empty. `scripts/run_migrations.sh` has a **personal profile** that builds a
new personal-KOI database in one run with no errors. It never edits an applied
migration: live databases checksum them.

## Procedure

Stop the KOI service first if one is configured to use this database: at startup the API
creates tables, and a database it has touched is no longer empty.

```bash
createdb personal_koi        # exactly this name: it selects the personal profile, and 086, 103, 104 require it
psql personal_koi -c 'CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
DATABASE_URL=postgresql://<user>@localhost:5432/personal_koi bash scripts/run_migrations.sh
```

Run it once, on an empty database. On a database named `personal_koi` the runner only
builds from empty. Before it writes anything it checks, and refuses unless all hold:

- psql is version 10 or later;
- the database has no `koi_migrations` (created by migration 052) and no
  `koi_schema_migrations` (the ledger of federation peers built by
  `scripts/federation/setup-node.sh`);
- no `koi_fresh_install_in_progress` marker is left from an earlier run;
- the `public` schema has no table, view, materialized view or foreign table, counted from
  `pg_catalog` (so tables the connecting role cannot read still count);
- `044_koi_memories_rag` is present (see "Deferred" below).

Any check it cannot answer counts as a refusal. Every psql call uses `-X`, and personal
runs set `search_path=public`. Its first write creates the marker table
`public.koi_fresh_install_in_progress`, after checking once more that `public` is empty.
It drops the marker at the end of the run, and only if this run created it.

If a run stops part-way (a failed migration, a killed process, a lost connection), drop
and recreate the database before running again. The runner does not resume: it refuses a
database that is no longer empty, and it refuses one that still holds the marker
("a previous fresh install did not finish").

Measured 2026-11-02 on this export, on PostgreSQL 14.15 with pgvector 0.8.0. A new
database, one runner run: 142 applied, 9 skipped by the profile, 0 failed, 0 files with
statement errors, 0 `ERROR` lines in the output. Afterwards the API started with
`/health` healthy, and `/tasks/stats`, `POST /tasks/ingest` and `GET /entities` worked.

## Existing databases

No runner applies migrations to an existing personal database:

- `scripts/run_migrations.sh` refuses a `personal_koi` that is not empty. For other
  databases it refuses `koi_migrations` (bypass: `KOI_ALLOW_LEGACY_RUNNER=1`) and
  `koi_schema_migrations` (no bypass).
- `scripts/run_migrations_with_backup.sh` refuses any database named `personal_koi`, and
  refuses `koi_migrations` (bypass: `KOI_ALLOW_LEGACY_RUNNER=1`) and `koi_schema_migrations`
  (no bypass).
- `scripts/federation/lib.sh` (used by `setup-node.sh`) migrates
  only databases it built, which track migrations in `koi_schema_migrations`. It refuses a
  database left with the fresh-install marker, and one that has `koi_migrations` or
  `koi_net_events` but no `koi_schema_migrations` (bypass: `KOI_ALLOW_LEGACY_RUNNER=1`).

`KOI_ALLOW_LEGACY_RUNNER=1` re-runs files the database's own ledger already recorded; use
it only on a database you can rebuild. To add a new migration to an existing database,
apply that one file and record it in the database's ledger:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/NNN_name.sql
```

Record it in `koi_migrations` (baseline manifests: `scripts/stamp_baseline.py`) or, on a
federation peer, let `scripts/federation/setup-node.sh` apply and record it in
`koi_schema_migrations`.

## What the personal profile does

The personal profile is chosen when the database is named exactly `personal_koi`
(`KOI_INSTALL_PROFILE=personal` with any other name is refused). `KOI_ALLOW_LEGACY_RUNNER` does not
change it. Files are applied in byte order (`LC_ALL=C`), and the run stops at the first
file that fails outright.

- **Skipped, RegenAI-era.** These target the RegenAI `eliza` schema (agents, koi_content,
  koi_transformation_receipts, the Milestone B dashboards, Jena, code-graph provenance)
  and fail on a personal database: `002`, `005`, `006_fix_cat_receipts`, `007`, `010`,
  `011`, `013`, `015` and `add_code_graph_provenance`. They are not recorded as applied.
- **Replaced.** The private repository's `083_mediawiki_redirect_resolver`, which is not in
  this export, refuses any database user except the maintainer's own role.
  `083a_mediawiki_redirect_resolver_fresh_install` carries its statements without that
  guard. It creates the view only when it is absent, so it never drops one (or the grants
  on it).
- **Deferred.** `033_email_sensor_tables` indexes `koi_memory_chunks`, which `044`
  creates, so it runs right after `044_koi_memories_rag`. The runner stops if `044` is
  missing.
- **Fresh-install migrations** (`*_fresh_install.sql`). Each acts only on a database
  named `personal_koi` while the runner is building it from empty: the runner sets
  `koi.fresh_install=on` and creates the `public.koi_fresh_install_in_progress` marker
  table for the duration of the run. Applied any other way, for example `psql -f` against
  a live database, with or without that setting, each one prints "nothing to do" and
  changes nothing, because the marker is absent. Line 1 of each sets `ON_ERROR_STOP`, so a
  psql older than 10, which does not know `\if`, stops instead of running the body. The
  other runners skip them: `scripts/run_migrations.sh` outside the personal profile,
  `scripts/federation/lib.sh` (it applies files with psycopg2, which cannot run their psql
  meta-commands), and `scripts/run_migrations_with_backup.sh` in every case.

| File | Why |
|---|---|
| `035a_entity_registry_aliases_text_array_fresh_install` | 036's JSONB→TEXT[] conversion uses a subquery PostgreSQL rejects in `USING`, and its view rewrite produces a one-argument `array_length`. 035a converts first, so 036 takes its own "already TEXT[]" branch. |
| `038a_koi_kg_extractions_fresh_install` | `039` needs `koi_kg_extractions`, from the skipped `013` (DDL copied). |
| `042a_koi_code_artifacts_run_id_fresh_install` | `024` creates `koi_code_artifacts` without `extraction_run_id`, which `043` indexes. |
| `044a_koi_memories_privacy_and_extraction_records_fresh_install` | Personal code uses `koi_memories.is_private`/`access_source` (from the skipped `015`) and `koi_extraction_records` (from the skipped `010`). DDL copied. |
| `061a_startup_tables_fresh_install` | `062`, `086` and `132` alter `document_entity_links` and `entity_rid_mappings`, which the API creates at startup. Same DDL as the startup code, which is unchanged. |
| `083a_mediawiki_redirect_resolver_fresh_install` | See "Replaced". |
| `123a_entity_registry_revoked_at_fresh_install` | `124` filters on `entity_registry.revoked_at`, which nothing created; live databases have it as `timestamp with time zone`, nullable, no default. |
| `139a_entity_registry_timestamps_fresh_install` | The API orders `GET /entities` and `GET /entity-search` by `entity_registry.created_at` and reads `updated_at`, but the chain creates `first_seen_at` and `last_seen_at` instead. Without 139a both routes return 500 on a fresh database. Live databases have both columns as `timestamp without time zone`, nullable, `DEFAULT now()`. |
| `139b_entity_registry_embedding_nullable_fresh_install` | `020` declares `entity_registry.embedding` `NOT NULL`, live databases allow null, and `store_new_entity` writes only `embedding_3072`, so `POST /ingest` could not create an entity on a fresh build. |

## Proofs, 2026-10-31

Each on a throwaway PostgreSQL 14.15 cluster.

- New database, one run: 141 applied, 10 skipped by the plan, 0 failed, 0 `ERROR` lines. The 10 were the nine RegenAI-era files and 083. This export does not carry 083, so here the plan skips 9, and since 139a it applies 142.
- A schema-only copy of a live database, restored as `personal_koi`: the runner refuses
  (`koi_migrations` present) and the schema dump hash is unchanged. Each fresh-install
  file applied by hand, with and without `koi.fresh_install=on`, left the hash unchanged.
- A `personal_koi` built the way `scripts/federation/setup-node.sh` builds a peer: the
  runner refuses before writing anything (`koi_schema_migrations` present), and the
  schema dump hash is unchanged.
- A leftover marker: the runner refuses ("a previous fresh install did not finish").
- A fresh run killed with SIGKILL part-way: the marker stays, and the next run refuses.
- A schema-only copy of a live database, restored as `personal_koi`: `lib.sh` refuses
  before writing (`koi_migrations` present, no `koi_schema_migrations`), and the schema dump
  hash is unchanged.

## Compared with the previous behaviour

A new database built by the previous runner (all files applied, errors and all) was
compared with one built by the personal profile. Every object the profile no longer
creates either has no users in `api/`, `src/` or `scripts/`, or is used only by
RegenAI-stack modules that the personal API does not import
(`src/core/koi_permissions_api.py`, `src/cat/cat_receipt_chain_v2.py`). `entity_registry.aliases` is now TEXT[], as on
live databases, instead of JSONB.
