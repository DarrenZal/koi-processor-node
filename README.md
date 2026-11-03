# KOI Processor

> **About this repository.** This is a public export of the personal-KOI processor, built
> by a script from one commit of the private development repository (its hash is the first
> line of [`EXPORT-MANIFEST.sha256`](EXPORT-MANIFEST.sha256)). This repository contains
> the personal KOI node: the FastAPI service started with
> `uvicorn api.personal_ingest_api:app`, the migration runner with the forward migrations
> it applies to build a new `personal_koi` database, an example configuration, and the
> document ingest and deep-extraction scripts the service loads. Since the wide review of
> 2026-11-02 it also carries most of the rest of the development repository as it stands:
> the code of the older RegenAI event-bridge stack (most of `src/`, without its two
> event-bridge services and the held scripts listed below, the TypeScript query API
> `koi-query-api.ts`, and the content, podcast and dashboard tooling), the GitHub
> repository sensor and its code-graph extractor, the parked TerminusDB mirror, the
> cross-source discourse discovery worker and review tools, the federation onboarding
> scripts, the sensor, re-embedding, repair, audit and migration tools under `scripts/`,
> down, rollback and draft migrations with the per-database baselines, the architecture,
> integration and operations docs, and most of the test suite, much of which needs a
> database or a running API. Many of these files need packages that `requirements.txt`
> does not list (see "Python version and optional packages" below), or a database a fresh
> install does not create (the RegenAI stack's `eliza` database, Apache AGE), so they are
> published for reading and reuse rather than as part of the node. Still held in the
> private repository: agent instructions and work orders; run outputs, reports, evaluation
> results and generated data; archives; deployment files, runbooks and scripts for the
> maintainer's own machines and servers (backups, scheduled jobs, cron installers, service
> wrappers and the files that hard-code that layout); memos and records about people;
> Regen's claims documents and ledger deploy scripts; client and engagement material (an
> export for one client, seed data and an evaluation set for specific engagements);
> one-off cleanup, audit and evaluation scripts written for the Regen team database; files
> that, run with no arguments, with `--help` or with `--dry-run`, delete, clear or
> overwrite data in a database, triple store or vault, write to a database not named on
> the command line, post to, poll or acknowledge a running service's API or event queue,
> anchor on a chain, or start paid API calls with no default cap, and scripts whose dry
> run commits; the scripts that generate Regen's weekly podcast and its map; the Apache
> AGE code-graph analysis scripts; the original content dashboard and the templates only
> it renders; operational notes, measurements and benchmarks from the maintainer's own
> nodes, and stale runbooks; files that cannot run here (imports of missing or held
> modules, scripts that run held scripts, stubs); and the tests and fixtures that need
> held code or data, describe the maintainer's own nodes and data, or copy a third-party
> web page. The vault and session ingesters and the `pageindex` search tool are not in
> this export, so unified search's vault and session surfaces return nothing. Some kept
> files act when run with no arguments, by design. The sensors and their scheduled
> wrappers ingest into the configured node. Three scheduled jobs write to the configured
> database and may call the node's API: `scripts/run_embedding_repair.sh` (through
> `scripts/backfill_null_embeddings.py`, which re-embeds at most 500 rows per surface with
> a cost limit and can file a task on the node's task API when a backlog persists),
> `scripts/deep_extract_substack_corpus.sh` (deep extraction of every pending Substack
> corpus post with the configured extractor) and `scripts/run_discourse_discovery.sh` (one
> discourse discovery job); the last two default to `personal_koi` on localhost:5432.
> `scripts/federation/soak-check.sh` sends read-only requests to the node, and
> `scripts/monitor_services.sh` checks the RegenAI stack's services, including two
> one-line requests to its embedding server, which calls the OpenAI API. The federation
> conflict drills write to the default database inside a transaction they roll back, and
> `scripts/federation/soak-cron-remove.sh` removes its own lines from your crontab.
> `scripts/run_migrations.sh` and `scripts/setup.sh` ignore `--help` and apply the
> migrations to `DATABASE_URL` (default the `eliza` database on localhost:5433); the
> servers bind their ports and create their own tables; and some scripts write to the
> working directory, HOME, `/tmp`, or test databases and containers of their own. Some
> comments and documents in this repository still mention held files by name. Passages of
> this README and of the operations docs that describe files outside this export are left
> out, and the remaining references in this README to such files are marked "(not in this
> export)".
>
> **What was changed for publication.** Replaced: public server addresses (with RFC 5737
> `203.0.113.x`); macOS home-directory paths (with a placeholder home); the maintainer's
> email (with `maintainer@example.com`); the maintainer's database role in connection
> strings outside `migrations/` (with `koi`); peer node names, logins and addresses (with
> "a bioregional node", "a peer node", `<peer-node>`, `<peer>@<peer-lan-ip>`); relay,
> admin and production-host SSH logins (with `<admin-user>@`, `<relay-user>@`); a
> teammate's first name where it serves as a peer or node id (with `peer`); the default
> recorded-client address (now set only by `KOI_RECORDED_CLIENT_IP`); paths to internal
> planning documents and records (with "an internal planning document", "an internal
> record"); a production host's pgvector version in migration comments; and the names of
> private individuals in comments, docstrings, docs and tests (with invented names). Not
> replaced: WireGuard mesh relay and admin addresses (hosts .1 and .2 of the mesh subnet);
> other private-range addresses in tests, comments and defaults; paths relative to the
> home directory in comments and defaults; the name of one peer host, `dobby`, which
> appears in an applied migration's file name, in migration content and in code; and the
> names of people in public roles. A build gate fails the export if a person classified
> private is named in full in a form it checks (first and last name, reversed with a
> comma, dotted, with initials, or wrapped across lines), or if any replaced host or login
> detail remains in the tree.

**Knowledge Organization Infrastructure** — a sensor-to-agent pipeline that ingests
content, resolves and deduplicates entities, builds a knowledge graph, and serves
semantic + graph search to AI agents. PostgreSQL + pgvector is the authoritative store.

This export centres on the personal-KOI node. It also carries the code of the older RegenAI
event-bridge stack, which has no quickstart here; only older architecture documents describe it.
> **Two deployment surfaces.** The same code serves both:
>
> | | **personal-KOI** | **RegenAI event-bridge stack** |
> |---|---|---|
> | Backend | `api/personal_ingest_api.py` on port 8351 | event-bridge services plus `personal_ingest_api` |
> | Database | `personal_koi` | `eliza` |
> | Embeddings | OpenAI `text-embedding-3-large` (3072-dim) | BGE `bge-large-en-v1.5` (1024-dim) via `src/core/bge_server.py` |
>
> This README and the personal-KOI quickstart describe the first. Older documents that
> mention `bge_server.py`, `koi_event_bridge_v2.py` (not in this export), `eliza` or ports 8090/8100 describe the
> second.

## Quick start

- **personal-KOI:** [`docs/operations/quickstart-personal-koi.md`](docs/operations/quickstart-personal-koi.md)

Health check (personal-KOI): `curl -s http://localhost:8351/health`

## What's live

The personal-KOI backend (`api/personal_ingest_api.py`) mounts these routers:

- **Entity resolution** (`/entities`) — 4-tier dedup: exact → fuzzy (Jaro-Winkler) → semantic (pgvector) → create. Quality pipeline (confidence filter, canonical resolver, ontology normalizer). See [`docs/architecture/`](docs/architecture/).
- **Documents & ingestion** (`/documents`) — chunk → embed (OpenAI 3072-dim) → store, with parent/child provenance and versioning.
- **Knowledge graph** — `/project`, `/intents`, plus **graph traversal** (neighborhood / shortest-path / directed relationships) via PostgreSQL recursive CTEs (`api/graph_queries.py`).
- **Tasks** (`/tasks`) — always-on task registry (ingest/list/patch/stats).
- **KOI-net federation** (`/koi-net`) — peer onboarding, **vault sync** (E2EE, `VAULT_SYNC_ENABLED=true`), commons intake. See [`scripts/federation/README.md`](scripts/federation/README.md).
- **Claims Engine** — impact-claim create / attest / verify / anchor on the Regen ledger. See [`docs/claims-engine-v1.md`](docs/claims-engine-v1.md). Anchoring runs the `regen` CLI and defaults to mainnet: set the `REGEN_*` variables in `config/personal.env.example` first.
- **Deep extraction** — entities + facts + discourse moves from documents (`scripts/extract_deep_documents.py`); model/transport is env-tunable (`DOC_EXTRACTOR_TRANSPORT`: `claude_p`, the default, runs the Claude Code `claude -p` CLI; `api` calls the Anthropic API; `openai` calls an OpenAI-compatible endpoint) with a repair loop and a batch wrapper (`scripts/run_batch_extract.sh`). See [`docs/operations/quickstart-personal-koi.md`](docs/operations/quickstart-personal-koi.md) §6.
- **Sensors** — `scripts/rss_sensor.py`, `scripts/substack_sensor.py` and the MediaWiki import tools (`scripts/mediawiki_*.py`) ingest into the running backend; their settings are in `config/*.example.yaml` and [`config/README.md`](config/README.md). The web sensor inside the API is on in the personal profile unless `WEB_SENSOR_ENABLED=false` (the example config sets it); the MediaWiki sensor is off unless `MEDIAWIKI_SENSOR_ENABLED` is set. The GitHub repository sensor (`api/github_sensor.py`) is off unless `GITHUB_SENSOR_ENABLED` is set, and it needs the tree-sitter packages, which `requirements.txt` does not list: without them the node logs that it was not started. Other ingest, repair, re-embedding and federation scripts under `scripts/` are published as they are; some need packages that `requirements.txt` does not list, and several default to the RegenAI stack's database.

## Architecture

PostgreSQL (pgvector) is authoritative for entities, documents, chunks, and relationships.
Ingestion resolves/dedupes entities, embeds chunks, and records provenance; retrieval fuses
semantic (vector) + keyword search and can expand along graph edges. Full detail (with the
BGE-vs-OpenAI embedding scoping note) in [`docs/architecture/overview.md`](docs/architecture/overview.md)
and [`docs/architecture/hybrid-rag.md`](docs/architecture/hybrid-rag.md).

```
sensors → ingest/resolve/embed → PostgreSQL (+pgvector)  → hybrid RAG + graph traversal → agents (MCP)
                                        │
                                        └─ Fuseki/SPARQL mirror (RegenAI event-bridge stack only; the personal node does not read FUSEKI_*)
```

## Project structure

```
api/            FastAPI backend (personal_ingest_api, routers/, graph_queries, embedding_provider)
scripts/        ingest and deep extraction, the migration runner, sensors, import, repair,
                re-embedding and federation tools, the parked TerminusDB mirror
src/            the RegenAI event-bridge stack: src/core (bge_server.py and most other services;
                its two event-bridge services and some scripts are not in this export), extraction,
                knowledge-graph post-processing, content and podcast tooling
config/         *.example configuration templates (see config/README.md) and the stack's settings
migrations/     forward migrations, down and rollback files, baselines/ per-database manifests
docs/           quickstart, operations, architecture, integration and spec docs, conformance register
tests/          unit, database and API tests, the KOI-net conformance suite, the test-target guard
static/         demo.html (the API redirects /demo to it) and the podcast map pages
templates/      the HTML template of src/content/content_dashboard_enhanced.py
data/           the canonical entity table the entity resolvers read, and the module alias
                file of the code-docs bridge's entity linker (docs/architecture/code-docs-bridge.md;
                the bridge scripts are not in this export)
utils/          date extraction for the content curator
ontologies/     the KOI provenance ontology (Turtle)
koi-query-api.ts  the RegenAI stack's TypeScript query API (with adaptive-features.ts)
koi-ontology.ttl, pipeline-metadata.ttl  ontology and pipeline metadata; the stack's metadata API
                (api/pipeline_metadata_api.py) loads them only from hard-coded paths under
                /opt/projects/koi-processor/, not from the checkout
adaptive_extraction_api.py  an HTTP service around the RegenAI stack's adaptive extractor
run-koi-mcp-stdio.sh  starts the KOI knowledge MCP server (src/core) over stdio
docker-compose.terminusdb.yml  the TerminusDB container for the parked mirror
```

At startup the API logs `SeaTrees router not mounted: No module named
'api.routers.seatrees_router'`. That module, `api.routers.seatrees_router` (not in this export),
is held, and the warning is expected.

## Configuration

Copy `config/personal.env.example` → `config/personal.env` (gitignored) and fill in
`POSTGRES_URL`, `OPENAI_API_KEY`, embedding model/dim. Personal choices (which
publications/feeds/authors to ingest, batch transport) use the committed `*.example` →
gitignored pattern — see [`config/README.md`](config/README.md). Load env with
`set -a; source config/personal.env; set +a`.

## Python version and optional packages

Use Python 3.11, as the quickstart does. The "Python 3.8+" line at the top of
`requirements.txt` and the 3.8 check in `scripts/setup.sh` are out of date: the pinned
`rid-lib` 3.3.0 and `jsonschema` 4.26.0 require Python 3.10 or later, and so does the
`scrapling` release that resolves on 3.11 (0.4.7), while the `numpy` that resolves there
(2.4.6) requires 3.11.

`requirements.txt` installs what the personal-KOI node needs. These modules need packages
it does not install. Install them yourself if you use the module:

| Package | Needed by |
|---|---|
| `PyJWT` | `src/services/auth_service.py`, and so `api/koi_content_api.py`, which mounts it |
| `rdflib` | `api/pipeline_metadata_api.py`, `src/core/namespaces.py`, `src/core/ontology_loader.py`, `src/extraction/llm_extractor.py`, `src/extraction/openai_extractor.py`, `src/provenance/jena_integration.py`, `src/knowledge_graph/graph_integration.py` (which also imports `rdflib_postgresql`), `scripts/regenerate_fuseki_graph.py` (also `SPARQLWrapper`) |
| `tree-sitter`, `tree-sitter-python`, `tree-sitter-typescript` | the GitHub sensor's extractor, `api/tree_sitter_extractor.py`; `src/core/tree_sitter_extractor.py` also needs `tree-sitter-go` |
| `terminusdb-client` | the parked TerminusDB mirror: `api/terminusdb_adapter.py` and `scripts/terminusdb/` |
| `google-genai` | the Gemini paths: `src/extraction/gemini_extractor.py`, and `api/llm_enricher.py` with `LLM_BACKEND=gemini` |
| `sentence-transformers` | `EMBEDDING_PROVIDER=sentence_transformers` in `api/embedding_provider.py`, `src/core/bge_server_local.py`, `scripts/reembed_on_h200.py` (also `torch`) |
| `podcastfy`, `mutagen` | the podcast tooling under `src/audio/` |

The Gemini code imports `google-genai` (`from google import genai`). `requirements.txt`
installs `google-generativeai` instead, which no file imports, so the Gemini extractor's
tests skip until `google-genai` is installed.

## Access control

Most routes, writes included, have no authentication in any mode. Only these check a
credential: the read routes (`/search`, `/query`, `/knowledge/unified-search`,
`/knowledge/discourse-search`), and only with `KOI_AUTH_MODE=enforce`; `/sql`; the claims,
episode, fact-retraction, document-ingest and entity-merge writes; and, with KOI-net on,
its admin routes. Keep the API on 127.0.0.1, or put a proxy that authenticates every
request in front of it. These start paths of the node's API bind all interfaces
(`0.0.0.0`) instead: running `api/personal_ingest_api.py` directly (its `__main__` block),
the `start.sh` that `scripts/federation/setup-node.sh` installs for a federation node from
`scripts/federation/start-sh.template`, the manual start command `setup-node.sh` prints
when it finds no `start.sh`, and the usage comment at the top of
`scripts/federation/personal-env.template`. A federation node bound this way relies on the firewall
`setup-node.sh` applies, which `--skip-firewall` or a "no" at its prompt skips and which on
macOS can fail without stopping the script; see
[Network exposure](scripts/federation/README.md#network-exposure) in the federation README.
Run directly, nothing in this repository restricts the port. The other servers in this
repository also bind `0.0.0.0` when run directly, among them `adaptive_extraction_api.py`,
`api/koi_content_api.py`, `api/pipeline_metadata_api.py`, `api/github_webhook.py` (and
`scripts/start_webhook_server.sh`) and the RegenAI stack's services under `src/`; that list
is not exhaustive. The full table, the token variables and the
`/sql` role setup are in the quickstart's
[Access control](docs/operations/quickstart-personal-koi.md#7-access-control) section.

## API & schema reference

- REST endpoints: a running node serves its full route list at `/docs` (interactive) and
  `/openapi.json`, for example `http://localhost:8351/docs`.
  [`docs/operations/api.md`](docs/operations/api.md) covers only the RegenAI event-bridge
  services (coordinator, event bridge, BGE server and MCP knowledge server), not the
  personal node's routes. The claims routes are in
  [`docs/claims-engine-v1.md`](docs/claims-engine-v1.md), and the routes that check a
  credential are in the quickstart's
  [Access control](docs/operations/quickstart-personal-koi.md#7-access-control) table.
- QueryPlan IR spec: [`docs/specs/b9a-query-plan-spec.md`](docs/specs/b9a-query-plan-spec.md)
- Isolated KOI tables + useful queries: [`docs/integration/personal-sensor.md`](docs/integration/personal-sensor.md)
- Changelog: [`docs/CHANGELOG.md`](docs/CHANGELOG.md)

## Experimental / parked (NOT deployed)

These are code-complete experiments behind flags, **not** running on any surface — don't
mistake them for live components:

- **TerminusDB graph mirror** (`TERMINUSDB_ENABLED=false`, no instance running anywhere;
  untouched since Feb 2026). Production graph traversal uses the PostgreSQL CTE path
  (`api/graph_queries.py`), not this mirror. Docs: [`scripts/terminusdb/README.md`](scripts/terminusdb/README.md).
- **Learning Field graph projection** (`scripts/project_bridge_notes.py`) — bridge-note →
  claims/concepts projection; run on demand, not a service.
- **Milestone B content tooling** (podcast/curator/weekly-aggregator under `src/content/`) —
  point-in-time deliverables, kept for reference.

## KOI-net conformance

This node implements the [KOI-net](https://github.com/DynamicalSystemsGroup/koi-net) node
protocol itself, on PostgreSQL, and builds on DynamicalSystemsGroup's (DSG)
[`rid-lib`](https://github.com/DynamicalSystemsGroup/rid-lib) (pinned 3.3.0) for RIDs,
manifests and bundles. Known differences from the reference implementation are recorded,
with a class and a reason, in
[`docs/architecture/koi-protocol-conformance.md`](docs/architecture/koi-protocol-conformance.md).
That register is maintained by hand and covers what has been found so far.

- **Interop test, 2026-10-31.** A stock node built from DSG's `koi-net-node-template`
  (koi-net 2.1.2) completes the handshake with this node only when the approved-edge gate
  is off; the example config turns it on (18). Edge negotiation never starts: after a
  handshake this node neither pushes its own node bundle nor proposes an edge (20), and its
  `provides.event` lists entity-type names with no `KoiNetNode`/`KoiNetEdge` (21). So no
  poll and no broadcast beyond the handshake happen in either direction. The gate is
  recorded as not passed.
- **Other open divergences** that affect a koi-net peer: vault-file RIDs use the reserved
  `koi-net.` namespace (1), FORGET events carry the full record (2), our own node bundle is
  not served by `/bundles/fetch` (15; the gate did not reach it), broadcast responses carry
  a body where the spec requires an empty one (22), poll responses carry an `event_id`
  field inside the signed payload, so a koi-net client cannot verify a non-empty poll
  response (16), domain events use application keys as RIDs (17), edges are negotiated
  through our own `/handshake` and exposed in a different shape (7, 12), `base_url` means
  the host root rather than the KOI-net API root (14), and edge scopes mix domain and
  entity types (9).
- **Conformance tests:** in [`tests/conformance/`](tests/conformance/), koi-net builds and
  signs each protocol request, the tests send it to a throwaway node, and they parse and
  verify the response with the `koi_net.protocol` models: 9 pass, and 4 strict xfails cover
  divergences 12, 15, 16 and 17. Requests our own node sends (the poller's poll and
  broadcast calls, where 14 lives) are not covered. koi-net is a test-only dependency there.
- **Plan:** adopt `koi_net.protocol` at runtime once this service is on the versions koi-net
  2.1.2 requires (FastAPI ≥ 0.115.12, Starlette ≥ 1.3.1, httpx ≥ 0.28.1, uvicorn ≥ 0.34.2,
  Python ≥ 3.12; this service pins FastAPI 0.104.1, httpx 0.25.2 and uvicorn 0.24.0 on Python
  3.11), and fix 14–17, 20 and 21 by conforming rather than forking.

## Testing

Run the tests from the repository root, in the venv from the quickstart:

```bash
pytest -rs tests/
```

With no target variable set, every test that needs a database, an API, a second node or a
live LLM skips and names what it needs (`pytest -rs` lists them). Ten more, in
`tests/test_agentic_crawler.py`, `tests/test_embedding_provider.py` and
`tests/test_vision_ocr.py`, count tokens with `tiktoken`. It downloads its public encoding
files (`cl100k_base`, `o200k_base`) from OpenAI's CDN the first time they are used, and
caches them in `TIKTOKEN_CACHE_DIR` (else `DATA_GYM_CACHE_DIR`, else `data-gym-cache` in
the system temporary directory). The suite downloads nothing by default: these ten skip,
and say why, unless their encodings are already cached or `KOI_TEST_TIKTOKEN_DOWNLOAD=1`
allows the download
([`tests/koi_test_targets.py`](tests/koi_test_targets.py), `requires_tiktoken`). To run
them offline, run them once with network, `KOI_TEST_TIKTOKEN_DOWNLOAD=1` and
`TIKTOKEN_CACHE_DIR` set to a directory you keep, then set the same `TIKTOKEN_CACHE_DIR`
for offline runs. The rest run offline. Measured 2026-11-02 on a build of this export
(2,436 tests) with no network, a new `HOME` and temporary directory and no encodings
cached: 1,490 passed, 946 skipped, the ten `tiktoken` tests among the skips. In that
venv the conformance module skips too, because koi-net is not installed there; it runs from
its own venv against a throwaway node you start, as
[`tests/conformance/README.md`](tests/conformance/README.md) describes.

Tests take every database and API target from these variables and nothing else. None has
a default ([`tests/koi_test_targets.py`](tests/koi_test_targets.py)):

| Variable | What it is |
|---|---|
| `KOI_TEST_DATABASE_URL` | A database tests may write to. Most tests roll back; some commit and delete their own rows. |
| `KOI_TEST_ADMIN_DSN` | A role that may create databases, connected to a database with `test` as a word of its name (`koi_test_admin`). Tests create `koi_test_<unique>` databases through it and drop only those. |
| `KOI_TEST_API_URL` | A running API tests may call. Several write through it, so use a throwaway API whose database is `KOI_TEST_DATABASE_URL`. |

[`tests/test_interop_matrix.py`](tests/test_interop_matrix.py) also needs a second node:
`KOI_TEST_PEER_API_URL` and `KOI_TEST_PEER_DATABASE_URL`.

- Nothing connects at import or collection.
- Before collection, [`tests/conftest.py`](tests/conftest.py) ends the run with exit code 4
  if a database URL or admin DSN names `personal_koi`, names a database without `test` as
  a word of its name (`koi_test`, `test_x`; not `latest`), or puts `dbname` or `database`
  in its query string, or if an API URL uses port 8351. `KOI_TEST_ALLOW_LIVE=1` lifts that
  refusal.
- The same conftest points the variables that code under test reads (`POSTGRES_URL`,
  `DATABASE_URL`, `KOI_API_URL`, `BASE_URL`, `PGHOST` and others) at these values, or at an
  address that cannot resolve when one is unset, and turns off `.env` loading. It also
  points `HOME`, the node-key, vault and state locations, `PGPASSFILE` and
  `KOI_READ_SCOPES_FILE` at a temporary directory for the run, and sets `KOI_AUTH_MODE` to
  `log`, so a test database that needs a password must carry it in its `KOI_TEST_*` URL.
- [`tests/test_isolation_policy.py`](tests/test_isolation_policy.py) fails if any `.py` or
  `.sh` file under `tests/` reads one of the runtime variables the guard redirects, names
  a local Postgres on the default port or socket (`localhost:5432`, `[::1]:5432`, a local
  URL with no port), names `:8351`, or names the `personal_koi` database in a URL, a
  `dbname=`/`-d` argument, a `connect(...)` call or a mapping. It is a pattern scan, so it
  catches these shapes, not every way of building a target.

Build the test databases on a cluster that holds no real data, from a fresh `personal_koi`
built there by `scripts/run_migrations.sh` (see
[fresh-install-migrations.md](docs/operations/fresh-install-migrations.md)). `createdb -T`
needs `personal_koi` to have no other connections, so stop any node using it first:

```bash
createdb -h 127.0.0.1 -p <port> -T personal_koi koi_test
createdb -h 127.0.0.1 -p <port> koi_test_admin
KOI_TEST_DATABASE_URL=postgresql://<role>@127.0.0.1:<port>/koi_test \
KOI_TEST_ADMIN_DSN=postgresql://<role>@127.0.0.1:<port>/koi_test_admin \
  pytest -rs tests/
```

The four shell suites that write through the API (`test_consent_leakage.sh`,
`test_steel_thread_phase_a.sh`, `test_steel_thread_phase_b.sh`,
`test_tbff_threshold_policy.sh`) run only with
`KOI_TEST_API_URL` and `KOI_TEST_DATABASE_URL` set, and refuse the same targets
([`tests/live_write_shell_guard.sh`](tests/live_write_shell_guard.sh)). So do the manual
scripts `tests/test_deep_extract_lease.py`, `tests/test_episode_batching_smoke.py` and
`tests/test_window_isolation_smoke.py`.

**Known failures.** The suite is not green against a fresh build. Measured 2026-11-02 on
this export (2,436 tests) with `KOI_TEST_DATABASE_URL` and `KOI_TEST_ADMIN_DSN` set on a
throwaway cluster, no API and no network: 1,995 passed, 213 failed, 9 errors, 219 skipped.
Of the 222 failures and errors:

- 195 insert into `entity_registry` without an `embedding`, which the fresh schema declares
  `NOT NULL` while live databases allow null.
- 7 are crawl-endpoint commit tests: commits end partly committed or need
  `entity_registry.merged_into`, which a fresh build lacks, and three exhaust their test
  doubles.
- 6: migration 126's dry run seeds 30 rows where it expects 28.
- 5 use columns or tables a fresh build lacks (`entity_merge_log.reversal`,
  `koi_relationships`).
- 8 others: embedding-column dimensions (2), read-auth surfaces (2), the merge endpoint now
  requires `dry_run` (2), the fresh vocabulary lacks `Component` and `Incident` (1) and a
  session-exclusion check (1).
- 1 comes from how it was measured:
  `tests/test_discourse_links.py::test_canary_refuses_to_commit_writes_outside_its_tables`.
  The cluster ran with outbound network blocked, so PostgreSQL 14 could not open its
  statistics socket and turned `track_counts` off, and the canary counts a run's writes
  from those statistics. It passes on a cluster with `track_counts` on.

With `KOI_TEST_API_URL` also set, on a throwaway API whose database is `koi_test` and with
no embedding provider reachable, 94 more tests run: 58 pass, 10 are expected failures (the
planned core endpoints in `tests/test_contract.py`), and 26 fail or error: 20 intent registry
tests (without embeddings their entity inserts hit the same `NOT NULL`), 4 cross-type dedup
cases that need semantic resolution, and 2 profile-gating contract tests.

## Related repositories

- [gaiaaiagent/koi-sensors](https://github.com/gaiaaiagent/koi-sensors) — sensor implementations
- [regen-network/koi-research](https://github.com/regen-network/koi-research) — research & evals
- [gaiaaiagent/GAIA](https://github.com/gaiaaiagent/GAIA) — Eliza AI agent framework

## License

Copyright 2025–2026 Darren Zal. Licensed under the Apache License, Version 2.0. See
[`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).

- This licence covers the files in this repository, exported from the commit recorded in
  [`EXPORT-MANIFEST.sha256`](EXPORT-MANIFEST.sha256).
- Code that also appears in [regen-network/koi-processor](https://github.com/regen-network/koi-processor)
  is available there under Apache-2.0 too, with its own NOTICE.
- Earlier copies of this code were public at `github.com/gaiaaiagent/koi-processor` (private
  since 2026-11-02). They carried the README line "MIT — see `LICENSE`" without a LICENSE
  file. This export does not withdraw any permission that line granted for those copies.
- Dependencies keep their own licences and none are bundled. See
  [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
