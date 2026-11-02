# KOI Processor

> **About this repository.** This is a public export of the personal-KOI processor, built
> by a script from one commit of the private development repository (its hash is the first
> line of [`EXPORT-MANIFEST.sha256`](EXPORT-MANIFEST.sha256)). This repository contains
> the personal KOI node: the FastAPI service started with
> `uvicorn api.personal_ingest_api:app`, the migration runner with the forward migrations
> it applies to build a new `personal_koi` database, an example configuration, and the
> document ingest and deep-extraction scripts the service loads. It also includes a small
> set of standalone tools: a batch wrapper for deep extraction, RSS, Substack and
> MediaWiki import tools, an embedding repair job, and a helper that creates the read-only
> database role used by `/sql`. The tests published here are the KOI-net wire conformance
> suite, which runs against a throwaway node, and unit tests that need no database, API or
> network. The documentation covers the personal-node quickstart, fresh-install
> migrations, the KOI-net conformance register and the query-plan specification. The
> private development repository holds more that is not published yet: federation
> onboarding scripts for specific relays and peers, operational and host scripts such as
> backups, rollback migrations, scheduled jobs and service wrappers, one-off data repairs
> and evaluation harnesses, most of the integration test suite (which needs a database or
> a running backend), and deployment-specific runbooks, reports and integration notes.
> Experimental components are held back as well, including the cross-source discourse
> discovery worker and review tools, the GitHub repository sensor and its code-graph
> extractor, the parked TerminusDB mirror, and the older event-bridge stack with its
> code-graph tooling and content dashboards. The vault and session ingesters and the
> `pageindex` search tool are held too, so unified search's vault and session surfaces
> return nothing. Some comments and documents in this repository still mention those files
> by name. Passages of this README and of the operations docs that describe files outside
> this export are left out, and the remaining references in this README to such files are
> marked "(not in this export)".
>
> **What was changed for publication.** Replaced: public server addresses (with RFC 5737
> `203.0.113.x`); macOS home-directory paths (with a placeholder home); the maintainer's
> email (with `maintainer@example.com`); the maintainer's database role in connection
> strings outside `migrations/` (with `koi`); peer node names, logins and addresses (with
> "a bioregional node", "a peer node"); relay, admin and production-host SSH logins (with
> `<admin-user>@`); the default recorded-client address (now set only by
> `KOI_RECORDED_CLIENT_IP`); paths to internal planning documents and records (with "an
> internal planning document", "an internal record"); a production host's pgvector version
> in migration comments; and the names of private individuals in comments, docstrings,
> docs and tests (with invented names). Not replaced: other private-range addresses in
> tests, comments and defaults; paths relative to the home directory in comments and
> defaults; the name of one peer host, `dobby`, which appears in an applied migration's
> file name, in migration content and in code; and the names of people in public roles. A
> build gate fails the export if a person classified private is named in full in a form it
> checks (first and last name, reversed with a comma, dotted, with initials, or wrapped
> across lines), or if any replaced host or login detail remains in the tree.

**Knowledge Organization Infrastructure** — a sensor-to-agent pipeline that ingests
content, resolves and deduplicates entities, builds a knowledge graph, and serves
semantic + graph search to AI agents. PostgreSQL + pgvector is the authoritative store.

This repository is the personal-KOI node: `api/personal_ingest_api.py` on port 8351,
database `personal_koi`, and OpenAI `text-embedding-3-large` (3072-dim) embeddings.

## Quick start

- **personal-KOI:** [`docs/operations/quickstart-personal-koi.md`](docs/operations/quickstart-personal-koi.md)

Health check (personal-KOI): `curl -s http://localhost:8351/health`

## What's live

The personal-KOI backend (`api/personal_ingest_api.py`) mounts these routers:

- **Entity resolution** (`/entities`) — 4-tier dedup: exact → fuzzy (Jaro-Winkler) → semantic (pgvector) → create. Quality pipeline (confidence filter, canonical resolver, ontology normalizer). See [`docs/architecture/`](docs/architecture/).
- **Documents & ingestion** (`/documents`) — chunk → embed (OpenAI 3072-dim) → store, with parent/child provenance and versioning.
- **Knowledge graph** — `/project`, `/intents`, plus **graph traversal** (neighborhood / shortest-path / directed relationships) via PostgreSQL recursive CTEs (`api/graph_queries.py`).
- **Tasks** (`/tasks`) — always-on task registry (ingest/list/patch/stats).
- **KOI-net federation** (`/koi-net`) — peer onboarding, **vault sync** (E2EE, `VAULT_SYNC_ENABLED=true`), commons intake. See `scripts/federation/README.md` (not in this export).
- **Claims Engine** — impact-claim create / attest / verify / anchor on the Regen ledger. See `docs/claims-engine-v1.md` (not in this export). Anchoring runs the `regen` CLI and defaults to mainnet: set the `REGEN_*` variables in `config/personal.env.example` first.
- **Deep extraction** — entities + facts + discourse moves from documents (`scripts/extract_deep_documents.py`); model/transport is env-tunable (`DOC_EXTRACTOR_TRANSPORT` = `claude_p`, which runs the Claude Code `claude -p` CLI, | `api` | `openai`) with a repair loop and a batch wrapper (`scripts/run_batch_extract.sh`). See [`docs/operations/quickstart-personal-koi.md`](docs/operations/quickstart-personal-koi.md) §6.
- **Sensors** — `scripts/rss_sensor.py`, `scripts/substack_sensor.py` and the MediaWiki import tools (`scripts/mediawiki_*.py`) ingest into the running backend; their settings are in `config/*.example.yaml` and [`config/README.md`](config/README.md). The web sensor inside the API is on in the personal profile unless `WEB_SENSOR_ENABLED=false` (the example config sets it); the MediaWiki sensor is off unless `MEDIAWIKI_SENSOR_ENABLED` is set. The GitHub repository sensor is not in this export: with `GITHUB_SENSOR_ENABLED` set, the node logs that it was not started.

## Architecture

PostgreSQL (pgvector) is authoritative for entities, documents, chunks, and relationships.
Ingestion resolves/dedupes entities, embeds chunks, and records provenance; retrieval fuses
semantic (vector) + keyword search and can expand along graph edges. Full detail (with the
BGE-vs-OpenAI embedding scoping note) in `docs/architecture/overview.md` (not in this export)
and `docs/architecture/hybrid-rag.md` (not in this export).

```
sensors → ingest/resolve/embed → PostgreSQL (+pgvector)  → hybrid RAG + graph traversal → agents (MCP)
                                        │
                                        └─ Fuseki/SPARQL mirror (RegenAI event-bridge stack only; the personal node does not read FUSEKI_*)
```

## Project structure

```
api/            FastAPI backend (personal_ingest_api, routers/, graph_queries, embedding_provider)
scripts/        document ingest and deep extraction, the migration runner, sensors and import tools
config/         *.example configuration templates (see config/README.md)
migrations/     the forward migrations the runner applies to a new personal_koi
docs/           quickstart, fresh-install migrations, KOI-net conformance register, QueryPlan spec
tests/          KOI-net conformance suite, offline unit tests, the test-target guard
```

## Configuration

Copy `config/personal.env.example` → `config/personal.env` (gitignored) and fill in
`POSTGRES_URL`, `OPENAI_API_KEY`, embedding model/dim. Personal choices (which
publications/feeds/authors to ingest, batch transport) use the committed `*.example` →
gitignored pattern — see [`config/README.md`](config/README.md). Load env with
`set -a; source config/personal.env; set +a`.

## Access control

Most routes, writes included, have no authentication in any mode. Only these check a
credential: the read routes (`/search`, `/query`, `/knowledge/unified-search`,
`/knowledge/discourse-search`), and only with `KOI_AUTH_MODE=enforce`; `/sql`; the claims,
episode, fact-retraction, document-ingest and entity-merge writes; and, with KOI-net on,
its admin routes. Keep the API on 127.0.0.1, or put a proxy that authenticates every
request in front of it. The full table, the token variables and the
`/sql` role setup are in the quickstart's
[Access control](docs/operations/quickstart-personal-koi.md#7-access-control) section.

## API & schema reference

- REST endpoints: `docs/operations/api.md` (not in this export) (scope-annotated per surface)
- QueryPlan IR spec: [`docs/specs/b9a-query-plan-spec.md`](docs/specs/b9a-query-plan-spec.md)
- Isolated KOI tables + useful queries: `docs/integration/personal-sensor.md` (not in this export)
- Changelog: `docs/CHANGELOG.md` (not in this export)

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

The published tests are offline unit tests and the KOI-net conformance suite. Run them from
the repository root, in the venv from the quickstart:

```bash
pytest -rs tests/
```

The unit tests need no API or network, and only one needs a database:
`test_connection_lands_in_the_named_database` in
[`tests/test_isolation_redirect.py`](tests/test_isolation_redirect.py) connects to
`KOI_TEST_DATABASE_URL` and skips without it. In that venv the conformance module skips,
because koi-net is not installed there; it runs from its own venv against a throwaway node
you start, as [`tests/conformance/README.md`](tests/conformance/README.md) describes.

- Tests take every database and API target from `KOI_TEST_DATABASE_URL`,
  `KOI_TEST_ADMIN_DSN` and `KOI_TEST_API_URL`, and nothing else
  ([`tests/koi_test_targets.py`](tests/koi_test_targets.py)). None has a default. Of the
  published tests, the conformance suite uses `KOI_TEST_API_URL` and the one test above
  uses `KOI_TEST_DATABASE_URL`; a test whose variable is unset skips and names it.
- Before collection, [`tests/conftest.py`](tests/conftest.py) ends the run with exit code 4
  if a database URL or admin DSN names `personal_koi`, names a database without `test` as
  a word of its name (`koi_test`, `test_x`; not `latest`), or puts `dbname` or `database`
  in its query string, or if an API URL uses port 8351. `KOI_TEST_ALLOW_LIVE=1` lifts that
  refusal.
- The same conftest points the variables that code under test reads (`POSTGRES_URL`,
  `DATABASE_URL`, `KOI_API_URL`, `BASE_URL`, `PGHOST` and others) at those values, or at an
  address that cannot resolve when one is unset, and turns off `.env` loading. It also
  points `HOME`, the node-key, vault and state locations, `PGPASSFILE` and
  `KOI_READ_SCOPES_FILE` at a temporary directory for the run, and sets `KOI_AUTH_MODE` to
  `log`, so a test database that needs a password must carry it in its `KOI_TEST_*` URL.

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
