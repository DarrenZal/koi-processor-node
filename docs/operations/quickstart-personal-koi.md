# Personal-KOI Quick Start

The **personal-KOI** surface: a single-user knowledge graph backend (Obsidian vault +
sessions + sensor corpora) served by `api/personal_ingest_api.py` on **port 8351**, backed
by PostgreSQL **`personal_koi`** with pgvector, embedding via **OpenAI
`text-embedding-3-large` (3072-dim)**.
The vault and session surfaces need tools that are not in this export (the vault and
session ingesters and the `pageindex` search tool), so unified search returns nothing
from them here. Sensors and document ingestion fill the rest.

## Prerequisites

- Python 3.11+ and a virtualenv
- PostgreSQL 14+ with the `pgvector` extension, version 0.8 or later: the 3072-dim
  `halfvec` HNSW indexes need 0.7, and filtered vector search sets `hnsw.iterative_scan`,
  which 0.8.0 added (migration 109 sets it for the database)
- A PostgreSQL server that sees the migration runner's connection as local: a Unix socket,
  or TCP to 127.0.0.1 or ::1 on the server's side. Migrations 086, 103 and 104 refuse any
  other server address (`Refusing: non-local inet_server_addr`). A server in a Docker
  container reached through a published port sees the connection arrive on the
  container's own address, so the runner stops at 086. Use a PostgreSQL installed on the
  same machine, or run the migrations from inside the container.
- A PostgreSQL superuser for two steps in §2: `CREATE EXTENSION vector` (pgvector is not a
  trusted extension) and migration 109, which changes the cost of pgvector's
  `cosine_distance` functions and sets `hnsw.iterative_scan` for the database. Only the
  functions' owner (the role that created the extension) or a superuser may do that.
  Everything else can run as an ordinary role that owns the database (§2).
- An `OPENAI_API_KEY` (embeddings; extraction can use a different transport — see below)

## 1. Install and config

```bash
python3.11 -m venv venv && venv/bin/pip install -r requirements.txt
cp config/personal.env.example config/personal.env   # gitignored; fill in your values
# minimum: POSTGRES_URL=postgresql://<user>@localhost:5432/personal_koi
#          OPENAI_API_KEY=sk-...
#          EMBEDDING_MODEL=text-embedding-3-large   EMBEDDING_DIMENSION=3072
#          KOI_NODE_NAME=<your node's name>   KOI_STATE_DIR=<a directory for node state>
```

Env is loaded with `set -a; source config/personal.env; set +a` (exports to child
processes). `config/personal.env.example` explains each variable it sets, including the
optional `DOC_EXTRACTOR_*` deep-extraction transport knobs and the access-control and
service-token settings (step 7). [`config/README.md`](../../config/README.md) covers the
`*.example` personal-config files.

## 2. Database

The simplest setup uses one superuser role for this whole section and for the API
(`POSTGRES_URL`):

```bash
createdb personal_koi        # exactly this name: the runner's personal profile and migrations 086/103/104 require it
psql personal_koi -c 'CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
DATABASE_URL=postgresql://<superuser>@localhost:5432/personal_koi bash scripts/run_migrations.sh
```

Use the same role for the API afterwards: the tables belong to the role that ran the
runner, and another role gets `permission denied`.

To run the node as an ordinary role that owns the database (`<owner>`, with LOGIN and no
other attributes), a superuser does two steps and the owner does the rest:

```bash
# as a superuser: the role (skip if it exists), the database and its public schema,
# both owned by <owner>, and the extensions
createuser -U <superuser> <owner>
createdb -U <superuser> -O <owner> personal_koi
psql -U <superuser> -d personal_koi -c 'ALTER SCHEMA public OWNER TO <owner>'
psql -U <superuser> -d personal_koi -c 'CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
# as the owner: every migration. Expect exit 1, with only 109_pgvector_cost_model listed
# under "Applied with statement errors" (must be owner of function cosine_distance)
DATABASE_URL=postgresql://<owner>@localhost:5432/personal_koi bash scripts/run_migrations.sh
# as a superuser: finish 109 (it only re-costs three functions and sets hnsw.iterative_scan)
psql -U <superuser> -d personal_koi -f migrations/109_pgvector_cost_model.sql
```

In this path, ignore the runner's advice to drop and recreate the database: 109 is the
only migration that needs a superuser, and the last command completes it. Then use
`<owner>` in `POSTGRES_URL`. The `ALTER SCHEMA` line matters on PostgreSQL 14, where
`public` belongs to the superuser that created the cluster and `<owner>` may create tables
in it only through the default grant to everyone (`PUBLIC`). The `/sql` setup in step 7
revokes that grant, and the API, which runs `CREATE TABLE IF NOT EXISTS` at startup, then
stops with `permission denied for schema public`. As the schema's owner, `<owner>` keeps
the right. PostgreSQL 15 and later already give `public` to the database owner. Without the 109 step the node still works, but its vector
searches run as slow sequential scans. Apply the whole file: its comments explain why its
two statements must go together.

Run the runner once, on the empty database, before starting the backend (the API creates
tables at startup). As a superuser it applies the chain with no errors. It refuses a
`personal_koi` that is not empty. If a run stops part-way for any other reason, drop and
recreate the database and run it again.
`scripts/setup.sh` targets the RegenAI `eliza` database on port 5433, not this one. What
the personal profile checks, skips, replaces and adds, and how to add a migration to an
existing database: [fresh-install-migrations.md](fresh-install-migrations.md).

## 3. Run the backend

```bash
set -a; source config/personal.env; set +a
venv/bin/python -m uvicorn api.personal_ingest_api:app --host 127.0.0.1 --port 8351
```

Run one instance per port; a second `uvicorn` on 8351 fails to bind. To run it as a
service, use your platform's service manager (launchd, systemd) with the same command.
Keep `--host 127.0.0.1`: most routes, writes included, have no authentication (step 7).

## 4. Verify

```bash
curl -s http://localhost:8351/health       # {"status":"healthy", "embedding_model":"text-embedding-3-large", "embedding_dimension":3072, ...}
curl -s http://localhost:8351/tasks/stats  # confirms the task router is mounted
```

`/health` also reports `embedding_available`, read from the state file that
`scripts/backfill_null_embeddings.py` (the embedding repair job) writes. It stays `false`,
with `status` still `healthy`, until that job runs every 300 seconds or so and
`EMBED_REPAIR_STATE_FILE` names the file it writes (`logs/embedding-repair-state.json` in
the repository by default, or its `--state-file`).

## 5. Sensors (optional)

`scripts/rss_sensor.py` and `scripts/substack_sensor.py` ingest feeds and Substack
publications into the running backend. They read `config/rss_feeds.yaml` and
`config/substack_publications.yaml`: copy each from its `.example.yaml` file and edit it
(see [`config/README.md`](../../config/README.md)). The `scripts/mediawiki_*.py` tools import a
MediaWiki. Schedule the ones you want with your platform's scheduler, from a checkout you
do not develop in. The source identifiers and the contract for the email and session
sensors, which live in the separate `koi-sensors` repository, are in
[personal-sensor.md](../integration/personal-sensor.md).

## 6. Deep extraction (entities/facts/discourse)

`scripts/extract_deep_documents.py` builds the graph from ingested docs. Model/transport is
env-tunable with `DOC_EXTRACTOR_TRANSPORT`:

- `claude_p` (the default) runs the Claude Code `claude -p` CLI: `CLAUDE_BIN`, or `claude`
  on `PATH`.
- `api` calls the Anthropic API with `ANTHROPIC_API_KEY` (model: `DOC_EXTRACTOR_MODEL`).
- `openai` calls an OpenAI-compatible endpoint set by the `DOC_EXTRACTOR_OPENAI_*`
  variables in `config/personal.env.example`.

The advisory lock is per document (`deep-extract-doc:<rid>`), so different documents can be
extracted concurrently; only the same document is serialized. One-off batches on an
alternate model go through `scripts/run_batch_extract.sh`. Set `KOI_VENV=venv` (your
virtualenv directory) when you pass it a `.py` script: its default interpreter path is the
maintainer's.

Extraction and the `standard` and `thorough` tiers of `scripts/ingest_document.py` write
through the running API, and those routes need a token:

- `KOI_CLAIMS_SERVICE_TOKEN`: set the same value (any long random string) for the API and
  for the scripts. `extract_deep_documents.py` sends it to `POST /knowledge/episodes` and
  `ingest_document.py` to `POST /claims/extract`; without it both get 401 after the
  extraction work is done.
- `DOC_INGEST_KOI_URL`: where the scripts send those writes (default
  `http://localhost:8351`).
- `KOI_INGEST_SERVICE_TOKEN` and `INGEST_SOURCE_ROOT`: only for `POST /documents/ingest`,
  the HTTP form of `ingest_document.py`. Callers send that token, and source files must be
  under `INGEST_SOURCE_ROOT`. The pipeline it starts runs inside the API process, so the
  API also needs `KOI_CLAIMS_SERVICE_TOKEN` and, if it is not on port 8351,
  `DOC_INGEST_KOI_URL`.

`ingest_document.py --dry-run` only chunks and reports: it embeds nothing and opens no
database connection.

## 7. Access control

Measured against the code and a running node on 2026-11-01. Send a token as
`Authorization: Bearer <token>` (the read routes also accept `X-API-Key: <token>`).

| Routes | Check |
|---|---|
| `GET` and `POST /search`, `POST /query`, `GET /knowledge/unified-search`, `GET /knowledge/discourse-search` | With `KOI_AUTH_MODE=enforce`: 401 without a token or with an unknown one. In the default mode (`log`, or unset): none. An anonymous caller gets the full personal scope, and an unknown token gets the public scope. |
| `POST /sql` | In every mode, a token with the personal scope (401 without one, 403 for a narrower scope), and `KOI_SQL_RO_DSN`. |
| `POST`/`PATCH` under `/claims` and `GET /claims/identity`; `POST /knowledge/episodes`, `POST /knowledge/facts/{id}/retract`; `POST /entities/merge`, `/unmerge`, `/retype`, `/reproject-closures`, `/entities/{uri}/close` | In every mode, `KOI_CLAIMS_SERVICE_TOKEN` or a valid session token from `session_tokens`. |
| `POST /documents/ingest`, `GET /documents/{rid}` | In every mode, `KOI_INGEST_SERVICE_TOKEN` or a valid session token. |
| With `KOI_NET_ENABLED=true`: `POST /koi-net/edges/approve` and `/reject`, `/koi-net/vault-sync/*`, `/koi-net/commons/intake*` | A caller at 127.0.0.1 or ::1, plus the admin token when one is set. `/koi-net/commons/intake*` also accepts `KOI_COMMONS_SERVICE_TOKEN` from any address. The KOI-net protocol routes accept unsigned requests unless `KOI_STRICT_MODE=true`. |

Every other route has no authentication in any mode. That includes writes such as
`POST /tasks/ingest`, `POST /ingest` and `PATCH /tasks/{key}`, and reads such as
`GET /tasks/`, `GET /entities` and `GET /entity-search`. Bind the API to 127.0.0.1, or
put a proxy in front that authenticates every request. `KOI_AUTH_MODE=enforce` alone does
not protect a node that others can reach.

The personal API issues no session tokens. They come from Regen's sign-in service,
`src/services/auth_service.py`, which `api/koi_content_api.py` serves under `/api/koi` on
port 8007, bound to 0.0.0.0. It is a Google sign-in (device flow and web login, the
`GET /api/koi/auth/web/login` that the 401 message's "sign in to obtain a session cookie"
refers to). It accepts only `@regen.network` accounts, its redirect and verification URIs
are hard-coded to Regen's production host, it reads Google client credentials from
`config/client_secret.json`, and it needs PyJWT, which `requirements.txt` does not install.
It writes tokens to `session_tokens` in the database its `POSTGRES_URL` names (default: the
RegenAI stack's `eliza` on port 5433), so this node accepts them only if both share one
database. Regen's TypeScript query API, `koi-query-api.ts`, has its own session-token code.
Outside Regen's deployment, the tokens you can create are the service tokens below. To
test the session-token paths, insert a `session_tokens` row by hand. The table stores only the
SHA-256 hex digest of the token (`token_hash`), never the token itself:

```bash
psql -d personal_koi -c "INSERT INTO session_tokens (user_email, token_hash, expires_at)
  VALUES ('you@example.org', encode(sha256(convert_to('<a token you choose>', 'UTF8')), 'hex'),
          now() + interval '30 days')"
curl -s -H 'Authorization: Bearer <a token you choose>' http://localhost:8351/claims/identity
```

The `curl` returns `{"email": "you@example.org", ...}`. Revoke the token by setting
`revoked_at = now()` on its row.

Settings (all optional; each token is any long random string):

- `KOI_ADMIN_TOKEN` (or the file `$KOI_STATE_DIR/admin_token`), `KOI_MCP_TOKEN`,
  `KOI_CLAIMS_SERVICE_TOKEN`: each gives the personal scope on the read routes.
  `KOI_INGEST_SERVICE_TOKEN` gives no read scope.
- `KOI_READ_SCOPES_FILE`: a JSON object that maps the SHA-256 hex digest of a token to a
  narrower scope, `{"view": "public"}` or `{"view": "field", "fields": [...]}`
  (format: `parse_scope_entry` in `api/read_scope.py`).
- `KOI_PERSONAL_PRINCIPALS`: comma-separated emails whose session tokens get the personal
  scope. Other valid sessions get 403 on the read routes in enforce mode.
- `KOI_READ_ROUTES_DISABLED`: route ids (`search`, `query`, `discourse-search`,
  `unified-search`, `sql`, or `all`) that return 503.
- `/sql` runs as a read-only role. Create it once with
  `venv/bin/python scripts/sql_ro_role.py --admin-dsn <DSN of a role that may create roles> --apply`,
  then set `KOI_SQL_RO_DSN=postgresql://koi_sql_ro@localhost:5432/personal_koi`. The role
  has no password, so this works only where `pg_hba.conf` lets `koi_sql_ro` in without
  one (`trust` for that role and address). Many installs require a password for host
  connections (`scram-sha-256`); there `/sql` returns 503 until you set one with
  `ALTER ROLE koi_sql_ro PASSWORD '...'` (again after each `--apply`, which resets it)
  and add it to `KOI_SQL_RO_DSN`. `--apply` also revokes `CREATE` on schema `public`
  from `PUBLIC`. On PostgreSQL 14, if the API runs as a role that is neither a superuser
  nor the schema's owner (the owner path in step 2 without its `ALTER SCHEMA` line), the
  API then fails at startup with `permission denied for schema public`. Run
  `ALTER SCHEMA public OWNER TO <owner>` as a superuser, before `--apply` or to repair a
  node that already fails; `--backout` does not restore the right.
