# Database Migration Setup Guide

## Overview

Migrations live in `migrations/`. Apply them with the runner, `scripts/run_migrations.sh`,
never by running every file with `psql -f`: the directory also holds down/rollback files,
`DRAFT_*` proposals and fresh-install-only files that must not be applied that way, and
some migrations need an order other than the file order.

## Prerequisites

- PostgreSQL 14+ with the `pgvector` extension
- psql 10 or later
- A database user that can create extensions (or a database where `vector` and
  `uuid-ossp` already exist)

## Personal-KOI (database `personal_koi`)

Follow [fresh-install-migrations.md](fresh-install-migrations.md). In short:

```bash
createdb personal_koi
psql personal_koi -c 'CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
DATABASE_URL=postgresql://<user>@localhost:5432/personal_koi bash scripts/run_migrations.sh
```

The runner builds `personal_koi` only from an empty database and refuses anything else.
To add a migration to an existing database, see the "Existing databases" section there.

## RegenAI event-bridge stack (database `eliza`)

```bash
DATABASE_URL=postgresql://<user>:<password>@localhost:5433/eliza bash scripts/run_migrations.sh
```

This does not build a new `eliza` database. The chain grew alongside the live RegenAI
database, and on an empty one several files fail: `005` declares indexes inside
`CREATE TABLE`, which PostgreSQL rejects, `002` needs an `agents` table that no migration
creates, and `086`, `103` and `104` refuse any database not named `personal_koi`. Measured 2026-11-02 on a fresh `eliza` database
with `vector` and `uuid-ossp`: the runner applied 143 files, 18 of them with statement
errors, warned that expected tables may be missing and exited 1. `scripts/setup.sh` runs
the runner at its Step 5 and stops there on that exit. It does not pass its `DB_*`
settings to the runner, which reads only `DATABASE_URL` and otherwise defaults to `eliza`
on localhost:5433.

`scripts/run_migrations_with_backup.sh --apply` does the same after taking a `pg_dump` backup
(without `--apply` it is a dry run that runs only read-only queries).
Both refuse databases that track migrations in `koi_migrations` (unless
`KOI_ALLOW_LEGACY_RUNNER=1`) or `koi_schema_migrations`.

## Docker

```yaml
# docker-compose.yml
services:
  postgres:
    image: pgvector/pgvector:pg14
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: eliza
    ports:
      - "5433:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Start the container (`docker compose up -d postgres`), then run the runner from the host
against `localhost:5433` as shown above.

## Troubleshooting

### "permission denied to create extension"

Create the extensions once as a superuser, then rerun the runner:

```bash
psql -U postgres -d <database> -c 'CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS "uuid-ossp";'
```

### "extension 'vector' does not exist"

Install pgvector: `sudo apt-get install postgresql-14-pgvector` (Debian/Ubuntu),
`brew install pgvector` (macOS), or build it from
[source](https://github.com/pgvector/pgvector).

### A run stopped part-way

For `personal_koi`, drop and recreate the database and run again. For other databases,
read the runner's output: it lists failed files and files applied with statement errors.
