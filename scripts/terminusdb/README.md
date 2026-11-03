# TerminusDB Evaluation — Phase 0 Results

## Summary

**Verdict: GO** — TerminusDB v12 is viable as a graph storage layer for KOI knowledge graph.

Both Phase 0a (single-instance) and Phase 0b (two-instance federation) passed all tests.

## Phase 0a: Single Instance (10/10 PASS)

| Metric | Result | Threshold |
|--------|--------|-----------|
| Import fidelity | 830/830 entities, 114/114 assertions | Exact match |
| Assertion hash idempotency | Identical on re-compute | Same hashes |
| P95 query latency | 21.9ms | < 100ms |
| Import time | 0.9s (fresh), 9.0s (with re-import) | < 60s |
| RAM | 60.7 MiB | < 512 MiB |
| Schema hash stability | Identical across fresh instances | No false drift |

### Test Results

| # | Test | Result |
|---|------|--------|
| 1 | Non-conflicting assertions merge | PASS |
| 2 | Conflicting literals (both preserved) | PASS |
| 3 | New entity on branch | PASS |
| 4 | New assertion linking entities | PASS |
| 5 | SameAs mapping | PASS |
| 6 | Diff/history readability | PASS |
| 7 | Time-travel queries | PASS |
| 8 | Conflict detection query | PASS |
| 9 | Status transition validation | PASS |
| 10 | Schema canonicalization | PASS |

## Phase 0b: Two-Instance Federation (8/8 PASS)

| Metric | Result |
|--------|--------|
| Schema parity after clone | Identical hashes |
| Data replication | All entities transferred |
| Divergent edits | Both instances can edit independently |
| Document transfer | Assertion hashes consistent across instances |
| Schema divergence detection | SchemaVersionMismatch raised correctly |
| Combined RAM | 102.8 MiB (two instances) |

## Key Findings

### What Works Well

1. **Branch/merge (rebase)** — clean, deterministic. Non-conflicting changes merge seamlessly.
2. **Conflict preservation** — conflicting assertions (e.g., founded_year: 2017 vs 2018) are BOTH preserved after merge. No silent overwrites.
3. **LexicalKey uniqueness** — deterministic document IDs from field values work correctly for deduplication.
4. **Time-travel** — querying at earlier commits returns correct historical state.
5. **Schema canonicalization** — stable hash across fresh instances, no false drift.
6. **Performance** — sub-30ms P95 queries, sub-1s import for 830 entities + 114 assertions, only 61 MiB RAM.

### Known Limitations

1. **Python SDK `clonedb()`/`push()`/`pull()`** — has a variable scoping bug (`headers`) when connecting between local Docker instances. Workaround: manual document transfer (which validates hash consistency). May work correctly with actual remote URLs.
2. **Diff API** — `client.diff()` requires documents with the same `@id`; cannot diff commit objects directly. Commit history provides change tracking.
3. **TerminusDB uses "rebase" not "merge"** — replays original commits rather than creating merge commits. Functionally equivalent but commit history looks different from git merge.
4. **`list` type not supported** — TerminusDB requires `Set[str]` from typing, not bare `list` in schema definitions.

## Architecture Decisions Validated

- **Assertion model with deterministic hash**: Works. Same fact → same hash, idempotent re-import.
- **Canonical object serialization**: `normalize_literal()` prevents false conflicts (e.g., `02017` vs `2017`).
- **Status lifecycle**: Transition rules enforced in code. Terminal states (superseded, retracted) have no outbound transitions.
- **Schema version tracking**: `SchemaVersion` + `compute_schema_hash()` provides reliable drift detection.
- **Conflict = query, not persistence**: Grouping active assertions by (subject, predicate) and comparing canonical object keys correctly identifies conflicts.

## Files

| File | Purpose |
|------|---------|
| `schema.py` | TerminusDB schema + canonical functions |
| `import_from_postgres.py` (not in this export) | PostgreSQL → TerminusDB import |
| `test_merge.py` | 10 single-instance tests |
| `test_federation.py` | 8 two-instance tests |
| `run_phase0.sh` (not in this export) | Test harness (0a/0b/all/clean) |

## Usage

These scripts need Docker and the `terminusdb-client` Python package, which
`requirements.txt` does not install (it was removed as unused, and it does not support
Python 3.13+). Install it into the venv first: `pip install terminusdb-client`.

The Phase 0 harness, `scripts/terminusdb/run_phase0.sh` (not in this export), and the
importer it runs, `scripts/terminusdb/import_from_postgres.py` (not in this export), are
left out: with no arguments they start a TerminusDB container and copy the default
`personal_koi` database into it. To run the tests, start a throwaway TerminusDB v12
container on port 6363 (and a second one on 6364 for the two-instance tests), then run
`python -m scripts.terminusdb.test_merge` and `python -m scripts.terminusdb.test_federation`
from the repository root. Each test script deletes and recreates its own test database
(`koi_merge_test`, `koi_federation_test`) and writes `results.json` in this directory.

## Phase 1: Live Integration (Outbox Pattern)

**Status: PASS** — Validated 2026-02-25.

Phase 1 wires TerminusDB into the live `personal_ingest_api.py` via an outbox pattern:
PG writes are authoritative, outbox rows are enqueued in the same transaction,
and an async worker drains them to TerminusDB.

### Architecture

```
register-entity / sync-relationships
        │
        ▼
  ┌─────────────────────┐
  │  PostgreSQL (PG)    │  ← authoritative
  │  + terminusdb_outbox│  ← enqueued in same txn
  └─────────┬───────────┘
            │ (async, 2s poll)
            ▼
  ┌─────────────────────┐
  │  outbox_worker.py   │  claim → apply → mark applied (not in this export)
  └─────────┬───────────┘
            │
            ▼
  ┌─────────────────────┐
  │  TerminusDB         │  ← graph mirror
  └─────────────────────┘
```

The outbox worker, `scripts/terminusdb/outbox_worker.py` (not in this export), is left
out: with no arguments it connects to the default `personal_koi` database and drains its
outbox in a loop.

### Key Properties

| Property | Implementation |
|----------|---------------|
| **Fail-open** | PG writes always succeed; outbox accumulates when TDB is down |
| **Recovery** | Worker retries with exponential backoff (jitter); drains backlog on TDB restart |
| **Idempotency** | `LexicalKey` on Entity(rid) / Assertion(assertion_hash) — same doc updated in place |
| **Conflict detection** | `/graph/conflicts` groups assertions by (subject, predicate), surfaces multi-value divergence |
| **Auth guard** | `/graph/*` endpoints restricted to localhost + the WireGuard subnet |
| **Schema guard** | Adapter checks schema hash on startup; fails fast on mismatch |
| **Reconciliation** | `reconcile.py` compares PG↔TDB counts and detects drift |

### Smoke Test

The Phase 1 smoke test, `scripts/terminusdb/smoke_phase1.sh` (not in this export), is
left out: it reads `config/personal.env` and writes test entities into that node, with no
cleanup.

### Files

| File | Role |
|------|------|
| `api/personal_ingest_api.py` | API + outbox write points + `/graph/*` endpoints |
| `api/terminusdb_adapter.py` | Adapter with schema guard + idempotent upserts |
| `api/vault_parser.py` | Relationship sync (SAVEPOINT isolation for FK failures) |
| `scripts/terminusdb/outbox_worker.py` (not in this export) | Async worker draining outbox → TDB |
| `scripts/terminusdb/schema.py` | Entity(rid), Assertion schema |
| `scripts/terminusdb/import_from_postgres.py` (not in this export) | Bulk import with `--fresh` flag |
| `scripts/terminusdb/reconcile.py` | Drift detection + `--repair` |
| `scripts/terminusdb/smoke_phase1.sh` (not in this export) | Automated smoke test |
| `migrations/048_terminusdb_outbox.sql` | Outbox table DDL |

### Starting the Stack

The API listens on 127.0.0.1 here. Most routes have no authentication, so bind it to
another interface only behind a firewall or an authenticating proxy (see "Access control"
in the repository README).

```bash
cd <koi-processor checkout>

# API (Terminal 1)
( set -a; source config/personal.env; set +a
  venv/bin/uvicorn api.personal_ingest_api:app --host 127.0.0.1 --port 8351 )
```

The outbox worker that drains the mirror's queue is not in this export (see Architecture
above).

**Important**: Use `set -a; source config/personal.env; set +a` (not `export $(grep ...)`)
to safely load env vars with spaces/quotes.

### Bug Fix: vault_parser.py SAVEPOINT Isolation (2026-02-25)

FK constraint violations on `pending_relationships` (e.g., unregistered predicate)
aborted the entire PG transaction, causing subsequent outbox enqueue to fail with
`InFailedSQLTransactionError`. Fixed by wrapping relationship INSERTs in
`SAVEPOINT`/`ROLLBACK TO SAVEPOINT` so individual failures are isolated without
aborting the enclosing transaction.

### Verified Metrics

| Metric | Value |
|--------|-------|
| Entities (PG=TDB) | 833 |
| Assertions (PG=TDB) | 114 |
| Drift | 0 |
| Schema hash | `206e7ff0a60f...` |
| Worker recovery time | < 15s after TDB restart |
| Auth: localhost | 200 |
| Auth: LAN IP | 403 |
