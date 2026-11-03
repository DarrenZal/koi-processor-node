# ADR-001: Graph Database Strategy — PostgreSQL-First for Commons Correctness

## Status: ACCEPTED (provisional)

**Decision owner:** Darren Zal
**Review owner:** Darren Zal
**Initial review:** 2026-03-11
**Cadence:** Monthly re-check (next: 2026-04-11) until status changes to SUPERSEDED or FINAL

## Context

TerminusDB Phase 1 integration is code-complete and smoke-validated but not in production use. PostgreSQL recursive CTEs handle all current graph queries at sub-3ms latency. We evaluated whether to invest further in TerminusDB (Phase 2), switch to Neo4j, adopt Graphiti, or stay PG-only.

Key facts:
- A personal node: under a thousand entities and relationships
- A production node: about 30,000 entities and 15,000 edges — trivial for PG
- PG recursive CTEs in `graph_queries.py` serve all current queries at sub-3ms
- TerminusDB adapter is ~500 LOC behind a fail-open flag (zero operational burden when off)
- Apache AGE already provides Cypher-in-Postgres via `code_graph.py` in Octo
- Graphiti is an **agent-memory framework** on top of a graph backend (Neo4j, FalkorDB, Neptune, Kuzu) — not a database alternative
- Kuzu repo is archived/read-only — high risk, not recommended for core despite Graphiti driver support
- FalkorDB is SSPL licensed — de-prioritized unless licensing is acceptable

## Decision

**2026 H1 default: PostgreSQL-first for commons correctness.**

Two explicit planes with different responsibilities:

### Commons Data Plane (source of truth)
PostgreSQL + KOI events + provenance/history tables.

1. **Keep PostgreSQL as canonical graph store.** CTEs + pgvector handle current scale. Apache AGE (already in Octo) provides Cypher without adding infrastructure if richer graph patterns are needed.

2. **Park TerminusDB Phase 2.** Keep Phase 1 code (adapter + outbox worker) as-is in trunk behind fail-open flag (`TERMINUSDB_ENABLED=false`). Tag milestone, create parking branch, no active development unless unpark triggers fire.

3. **Add bi-temporal + provenance assertion history schema.** PG tracks assertions with two explicit temporal dimensions:

   - **Transaction time** — when the DB recorded the assertion (system-managed, immutable)
   - **Valid time** — when the fact was/is true in the real world (user-managed)

   Required fields:

   ```
   assertion_id             UUID PRIMARY KEY
   subject                  TEXT NOT NULL        -- entity URI
   predicate                TEXT NOT NULL        -- relationship type
   object_uri               TEXT                 -- entity URI (NULL if literal)
   object_literal           TEXT                 -- literal value (NULL if URI)
   object_datatype          TEXT                 -- XSD datatype for literal (NULL if URI)
   object_lang              TEXT                 -- language tag for literal (NULL if URI or untagged)
   -- CHECK: exactly one of object_uri, object_literal is NOT NULL
   asserted_by_node_rid     TEXT NOT NULL        -- peer node RID (stable identifier)
   -- Transaction time (system-managed, immutable once written)
   tx_recorded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()  -- when DB recorded this
   tx_retracted_at          TIMESTAMPTZ          -- when assertion was retracted (NULL = active)
   -- Valid time (user/domain-managed)
   valid_from               TIMESTAMPTZ          -- when fact became true in world
   valid_to                 TIMESTAMPTZ          -- when fact ceased to be true (NULL = still valid)
   -- Provenance
   supersedes_assertion_id  UUID                 -- FK to prior assertion this replaces
   provenance_doc_rid       TEXT                 -- source document RID
   -- Cross-peer replay idempotency
   source_event_id          UUID                 -- originating KOI-net event (dedup key)
   source_node_rid          TEXT                 -- node that originated the event (optional)
   ```

   **Invariant:** No destructive overwrite of assertions. Retraction sets `tx_retracted_at`; correction creates new assertion with `supersedes_assertion_id` pointing to the prior one.

   **DB-enforced constraints:**

   Immutability (trigger rejects UPDATE):
   - `tx_recorded_at` — immutable after INSERT, never changed
   - `tx_retracted_at` — write-once (NULL → timestamp, never changed again)

   Temporal (CHECK constraints):
   - `tx_retracted_at IS NULL OR tx_retracted_at >= tx_recorded_at`
   - `valid_to IS NULL OR valid_to >= valid_from`

   Object type (CHECK):
   - Exactly one of `object_uri`, `object_literal` is NOT NULL

   Replay idempotency (partial unique index):
   - `UNIQUE (source_node_rid, source_event_id) WHERE source_node_rid IS NOT NULL AND source_event_id IS NOT NULL`
   - Replaying the same event from the same node does not create duplicate assertions

   Active assertion dedup (partial unique index):
   - `UNIQUE (subject, predicate, COALESCE(object_uri, object_literal), asserted_by_node_rid) WHERE tx_retracted_at IS NULL`
   - Prevents duplicate active assertions from the same node for the same triple

### Agent Memory Plane (not source of truth)
Optional Graphiti+Neo4j prototype — sidecar experiment only.

4. **Graphiti+Neo4j as optional R&D prototype.** Purpose: evaluate whether Graphiti's temporal memory and hybrid search meaningfully improve Claude agent retrieval quality on real tasks. Not a core DB replacement, not source of truth.

5. **Revisit at 2026-03-11 with hard benchmarks** tied to real use-cases.

## Non-Goals

- **No replacement of PG as source of truth in H1.** Any new system is additive/sidecar only.
- **No Graphiti write-back into canonical graph during prototype.** Graphiti reads from PG export; it does not write back. The commons data plane is not polluted by prototype data.

## TerminusDB Unpark Trigger

TerminusDB Phase 2 pilot starts **if and only if**:
- 3+ peers with recurring claim conflicts (multi-peer divergence becomes routine)
- KOI-net event model proves insufficient for conflict-preserving federation
- Branch/merge semantics are needed beyond what PG provenance tables provide

## Rationale

- At current scale, infrastructure complexity is the bigger risk than query performance
- PG-first is stable, proven, and already running
- Commons correctness requires: conflict preservation without overwriting, full provenance/audit replay across peers, sovereign local edits + deterministic reconciliation — all achievable in PG with proper schema
- AGE gives a no-new-infra path to Cypher expressiveness if CTEs become limiting
- Graphiti prototype is low-commitment (Docker + pip install) and answers the right question: "does agent memory quality improve?"
- TerminusDB's unique value (branch/merge/conflict-preserving federation) isn't needed until multi-peer divergence is routine

## What We're Deferring

| Item | Revisit When |
|------|-------------|
| TerminusDB Phase 2 | 3+ peers with recurring claim conflicts |
| Neo4j standalone | Only if AGE insufficient AND dedicated graph DB needed |
| Graphiti core integration | Only if prototype exceeds promotion thresholds |
| FalkorDB | Only if SSPL licensing is acceptable |
| Kuzu | High risk (archived repo) — not recommended |

## Decision Gates

### Promotion Thresholds

Any system must meet ALL of these to be promoted beyond prototype:

| Metric | Threshold |
|--------|-----------|
| Correctness | >= 95% on benchmark queries |
| Completeness | >= 90% on benchmark queries |
| p95 latency regression | <= 25% vs PG baseline |
| Operational overhead | <= 1 new always-on service |
| Agent-quality improvement (Graphiti) | >= 20% on retrieval/answer quality to justify sidecar |

### Commoning-Specific Gates

These gates test commons correctness, not just latency:

| # | Gate | What it tests | Pass criteria |
|---|------|--------------|---------------|
| C1 | Conflicting claims | Two peers extract conflicting entity types for same name | Preserve both claims with provenance, no silent overwrite |
| C2 | Provenance replay | Full audit trail of who asserted what, when | Complete replay from assertion history |
| C3 | Sovereign edits | Local peer edits entity, remote peer has different version | Deterministic reconciliation with both versions preserved |

### Mandatory Federation Conflict Drill (before 2026-03-11)

Simulate conflicting claims across 3 peers before the review date:
1. Peer A asserts "Regen Network" is type ORGANIZATION
2. Peer B asserts "Regen Network" is type PROJECT
3. Peer C asserts "Regen Network" is type DAO
4. **Pass:** All three assertions preserved with provenance. Reconciliation is deterministic. No silent overwrite.

### Retrieval & Query Gates

| # | Category | Query/Task | What it tests |
|---|----------|-----------|---------------|
| 1 | Multi-hop retrieval | "What organizations are connected to Gregory Landua through 3+ hops?" | Graph traversal depth |
| 2 | Temporal memory | "What changed about Regen Network's governance between Jan and Feb 2026?" | Temporal reasoning |
| 3 | Agent answer quality | "Summarize the relationship between BKC and Regen Network" | Entity-aware retrieval |
| 4 | Semantic + graph hybrid | "Find documents about carbon credits involving organizations in BC" | Hybrid search |
| 5 | Reverse lookup | "What meetings mention both DFO and herring?" | Bidirectional linking |
| 6 | Path discovery | "How is Soren Greywell connected to Knowledge Commoning?" | Shortest path |
| 7 | Subgraph extraction | "Show me the full context around Landscape Hub" | Neighborhood query |
| 8 | Temporal edge | "When was the relationship between X and Y first established?" | Edge temporality |
| 9 | Agent memory recall | "What did we discuss about federation in the last 3 sessions?" | Session memory |
| 10 | Cross-peer retrieval | "What does a peer's node know about X that mine doesn't?" | Federation awareness |

### Evaluation Process

1. Run all gates against PG baseline (current CTEs + pgvector)
2. Run applicable ones against Graphiti+Neo4j prototype
3. Score against promotion thresholds
4. **Promote only if**: all thresholds met AND operational burden acceptable
5. **Otherwise**: keep PG core, invoke rollback/cleanup rule

## Rollback / Cleanup Rule

If promotion thresholds are not met after prototype evaluation:
- Remove prototype services (Neo4j Docker, Graphiti scripts) within 48h
- ADR decision remains unchanged — PG-first continues
- Prototype artifacts deleted, not parked (unlike TerminusDB which stays parked behind flag)
- Document findings in ADR appendix for future reference

## Implementation Plan

### Phase 1: ADR + TerminusDB Parking (this session)

1. Commit ADR to `docs/ADR-001-graph-db-strategy.md`
2. Tag TerminusDB Phase 1 milestone and create parking branch
3. Verify defaults: `TERMINUSDB_ENABLED=false` in env
4. Monthly Terminus smoke test (or before unpark decision)

### Phase 2: Graphiti+Neo4j Prototype (optional, ~1 day when ready)

Only if user decides to proceed with R&D prototype:

1. Docker compose with `neo4j:5-community`
2. `pip install graphiti-core`
3. Export 100 entities + relationships from PG
4. Feed through Graphiti ingestion
5. Run applicable benchmark queries (2, 3, 4, 8, 9)
6. Compare against PG baseline and promotion thresholds

### Phase 3: Federation Conflict Drill (before 2026-03-11)

Simulate 3-peer conflicting claims against PG assertion history schema.
Must pass C1-C3 gates before review date.

### Phase 4: Decision Review (2026-03-11)

Review benchmark results against all gates and promotion thresholds. Either:
- Promote Graphiti to agent memory sidecar (not source of truth)
- Keep PG-only and invoke rollback/cleanup rule
- Unpark TerminusDB if multi-peer divergence trigger is met
- Continue provisional status with next review at 2026-04-11

## Verification

1. Commit ADR to `docs/ADR-001-graph-db-strategy.md`
2. Verify TerminusDB adapter remains behind fail-open flag (already true)
3. Run federation conflict drill (3-peer simulation)
4. If prototyping: run Graphiti quickstart, execute benchmark queries, score against thresholds
5. First review at 2026-03-11, monthly thereafter

## Appendix A: Graphiti Decision (2026-03-04)

**Decision:** _PENDING — COMMIT or DEFER_

**Rationale:**

- Is there a concrete agent retrieval quality question we need answered? _TBD_
- Do we have bandwidth for ~1 day of prototype work Mar 5–8? _TBD_
- If no to either → DEFER

_To be completed by 2026-03-04._

