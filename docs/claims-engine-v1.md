# Claims Engine — Implementation Reference

**Status:** V1 Complete + V2 Hardening Deployed (Mar 9, 2026) — live on regen-upgrade testnet
**Target:** Internal dogfooding (Phase 3)

## Scope

Lightweight, ledger-anchored impact claim system. V1 covers:
- Lean core schema with extensible JSONB metadata
- CRUD + verification state machine
- Entity graph integration (claims as first-class entities)
- AI-powered claim extraction from documents
- Content hashing + live ledger anchoring via regen CLI (regen-upgrade testnet)
- MCP tools for Claude Code integration (including `anchor_claim`)

## Schema Design

Three authoritative sources:
1. **Smith/Bennetts** — Claims are "self-stable assertions" with structure (evidence + verification)
2. **Impact Registry using DLT** — `impactClaim = {claimStatement, claimEvidence[], claimAuthor}`
3. **The "atomic primitive"** — "Name it, vault it, map it, value it"

### Core Table: `claims`

| Column | Type | Purpose |
|--------|------|---------|
| claim_rid | TEXT UNIQUE | Content-addressable RID: `orn:koi-net.claim:<hash>` |
| entity_uri | TEXT | entity_registry.fuseki_uri (claim as graph entity) |
| claimant_uri | TEXT NOT NULL | Who makes the claim |
| statement | TEXT NOT NULL | Plain-language impact assertion |
| claim_type | TEXT | ecological, social, financial, governance |
| verification | TEXT | self_reported → peer_reviewed → verified → ledger_anchored |
| source_document | TEXT | Provenance: document RID or path |
| ai_confidence | FLOAT | NULL if manually created |
| content_hash | TEXT | BLAKE2b-256 for ledger anchoring |
| supersedes_rid | TEXT | Previous version (append-only versioning) |
| metadata | JSONB | Extensible: quantity, dates, SDGs, methodology, etc. |

### Predicates

| Predicate | Subject → Object | Purpose |
|-----------|-----------------|---------|
| makes_claim | Person/Org → Claim | Claimant relationship |
| evidences_claim | Evidence → Claim | Evidence attachment |
| supersedes_claim | Claim → Claim | Version chain |
| about | Claim → Location/Org/etc | Claim subject (reuses existing) |

## API Contract

**Base:** `POST/GET /claims/...` on personal KOI API (localhost:8351)

| Method | Path | Purpose |
|--------|------|---------|
| POST | /claims/ | Create claim (entity reg + graph edges + SQL) |
| GET | /claims/{rid} | Get claim with linked evidence |
| GET | /claims/ | List/search with filters |
| PATCH | /claims/{rid}/verify | Advance verification level |
| POST | /claims/{rid}/evidence | Attach evidence entity |
| GET | /claims/{rid}/history | Verification audit log |
| POST | /claims/extract | AI extraction from document text |
| POST | /claims/{rid}/prepare-anchor | Compute content hash + predict IRI (non-broadcasting) |
| POST | /claims/{rid}/anchor | Anchor verified claim on Regen Ledger (200 on success, 202 if broadcast times out) |
| POST | /claims/{rid}/reconcile | Check on-chain status of timed-out broadcast |

## Verification State Machine

```
self_reported → peer_reviewed → verified → ledger_anchored (terminal)
     ↓              ↓
  withdrawn      withdrawn
  (terminal)     (terminal)
```

## RID Strategy

Content-addressable, append-only:
- `claim_rid = orn:koi-net.claim:{sha256(canonical_json(about_uri, claimant, claim_type, metadata, statement))[:16]}`
- `about_uri` is part of the identity — same statement about different entities produces distinct RIDs
- `about_uri` is validated (must exist in entity_registry) before entering the hash
- Same content → same RID → idempotent (concurrent duplicates caught via unique constraint)
- Any field change → new RID → new row + `supersedes_rid` link

## MCP Tools (personal-koi-mcp)

| Tool | Backend Call |
|------|-------------|
| create_claim | POST /claims/ |
| search_claims | GET /claims/ |
| get_claim | GET /claims/{rid} |
| verify_claim | PATCH /claims/{rid}/verify |
| extract_claims | POST /claims/extract |
| link_evidence | POST /claims/{rid}/evidence |
| anchor_claim | POST /claims/{rid}/anchor |
| reconcile_claim | POST /claims/{rid}/reconcile |

## Build Status

- [x] Phase 0: Implementation doc
- [x] Phase 1: Schema + migration (064_claims_engine.sql)
- [x] Phase 1b: Router + graph integration (claims_router.py)
- [x] Phase 2: Claim extraction pipeline (claim_extractor.py)
- [x] Phase 3: MCP tools (personal-koi-mcp)
- [x] Phase 4: Ledger anchoring — live testnet via regen CLI (ledger_anchor.py)
- [x] Phase 5: Testing (46 smoke tests + 16 pytest passing)
- [x] Phase 6: V2 Hardening — ghost anchor fix, reconcile endpoint, 202 pending responses

## Ledger Anchoring

Claims at `verified` state can be anchored on the Regen Ledger `regen-upgrade` testnet.

**Flow:**
1. `POST /claims/{rid}/prepare-anchor` — computes BLAKE2b-256 content hash, derives predicted IRI via `regen` CLI
2. `POST /claims/{rid}/anchor` — broadcasts `MsgAnchor` to testnet, polls for confirmation, transitions to `ledger_anchored`

**Implementation:** CLI subprocess (`regen tx data anchor`) — no Python signing library needed. Key material stays in the regen keyring (`--keyring-backend test`).

**Env vars** (in `personal.env` — no mnemonic!):
- `REGEN_CHAIN_ID=regen-upgrade`
- `REGEN_RPC_URL=https://rpc-regen-upgrade.vitwit.com/`
- `REGEN_REST_URL=https://api-regen-upgrade.vitwit.com/`
- `REGEN_KEY_NAME=claims-service`

**Verification:** `GET https://api-regen-upgrade.vitwit.com/regen/data/v2/anchor-by-iri/{iri}`

## V2 Hardening (Mar 9, 2026)

### Ghost Anchor Bug Fix

**Problem:** `broadcast_anchor()` polled for tx confirmation up to 30s. On timeout, it returned `ready_to_anchor: True` with `ledger_timestamp: None`. The router then transitioned the claim to `ledger_anchored` without confirming the tx landed on-chain — creating ghost anchors.

**Fix:** Timeout now returns `ready_to_anchor: False` with `tx_hash` for later reconciliation. The claim stays at `verified` state.

### Reconcile Endpoint

`POST /claims/{rid}/reconcile` — for claims with a `tx_hash` but still at `verified` state:

| On-chain status | Action | Response |
|-----------------|--------|----------|
| Tx confirmed + anchor queryable | Transition to `ledger_anchored` | `status: "anchored"` |
| Tx confirmed + anchor not indexed | Finalize (tx confirmation sufficient) | `status: "anchored"` |
| Tx failed (code != 0) | Clear tx_hash + ledger_iri | `status: "failed"` |
| Tx not found (not indexed yet) | Preserve tx_hash | `status: "pending"` |

### Anchor Response Behavior

`/anchor` returns `AnchorResponse` (HTTP 200) when broadcast succeeds. Tx confirmation
is treated as sufficient — IRI verification via REST is skipped because most public
endpoints don't support Data Module queries.

`/anchor` returns `AnchorPendingResponse` (HTTP 202) only when broadcast times out
(tx_hash present but no confirmation). Client calls `/reconcile` to finalize.

`/reconcile` also finalizes on tx confirmation (code=0) regardless of REST IRI
queryability. The audit log records whether IRI verification succeeded or was skipped.

### Schema Changes

- Migration 065: `ALTER TABLE claims ADD COLUMN IF NOT EXISTS tx_hash TEXT`
- `tx_hash` exposed in `ClaimResponse` model and MCP `get_claim` tool

### Key Files

| File | Changes |
|------|---------|
| `api/ledger_anchor.py` | Timeout fix, `verify_anchor_onchain()`, `query_tx_status()` (never raises) |
| `api/routers/claims_router.py` | `AnchorPendingResponse`, `ReconcileResponse`, `/reconcile` endpoint, 202 paths |
| `tests/test_claims_reconcile.py` | 16 pytest tests (in-process ASGI + monkeypatch) |
| `scripts/test_claims_api.py` (not in this export) | 4 HTTP smoke tests (tests 17-20) |

### Testing

```bash
# Pytest (monkeypatched, no live network needed)
pytest tests/test_claims_reconcile.py -v
```

The HTTP smoke suite, `scripts/test_claims_api.py` (not in this export), is left out: it
writes claims through the API it is given, and its cleanup connects to the database
`KOI_LIVE_POSTGRES_URL` or `POSTGRES_URL` names, not necessarily the API's.

## What V1 Does NOT Cover

- User wallet integration (V2: MsgAttest multi-party)
- Dashboard UI
- COMET Planner PDF parsing
- Credit issuance (x/ecocredit)
- Multi-tenant / multi-org
- URDNA2015 graph canonicalization
- Mixed evidence types (document RIDs, external URLs)
