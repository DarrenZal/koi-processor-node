# B9a — QueryPlan IR Specification

**Status:** Implemented (B9a+B9b+B9b.1+B9c). Decision matrix updated to match deployed code at `27c23daa`.
**Schema:** `api/schemas/query_plan.py`
**Architecture ref:** `BioregionalKnowledgeCommoning/docs/foundations/federated-memory-architecture.md`

## 1. Overview

B9a introduces the QueryPlan IR — a typed retrieval plan layer between the user query and the existing retrieval pipeline. The planner classifies queries, extracts entities, assembles an ordered plan of typed retrieval operations, and hands execution to trusted executors. The LLM never writes raw SQL.

## 2. Router Contract

Three layers, applied in order:

### Layer 1: PolicyScope (deterministic, no LLM)

- **Input:** Request context (currently: no auth identity)
- **Output:** `PolicyScope(visibility_tier="public", include_node_private=false)`
- **v0 implementation:** Hardcoded public scope with local-only behavior. Mirrors existing `node_private` filtering in `/chat` (personal_ingest_api.py lines 5199, 5299).
- **Future:** Derive from `commons_memberships` + edge governance tables when auth is wired.

### Layer 2: Query Classification (LLM-driven)

- **Input:** `original_query` + `PolicyScope`
- **Output:** `ClassifierOutput` (taxonomy + depth_tier + entities + reasoning)
- **Implementation:** GPT-4o-mini structured output (see classifier prompt template below)
- **Cost:** ~100-200 tokens input, ~50 tokens output, ~200ms

### Layer 3: Plan Assembly (deterministic, no LLM)

- **Input:** `ClassifierOutput`
- **Output:** `QueryPlan` with ordered `steps`
- **Implementation:** Decision matrix lookup (see §3)

## 3. Decision Matrix

**Current matrix** — reflects B9b.1+B9c tuning. Text-search-first + multi_query proved superior to STRUCTURED_SQL and RELATIONSHIP_TRAVERSE for commitment_claim and relationship_path (over-retrieval noise).

| QueryTaxonomy | Steps | Depth | Key Params | Notes |
|---------------|-------|-------|------------|-------|
| `entity_definition` | entity_lookup → text_search | standard | el=5, top_k=8 | — |
| `relationship_path` | entity_lookup(3) → text_search(multi_query) | standard | el=3, top_k=8 | B9c: RELATIONSHIP_TRAVERSE removed (over-retrieval) |
| `governance_policy` | text_search(multi_query) → entity_lookup → relationship_traverse(hops=1) | deep | el=8, top_k=12, rt=30 | B9d/B9d.1/B9d.2 tuning attempted, none beat default. Drag accepted. |
| `roadmap_status` | entity_lookup → structured_sql(roadmap) → text_search | standard | el=5, sql=15, top_k=6 | — |
| `commitment_claim` | entity_lookup(3) → text_search(multi_query) | standard | el=3, top_k=8 | B9b.1: STRUCTURED_SQL removed (over-retrieval) |
| `cross_node_provenance` | entity_lookup → text_search | standard | el=5, top_k=8 | — |
| `out_of_domain` | (none — immediate abstention) | — | — | — |

### Depth tier overrides

The classifier may recommend a depth tier. The plan assembly applies it:

| Depth | Effect |
|-------|--------|
| `shallow` | entity_lookup only. Skip text_search and relationship_traverse. |
| `standard` | Use steps from decision matrix as-is. |
| `deep` | Add `multi_query=true` to all text_search steps. Increase `max_results` by 50%. |

### governance_policy tuning closeout (B9d/B9d.1/B9d.2)

Governance retrieval was the last remaining category drag (-3.4% CR vs default). Three rounds of tuning were attempted:

- **B9d** (text-search-first, remove RELATIONSHIP_TRAVERSE): -3.4% CR, failed subset gate
- **B9d.1** (Variant A: text+entity=3, Variant B: text-only): Both lost. Variant B won on policy questions, lost on definitions. Revealed governance is internally mixed (definition vs policy sub-intents).
- **B9d.2** (within-category sub-routing): definition branch (ENTITY_LOOKUP→TEXT_SEARCH) massively improved over v7 planner (governance_4: 0.175→0.419, governance_5: 0.119→0.706, governance_9: 0.086→0.393). Policy branch still lost to default in both variants tested (text-only: -15.3% on governance_7; text+entity: -10.6%).

**Conclusion:** The planner's governance plan produces different retrieval composition from the default path, and no configuration of planner-controlled steps has matched default's governance performance. The overall planner still passes canonically (+6.2% CR) with governance drag included. No further governance tuning unless product priorities change.

## 4. Typed Retrieval Op Set

Each op maps to a trusted executor. All executors return `list[EvidenceBundle]`.

### Live ops (B9a)

#### entity_lookup
- **Wraps:** Semantic + keyword search on `entity_registry` (personal_ingest_api.py lines 5193-5248)
- **Params:** `query: str`, `max_results: int`, `entity_type: str | None`
- **Returns:** EvidenceBundles with `source_type=LOCAL_AUTHORITATIVE`

#### relationship_traverse
- **Wraps:** N-hop recursive CTE on `entity_relationships` (`retrieval_executors.py`)
- **Params:** `entity_uris: list[str]`, `max_hops: int = 1`, `predicate_filter: list[str] | None`
- **Returns:** EvidenceBundles with `source_type=LOCAL_AUTHORITATIVE`, text = "subject --[predicate]--> object"
- **Used by:** governance_policy (max_hops=1, max=30). Removed from relationship_path in B9c (over-retrieval even at 1-hop).

#### text_search
- **Wraps:** BM25+vector RRF fusion + FlashRank rerank (lines 5310-5448)
- **Params:** `query: str`, `multi_query: bool = false`, `include_code: bool = false`, `top_k: int = 8`
- **Returns:** EvidenceBundles with `source_type=LOCAL_DOCUMENT`

#### structured_sql (B9b — live)
- **Wraps:** Template-driven SQL queries against `commitment_registry`, `entity_registry` (roadmap types)
- **Params:** `template: str`, `entity_uris: list[str] | None`, `max_results: int`
- **Returns:** EvidenceBundles with `source_type=LOCAL_AUTHORITATIVE`
- **Used by:** roadmap_status. Removed from commitment_claim in B9b.1 (over-retrieval).

### Stub ops (future phases)

#### graph_query
- **Future:** Cypher via Apache AGE for typed pattern matching
- **Current behavior:** Returns empty list

#### peer_query (B12)
- **Future:** Federation fan-out to eligible peers. Requires PolicyScope.eligible_peers (not in v0).
- **Current behavior:** Returns empty list

## 5. Classifier Prompt Template

```
You are a query classifier for a bioregional knowledge commons (BKC).
Given a user question, classify it into exactly one taxonomy category
and extract any entity mentions.

## Taxonomy

- entity_definition: "What is X?" — asking for a definition or description of a single concept, species, place, organization, or person.
- relationship_path: "How does X relate to Y?" — asking about connections, relationships, or multi-hop paths between entities.
- governance_policy: "What is the policy on X?" — asking about protocols, governance rules, data sovereignty, meta-protocol, or decision-making frameworks.
- roadmap_status: "What is the status of X?" — asking about project status, milestones, work items, timelines.
- commitment_claim: "What commitments exist for X?" — asking about pledges, claims, evidence, commitment pools, flow funding settlements.
- cross_node_provenance: "What does node Y know about X?" — asking about information from a specific bioregional node or cross-node comparison.
- out_of_domain: Questions about topics outside bioregional knowledge (technology installs, stock prices, general knowledge unrelated to ecology/governance/stewardship).

## Depth

- shallow: Simple lookup, single entity, well-known concept
- standard: Typical question requiring entity + document search
- deep: Complex question requiring multiple search strategies or synthesis

## Entities

Extract named entities mentioned in the question. Include:
- Species, ecosystems, locations, bioregions
- Organizations, people, projects
- Concepts, protocols, practices
- Specific items like "commitment pool", "Victoria Landscape Hub"

## Output format (JSON)

{
  "query_taxonomy": "<one of the 7 categories>",
  "depth_tier": "shallow | standard | deep",
  "entities": [
    {"name": "<entity name>", "type": "<entity type or null>"}
  ],
  "reasoning": "<1 sentence explaining classification>"
}
```

## 6. Acceptance Criteria

### Design-complete (this document)

1. Schema committed: all models in `api/schemas/query_plan.py` import cleanly and export valid JSON Schema.
2. Decision matrix spec'd: each taxonomy maps to ordered live ops with future stubs documented separately.
3. Router contract documented: Layer 1 (PolicyScope v0), Layer 2 (classifier prompt), Layer 3 (plan assembly rules).
4. Classifier prompt template drafted with taxonomy definitions, examples, and output format.
5. This spec doc committed.

### Implementation-complete (follow-up session)

6. Classifier works: query string → `ClassifierOutput` via GPT-4o-mini structured output.
7. Plan assembly works: `ClassifierOutput` → valid `QueryPlan` with correct step ordering per decision matrix.
8. 3 executors work: entity_lookup, relationship_traverse, text_search produce `EvidenceBundle[]`.
9. 3 executors stub: graph_query, structured_sql, peer_query return empty lists with log messages.
10. End-to-end path: `POST /chat?planner=true` uses QueryPlan path, same answer quality, plan/trace in response.
11. Trace captured: StepTrace populated with timing, result counts, errors.
12. Safety guards enforced: max_steps, max_tokens, timeout_ms respected; violations logged and halt execution.
13. Eval regression: B5.5 eval scores do not regress with `planner=true` (within 5% of baseline).
14. No B9b/B9c implementation: structured_sql and graph_query are stubs only.
