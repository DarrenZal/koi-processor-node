KOI Processor Core - Operations
===============================

This directory contains core scripts and services for refining the KOI knowledge graph and powering adaptive NL→SPARQL.

Key scripts and services
- refine_graph.py: Consolidate predicates, deduplicate, generate CAT receipts, and write refined TTL.
  - Optional: Adds regx:canonicalPredicate using CANONICAL_PATH or heuristics.
- compute_predicate_communities.py: Builds predicate co-occurrence graph and communities for graph-aware retrieval.
- embedding_service.py: Lightweight HTTP service for predicate similarity using precomputed embeddings.
  - Endpoints: GET /health, POST /similar, POST /schema

Environment variables
- CONSOLIDATION_PATH: Path to final consolidation JSON (prefers t=0.25).
- PATTERNS_PATH: Path to predicate_patterns.json.
- EMBEDDINGS_PATH: Path to predicate_embeddings.pkl.
- CANONICAL_PATH: Path to canonical_predicates.json (optional).

Usage
1) Refine graph
   python3 refine_graph.py
   - Produces refined_graph_*.ttl, cat_receipts_*.json, refinement_results.json

2) Compute predicate communities
   python3 compute_predicate_communities.py
   - Writes predicate_communities.json used by MCP adaptive retrieval

3) Start embedding service (dev)
   python3 embedding_service.py
   - Default port 8095; set OPENAI_API_KEY to use OpenAI embeddings for query text (optional)

Running it as a service
- No systemd unit file is included. To run the embedding service under systemd, write a
  unit whose ExecStart runs python3 embedding_service.py from this directory.

Canonical predicates
- Provide a file canonical_predicates.json (see canonical_predicates.example.json) to map consolidated predicates to 5–20 buckets for dashboards/rollups.
- refine_graph.py will add regx:canonicalPredicate per statement when available.

