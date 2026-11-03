"""
B9a Phase 5b — Embedding-based semantic classifier (Variant D).

Classifies queries by cosine similarity to precomputed category centroids.
Not a replacement for the LLM classifier — evaluated as an ensemble signal.

Uses text-embedding-3-small via an injected embed_fn to avoid coupling
to the app's module-level state.
"""

from __future__ import annotations

import math
from typing import Awaitable, Callable

# ---------------------------------------------------------------------------
# Exemplar questions per taxonomy category
# ---------------------------------------------------------------------------

ROUTE_EXEMPLARS: dict[str, list[str]] = {
    "entity_definition": [
        "What is eelgrass?",
        "What is the Salish Sea?",
        "What is bioregionalism?",
        "What are Southern Resident Killer Whales?",
        "What is Regenerate Cascadia?",
    ],
    "governance_policy": [
        "What is the BKC meta-protocol?",
        "What are OCAP principles?",
        "How does federation membrane governance work?",
        "What is the CommonsChange reference profile?",
        "What is FPIC and why is it relevant?",
        "What is the BKC pattern language?",
        "What is a node participation profile?",
    ],
    "commitment_claim": [
        "What is a commitment pool?",
        "How does the claims engine work?",
        "What are commitment routing scores?",
        "What is the lifecycle of a commitment?",
        "What is the relationship between commitments and flow funding?",
    ],
    "relationship_path": [
        "Which organizations work on restoration in the Salish Sea?",
        "What species are connected to Chinook salmon?",
        "What is the relationship between BKC and KOI-net?",
        "Which organizations are part of the Victoria Landscape Hub?",
    ],
    "roadmap_status": [
        "What is the current status of commitment pooling?",
        "What bioregional nodes are currently active?",
        "What is the status of the dual-bioregion pilot?",
        "What retrieval techniques does the BKC use?",
    ],
    "cross_node_provenance": [
        "What does the Greater Victoria node know about reef nets?",
        "What information has the Front Range node shared about bioregionalism?",
        "How do different nodes describe the Salish Sea?",
    ],
    "out_of_domain": [
        "What is the population of Mars?",
        "What is the stock price of Apple?",
        "How do I install TensorFlow?",
        "What is the capital of France?",
        "What are the lyrics to Bohemian Rhapsody?",
    ],
}


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# Module-level centroid cache
_CENTROIDS: dict[str, list[float]] | None = None


async def compute_centroids(
    embed_fn: Callable[[str], Awaitable[list[float]]],
) -> dict[str, list[float]]:
    """Compute mean-embedding centroids for each taxonomy category."""
    centroids: dict[str, list[float]] = {}
    for category, exemplars in ROUTE_EXEMPLARS.items():
        embeddings = [await embed_fn(q) for q in exemplars]
        dim = len(embeddings[0])
        n = len(embeddings)
        centroid = [
            sum(embeddings[j][i] for j in range(n)) / n for i in range(dim)
        ]
        centroids[category] = centroid
    return centroids


async def semantic_classify(
    query: str,
    embed_fn: Callable[[str], Awaitable[list[float]]],
    centroids: dict[str, list[float]] | None = None,
) -> tuple[str, float]:
    """Classify query by cosine similarity to category centroids.

    Returns (taxonomy_str, confidence) where confidence is derived
    from the margin between top and second-best match, scaled to 0.5-1.0.
    """
    global _CENTROIDS
    if centroids is None:
        if _CENTROIDS is None:
            _CENTROIDS = await compute_centroids(embed_fn)
        centroids = _CENTROIDS

    query_emb = await embed_fn(query)

    similarities = {
        cat: _cosine_similarity(query_emb, centroid)
        for cat, centroid in centroids.items()
    }
    ranked = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    top_taxonomy = ranked[0][0]
    margin = ranked[0][1] - ranked[1][1]
    confidence = min(0.5 + margin * 5, 1.0)

    return top_taxonomy, confidence
