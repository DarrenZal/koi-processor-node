"""
B9a — Query classifier.

Layer 2 of the router: classifies a user query into a QueryTaxonomy
category via GPT-4o structured output. Returns ClassifierOutput
with confidence score for fallback gating.

Phase 5b: Tuned prompt with contrastive disambiguation, hardened OOD,
confidence recalibration, and post-classifier OOD-recovery guardrail.
Model upgraded from gpt-4o-mini to gpt-4o (zero regressions, 96.2% accuracy).

See: docs/specs/b9a-query-plan-spec.md §5 for the prompt template.
"""

from __future__ import annotations

import json
import logging
import os

from api.chat_provider import ChatProvider
from api.schemas.query_plan import (
    ClassifierOutput,
    DepthTier,
    EntityCandidate,
    QueryTaxonomy,
)

logger = logging.getLogger(__name__)

CLASSIFIER_MODEL = os.getenv("CLASSIFIER_MODEL", "gpt-4o")

CLASSIFIER_CONFIDENCE_THRESHOLD = 0.7

CLASSIFIER_PROMPT = """You are a query classifier for a bioregional knowledge commons (BKC).
Given a user question, classify it into exactly one taxonomy category
and extract any entity mentions.

## Taxonomy

- entity_definition: "What is X?" where X is a species, place, ecosystem, organization, person, technical system, funding mechanism, or general concept. Also includes: technical protocols (KOI protocol, federation protocol), funding mechanisms (TBFF, flow funding), and broad thematic descriptions of approaches or movements. If X is a technical system, protocol, or mechanism — even one used in governance contexts — it belongs here. Examples: "What is eelgrass?", "What is the Salish Sea?", "What is bioregionalism?", "What is Regenerate Cascadia?", "What is the KOI protocol?", "What is Threshold-Based Flow Funding?", "What is the overall approach to ecological stewardship in Cascadia?"
- relationship_path: "How does X relate to Y?" — asking about connections, relationships, multi-hop paths between entities, process flows involving multiple steps, or how a technical system enables something. Also includes questions about which entities are associated with another entity, and questions relating two domain concepts even if both are in the commitment or governance domain. Examples: "Which organizations work on restoration?", "What species are connected to Chinook salmon?", "What restoration practices does the Victoria Landscape Hub focus on?", "Which organizations are part of a network?", "How does a commitment become an attestation?", "How does the KOI federation protocol enable knowledge sharing?", "What is Commitment Pooling and how does it relate to flow funding?"
- governance_policy: Asking about governance RULES, decision processes, data sovereignty principles, indigenous data frameworks, or policy structures. Also includes: meta-protocol, CommonsChange profiles, onboarding playbooks, pattern languages, ontology frameworks, node participation profiles, FPIC, data sovereignty, visibility scoping, membrane governance. NOT entity_definition even when phrased as "What is X?" — if X is a governance rule, policy framework, or decision process, it belongs here. BUT NOT for technical systems or mechanisms, even if related to governance: "What is the KOI protocol?" → entity_definition (a technical system), "What is Threshold-Based Flow Funding?" → entity_definition (a funding mechanism), "What is the overall approach to stewardship?" → entity_definition (a broad thematic description). Examples: "What are OCAP principles?", "How does data sovereignty work in the BKC?", "What is the federation membrane governance?", "What is the BKC meta-protocol?", "What is the BKC pattern language?", "What is the bioregion onboarding playbook?"
- roadmap_status: Asking about project status, milestones, timelines, deployed features, or technical capabilities. Also includes: deployed features (VCV token, SwapPool), active node lists, pilot progress, retrieval technique summaries, implementation milestones. NOT out_of_domain — questions about BKC technical capabilities are roadmap questions. Examples: "What is the status of commitment pooling?", "What milestones have been completed?", "What retrieval techniques does the BKC use?", "What is the Victoria Commitment Voucher?"
- commitment_claim: Asking about pledges, claims, evidence, commitment pools, flow funding settlements, routing scores, or the claims engine. NOT entity_definition — commitment mechanisms are domain-specific infrastructure. NOT out_of_domain — questions about claims, commitments, and pools are core BKC. Examples: "What commitments has Victoria Landscape Hub made?", "How does the claims engine work?", "What is a commitment pool?", "What are commitment routing scores?"
- cross_node_provenance: "What does node Y know about X?" — asking about information from a specific bioregional node or cross-node comparison.
- out_of_domain: Questions with NO connection to ecology, bioregions, governance, stewardship, knowledge commons, or any BKC concept. Must be purely about external topics (stock prices, celebrity gossip, software installation, general trivia).

CRITICAL: If the question mentions ANY of these, it is NOT out_of_domain:
- Bioregional concepts (knowledge commons, bioregionalism, stewardship)
- BKC infrastructure (claims, commitments, retrieval, federation, routing)
- Ecological topics (species, ecosystems, restoration, watersheds)
- Governance terms (protocol, sovereignty, FPIC, OCAP, policy)
When in doubt, classify as the closest in-domain category, NOT out_of_domain.

IMPORTANT — "What is X?" disambiguation:
Many governance frameworks, commitment mechanisms, and roadmap features
use "What is X?" phrasing. Route based on WHAT X IS, not the question form:
- "What is the meta-protocol?" → governance_policy (X is a governance rule framework)
- "What is a commitment pool?" → commitment_claim (X is a commitment mechanism)
- "What is the Victoria Commitment Voucher?" → roadmap_status (X is a deployed feature)
- "What is eelgrass?" → entity_definition (X is a species/concept)
- "What is the KOI protocol?" → entity_definition (X is a technical system)
- "What is Threshold-Based Flow Funding?" → entity_definition (X is a funding mechanism)
- "What is the overall approach to ecological stewardship?" → entity_definition (broad thematic description)

## Depth

- shallow: ONLY for simple single-entity lookups where the entity name is explicit and well-known (e.g., "What is eelgrass?"). If the question asks about a complex concept, protocol, framework, or process, use standard or deep even if it looks like "What is X?".
- standard: Typical question requiring entity + document search. DEFAULT choice when unsure.
- deep: Complex question requiring multiple search strategies or synthesis across sources.

## Entities

Extract named entities mentioned in the question. Include:
- Species, ecosystems, locations, bioregions
- Organizations, people, projects
- Concepts, protocols, practices
- Specific items like "commitment pool", "Victoria Landscape Hub"

## Confidence

Confidence calibration:
- 0.9-1.0: Unambiguously matches ONE category, could not fit any other.
- 0.7-0.9: Strong match, but touches aspects of multiple categories. Expected range.
- 0.5-0.7: Genuinely ambiguous, could be 2+ categories. Triggers fallback.
- below 0.5: Very unsure.
Do NOT default to a fixed confidence. Score each question individually.

## Output format (JSON)

{
  "query_taxonomy": "<one of the 7 categories>",
  "depth_tier": "shallow | standard | deep",
  "entities": [
    {"name": "<entity name>", "type": "<entity type or null>"}
  ],
  "reasoning": "<1 sentence explaining classification>",
  "confidence": 0.0-1.0
}"""

# ---------------------------------------------------------------------------
# OOD-recovery guardrail (Guard 3)
# ---------------------------------------------------------------------------

_GOVERNANCE_SIGNALS = {
    "meta-protocol", "governance", "sovereignty", "OCAP", "FPIC",
    "onboarding playbook", "pattern language", "participation profile",
    "decision-making", "commons change", "commonschange", "ontology framework",
    "visibility scoping", "membrane governance",
}

_COMMITMENT_SIGNALS = {
    "commitment", "pledge", "claim", "claims engine", "pool", "settlement",
    "routing score", "flow funding", "voucher", "VCV", "attestation",
    "TBFF", "threshold",
}

_BKC_SIGNALS = {
    "BKC", "bioregion", "knowledge commons", "claims engine",
    "commitment", "federation", "retrieval", "koi",
    "discourse graph", "stewardship", "restoration",
}


def _apply_guardrails(query: str, output: ClassifierOutput) -> ClassifierOutput:
    """Post-classifier OOD-recovery guardrail.

    Only fires when the LLM says out_of_domain but the query clearly
    mentions BKC concepts. Guards 1+2 (entity_definition overrides)
    were removed because the tuned prompt handles those confusion
    patterns, and keyword-based overrides caused regressions.
    """
    if output.query_taxonomy != QueryTaxonomy.OUT_OF_DOMAIN:
        return output

    query_lower = query.lower()
    if not any(s.lower() in query_lower for s in _BKC_SIGNALS):
        return output

    # Reclassify based on which domain signals are present
    if any(s.lower() in query_lower for s in _GOVERNANCE_SIGNALS):
        return output.model_copy(update={
            "query_taxonomy": QueryTaxonomy.GOVERNANCE_POLICY,
            "confidence": 0.65,
        })
    if any(s.lower() in query_lower for s in _COMMITMENT_SIGNALS):
        return output.model_copy(update={
            "query_taxonomy": QueryTaxonomy.COMMITMENT_CLAIM,
            "confidence": 0.65,
        })
    # Generic BKC signal, no specific category → low-confidence fallback
    return output.model_copy(update={
        "query_taxonomy": QueryTaxonomy.ENTITY_DEFINITION,
        "confidence": 0.5,
    })


# ---------------------------------------------------------------------------
# Main classifier
# ---------------------------------------------------------------------------

async def classify_query(
    query: str,
    provider: ChatProvider,
    model: str | None = None,
) -> ClassifierOutput:
    """Classify a query into a QueryTaxonomy category via an LLM provider.

    Returns ClassifierOutput with confidence score.
    On parse error or unexpected output, returns OUT_OF_DOMAIN with confidence=0.0
    to trigger fallback to the baseline retrieval path.
    """
    try:
        raw = await provider.complete(
            [
                {"role": "system", "content": CLASSIFIER_PROMPT},
                {"role": "user", "content": query},
            ],
            model=model or CLASSIFIER_MODEL,
            temperature=0.0,
            max_tokens=200,
            json_mode=True,
        )
        data = json.loads(raw)

        # Parse taxonomy
        taxonomy_str = data.get("query_taxonomy", "out_of_domain")
        try:
            taxonomy = QueryTaxonomy(taxonomy_str)
        except ValueError:
            logger.warning(f"Unknown taxonomy '{taxonomy_str}', defaulting to out_of_domain")
            taxonomy = QueryTaxonomy.OUT_OF_DOMAIN

        # Parse depth tier
        depth_str = data.get("depth_tier", "standard")
        try:
            depth = DepthTier(depth_str)
        except ValueError:
            depth = DepthTier.STANDARD

        # Parse entities
        entities = []
        for e in data.get("entities", []):
            if isinstance(e, dict) and "name" in e:
                entities.append(EntityCandidate(
                    name=e["name"],
                    type=e.get("type"),
                ))

        confidence = float(data.get("confidence", 0.5))
        confidence = max(0.0, min(1.0, confidence))

        result = ClassifierOutput(
            query_taxonomy=taxonomy,
            depth_tier=depth,
            entities=entities,
            reasoning=data.get("reasoning", ""),
            confidence=confidence,
        )

        return _apply_guardrails(query, result)

    except Exception as e:
        logger.warning(f"Query classification failed, falling back to OUT_OF_DOMAIN: {e}")
        return ClassifierOutput(
            query_taxonomy=QueryTaxonomy.OUT_OF_DOMAIN,
            depth_tier=DepthTier.STANDARD,
            reasoning=f"parse_error: {e}",
            confidence=0.0,
        )
