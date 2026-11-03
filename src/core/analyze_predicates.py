#!/usr/bin/env python3
"""
Analyze predicate distribution in the knowledge graph
Identify clustering opportunities and semantic similarities
"""

import json
import requests
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple
import re

JENA_ENDPOINT = "http://localhost:3030/koi/sparql"

def execute_sparql(query: str) -> dict:
    """Execute a SPARQL query against Apache Jena"""
    response = requests.post(
        JENA_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    return response.json() if response.status_code == 200 else {"results": {"bindings": []}}

def get_all_predicates_with_context():
    """Get all predicates with their subject/object type context"""
    print("Fetching all predicates with context...")

    query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?predicate (COUNT(*) as ?count)
           (SAMPLE(?subjectType) as ?sampleSubjectType)
           (SAMPLE(?objectLiteral) as ?sampleObjectLiteral)
    WHERE {
        ?stmt a regx:Statement .
        ?stmt regx:subject ?subject .
        ?stmt regx:predicate ?predicate .
        ?stmt regx:object ?object .

        # Try to get subject entity type if it exists
        OPTIONAL {
            ?subjectEntity regx:subjectEntity ?subject .
            ?subjectEntity regx:entityType ?subjectType .
        }

        # Check if object is literal or URI
        BIND(isLiteral(?object) as ?objectLiteral)
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)
    """

    results = execute_sparql(query)
    predicates = []

    for binding in results["results"]["bindings"]:
        predicates.append({
            "predicate": binding["predicate"]["value"],
            "count": int(binding["count"]["value"]),
            "subjectType": binding.get("sampleSubjectType", {}).get("value"),
            "hasLiteralObject": binding.get("sampleObjectLiteral", {}).get("value") == "true"
        })

    return predicates

def categorize_predicates(predicates: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorize predicates by semantic patterns"""
    categories = defaultdict(list)

    for pred_info in predicates:
        pred = pred_info["predicate"]

        # Normalize predicate for analysis
        pred_lower = pred.lower()

        # Identity/Classification predicates
        if any(x in pred_lower for x in ["is", "are", "was", "were", "be", "being", "been"]):
            categories["identity"].append(pred_info)

        # Action predicates (past tense)
        elif any(pred_lower.endswith(x) for x in ["ed", "wrote", "built", "made"]):
            categories["action_past"].append(pred_info)

        # Action predicates (present)
        elif any(pred_lower.endswith(x) for x in ["s", "es"]) and not pred_lower.endswith("is"):
            categories["action_present"].append(pred_info)

        # Relationship predicates
        elif any(x in pred_lower for x in ["with", "to", "from", "of", "in", "on", "at", "by"]):
            categories["relationship"].append(pred_info)

        # Modal/Auxiliary predicates
        elif any(x in pred_lower for x in ["can", "could", "will", "would", "should", "may", "might"]):
            categories["modal"].append(pred_info)

        # Question predicates
        elif any(x in pred_lower for x in ["question", "ask", "wonder", "inquire"]):
            categories["question"].append(pred_info)

        # Possession predicates
        elif any(x in pred_lower for x in ["has", "have", "owns", "possess"]):
            categories["possession"].append(pred_info)

        # Support/Enable predicates
        elif any(x in pred_lower for x in ["support", "enable", "allow", "help", "facilitate"]):
            categories["support"].append(pred_info)

        # Creation predicates
        elif any(x in pred_lower for x in ["create", "develop", "build", "make", "produce"]):
            categories["creation"].append(pred_info)

        # Communication predicates
        elif any(x in pred_lower for x in ["say", "state", "discuss", "mention", "announce"]):
            categories["communication"].append(pred_info)

        else:
            categories["other"].append(pred_info)

    return dict(categories)

def find_similar_predicates(predicates: List[Dict]) -> Dict[str, List[str]]:
    """Find groups of similar predicates using simple heuristics"""
    groups = defaultdict(list)

    # Group by lemma/stem patterns
    for pred_info in predicates:
        pred = pred_info["predicate"]

        # Normalize and extract root
        normalized = pred.lower().strip()

        # Handle "is/are/was/were X"
        if normalized.startswith(("is ", "are ", "was ", "were ")):
            root = normalized.split(" ", 1)[1] if " " in normalized else normalized
            groups[f"is_{root}"].append(pred)

        # Handle "has/have X"
        elif normalized.startswith(("has ", "have ")):
            root = normalized.split(" ", 1)[1] if " " in normalized else normalized
            groups[f"has_{root}"].append(pred)

        # Handle verb endings
        elif normalized.endswith("s") and not normalized.endswith(("is", "as", "us")):
            root = normalized[:-1]
            groups[root].append(pred)
        elif normalized.endswith("es"):
            root = normalized[:-2]
            groups[root].append(pred)
        elif normalized.endswith("ed"):
            root = normalized[:-2] if not normalized.endswith("eed") else normalized[:-1]
            groups[root].append(pred)
        elif normalized.endswith("ing"):
            root = normalized[:-3]
            groups[root].append(pred)
        else:
            groups[normalized].append(pred)

    # Filter to only show groups with multiple predicates
    similar_groups = {k: v for k, v in groups.items() if len(v) > 1}

    return similar_groups

def suggest_consolidation_mapping(predicates: List[Dict]) -> Dict[str, str]:
    """Suggest a mapping from current predicates to consolidated forms"""
    mapping = {}

    for pred_info in predicates:
        pred = pred_info["predicate"]
        pred_lower = pred.lower()

        # Identity mappings
        if pred_lower in ["is", "are", "was", "were"]:
            mapping[pred] = "is"
        elif pred_lower in ["is a", "is an", "is a type of"]:
            mapping[pred] = "type_of"
        elif pred_lower in ["is part of", "is a part of", "belongs to"]:
            mapping[pred] = "part_of"
        elif pred_lower in ["is associated with", "is related to", "relates to"]:
            mapping[pred] = "related_to"
        elif pred_lower in ["is involved in", "participates in", "is engaged in"]:
            mapping[pred] = "participates_in"

        # Action mappings
        elif pred_lower in ["creates", "created", "develops", "developed", "builds", "built"]:
            mapping[pred] = "creates"
        elif pred_lower in ["provides", "provided", "offers", "offered", "supplies"]:
            mapping[pred] = "provides"
        elif pred_lower in ["supports", "supported", "enables", "enabled", "facilitates"]:
            mapping[pred] = "supports"
        elif pred_lower in ["uses", "used", "utilizes", "utilized", "employs"]:
            mapping[pred] = "uses"
        elif pred_lower in ["includes", "included", "contains", "contained", "comprises"]:
            mapping[pred] = "includes"
        elif pred_lower in ["requires", "required", "needs", "needed"]:
            mapping[pred] = "requires"

        # Communication mappings
        elif pred_lower in ["states", "stated", "says", "said", "mentions", "mentioned"]:
            mapping[pred] = "states"
        elif pred_lower in ["discusses", "discussed", "talks about", "explores"]:
            mapping[pred] = "discusses"
        elif pred_lower in ["proposes", "proposed", "suggests", "suggested"]:
            mapping[pred] = "proposes"
        elif pred_lower in ["announces", "announced", "declares", "declared"]:
            mapping[pred] = "announces"
        elif pred_lower in ["questions", "questioned", "asks", "asked", "inquires"]:
            mapping[pred] = "questions"

        # Possession mappings
        elif pred_lower in ["has", "have", "owns", "possesses"]:
            mapping[pred] = "has"

        # Publication mappings
        elif pred_lower in ["published", "publishes", "wrote", "writes", "authored"]:
            mapping[pred] = "published"

        # Focus mappings
        elif pred_lower in ["focuses on", "focuses upon", "concentrates on", "emphasizes"]:
            mapping[pred] = "focuses_on"

        # Keep as-is if no clear mapping
        else:
            # Try to simplify to present tense base form
            if pred_lower.endswith("ed") and len(pred) > 4:
                base = pred_lower[:-2] if not pred_lower.endswith("eed") else pred_lower[:-1]
                mapping[pred] = base
            elif pred_lower.endswith("s") and not pred_lower.endswith(("is", "as", "us")) and len(pred) > 3:
                base = pred_lower[:-1]
                mapping[pred] = base
            else:
                mapping[pred] = pred_lower.replace(" ", "_")

    return mapping

def analyze_consolidation_impact(predicates: List[Dict], mapping: Dict[str, str]) -> Dict:
    """Analyze the impact of consolidation"""

    # Count occurrences for each consolidated predicate
    consolidated_counts = defaultdict(int)
    original_to_consolidated = defaultdict(list)

    for pred_info in predicates:
        pred = pred_info["predicate"]
        count = pred_info["count"]
        consolidated = mapping.get(pred, pred)

        consolidated_counts[consolidated] += count
        original_to_consolidated[consolidated].append((pred, count))

    # Calculate statistics
    original_count = len(predicates)
    consolidated_count = len(consolidated_counts)
    reduction_rate = (1 - consolidated_count / original_count) * 100

    # Find top consolidated predicates
    top_consolidated = sorted(consolidated_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    return {
        "original_predicates": original_count,
        "consolidated_predicates": consolidated_count,
        "reduction_rate": reduction_rate,
        "top_consolidated": top_consolidated,
        "mapping_details": dict(original_to_consolidated)
    }

def main():
    print("\n" + "="*60)
    print("PREDICATE ANALYSIS AND CONSOLIDATION PLANNING")
    print("="*60)

    # Get all predicates with context
    predicates = get_all_predicates_with_context()
    print(f"\nFound {len(predicates)} unique predicates")

    # Show distribution
    print("\n=== Predicate Distribution ===")
    total_uses = sum(p["count"] for p in predicates)
    print(f"Total predicate uses: {total_uses:,}")

    # Show top and long tail
    top_20_uses = sum(p["count"] for p in predicates[:20])
    print(f"Top 20 predicates: {top_20_uses:,} uses ({top_20_uses/total_uses*100:.1f}%)")

    singleton_preds = sum(1 for p in predicates if p["count"] == 1)
    print(f"Predicates used only once: {singleton_preds} ({singleton_preds/len(predicates)*100:.1f}%)")

    # Categorize predicates
    print("\n=== Predicate Categories ===")
    categories = categorize_predicates(predicates)
    for cat, preds in categories.items():
        total_count = sum(p["count"] for p in preds)
        print(f"{cat}: {len(preds)} predicates, {total_count:,} uses")

    # Find similar groups
    print("\n=== Similar Predicate Groups (Sample) ===")
    similar_groups = find_similar_predicates(predicates)
    for root, group in list(similar_groups.items())[:10]:
        if len(group) > 2:
            print(f"{root}: {group[:5]}")

    # Generate consolidation mapping
    print("\n=== Generating Consolidation Mapping ===")
    mapping = suggest_consolidation_mapping(predicates)

    # Analyze impact
    impact = analyze_consolidation_impact(predicates, mapping)

    print(f"\nConsolidation Impact:")
    print(f"- Original predicates: {impact['original_predicates']}")
    print(f"- Consolidated predicates: {impact['consolidated_predicates']}")
    print(f"- Reduction rate: {impact['reduction_rate']:.1f}%")

    print(f"\nTop 20 Consolidated Predicates:")
    for pred, count in impact['top_consolidated']:
        sources = impact['mapping_details'][pred]
        source_count = len(sources)
        print(f"  {pred}: {count:,} uses (from {source_count} original predicates)")

    # Save results
    results = {
        "predicates": predicates,
        "categories": {k: [p["predicate"] for p in v] for k, v in categories.items()},
        "similar_groups": similar_groups,
        "mapping": mapping,
        "impact": impact
    }

    with open("predicate_analysis.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n✓ Full analysis saved to: predicate_analysis.json")

    # Save mapping for use
    with open("predicate_mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print(f"✓ Mapping saved to: predicate_mapping.json")

    return results

if __name__ == "__main__":
    main()