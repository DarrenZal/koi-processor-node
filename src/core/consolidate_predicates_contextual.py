#!/usr/bin/env python3
"""
Contextual Predicate Consolidation using Triple Patterns
Embeds subject-type + predicate + object-type for better semantic clustering

With --apply it reads every predicate's triple patterns from Fuseki, embeds up to --limit
of them (default 500) with paid OpenAI calls (text-embedding-3-small, 100 per request),
and writes contextual_predicate_consolidation.json and contextual_predicate_mapping.json
in the working directory. With no arguments or --dry-run it only prints what it would do.
"""

import argparse
import json
import math
import requests
import numpy as np
import os
import re
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Optional
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
import openai
from tqdm import tqdm
import sys
import time

JENA_ENDPOINT = "http://localhost:3030/koi/sparql"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# The OpenAI client is created on first use (main() with --apply), not at import.
client = None


def _get_client():
    """The OpenAI client, created once on first use."""
    global client
    if client is None:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
    return client


def build_parser(description: str, default_limit: int) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description + " Default is a dry run that "
                                     "contacts nothing.")
    parser.add_argument("--endpoint", default=JENA_ENDPOINT,
                        help="SPARQL query endpoint to read predicates from (default: %(default)s)")
    parser.add_argument("--limit", type=int, default=default_limit,
                        help="Most predicates to embed and cluster (default: %(default)s; "
                             "100 texts per paid embedding request)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Read Fuseki, make the paid embedding calls and write the JSON files")
    return parser


def execute_sparql(query: str) -> dict:
    """Execute a SPARQL query against Apache Jena"""
    response = requests.post(
        JENA_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    return response.json() if response.status_code == 200 else {"results": {"bindings": []}}

def get_entity_types() -> Dict[str, str]:
    """Get all entities and their types from the graph"""
    print("Fetching entity types...")

    query = """
    PREFIX schema: <http://schema.org/>
    PREFIX regx: <https://regen.network/ontology/experimental#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?entity ?type ?label WHERE {
        {
            ?entity a schema:Person .
            BIND("Person" as ?type)
        } UNION {
            ?entity a schema:Organization .
            BIND("Organization" as ?type)
        } UNION {
            ?entity a schema:Project .
            BIND("Project" as ?type)
        }
        OPTIONAL { ?entity rdfs:label ?label }
    }
    """

    results = execute_sparql(query)
    entity_types = {}

    for binding in results["results"]["bindings"]:
        entity = binding.get("entity", {}).get("value", "")
        entity_type = binding.get("type", {}).get("value", "")
        label = binding.get("label", {}).get("value", "")

        # Store by both URI and label
        if entity:
            entity_types[entity] = entity_type
        if label:
            entity_types[label] = entity_type
            entity_types[label.lower()] = entity_type  # Also store lowercase version

    print(f"Loaded {len(entity_types)} entity type mappings")
    return entity_types

def infer_literal_type(value: str) -> str:
    """Infer the type of a literal value"""
    value_lower = value.lower()

    # URL
    if value_lower.startswith(("http://", "https://", "www.")):
        return "URL"

    # Date patterns
    if re.match(r'\d{4}-\d{2}-\d{2}', value):
        return "Date"

    # Number
    if re.match(r'^-?\d+\.?\d*$', value):
        return "Number"

    # Boolean
    if value_lower in ["true", "false", "yes", "no"]:
        return "Boolean"

    # Email
    if "@" in value and "." in value:
        return "Email"

    # Short text (likely a name or identifier)
    if len(value) < 50 and not " " in value:
        return "Identifier"

    # Question
    if value.strip().endswith("?"):
        return "Question"

    # Default
    return "Text"

def get_triple_patterns() -> List[Dict]:
    """Get all statement triples with typed patterns"""
    print("Fetching triple patterns...")

    # First get entity types
    entity_types = get_entity_types()

    # Get all predicates with their usage patterns
    query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>

    SELECT ?predicate ?subject ?object (COUNT(*) as ?count)
    WHERE {
        ?stmt a regx:Statement .
        ?stmt regx:predicate ?predicate .
        ?stmt regx:subject ?subject .
        ?stmt regx:object ?object .
    }
    GROUP BY ?predicate ?subject ?object
    ORDER BY ?predicate DESC(?count)
    """

    results = execute_sparql(query)

    # Aggregate patterns by predicate
    predicate_patterns = defaultdict(lambda: {
        "patterns": [],
        "total_count": 0,
        "subject_types": Counter(),
        "object_types": Counter()
    })

    for binding in results["results"]["bindings"]:
        predicate = binding["predicate"]["value"]
        subject = binding["subject"]["value"]
        obj = binding["object"]["value"]
        count = int(binding["count"]["value"])

        # Determine types
        subject_type = entity_types.get(subject, entity_types.get(subject.lower(), "Entity"))

        # For objects, check if it's an entity or literal
        if obj in entity_types:
            object_type = entity_types[obj]
        else:
            object_type = infer_literal_type(obj)

        # Store pattern
        pattern = {
            "subject": subject[:100],  # Sample subject
            "subject_type": subject_type,
            "object": obj[:100],  # Sample object
            "object_type": object_type,
            "count": count
        }

        predicate_patterns[predicate]["patterns"].append(pattern)
        predicate_patterns[predicate]["total_count"] += count
        predicate_patterns[predicate]["subject_types"][subject_type] += count
        predicate_patterns[predicate]["object_types"][object_type] += count

    # Convert to list with dominant patterns
    result = []
    for predicate, data in predicate_patterns.items():
        # Get most common subject and object types
        if data["subject_types"]:
            dominant_subject = data["subject_types"].most_common(1)[0][0]
        else:
            dominant_subject = "Entity"

        if data["object_types"]:
            dominant_object = data["object_types"].most_common(1)[0][0]
        else:
            dominant_object = "Text"

        result.append({
            "predicate": predicate,
            "total_count": data["total_count"],
            "dominant_pattern": f"{dominant_subject} {predicate} {dominant_object}",
            "subject_types": dict(data["subject_types"]),
            "object_types": dict(data["object_types"]),
            "sample_patterns": data["patterns"][:3]  # Keep a few samples
        })

    print(f"Found {len(result)} unique predicates with patterns")
    return result

def create_pattern_embedding_text(pattern_data: Dict) -> str:
    """Create rich text representation of a triple pattern for embedding"""
    predicate = pattern_data["predicate"]
    dominant = pattern_data["dominant_pattern"]

    # Build comprehensive description
    parts = [dominant]

    # Add predicate variations
    pred_lower = predicate.lower()

    # Add semantic context based on predicate
    if any(word in pred_lower for word in ["create", "develop", "build", "make", "produce"]):
        parts.append("(creation/production relationship)")
    elif any(word in pred_lower for word in ["is", "are", "was", "were"]):
        parts.append("(identity/state relationship)")
    elif any(word in pred_lower for word in ["has", "have", "owns", "possess"]):
        parts.append("(possession/ownership relationship)")
    elif any(word in pred_lower for word in ["support", "help", "enable", "facilitate"]):
        parts.append("(support/enablement relationship)")
    elif any(word in pred_lower for word in ["use", "utilize", "employ", "apply"]):
        parts.append("(usage/application relationship)")
    elif any(word in pred_lower for word in ["part", "include", "contain", "comprise"]):
        parts.append("(composition/containment relationship)")

    # Add type distribution info
    subject_types = pattern_data.get("subject_types", {})
    object_types = pattern_data.get("object_types", {})

    if len(subject_types) > 1:
        top_subjects = ", ".join([f"{k}({v})" for k, v in list(subject_types.items())[:3]])
        parts.append(f"subjects: {top_subjects}")

    if len(object_types) > 1:
        top_objects = ", ".join([f"{k}({v})" for k, v in list(object_types.items())[:3]])
        parts.append(f"objects: {top_objects}")

    # Add sample usage if available
    samples = pattern_data.get("sample_patterns", [])
    if samples:
        sample = samples[0]
        parts.append(f"example: {sample['subject'][:30]} {predicate} {sample['object'][:30]}")

    return " ".join(parts)

def get_openai_embeddings(texts: List[str], model: str = "text-embedding-3-small") -> np.ndarray:
    """Get OpenAI embeddings for a list of texts"""
    print(f"Getting embeddings for {len(texts)} patterns...")
    embeddings = []

    # Process in batches
    batch_size = 100
    for i in tqdm(range(0, len(texts), batch_size)):
        batch = texts[i:i+batch_size]

        try:
            response = _get_client().embeddings.create(
                model=model,
                input=batch
            )

            for item in response.data:
                embeddings.append(item.embedding)

        except Exception as e:
            print(f"Error getting embeddings: {e}")
            # Add zero embeddings for failed items
            for _ in batch:
                embeddings.append([0.0] * 1536)

        # Small delay to avoid rate limits
        if i + batch_size < len(texts):
            time.sleep(0.1)

    return np.array(embeddings)

def cluster_patterns(patterns: List[Dict], threshold: float = 0.25) -> Dict[int, List[Dict]]:
    """Cluster triple patterns using embeddings"""
    print(f"Clustering {len(patterns)} patterns with threshold {threshold}...")

    # Create embedding texts
    texts = [create_pattern_embedding_text(p) for p in patterns]

    # Get embeddings
    embeddings = get_openai_embeddings(texts)

    # Perform hierarchical clustering
    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=threshold,
        metric='cosine',
        linkage='average'
    )

    cluster_labels = clustering.fit_predict(embeddings)

    # Group patterns by cluster
    clusters = defaultdict(list)
    for i, label in enumerate(cluster_labels):
        clusters[label].append({
            **patterns[i],
            "embedding": embeddings[i]
        })

    print(f"Created {len(clusters)} clusters")

    return dict(clusters)

def select_cluster_representative(cluster: List[Dict]) -> Tuple[str, str]:
    """Select the best consolidated predicate for a cluster"""

    # Group by dominant pattern
    pattern_groups = defaultdict(list)
    for item in cluster:
        pattern = item["dominant_pattern"]
        pattern_groups[pattern].append(item)

    # Find most common pattern
    most_common_pattern = max(pattern_groups.keys(),
                             key=lambda p: sum(item["total_count"] for item in pattern_groups[p]))

    # Extract subject type, predicate, object type
    parts = most_common_pattern.split(" ", 2)
    if len(parts) >= 3:
        subj_type = parts[0].lower()
        obj_type = parts[-1].lower()

        # Find best predicate from cluster
        sorted_cluster = sorted(cluster, key=lambda x: (
            -x["total_count"],  # Most frequent
            len(x["predicate"].split()),  # Fewer words
            len(x["predicate"])  # Shorter
        ))

        base_predicate = sorted_cluster[0]["predicate"].lower().replace(" ", "_")

        # Create contextual predicate name
        if subj_type != "entity" and obj_type != "text":
            consolidated = f"{subj_type}:{base_predicate}:{obj_type}"
        elif subj_type != "entity":
            consolidated = f"{subj_type}:{base_predicate}"
        else:
            consolidated = base_predicate

        return consolidated, most_common_pattern

    # Fallback
    return cluster[0]["predicate"].lower().replace(" ", "_"), most_common_pattern

def create_contextual_mapping(patterns: List[Dict]) -> Tuple[Dict[str, str], Dict]:
    """Create mapping from original predicates to contextual consolidated predicates"""

    # Cluster patterns
    clusters = cluster_patterns(patterns)

    # Create mapping
    mapping = {}
    cluster_info = {}

    for cluster_id, cluster in clusters.items():
        # Select representative
        consolidated, pattern = select_cluster_representative(cluster)

        # Store cluster info
        members = [p["predicate"] for p in cluster]
        total_usage = sum(p["total_count"] for p in cluster)

        cluster_info[consolidated] = {
            "pattern": pattern,
            "members": members,
            "member_count": len(members),
            "total_usage": total_usage,
            "subject_types": Counter(),
            "object_types": Counter()
        }

        # Aggregate type info
        for p in cluster:
            for st, count in p.get("subject_types", {}).items():
                cluster_info[consolidated]["subject_types"][st] += count
            for ot, count in p.get("object_types", {}).items():
                cluster_info[consolidated]["object_types"][ot] += count

        # Create individual mappings
        for predicate in members:
            mapping[predicate] = consolidated

    return mapping, cluster_info

def analyze_results(patterns: List[Dict], mapping: Dict[str, str], cluster_info: Dict) -> Dict:
    """Analyze the consolidation results"""

    original_count = len(patterns)
    consolidated_count = len(set(mapping.values()))

    # Count by pattern type
    pattern_types = Counter()
    for consolidated, info in cluster_info.items():
        pattern = info["pattern"]
        pattern_types[pattern] += 1

    # Usage distribution
    usage_by_consolidated = Counter()
    for pattern in patterns:
        if pattern["predicate"] in mapping:
            consolidated = mapping[pattern["predicate"]]
            usage_by_consolidated[consolidated] += pattern["total_count"]

    return {
        "original_predicates": original_count,
        "consolidated_predicates": consolidated_count,
        "reduction_rate": (1 - consolidated_count / original_count) * 100 if original_count > 0 else 0,
        "unique_patterns": len(pattern_types),
        "top_patterns": pattern_types.most_common(20),
        "top_consolidated": usage_by_consolidated.most_common(50),
        "average_cluster_size": original_count / consolidated_count if consolidated_count > 0 else 0
    }

def main(argv=None):
    global JENA_ENDPOINT
    args = build_parser("Contextual predicate consolidation with OpenAI embeddings.", 500).parse_args(argv)
    JENA_ENDPOINT = args.endpoint

    print("\n" + "="*80)
    print("CONTEXTUAL PREDICATE CONSOLIDATION WITH TRIPLE PATTERNS")
    print("="*80)
    print(f"Fuseki: {JENA_ENDPOINT}")
    print(f"Steps: read triple patterns, embed up to {args.limit} with text-embedding-3-small "
          f"(at most {math.ceil(args.limit / 100)} paid requests), cluster, write "
          "contextual_predicate_consolidation.json and contextual_predicate_mapping.json")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was read, sent or written. Re-run with --apply to do this.")
        return None
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        sys.exit(1)

    # Get triple patterns
    patterns = get_triple_patterns()

    # Filter to more common patterns
    min_usage = 2
    patterns = [p for p in patterns if p["total_count"] >= min_usage]
    print(f"Processing {len(patterns)} predicates with usage >= {min_usage}")

    # Create contextual mapping
    print("\n=== Creating Contextual Consolidation ===")
    mapping, cluster_info = create_contextual_mapping(patterns[:args.limit])  # Process top --limit (500)

    # Analyze results
    analysis = analyze_results(patterns[:args.limit], mapping, cluster_info)

    print(f"\n=== Consolidation Results ===")
    print(f"Original predicates: {analysis['original_predicates']}")
    print(f"Consolidated predicates: {analysis['consolidated_predicates']}")
    print(f"Reduction rate: {analysis['reduction_rate']:.1f}%")
    print(f"Unique triple patterns: {analysis['unique_patterns']}")
    print(f"Average cluster size: {analysis['average_cluster_size']:.1f}")

    print(f"\n=== Top 20 Triple Patterns ===")
    for pattern, count in analysis['top_patterns']:
        print(f"  {pattern:50} {count:3} consolidated predicates")

    print(f"\n=== Top 30 Consolidated Predicates ===")
    for i, (pred, usage) in enumerate(analysis['top_consolidated'][:30], 1):
        info = cluster_info.get(pred, {})
        members = info.get('member_count', 1)
        pattern = info.get('pattern', '')
        print(f"{i:2}. {pred:40} {usage:6,} uses ({members} predicates)")
        if pattern:
            print(f"    Pattern: {pattern}")

    # Show interesting clusters
    print(f"\n=== Sample Contextual Clusters ===")
    interesting = sorted(cluster_info.items(),
                        key=lambda x: (x[1]['member_count'], x[1]['total_usage']),
                        reverse=True)[:10]

    for consolidated, info in interesting:
        if info['member_count'] > 1:
            print(f"\n{consolidated}:")
            print(f"  Pattern: {info['pattern']}")
            print(f"  Members: {', '.join(info['members'][:5])}")
            if len(info['members']) > 5:
                print(f"  ... and {len(info['members']) - 5} more")

            # Show type distribution
            if info['subject_types']:
                top_subjects = ", ".join([f"{k}({v})" for k, v in info['subject_types'].most_common(3)])
                print(f"  Subject types: {top_subjects}")
            if info['object_types']:
                top_objects = ", ".join([f"{k}({v})" for k, v in info['object_types'].most_common(3)])
                print(f"  Object types: {top_objects}")

    # Save results
    results = {
        "mapping": mapping,
        "cluster_info": {k: {**v, "subject_types": dict(v["subject_types"]),
                             "object_types": dict(v["object_types"])}
                        for k, v in cluster_info.items()},
        "analysis": analysis
    }

    with open("contextual_predicate_consolidation.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Results saved to: contextual_predicate_consolidation.json")

    with open("contextual_predicate_mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print(f"✓ Mapping saved to: contextual_predicate_mapping.json")

    return results

if __name__ == "__main__":
    main()