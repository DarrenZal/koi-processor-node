#!/usr/bin/env python3
"""
Advanced Predicate Consolidation using OpenAI Embeddings
Uses real semantic similarity to cluster and consolidate predicates

With --apply it reads every predicate from Fuseki, embeds the standard predicates and up
to --limit of the graph's predicates (default 1000) with paid OpenAI calls
(text-embedding-3-small, 100 per request), and writes openai_predicate_consolidation.json
and openai_predicate_mapping.json in the working directory. With no arguments or --dry-run
it only prints what it would do.
"""

import argparse
import json
import math
import requests
import numpy as np
import os
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


# Standard ontology predicates (from Schema.org, FOAF, Dublin Core, etc.)
STANDARD_PREDICATES = {
    # Schema.org relationships
    "author": "schema:author",
    "creator": "schema:creator",
    "publisher": "schema:publisher",
    "about": "schema:about",
    "mentions": "schema:mentions",
    "citation": "schema:citation",
    "hasPart": "schema:hasPart",
    "isPartOf": "schema:isPartOf",
    "knows": "schema:knows",
    "affiliation": "schema:affiliation",
    "memberOf": "schema:memberOf",
    "employee": "schema:employee",
    "founder": "schema:founder",
    "owns": "schema:owns",
    "sponsor": "schema:sponsor",
    "funder": "schema:funder",
    "provider": "schema:provider",
    "organizer": "schema:organizer",
    "contributor": "schema:contributor",
    "participant": "schema:participant",

    # Dublin Core
    "subject": "dc:subject",
    "type": "dc:type",
    "relation": "dc:relation",
    "source": "dc:source",
    "coverage": "dc:coverage",

    # FOAF (Friend of a Friend)
    "made": "foaf:made",
    "maker": "foaf:maker",
    "primaryTopic": "foaf:primaryTopic",
    "topic": "foaf:topic",
    "depicts": "foaf:depicts",
    "interest": "foaf:interest",

    # PROV-O (Provenance Ontology)
    "wasGeneratedBy": "prov:wasGeneratedBy",
    "wasDerivedFrom": "prov:wasDerivedFrom",
    "wasAttributedTo": "prov:wasAttributedTo",
    "used": "prov:used",
    "wasAssociatedWith": "prov:wasAssociatedWith",
    "actedOnBehalfOf": "prov:actedOnBehalfOf",
    "wasInformedBy": "prov:wasInformedBy",

    # SKOS (Simple Knowledge Organization System)
    "broader": "skos:broader",
    "narrower": "skos:narrower",
    "related": "skos:related",
    "definition": "skos:definition",
    "example": "skos:example",
    "note": "skos:note",

    # Custom consolidated predicates for common patterns
    "creates": "custom:creates",
    "provides": "custom:provides",
    "supports": "custom:supports",
    "requires": "custom:requires",
    "participatesIn": "custom:participatesIn",
    "manages": "custom:manages",
    "develops": "custom:develops",
    "implements": "custom:implements",
    "enables": "custom:enables",
    "facilitates": "custom:facilitates",
    "promotes": "custom:promotes",
    "advocates": "custom:advocates",
    "represents": "custom:represents",
    "contains": "custom:contains",
    "includes": "custom:includes",
    "comprises": "custom:comprises",
    "consistsOf": "custom:consistsOf",
    "relatesTo": "custom:relatesTo",
    "associatedWith": "custom:associatedWith",
    "connectedTo": "custom:connectedTo",
    "linkedTo": "custom:linkedTo",
    "uses": "custom:uses",
    "utilizes": "custom:utilizes",
    "employs": "custom:employs",
    "applies": "custom:applies",
    "focusesOn": "custom:focusesOn",
    "emphasizes": "custom:emphasizes",
    "addresses": "custom:addresses",
    "discusses": "custom:discusses",
    "describes": "custom:describes",
    "explains": "custom:explains",
    "communicates": "custom:communicates",
    "states": "custom:states",
    "announces": "custom:announces",
    "proposes": "custom:proposes",
    "suggests": "custom:suggests",
    "recommends": "custom:recommends",
    "questions": "custom:questions",
    "challenges": "custom:challenges",
    "critiques": "custom:critiques"
}

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

def get_all_predicates_with_context():
    """Get all predicates with their usage context"""
    print("Fetching all predicates with context...")

    # First get all predicates with counts
    query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>
    SELECT ?predicate (COUNT(*) as ?count)
    WHERE {
        ?stmt a regx:Statement .
        ?stmt regx:predicate ?predicate .
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)
    """

    results = execute_sparql(query)
    predicates = []

    for binding in results["results"]["bindings"]:
        predicates.append({
            "predicate": binding["predicate"]["value"],
            "count": int(binding["count"]["value"])
        })

    print(f"Found {len(predicates)} unique predicates")

    # For top predicates, get sample usage
    for pred_info in predicates[:500]:  # Get context for top 500
        pred = pred_info["predicate"]

        # Get sample subjects and objects
        context_query = f"""
        PREFIX regx: <https://regen.network/ontology/experimental#>
        SELECT ?subject ?object WHERE {{
            ?stmt regx:predicate "{pred}" .
            ?stmt regx:subject ?subject .
            ?stmt regx:object ?object .
        }}
        LIMIT 3
        """

        context_results = execute_sparql(context_query)

        subjects = []
        objects = []
        for binding in context_results["results"]["bindings"]:
            if "subject" in binding:
                subjects.append(binding["subject"]["value"][:50])
            if "object" in binding:
                objects.append(binding["object"]["value"][:50])

        pred_info["sample_subjects"] = subjects
        pred_info["sample_objects"] = objects

    return predicates

def create_predicate_text(pred_info: Dict) -> str:
    """Create a rich text representation of predicate for embedding"""
    predicate = pred_info["predicate"]
    subjects = pred_info.get("sample_subjects", [])
    objects = pred_info.get("sample_objects", [])

    # Build context-rich description
    parts = [f"Predicate: {predicate}"]

    if subjects:
        parts.append(f"Typical subjects: {', '.join(subjects[:2])}")

    if objects:
        parts.append(f"Typical objects: {', '.join(objects[:2])}")

    # Add linguistic variations
    pred_lower = predicate.lower()

    # Add tense information
    if pred_lower.endswith("ed"):
        parts.append("(past tense action)")
    elif pred_lower.endswith("ing"):
        parts.append("(ongoing action)")
    elif pred_lower.endswith("s") and not pred_lower.endswith("is"):
        parts.append("(present tense)")

    # Add semantic hints
    if "is" in pred_lower or "are" in pred_lower:
        parts.append("(state or identity)")
    elif any(word in pred_lower for word in ["create", "make", "build", "develop"]):
        parts.append("(creation or production)")
    elif any(word in pred_lower for word in ["support", "help", "enable", "facilitate"]):
        parts.append("(support or enablement)")
    elif any(word in pred_lower for word in ["use", "utilize", "employ", "apply"]):
        parts.append("(usage or application)")

    return " ".join(parts)

def get_openai_embeddings(texts: List[str], model: str = "text-embedding-3-small") -> np.ndarray:
    """Get OpenAI embeddings for a list of texts"""
    print(f"Getting embeddings for {len(texts)} texts...")
    embeddings = []

    # Process in batches to avoid rate limits
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
                embeddings.append([0.0] * 1536)  # Default embedding size

        # Small delay to avoid rate limits
        if i + batch_size < len(texts):
            time.sleep(0.1)

    return np.array(embeddings)

def get_standard_predicate_embeddings() -> Tuple[List[str], np.ndarray]:
    """Get embeddings for standard ontology predicates"""
    print("Getting embeddings for standard predicates...")

    standard_texts = []
    standard_names = []

    for name, uri in STANDARD_PREDICATES.items():
        # Create rich description of standard predicate
        text = f"Standard predicate: {name}"

        # Add semantic description based on category
        if uri.startswith("schema:"):
            text += f" (Schema.org relationship for {name})"
        elif uri.startswith("dc:"):
            text += f" (Dublin Core metadata for {name})"
        elif uri.startswith("foaf:"):
            text += f" (Friend of a Friend social relationship for {name})"
        elif uri.startswith("prov:"):
            text += f" (Provenance tracking for {name})"
        elif uri.startswith("custom:"):
            text += f" (Common action or relationship: {name})"

        standard_texts.append(text)
        standard_names.append(uri)

    # Get embeddings
    standard_embeddings = get_openai_embeddings(standard_texts)

    return standard_names, standard_embeddings

def cluster_predicates_with_embeddings(predicates: List[Dict], threshold: float = 0.35) -> Dict[int, List[Dict]]:
    """Cluster predicates using real embeddings"""
    print(f"Clustering {len(predicates)} predicates with threshold {threshold}...")

    # Create text representations
    texts = [create_predicate_text(p) for p in predicates]

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

    # Group predicates by cluster
    clusters = defaultdict(list)
    for i, label in enumerate(cluster_labels):
        clusters[label].append({
            **predicates[i],
            "embedding": embeddings[i]
        })

    print(f"Created {len(clusters)} clusters")

    return dict(clusters)

def map_cluster_to_standard(cluster: List[Dict], standard_names: List[str], standard_embeddings: np.ndarray) -> str:
    """Map a cluster to the best matching standard predicate"""

    # Get average embedding for cluster
    cluster_embeddings = np.array([p["embedding"] for p in cluster])
    cluster_centroid = np.mean(cluster_embeddings, axis=0)

    # Find most similar standard predicate
    similarities = cosine_similarity([cluster_centroid], standard_embeddings)[0]
    best_idx = np.argmax(similarities)
    best_similarity = similarities[best_idx]

    # If similarity is high enough, use standard predicate
    if best_similarity > 0.7:
        return standard_names[best_idx]

    # Otherwise, select best representative from cluster
    cluster_preds = [p for p in cluster if "embedding" not in p or p["embedding"] is not None]

    # Sort by frequency and simplicity
    sorted_cluster = sorted(cluster_preds, key=lambda x: (
        -x["count"],  # Higher frequency first
        len(x["predicate"].split()),  # Fewer words
        len(x["predicate"])  # Shorter length
    ))

    if sorted_cluster:
        best_local = sorted_cluster[0]["predicate"].lower().replace(" ", "_")
        return f"regen:{best_local}"

    return f"regen:predicate_{cluster[0]['predicate'][:20]}"

def create_final_mapping(predicates: List[Dict]) -> Tuple[Dict[str, str], Dict]:
    """Create final consolidated mapping using OpenAI embeddings"""

    # Get standard predicate embeddings
    standard_names, standard_embeddings = get_standard_predicate_embeddings()

    # Cluster predicates
    clusters = cluster_predicates_with_embeddings(predicates)

    # Create mapping
    mapping = {}
    cluster_info = {}

    for cluster_id, cluster in clusters.items():
        # Map to standard or create custom
        consolidated = map_cluster_to_standard(cluster, standard_names, standard_embeddings)

        # Remove embeddings from cluster info for storage
        cluster_members = [p["predicate"] for p in cluster]
        total_count = sum(p["count"] for p in cluster)

        cluster_info[consolidated] = {
            "id": cluster_id,
            "consolidated": consolidated,
            "members": cluster_members,
            "member_count": len(cluster_members),
            "total_usage": total_count,
            "top_member": cluster_members[0] if cluster_members else ""
        }

        # Create individual mappings
        for pred in cluster_members:
            mapping[pred] = consolidated

    return mapping, cluster_info

def analyze_results(predicates: List[Dict], mapping: Dict[str, str], cluster_info: Dict) -> Dict:
    """Analyze consolidation results"""

    original_count = len(predicates)
    consolidated_count = len(set(mapping.values()))

    # Count by namespace
    namespace_counts = Counter()
    for consolidated in set(mapping.values()):
        if ":" in consolidated:
            namespace = consolidated.split(":")[0]
        else:
            namespace = "unknown"
        namespace_counts[namespace] += 1

    # Usage distribution
    usage_by_consolidated = Counter()
    for pred_info in predicates:
        if pred_info["predicate"] in mapping:
            consolidated = mapping[pred_info["predicate"]]
            usage_by_consolidated[consolidated] += pred_info["count"]

    return {
        "original_predicates": original_count,
        "consolidated_predicates": consolidated_count,
        "reduction_rate": (1 - consolidated_count / original_count) * 100 if original_count > 0 else 0,
        "namespace_distribution": dict(namespace_counts),
        "top_consolidated": usage_by_consolidated.most_common(50),
        "average_cluster_size": original_count / consolidated_count if consolidated_count > 0 else 0
    }

def main(argv=None):
    global JENA_ENDPOINT
    args = build_parser("Predicate consolidation with OpenAI embeddings.", 1000).parse_args(argv)
    JENA_ENDPOINT = args.endpoint

    print("\n" + "="*80)
    print("PREDICATE CONSOLIDATION USING OPENAI EMBEDDINGS")
    print("="*80)
    n_standard = len(STANDARD_PREDICATES)
    print(f"Fuseki: {JENA_ENDPOINT}")
    print(f"Steps: read predicates, embed {n_standard} standard predicates and up to {args.limit} "
          f"graph predicates with text-embedding-3-small (at most "
          f"{math.ceil(n_standard / 100) + math.ceil(args.limit / 100)} paid requests), cluster, "
          "write openai_predicate_consolidation.json and openai_predicate_mapping.json")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was read, sent or written. Re-run with --apply to do this.")
        return None
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY environment variable not set")
        sys.exit(1)

    # Get all predicates
    predicates = get_all_predicates_with_context()

    # Filter to focus on more common predicates
    min_usage = 2  # Only consolidate predicates used at least twice
    predicates = [p for p in predicates if p["count"] >= min_usage]
    print(f"Focusing on {len(predicates)} predicates with usage >= {min_usage}")

    # Create consolidated mapping
    print("\n=== Creating Semantic Consolidation ===")
    mapping, cluster_info = create_final_mapping(predicates[:args.limit])  # Process top --limit (1000)

    # Analyze results
    analysis = analyze_results(predicates[:args.limit], mapping, cluster_info)

    print(f"\n=== Consolidation Results ===")
    print(f"Original predicates: {analysis['original_predicates']}")
    print(f"Consolidated predicates: {analysis['consolidated_predicates']}")
    print(f"Reduction rate: {analysis['reduction_rate']:.1f}%")
    print(f"Average cluster size: {analysis['average_cluster_size']:.1f}")

    print(f"\n=== Namespace Distribution ===")
    for namespace, count in sorted(analysis['namespace_distribution'].items()):
        print(f"  {namespace:10} {count:4} predicates")

    print(f"\n=== Top 30 Consolidated Predicates ===")
    for i, (pred, usage) in enumerate(analysis['top_consolidated'][:30], 1):
        info = cluster_info.get(pred, {})
        members = info.get('member_count', 1)
        print(f"{i:2}. {pred:40} {usage:6,} uses ({members} original predicates)")

    # Show sample clusters
    print(f"\n=== Sample Cluster Details ===")
    for consolidated, info in list(cluster_info.items())[:10]:
        if info['member_count'] > 1:
            print(f"\n{consolidated}:")
            print(f"  Members: {', '.join(info['members'][:5])}")
            if len(info['members']) > 5:
                print(f"  ... and {len(info['members']) - 5} more")

    # Save results
    results = {
        "mapping": mapping,
        "cluster_info": cluster_info,
        "analysis": analysis
    }

    with open("openai_predicate_consolidation.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Full results saved to: openai_predicate_consolidation.json")

    with open("openai_predicate_mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print(f"✓ Mapping saved to: openai_predicate_mapping.json")

    return results

if __name__ == "__main__":
    main()