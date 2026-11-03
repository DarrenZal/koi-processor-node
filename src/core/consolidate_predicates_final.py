#!/usr/bin/env python3
"""
Final Predicate Consolidation with Saved Embeddings
1. Computes embeddings once and saves them
2. Allows tunable clustering thresholds
3. Produces single consolidated predicates (not type-specific)

With --apply it reads predicate patterns from Fuseki (or predicate_patterns.json), embeds
the top 1000 (all with --full) with paid OpenAI calls (text-embedding-3-small, 100 per
request) unless predicate_embeddings.pkl already exists, and writes those two caches and
final_consolidation_[all_]t<threshold>.json in the working directory. With no arguments or
--dry-run it only prints what it would do.
"""

import json
import requests
import numpy as np
import os
import math
import pickle
import sys
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Optional
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
try:
    import openai  # Optional: only needed when computing embeddings
except ImportError:
    openai = None
from tqdm import tqdm
import time
from pathlib import Path
import argparse

JENA_ENDPOINT = "http://localhost:3030/koi/sparql"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
EMBEDDINGS_FILE = "predicate_embeddings.pkl"
PATTERNS_FILE = "predicate_patterns.json"

# The OpenAI client is created on first use (main() with --apply), not at import.
client = None


def _get_client():
    """The OpenAI client, created once on first use; None without a key or the package."""
    global client
    if client is None and OPENAI_API_KEY and openai is not None:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
    return client

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

        if entity:
            entity_types[entity] = entity_type
        if label:
            entity_types[label] = entity_type
            entity_types[label.lower()] = entity_type

    print(f"Loaded {len(entity_types)} entity type mappings")
    return entity_types

def get_predicate_patterns() -> List[Dict]:
    """Get all predicates with their usage patterns"""

    # Check if we already have saved patterns
    if Path(PATTERNS_FILE).exists():
        print(f"Loading patterns from {PATTERNS_FILE}")
        with open(PATTERNS_FILE, 'r') as f:
            return json.load(f)

    print("Fetching predicate patterns from graph...")
    entity_types = get_entity_types()

    # Get all predicates with counts
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
        predicate = binding["predicate"]["value"]
        count = int(binding["count"]["value"])

        # Get sample usage for context
        sample_query = f"""
        PREFIX regx: <https://regen.network/ontology/experimental#>
        SELECT ?subject ?object WHERE {{
            ?stmt regx:predicate "{predicate}" .
            ?stmt regx:subject ?subject .
            ?stmt regx:object ?object .
        }}
        LIMIT 5
        """

        sample_results = execute_sparql(sample_query)

        subjects = []
        objects = []
        subject_types = Counter()
        object_types = Counter()

        for sample in sample_results["results"]["bindings"]:
            subj = sample.get("subject", {}).get("value", "")
            obj = sample.get("object", {}).get("value", "")

            subjects.append(subj[:100])
            objects.append(obj[:100])

            # Determine types
            subj_type = entity_types.get(subj, entity_types.get(subj.lower(), "Entity"))
            subject_types[subj_type] += 1

            # For objects, simple type inference
            if obj in entity_types:
                obj_type = entity_types[obj]
            elif obj.startswith("http"):
                obj_type = "URL"
            elif len(obj) < 50:
                obj_type = "Short"
            else:
                obj_type = "Text"
            object_types[obj_type] += 1

        predicates.append({
            "predicate": predicate,
            "count": count,
            "sample_subjects": subjects,
            "sample_objects": objects,
            "subject_types": dict(subject_types),
            "object_types": dict(object_types)
        })

    # Save patterns for reuse
    print(f"Saving patterns to {PATTERNS_FILE}")
    with open(PATTERNS_FILE, 'w') as f:
        json.dump(predicates, f, indent=2)

    return predicates

def create_embedding_text(pred_info: Dict) -> str:
    """Create rich text for embedding that captures semantic meaning"""
    predicate = pred_info["predicate"]

    # Build comprehensive description
    parts = [f"Predicate: {predicate}"]

    # Add usage context
    if pred_info.get("subject_types"):
        top_subj_types = ", ".join(list(pred_info["subject_types"].keys())[:3])
        parts.append(f"Common subjects: {top_subj_types}")

    if pred_info.get("object_types"):
        top_obj_types = ", ".join(list(pred_info["object_types"].keys())[:3])
        parts.append(f"Common objects: {top_obj_types}")

    # Add sample usage
    if pred_info.get("sample_subjects") and pred_info.get("sample_objects"):
        sample_subj = pred_info["sample_subjects"][0] if pred_info["sample_subjects"] else ""
        sample_obj = pred_info["sample_objects"][0] if pred_info["sample_objects"] else ""
        if sample_subj and sample_obj:
            parts.append(f"Example: {sample_subj[:30]} {predicate} {sample_obj[:30]}")

    # Add linguistic hints
    pred_lower = predicate.lower()
    if any(word in pred_lower for word in ["create", "develop", "build", "make", "produce"]):
        parts.append("(creation/production)")
    elif any(word in pred_lower for word in ["support", "enable", "help", "facilitate"]):
        parts.append("(support/enablement)")
    elif any(word in pred_lower for word in ["is", "are", "was", "were"]):
        parts.append("(state/identity)")
    elif any(word in pred_lower for word in ["has", "have", "own", "possess"]):
        parts.append("(possession)")

    return " ".join(parts)

def compute_and_save_embeddings(predicates: List[Dict]) -> np.ndarray:
    """Compute embeddings and save them to disk"""

    # Check if embeddings already exist
    if Path(EMBEDDINGS_FILE).exists():
        print(f"Loading existing embeddings from {EMBEDDINGS_FILE}")
        with open(EMBEDDINGS_FILE, 'rb') as f:
            return pickle.load(f)

    print(f"Computing embeddings for {len(predicates)} predicates...")

    # Create embedding texts
    texts = [create_embedding_text(p) for p in predicates]

    # Get embeddings from OpenAI
    embeddings = []
    batch_size = 100

    for i in tqdm(range(0, len(texts), batch_size)):
        batch = texts[i:i+batch_size]

        try:
            client = _get_client()
            if client is None:
                raise RuntimeError("OpenAI client not available; cannot compute embeddings")
            response = client.embeddings.create(
                model="text-embedding-3-small",
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

    embeddings_array = np.array(embeddings)

    # Save embeddings
    print(f"Saving embeddings to {EMBEDDINGS_FILE}")
    with open(EMBEDDINGS_FILE, 'wb') as f:
        pickle.dump(embeddings_array, f)

    return embeddings_array

def cluster_predicates_func(predicates: List[Dict], embeddings: np.ndarray, threshold: float = 0.3) -> Dict[int, List[int]]:
    """Cluster predicates using hierarchical clustering with adjustable threshold"""
    print(f"Clustering with threshold {threshold}...")

    # Perform hierarchical clustering
    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=threshold,
        metric='cosine',
        linkage='average'
    )

    cluster_labels = clustering.fit_predict(embeddings)

    # Group indices by cluster
    clusters = defaultdict(list)
    for idx, label in enumerate(cluster_labels):
        clusters[label].append(idx)

    print(f"Created {len(clusters)} clusters")
    return dict(clusters)

def select_best_predicate(cluster_indices: List[int], predicates: List[Dict]) -> str:
    """Select the best representative predicate for a cluster"""

    cluster_predicates = [predicates[i] for i in cluster_indices]

    # Sort by frequency, simplicity, and clarity
    sorted_preds = sorted(cluster_predicates, key=lambda x: (
        -x["count"],                          # Most frequent first
        -sum(x["subject_types"].values()) if x.get("subject_types") else 0,  # Most typed usage
        len(x["predicate"].split()),          # Fewer words
        len(x["predicate"])                   # Shorter length
    ))

    # Get the best predicate and normalize it
    best = sorted_preds[0]["predicate"]

    # Normalize to a clean form
    best_lower = best.lower()

    # Remove common prefixes
    for prefix in ["is ", "are ", "was ", "were ", "has ", "have ", "had "]:
        if best_lower.startswith(prefix):
            best_lower = best_lower[len(prefix):]
            break

    # Convert to snake_case
    best_normalized = best_lower.replace(" ", "_").replace("-", "_")

    # Remove trailing 's' for some predicates to get base form
    if best_normalized.endswith("s") and len(best_normalized) > 3:
        # But keep words like "is", "has", "was"
        if best_normalized not in ["is", "has", "was", "does", "goes"]:
            best_normalized = best_normalized[:-1]

    return best_normalized

def create_consolidation_mapping(predicates: List[Dict], threshold: float = 0.3) -> Tuple[Dict[str, str], Dict]:
    """Create mapping with adjustable clustering threshold"""

    # Get or compute embeddings
    embeddings = compute_and_save_embeddings(predicates)

    # Cluster with given threshold - limit predicates to match embeddings size
    predicates_to_cluster = predicates[:len(embeddings)]
    clusters = cluster_predicates_func(predicates_to_cluster, embeddings, threshold)

    # Create mapping
    mapping = {}
    consolidation_info = {}

    for cluster_id, indices in clusters.items():
        # Select best predicate for cluster
        consolidated = select_best_predicate(indices, predicates_to_cluster)

        # Get all predicates in cluster
        cluster_predicates = [predicates_to_cluster[i]["predicate"] for i in indices]
        total_usage = sum(predicates_to_cluster[i]["count"] for i in indices)

        # Aggregate type information
        all_subject_types = Counter()
        all_object_types = Counter()

        for idx in indices:
            for st, count in predicates_to_cluster[idx].get("subject_types", {}).items():
                all_subject_types[st] += count
            for ot, count in predicates_to_cluster[idx].get("object_types", {}).items():
                all_object_types[ot] += count

        consolidation_info[consolidated] = {
            "members": cluster_predicates,
            "member_count": len(cluster_predicates),
            "total_usage": total_usage,
            "subject_types": dict(all_subject_types),
            "object_types": dict(all_object_types),
            "cluster_id": cluster_id
        }

        # Map each original predicate to consolidated
        for pred in cluster_predicates:
            mapping[pred] = consolidated

    return mapping, consolidation_info

def experiment_with_thresholds(predicates: List[Dict], thresholds: List[float] = None):
    """Experiment with different clustering thresholds"""

    if thresholds is None:
        thresholds = [0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5]

    # Get embeddings once
    embeddings = compute_and_save_embeddings(predicates)

    results = []

    print("\n" + "="*80)
    print("EXPERIMENTING WITH DIFFERENT CLUSTERING THRESHOLDS")
    print("="*80)

    for threshold in thresholds:
        clusters = cluster_predicates_func(predicates, embeddings, threshold)

        # Calculate statistics
        num_clusters = len(clusters)
        reduction_rate = (1 - num_clusters / len(predicates)) * 100

        # Get cluster size distribution
        cluster_sizes = [len(indices) for indices in clusters.values()]
        avg_cluster_size = np.mean(cluster_sizes)
        max_cluster_size = max(cluster_sizes)

        # Count singleton clusters
        singletons = sum(1 for size in cluster_sizes if size == 1)

        result = {
            "threshold": threshold,
            "num_clusters": num_clusters,
            "reduction_rate": reduction_rate,
            "avg_cluster_size": avg_cluster_size,
            "max_cluster_size": max_cluster_size,
            "singleton_clusters": singletons,
            "singleton_rate": (singletons / num_clusters * 100) if num_clusters > 0 else 0
        }

        results.append(result)

        print(f"\nThreshold: {threshold:.2f}")
        print(f"  Clusters: {num_clusters:4} | Reduction: {reduction_rate:5.1f}%")
        print(f"  Avg size: {avg_cluster_size:5.1f} | Max size: {max_cluster_size:4}")
        print(f"  Singletons: {singletons:4} ({result['singleton_rate']:.1f}%)")

    # Save results
    with open("threshold_experiments.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n✓ Experiment results saved to threshold_experiments.json")

    return results

def main(argv=None):
    global JENA_ENDPOINT
    parser = argparse.ArgumentParser(description='Consolidate predicates with adjustable threshold. '
                                                 'Default is a dry run that contacts nothing.')
    parser.add_argument('-t', '--threshold', type=float, default=0.30, help='Clustering threshold (0.1-0.5)')
    parser.add_argument('--full', action='store_true', help='Process all predicates (not just top 1000)')
    parser.add_argument('--min-usage', type=int, default=2, help='Minimum usage to include predicates')
    parser.add_argument('--endpoint', default=JENA_ENDPOINT,
                        help='SPARQL query endpoint to read predicates from (default: %(default)s)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report what would happen and exit (the default; wins over --apply)')
    parser.add_argument('--apply', action='store_true',
                        help='Read Fuseki, make the paid embedding calls (unless cached) and write the files')
    args = parser.parse_args(argv)
    JENA_ENDPOINT = args.endpoint

    print("\n" + "="*80)
    print("FINAL PREDICATE CONSOLIDATION WITH SAVED EMBEDDINGS")
    print("="*80)
    cached_patterns = Path(PATTERNS_FILE).exists()
    cached_embeddings = Path(EMBEDDINGS_FILE).exists()
    scope = "all predicates" if args.full else "the top 1000 predicates"
    print(f"Fuseki: {JENA_ENDPOINT}" + (f" (not read: {PATTERNS_FILE} exists)" if cached_patterns else ""))
    if cached_embeddings:
        print(f"Embeddings: loaded from {EMBEDDINGS_FILE}, no paid calls")
    else:
        print(f"Embeddings: {scope} with text-embedding-3-small, 100 per paid request"
              + ("" if args.full else f" (at most {math.ceil(1000 / 100)} requests)"))
    suffix = 'all_' if args.full else ''
    print(f"Writes: {PATTERNS_FILE}, {EMBEDDINGS_FILE}, final_consolidation_{suffix}t{args.threshold:.2f}.json")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was read, sent or written. Re-run with --apply to do this.")
        return None
    # Only require API key if embeddings don't exist
    if not OPENAI_API_KEY and not cached_embeddings:
        print("ERROR: OPENAI_API_KEY environment variable not set and no saved embeddings found")
        sys.exit(1)

    # Get predicate patterns
    predicates = get_predicate_patterns()

    # Filter to meaningful predicates
    predicates = [p for p in predicates if p["count"] >= args.min_usage]
    print(f"Processing {len(predicates)} predicates with usage >= {args.min_usage}")

    # Create consolidation mapping
    threshold = args.threshold
    print(f"\n=== Creating Consolidation with threshold {threshold} ===")
    cluster_set = predicates if args.full else predicates[:1000]
    mapping, consolidation_info = create_consolidation_mapping(cluster_set, threshold)

    # Analyze results
    original_count = len(cluster_set)
    consolidated_count = len(set(mapping.values()))
    reduction_rate = (1 - consolidated_count / original_count) * 100

    print(f"\n=== Results ===")
    print(f"Original predicates: {original_count}")
    print(f"Consolidated predicates: {consolidated_count}")
    print(f"Reduction rate: {reduction_rate:.1f}%")

    # Save final results
    results = {
        "threshold": threshold,
        "mapping": mapping,
        "consolidation_info": consolidation_info,
        "statistics": {
            "original_count": original_count,
            "consolidated_count": consolidated_count,
            "reduction_rate": reduction_rate
        }
    }

    def convert_numpy(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj

    suffix = 'all_' if args.full else ''
    output_file = f"final_consolidation_{suffix}t{threshold:.2f}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=convert_numpy)

    print(f"\n✓ Final consolidation saved to {output_file}")
    print(f"✓ Embeddings saved to {EMBEDDINGS_FILE} (reusable)")
    print(f"✓ Patterns saved to {PATTERNS_FILE} (reusable)")

    return results

if __name__ == "__main__":
    main()
