#!/usr/bin/env python3
"""
Advanced Predicate Consolidation using Semantic Similarity
Uses embeddings to find similar predicates and maps them to standard ontologies
"""

import json
import requests
import numpy as np
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Optional
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
import re

JENA_ENDPOINT = "http://localhost:3030/koi/sparql"

# Standard ontology predicates (from Schema.org, FOAF, Dublin Core, etc.)
STANDARD_PREDICATES = {
    # Schema.org relationships
    "schema:author": "The author of this content or rating",
    "schema:creator": "The creator/author of this CreativeWork",
    "schema:publisher": "The publisher of the creative work",
    "schema:about": "The subject matter of the content",
    "schema:mentions": "Indicates that the CreativeWork contains a reference to this",
    "schema:citation": "A citation or reference to another creative work",
    "schema:hasPart": "Indicates an item is part of this item",
    "schema:isPartOf": "Indicates an item that this item is part of",
    "schema:knows": "The most generic bi-directional social relation",
    "schema:affiliation": "An organization that this person is affiliated with",
    "schema:memberOf": "An Organization to which this Person belongs",
    "schema:employee": "Someone working for this organization",
    "schema:founder": "A person who founded this organization",
    "schema:owns": "Products owned by the organization or person",
    "schema:parentOrganization": "The larger organization that this organization is a subOrganization of",
    "schema:subOrganization": "A relationship between two organizations where one includes the other",

    # Dublin Core
    "dc:creator": "An entity primarily responsible for making the resource",
    "dc:contributor": "An entity responsible for making contributions to the resource",
    "dc:publisher": "An entity responsible for making the resource available",
    "dc:subject": "The topic of the resource",
    "dc:type": "The nature or genre of the resource",
    "dc:relation": "A related resource",
    "dc:source": "A related resource from which the described resource is derived",

    # FOAF (Friend of a Friend)
    "foaf:knows": "A person known by this person",
    "foaf:member": "Indicates a member of a Group",
    "foaf:made": "Something that was made by this agent",
    "foaf:maker": "An agent that made this thing",
    "foaf:primaryTopic": "The primary topic of some page or document",
    "foaf:topic": "A topic of some page or document",

    # PROV-O (Provenance Ontology)
    "prov:wasGeneratedBy": "Generation is the completion of production of a new entity",
    "prov:wasDerivedFrom": "A derivation is a transformation of an entity into another",
    "prov:wasAttributedTo": "Attribution is the ascribing of an entity to an agent",
    "prov:used": "Usage is the beginning of utilizing an entity by an activity",
    "prov:wasAssociatedWith": "An activity association is an assignment of responsibility",
    "prov:actedOnBehalfOf": "Delegation is the assignment of authority to an agent",

    # Common semantic relationships
    "rdf:type": "The subject is an instance of a class",
    "rdfs:subClassOf": "The subject is a subclass of a class",
    "owl:sameAs": "The property that determines that two URIs refer to the same thing",

    # Custom consolidated predicates for common patterns
    "custom:creates": "Creates, develops, builds, produces, generates",
    "custom:provides": "Provides, offers, supplies, delivers, gives",
    "custom:supports": "Supports, enables, facilitates, helps, assists",
    "custom:requires": "Requires, needs, depends on, necessitates",
    "custom:participatesIn": "Participates in, is involved in, engages in",
    "custom:manages": "Manages, controls, oversees, administers",
    "custom:communicates": "States, says, announces, declares, expresses",
    "custom:discusses": "Discusses, talks about, explores, examines",
    "custom:proposes": "Proposes, suggests, recommends, advocates",
    "custom:implements": "Implements, executes, carries out, performs",
    "custom:represents": "Represents, stands for, symbolizes, embodies",
    "custom:contains": "Contains, includes, comprises, consists of",
    "custom:relatesTo": "Relates to, is associated with, connects to",
    "custom:uses": "Uses, utilizes, employs, applies",
    "custom:focusesOn": "Focuses on, concentrates on, emphasizes, prioritizes"
}

def execute_sparql(query: str) -> dict:
    """Execute a SPARQL query against Apache Jena"""
    response = requests.post(
        JENA_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    return response.json() if response.status_code == 200 else {"results": {"bindings": []}}

def get_all_predicates_with_full_context():
    """Get all predicates with their usage context including subject/object examples"""
    print("Fetching predicates with full context...")

    query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?predicate
           (COUNT(*) as ?count)
           (GROUP_CONCAT(DISTINCT ?subject; separator=" | ") as ?subjects)
           (GROUP_CONCAT(DISTINCT ?object; separator=" | ") as ?objects)
    WHERE {
        ?stmt a regx:Statement .
        ?stmt regx:subject ?subject .
        ?stmt regx:predicate ?predicate .
        ?stmt regx:object ?object .
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)
    LIMIT 1000
    """

    results = execute_sparql(query)
    predicates = []

    for binding in results["results"]["bindings"]:
        # Get sample subjects and objects (first 3)
        subjects = binding.get("subjects", {}).get("value", "").split(" | ")[:3]
        objects = binding.get("objects", {}).get("value", "").split(" | ")[:3]

        predicates.append({
            "predicate": binding["predicate"]["value"],
            "count": int(binding["count"]["value"]),
            "sample_subjects": subjects,
            "sample_objects": objects
        })

    return predicates

def create_predicate_embedding_text(pred_info: Dict) -> str:
    """Create a text representation of predicate for embedding"""
    predicate = pred_info["predicate"]
    subjects = pred_info.get("sample_subjects", [])
    objects = pred_info.get("sample_objects", [])

    # Create context-rich text
    text_parts = [predicate]

    # Add subject context
    if subjects:
        subj_text = ", ".join(s[:50] for s in subjects[:2] if s)
        text_parts.append(f"subjects like {subj_text}")

    # Add object context
    if objects:
        obj_text = ", ".join(o[:50] for o in objects[:2] if o)
        text_parts.append(f"objects like {obj_text}")

    return " ".join(text_parts)

def get_mock_embeddings(texts: List[str]) -> np.ndarray:
    """Generate mock embeddings based on text features (placeholder for real embeddings)"""
    # In production, use real embeddings from OpenAI, BGE, or Sentence-Transformers
    # For now, create feature-based mock embeddings

    embeddings = []
    for text in texts:
        features = []

        # Length feature
        features.append(len(text) / 100)

        # Word count feature
        features.append(len(text.split()) / 10)

        # Verb tense features
        text_lower = text.lower()
        features.append(1.0 if any(x in text_lower for x in ["ed", "was", "were", "had"]) else 0.0)  # past
        features.append(1.0 if any(x in text_lower for x in ["s", "is", "are", "has"]) else 0.0)  # present
        features.append(1.0 if any(x in text_lower for x in ["ing"]) else 0.0)  # continuous

        # Semantic category features
        features.append(1.0 if any(x in text_lower for x in ["create", "develop", "build", "make"]) else 0.0)
        features.append(1.0 if any(x in text_lower for x in ["provide", "offer", "supply", "give"]) else 0.0)
        features.append(1.0 if any(x in text_lower for x in ["support", "enable", "help", "assist"]) else 0.0)
        features.append(1.0 if any(x in text_lower for x in ["require", "need", "depend"]) else 0.0)
        features.append(1.0 if any(x in text_lower for x in ["part", "include", "contain"]) else 0.0)
        features.append(1.0 if any(x in text_lower for x in ["relate", "associate", "connect"]) else 0.0)
        features.append(1.0 if any(x in text_lower for x in ["use", "utilize", "employ"]) else 0.0)

        # Preposition features
        for prep in ["with", "to", "from", "of", "in", "on", "at", "by", "for"]:
            features.append(1.0 if prep in text_lower else 0.0)

        # Pad to fixed size
        while len(features) < 50:
            features.append(0.0)

        embeddings.append(features[:50])

    return np.array(embeddings)

def cluster_predicates(predicates: List[Dict], threshold: float = 0.7) -> Dict[int, List[Dict]]:
    """Cluster predicates using hierarchical clustering"""
    print(f"Clustering {len(predicates)} predicates...")

    # Create embedding texts
    texts = [create_predicate_embedding_text(p) for p in predicates]

    # Get embeddings (mock for now, replace with real embeddings)
    embeddings = get_mock_embeddings(texts)

    # Compute similarity matrix
    similarity_matrix = cosine_similarity(embeddings)

    # Perform hierarchical clustering
    # Use cosine metric with average linkage
    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=1.0 - threshold,  # Convert similarity threshold to distance
        metric='cosine',
        linkage='average'
    )

    # Fit on embeddings directly (scikit-learn will compute distances internally)
    cluster_labels = clustering.fit_predict(embeddings)

    # Group predicates by cluster
    clusters = defaultdict(list)
    for i, label in enumerate(cluster_labels):
        clusters[label].append(predicates[i])

    return dict(clusters)

def select_cluster_representative(cluster: List[Dict]) -> str:
    """Select the best representative predicate for a cluster"""
    # Sort by frequency and simplicity
    sorted_cluster = sorted(cluster, key=lambda x: (
        -x["count"],  # Higher frequency first
        len(x["predicate"].split()),  # Fewer words first
        len(x["predicate"])  # Shorter length first
    ))

    # Get the most frequent, simplest predicate
    best = sorted_cluster[0]["predicate"]

    # Try to normalize to base form
    best_lower = best.lower()

    # Remove common prefixes
    for prefix in ["is ", "are ", "was ", "were ", "has ", "have ", "had "]:
        if best_lower.startswith(prefix):
            best_lower = best_lower[len(prefix):]
            break

    # Convert to snake_case
    best_normalized = best_lower.replace(" ", "_").replace("-", "_")

    return best_normalized

def map_to_standard_predicates(cluster_representative: str, cluster: List[Dict]) -> str:
    """Map cluster to standard ontology predicate if possible"""
    rep_lower = cluster_representative.lower()

    # Direct mappings to standard predicates
    mappings = {
        # Schema.org
        "author": "schema:author",
        "authored": "schema:author",
        "wrote": "schema:author",
        "created": "schema:creator",
        "creates": "schema:creator",
        "published": "schema:publisher",
        "publishes": "schema:publisher",
        "about": "schema:about",
        "mentions": "schema:mentions",
        "part_of": "schema:isPartOf",
        "has_part": "schema:hasPart",
        "knows": "schema:knows",
        "affiliated_with": "schema:affiliation",
        "member_of": "schema:memberOf",
        "employs": "schema:employee",
        "founded": "schema:founder",
        "owns": "schema:owns",

        # Dublin Core
        "contributor": "dc:contributor",
        "subject": "dc:subject",
        "type_of": "dc:type",
        "related_to": "dc:relation",
        "source": "dc:source",

        # FOAF
        "made": "foaf:made",
        "maker": "foaf:maker",

        # PROV-O
        "generated_by": "prov:wasGeneratedBy",
        "derived_from": "prov:wasDerivedFrom",
        "attributed_to": "prov:wasAttributedTo",
        "used": "prov:used",

        # Custom consolidated
        "provides": "custom:provides",
        "supports": "custom:supports",
        "requires": "custom:requires",
        "participates_in": "custom:participatesIn",
        "manages": "custom:manages",
        "discusses": "custom:discusses",
        "proposes": "custom:proposes",
        "implements": "custom:implements",
        "represents": "custom:represents",
        "contains": "custom:contains",
        "uses": "custom:uses",
        "focuses_on": "custom:focusesOn"
    }

    # Check for direct mapping
    if rep_lower in mappings:
        return mappings[rep_lower]

    # Check for partial matches
    for key, value in mappings.items():
        if key in rep_lower or rep_lower in key:
            return value

    # Return custom predicate with namespace
    return f"regen:{cluster_representative}"

def create_consolidated_mapping(predicates: List[Dict]) -> Tuple[Dict[str, str], Dict]:
    """Create final mapping from original to consolidated predicates"""
    # Cluster predicates
    clusters = cluster_predicates(predicates)
    print(f"Created {len(clusters)} clusters from {len(predicates)} predicates")

    # Create mapping
    mapping = {}
    cluster_info = {}

    for cluster_id, cluster in clusters.items():
        # Select representative
        representative = select_cluster_representative(cluster)

        # Map to standard if possible
        standard = map_to_standard_predicates(representative, cluster)

        # Store cluster info
        cluster_info[standard] = {
            "representative": representative,
            "standard": standard,
            "members": [p["predicate"] for p in cluster],
            "total_count": sum(p["count"] for p in cluster)
        }

        # Create individual mappings
        for pred_info in cluster:
            mapping[pred_info["predicate"]] = standard

    return mapping, cluster_info

def analyze_consolidation(predicates: List[Dict], mapping: Dict[str, str], cluster_info: Dict) -> Dict:
    """Analyze the impact and quality of consolidation"""
    # Count statistics
    original_count = len(predicates)
    consolidated_count = len(set(mapping.values()))

    # Calculate distribution
    consolidated_counts = Counter()
    for pred_info in predicates:
        consolidated = mapping[pred_info["predicate"]]
        consolidated_counts[consolidated] += pred_info["count"]

    # Find predicates mapped to standard ontologies
    standard_mappings = [p for p in set(mapping.values()) if ":" in p and not p.startswith("regen:")]
    custom_mappings = [p for p in set(mapping.values()) if p.startswith("custom:")]
    regen_mappings = [p for p in set(mapping.values()) if p.startswith("regen:")]

    return {
        "original_count": original_count,
        "consolidated_count": consolidated_count,
        "reduction_rate": (1 - consolidated_count / original_count) * 100,
        "standard_ontology_predicates": len(standard_mappings),
        "custom_consolidated_predicates": len(custom_mappings),
        "regen_specific_predicates": len(regen_mappings),
        "top_consolidated": consolidated_counts.most_common(30),
        "cluster_sizes": Counter(len(ci["members"]) for ci in cluster_info.values())
    }

def main():
    print("\n" + "="*70)
    print("SEMANTIC PREDICATE CONSOLIDATION")
    print("="*70)

    # Get predicates with context
    predicates = get_all_predicates_with_full_context()
    print(f"Loaded {len(predicates)} predicates")

    # Create consolidated mapping
    print("\n=== Creating Semantic Consolidation ===")
    mapping, cluster_info = create_consolidated_mapping(predicates)

    # Analyze results
    analysis = analyze_consolidation(predicates, mapping, cluster_info)

    print(f"\n=== Consolidation Results ===")
    print(f"Original predicates: {analysis['original_count']}")
    print(f"Consolidated predicates: {analysis['consolidated_count']}")
    print(f"Reduction rate: {analysis['reduction_rate']:.1f}%")
    print(f"\nMapping to standard ontologies:")
    print(f"- Standard ontology predicates: {analysis['standard_ontology_predicates']}")
    print(f"- Custom consolidated predicates: {analysis['custom_consolidated_predicates']}")
    print(f"- Regen-specific predicates: {analysis['regen_specific_predicates']}")

    print(f"\n=== Cluster Size Distribution ===")
    for size, count in sorted(analysis['cluster_sizes'].items()):
        print(f"Clusters with {size} members: {count}")

    print(f"\n=== Top 30 Consolidated Predicates ===")
    for pred, count in analysis['top_consolidated']:
        namespace = pred.split(":")[0] if ":" in pred else "regen"
        print(f"{pred:40} {count:6,} uses ({namespace})")

    # Save results
    results = {
        "mapping": mapping,
        "cluster_info": cluster_info,
        "analysis": analysis
    }

    with open("semantic_predicate_mapping.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Semantic mapping saved to: semantic_predicate_mapping.json")

    # Save just the mapping for easy use
    with open("final_predicate_mapping.json", "w") as f:
        json.dump(mapping, f, indent=2)
    print(f"✓ Final mapping saved to: final_predicate_mapping.json")

    return results

if __name__ == "__main__":
    main()