#!/usr/bin/env python3
"""
Extract Ontology from Apache Jena Graph
Analyzes the existing RDF graph to generate an ontology with:
- Classes and their instance counts
- Properties and their usage patterns
- Domain/range inference
- Common predicate patterns
"""

import json
import requests
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import sys
from urllib.parse import urlparse

# Configuration
JENA_ENDPOINT = "http://localhost:3030/koi/sparql"

def execute_sparql(query: str) -> dict:
    """Execute a SPARQL query against Apache Jena"""
    response = requests.post(
        JENA_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"}
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error executing SPARQL: {response.status_code}")
        return {"results": {"bindings": []}}

def get_namespace_prefix(uri: str) -> Tuple[str, str]:
    """Extract namespace and local name from URI"""
    if "#" in uri:
        namespace, local = uri.rsplit("#", 1)
        return namespace + "#", local
    elif "/" in uri:
        namespace, local = uri.rsplit("/", 1)
        return namespace + "/", local
    return uri, ""

def extract_classes():
    """Extract all classes and their instance counts"""
    print("\n=== Extracting Classes ===")

    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?class (COUNT(?instance) as ?count)
    WHERE {
        ?instance rdf:type ?class .
    }
    GROUP BY ?class
    ORDER BY DESC(?count)
    """

    results = execute_sparql(query)
    classes = {}

    for binding in results["results"]["bindings"]:
        class_uri = binding["class"]["value"]
        count = int(binding["count"]["value"])
        namespace, local_name = get_namespace_prefix(class_uri)

        classes[class_uri] = {
            "uri": class_uri,
            "namespace": namespace,
            "localName": local_name,
            "instanceCount": count
        }

    return classes

def extract_properties():
    """Extract all properties and their usage counts"""
    print("\n=== Extracting Properties ===")

    query = """
    SELECT ?property (COUNT(*) as ?count)
    WHERE {
        ?s ?property ?o .
    }
    GROUP BY ?property
    ORDER BY DESC(?count)
    """

    results = execute_sparql(query)
    properties = {}

    for binding in results["results"]["bindings"]:
        prop_uri = binding["property"]["value"]
        count = int(binding["count"]["value"])
        namespace, local_name = get_namespace_prefix(prop_uri)

        properties[prop_uri] = {
            "uri": prop_uri,
            "namespace": namespace,
            "localName": local_name,
            "usageCount": count,
            "domains": set(),
            "ranges": set()
        }

    return properties

def infer_property_domains_ranges(properties: Dict, limit: int = 100):
    """Infer domain and range for properties based on usage"""
    print("\n=== Inferring Property Domains and Ranges ===")

    for prop_uri, prop_data in list(properties.items())[:limit]:  # Limit to avoid too many queries
        # Get sample subjects and objects for this property
        query = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?subjectType ?objectType
        WHERE {{
            ?s <{prop_uri}> ?o .
            OPTIONAL {{ ?s rdf:type ?subjectType }}
            OPTIONAL {{ ?o rdf:type ?objectType }}
        }}
        LIMIT 100
        """

        results = execute_sparql(query)

        for binding in results["results"]["bindings"]:
            if "subjectType" in binding:
                prop_data["domains"].add(binding["subjectType"]["value"])
            if "objectType" in binding:
                prop_data["ranges"].add(binding["objectType"]["value"])

    # Convert sets to lists for JSON serialization
    for prop_data in properties.values():
        prop_data["domains"] = list(prop_data["domains"])
        prop_data["ranges"] = list(prop_data["ranges"])

def extract_statement_predicates():
    """Extract all unique predicates used in statements"""
    print("\n=== Extracting Statement Predicates ===")

    query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>
    SELECT DISTINCT ?predicate (COUNT(*) as ?count)
    WHERE {
        ?stmt a regx:Statement .
        ?stmt regx:predicate ?predicate .
    }
    GROUP BY ?predicate
    ORDER BY DESC(?count)
    LIMIT 100
    """

    results = execute_sparql(query)
    predicates = []

    for binding in results["results"]["bindings"]:
        predicate = binding["predicate"]["value"]
        count = int(binding["count"]["value"])
        predicates.append({
            "predicate": predicate,
            "count": count
        })

    return predicates

def extract_entity_types():
    """Extract entity type distribution"""
    print("\n=== Extracting Entity Types ===")

    query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>
    SELECT ?entityType (COUNT(*) as ?count)
    WHERE {
        ?entity regx:entityType ?entityType .
    }
    GROUP BY ?entityType
    ORDER BY DESC(?count)
    """

    results = execute_sparql(query)
    entity_types = []

    for binding in results["results"]["bindings"]:
        entity_types.append({
            "type": binding["entityType"]["value"],
            "count": int(binding["count"]["value"])
        })

    return entity_types

def extract_namespace_statistics():
    """Get statistics about namespace usage"""
    print("\n=== Analyzing Namespaces ===")

    # Count subjects by namespace
    query = """
    SELECT ?s WHERE { ?s ?p ?o }
    LIMIT 10000
    """

    results = execute_sparql(query)
    namespace_counts = defaultdict(int)

    for binding in results["results"]["bindings"]:
        if "s" in binding and binding["s"]["type"] == "uri":
            uri = binding["s"]["value"]
            namespace, _ = get_namespace_prefix(uri)
            namespace_counts[namespace] += 1

    return dict(namespace_counts)

def generate_ontology_report():
    """Generate a comprehensive ontology report from the graph"""
    print("Extracting Ontology from Apache Jena Graph")
    print("=" * 50)

    # Extract all components
    classes = extract_classes()
    properties = extract_properties()
    infer_property_domains_ranges(properties, limit=50)  # Limit for performance
    predicates = extract_statement_predicates()
    entity_types = extract_entity_types()
    namespaces = extract_namespace_statistics()

    # Create ontology structure
    ontology = {
        "metadata": {
            "extractedFrom": JENA_ENDPOINT,
            "totalClasses": len(classes),
            "totalProperties": len(properties),
            "totalStatementPredicates": len(predicates)
        },
        "namespaces": {
            "regx": "https://regen.network/ontology/experimental#",
            "schema": "http://schema.org/",
            "prov": "http://www.w3.org/ns/prov#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        },
        "classes": classes,
        "properties": properties,
        "statementPredicates": predicates,
        "entityTypes": entity_types,
        "namespaceStatistics": namespaces
    }

    return ontology

def generate_natural_language_summary(ontology: dict) -> str:
    """Generate a human-readable summary of the ontology"""
    summary = """
# Extracted Ontology Summary

## Overview
- Total Classes: {total_classes}
- Total Properties: {total_properties}
- Total Statement Predicates: {total_predicates}

## Top Classes by Instance Count:
""".format(
        total_classes=ontology["metadata"]["totalClasses"],
        total_properties=ontology["metadata"]["totalProperties"],
        total_predicates=ontology["metadata"]["totalStatementPredicates"]
    )

    # Top 10 classes
    sorted_classes = sorted(
        ontology["classes"].items(),
        key=lambda x: x[1]["instanceCount"],
        reverse=True
    )[:10]

    for class_uri, class_data in sorted_classes:
        summary += f"- {class_data['localName']}: {class_data['instanceCount']:,} instances\n"

    summary += "\n## Top Properties by Usage:\n"

    # Top 10 properties
    sorted_props = sorted(
        ontology["properties"].items(),
        key=lambda x: x[1]["usageCount"],
        reverse=True
    )[:10]

    for prop_uri, prop_data in sorted_props:
        summary += f"- {prop_data['localName']}: {prop_data['usageCount']:,} uses\n"

    summary += "\n## Top Statement Predicates (Relationships):\n"

    # Top 20 predicates
    for pred in ontology["statementPredicates"][:20]:
        summary += f"- {pred['predicate']}: {pred['count']} occurrences\n"

    summary += "\n## Entity Type Distribution:\n"
    for entity_type in ontology["entityTypes"]:
        summary += f"- {entity_type['type']}: {entity_type['count']:,} entities\n"

    return summary

def generate_sparql_context(ontology: dict) -> str:
    """Generate context for natural language to SPARQL conversion"""
    context = """
# SPARQL Query Context for Regen Network Knowledge Graph

## Namespaces
```sparql
PREFIX regx: <https://regen.network/ontology/experimental#>
PREFIX schema: <http://schema.org/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
```

## Core Classes
"""

    # Add top classes
    for class_uri, class_data in list(ontology["classes"].items())[:10]:
        if class_data["instanceCount"] > 100:
            context += f"- `{class_data['localName']}` ({class_data['instanceCount']} instances)\n"

    context += "\n## Core Properties for Statements\n"
    context += """
- `regx:subject` - The subject of a statement
- `regx:predicate` - The relationship/action
- `regx:object` - The object of the statement
- `regx:confidence` - Confidence score (0.0-1.0)
"""

    context += "\n## Common Predicates (Relationships)\n"
    context += "These are the actual relationships found in the data:\n"

    # Add top predicates
    for pred in ontology["statementPredicates"][:30]:
        context += f"- `{pred['predicate']}` ({pred['count']} uses)\n"

    context += "\n## Example SPARQL Patterns\n"
    context += """
### Find statements about an entity:
```sparql
SELECT ?stmt ?predicate ?object ?confidence WHERE {
  ?stmt a regx:Statement .
  ?stmt regx:subject ?subject .
  ?stmt regx:predicate ?predicate .
  ?stmt regx:object ?object .
  ?stmt regx:confidence ?confidence .
  FILTER(CONTAINS(LCASE(?subject), "regen network"))
}
```

### Find organizations:
```sparql
SELECT ?org ?label WHERE {
  ?org a schema:Organization .
  ?org rdfs:label ?label .
}
```

### Find high-confidence statements:
```sparql
SELECT ?subject ?predicate ?object WHERE {
  ?stmt regx:subject ?subject .
  ?stmt regx:predicate ?predicate .
  ?stmt regx:object ?object .
  ?stmt regx:confidence ?confidence .
  FILTER(?confidence > 0.8)
}
```
"""

    return context

def main():
    """Main execution"""
    print("\n" + "="*60)
    print("ONTOLOGY EXTRACTION FROM APACHE JENA GRAPH")
    print("="*60)

    # Generate ontology
    ontology = generate_ontology_report()

    # Save full ontology as JSON
    with open("extracted_ontology.json", "w") as f:
        json.dump(ontology, f, indent=2, default=str)
    print(f"\n✓ Full ontology saved to: extracted_ontology.json")

    # Generate and save human-readable summary
    summary = generate_natural_language_summary(ontology)
    with open("ontology_summary.md", "w") as f:
        f.write(summary)
    print(f"✓ Summary saved to: ontology_summary.md")

    # Generate and save SPARQL context
    context = generate_sparql_context(ontology)
    with open("sparql_context.md", "w") as f:
        f.write(context)
    print(f"✓ SPARQL context saved to: sparql_context.md")

    # Print summary to console
    print("\n" + "="*60)
    print("ONTOLOGY SUMMARY")
    print("="*60)
    print(summary)

    return ontology

if __name__ == "__main__":
    main()