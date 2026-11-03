#!/usr/bin/env python3
"""
Deploy refined graph to Apache Jena
Creates a new named graph to preserve the original

With --apply it reads refined_graph_20251017_001349.ttl from the working directory,
sends DROP GRAPH for today's named graph (https://regen.network/graphs/refined-<YYYYMMDD>,
the graph it then writes; no other graph is touched) and PUTs the file into it.

Run with no arguments (or --dry-run) to see what it would do; nothing is sent.
"""

import argparse
import os
import requests
import sys
from datetime import datetime

JENA_ENDPOINT = "http://localhost:3030/koi"
REFINED_GRAPH_FILE = "refined_graph_20251017_001349.ttl"
NAMED_GRAPH = f"https://regen.network/graphs/refined-{datetime.now().strftime('%Y%m%d')}"

def load_refined_graph():
    """Load refined graph to Jena as a named graph"""

    print(f"Loading refined graph to: {NAMED_GRAPH}")

    # Read the refined graph
    print(f"Reading {REFINED_GRAPH_FILE}...")
    with open(REFINED_GRAPH_FILE, 'r') as f:
        ttl_data = f.read()

    print(f"Graph size: {len(ttl_data)} bytes")

    # First, clear the named graph if it exists
    clear_query = f"DROP GRAPH <{NAMED_GRAPH}>"
    print(f"Clearing existing refined graph...")

    try:
        response = requests.post(
            f"{JENA_ENDPOINT}/update",
            data={"update": clear_query},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        print(f"Clear response: {response.status_code}")
    except Exception as e:
        print(f"Warning: Could not clear graph: {e}")

    # Load the refined graph
    print(f"Loading refined graph...")

    # Use the data endpoint for bulk loading
    try:
        response = requests.put(
            f"{JENA_ENDPOINT}/data?graph={NAMED_GRAPH}",
            data=ttl_data.encode('utf-8'),
            headers={"Content-Type": "text/turtle"}
        )

        if response.status_code in [200, 201, 204]:
            print(f"✅ Successfully loaded refined graph!")
        else:
            print(f"❌ Failed to load: {response.status_code}")
            print(response.text[:500])
            return False

    except Exception as e:
        print(f"Error loading graph: {e}")
        return False

    # Verify the load
    count_query = f"""
    SELECT (COUNT(*) as ?count)
    WHERE {{
        GRAPH <{NAMED_GRAPH}> {{
            ?s ?p ?o
        }}
    }}
    """

    try:
        response = requests.post(
            f"{JENA_ENDPOINT}/sparql",
            data={"query": count_query},
            headers={"Accept": "application/sparql-results+json"}
        )

        if response.status_code == 200:
            result = response.json()
            count = result['results']['bindings'][0]['count']['value']
            print(f"\n✅ Verification: {count} triples loaded to named graph")
            print(f"   Graph URI: {NAMED_GRAPH}")
        else:
            print(f"Could not verify: {response.status_code}")

    except Exception as e:
        print(f"Verification error: {e}")

    # Show how to query both graphs
    print("\n" + "="*60)
    print("DEPLOYMENT COMPLETE")
    print("="*60)
    print(f"\nTo query ONLY the refined graph:")
    print(f"  SELECT * WHERE {{ GRAPH <{NAMED_GRAPH}> {{ ?s ?p ?o }} }}")
    print(f"\nTo query the original graph:")
    print(f"  SELECT * WHERE {{ ?s ?p ?o }}")
    print(f"\nTo query BOTH graphs:")
    print(f"  SELECT * WHERE {{ {{ ?s ?p ?o }} UNION {{ GRAPH <{NAMED_GRAPH}> {{ ?s ?p ?o }} }} }}")

    return True

def test_refined_queries():
    """Test some queries against the refined graph"""

    print("\n" + "="*60)
    print("TESTING REFINED GRAPH QUERIES")
    print("="*60)

    # Test query 1: Count unique predicates in refined graph
    query1 = f"""
    PREFIX regx: <https://regen.network/ontology/experimental#>
    SELECT (COUNT(DISTINCT ?pred) as ?count)
    WHERE {{
        GRAPH <{NAMED_GRAPH}> {{
            ?stmt regx:predicate ?pred
        }}
    }}
    """

    response = requests.post(
        f"{JENA_ENDPOINT}/sparql",
        data={"query": query1},
        headers={"Accept": "application/sparql-results+json"}
    )

    if response.status_code == 200:
        result = response.json()
        count = result['results']['bindings'][0]['count']['value']
        print(f"\nUnique predicates in refined graph: {count}")

    # Test query 2: Sample consolidated predicates
    query2 = f"""
    PREFIX regx: <https://regen.network/ontology/experimental#>
    SELECT DISTINCT ?pred
    WHERE {{
        GRAPH <{NAMED_GRAPH}> {{
            ?stmt regx:predicate ?pred
        }}
    }}
    LIMIT 20
    """

    response = requests.post(
        f"{JENA_ENDPOINT}/sparql",
        data={"query": query2},
        headers={"Accept": "application/sparql-results+json"}
    )

    if response.status_code == 200:
        result = response.json()
        print(f"\nSample predicates from refined graph:")
        for binding in result['results']['bindings'][:10]:
            pred = binding['pred']['value']
            print(f"  - {pred}")

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replace today's refined named graph in Fuseki with the refined file. "
                    "Default is a dry run that sends nothing.")
    parser.add_argument("--endpoint", default=JENA_ENDPOINT,
                        help="Fuseki dataset URL (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Contact the endpoint: DROP GRAPH and PUT the named graph, then query it")
    return parser


def main(argv=None) -> int:
    global JENA_ENDPOINT
    args = build_parser().parse_args(argv)
    JENA_ENDPOINT = args.endpoint.rstrip("/")

    print(f"Target: {JENA_ENDPOINT}")
    present = os.path.exists(REFINED_GRAPH_FILE)
    print(f"Refined file: {REFINED_GRAPH_FILE} ({'present' if present else 'missing'} in {os.getcwd()})")
    print(f"Steps: DROP GRAPH <{NAMED_GRAPH}>, PUT the refined file into it, then run test queries")

    if args.dry_run or not args.apply:
        print("\nDry run: nothing was sent. Re-run with --apply to do this.")
        return 0

    if load_refined_graph():
        test_refined_queries()
    else:
        print("Failed to load refined graph")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
