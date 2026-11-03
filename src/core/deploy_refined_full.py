#!/usr/bin/env python3
"""
Deploy full refined graph to Apache Jena with authentication
Loads in batches to avoid timeout

Destructive: with --apply --confirm-destructive it first sends DELETE WHERE over every
regx:Statement that has a regx:originalPredicate (all previously loaded refined
statements, with no archive), then loads refined_graph_20251017_001349.ttl from the
working directory in INSERT DATA batches.

Run with no arguments (or --dry-run) to see what it would do; nothing is sent.
"""

import argparse
import os
import sys
import requests
import time
from tqdm import tqdm

JENA_ENDPOINT = "http://localhost:3030/koi"
REFINED_GRAPH_FILE = "refined_graph_20251017_001349.ttl"
AUTH = ('admin', 'admin')
BATCH_SIZE = 100  # Number of statements per batch

def clear_existing_refined():
    """Clear any existing refined statements"""
    print("Clearing existing refined statements...")

    # Delete all statements that have originalPredicate (indicates refined)
    clear_query = """
    PREFIX regx: <https://regen.network/ontology/experimental#>
    DELETE WHERE {
        ?stmt a regx:Statement ;
              regx:originalPredicate ?orig ;
              ?p ?o .
    }
    """

    response = requests.post(
        f"{JENA_ENDPOINT}/update",
        auth=AUTH,
        data={"update": clear_query},
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )

    print(f"Clear response: {response.status_code}")

def load_refined_graph():
    """Load the full refined graph in batches"""

    print(f"Loading refined graph from {REFINED_GRAPH_FILE}")

    # Read the file
    with open(REFINED_GRAPH_FILE, 'r') as f:
        lines = f.readlines()

    # Skip prefix and process lines
    statements = []
    current_statement = []

    for line in lines[1:]:  # Skip @prefix
        line = line.strip()
        if line and not line.startswith('#'):
            current_statement.append(line)
            # Each statement is 4-7 lines ending with a period on its own
            if line == '.':
                statements.append(' '.join(current_statement))
                current_statement = []

    print(f"Found {len(statements)} statements to load")

    # Load in batches
    successful = 0
    failed = 0

    for i in tqdm(range(0, len(statements), BATCH_SIZE), desc="Loading batches"):
        batch = statements[i:i+BATCH_SIZE]

        # Build INSERT DATA query
        triples = ' '.join(batch)
        update_query = f"INSERT DATA {{ {triples} }}"

        try:
            response = requests.post(
                f"{JENA_ENDPOINT}/update",
                auth=AUTH,
                data={"update": update_query},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30
            )

            if response.status_code == 200:
                successful += len(batch)
            else:
                failed += len(batch)
                print(f"\nBatch {i//BATCH_SIZE} failed: {response.status_code}")

        except Exception as e:
            failed += len(batch)
            print(f"\nBatch {i//BATCH_SIZE} error: {e}")

        # Small delay to avoid overwhelming the server
        time.sleep(0.1)

    print(f"\n✅ Loaded {successful} statements successfully")
    if failed > 0:
        print(f"❌ Failed to load {failed} statements")

    return successful

def verify_load():
    """Verify the refined graph was loaded"""

    queries = [
        # Count all statements
        ("Total statements", """
            PREFIX regx: <https://regen.network/ontology/experimental#>
            SELECT (COUNT(DISTINCT ?stmt) as ?count)
            WHERE { ?stmt a regx:Statement }
        """),

        # Count unique predicates
        ("Unique predicates", """
            PREFIX regx: <https://regen.network/ontology/experimental#>
            SELECT (COUNT(DISTINCT ?pred) as ?count)
            WHERE { ?stmt regx:predicate ?pred }
        """),

        # Count refined statements (with originalPredicate)
        ("Refined statements", """
            PREFIX regx: <https://regen.network/ontology/experimental#>
            SELECT (COUNT(DISTINCT ?stmt) as ?count)
            WHERE { ?stmt regx:originalPredicate ?orig }
        """),

        # Sample consolidated predicates
        ("Sample predicates", """
            PREFIX regx: <https://regen.network/ontology/experimental#>
            SELECT DISTINCT ?pred WHERE {
                ?stmt regx:predicate ?pred
            } LIMIT 10
        """)
    ]

    print("\n" + "="*60)
    print("VERIFICATION")
    print("="*60)

    for label, query in queries:
        response = requests.post(
            f"{JENA_ENDPOINT}/sparql",
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"}
        )

        if response.status_code == 200:
            result = response.json()
            bindings = result['results']['bindings']

            if label == "Sample predicates":
                print(f"\n{label}:")
                for binding in bindings:
                    print(f"  - {binding['pred']['value']}")
            else:
                count = bindings[0]['count']['value'] if bindings else 0
                print(f"{label}: {count}")

def test_nl_queries():
    """Test some natural language style queries"""

    print("\n" + "="*60)
    print("TESTING NATURAL LANGUAGE QUERIES")
    print("="*60)

    test_queries = [
        ("Find creation relationships", """
            PREFIX regx: <https://regen.network/ontology/experimental#>
            SELECT ?subject ?object WHERE {
                ?stmt regx:subject ?subject ;
                      regx:predicate ?pred ;
                      regx:object ?object .
                FILTER(?pred = "created" || ?pred = "developed" || ?pred = "built")
            } LIMIT 5
        """),

        ("Find Gregory Landua's work", """
            PREFIX regx: <https://regen.network/ontology/experimental#>
            SELECT ?predicate ?object WHERE {
                ?stmt regx:subject ?subject ;
                      regx:predicate ?predicate ;
                      regx:object ?object .
                FILTER(CONTAINS(LCASE(?subject), "gregory"))
            } LIMIT 5
        """)
    ]

    for label, query in test_queries:
        print(f"\n{label}:")
        response = requests.post(
            f"{JENA_ENDPOINT}/sparql",
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"}
        )

        if response.status_code == 200:
            result = response.json()
            bindings = result['results']['bindings']

            if bindings:
                for binding in bindings[:3]:
                    if 'subject' in binding:
                        subj = binding['subject']['value'][:50]
                        obj = binding['object']['value'][:50]
                        print(f"  {subj} → {obj}")
                    else:
                        pred = binding['predicate']['value']
                        obj = binding['object']['value'][:50]
                        print(f"  → {pred} → {obj}")
            else:
                print("  No results found")

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Delete every refined statement in Fuseki, then load the refined graph "
                    "in batches. Default is a dry run that sends nothing.")
    parser.add_argument("--endpoint", default=JENA_ENDPOINT,
                        help="Fuseki dataset URL (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Contact the endpoint and run the operation")
    parser.add_argument("--confirm-destructive", action="store_true",
                        help="Required with --apply: the operation deletes every refined "
                             "statement in the dataset, with no archive")
    return parser


def main(argv=None) -> int:
    global JENA_ENDPOINT
    args = build_parser().parse_args(argv)
    JENA_ENDPOINT = args.endpoint.rstrip("/")

    print(f"Target: {JENA_ENDPOINT} (user {AUTH[0]}, password ***)")
    present = os.path.exists(REFINED_GRAPH_FILE)
    print(f"Refined file: {REFINED_GRAPH_FILE} ({'present' if present else 'missing'} in {os.getcwd()})")
    print("Steps: DELETE WHERE over every regx:Statement with regx:originalPredicate (no archive), "
          f"load the refined file in batches of {BATCH_SIZE}, then run verification queries")

    if args.dry_run or not args.apply:
        print("\nDry run: nothing was sent. Re-run with --apply --confirm-destructive to do this.")
        return 0
    if not args.confirm_destructive:
        print("\nRefusing: this deletes every refined statement in the dataset. "
              "Add --confirm-destructive.", file=sys.stderr)
        return 2

    # Clear existing refined data
    clear_existing_refined()

    # Load new refined graph
    count = load_refined_graph()

    # Verify the load
    if count > 0:
        verify_load()
        test_nl_queries()

    print("\n✅ Deployment complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
