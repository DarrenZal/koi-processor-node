#!/usr/bin/env python3
"""
Clean Jena: Archive current graph and load only refined

Destructive: with --apply --confirm-destructive it archives every triple of the dataset
to archive_full_<timestamp>.ttl.gz in the working directory, sends DROP ALL to the
dataset, then loads refined_graph_20251017_001349.ttl from the working directory.

Run with no arguments (or --dry-run) to see what it would do; nothing is sent.
"""
import argparse
import sys
import requests
import gzip
from datetime import datetime
import json

ENDPOINT = "http://localhost:3030/koi"
AUTH = ("admin", "admin")

def get_triple_count():
    """Get current triple count"""
    response = requests.post(
        f"{ENDPOINT}/sparql",
        auth=AUTH,
        headers={"Accept": "application/sparql-results+json"},
        data={"query": "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"}
    )
    if response.status_code == 200:
        data = json.loads(response.text)
        return int(data['results']['bindings'][0]['count']['value'])
    return 0

def archive_graph():
    """Archive current graph"""
    print("📦 Archiving current graph...")

    # Use CONSTRUCT to get all triples
    query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"

    response = requests.post(
        f"{ENDPOINT}/sparql",
        auth=AUTH,
        headers={"Accept": "text/turtle"},
        data={"query": query}
    )

    if response.status_code == 200:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = f"archive_full_{timestamp}.ttl.gz"

        # Compress and save
        with gzip.open(archive_path, 'wt', encoding='utf-8') as f:
            f.write(response.text)

        size_mb = len(response.text) / (1024*1024)
        compressed_mb = os.path.getsize(archive_path) / (1024*1024)
        print(f"✅ Archived {size_mb:.1f}MB → {compressed_mb:.1f}MB compressed")
        print(f"   Saved to: {archive_path}")
        return True
    else:
        print(f"❌ Archive failed: {response.status_code}")
        return False

def clear_graph():
    """Clear all data"""
    print("🗑️  Clearing graph...")

    response = requests.post(
        f"{ENDPOINT}/update",
        auth=AUTH,
        data={"update": "DROP ALL"}
    )

    if response.status_code in [200, 204]:
        count = get_triple_count()
        print(f"✅ Graph cleared. Triples: {count}")
        return True
    else:
        print(f"❌ Clear failed: {response.status_code}")
        return False

def load_refined():
    """Load refined graph in batches"""
    print("📥 Loading refined graph...")

    refined_path = REFINED_FILE

    if not os.path.exists(refined_path):
        print(f"❌ Refined graph not found: {refined_path}")
        return False

    # Read file
    with open(refined_path, 'r') as f:
        content = f.read()

    # Parse into statements (simple approach)
    statements = []
    current = []

    for line in content.split('\n'):
        if line.strip():
            current.append(line)
            if line.strip().endswith('.'):
                statements.append('\n'.join(current))
                current = []

    print(f"📊 Found {len(statements)} statements to load")

    # Load in batches of 1000
    batch_size = 1000
    loaded = 0

    for i in range(0, len(statements), batch_size):
        batch = statements[i:i+batch_size]
        ttl_batch = '@prefix regx: <https://regen.network/rdf/> .\n'
        ttl_batch += '\n'.join(batch)

        response = requests.post(
            f"{ENDPOINT}/data?default",
            auth=AUTH,
            headers={"Content-Type": "text/turtle"},
            data=ttl_batch
        )

        if response.status_code in [200, 201, 204]:
            loaded += len(batch)
            print(f"   Loaded {loaded}/{len(statements)} statements...")
        else:
            print(f"❌ Batch failed: {response.status_code}")
            print(response.text[:500])
            break

    final_count = get_triple_count()
    print(f"✅ Loading complete. Total triples: {final_count}")
    return True

import os

REFINED_FILE = "refined_graph_20251017_001349.ttl"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive the Fuseki dataset, DROP ALL, then load the refined graph. "
                    "Default is a dry run that sends nothing.")
    parser.add_argument("--endpoint", default=ENDPOINT,
                        help="Fuseki dataset URL (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Contact the endpoint and run the operation")
    parser.add_argument("--confirm-destructive", action="store_true",
                        help="Required with --apply: the operation drops every graph in the dataset")
    return parser


def main(argv=None) -> int:
    global ENDPOINT
    args = build_parser().parse_args(argv)
    ENDPOINT = args.endpoint.rstrip("/")

    print("=" * 60)
    print("JENA CLEANUP OPERATION")
    print("=" * 60)
    print(f"Target: {ENDPOINT} (user {AUTH[0]}, password ***)")
    print(f"Refined file: {REFINED_FILE} ({'present' if os.path.exists(REFINED_FILE) else 'missing'} in {os.getcwd()})")
    print("Steps: count triples; if any, archive them to archive_full_<timestamp>.ttl.gz, "
          "send DROP ALL, then load the refined file")

    if args.dry_run or not args.apply:
        print("\nDry run: nothing was sent. Re-run with --apply --confirm-destructive to do this.")
        return 0
    if not args.confirm_destructive:
        print("\nRefusing: this drops every graph in the dataset. Add --confirm-destructive.",
              file=sys.stderr)
        return 2

    # Check current state
    initial_count = get_triple_count()
    print(f"📊 Current triple count: {initial_count:,}")

    if initial_count > 0:
        # Archive
        if archive_graph():
            # Clear
            if clear_graph():
                # Load refined
                load_refined()
    else:
        print("⚠️  Graph is empty, loading refined...")
        load_refined()

    print("\n✨ Done!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
