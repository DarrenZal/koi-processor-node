#!/usr/bin/env python3
"""
Archive old graph and deploy only refined graph

Destructive: with --apply --confirm-destructive it exports the dataset to
archive_full_graph_<timestamp>.ttl.gz in the working directory, sends CLEAR ALL to the
dataset, then loads refined_graph_20251017_001349.ttl from the working directory.

Run with no arguments (or --dry-run) to see what it would do; nothing is sent.
"""
import argparse
import os
import sys
import requests
from datetime import datetime
import gzip

ENDPOINT = "http://localhost:3030/koi"
AUTH = ("admin", "admin")

def archive_current():
    """Archive current graph to compressed file"""
    print("Archiving current graph...")

    # Export all triples
    response = requests.get(
        f"{ENDPOINT}/data",
        headers={"Accept": "text/turtle"},
        auth=AUTH
    )

    if response.status_code == 200:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = f"archive_full_graph_{timestamp}.ttl.gz"

        # Compress and save
        with gzip.open(archive_path, 'wt', encoding='utf-8') as f:
            f.write(response.text)

        size_mb = len(response.text) / (1024*1024)
        print(f"✅ Archived {size_mb:.2f}MB to {archive_path}")
        return True
    else:
        print(f"❌ Failed to export: {response.status_code}")
        return False

def clear_graph():
    """Clear entire graph"""
    print("Clearing graph...")

    response = requests.post(
        f"{ENDPOINT}/update",
        auth=AUTH,
        data={"update": "CLEAR ALL"}
    )

    if response.status_code == 200:
        print("✅ Graph cleared")
        return True
    else:
        print(f"❌ Failed to clear: {response.status_code}")
        return False

def load_refined():
    """Load refined graph"""
    print("Loading refined graph...")

    # Load the refined graph
    refined_path = REFINED_FILE

    with open(refined_path, 'r') as f:
        data = f.read()

    response = requests.post(
        f"{ENDPOINT}/data",
        data=data,
        headers={"Content-Type": "text/turtle"},
        auth=AUTH
    )

    if response.status_code in [200, 201, 204]:
        print("✅ Refined graph loaded")

        # Verify
        count_response = requests.post(
            f"{ENDPOINT}/sparql",
            auth=AUTH,
            data={"query": "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"}
        )

        if count_response.status_code == 200:
            import json
            count = json.loads(count_response.text)['results']['bindings'][0]['count']['value']
            print(f"✅ New triple count: {count}")

        return True
    else:
        print(f"❌ Failed to load: {response.status_code}")
        print(response.text[:500])
        return False

REFINED_FILE = "refined_graph_20251017_001349.ttl"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive the Fuseki dataset, CLEAR ALL, then load the refined graph. "
                    "Default is a dry run that sends nothing.")
    parser.add_argument("--endpoint", default=ENDPOINT,
                        help="Fuseki dataset URL (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Contact the endpoint and run the operation")
    parser.add_argument("--confirm-destructive", action="store_true",
                        help="Required with --apply: the operation clears every graph in the dataset")
    return parser


def main(argv=None) -> int:
    global ENDPOINT
    args = build_parser().parse_args(argv)
    ENDPOINT = args.endpoint.rstrip("/")

    print(f"Target: {ENDPOINT} (user {AUTH[0]}, password ***)")
    print(f"Refined file: {REFINED_FILE} ({'present' if os.path.exists(REFINED_FILE) else 'missing'} in {os.getcwd()})")
    print("Steps: export the dataset to archive_full_graph_<timestamp>.ttl.gz, send CLEAR ALL, "
          "then load the refined file")

    if args.dry_run or not args.apply:
        print("\nDry run: nothing was sent. Re-run with --apply --confirm-destructive to do this.")
        return 0
    if not args.confirm_destructive:
        print("\nRefusing: this clears every graph in the dataset. Add --confirm-destructive.",
              file=sys.stderr)
        return 2

    if archive_current():
        if clear_graph():
            load_refined()
    return 0


if __name__ == "__main__":
    sys.exit(main())
