#!/usr/bin/env python3
"""Re-embed exported JSONL files via the remote embedding service.

Uses a remote FastAPI embedding service (default http://10.100.0.1:8352/embed, the
relay address of the federation tooling's WireGuard convention; override with
--service-url) instead of a local GPU/CPU. Slower than a local GPU, but works from any
machine that can reach the service. Load the results with import_reembeddings.py.

DOCUMENT mode only (is_query=false) — stored embeddings.

Usage:
  python3 scripts/reembed_via_poly.py --input-dir ./reembed_data --output-dir ./reembed_results [--batch-size 32]
"""

import argparse
import json
import os
import time

import requests


def reembed_file(service_url: str, input_path: str, output_path: str, batch_size: int = 32):
    """Re-embed all texts via the remote embedding service."""
    records = []
    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    if not records:
        print(f"  {os.path.basename(input_path)}: 0 records, skipping")
        return 0

    print(f"  {os.path.basename(input_path)}: {len(records)} records, batch_size={batch_size}")
    t0 = time.time()

    all_results = []
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        texts = [r["text"] for r in batch]
        ids = [r["id"] for r in batch]

        resp = requests.post(
            f"{service_url}/embed",
            json={"texts": texts, "is_query": False},
            timeout=120,
        )
        resp.raise_for_status()
        embeddings = resp.json()["embeddings"]

        for rid, emb in zip(ids, embeddings):
            all_results.append({"id": rid, "embedding": emb})

        if (i + batch_size) % (batch_size * 10) == 0 or i + batch_size >= len(records):
            elapsed = time.time() - t0
            done = min(i + batch_size, len(records))
            rate = done / elapsed if elapsed > 0 else 0
            print(f"    {done}/{len(records)} ({rate:.0f}/sec)")

    # Write output
    with open(output_path, "w") as f:
        for rec in all_results:
            f.write(json.dumps(rec) + "\n")

    elapsed = time.time() - t0
    print(f"    Done in {elapsed:.1f}s ({len(records)/elapsed:.0f} texts/sec)")
    return len(records)


def main():
    parser = argparse.ArgumentParser(description="Re-embed via poly service")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--service-url", default="http://10.100.0.1:8352")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # Health check
    resp = requests.get(f"{args.service_url}/health", timeout=5)
    resp.raise_for_status()
    health = resp.json()
    print(f"Poly service: {health['model']}, dim={health['dimension']}, norm={health['normalized']}")

    total = 0
    for fname in sorted(os.listdir(args.input_dir)):
        if not fname.endswith("_for_reembed.jsonl"):
            continue
        input_path = os.path.join(args.input_dir, fname)
        output_fname = fname.replace("_for_reembed.jsonl", "_embeddings.jsonl")
        output_path = os.path.join(args.output_dir, output_fname)
        total += reembed_file(args.service_url, input_path, output_path, args.batch_size)

    print(f"\nTotal: {total} embeddings generated")
    print("Next: python3 scripts/import_reembeddings.py --input-dir ./reembed_results")


if __name__ == "__main__":
    main()
