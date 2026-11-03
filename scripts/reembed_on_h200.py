#!/usr/bin/env python3
"""Re-embed exported JSONL files using Qwen3-Embedding-0.6B on a local GPU.

Run this on a machine or notebook with GPU access (the file name records the GPU it
was first run on). Load the results with import_reembeddings.py.

Input: JSONL files with {"id": ..., "text": "..."}
Output: JSONL files with {"id": ..., "embedding": [...]}

DOCUMENT mode only (no instruction prefix) — these are stored embeddings.

Usage:
  pip install sentence-transformers
  python3 scripts/reembed_on_h200.py --input-dir ./reembed_data --output-dir ./reembed_results [--batch-size 128]
"""

import argparse
import json
import os
import time
from pathlib import Path

import torch
from sentence_transformers import SentenceTransformer


def reembed_file(model, input_path: str, output_path: str, batch_size: int = 128):
    """Re-embed all texts in a JSONL file."""
    # Load all records
    records = []
    with open(input_path) as f:
        for line in f:
            records.append(json.loads(line))

    if not records:
        print(f"  {os.path.basename(input_path)}: 0 records, skipping")
        return 0

    texts = [r["text"] for r in records]
    ids = [r["id"] for r in records]

    print(f"  {os.path.basename(input_path)}: {len(records)} records, batch_size={batch_size}")
    t0 = time.time()

    # Embed in batches — DOCUMENT mode (no prompt_name)
    all_embeddings = model.encode(
        texts,
        batch_size=batch_size,
        normalize_embeddings=True,
        show_progress_bar=True,
    )

    elapsed = time.time() - t0
    print(f"    Embedded in {elapsed:.1f}s ({len(records)/elapsed:.0f} texts/sec)")

    # Write output
    with open(output_path, "w") as f:
        for rid, emb in zip(ids, all_embeddings):
            f.write(json.dumps({"id": rid, "embedding": emb.tolist()}) + "\n")

    return len(records)


def main():
    parser = argparse.ArgumentParser(description="Re-embed on H200 GPU")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--model", default="Qwen/Qwen3-Embedding-0.6B")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    print(f"Device: {device}")
    print(f"Loading {args.model}...")
    model = SentenceTransformer(args.model, device=device)
    print(f"Model loaded. Dimension: {model.get_sentence_embedding_dimension()}")

    total = 0
    for fname in sorted(os.listdir(args.input_dir)):
        if not fname.endswith("_for_reembed.jsonl"):
            continue
        input_path = os.path.join(args.input_dir, fname)
        output_fname = fname.replace("_for_reembed.jsonl", "_embeddings.jsonl")
        output_path = os.path.join(args.output_dir, output_fname)
        total += reembed_file(model, input_path, output_path, args.batch_size)

    print(f"\nTotal: {total} embeddings generated in {args.output_dir}/")
    print("Transfer back and run: python3 scripts/import_reembeddings.py --input-dir ./reembed_results")


if __name__ == "__main__":
    main()
