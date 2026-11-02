#!/usr/bin/env python3
"""
Standalone CLI to parse a MediaWiki XML dump into per-page JSON,
a manifest JSONL, and an edges JSONL using the BKC mediawiki_parser.

No database connection required.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from dataclasses import asdict

# Ensure the koi-processor root is on sys.path so `api.*` is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from api.mediawiki_parser import PARSER_VERSION, parse_dump, parse_json_export

# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------

_SLUG_UNSAFE_RE = re.compile(r"[^a-z0-9\s-]")
_SLUG_SPACE_RE = re.compile(r"[\s]+")


def _title_to_slug(title: str, max_len: int = 100) -> str:
    """Convert a page title to a filesystem-safe slug."""
    s = unicodedata.normalize("NFC", title.strip().lower())
    s = _SLUG_UNSAFE_RE.sub("", s)
    s = _SLUG_SPACE_RE.sub("-", s).strip("-")
    return s[:max_len] if s else "untitled"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse a MediaWiki XML dump into structured JSON artifacts."
    )
    parser.add_argument(
        "--dump", required=True, help="Path to the MediaWiki dump file (XML or JSON)."
    )
    parser.add_argument(
        "--format",
        choices=["xml", "json"],
        default=None,
        help="Input format. Auto-detected from file extension if omitted.",
    )
    parser.add_argument(
        "--wiki-domain",
        required=True,
        help="Wiki domain for source_rid (e.g. salishsearestoration.org).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory for per-page JSON files.",
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path for the manifest JSONL file.",
    )
    parser.add_argument(
        "--edges",
        required=True,
        help="Path for the edges JSONL file.",
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.manifest)), exist_ok=True)
    os.makedirs(os.path.dirname(os.path.abspath(args.edges)), exist_ok=True)

    # Auto-detect format from extension if not specified
    fmt = args.format
    if fmt is None:
        fmt = "json" if args.dump.endswith(".json") else "xml"

    # Collect all parses
    if fmt == "json":
        parses = list(parse_json_export(args.dump, args.wiki_domain))
    else:
        parses = list(parse_dump(args.dump, args.wiki_domain))

    # Sort by promotion_priority descending for the manifest
    parses.sort(key=lambda p: p.promotion_priority, reverse=True)

    # Counters
    page_class_counts: dict[str, int] = {}
    template_type_counts: dict[str, int] = {}
    redirect_count = 0
    warning_count = 0
    auto_promotable = 0
    needs_review = 0
    below_threshold = 0

    edge_structural_t1 = 0
    edge_structural_t2 = 0
    edge_structural_t3 = 0
    edge_editorial = 0
    total_edges = 0

    # Track slug collisions
    slug_seen: dict[str, int] = {}

    # Write per-page JSON + collect manifest/edge rows
    manifest_rows: list[dict] = []
    edge_rows: list[dict] = []

    for p in parses:
        # Per-page JSON
        slug = _title_to_slug(p.title)
        if slug in slug_seen:
            slug_seen[slug] += 1
            slug = f"{slug}-{slug_seen[slug]}"
        else:
            slug_seen[slug] = 1

        page_path = os.path.join(args.output_dir, f"{slug}.json")
        with open(page_path, "w", encoding="utf-8") as f:
            json.dump(asdict(p), f, ensure_ascii=False, indent=2)

        # Manifest row
        manifest_rows.append({
            "title": p.title,
            "source_rid": p.source_rid,
            "page_id": p.page_id,
            "template_type": p.template_type,
            "bkc_entity_type": p.bkc_entity_type,
            "page_class": p.page_class,
            "word_count": p.word_count,
            "wikilink_count": len(p.wikilinks),
            "ingest_confidence": round(p.ingest_confidence, 2),
            "promotion_priority": round(p.promotion_priority, 2),
            "is_redirect": p.is_redirect,
            "parse_version": p.parse_version,
        })

        # Edges
        for se in p.structural_edges:
            edge_rows.append({
                "source": p.title,
                "target": se.target_title,
                "predicate": se.predicate,
                "edge_class": "structural",
                "field_name": se.field_name,
                "confidence": se.confidence,
                "source_section": se.source_section,
                "source_rid": p.source_rid,
            })
            total_edges += 1
            if se.confidence >= 0.95:
                edge_structural_t1 += 1
            elif se.confidence >= 0.85:
                edge_structural_t2 += 1
            else:
                edge_structural_t3 += 1

        for ee in p.editorial_edges:
            edge_rows.append({
                "source": p.title,
                "target": ee.target_title,
                "predicate": "related_to",
                "edge_class": "editorial",
                "field_name": None,
                "confidence": ee.confidence,
                "source_section": ee.source_section,
                "source_rid": p.source_rid,
            })
            total_edges += 1
            edge_editorial += 1

        # Counters
        page_class_counts[p.page_class] = page_class_counts.get(p.page_class, 0) + 1
        ttype = p.template_type or "None"
        template_type_counts[ttype] = template_type_counts.get(ttype, 0) + 1
        if p.is_redirect:
            redirect_count += 1
        warning_count += len(p.parse_warnings)

        if p.ingest_confidence >= 0.6:
            auto_promotable += 1
        elif p.ingest_confidence >= 0.4:
            needs_review += 1
        else:
            below_threshold += 1

    # Write manifest JSONL (already sorted by promotion_priority)
    with open(args.manifest, "w", encoding="utf-8") as f:
        for row in manifest_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # Write edges JSONL
    with open(args.edges, "w", encoding="utf-8") as f:
        for row in edge_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    # Count redirects vs disambig within alias_only
    alias_count = page_class_counts.get("alias_only", 0)
    disambig_count = alias_count - redirect_count  # rough heuristic
    if disambig_count < 0:
        disambig_count = 0

    # Dry-run summary
    total = len(parses)
    print(f"Parse version: {PARSER_VERSION}")
    print()
    print(f"Pages parsed:        {total}")
    print(f"  By page class:")
    print(f"    entity_bearing:  {page_class_counts.get('entity_bearing', 0)}")
    print(f"    source_only:     {page_class_counts.get('source_only', 0)}")
    print(f"    alias_only:      {alias_count}  (redirects: {redirect_count}, disambig: {disambig_count})")
    print()
    print(f"Auto-promotable (ingest_confidence >= 0.6):     {auto_promotable}")
    print(f"Needs review   (0.4 <= confidence < 0.6):       {needs_review}")
    print(f"Below threshold (confidence < 0.4):             {below_threshold}")
    print()
    print(f"Candidate edges:     {total_edges}")
    print(f"  structural tier 1: {edge_structural_t1}  (0.95)")
    print(f"  structural tier 2: {edge_structural_t2}  (0.85)")
    print(f"  structural tier 3: {edge_structural_t3}  (0.70)")
    print(f"  editorial:         {edge_editorial}  (0.60)")
    print()
    print(f"By template type:")
    for ttype in ["Topic", "Effort", "Workgroup", "Place", "Product", "None"]:
        print(f"  {ttype + ':':14s}{template_type_counts.get(ttype, 0)}")
    print()
    print(f"Redirects:           {redirect_count}")
    print(f"Parse warnings:      {warning_count}")


if __name__ == "__main__":
    main()
