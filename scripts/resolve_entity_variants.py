#!/usr/bin/env python3
"""
Polysemy-Aware Entity Resolution CLI

Command-line interface for the polysemy resolver module. Given a label
(and optional context/type hint), returns ranked entity variants for
disambiguation.

Usage:
    cd <repo> && set -a; source .env; set +a
    python scripts/resolve_entity_variants.py --label "notion"
    python scripts/resolve_entity_variants.py --label "ethereum" --type-hint TECHNOLOGY
    python scripts/resolve_entity_variants.py --report  # Generate sample report

For library usage, import from the module directly:
    from knowledge_graph.polysemy_resolver import resolve_entity_variants, resolve_entity

Author: Claude Code
Date: 2025-12-24
Version: 2.0.0 (CLI wrapper for module)
"""

import argparse
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import asdict
from pathlib import Path

# Add src to path for module imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# Import from the module
from knowledge_graph.polysemy_resolver import (
    resolve_entity,
    resolve_entity_variants,
    get_default_db_config,
    DEFAULT_TYPE_PRIORITY,
    EntityVariant,
    ResolutionResult,
)


# Re-export for backwards compatibility
def get_db_config() -> Dict[str, Any]:
    """Get database configuration from environment (backwards compat)."""
    return get_default_db_config()


def format_result_text(result: ResolutionResult) -> str:
    """Format resolution result as human-readable text."""
    lines = []
    lines.append(f"Query: '{result.query_label}'")
    if result.type_hint:
        lines.append(f"Type Hint: {result.type_hint}")
    lines.append(f"Variants Found: {result.variant_count}")
    lines.append(f"Is Polysemy: {result.is_polysemy}")
    lines.append(f"Resolution Method: {result.resolution_method}")
    lines.append("")

    if result.winner:
        lines.append("=== WINNER ===")
        lines.append(f"  Label: {result.winner.entity_text}")
        lines.append(f"  Type: {result.winner.entity_type}")
        lines.append(f"  URI: {result.winner.uri}")
        lines.append(f"  Occurrences: {result.winner.occurrence_count}")
        lines.append(f"  Relationships: {result.winner.relationship_count}")
        lines.append(f"  Score: {result.winner.score:.0f}")
        lines.append(f"  Why: {result.winner.score_breakdown}")
    else:
        lines.append("No matching entities found.")

    if result.alternatives:
        lines.append("")
        lines.append("=== ALTERNATIVES ===")
        for i, alt in enumerate(result.alternatives, 1):
            lines.append(f"  [{i}] {alt.entity_text} ({alt.entity_type})")
            lines.append(f"      Occ: {alt.occurrence_count}, Rels: {alt.relationship_count}, Score: {alt.score:.0f}")
            lines.append(f"      Why: {alt.score_breakdown}")

    return "\n".join(lines)


def format_result_json(result: ResolutionResult) -> str:
    """Format resolution result as JSON."""
    return json.dumps(result.to_dict(), indent=2)


def generate_sample_report(conn, output_path: str) -> str:
    """
    Generate a sample report for known polysemy labels.
    """
    sample_labels = [
        ("notion", None),
        ("discord", None),
        ("telegram", None),
        ("ethereum", None),
        ("ethereum", "TECHNOLOGY"),
        ("ethereum", "ORGANIZATION"),
        ("sparql", None),
        ("rdf", None),
        ("regen commons", None),
        ("aerodrome", None),
        ("koi", None),
        ("blockchain", None),
        ("usdc", None),
        ("base", None),
        ("osmosis", None),
    ]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = []
    lines.append("# Polysemy Resolution Examples - Week 7")
    lines.append("")
    lines.append(f"**Generated:** {now}")
    lines.append(f"**Database:** {os.getenv('POSTGRES_DB', 'eliza')}")
    lines.append("")
    lines.append("This report demonstrates the polysemy-aware entity resolution")
    lines.append("system for handling ambiguous labels in query/GraphRAG contexts.")
    lines.append("")
    lines.append("---")
    lines.append("")

    for label, type_hint in sample_labels:
        result = resolve_entity(label, type_hint=type_hint, conn=conn)

        lines.append(f"## `{label}`" + (f" (hint: {type_hint})" if type_hint else ""))
        lines.append("")

        if result.winner:
            lines.append(f"**Winner:** {result.winner.entity_text} ({result.winner.entity_type})")
            lines.append(f"- Occurrences: {result.winner.occurrence_count}")
            lines.append(f"- Relationships: {result.winner.relationship_count}")
            lines.append(f"- Resolution: {result.resolution_method}")
            lines.append(f"- Is Polysemy: {result.is_polysemy}")

            if result.alternatives:
                lines.append("")
                lines.append("**Alternatives:**")
                lines.append("")
                lines.append("| Type | Occurrences | Relationships | Score |")
                lines.append("|------|-------------|---------------|-------|")
                for alt in result.alternatives:
                    lines.append(f"| {alt.entity_type} | {alt.occurrence_count} | {alt.relationship_count} | {alt.score:.0f} |")
        else:
            lines.append("No matching entities found.")

        lines.append("")
        lines.append("---")
        lines.append("")

    lines.append("## Usage in GraphRAG")
    lines.append("")
    lines.append("```python")
    lines.append("from knowledge_graph.polysemy_resolver import resolve_entity, resolve_entity_variants")
    lines.append("")
    lines.append("# Basic resolution (uses occurrence_count + connectivity)")
    lines.append("result = resolve_entity('notion')")
    lines.append("winner = result.winner  # EntityVariant object")
    lines.append("")
    lines.append("# With type hint (boost matching type)")
    lines.append("result = resolve_entity('ethereum', type_hint='TECHNOLOGY')")
    lines.append("")
    lines.append("# Get all variants for multi-type queries")
    lines.append("all_variants = [result.winner] + result.alternatives")
    lines.append("")
    lines.append("# Simple list of ranked variants (no ResolutionResult wrapper)")
    lines.append("variants = resolve_entity_variants('notion', limit=5)")
    lines.append("for v in variants:")
    lines.append("    print(f\"{v['entity_text']} ({v['entity_type']}): {v['score']}\")")
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Report generated by `scripts/resolve_entity_variants.py`*")

    report = "\n".join(lines)

    with open(output_path, 'w') as f:
        f.write(report)

    return report


def main():
    parser = argparse.ArgumentParser(
        description="Polysemy-aware entity resolution for KOI knowledge graph"
    )
    parser.add_argument("--label", help="Entity label to resolve")
    parser.add_argument("--type-hint", help="Preferred entity type")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--report", action="store_true",
                       help="Generate sample report for known polysemy labels")
    parser.add_argument("--report-out",
                       default="docs/archive/reports/polysemy_resolution_examples_week7.md",
                       help="Output path for sample report")
    args = parser.parse_args()

    if not args.label and not args.report:
        parser.error("Either --label or --report is required")

    # Connect to database
    db_config = get_db_config()

    try:
        conn = psycopg2.connect(**db_config)
    except Exception as e:
        print(f"Error: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    if args.report:
        print(f"Generating polysemy resolution sample report...")
        report = generate_sample_report(conn, args.report_out)
        print(f"Report written to: {args.report_out}")
    else:
        result = resolve_entity(
            args.label,
            type_hint=args.type_hint,
            conn=conn
        )

        if args.json:
            print(format_result_json(result))
        else:
            print(format_result_text(result))

    conn.close()


if __name__ == "__main__":
    main()
