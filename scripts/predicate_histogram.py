#!/usr/bin/env python3
"""
Predicate Histogram Audit Script

Generates a histogram of predicates from koi_relationships with:
1. Top 20 predicates with counts
2. Long tail predicates (count <= 5)
3. Sample edges for each predicate
4. Synonym/near-duplicate candidate detection

Used to inform the predicate normalization strategy.
"""

import os
import sys
import json
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv(Path(__file__).parent.parent / ".env")


class PredicateHistogram:
    """Generate predicate histogram and candidate detection."""

    # Known synonyms/near-duplicates to flag
    SYNONYM_CANDIDATES = {
        # linked_to family
        "linked_to": ["associated_with", "related_to", "connected_to"],
        "related_to": ["associated_with", "linked_to"],

        # published_on family
        "published_on": ["documents_on", "hosted_on"],
        "documents_on": ["published_on"],

        # hosted_on family
        "hosted_on": ["documents_on", "uses", "deployed_on"],

        # communicates_via family
        "communicates_via": ["uses", "integrates_with"],

        # integrates_with family
        "integrates_with": ["uses", "works_with", "connects"],
    }

    def __init__(self):
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5433)),
            "database": os.getenv("POSTGRES_DB", "eliza"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }

    def connect_db(self):
        return psycopg2.connect(**self.db_config)

    def get_predicate_distribution(self) -> List[Tuple[str, int]]:
        """Get all predicates with counts, ordered by frequency."""
        conn = self.connect_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT predicate, COUNT(*) as count
            FROM koi_relationships
            GROUP BY predicate
            ORDER BY count DESC
        """)
        predicates = cursor.fetchall()
        conn.close()
        return predicates

    def get_sample_edges(self, predicate: str, limit: int = 3) -> List[Dict]:
        """Get sample edges for a predicate."""
        conn = self.connect_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT
                s.normalized_text as subject,
                s.entity_type as subject_type,
                r.predicate,
                o.normalized_text as object,
                o.entity_type as object_type,
                r.confidence
            FROM koi_relationships r
            JOIN entity_registry s ON r.subject_entity_id = s.id
            JOIN entity_registry o ON r.object_entity_id = o.id
            WHERE r.predicate = %s
            ORDER BY r.confidence DESC
            LIMIT %s
        """, (predicate, limit))
        edges = cursor.fetchall()
        conn.close()
        return [dict(e) for e in edges]

    def find_overlapping_predicates(self, predicates: List[Tuple[str, int]]) -> Dict[str, List[str]]:
        """Find predicates that might overlap based on naming patterns."""
        pred_set = {p[0] for p in predicates}
        overlaps = defaultdict(list)

        for pred, _count in predicates:
            # Check known synonym candidates
            if pred in self.SYNONYM_CANDIDATES:
                for candidate in self.SYNONYM_CANDIDATES[pred]:
                    if candidate in pred_set:
                        overlaps[pred].append(candidate)

            # Check for tense variants
            base = pred
            variants = []

            # Past tense
            if pred.endswith("ed"):
                variants.append(pred[:-2])  # published -> publish
                variants.append(pred[:-2] + "s")  # published -> publishs (rare but check)
                if pred.endswith("ied"):
                    variants.append(pred[:-3] + "ies")  # carried -> carries

            # Present continuous
            if pred.endswith("ing"):
                variants.append(pred[:-3])  # using -> us
                variants.append(pred[:-3] + "es")  # using -> uses
                variants.append(pred[:-3] + "s")  # linking -> links

            # Check _on, _to, _with patterns
            suffixes = ["_on", "_to", "_with", "_in", "_for", "_by"]
            for suffix in suffixes:
                if pred.endswith(suffix):
                    base_pred = pred[:-len(suffix)]
                    # Check if base + other suffixes exist
                    for other_suffix in suffixes:
                        if other_suffix != suffix:
                            check = base_pred + other_suffix
                            if check in pred_set and check not in overlaps[pred]:
                                overlaps[pred].append(check)

            for var in variants:
                if var in pred_set and var != pred:
                    overlaps[pred].append(var)

        # Only return predicates that have overlaps
        return {k: v for k, v in overlaps.items() if v}

    def generate_report(self, output_path: Optional[str] = None) -> Dict:
        """Generate full histogram report."""
        print("\n" + "=" * 80)
        print("PREDICATE HISTOGRAM AUDIT")
        print("=" * 80)
        print(f"Generated: {datetime.now().isoformat()}")

        predicates = self.get_predicate_distribution()
        total_predicates = len(predicates)
        total_relationships = sum(c for _, c in predicates)

        print(f"\n📊 Summary:")
        print(f"   Total distinct predicates: {total_predicates}")
        print(f"   Total relationships: {total_relationships}")

        # Top 20
        top_20 = predicates[:20]
        print(f"\n📈 Top 20 Predicates (by frequency):")
        print("-" * 60)
        top_20_data = []
        for pred, count in top_20:
            pct = (count / total_relationships) * 100
            print(f"   {pred:40s} {count:5d} ({pct:.1f}%)")
            samples = self.get_sample_edges(pred, limit=2)
            top_20_data.append({
                "predicate": pred,
                "count": count,
                "percentage": round(pct, 2),
                "samples": samples
            })

        # Long tail (count <= 5)
        long_tail = [(p, c) for p, c in predicates if c <= 5]
        print(f"\n📉 Long Tail Predicates (count ≤ 5):")
        print(f"   Total: {len(long_tail)} predicates")
        print(f"   These represent {len(long_tail) / total_predicates * 100:.1f}% of distinct predicates")
        print(f"   But only {sum(c for _, c in long_tail)} relationships ({sum(c for _, c in long_tail) / total_relationships * 100:.2f}%)")

        # Show first 30 long tail
        print("\n   Sample long tail predicates:")
        for pred, count in long_tail[:30]:
            print(f"      {pred}: {count}")

        # Find overlaps
        overlaps = self.find_overlapping_predicates(predicates)
        print(f"\n🔄 Potential Overlapping Predicates:")
        print("-" * 60)
        if overlaps:
            for pred, candidates in sorted(overlaps.items(), key=lambda x: dict(predicates).get(x[0], 0), reverse=True)[:20]:
                pred_count = dict(predicates).get(pred, 0)
                print(f"   {pred} ({pred_count})")
                for cand in candidates:
                    cand_count = dict(predicates).get(cand, 0)
                    print(f"      → may overlap with: {cand} ({cand_count})")
        else:
            print("   No obvious overlaps detected")

        # Build report data
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_distinct_predicates": total_predicates,
                "total_relationships": total_relationships,
                "top_20_coverage": sum(c for _, c in top_20) / total_relationships * 100,
                "long_tail_count": len(long_tail),
                "long_tail_relationships": sum(c for _, c in long_tail),
            },
            "top_20": top_20_data,
            "long_tail": [{"predicate": p, "count": c} for p, c in long_tail],
            "overlapping_candidates": overlaps,
            "all_predicates": [{"predicate": p, "count": c} for p, c in predicates],
        }

        if output_path:
            with open(output_path, "w") as f:
                json.dump(report, f, indent=2)
            print(f"\n💾 Report saved to: {output_path}")

        return report


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Predicate Histogram Audit")
    parser.add_argument("--output", "-o", default="predicate_histogram_report.json",
                        help="Output JSON file path")
    parser.add_argument("--samples", "-s", type=int, default=3,
                        help="Number of sample edges per predicate")
    args = parser.parse_args()

    histogram = PredicateHistogram()
    report = histogram.generate_report(output_path=args.output)

    print("\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
