#!/usr/bin/env python3
"""
Week 12: Predicate Normalization (named after the sprint that wrote it)

Strategy: Collapse near-duplicate predicates into a canonical set.
Focus areas:
1. Platform predicates (linked_to, published_on, hosted_on)
2. Tense variants (support → supports, use → uses)
3. Role predicates (is_ceo_of → leads, founder_of → founded)

This extends the earlier predicate cleanup with targeted normalization for
semantic consistency.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv(Path(__file__).parent.parent / ".env")


# ============================================================================
# WEEK 12 NORMALIZATION MAP
# ============================================================================

WEEK12_NORMALIZATION_MAP = {
    # =========================================================================
    # 1. PLATFORM PREDICATES (platform/tool relationships)
    # =========================================================================

    # linked_to → associated_with (generic relationship)
    # Rationale: "linked_to" is vague, maps to existing canonical "associated_with"
    "linked_to": "associated_with",

    # published_on → documents_on (content on platforms)
    # Rationale: Both express "content exists on platform", consolidate for queryability
    "published_on": "documents_on",

    # hosted_on → uses (infrastructure relationship)
    # Rationale: "hosted_on" implies infrastructure usage, "uses" is the canonical
    # for technology/platform relationships. Keep "documents_on" for documentation.
    "hosted_on": "uses",

    # =========================================================================
    # 2. TENSE VARIANTS (present tense base forms)
    # =========================================================================

    # support/supporting → supports
    "support": "supports",
    "supporting": "supports",

    # use/utilized → uses
    "use": "uses",
    "utilized": "uses",
    "utilizing": "uses",

    # create/creating → creates
    "create": "creates",
    "creating": "creates",

    # enable/enabling → enables
    "enable": "enables",
    "enabling": "enables",

    # provide/providing → provides
    "provide": "provides",
    "providing": "provides",

    # generate/generating → creates (semantic mapping)
    "generate": "creates",
    "generating": "creates",

    # represent/representing → represents
    "represent": "represents",
    "representing": "represents",

    # explore/exploring → discusses (semantic mapping for investigative predicates)
    "explore": "discusses",
    "explores": "discusses",
    "exploring": "discusses",

    # present/presenting → discusses
    "presented": "discusses",
    "presenting": "discusses",
    "presented_at": "participates_in",  # More specific: presented at event

    # =========================================================================
    # 3. ROLE PREDICATES (founder/CEO patterns)
    # =========================================================================

    # founder variants → founded
    "founder_of": "founded",
    "is_founder_of": "founded",
    "co_founder_of": "founded",
    "is_co_founder_of": "founded",
    "co_founded": "founded",

    # CEO/leader variants → leads
    "is_ceo_of": "leads",
    "ceo_of": "leads",
    "is_leader_of": "leads",

    # =========================================================================
    # 4. COMPOUND PREDICATES (simplify to base)
    # =========================================================================

    # asked_* patterns → discusses
    "asked_about": "discusses",
    "asks_about": "discusses",
    "asked_question_about": "discusses",
    "asked": "discusses",
    "asks": "discusses",

    # helps_* patterns → supports
    "helps_solve": "supports",
    "helps_with": "supports",

    # allows_* patterns → enables
    "allows_querying_of": "enables",
    "allows": "enables",

    # =========================================================================
    # 5. MISCELLANEOUS CLEANUP
    # =========================================================================

    # Typos and malformed
    "alignswith": "aligns_with",
    "measuresintensityof": "measures",

    # Unnecessary specificity
    "forms_part_of": "part_of",
    "falls_under": "part_of",
    "described_as": "is_a",
    "categorized_as": "is_a",
    "positioned_as": "is_a",

    # Process words that should be verb forms
    "process": "processes",
    "briefing": "discusses",

    # Ownership patterns
    "is_issuer_of": "issues",
    "is_subject_of": "addresses",

    # Participation patterns
    "joined": "participates_in",
    "joins": "participates_in",
    "attends": "participates_in",
}


class Week12PredicateNormalizer:
    """Apply Week 12 predicate normalization to koi_relationships."""

    def __init__(self, dry_run: bool = True):
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5433)),
            "database": os.getenv("POSTGRES_DB", "eliza"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }
        self.dry_run = dry_run
        self.stats = {
            "before": {},
            "after": {},
            "mappings_applied": {},
            "duplicates_removed": 0,
        }

    def connect_db(self):
        return psycopg2.connect(**self.db_config)

    def get_predicate_counts(self) -> Dict[str, int]:
        """Get current predicate distribution."""
        conn = self.connect_db()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT predicate, COUNT(*) as count
            FROM koi_relationships
            GROUP BY predicate
            ORDER BY count DESC
        """)
        result = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return result

    def get_affected_predicates(self) -> List[Tuple[str, int]]:
        """Get predicates that will be affected by normalization."""
        counts = self.get_predicate_counts()
        affected = []
        for old_pred, new_pred in WEEK12_NORMALIZATION_MAP.items():
            if old_pred in counts:
                affected.append((old_pred, counts[old_pred], new_pred))
        return sorted(affected, key=lambda x: x[1], reverse=True)

    def analyze(self):
        """Analyze impact of normalization."""
        print("\n" + "=" * 80)
        print("WEEK 12: PREDICATE NORMALIZATION ANALYSIS")
        print("=" * 80)
        print(f"Generated: {datetime.now().isoformat()}")
        print(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE UPDATE'}")

        # Before counts
        before = self.get_predicate_counts()
        self.stats["before"] = {
            "distinct_predicates": len(before),
            "total_relationships": sum(before.values()),
        }

        print(f"\n📊 Current State:")
        print(f"   Distinct predicates: {self.stats['before']['distinct_predicates']}")
        print(f"   Total relationships: {self.stats['before']['total_relationships']}")

        # Affected predicates
        affected = self.get_affected_predicates()
        total_affected_rows = sum(count for _, count, _ in affected)

        print(f"\n📋 Predicates to Normalize: {len(affected)}")
        print(f"   Total rows affected: {total_affected_rows}")
        print("-" * 70)

        # Group by target predicate
        by_target = defaultdict(list)
        for old_pred, count, new_pred in affected:
            by_target[new_pred].append((old_pred, count))

        for target, sources in sorted(by_target.items(), key=lambda x: sum(c for _, c in x[1]), reverse=True):
            target_current = before.get(target, 0)
            incoming = sum(c for _, c in sources)
            print(f"\n   → {target} (current: {target_current}, +{incoming} incoming)")
            for old_pred, count in sorted(sources, key=lambda x: x[1], reverse=True):
                print(f"      {old_pred}: {count}")

        # Estimate final counts
        final_counts = before.copy()
        for old_pred, new_pred in WEEK12_NORMALIZATION_MAP.items():
            if old_pred in final_counts:
                old_count = final_counts.pop(old_pred)
                final_counts[new_pred] = final_counts.get(new_pred, 0) + old_count

        self.stats["after"] = {
            "distinct_predicates": len(final_counts),
            "total_relationships": sum(final_counts.values()),
        }

        reduction = self.stats["before"]["distinct_predicates"] - self.stats["after"]["distinct_predicates"]
        pct = (reduction / self.stats["before"]["distinct_predicates"]) * 100

        print(f"\n📈 Expected Results:")
        print(f"   Predicates: {self.stats['before']['distinct_predicates']} → {self.stats['after']['distinct_predicates']}")
        print(f"   Reduction: {reduction} ({pct:.1f}%)")

        return affected

    def apply(self):
        """Apply the normalization."""
        if self.dry_run:
            print("\n⚠️  DRY RUN - No changes will be made")
            return

        print("\n" + "=" * 80)
        print("APPLYING WEEK 12 NORMALIZATION")
        print("=" * 80)

        conn = self.connect_db()
        cursor = conn.cursor()

        # Group mappings by target to handle dedup properly
        by_target = defaultdict(list)
        for old_pred, new_pred in WEEK12_NORMALIZATION_MAP.items():
            by_target[new_pred].append(old_pred)

        total_updated = 0
        total_deduped = 0

        for target, sources in by_target.items():
            # All predicates that will become this target (including target itself)
            all_preds = sources + [target]

            # Step 1: Delete duplicates that would be created
            # Keep the canonical version if it exists, otherwise first variant
            cursor.execute("""
                WITH to_delete AS (
                    SELECT r.id,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.subject_entity_id, r.object_entity_id
                               ORDER BY CASE WHEN r.predicate = %s THEN 0 ELSE 1 END,
                                        r.confidence DESC, r.id
                           ) as rn
                    FROM koi_relationships r
                    WHERE r.predicate = ANY(%s)
                )
                DELETE FROM koi_relationships
                WHERE id IN (SELECT id FROM to_delete WHERE rn > 1)
            """, (target, all_preds))
            deleted = cursor.rowcount
            total_deduped += deleted
            if deleted > 0:
                print(f"   {target}: removed {deleted} duplicates")

            # Step 2: Update remaining rows
            for old_pred in sources:
                cursor.execute("""
                    UPDATE koi_relationships
                    SET predicate = %s
                    WHERE predicate = %s
                """, (target, old_pred))
                updated = cursor.rowcount
                total_updated += updated
                if updated > 0:
                    print(f"   {old_pred} → {target}: {updated} rows")
                    self.stats["mappings_applied"][old_pred] = {
                        "target": target,
                        "rows": updated
                    }

        conn.commit()
        conn.close()

        self.stats["duplicates_removed"] = total_deduped

        print(f"\n✓ Total rows updated: {total_updated}")
        print(f"✓ Total duplicates removed: {total_deduped}")

    def verify(self):
        """Verify the results."""
        print("\n" + "=" * 80)
        print("VERIFICATION")
        print("=" * 80)

        after = self.get_predicate_counts()
        print(f"\n📊 Final State:")
        print(f"   Distinct predicates: {len(after)}")
        print(f"   Total relationships: {sum(after.values())}")

        # Check that old predicates are gone
        remaining_old = [p for p in WEEK12_NORMALIZATION_MAP.keys() if p in after]
        if remaining_old:
            print(f"\n⚠️  Old predicates still present: {remaining_old}")
        else:
            print(f"\n✓ All old predicates successfully normalized")

        # Show new top 20
        print(f"\n📈 New Top 20 Predicates:")
        for pred, count in list(after.items())[:20]:
            print(f"   {pred}: {count}")

    def save_report(self, output_path: str):
        """Save the normalization report."""
        report = {
            "generated_at": datetime.now().isoformat(),
            "stats": self.stats,
            "normalization_map": WEEK12_NORMALIZATION_MAP,
            "rationale": {
                "platform_predicates": {
                    "linked_to → associated_with": "Generic relationship, no semantic value vs associated_with",
                    "published_on → documents_on": "Both express content-on-platform, consolidate for queryability",
                    "hosted_on → uses": "Infrastructure relationship maps to canonical 'uses'",
                },
                "tense_variants": "Normalize to present tense 3rd person singular (e.g., 'supports' not 'support')",
                "role_predicates": "Consolidate founder/CEO variants to canonical 'founded' and 'leads'",
                "compound_predicates": "Simplify 'helps_solve' → 'supports', 'asked_about' → 'discusses'",
            }
        }

        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)

        print(f"\n💾 Report saved to: {output_path}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Week 12 Predicate Normalization")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry run)")
    parser.add_argument("--output", "-o", default="week12_normalization_report.json",
                        help="Output report file")
    args = parser.parse_args()

    normalizer = Week12PredicateNormalizer(dry_run=not args.apply)
    normalizer.analyze()

    if args.apply:
        normalizer.apply()
        normalizer.verify()

    normalizer.save_report(args.output)

    print(f"\nCompleted: {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
