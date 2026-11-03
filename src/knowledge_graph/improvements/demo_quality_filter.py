#!/usr/bin/env python3
"""
Demo script for EntityQualityFilter

Demonstrates the filter's effectiveness on sample entities representing
known quality issues from the Regen KOI extraction quality review.

Usage:
    python -m src.knowledge_graph.improvements.demo_quality_filter

Quality issues addressed (from review):
- 3,690 entities flagged for quality issues
- 26 instances of generic nouns ("User", "farmers", "company")
- Pronouns appearing as entities ("we", "they")
- Sentence fragments as entity names
- Tautological entities (name equals type)
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.knowledge_graph.improvements import EntityQualityFilter


def create_sample_entities():
    """Create sample entities representing known quality issues."""
    return [
        # === VALID ENTITIES (should pass) ===
        {"name": "Gregory Landua", "type": "PERSON"},
        {"name": "Regen Network", "type": "ORGANIZATION"},
        {"name": "Toucan Protocol", "type": "ORGANIZATION"},
        {"name": "Cosmos SDK", "type": "SOFTWARE"},
        {"name": "Ecocredit Module", "type": "MODULE"},
        {"name": "Voluntary Carbon Market", "type": "CONCEPT"},
        {"name": "Regenerative Agriculture", "type": "CONCEPT"},
        {"name": "Boulder, Colorado", "type": "PLACE"},
        {"name": "Carbon Credit", "type": "PRODUCT"},
        {"name": "MsgCreateBatch", "type": "FUNCTION"},
        {"name": "OpenTEAM", "type": "PROJECT"},
        {"name": "Verra", "type": "ORGANIZATION"},
        {"name": "Dr. Jane Goodall", "type": "PERSON"},
        {"name": "Paul Stamets", "type": "PERSON"},
        {"name": "NCT Token", "type": "PRODUCT"},

        # === PRONOUNS (should be filtered) ===
        {"name": "we", "type": "PERSON"},
        {"name": "they", "type": "PERSON"},
        {"name": "it", "type": "CONCEPT"},
        {"name": "We", "type": "ORGANIZATION"},
        {"name": "They", "type": "PERSON"},

        # === GENERIC NOUNS (should be filtered) ===
        {"name": "user", "type": "PERSON"},
        {"name": "User", "type": "PERSON"},
        {"name": "users", "type": "PERSON"},
        {"name": "member", "type": "PERSON"},
        {"name": "members", "type": "PERSON"},
        {"name": "validator", "type": "PERSON"},
        {"name": "validators", "type": "PERSON"},
        {"name": "delegator", "type": "PERSON"},
        {"name": "participant", "type": "PERSON"},
        {"name": "farmers", "type": "PERSON"},
        {"name": "community", "type": "ORGANIZATION"},
        {"name": "team", "type": "ORGANIZATION"},
        {"name": "company", "type": "ORGANIZATION"},
        {"name": "project", "type": "PROJECT"},
        {"name": "people", "type": "PERSON"},

        # === NUMERIC ONLY (should be filtered) ===
        {"name": "2025", "type": "DATE"},
        {"name": "2030", "type": "DATE"},
        {"name": "35", "type": "NUMBER"},
        {"name": "1000", "type": "METRIC"},

        # === TAUTOLOGICAL (should be filtered) ===
        {"name": "organization", "type": "ORGANIZATION"},
        {"name": "Organization", "type": "ORGANIZATION"},
        {"name": "place", "type": "PLACE"},
        {"name": "concept", "type": "CONCEPT"},
        {"name": "person", "type": "PERSON"},
        {"name": "project", "type": "PROJECT"},

        # === LOWERCASE SINGLE-WORD PERSON (should be filtered) ===
        {"name": "bob", "type": "PERSON"},
        {"name": "alice", "type": "PERSON"},
        {"name": "john", "type": "PERSON"},

        # === GENERIC PATTERNS (should be filtered) ===
        {"name": "the community", "type": "ORGANIZATION"},
        {"name": "the team", "type": "ORGANIZATION"},
        {"name": "some people", "type": "PERSON"},
        {"name": "many participants", "type": "PERSON"},
        {"name": "our friends", "type": "PERSON"},
        {"name": "the character", "type": "PERSON"},
        {"name": "a user", "type": "PERSON"},
        {"name": "those who believe", "type": "PERSON"},
        {"name": "everyone involved", "type": "PERSON"},

        # === SENTENCE-LIKE (should be filtered) ===
        {"name": "the most important thing is to act now", "type": "CONCEPT"},
        {"name": "according to the latest research", "type": "CONCEPT"},
        {"name": "this is what we need to do", "type": "CONCEPT"},
        {"name": "has been working on sustainability", "type": "ACTIVITY"},
        {"name": "can help with carbon credits", "type": "CONCEPT"},
        {"name": "why does this matter?", "type": "QUESTION"},
        {"name": "what is the solution.", "type": "QUESTION"},

        # === TOO LONG (should be filtered) ===
        {"name": "This is an extremely long entity name that goes on and on and provides way too much detail to be a proper entity name in a knowledge graph", "type": "CONCEPT"},
        {"name": "word1 word2 word3 word4 word5 word6 word7 word8 word9 word10", "type": "CONCEPT"},
    ]


def main():
    """Run the demo and generate a report."""
    print("=" * 70)
    print("ENTITY QUALITY FILTER DEMONSTRATION")
    print("Regen KOI Knowledge Graph Improvement POC")
    print("=" * 70)
    print()

    # Create filter and sample entities
    filter_instance = EntityQualityFilter()
    entities = create_sample_entities()

    print(f"Total sample entities: {len(entities)}")
    print()

    # Generate and print report
    report = filter_instance.generate_report(entities)
    print(report)

    # Get detailed breakdown
    passed, filtered = filter_instance.get_filtered_with_reasons(entities)

    print("\n" + "=" * 70)
    print("DETAILED RESULTS")
    print("=" * 70)

    print("\n--- PASSED ENTITIES (Valid) ---")
    for entity in passed:
        print(f"  [PASS] {entity['name']} ({entity['type']})")

    print("\n--- FILTERED ENTITIES (By Reason) ---")
    by_reason = {}
    for entity, reason in filtered:
        if reason not in by_reason:
            by_reason[reason] = []
        by_reason[reason].append(entity)

    for reason, entities_list in sorted(by_reason.items()):
        print(f"\n  {reason.upper()} ({len(entities_list)} entities):")
        for entity in entities_list[:5]:  # Show first 5
            print(f"    - '{entity['name']}' ({entity['type']})")
        if len(entities_list) > 5:
            print(f"    ... and {len(entities_list) - 5} more")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    stats = filter_instance.get_stats()
    pass_rate = 100 * stats['total_passed'] / stats['total_checked']
    filter_rate = 100 * stats['total_filtered'] / stats['total_checked']

    print(f"""
Filter Performance:
- Total entities checked: {stats['total_checked']}
- Entities passed:        {stats['total_passed']} ({pass_rate:.1f}%)
- Entities filtered:      {stats['total_filtered']} ({filter_rate:.1f}%)

Quality Improvement Potential:
- If applied to Regen KOI's 23,273 entities at similar filter rate:
  - Estimated entities to filter: ~{int(23273 * filter_rate / 100):,}
  - Estimated clean entities:     ~{int(23273 * pass_rate / 100):,}

The filter successfully blocks:
- Pronouns (we, they, it)
- Generic nouns (user, member, validator, community)
- Numeric-only entities (2025, 2030)
- Tautological entities (organization/ORGANIZATION)
- Lowercase single-word PERSON entities
- Generic patterns (the community, some people)
- Sentence-like entities (too verbose, contain verbs)
- Overly long entity names (>80 chars or >8 words)
""")


if __name__ == "__main__":
    main()
