#!/usr/bin/env python3
"""
Refine Knowledge Graph with:
1. Predicate Consolidation
2. Deduplication
3. CAT Receipt Generation for Transformations

Aligns with HYBRID_RAG_KNOWLEDGE_GRAPH_ARCHITECTURE.md
"""

import json
import requests
import hashlib
import pickle
from datetime import datetime
from typing import Dict, List, Tuple, Set
from collections import defaultdict, Counter
import os
import uuid
import re

# Configuration
JENA_ENDPOINT = "http://localhost:3030/koi/sparql"
JENA_UPDATE_ENDPOINT = "http://localhost:3030/koi/update"

class GraphRefiner:
    """Refines knowledge graph with consolidation and deduplication"""

    def __init__(self, consolidation_file="final_consolidation_all_t0.30.json"):
        """Initialize with consolidation mapping"""

        # Load consolidation mapping (allow CONSOLIDATION_PATH override)
        print("Loading consolidation mapping...")
        cons_path = os.getenv("CONSOLIDATION_PATH", consolidation_file)
        with open(cons_path, 'r') as f:
            data = json.load(f)
            self.consolidation_mapping = data.get('mapping') or data.get('predicate_mapping')
            self.consolidation_info = data.get('consolidation_info') or data.get('cluster_info')
            self.consolidation_stats = data.get('statistics', {})

        print(f"  - Loaded mapping for {len(self.consolidation_mapping)} predicates from {cons_path}")
        if 'consolidated_count' in self.consolidation_stats:
            print(f"  - Consolidating to {self.consolidation_stats['consolidated_count']} predicates")

        # Initialize transformation tracking
        self.transformations = []
        self.cat_receipts = []

        # Optional canonical predicate mapping (for dashboards/rollups)
        self.canonical_map = {}
        canonical_path_env = os.getenv("CANONICAL_PATH", "")
        for candidate in [
            canonical_path_env,
            "canonical_predicates.json",
            "canonical_predicate_mapping.json"
        ]:
            if not candidate:
                continue
            if os.path.exists(candidate):
                try:
                    with open(candidate, 'r') as f:
                        data = json.load(f)
                        # Support both {"mapping": {...}} and flat {pred: canonical}
                        self.canonical_map = data.get('mapping', data)
                        print(f"  - Loaded canonical mapping from {candidate} ({len(self.canonical_map)} entries)")
                        break
                except Exception as e:
                    print(f"  - Failed to load canonical mapping from {candidate}: {e}")

        # Optional published time enrichment mapping
        # Expect JSON mapping of one of the forms:
        # {"subject|predicate|object": "2025-10-01T12:00:00Z"} or {"subject|object": "..."}
        # or {"subject": "..."} / {"object": "..."}
        self.published_map = {}
        pub_map_path = os.getenv("PUBLISHED_MAP_PATH", "")
        if pub_map_path and os.path.exists(pub_map_path):
            try:
                with open(pub_map_path, 'r') as f:
                    self.published_map = json.load(f)
                    print(f"  - Loaded published_at mapping from {pub_map_path} ({len(self.published_map)} entries)")
            except Exception as e:
                print(f"  - Failed to load published_at mapping from {pub_map_path}: {e}")

    # Helpers for published_at substring matching
    def _normalize_for_substring(self, text: str) -> str:
        t = text.lower()
        t = re.sub(r"[^a-z0-9]+", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    def _first_prefix(self, norm: str, k: int = 2) -> str:
        return norm[:k]

    def _build_published_index(self):
        try:
            self._published_norm_map = {}
            self._published_index = {}
            for key, ts in getattr(self, 'published_map', {}).items():
                norm = self._normalize_for_substring(str(key))
                if len(norm) < 8:
                    continue
                self._published_norm_map[norm] = ts
                pref = self._first_prefix(norm)
                self._published_index.setdefault(pref, []).append(norm)
            for pref in list(self._published_index.keys()):
                self._published_index[pref] = sorted(self._published_index[pref], key=len, reverse=True)[:500]
        except Exception:
            self._published_norm_map = {}
            self._published_index = {}

    def _ensure_published_index(self):
        if not hasattr(self, '_published_index') or self._published_index is None:
            self._build_published_index()

    def _lookup_published(self, stmt: Dict) -> str:
        """Lookup published ISO timestamp for a statement, if available."""
        if not self.published_map:
            return ""
        s = stmt.get("subject", "")
        p = stmt.get("predicate", "")
        o = stmt.get("object", "")
        for key in (
            f"{s}|{p}|{o}",
            f"{s}|{o}",
            s,
            o,
        ):
            if key in self.published_map:
                return self.published_map[key]
        # Substring fallback with lightweight index
        self._ensure_published_index()
        try:
            if getattr(self, '_published_index', None):
                s_norm = self._normalize_for_substring(s)
                o_norm = self._normalize_for_substring(o)
                tokens = set([t for t in (s_norm + " " + o_norm).split() if len(t) >= 4])
                seen = set()
                for t in tokens:
                    pref = self._first_prefix(t)
                    if pref in seen:
                        continue
                    seen.add(pref)
                    for cand in self._published_index.get(pref, [])[:200]:
                        if cand in s_norm or cand in o_norm:
                            return self._published_norm_map.get(cand, "")
        except Exception:
            pass
        return ""

    def _canonical_for(self, consolidated_predicate: str) -> str:
        """Return canonical category for a predicate using mapping or heuristics."""
        if consolidated_predicate in self.canonical_map:
            return self.canonical_map[consolidated_predicate]

        p = consolidated_predicate.lower()
        if any(w in p for w in ["create", "develop", "build", "author", "invent", "design"]):
            return "creation"
        if any(w in p for w in ["fund", "finance", "sponsor", "invest", "donor"]):
            return "funding"
        if any(w in p for w in ["partner", "collaborat", "work_with", "work with", "joint"]):
            return "collaboration"
        if any(w in p for w in ["lead", "direct", "head", "manage", "oversee"]):
            return "leadership"
        if any(w in p for w in ["govern", "vote", "proposal", "validator"]):
            return "governance"
        if any(w in p for w in ["water", "hydro", "river", "benefit_unit"]):
            return "water"
        if any(w in p for w in ["eco-credit", "ecocredit", "retire", "credit"]):
            return "eco_credit"
        if any(w in p for w in ["token", "treasury", "flywheel", "usd", "usdc", "usdt", "stable"]):
            return "finance"
        return "general"

    def execute_sparql(self, query: str) -> dict:
        """Execute a SPARQL query"""
        response = requests.post(
            JENA_ENDPOINT,
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"}
        )
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code} - {response.text}")
            return {"results": {"bindings": []}}

    def execute_update(self, update: str) -> bool:
        """Execute a SPARQL UPDATE query"""
        response = requests.post(
            JENA_UPDATE_ENDPOINT,
            data={"update": update},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        return response.status_code == 204

    def fetch_all_statements(self, limit: int = None) -> List[Dict]:
        """Fetch all statements from the graph"""
        print("\n=== Fetching Statements ===")

        query = f"""
        PREFIX regx: <https://regen.network/ontology/experimental#>

        SELECT ?stmt ?subject ?predicate ?object WHERE {{
            ?stmt a regx:Statement .
            ?stmt regx:subject ?subject .
            ?stmt regx:predicate ?predicate .
            ?stmt regx:object ?object .
        }}
        {f'LIMIT {limit}' if limit else ''}
        """

        results = self.execute_sparql(query)
        statements = []

        for binding in results["results"]["bindings"]:
            statements.append({
                "stmt_uri": binding["stmt"]["value"],
                "subject": binding["subject"]["value"],
                "predicate": binding["predicate"]["value"],
                "object": binding["object"]["value"]
            })

        print(f"  - Fetched {len(statements)} statements")
        return statements

    def identify_duplicates(self, statements: List[Dict]) -> Dict[str, List[Dict]]:
        """Identify duplicate statements based on content hash"""
        print("\n=== Identifying Duplicates ===")

        # Group by content hash
        hash_groups = defaultdict(list)

        for stmt in statements:
            # Create canonical hash of triple content
            content = f"{stmt['subject']}|{stmt['predicate']}|{stmt['object']}"
            content_hash = hashlib.md5(content.encode()).hexdigest()
            hash_groups[content_hash].append(stmt)

        # Filter to only duplicates
        duplicates = {h: stmts for h, stmts in hash_groups.items() if len(stmts) > 1}

        total_duplicates = sum(len(stmts) - 1 for stmts in duplicates.values())
        print(f"  - Found {len(duplicates)} duplicate groups")
        print(f"  - Total duplicate statements to remove: {total_duplicates}")

        return duplicates

    def consolidate_predicates(self, statements: List[Dict]) -> List[Dict]:
        """Apply predicate consolidation mapping"""
        print("\n=== Consolidating Predicates ===")

        # Track consolidation statistics
        consolidation_counts = Counter()
        unchanged_count = 0

        # Create new statements with consolidated predicates
        consolidated_statements = []

        for stmt in statements:
            original_pred = stmt["predicate"]

            # Check if predicate should be consolidated
            if original_pred in self.consolidation_mapping:
                consolidated_pred = self.consolidation_mapping[original_pred]
                consolidation_counts[f"{original_pred} → {consolidated_pred}"] += 1
            else:
                consolidated_pred = original_pred
                unchanged_count += 1

            # Create new statement
            new_stmt = {
                **stmt,
                "predicate": consolidated_pred,
                "original_predicate": original_pred
            }
            consolidated_statements.append(new_stmt)

        # Report statistics
        print(f"  - Consolidated {len(consolidation_counts)} predicate types")
        print(f"  - Unchanged predicates: {unchanged_count}")
        print(f"\n  Top consolidations:")
        for mapping, count in consolidation_counts.most_common(10):
            print(f"    {count:5}x {mapping}")

        return consolidated_statements

    def deduplicate_statements(self, statements: List[Dict]) -> List[Dict]:
        """Remove duplicate statements with normalization"""
        print("\n=== Deduplicating Statements (with normalization) ===")

        seen = set()
        unique_statements = []
        duplicates_removed = 0

        def normalize(text: str) -> str:
            """Normalize text for deduplication"""
            # Trim whitespace
            text = text.strip()
            # Collapse multiple spaces
            import re
            text = re.sub(r'\s+', ' ', text)
            # Normalize quotes
            text = text.replace('"', '"').replace('"', '"').replace("'", "'").replace("'", "'")
            # Lowercase for comparison (but keep original)
            return text.lower()

        for stmt in statements:
            # Create normalized canonical representation
            canonical = (
                normalize(stmt["subject"]),
                normalize(stmt["predicate"]),
                normalize(stmt["object"])
            )

            if canonical not in seen:
                seen.add(canonical)
                unique_statements.append(stmt)
            else:
                duplicates_removed += 1

        print(f"  - Removed {duplicates_removed} duplicate statements")
        print(f"  - Unique statements remaining: {len(unique_statements)}")

        return unique_statements

    def create_cat_receipt(self, transformation_type: str, input_data: Dict,
                          output_data: Dict, metadata: Dict = None) -> Dict:
        """
        Create a Content Addressable Transformation (CAT) receipt
        Following the architecture from HYBRID_RAG_KNOWLEDGE_GRAPH_ARCHITECTURE.md
        """

        # Generate unique receipt ID
        receipt_id = f"cat:{transformation_type}:{uuid.uuid4()}"

        # Calculate content hashes
        input_hash = hashlib.sha256(
            json.dumps(input_data, sort_keys=True).encode()
        ).hexdigest()

        output_hash = hashlib.sha256(
            json.dumps(output_data, sort_keys=True).encode()
        ).hexdigest()

        # Create receipt
        receipt = {
            "id": receipt_id,
            "type": transformation_type,
            "timestamp": datetime.now().isoformat(),
            "input_hash": input_hash,
            "output_hash": output_hash,
            "input_summary": {
                "count": input_data.get("count", 0),
                "predicates": input_data.get("predicates", 0)
            },
            "output_summary": {
                "count": output_data.get("count", 0),
                "predicates": output_data.get("predicates", 0)
            },
            "metadata": metadata or {},
            "provenance": {
                "tool": "koi-processor/graph-refiner",
                "version": "1.0.0",
                "consolidation_mapping": self.consolidation_stats
            }
        }

        self.cat_receipts.append(receipt)
        return receipt

    def create_refined_graph(self, statements: List[Dict],
                           graph_name: str = "koi-refined") -> bool:
        """Create new refined graph in Apache Jena"""
        print(f"\n=== Creating Refined Graph: {graph_name} ===")

        # Create new dataset
        # Note: This is a simplified version - in production you'd use Jena's
        # dataset management API properly

        # Generate RDF for refined statements
        rdf_triples = []

        for i, stmt in enumerate(statements):
            stmt_uri = f"<https://regen.network/statements/refined/{i}>"

            # Escape special characters in literals
            subject = stmt["subject"].replace('"', '\\"').replace('\n', '\\n')
            predicate = stmt["predicate"].replace('"', '\\"').replace('\n', '\\n')
            obj = stmt["object"].replace('"', '\\"').replace('\n', '\\n')

            rdf_triples.extend([
                f'{stmt_uri} a <https://regen.network/ontology/experimental#Statement> .',
                f'{stmt_uri} <https://regen.network/ontology/experimental#subject> "{subject}" .',
                f'{stmt_uri} <https://regen.network/ontology/experimental#predicate> "{predicate}" .',
                f'{stmt_uri} <https://regen.network/ontology/experimental#object> "{obj}" .'
            ])

            # Add provenance if original predicate was different
            if "original_predicate" in stmt and stmt["original_predicate"] != stmt["predicate"]:
                orig_pred = stmt["original_predicate"].replace('"', '\\"')
                rdf_triples.append(
                    f'{stmt_uri} <https://regen.network/ontology/experimental#originalPredicate> "{orig_pred}" .'
                )

            # Add canonical category (optional; used for dashboards/rollups)
            try:
                canonical = self._canonical_for(stmt["predicate"])
                if canonical:
                    rdf_triples.append(
                        f'{stmt_uri} <https://regen.network/ontology/experimental#canonicalPredicate> "{canonical}" .'
                    )
            except Exception:
                pass

            # Optional: write publishedAt if enrichment mapping provided
            try:
                pub = self._lookup_published(stmt)
                if pub:
                    # Ensure ISO 8601 with Z and typed literal
                    iso = pub if pub.endswith('Z') else (pub + 'Z' if 'T' in pub else pub + 'T00:00:00Z')
                    rdf_triples.append(
                        f'{stmt_uri} <https://regen.network/ontology/experimental#publishedAt> "{iso}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .'
                    )
            except Exception:
                pass

        print(f"  - Generated {len(rdf_triples)} RDF triples")
        print(f"  - From {len(statements)} refined statements")

        # Save to file for backup
        output_file = f"refined_graph_{datetime.now().strftime('%Y%m%d_%H%M%S')}.ttl"
        with open(output_file, 'w') as f:
            f.write("@prefix regx: <https://regen.network/ontology/experimental#> .\n")
            f.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n")
            f.write("\n".join(rdf_triples))

        print(f"  - Saved refined graph to: {output_file}")

        return True

    def generate_statistics(self, original: List[Dict], refined: List[Dict]) -> Dict:
        """Generate statistics about the refinement process"""

        # Count unique predicates
        original_predicates = set(s["predicate"] for s in original)
        refined_predicates = set(s["predicate"] for s in refined)

        stats = {
            "original": {
                "total_statements": len(original),
                "unique_predicates": len(original_predicates),
                "predicate_list": sorted(original_predicates)[:20]  # Top 20
            },
            "refined": {
                "total_statements": len(refined),
                "unique_predicates": len(refined_predicates),
                "predicate_list": sorted(refined_predicates)[:20]  # Top 20
            },
            "reduction": {
                "statements": len(original) - len(refined),
                "statements_pct": (1 - len(refined) / len(original)) * 100,
                "predicates": len(original_predicates) - len(refined_predicates),
                "predicates_pct": (1 - len(refined_predicates) / len(original_predicates)) * 100
            }
        }

        return stats

    def refine_graph(self, limit: int = None) -> Dict:
        """Main refinement process"""
        print("\n" + "="*80)
        print("KNOWLEDGE GRAPH REFINEMENT PROCESS")
        print("="*80)

        # Step 1: Fetch original statements
        original_statements = self.fetch_all_statements(limit)

        # Track original statistics
        original_predicates = set(s["predicate"] for s in original_statements)

        # Step 2: Consolidate predicates
        consolidated_statements = self.consolidate_predicates(original_statements)

        # Step 3: Deduplicate
        refined_statements = self.deduplicate_statements(consolidated_statements)

        # Step 4: Generate statistics
        stats = self.generate_statistics(original_statements, refined_statements)

        # Step 5: Create CAT receipt for the transformation
        cat_receipt = self.create_cat_receipt(
            transformation_type="graph_refinement",
            input_data={
                "count": len(original_statements),
                "predicates": len(original_predicates)
            },
            output_data={
                "count": len(refined_statements),
                "predicates": len(set(s["predicate"] for s in refined_statements))
            },
            metadata={
                "consolidation_threshold": 0.30,
                "deduplication_method": "content_hash",
                "timestamp": datetime.now().isoformat(),
                "canonical_predicates": bool(self.canonical_map)
            }
        )

        # Step 6: Create refined graph
        self.create_refined_graph(refined_statements)

        # Step 7: Save CAT receipts
        receipts_file = f"cat_receipts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(receipts_file, 'w') as f:
            json.dump(self.cat_receipts, f, indent=2)

        print(f"\n=== Refinement Complete ===")
        print(f"  Original statements: {stats['original']['total_statements']:,}")
        print(f"  Refined statements: {stats['refined']['total_statements']:,}")
        print(f"  Reduction: {stats['reduction']['statements']:,} ({stats['reduction']['statements_pct']:.1f}%)")
        print(f"  ")
        print(f"  Original predicates: {stats['original']['unique_predicates']:,}")
        print(f"  Refined predicates: {stats['refined']['unique_predicates']:,}")
        print(f"  Reduction: {stats['reduction']['predicates']:,} ({stats['reduction']['predicates_pct']:.1f}%)")
        print(f"  ")
        print(f"  CAT receipts saved to: {receipts_file}")

        return {
            "stats": stats,
            "cat_receipt": cat_receipt,
            "refined_statements": refined_statements[:10]  # Sample
        }


def main():
    """Main execution"""

    # Resolve consolidation path
    cons_path = os.getenv("CONSOLIDATION_PATH", "final_consolidation_all_t0.30.json")
    if not os.path.exists(cons_path):
        print(f"ERROR: Consolidation file not found at {cons_path}!")
        print("Run consolidate_predicates_final.py --apply (use --full -t 0.25 for t=0.25 all) and set CONSOLIDATION_PATH accordingly.")
        return

    # Create refiner
    refiner = GraphRefiner(consolidation_file=cons_path)

    # Run refinement (use limit for testing)
    # Remove limit=None to process entire graph
    results = refiner.refine_graph(limit=None)

    # Save results
    with open("refinement_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("\n✅ Graph refinement complete!")
    print("📊 Results saved to: refinement_results.json")


if __name__ == "__main__":
    main()
