#!/usr/bin/env python3
"""
Natural Language to SPARQL Generator using Hybrid Approach
Combines semantic search on predicate embeddings with structured schema
"""

import json
import numpy as np
import pickle
import openai
import os
from typing import Dict, List, Tuple, Optional
from sklearn.metrics.pairwise import cosine_similarity

class NaturalLanguageToSPARQL:
    """
    Converts natural language queries to SPARQL using:
    1. Semantic search on predicate embeddings
    2. Focused schema construction
    3. LLM-based query generation
    """

    def __init__(self, embeddings_file="predicate_embeddings.pkl",
                 patterns_file="predicate_patterns.json",
                 consolidation_file="final_consolidation_all_t0.30.json"):
        """Initialize with pre-computed embeddings and mappings"""

        # Load predicate embeddings
        print("Loading predicate embeddings...")
        with open(embeddings_file, 'rb') as f:
            self.embeddings = pickle.load(f)

        # Load predicate patterns
        print("Loading predicate patterns...")
        with open(patterns_file, 'r') as f:
            self.patterns = json.load(f)

        # Load consolidation mapping if available
        self.consolidation = None
        if os.path.exists(consolidation_file):
            print("Loading consolidation mapping...")
            with open(consolidation_file, 'r') as f:
                data = json.load(f)
                self.consolidation = data.get('mapping', {})
                self.consolidation_info = data.get('consolidation_info', {})

        # Initialize OpenAI client
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Entity types from Schema.org
        self.entity_types = {
            "person": "schema:Person",
            "people": "schema:Person",
            "organization": "schema:Organization",
            "organizations": "schema:Organization",
            "company": "schema:Organization",
            "companies": "schema:Organization",
            "project": "schema:Project",
            "projects": "schema:Project"
        }

    def embed_text(self, text: str) -> np.ndarray:
        """Get embedding for a text string"""
        response = self.client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return np.array(response.data[0].embedding)

    def find_similar_predicates(self, query: str, top_k: int = 20) -> List[Dict]:
        """Find predicates similar to the query using semantic search"""

        # Embed the query
        query_embedding = self.embed_text(query)

        # Calculate similarities
        similarities = cosine_similarity([query_embedding], self.embeddings)[0]

        # Get top-k indices
        top_indices = similarities.argsort()[-top_k:][::-1]

        # Build results with metadata
        results = []
        for idx in top_indices:
            if idx < len(self.patterns):
                pattern = self.patterns[idx]
                result = {
                    "predicate": pattern["predicate"],
                    "similarity": float(similarities[idx]),
                    "usage_count": pattern.get("count", 0),
                    "subject_types": pattern.get("subject_types", {}),
                    "object_types": pattern.get("object_types", {})
                }

                # Add consolidation info if available
                if self.consolidation and pattern["predicate"] in self.consolidation:
                    consolidated = self.consolidation[pattern["predicate"]]
                    result["consolidated_as"] = consolidated
                    if consolidated in self.consolidation_info:
                        result["cluster_members"] = self.consolidation_info[consolidated].get("members", [])

                results.append(result)

        return results

    def extract_entities(self, query: str) -> Dict[str, str]:
        """Extract entity types mentioned in the query"""
        entities = {}
        query_lower = query.lower()

        for term, entity_type in self.entity_types.items():
            if term in query_lower:
                entities[term] = entity_type

        return entities

    def build_schema_context(self, query: str, predicates: List[Dict]) -> str:
        """Build focused schema context for LLM"""

        entities = self.extract_entities(query)

        context_parts = [
            "# Knowledge Graph Schema\n",
            "## Entity Types:",
            "- schema:Person (people, individuals)",
            "- schema:Organization (companies, organizations)",
            "- schema:Project (projects, initiatives)\n",
            "## Relevant Predicates:"
        ]

        # Group predicates by subject-object patterns
        patterns = {}
        for pred in predicates[:10]:  # Use top 10 most relevant
            subj_types = list(pred.get("subject_types", {}).keys())
            obj_types = list(pred.get("object_types", {}).keys())

            if subj_types and obj_types:
                pattern_key = f"{subj_types[0]} -> {obj_types[0]}"
                if pattern_key not in patterns:
                    patterns[pattern_key] = []
                patterns[pattern_key].append(pred["predicate"])

        for pattern, preds in patterns.items():
            context_parts.append(f"\n### {pattern}:")
            for p in preds[:5]:  # Limit to 5 per pattern
                context_parts.append(f"  - {p}")

        # Add example SPARQL structure
        context_parts.extend([
            "\n## SPARQL Query Structure:",
            "```sparql",
            "PREFIX regx: <https://regen.network/ontology/experimental#>",
            "PREFIX schema: <http://schema.org/>",
            "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
            "",
            "SELECT ?subject ?predicate ?object WHERE {",
            "  ?stmt a regx:Statement .",
            "  ?stmt regx:subject ?subject .",
            "  ?stmt regx:predicate ?predicate .",
            "  ?stmt regx:object ?object .",
            "  # Add filters here",
            "}",
            "```"
        ])

        return "\n".join(context_parts)

    def generate_sparql(self, query: str, temperature: float = 0.3) -> Dict:
        """Generate SPARQL query from natural language"""

        # Find relevant predicates
        print(f"Finding relevant predicates for: {query}")
        relevant_predicates = self.find_similar_predicates(query)

        # Build focused schema
        schema_context = self.build_schema_context(query, relevant_predicates)

        # Create prompt for LLM
        prompt = f"""Convert this natural language query to SPARQL.

Natural Language Query: {query}

{schema_context}

Instructions:
1. Use the exact predicate names from the schema above
2. Return only the SPARQL query, no explanation
3. Use the regx:Statement pattern shown in the example
4. Include appropriate filters for the query

SPARQL Query:"""

        # Generate with LLM
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SPARQL query expert. Generate valid SPARQL queries based on the provided schema."},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature
        )

        sparql = response.choices[0].message.content.strip()

        # Clean up the response
        if "```sparql" in sparql:
            sparql = sparql.split("```sparql")[1].split("```")[0].strip()
        elif "```" in sparql:
            sparql = sparql.split("```")[1].split("```")[0].strip()

        return {
            "query": query,
            "sparql": sparql,
            "relevant_predicates": [p["predicate"] for p in relevant_predicates[:5]],
            "confidence": self.calculate_confidence(relevant_predicates)
        }

    def calculate_confidence(self, predicates: List[Dict]) -> float:
        """Calculate confidence score based on predicate similarities"""
        if not predicates:
            return 0.0

        # Use top-3 similarities
        top_sims = [p["similarity"] for p in predicates[:3]]
        return float(np.mean(top_sims))

    def expand_with_consolidation(self, sparql: str) -> str:
        """Expand SPARQL to include all predicate variants from consolidation"""
        if not self.consolidation_info:
            return sparql

        # Find predicates in the SPARQL
        import re
        predicate_pattern = r'regx:predicate\s+"([^"]+)"'
        predicates = re.findall(predicate_pattern, sparql)

        for pred in predicates:
            # Check if this is a consolidated predicate
            for consolidated, info in self.consolidation_info.items():
                if consolidated == pred or pred in info.get("members", []):
                    # Build FILTER with all variants
                    members = info["members"]
                    if len(members) > 1:
                        filter_parts = [f'?predicate = "{m}"' for m in members]
                        filter_clause = f"FILTER({' || '.join(filter_parts)})"

                        # Replace single predicate with filter
                        old = f'?stmt regx:predicate "{pred}"'
                        new = f'?stmt regx:predicate ?predicate .\n  {filter_clause}'
                        sparql = sparql.replace(old, new)
                    break

        return sparql


def test_nl_to_sparql():
    """Test the natural language to SPARQL conversion"""

    # Initialize the converter
    nl2sparql = NaturalLanguageToSPARQL()

    # Test queries
    test_queries = [
        "Who works for Regen Network?",
        "What projects are developed by Microsoft?",
        "Find all organizations that partner with Google",
        "Show me people who created climate projects",
        "Which organizations fund regenerative agriculture?",
        "List all employees of OpenAI"
    ]

    print("\n" + "="*80)
    print("NATURAL LANGUAGE TO SPARQL TESTS")
    print("="*80)

    for query in test_queries:
        print(f"\n--- Query: {query} ---")

        result = nl2sparql.generate_sparql(query)

        print(f"Confidence: {result['confidence']:.3f}")
        print(f"Relevant predicates: {', '.join(result['relevant_predicates'])}")
        print(f"\nGenerated SPARQL:\n{result['sparql']}")

        # Show expanded version if consolidation is used
        expanded = nl2sparql.expand_with_consolidation(result['sparql'])
        if expanded != result['sparql']:
            print(f"\nExpanded SPARQL (with all predicate variants):\n{expanded}")


if __name__ == "__main__":
    test_nl_to_sparql()
