#!/usr/bin/env python3
"""
OpenAI-based SPARQL Query Builder
Uses GPT-4 to convert natural language to SPARQL queries
"""
import os
import json
from typing import Dict, List, Optional
import openai
from openai import OpenAI

class OpenAISPARQLBuilder:
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

        # Load our graph schema
        self.schema = self.load_schema()

    def load_schema(self) -> Dict:
        """Load information about our RDF schema"""
        return {
            "namespace": "https://regen.network/ontology/experimental#",
            "prefix": "regx",
            "structure": {
                "statement_type": "regx:Statement",
                "properties": {
                    "subject": "regx:subject",
                    "predicate": "regx:predicate",
                    "object": "regx:object",
                    "originalPredicate": "regx:originalPredicate"
                }
            },
            "top_predicates": [
                "provide", "is", "support", "include", "has",
                "associated_with", "published", "offer", "suggest",
                "discusse", "question", "develop", "require"
            ],
            "example_subjects": [
                "Gregory Landua", "Regen Network", "Eco-credit retirements",
                "Regenerative Treasury Flywheel", "Water claims engine"
            ]
        }

    def build_sparql(self, natural_query: str) -> str:
        """Convert natural language to SPARQL using OpenAI"""

        system_prompt = """You are a SPARQL query expert for a Regen Network knowledge graph.

The graph structure:
- All data is stored as reified statements with type regx:Statement
- Each statement has: regx:subject, regx:predicate, regx:object
- Namespace: PREFIX regx: <https://regen.network/ontology/experimental#>

Common predicates: provide, is, support, include, has, associated_with, published, offer, suggest

Example SPARQL query:
```sparql
PREFIX regx: <https://regen.network/ontology/experimental#>
SELECT ?subject ?predicate ?object WHERE {
  ?stmt a regx:Statement .
  ?stmt regx:subject ?subject .
  ?stmt regx:predicate ?predicate .
  ?stmt regx:object ?object .
  FILTER(regex(str(?subject), "Gregory", "i"))
}
LIMIT 20
```

Convert the user's natural language query to SPARQL. Focus on:
1. Use regex filters for flexible matching
2. Search in subject, predicate, or object as appropriate
3. Use DISTINCT when looking for unique entities
4. Add COUNT for aggregation queries
5. Use LIMIT to control result size

Return ONLY the SPARQL query, no explanation."""

        user_prompt = f"""Convert this to SPARQL: "{natural_query}"

Top predicates in the graph: {', '.join(self.schema['top_predicates'][:10])}
Example entities: {', '.join(self.schema['example_subjects'])}"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )

            sparql = response.choices[0].message.content

            # Clean up the response
            sparql = sparql.strip()
            if sparql.startswith("```sparql"):
                sparql = sparql[9:]
            if sparql.startswith("```"):
                sparql = sparql[3:]
            if sparql.endswith("```"):
                sparql = sparql[:-3]

            return sparql.strip()

        except Exception as e:
            print(f"OpenAI error: {e}")
            # Fallback to simple keyword-based query
            return self.fallback_sparql(natural_query)

    def fallback_sparql(self, query: str) -> str:
        """Simple fallback SPARQL generation"""
        keywords = [w.lower() for w in query.split() if len(w) > 3]

        filter_conditions = " || ".join([
            f'regex(str(?subject), "{kw}", "i") || regex(str(?object), "{kw}", "i")'
            for kw in keywords[:3]
        ])

        return f"""PREFIX regx: <https://regen.network/ontology/experimental#>
SELECT DISTINCT ?subject ?predicate ?object WHERE {{
  ?stmt a regx:Statement .
  ?stmt regx:subject ?subject .
  ?stmt regx:predicate ?predicate .
  ?stmt regx:object ?object .
  FILTER({filter_conditions})
}}
LIMIT 20"""

# Example usage
if __name__ == "__main__":
    # Test queries
    test_queries = [
        "What is Gregory Landua working on?",
        "Show me all carbon credit initiatives",
        "How many water-related projects are there?",
        "What does Regen Network support?",
        "List all treasury mechanisms"
    ]

    # Check if API key exists
    if not os.getenv('OPENAI_API_KEY'):
        print("⚠️  No OpenAI API key found. Using fallback query builder.")
        print("Set OPENAI_API_KEY environment variable for better results.")

        builder = OpenAISPARQLBuilder()
        for q in test_queries[:2]:
            print(f"\nQuery: {q}")
            print("Fallback SPARQL:")
            print(builder.fallback_sparql(q))
    else:
        builder = OpenAISPARQLBuilder()

        for q in test_queries:
            print(f"\nQuery: {q}")
            print("Generated SPARQL:")
            sparql = builder.build_sparql(q)
            print(sparql)