#!/usr/bin/env python3
"""
Enhanced Natural Language to SPARQL with Practical Improvements
Adds intent detection, validation, and caching without overengineering
"""

import json
import numpy as np
import pickle
import openai
import os
import re
from typing import Dict, List, Tuple, Optional
from sklearn.metrics.pairwise import cosine_similarity
from functools import lru_cache
from datetime import datetime
import hashlib

class EnhancedNLtoSPARQL:
    """
    Improved NL to SPARQL with:
    - Intent detection for better routing
    - Query validation
    - Result caching
    - Example-based generation
    """

    def __init__(self, embeddings_file="predicate_embeddings.pkl",
                 patterns_file="predicate_patterns.json",
                 consolidation_file="final_consolidation_all_t0.30.json"):

        # Load resources (same as before)
        with open(embeddings_file, 'rb') as f:
            self.embeddings = pickle.load(f)
        with open(patterns_file, 'r') as f:
            self.patterns = json.load(f)

        # Load consolidation if available
        if os.path.exists(consolidation_file):
            with open(consolidation_file, 'r') as f:
                data = json.load(f)
                self.consolidation = data.get('mapping', {})
                self.consolidation_info = data.get('consolidation_info', {})

        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        # Query cache
        self.query_cache = {}

        # Intent-based predicate clusters (precomputed)
        self.intent_clusters = {
            "employment": ["works for", "employed by", "employee of", "works at",
                          "job at", "position at", "hired by"],
            "creation": ["created", "developed", "built", "made", "produced",
                        "designed", "invented", "established"],
            "collaboration": ["partners with", "collaborates with", "works with",
                            "cooperates with", "joint venture with"],
            "ownership": ["owns", "has", "possesses", "controls", "manages"],
            "funding": ["funds", "finances", "sponsors", "invests in", "backs"],
            "leadership": ["leads", "directs", "heads", "manages", "oversees"],
            "membership": ["member of", "belongs to", "part of", "affiliated with"],
            "location": ["located in", "based in", "headquarters in", "operates in"]
        }

        # Example queries for better generation
        self.example_queries = {
            "employment": {
                "nl": "Who works for OpenAI?",
                "sparql": """SELECT ?person ?name WHERE {
                    ?stmt regx:subject ?person .
                    ?stmt regx:predicate "works for" .
                    ?stmt regx:object ?org .
                    ?org rdfs:label "OpenAI" .
                    ?person a schema:Person .
                    ?person rdfs:label ?name .
                }"""
            },
            "creation": {
                "nl": "What projects did Microsoft create?",
                "sparql": """SELECT ?project ?title WHERE {
                    ?stmt regx:subject ?org .
                    ?stmt regx:predicate "created" .
                    ?stmt regx:object ?project .
                    ?org rdfs:label "Microsoft" .
                    ?project a schema:Project .
                    ?project rdfs:label ?title .
                }"""
            },
            "aggregation": {
                "nl": "How many people work for Google?",
                "sparql": """SELECT (COUNT(DISTINCT ?person) as ?count) WHERE {
                    ?stmt regx:subject ?person .
                    ?stmt regx:predicate "works for" .
                    ?stmt regx:object ?org .
                    ?org rdfs:label "Google" .
                    ?person a schema:Person .
                }"""
            }
        }

    def detect_intent(self, query: str) -> Tuple[str, List[str]]:
        """
        Detect query intent and extract key entities
        Returns: (intent_type, extracted_entities)
        """
        query_lower = query.lower()

        # Detect intent
        intent = "general"
        if any(word in query_lower for word in ["who", "which person", "which people"]):
            intent = "person_search"
        elif any(word in query_lower for word in ["works", "employed", "employee"]):
            intent = "employment"
        elif any(word in query_lower for word in ["created", "developed", "built", "made"]):
            intent = "creation"
        elif any(word in query_lower for word in ["partners", "collaborates"]):
            intent = "collaboration"
        elif any(word in query_lower for word in ["funds", "finances", "sponsors"]):
            intent = "funding"
        elif any(word in query_lower for word in ["how many", "count", "number of", "total"]):
            intent = "aggregation"
        elif any(word in query_lower for word in ["owns", "has", "possesses"]):
            intent = "ownership"

        # Extract potential entities (simple NER)
        # Look for capitalized words that might be entity names
        entities = []
        words = query.split()
        for i, word in enumerate(words):
            # Check for capitalized words (potential entities)
            if word[0].isupper() and word.lower() not in ["who", "what", "where", "when", "how"]:
                # Collect multi-word entities
                entity = word
                j = i + 1
                while j < len(words) and words[j][0].isupper():
                    entity += " " + words[j]
                    j += 1
                entities.append(entity)

        return intent, entities

    def get_intent_predicates(self, intent: str, top_k: int = 10) -> List[str]:
        """Get relevant predicates for the detected intent"""
        if intent in self.intent_clusters:
            # Return predefined cluster for known intents
            return self.intent_clusters[intent]
        else:
            # Fallback to general semantic search
            return []

    @lru_cache(maxsize=100)
    def embed_text_cached(self, text: str) -> np.ndarray:
        """Cached version of text embedding"""
        response = self.client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return np.array(response.data[0].embedding)

    def find_similar_predicates_enhanced(self, query: str, intent: str, top_k: int = 15) -> List[Dict]:
        """Enhanced predicate finding using intent"""

        # Get intent-specific predicates
        intent_predicates = self.get_intent_predicates(intent)

        # Also do semantic search
        query_embedding = self.embed_text_cached(query)
        similarities = cosine_similarity([query_embedding], self.embeddings)[0]
        top_indices = similarities.argsort()[-top_k:][::-1]

        # Combine intent-based and semantic results
        results = []
        seen = set()

        # Add intent predicates first (higher priority)
        for pred in intent_predicates:
            # Find this predicate in our patterns
            for pattern in self.patterns:
                if pattern["predicate"] == pred and pred not in seen:
                    results.append({
                        "predicate": pred,
                        "similarity": 1.0,  # High confidence for intent match
                        "source": "intent",
                        "usage_count": pattern.get("count", 0)
                    })
                    seen.add(pred)
                    break

        # Add semantic search results
        for idx in top_indices:
            if idx < len(self.patterns):
                pattern = self.patterns[idx]
                pred = pattern["predicate"]
                if pred not in seen:
                    results.append({
                        "predicate": pred,
                        "similarity": float(similarities[idx]),
                        "source": "semantic",
                        "usage_count": pattern.get("count", 0)
                    })
                    seen.add(pred)

        return results[:top_k]

    def validate_sparql(self, sparql: str) -> Tuple[bool, List[str]]:
        """
        Validate SPARQL query structure
        Returns: (is_valid, error_messages)
        """
        errors = []

        # Check basic structure
        if not sparql.strip():
            errors.append("Empty query")
            return False, errors

        # Check for required components
        required_keywords = ["SELECT", "WHERE"]
        for keyword in required_keywords:
            if keyword not in sparql.upper():
                errors.append(f"Missing {keyword} clause")

        # Check for balanced brackets
        if sparql.count("{") != sparql.count("}"):
            errors.append("Unbalanced brackets")

        # Check for at least one triple pattern
        if "regx:Statement" not in sparql and "regx:predicate" not in sparql:
            errors.append("No triple patterns found")

        # Check variable consistency
        variables = re.findall(r'\?(\w+)', sparql)
        select_vars = re.findall(r'SELECT.*?\?(\w+)', sparql, re.IGNORECASE)
        for var in select_vars:
            if variables.count(var) < 2:  # Should appear in SELECT and WHERE
                errors.append(f"Variable ?{var} not bound in WHERE clause")

        return len(errors) == 0, errors

    def generate_sparql_enhanced(self, query: str) -> Dict:
        """Enhanced SPARQL generation with intent and validation"""

        # Check cache first
        query_hash = hashlib.md5(query.encode()).hexdigest()
        if query_hash in self.query_cache:
            cached = self.query_cache[query_hash]
            cached["from_cache"] = True
            return cached

        # Detect intent and entities
        intent, entities = self.detect_intent(query)
        print(f"Intent: {intent}, Entities: {entities}")

        # Find relevant predicates with intent boost
        predicates = self.find_similar_predicates_enhanced(query, intent)

        # Build context with relevant example
        context_parts = ["# Knowledge Graph Schema\n"]

        # Add relevant example if available
        if intent in self.example_queries:
            example = self.example_queries[intent]
            context_parts.append(f"## Example Similar Query:")
            context_parts.append(f"NL: {example['nl']}")
            context_parts.append(f"SPARQL:\n{example['sparql']}\n")

        # Add predicates
        context_parts.append("## Relevant Predicates:")
        for pred in predicates[:10]:
            source = pred.get("source", "semantic")
            context_parts.append(f"- {pred['predicate']} (match: {source}, used: {pred['usage_count']} times)")

        # Add entities if found
        if entities:
            context_parts.append(f"\n## Detected Entities:")
            for entity in entities:
                context_parts.append(f"- {entity}")

        context = "\n".join(context_parts)

        # Generate SPARQL
        prompt = f"""Convert this natural language query to SPARQL.

Query: {query}
Intent: {intent}

{context}

Instructions:
1. Use predicates from the list above
2. Follow the example structure if provided
3. Include proper PREFIX declarations
4. Return only valid SPARQL

SPARQL:"""

        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a SPARQL expert. Generate valid queries."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2  # Lower temperature for consistency
        )

        sparql = response.choices[0].message.content.strip()

        # Clean up
        if "```" in sparql:
            sparql = sparql.split("```")[1].split("```")[0].strip()
            if sparql.startswith("sparql"):
                sparql = sparql[6:].strip()

        # Validate
        is_valid, errors = self.validate_sparql(sparql)

        result = {
            "query": query,
            "intent": intent,
            "entities": entities,
            "sparql": sparql,
            "is_valid": is_valid,
            "validation_errors": errors,
            "relevant_predicates": [p["predicate"] for p in predicates[:5]],
            "confidence": np.mean([p["similarity"] for p in predicates[:3]]),
            "timestamp": datetime.now().isoformat(),
            "from_cache": False
        }

        # Cache if valid
        if is_valid:
            self.query_cache[query_hash] = result

        return result

    def auto_fix_sparql(self, sparql: str, errors: List[str]) -> str:
        """Attempt to auto-fix common SPARQL errors"""
        fixed = sparql

        for error in errors:
            if "Missing SELECT" in error:
                fixed = "SELECT * WHERE { " + fixed + " }"
            elif "Unbalanced brackets" in error:
                # Add missing brackets
                open_count = fixed.count("{")
                close_count = fixed.count("}")
                if open_count > close_count:
                    fixed += "}" * (open_count - close_count)
                elif close_count > open_count:
                    fixed = "{" * (close_count - open_count) + fixed

        return fixed


def test_enhanced_system():
    """Test the enhanced NL to SPARQL system"""

    converter = EnhancedNLtoSPARQL()

    test_queries = [
        "Who works for Regen Network?",
        "How many projects did Microsoft create?",
        "Find organizations that partner with Google",
        "What climate projects were funded by the EU?",
        "Count the employees of OpenAI",
        "List people who created regenerative agriculture projects"
    ]

    print("\n" + "="*80)
    print("ENHANCED NL TO SPARQL TESTS")
    print("="*80)

    for query in test_queries:
        print(f"\n{'='*40}")
        print(f"Query: {query}")
        print(f"{'='*40}")

        result = converter.generate_sparql_enhanced(query)

        print(f"Intent: {result['intent']}")
        print(f"Entities: {result['entities']}")
        print(f"Valid: {result['is_valid']}")

        if not result['is_valid']:
            print(f"Errors: {result['validation_errors']}")
            # Try auto-fix
            fixed = converter.auto_fix_sparql(result['sparql'], result['validation_errors'])
            print(f"Auto-fixed:\n{fixed}")

        print(f"Confidence: {result['confidence']:.3f}")
        print(f"\nSPARQL:\n{result['sparql']}")

    # Test caching
    print("\n\n=== CACHE TEST ===")
    result2 = converter.generate_sparql_enhanced(test_queries[0])
    if result2.get("from_cache"):
        print("✓ Query successfully retrieved from cache")


if __name__ == "__main__":
    test_enhanced_system()