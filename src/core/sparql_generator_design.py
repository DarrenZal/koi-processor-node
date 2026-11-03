#!/usr/bin/env python3
"""
Hybrid Natural Language to SPARQL System Design
Combines semantic search with structured schema for optimal query generation
"""

class HybridSPARQLGenerator:
    """
    Three-stage approach:
    1. Semantic Retrieval: Find relevant predicates/patterns
    2. Schema Construction: Build focused ontology subset
    3. Query Generation: LLM creates SPARQL with context
    """

    def __init__(self):
        # Pre-computed resources
        self.predicate_embeddings = None  # 7,037 embeddings
        self.consolidated_mapping = None  # Original -> consolidated
        self.pattern_index = None  # Triple patterns by type
        self.query_templates = None  # Common SPARQL patterns

    def generate_sparql(self, prompt: str) -> str:
        """Main pipeline for natural language to SPARQL"""

        # Stage 1: Semantic Retrieval
        relevant_predicates = self.semantic_retrieval(prompt)

        # Stage 2: Schema Construction
        focused_schema = self.build_focused_schema(
            prompt,
            relevant_predicates
        )

        # Stage 3: Query Generation
        sparql = self.llm_generate(prompt, focused_schema)

        return sparql

    def semantic_retrieval(self, prompt: str, top_k: int = 20):
        """
        Find relevant predicates using semantic search
        """
        # 1. Extract key terms from prompt
        key_terms = self.extract_key_terms(prompt)
        # Example: "Who works for OpenAI?" -> ["works", "for"]

        # 2. Embed the terms
        term_embeddings = self.embed_terms(key_terms)

        # 3. Search predicate embeddings
        relevant_predicates = []
        for term_emb in term_embeddings:
            similar = cosine_similarity(term_emb, self.predicate_embeddings)
            top_indices = similar.argsort()[-top_k:]
            relevant_predicates.extend(top_indices)

        # 4. Expand consolidated predicates to include all variants
        expanded = self.expand_consolidated(relevant_predicates)

        return expanded

    def build_focused_schema(self, prompt: str, predicates: list):
        """
        Build a focused ontology subset for the LLM
        """
        schema = {
            "entity_types": ["Person", "Organization", "Project"],
            "predicates": {},
            "patterns": [],
            "examples": []
        }

        # Group predicates by subject/object type patterns
        for pred in predicates:
            pattern = self.get_pattern(pred)  # e.g., "Person -> Organization"

            if pattern not in schema["predicates"]:
                schema["predicates"][pattern] = []

            schema["predicates"][pattern].append({
                "predicate": pred,
                "usage_count": self.get_usage_count(pred),
                "consolidated_as": self.consolidated_mapping.get(pred),
                "examples": self.get_examples(pred, limit=2)
            })

        # Add common patterns based on prompt type
        if self.is_aggregation_query(prompt):
            schema["patterns"].append("Aggregation queries use COUNT, GROUP BY")
            schema["examples"].append(self.get_aggregation_example())

        if self.is_path_query(prompt):
            schema["patterns"].append("Path queries traverse relationships")
            schema["examples"].append(self.get_path_example())

        return schema

    def llm_generate(self, prompt: str, schema: dict):
        """
        Generate SPARQL using LLM with focused context
        """
        context = f"""
        Convert this natural language query to SPARQL.

        Query: {prompt}

        Available Schema:
        - Entity Types: {schema['entity_types']}
        - Relevant Predicates:
        {self.format_predicates(schema['predicates'])}

        Examples:
        {self.format_examples(schema['examples'])}

        Generate a SPARQL query using the predicates above.
        Use the exact predicate names from the schema.
        """

        # Call LLM (GPT-4 or similar)
        sparql = self.llm.generate(context)

        # Validate and fix common issues
        sparql = self.validate_and_fix(sparql, schema)

        return sparql


# Pre-computed Pattern Index Structure
PATTERN_INDEX = {
    "Person": {
        "outgoing": {
            "Organization": ["works for", "employed by", "founded", "leads"],
            "Project": ["contributes to", "created", "manages", "participates in"],
            "Person": ["knows", "collaborated with", "reports to"]
        },
        "incoming": {
            "Organization": ["employs", "was founded by"],
            "Project": ["was created by", "is managed by"]
        }
    },
    "Organization": {
        "outgoing": {
            "Project": ["develops", "funds", "sponsors", "hosts"],
            "Organization": ["partners with", "acquired", "is part of"],
            "Person": ["employs", "was founded by"]
        }
    },
    # ... etc
}


# Query Template Library
QUERY_TEMPLATES = {
    "entity_search": """
        SELECT ?entity ?label WHERE {{
            ?entity a schema:{entity_type} .
            ?entity rdfs:label ?label .
            FILTER(CONTAINS(LCASE(?label), "{search_term}"))
        }}
    """,

    "relationship_query": """
        SELECT ?subject ?object WHERE {{
            ?stmt regx:subject ?subject .
            ?stmt regx:predicate "{predicate}" .
            ?stmt regx:object ?object .
            {filters}
        }}
    """,

    "aggregation": """
        SELECT ?group (COUNT(?item) as ?count) WHERE {{
            {pattern}
        }}
        GROUP BY ?group
        ORDER BY DESC(?count)
    """
}


def evaluate_approaches():
    """
    Comparative analysis of different approaches
    """

    approaches = {
        "full_consolidation": {
            "predicate_count": 2627,
            "context_size_kb": 52,  # ~20 bytes per predicate
            "pros": ["Complete ontology visible", "Consistent naming"],
            "cons": ["Large context", "May confuse LLM", "Loses specificity"],
            "estimated_accuracy": 0.65
        },

        "pure_semantic": {
            "predicate_count": "unlimited",
            "context_size_kb": 2,  # Just the prompt + retrieved predicates
            "pros": ["Scales infinitely", "Fast", "Precise matching"],
            "cons": ["Misses relationships", "No structural understanding"],
            "estimated_accuracy": 0.55
        },

        "hybrid_semantic_schema": {
            "predicate_count": "20-50 relevant",
            "context_size_kb": 5,  # Focused subset
            "pros": [
                "Optimal context size",
                "Includes structure",
                "Scalable",
                "High precision"
            ],
            "cons": ["More complex", "Requires preprocessing"],
            "estimated_accuracy": 0.85
        },

        "few_shot_learning": {
            "predicate_count": "N/A",
            "context_size_kb": 10,  # Examples
            "pros": ["Works well for common patterns", "No ontology needed"],
            "cons": ["Fails on novel queries", "Needs many examples"],
            "estimated_accuracy": 0.70
        }
    }

    return approaches


# Optimizations for the Hybrid Approach

class OptimizedRetrieval:
    """
    Optimizations to make the hybrid approach production-ready
    """

    def __init__(self):
        # Cache frequent queries
        self.query_cache = {}

        # Pre-compute predicate clusters by domain
        self.domain_clusters = self.cluster_by_domain()

        # Build inverted index for fast entity lookup
        self.entity_index = self.build_entity_index()

    def cluster_by_domain(self):
        """
        Group predicates by semantic domain for faster retrieval
        """
        domains = {
            "employment": ["works for", "employed by", "employee of", ...],
            "creation": ["created", "developed", "built", "made", ...],
            "collaboration": ["partners with", "collaborates", "works with", ...],
            "ownership": ["owns", "has", "possesses", "controls", ...],
            # ...
        }
        return domains

    def smart_retrieval(self, prompt: str):
        """
        Multi-strategy retrieval based on prompt analysis
        """
        # 1. Check cache
        if prompt in self.query_cache:
            return self.query_cache[prompt]

        # 2. Identify query intent
        intent = self.classify_intent(prompt)

        # 3. Retrieve based on intent
        if intent == "employment_query":
            predicates = self.domain_clusters["employment"]
        elif intent == "network_analysis":
            predicates = self.domain_clusters["collaboration"]
        else:
            # Fallback to semantic search
            predicates = self.semantic_search(prompt)

        # 4. Cache for reuse
        self.query_cache[prompt] = predicates

        return predicates