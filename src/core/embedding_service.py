#!/usr/bin/env python3
"""
Embedding Service for Semantic Predicate Matching
Uses the pre-computed predicate embeddings for similarity search
"""
import os
import pickle
import numpy as np
from typing import List, Dict
import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import traceback

app = Flask(__name__)
CORS(app)

class PredicateEmbeddingService:
    def __init__(self):
        print("Loading predicate embeddings...")

        # Load embeddings (just numpy array)
        emb_path = os.getenv('EMBEDDINGS_PATH', 'predicate_embeddings.pkl')
        with open(emb_path, 'rb') as f:
            self.embeddings = pickle.load(f)

        # Load predicate patterns to get predicate list
        patterns_path = os.getenv('PATTERNS_PATH', 'predicate_patterns.json')
        with open(patterns_path, 'r') as f:
            self.patterns = json.load(f)
            self.predicates = [p['predicate'] for p in self.patterns]
            self.pattern_lookup = {p['predicate']: p for p in self.patterns}

        # Load consolidation mapping (t=0.25 preferred, fallback to t=0.30)
        self.predicate_mapping = {}
        self.consolidation_info = {}
        cons_path = os.getenv('CONSOLIDATION_PATH', '')
        tried = []
        for path in [
            cons_path,
            'final_consolidation_all_t0.25.json',
            'final_consolidation_t0.25.json',
            'final_consolidation_all_t0.30.json',
            'final_consolidation_t0.30.json'
        ]:
            if not path:
                continue
            if not os.path.exists(path):
                tried.append(path)
                continue
            try:
                with open(path, 'r') as f:
                    consolidation = json.load(f)
                    # Support multiple schemas
                    self.predicate_mapping = consolidation.get('mapping') or consolidation.get('predicate_mapping') or {}
                    self.consolidation_info = consolidation.get('consolidation_info') or consolidation.get('cluster_info') or {}
                    print(f"Loaded consolidation from {path} with {len(self.predicate_mapping)} mappings")
                    break
            except Exception:
                print(f"Failed to load consolidation from {path}")
                traceback.print_exc()
        if not self.predicate_mapping:
            print("WARNING: No consolidation mapping loaded. Proceeding without consolidation info.")


        print(f"Loaded {len(self.predicates)} predicate embeddings")
        print(f"Loaded {len(self.predicate_mapping)} consolidation mappings")

        # Normalize embeddings for cosine similarity
        norms = np.linalg.norm(self.embeddings, axis=1, keepdims=True)
        self.embeddings_normalized = self.embeddings / (norms + 1e-10)

    def get_embedding(self, text: str) -> np.ndarray:
        """Get embedding for text.
        If OPENAI_API_KEY is set, attempt to use OpenAI; otherwise fallback to naive keyword average.
        """
        try:
            import openai
            api_key = os.getenv('OPENAI_API_KEY')
            if api_key:
                client = openai.OpenAI(api_key=api_key)
                resp = client.embeddings.create(model='text-embedding-3-small', input=text)
                vec = np.array(resp.data[0].embedding, dtype=float)
                return vec / (np.linalg.norm(vec) + 1e-10)
        except Exception:
            pass

        # Fallback: average embeddings of tokens found in known predicates
        text_lower = text.lower()
        similar = []
        for i, pred in enumerate(self.predicates):
            if any(word in pred.lower() for word in text_lower.split() if len(word) > 2):
                similar.append(self.embeddings_normalized[i])
        if similar:
            return np.mean(similar, axis=0)
        # Final fallback: random unit vector (stable seed)
        rng = np.random.default_rng(abs(hash(text)) % (2**32))
        v = rng.standard_normal(self.embeddings_normalized.shape[1])
        return v / (np.linalg.norm(v) + 1e-10)

    def find_similar_predicates(self, query: str, top_k: int = 10) -> List[Dict]:
        """Find semantically similar predicates"""

        # Get query embedding
        query_embedding = self.get_embedding(query)
        query_norm = query_embedding / (np.linalg.norm(query_embedding) + 1e-10)

        # Compute cosine similarities
        similarities = np.dot(self.embeddings_normalized, query_norm)

        # Get top-k indices
        top_indices = np.argsort(similarities)[-max(top_k*3, 20):][::-1]

        results = []
        seen_consolidated: Dict[str, int] = {}

        for idx in top_indices:
            predicate = self.predicates[idx]
            score = float(similarities[idx])

            # Get consolidated form
            consolidated = self.predicate_mapping.get(predicate, predicate)

            # Keep a small number per consolidated to diversify
            if consolidated in seen_consolidated:
                if seen_consolidated[consolidated] >= 3:
                    continue
                seen_consolidated[consolidated] += 1
            else:
                seen_consolidated[consolidated] = 1

            # Get pattern info
            pattern = self.pattern_lookup.get(predicate, {})

            result = {
                'predicate': predicate,
                'consolidated': consolidated,
                'score': score,
                'count': pattern.get('count', 0),
                'examples': pattern.get('example_objects', [])[:3]
            }

            # Add consolidation members if this is a consolidated predicate
            if consolidated in self.consolidation_info:
                info = self.consolidation_info[consolidated]
                result['member_count'] = len(info.get('members', []))
                result['members'] = info.get('members', [])[:5]

            results.append(result)

        return results[:top_k]

    def get_focused_schema(self, query: str) -> Dict:
        """Get focused schema for query"""

        # Find similar predicates
        predicates = self.find_similar_predicates(query, top_k=15)

        # Detect intent
        query_lower = query.lower()
        intent = 'search'

        if any(word in query_lower for word in ['how many', 'count', 'number of', 'total']):
            intent = 'aggregation'
        elif any(word in query_lower for word in ['list', 'show', 'what are', 'which']):
            intent = 'enumeration'
        elif any(word in query_lower for word in ['relationship', 'connected', 'related']):
            intent = 'relationship'

        return {
            'intent': intent,
            'predicates': predicates,
            'query': query
        }

service = PredicateEmbeddingService()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'predicates': len(service.predicates)})

@app.route('/similar', methods=['POST'])
def find_similar():
    data = request.json
    query = data.get('query', '')
    top_k = data.get('top_k', 10)

    results = service.find_similar_predicates(query, top_k)
    return jsonify(results)

@app.route('/schema', methods=['POST'])
def get_schema():
    data = request.json
    query = data.get('query', '')

    schema = service.get_focused_schema(query)
    return jsonify(schema)

if __name__ == '__main__':
    print("Starting Embedding Service on port 8095...")
    app.run(host='0.0.0.0', port=8095, debug=False)
