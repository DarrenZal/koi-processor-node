#!/usr/bin/env python3
"""
Compute predicate communities from co-occurrence in the refined RDF graph.
Builds a simple co-occurrence graph (predicates co-used on the same subject),
then runs a lightweight label-propagation clustering (no external deps required).
Outputs:
  - predicate_communities.json (communities, mapping, centrality, stats)
"""

import json
import requests
from collections import defaultdict, Counter
from itertools import combinations
from typing import Dict, List, Tuple
import math

import os
JENA_ENDPOINT = os.getenv("JENA_ENDPOINT", "http://localhost:3030/koi/sparql")

def execute_sparql(query: str) -> dict:
    r = requests.post(
        JENA_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=30,
    )
    if r.status_code == 200:
        return r.json()
    return {"results": {"bindings": []}}

def fetch_subject_predicates(limit: int = None) -> Dict[str, List[str]]:
    print("Fetching subject→predicates map …")
    q = f"""
PREFIX regx: <https://regen.network/ontology/experimental#>
SELECT ?subject ?predicate WHERE {{
  ?stmt a regx:Statement .
  ?stmt regx:subject ?subject .
  ?stmt regx:predicate ?predicate .
}}
{f'LIMIT {limit}' if limit else ''}
"""
    data = execute_sparql(q)
    subj_map: Dict[str, List[str]] = defaultdict(list)
    for b in data.get("results", {}).get("bindings", []):
        s = b["subject"]["value"]
        p = b["predicate"]["value"]
        subj_map[s].append(p)
    print(f"  Subjects: {len(subj_map)}")
    return subj_map

def build_cooccurrence(subj_map: Dict[str, List[str]]) -> Dict[Tuple[str, str], int]:
    print("Building co-occurrence graph …")
    co = Counter()
    for preds in subj_map.values():
        uniq = sorted(set(preds))
        for a, b in combinations(uniq, 2):
            co[(a, b)] += 1
    print(f"  Edges (unique pairs): {len(co)}")
    return co

def build_adj(co: Dict[Tuple[str, str], int], min_weight: int = 2):
    adj = defaultdict(dict)  # pred -> neighbor -> weight
    for (a, b), w in co.items():
        if w >= min_weight:
            adj[a][b] = w
            adj[b][a] = w
    print(f"  Filtered edges (w>={min_weight}): ~{sum(len(v) for v in adj.values())//2}")
    return adj

def label_propagation(adj: Dict[str, Dict[str, int]], max_iter: int = 20) -> Dict[str, int]:
    # Initialize each node to its own label
    labels = {node: i for i, node in enumerate(adj.keys())}
    nodes = list(adj.keys())
    changed = True
    it = 0
    while changed and it < max_iter:
        changed = False
        it += 1
        for n in nodes:
            # Tally neighbor labels weighted by edge weight
            scores = Counter()
            for nb, w in adj[n].items():
                scores[labels[nb]] += w
            if scores:
                best_label, _ = scores.most_common(1)[0]
                if best_label != labels[n]:
                    labels[n] = best_label
                    changed = True
        print(f"  Iter {it}: {'changed' if changed else 'converged'}")
    return labels

def degree_centrality(adj: Dict[str, Dict[str, int]]) -> Dict[str, float]:
    # Weighted degree centrality (sum of weights)
    return {n: float(sum(adj[n].values())) for n in adj}

def main():
    subj_map = fetch_subject_predicates()
    co = build_cooccurrence(subj_map)
    adj = build_adj(co, min_weight=2)
    if not adj:
        print("No edges after filtering; aborting.")
        return
    labels = label_propagation(adj)
    centrality = degree_centrality(adj)

    # Group by label
    comm_to_members: Dict[int, List[str]] = defaultdict(list)
    for n, lab in labels.items():
        comm_to_members[lab].append(n)

    # Build output structures
    communities = []
    for cid, members in comm_to_members.items():
        members_sorted = sorted(members, key=lambda x: centrality.get(x, 0.0), reverse=True)
        communities.append({
            "id": cid,
            "size": len(members),
            "members": members_sorted,
            "top_members": members_sorted[:10]
        })
    communities.sort(key=lambda c: c["size"], reverse=True)

    predicate_to_community = {n: labels[n] for n in labels}
    num_edges = sum(len(v) for v in adj.values()) // 2
    result = {
        "graph_stats": {
            "num_predicates": len(adj),
            "num_edges": num_edges,
        },
        "communities": communities,
        "predicate_to_community": predicate_to_community,
        "centrality": centrality,
        "params": {"min_weight": 2}
    }

    with open("predicate_communities.json", "w") as f:
        json.dump(result, f, indent=2)
    print("Saved predicate_communities.json")

if __name__ == "__main__":
    main()
