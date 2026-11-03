#!/usr/bin/env bash
set -euo pipefail

API="${1:-http://localhost:8351}"
QUERIES=(
  "anthropic claude" "graph neural network" "regen network" "p2p commons" "agentic coding"
  "memory architecture" "RAG retrieval" "matchmaking algorithm" "bioregional governance" "discourse graph"
  "OpenAI embedding" "vector database" "knowledge graph" "claims engine" "intelligence primitives"
  "session indexing" "morning brief" "task registry" "vault sync" "convergence detection"
)

for q in "${QUERIES[@]}"; do
  python3 -c "
import sys
import time
import urllib.parse
import urllib.request

query = sys.argv[1]
api = sys.argv[2].rstrip('/')
url = f'{api}/knowledge/unified-search?query={urllib.parse.quote(query)}&limit=10'
t0 = time.time()
try:
    urllib.request.urlopen(url, timeout=10).read()
except Exception as e:
    print(f'ERR {e}', file=sys.stderr)
print(f'{(time.time() - t0) * 1000:.1f}')
" "$q" "$API"
done | sort -n | awk 'BEGIN{c=0} /^[0-9.]+$/ {a[c++]=$1} END{if(c==0){exit 1}; p95=a[int(c*0.95)]; print "p95="p95"ms"}'
