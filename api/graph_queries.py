"""
Graph traversal queries for Personal KOI.

All traversal SQL lives here — only PG implementation (asyncpg).
Static SQL constants per direction/predicate variant. No dynamic SQL injection.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# Safety caps
# =============================================================================

MAX_DEPTH_NEIGHBORHOOD = 4
MAX_DEPTH_SHORTEST_PATH = 8
MAX_NODES = 500
MAX_EDGES = 2000
DEFAULT_DEPTH = 2
DEFAULT_MAX_NODES = 200
DEFAULT_MAX_EDGES = 1000
DEFAULT_SHORTEST_PATH_DEPTH = 6
QUERY_TIMEOUT = 5.0

# =============================================================================
# Neighborhood CTE constants — 3 directions x 2 predicate variants = 6
# =============================================================================

# Frontier fanout guard: max_cte_rows = max_nodes * 3, passed as parameter

NEIGHBORHOOD_CTE_BOTH = """
WITH RECURSIVE traverse AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS visited, 1 AS row_num
    UNION ALL
    SELECT
        CASE WHEN er.subject_uri = t.uri THEN er.object_uri ELSE er.subject_uri END,
        t.depth + 1,
        t.visited || CASE WHEN er.subject_uri = t.uri THEN er.object_uri ELSE er.subject_uri END,
        t.row_num + 1
    FROM traverse t
    JOIN entity_relationships er
        ON (er.subject_uri = t.uri OR er.object_uri = t.uri)
    WHERE t.depth < $2
      AND CASE WHEN er.subject_uri = t.uri THEN er.object_uri ELSE er.subject_uri END != ALL(t.visited)
      AND t.row_num < $3
)
SELECT DISTINCT ON (uri) uri, depth
FROM traverse
ORDER BY uri, depth ASC
"""

NEIGHBORHOOD_CTE_BOTH_PREDICATE = """
WITH RECURSIVE traverse AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS visited, 1 AS row_num
    UNION ALL
    SELECT
        CASE WHEN er.subject_uri = t.uri THEN er.object_uri ELSE er.subject_uri END,
        t.depth + 1,
        t.visited || CASE WHEN er.subject_uri = t.uri THEN er.object_uri ELSE er.subject_uri END,
        t.row_num + 1
    FROM traverse t
    JOIN entity_relationships er
        ON (er.subject_uri = t.uri OR er.object_uri = t.uri)
    WHERE t.depth < $2
      AND CASE WHEN er.subject_uri = t.uri THEN er.object_uri ELSE er.subject_uri END != ALL(t.visited)
      AND t.row_num < $3
      AND er.predicate = $4
)
SELECT DISTINCT ON (uri) uri, depth
FROM traverse
ORDER BY uri, depth ASC
"""

NEIGHBORHOOD_CTE_OUTGOING = """
WITH RECURSIVE traverse AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS visited, 1 AS row_num
    UNION ALL
    SELECT
        er.object_uri,
        t.depth + 1,
        t.visited || er.object_uri,
        t.row_num + 1
    FROM traverse t
    JOIN entity_relationships er ON er.subject_uri = t.uri
    WHERE t.depth < $2
      AND er.object_uri != ALL(t.visited)
      AND t.row_num < $3
)
SELECT DISTINCT ON (uri) uri, depth
FROM traverse
ORDER BY uri, depth ASC
"""

NEIGHBORHOOD_CTE_OUTGOING_PREDICATE = """
WITH RECURSIVE traverse AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS visited, 1 AS row_num
    UNION ALL
    SELECT
        er.object_uri,
        t.depth + 1,
        t.visited || er.object_uri,
        t.row_num + 1
    FROM traverse t
    JOIN entity_relationships er ON er.subject_uri = t.uri
    WHERE t.depth < $2
      AND er.object_uri != ALL(t.visited)
      AND t.row_num < $3
      AND er.predicate = $4
)
SELECT DISTINCT ON (uri) uri, depth
FROM traverse
ORDER BY uri, depth ASC
"""

NEIGHBORHOOD_CTE_INCOMING = """
WITH RECURSIVE traverse AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS visited, 1 AS row_num
    UNION ALL
    SELECT
        er.subject_uri,
        t.depth + 1,
        t.visited || er.subject_uri,
        t.row_num + 1
    FROM traverse t
    JOIN entity_relationships er ON er.object_uri = t.uri
    WHERE t.depth < $2
      AND er.subject_uri != ALL(t.visited)
      AND t.row_num < $3
)
SELECT DISTINCT ON (uri) uri, depth
FROM traverse
ORDER BY uri, depth ASC
"""

NEIGHBORHOOD_CTE_INCOMING_PREDICATE = """
WITH RECURSIVE traverse AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS visited, 1 AS row_num
    UNION ALL
    SELECT
        er.subject_uri,
        t.depth + 1,
        t.visited || er.subject_uri,
        t.row_num + 1
    FROM traverse t
    JOIN entity_relationships er ON er.object_uri = t.uri
    WHERE t.depth < $2
      AND er.subject_uri != ALL(t.visited)
      AND t.row_num < $3
      AND er.predicate = $4
)
SELECT DISTINCT ON (uri) uri, depth
FROM traverse
ORDER BY uri, depth ASC
"""

# =============================================================================
# Shortest-path CTE constants — 3 directions x 2 predicate variants = 6
# =============================================================================

SHORTEST_PATH_CTE_BOTH = """
WITH RECURSIVE bfs AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS path, ARRAY[$1::text] AS visited
    UNION ALL
    SELECT
        CASE WHEN er.subject_uri = b.uri THEN er.object_uri ELSE er.subject_uri END,
        b.depth + 1,
        b.path || CASE WHEN er.subject_uri = b.uri THEN er.object_uri ELSE er.subject_uri END,
        b.visited || CASE WHEN er.subject_uri = b.uri THEN er.object_uri ELSE er.subject_uri END
    FROM bfs b
    JOIN entity_relationships er
        ON (er.subject_uri = b.uri OR er.object_uri = b.uri)
    WHERE b.depth < $3
      AND CASE WHEN er.subject_uri = b.uri THEN er.object_uri ELSE er.subject_uri END != ALL(b.visited)
)
SELECT uri, depth, path
FROM bfs
WHERE uri = $2
ORDER BY depth ASC
LIMIT 1
"""

SHORTEST_PATH_CTE_OUTGOING = """
WITH RECURSIVE bfs AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS path, ARRAY[$1::text] AS visited
    UNION ALL
    SELECT
        er.object_uri,
        b.depth + 1,
        b.path || er.object_uri,
        b.visited || er.object_uri
    FROM bfs b
    JOIN entity_relationships er ON er.subject_uri = b.uri
    WHERE b.depth < $3
      AND er.object_uri != ALL(b.visited)
)
SELECT uri, depth, path
FROM bfs
WHERE uri = $2
ORDER BY depth ASC
LIMIT 1
"""

SHORTEST_PATH_CTE_INCOMING = """
WITH RECURSIVE bfs AS (
    SELECT $1::text AS uri, 0 AS depth, ARRAY[$1::text] AS path, ARRAY[$1::text] AS visited
    UNION ALL
    SELECT
        er.subject_uri,
        b.depth + 1,
        b.path || er.subject_uri,
        b.visited || er.subject_uri
    FROM bfs b
    JOIN entity_relationships er ON er.object_uri = b.uri
    WHERE b.depth < $3
      AND er.subject_uri != ALL(b.visited)
)
SELECT uri, depth, path
FROM bfs
WHERE uri = $2
ORDER BY depth ASC
LIMIT 1
"""

# =============================================================================
# Directed relationship queries (1-hop)
# =============================================================================

RELATIONSHIPS_BOTH = """
SELECT subject_uri, predicate, object_uri, confidence, source, source_rid
FROM entity_relationships
WHERE subject_uri = $1 OR object_uri = $1
ORDER BY predicate, confidence DESC
"""

RELATIONSHIPS_BOTH_PREDICATE = """
SELECT subject_uri, predicate, object_uri, confidence, source, source_rid
FROM entity_relationships
WHERE (subject_uri = $1 OR object_uri = $1)
AND predicate = $2
ORDER BY confidence DESC
"""

RELATIONSHIPS_OUTGOING = """
SELECT subject_uri, predicate, object_uri, confidence, source, source_rid
FROM entity_relationships
WHERE subject_uri = $1
ORDER BY predicate, confidence DESC
"""

RELATIONSHIPS_OUTGOING_PREDICATE = """
SELECT subject_uri, predicate, object_uri, confidence, source, source_rid
FROM entity_relationships
WHERE subject_uri = $1 AND predicate = $2
ORDER BY confidence DESC
"""

RELATIONSHIPS_INCOMING = """
SELECT subject_uri, predicate, object_uri, confidence, source, source_rid
FROM entity_relationships
WHERE object_uri = $1
ORDER BY predicate, confidence DESC
"""

RELATIONSHIPS_INCOMING_PREDICATE = """
SELECT subject_uri, predicate, object_uri, confidence, source, source_rid
FROM entity_relationships
WHERE object_uri = $1 AND predicate = $2
ORDER BY confidence DESC
"""

# =============================================================================
# Index verification
# =============================================================================

REQUIRED_INDEXES = [
    "idx_rel_subject",
    "idx_rel_object",
    "idx_rel_subject_predicate",
    "idx_rel_object_predicate",
]


async def verify_indexes(conn) -> None:
    """Check that required indexes exist. Logs warnings for missing ones. Non-fatal."""
    rows = await conn.fetch("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'entity_relationships'
    """)
    existing = {r["indexname"] for r in rows}
    for idx in REQUIRED_INDEXES:
        if idx not in existing:
            logger.warning(
                f"Missing index '{idx}' on entity_relationships — "
                f"graph traversal queries may be slow"
            )


# =============================================================================
# Neighborhood traversal
# =============================================================================


def _clamp(value: int, default: int, maximum: int) -> int:
    if value is None:
        return default
    return min(max(value, 1), maximum)


async def get_neighborhood(
    conn,
    entity_uri: str,
    max_depth: int = DEFAULT_DEPTH,
    direction: str = "both",
    predicate: Optional[str] = None,
    entity_type: Optional[str] = None,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_edges: int = DEFAULT_MAX_EDGES,
) -> dict:
    """Return {nodes, edges, total_nodes_discovered, truncated, ...}."""

    max_depth = _clamp(max_depth, DEFAULT_DEPTH, MAX_DEPTH_NEIGHBORHOOD)
    max_nodes = _clamp(max_nodes, DEFAULT_MAX_NODES, MAX_NODES)
    max_edges = _clamp(max_edges, DEFAULT_MAX_EDGES, MAX_EDGES)
    max_cte_rows = max_nodes * 3  # frontier fanout guard

    # 1. Discover reachable nodes via recursive CTE
    if predicate:
        if direction == "outgoing":
            cte = NEIGHBORHOOD_CTE_OUTGOING_PREDICATE
        elif direction == "incoming":
            cte = NEIGHBORHOOD_CTE_INCOMING_PREDICATE
        else:
            cte = NEIGHBORHOOD_CTE_BOTH_PREDICATE
        all_nodes = await conn.fetch(
            cte, entity_uri, max_depth, max_cte_rows, predicate,
            timeout=QUERY_TIMEOUT,
        )
    else:
        if direction == "outgoing":
            cte = NEIGHBORHOOD_CTE_OUTGOING
        elif direction == "incoming":
            cte = NEIGHBORHOOD_CTE_INCOMING
        else:
            cte = NEIGHBORHOOD_CTE_BOTH
        all_nodes = await conn.fetch(
            cte, entity_uri, max_depth, max_cte_rows,
            timeout=QUERY_TIMEOUT,
        )

    total_nodes_discovered = len(all_nodes)

    # Sort by depth ASC, uri ASC and apply max_nodes cap
    sorted_nodes = sorted(all_nodes, key=lambda r: (r["depth"], r["uri"]))
    capped_nodes = sorted_nodes[:max_nodes]
    node_uris = [r["uri"] for r in capped_nodes]
    depth_map = {r["uri"]: r["depth"] for r in capped_nodes}

    # 2. Batch enrich with entity metadata
    enrichment = {}
    if node_uris:
        rows = await conn.fetch("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry
            WHERE fuseki_uri = ANY($1)
        """, node_uris, timeout=QUERY_TIMEOUT)
        for r in rows:
            enrichment[r["fuseki_uri"]] = {
                "name": r["entity_text"],
                "entity_type": r["entity_type"],
            }

    # 3. Apply entity_type filter (root always kept)
    if entity_type:
        filtered_uris = set()
        for uri in node_uris:
            if uri == entity_uri:
                filtered_uris.add(uri)
            elif enrichment.get(uri, {}).get("entity_type") == entity_type:
                filtered_uris.add(uri)
        node_uris = [u for u in node_uris if u in filtered_uris]

    # 4. Get edges between included nodes
    edges = []
    total_edges_discovered = 0
    if len(node_uris) > 1:
        # Count total edges
        total_edges_discovered = await conn.fetchval("""
            SELECT COUNT(*)
            FROM entity_relationships
            WHERE subject_uri = ANY($1) AND object_uri = ANY($1)
        """, node_uris, timeout=QUERY_TIMEOUT)

        # Fetch capped edges
        edge_rows = await conn.fetch("""
            SELECT subject_uri, object_uri, predicate, confidence
            FROM entity_relationships
            WHERE subject_uri = ANY($1) AND object_uri = ANY($1)
            ORDER BY confidence DESC, predicate ASC
            LIMIT $2
        """, node_uris, max_edges, timeout=QUERY_TIMEOUT)

        edges = [
            {
                "source": r["subject_uri"],
                "target": r["object_uri"],
                "predicate": r["predicate"],
                "confidence": float(r["confidence"]) if r["confidence"] else 1.0,
            }
            for r in edge_rows
        ]

    # 5. Build node list
    nodes = []
    for uri in node_uris:
        info = enrichment.get(uri, {})
        nodes.append({
            "uri": uri,
            "name": info.get("name"),
            "entity_type": info.get("entity_type"),
            "depth": depth_map.get(uri, 0),
        })

    node_count = len(nodes)
    edge_count = len(edges)
    truncated = node_count < total_nodes_discovered or edge_count < total_edges_discovered

    return {
        "root": entity_uri,
        "max_depth": max_depth,
        "direction": direction,
        "nodes": nodes,
        "edges": edges,
        "node_count": node_count,
        "total_nodes_discovered": total_nodes_discovered,
        "edge_count": edge_count,
        "total_edges_discovered": total_edges_discovered,
        "truncated": truncated,
    }


# =============================================================================
# Shortest path
# =============================================================================


async def get_shortest_path(
    conn,
    source: str,
    target: str,
    max_depth: int = DEFAULT_SHORTEST_PATH_DEPTH,
    direction: str = "both",
) -> dict:
    """Return {found, path_length, steps, nodes} or {found: False}."""

    max_depth = _clamp(max_depth, DEFAULT_SHORTEST_PATH_DEPTH, MAX_DEPTH_SHORTEST_PATH)

    # Same entity → trivial path
    if source == target:
        # Enrich root node
        row = await conn.fetchrow("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry WHERE fuseki_uri = $1
        """, source, timeout=QUERY_TIMEOUT)

        root_node = {
            "uri": source,
            "name": row["entity_text"] if row else None,
            "entity_type": row["entity_type"] if row else None,
            "depth": 0,
        }
        return {
            "source": source,
            "target": target,
            "found": True,
            "path_length": 0,
            "direction": direction,
            "steps": [],
            "nodes": [root_node],
        }

    # BFS CTE
    if direction == "outgoing":
        cte = SHORTEST_PATH_CTE_OUTGOING
    elif direction == "incoming":
        cte = SHORTEST_PATH_CTE_INCOMING
    else:
        cte = SHORTEST_PATH_CTE_BOTH

    result = await conn.fetchrow(
        cte, source, target, max_depth, timeout=QUERY_TIMEOUT,
    )

    if not result:
        return {
            "source": source,
            "target": target,
            "found": False,
            "path_length": None,
            "direction": direction,
            "steps": [],
            "nodes": [],
        }

    path = result["path"]  # array of URIs
    path_length = result["depth"]

    # Enrich all nodes on path
    enrichment = {}
    if path:
        rows = await conn.fetch("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry WHERE fuseki_uri = ANY($1)
        """, path, timeout=QUERY_TIMEOUT)
        for r in rows:
            enrichment[r["fuseki_uri"]] = {
                "name": r["entity_text"],
                "entity_type": r["entity_type"],
            }

    nodes = []
    for i, uri in enumerate(path):
        info = enrichment.get(uri, {})
        nodes.append({
            "uri": uri,
            "name": info.get("name"),
            "entity_type": info.get("entity_type"),
            "depth": i,
        })

    # Build steps with deterministic edge selection
    steps = []
    for i in range(len(path) - 1):
        from_uri = path[i]
        to_uri = path[i + 1]

        # Find the edge connecting these two nodes (deterministic: highest confidence, then alpha)
        edge = await conn.fetchrow("""
            SELECT subject_uri, predicate, object_uri, confidence
            FROM entity_relationships
            WHERE (subject_uri = $1 AND object_uri = $2)
               OR (subject_uri = $2 AND object_uri = $1)
            ORDER BY confidence DESC, predicate ASC
            LIMIT 1
        """, from_uri, to_uri, timeout=QUERY_TIMEOUT)

        if edge:
            from_info = enrichment.get(from_uri, {})
            to_info = enrichment.get(to_uri, {})
            step_direction = "outgoing" if edge["subject_uri"] == from_uri else "incoming"
            steps.append({
                "from_uri": from_uri,
                "from_name": from_info.get("name"),
                "predicate": edge["predicate"],
                "direction": step_direction,
                "to_uri": to_uri,
                "to_name": to_info.get("name"),
            })

    return {
        "source": source,
        "target": target,
        "found": True,
        "path_length": path_length,
        "direction": direction,
        "steps": steps,
        "nodes": nodes,
    }


# =============================================================================
# Directed relationship queries (1-hop, for existing endpoint)
# =============================================================================


async def get_relationships_directed(
    conn,
    entity_uri: str,
    predicate: Optional[str] = None,
    direction: str = "both",
) -> List[Dict[str, Any]]:
    """1-hop relationships with direction filter."""
    if direction == "outgoing":
        if predicate:
            rows = await conn.fetch(
                RELATIONSHIPS_OUTGOING_PREDICATE, entity_uri, predicate,
                timeout=QUERY_TIMEOUT,
            )
        else:
            rows = await conn.fetch(
                RELATIONSHIPS_OUTGOING, entity_uri, timeout=QUERY_TIMEOUT,
            )
    elif direction == "incoming":
        if predicate:
            rows = await conn.fetch(
                RELATIONSHIPS_INCOMING_PREDICATE, entity_uri, predicate,
                timeout=QUERY_TIMEOUT,
            )
        else:
            rows = await conn.fetch(
                RELATIONSHIPS_INCOMING, entity_uri, timeout=QUERY_TIMEOUT,
            )
    else:
        if predicate:
            rows = await conn.fetch(
                RELATIONSHIPS_BOTH_PREDICATE, entity_uri, predicate,
                timeout=QUERY_TIMEOUT,
            )
        else:
            rows = await conn.fetch(
                RELATIONSHIPS_BOTH, entity_uri, timeout=QUERY_TIMEOUT,
            )

    return [dict(r) for r in rows]
