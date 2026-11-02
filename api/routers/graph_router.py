"""Graph traversal endpoints extracted from personal_ingest_api.py.

Provides neighborhood traversal, shortest-path, ad-hoc query, stats,
and temporal query endpoints (assertion history, timeline).
Pure SQL via asyncpg -- no external deps, always enabled.
"""

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from api.graph_queries import get_neighborhood, get_shortest_path


# -- Response models ---------------------------------------------------------

class GraphNode(BaseModel):
    uri: str
    name: Optional[str] = None
    entity_type: Optional[str] = None
    depth: int = 0


class GraphEdge(BaseModel):
    source: str
    target: str
    predicate: str
    confidence: float = 1.0


class NeighborhoodResponse(BaseModel):
    root: str
    max_depth: int
    direction: str
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    node_count: int
    total_nodes_discovered: int
    edge_count: int
    total_edges_discovered: int
    truncated: bool


class PathStep(BaseModel):
    from_uri: str
    from_name: Optional[str] = None
    predicate: str
    direction: str
    to_uri: str
    to_name: Optional[str] = None


class ShortestPathResponse(BaseModel):
    source: str
    target: str
    found: bool
    path_length: Optional[int] = None
    direction: str
    steps: List[PathStep]
    nodes: List[GraphNode]


class GraphQueryRequest(BaseModel):
    """Ad-hoc graph query request."""
    entity_uri: str = Field(..., description="Root entity URI to query from")
    max_depth: int = Field(2, ge=1, le=4, description="Traversal depth")
    direction: Literal["incoming", "outgoing", "both"] = "both"
    predicate: Optional[str] = Field(None, description="Filter edges by predicate")
    entity_type: Optional[str] = Field(None, description="Filter nodes by entity type")
    max_nodes: int = Field(200, ge=1, le=500)
    max_edges: int = Field(1000, ge=1, le=2000)


class AssertionRecord(BaseModel):
    """A single assertion from the bi-temporal assertion history."""
    assertion_id: str
    subject: str
    predicate: str
    object_uri: Optional[str] = None
    object_literal: Optional[str] = None
    asserted_by_node_rid: str
    tx_recorded_at: datetime
    tx_retracted_at: Optional[datetime] = None
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    supersedes_assertion_id: Optional[str] = None
    provenance_doc_rid: Optional[str] = None


class AssertionHistoryResponse(BaseModel):
    """Assertion history for a single entity."""
    entity_uri: str
    assertions: List[AssertionRecord]
    total: int


class TimelineEntry(BaseModel):
    """An assertion relevant to a temporal range query."""
    assertion_id: str
    subject: str
    predicate: str
    object_uri: Optional[str] = None
    object_literal: Optional[str] = None
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None
    tx_recorded_at: datetime


class TimelineResponse(BaseModel):
    """Assertions within a temporal range."""
    start: Optional[str] = None
    end: Optional[str] = None
    entity_type: Optional[str] = None
    entries: List[TimelineEntry]
    total: int


# -- Router factory ----------------------------------------------------------

def create_router(pool, caps):
    """Return an APIRouter with graph traversal and temporal endpoints.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities (graph_queries and assertion_history flags).
    """
    router = APIRouter(prefix="/graph", tags=["graph"])

    # -- Neighborhood --------------------------------------------------------

    @router.get(
        "/neighborhood/{entity_uri:path}",
        response_model=NeighborhoodResponse,
    )
    async def graph_neighborhood(
        request: Request,
        entity_uri: str,
        max_depth: int = 2,
        direction: Literal["incoming", "outgoing", "both"] = "both",
        predicate: Optional[str] = None,
        entity_type: Optional[str] = None,
        max_nodes: int = 200,
        max_edges: int = 1000,
    ):
        """Return the neighborhood graph around an entity via recursive traversal.

        Discovers reachable nodes up to ``max_depth`` hops and returns nodes
        plus edges between them.  Safety caps prevent runaway queries.
        """
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM entity_registry WHERE fuseki_uri = $1)",
                entity_uri,
            )
            if not exists:
                raise HTTPException(
                    status_code=404, detail=f"Entity not found: {entity_uri}"
                )

            result = await get_neighborhood(
                conn, entity_uri, max_depth, direction, predicate,
                entity_type, max_nodes, max_edges,
            )
        return result

    # -- Shortest path -------------------------------------------------------

    @router.get("/shortest-path", response_model=ShortestPathResponse)
    async def graph_shortest_path(
        request: Request,
        source: str,
        target: str,
        max_depth: int = 6,
        direction: Literal["incoming", "outgoing", "both"] = "both",
    ):
        """Find the shortest path between two entities via BFS."""
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        async with pool.acquire() as conn:
            for uri, label in [(source, "source"), (target, "target")]:
                exists = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM entity_registry WHERE fuseki_uri = $1)",
                    uri,
                )
                if not exists:
                    raise HTTPException(
                        status_code=404,
                        detail=f"{label.capitalize()} entity not found: {uri}",
                    )

            result = await get_shortest_path(
                conn, source, target, max_depth, direction,
            )
        return result

    # -- Ad-hoc query --------------------------------------------------------

    @router.post("/query", response_model=NeighborhoodResponse)
    async def graph_query(body: GraphQueryRequest):
        """Ad-hoc graph query — POST variant of neighborhood with JSON body.

        Useful for programmatic clients that want to pass all parameters
        in a structured request body rather than query strings.
        """
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM entity_registry WHERE fuseki_uri = $1)",
                body.entity_uri,
            )
            if not exists:
                raise HTTPException(
                    status_code=404,
                    detail=f"Entity not found: {body.entity_uri}",
                )

            result = await get_neighborhood(
                conn, body.entity_uri, body.max_depth, body.direction,
                body.predicate, body.entity_type, body.max_nodes, body.max_edges,
            )
        return result

    # -- Stats ---------------------------------------------------------------

    @router.get("/stats")
    async def graph_stats():
        """Basic graph statistics (node/edge counts by type and predicate)."""
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        async with pool.acquire() as conn:
            node_count = await conn.fetchval(
                "SELECT COUNT(*) FROM entity_registry"
            )
            edge_count = await conn.fetchval(
                "SELECT COUNT(*) FROM entity_relationships"
            )
            type_counts = await conn.fetch("""
                SELECT entity_type, COUNT(*) AS count
                FROM entity_registry
                GROUP BY entity_type
                ORDER BY count DESC
            """)
            predicate_counts = await conn.fetch("""
                SELECT predicate, COUNT(*) AS count
                FROM entity_relationships
                GROUP BY predicate
                ORDER BY count DESC
            """)

        return {
            "node_count": node_count,
            "edge_count": edge_count,
            "types": {r["entity_type"]: r["count"] for r in type_counts},
            "predicates": {r["predicate"]: r["count"] for r in predicate_counts},
        }

    # -- Temporal: assertion history -----------------------------------------

    @router.get(
        "/history/{entity_uri:path}",
        response_model=AssertionHistoryResponse,
    )
    async def assertion_history(
        entity_uri: str,
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
        active_only: bool = Query(
            False, description="If true, exclude retracted assertions"
        ),
    ):
        """Return the assertion history for a given entity.

        Queries the ``assertion_history`` table, returning assertions where
        the entity appears as subject.  Results ordered by ``tx_recorded_at``
        descending (most recent first).
        """
        if not caps.assertion_history:
            raise HTTPException(
                status_code=501,
                detail="Assertion history not enabled for this deployment",
            )
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        async with pool.acquire() as conn:
            where = "WHERE subject = $1"
            params: list = [entity_uri]
            idx = 2

            if active_only:
                where += " AND tx_retracted_at IS NULL"

            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM assertion_history {where}", *params,
            )

            rows = await conn.fetch(
                f"""
                SELECT assertion_id, subject, predicate,
                       object_uri, object_literal,
                       asserted_by_node_rid,
                       tx_recorded_at, tx_retracted_at,
                       valid_from, valid_to,
                       supersedes_assertion_id, provenance_doc_rid
                FROM assertion_history
                {where}
                ORDER BY tx_recorded_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}
                """,
                *params, limit, offset,
            )

        assertions = [
            AssertionRecord(
                assertion_id=str(r["assertion_id"]),
                subject=r["subject"],
                predicate=r["predicate"],
                object_uri=r["object_uri"],
                object_literal=r["object_literal"],
                asserted_by_node_rid=r["asserted_by_node_rid"],
                tx_recorded_at=r["tx_recorded_at"],
                tx_retracted_at=r["tx_retracted_at"],
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                supersedes_assertion_id=(
                    str(r["supersedes_assertion_id"])
                    if r["supersedes_assertion_id"]
                    else None
                ),
                provenance_doc_rid=r["provenance_doc_rid"],
            )
            for r in rows
        ]

        return AssertionHistoryResponse(
            entity_uri=entity_uri, assertions=assertions, total=count,
        )

    # -- Temporal: timeline range query --------------------------------------

    @router.get("/timeline", response_model=TimelineResponse)
    async def timeline(
        start: Optional[str] = Query(
            None, description="ISO-8601 start of valid-time range"
        ),
        end: Optional[str] = Query(
            None, description="ISO-8601 end of valid-time range"
        ),
        entity_type: Optional[str] = Query(
            None, description="Filter by entity type (joins entity_registry)"
        ),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        """Query assertions within a valid-time range.

        Returns assertions whose ``valid_from`` / ``valid_to`` window
        overlaps with the requested ``[start, end]`` range.  Optionally
        filter by entity type (requires join to ``entity_registry``).
        """
        if not caps.assertion_history:
            raise HTTPException(
                status_code=501,
                detail="Assertion history not enabled for this deployment",
            )
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        conditions: list[str] = ["tx_retracted_at IS NULL"]
        params: list = []
        idx = 1

        if start:
            conditions.append(
                f"(valid_to IS NULL OR valid_to >= ${idx}::timestamptz)"
            )
            params.append(start)
            idx += 1

        if end:
            conditions.append(
                f"(valid_from IS NULL OR valid_from <= ${idx}::timestamptz)"
            )
            params.append(end)
            idx += 1

        join_clause = ""
        if entity_type:
            join_clause = (
                "JOIN entity_registry er "
                "ON ah.subject = er.fuseki_uri"
            )
            conditions.append(f"er.entity_type = ${idx}")
            params.append(entity_type)
            idx += 1

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM assertion_history ah {join_clause} {where}",
                *params,
            )

            rows = await conn.fetch(
                f"""
                SELECT ah.assertion_id, ah.subject, ah.predicate,
                       ah.object_uri, ah.object_literal,
                       ah.valid_from, ah.valid_to, ah.tx_recorded_at
                FROM assertion_history ah
                {join_clause}
                {where}
                ORDER BY ah.valid_from ASC NULLS LAST, ah.tx_recorded_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}
                """,
                *params, limit, offset,
            )

        entries = [
            TimelineEntry(
                assertion_id=str(r["assertion_id"]),
                subject=r["subject"],
                predicate=r["predicate"],
                object_uri=r["object_uri"],
                object_literal=r["object_literal"],
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                tx_recorded_at=r["tx_recorded_at"],
            )
            for r in rows
        ]

        return TimelineResponse(
            start=start, end=end, entity_type=entity_type,
            entries=entries, total=count,
        )

    return router


def create_temporal_router(pool, caps):
    """Return an APIRouter with ONLY the temporal endpoints.

    Use this when the core graph endpoints (/neighborhood, /shortest-path, /stats)
    are already defined inline on the main app. This avoids duplicate route
    registration while still adding the new /history and /timeline endpoints.
    """
    router = APIRouter(prefix="/graph", tags=["graph-temporal"])

    @router.get(
        "/history/{entity_uri:path}",
        response_model=AssertionHistoryResponse,
    )
    async def assertion_history(
        entity_uri: str,
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
        active_only: bool = Query(
            False, description="If true, exclude retracted assertions"
        ),
    ):
        """Return the assertion history for a given entity."""
        if not caps.assertion_history:
            raise HTTPException(
                status_code=501,
                detail="Assertion history not enabled for this deployment",
            )
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        async with pool.acquire() as conn:
            where = "WHERE subject = $1"
            params: list = [entity_uri]
            idx = 2

            if active_only:
                where += " AND tx_retracted_at IS NULL"

            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM assertion_history {where}", *params,
            )

            rows = await conn.fetch(
                f"""
                SELECT assertion_id, subject, predicate,
                       object_uri, object_literal,
                       asserted_by_node_rid,
                       tx_recorded_at, tx_retracted_at,
                       valid_from, valid_to,
                       supersedes_assertion_id, provenance_doc_rid
                FROM assertion_history
                {where}
                ORDER BY tx_recorded_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}
                """,
                *params, limit, offset,
            )

        assertions = [
            AssertionRecord(
                assertion_id=str(r["assertion_id"]),
                subject=r["subject"],
                predicate=r["predicate"],
                object_uri=r["object_uri"],
                object_literal=r["object_literal"],
                asserted_by_node_rid=r["asserted_by_node_rid"],
                tx_recorded_at=r["tx_recorded_at"],
                tx_retracted_at=r["tx_retracted_at"],
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                supersedes_assertion_id=(
                    str(r["supersedes_assertion_id"])
                    if r["supersedes_assertion_id"]
                    else None
                ),
                provenance_doc_rid=r["provenance_doc_rid"],
            )
            for r in rows
        ]

        return AssertionHistoryResponse(
            entity_uri=entity_uri, assertions=assertions, total=count,
        )

    @router.get("/timeline", response_model=TimelineResponse)
    async def timeline(
        start: Optional[str] = Query(
            None, description="ISO-8601 start of valid-time range"
        ),
        end: Optional[str] = Query(
            None, description="ISO-8601 end of valid-time range"
        ),
        entity_type: Optional[str] = Query(
            None, description="Filter by entity type (joins entity_registry)"
        ),
        limit: int = Query(100, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ):
        """Query assertions within a valid-time range."""
        if not caps.assertion_history:
            raise HTTPException(
                status_code=501,
                detail="Assertion history not enabled for this deployment",
            )
        if not pool:
            raise HTTPException(status_code=503, detail="Database not available")

        conditions: list[str] = ["tx_retracted_at IS NULL"]
        params: list = []
        idx = 1

        if start:
            conditions.append(
                f"(valid_to IS NULL OR valid_to >= ${idx}::timestamptz)"
            )
            params.append(start)
            idx += 1

        if end:
            conditions.append(
                f"(valid_from IS NULL OR valid_from <= ${idx}::timestamptz)"
            )
            params.append(end)
            idx += 1

        join_clause = ""
        if entity_type:
            join_clause = (
                "JOIN entity_registry er "
                "ON ah.subject = er.fuseki_uri"
            )
            conditions.append(f"er.entity_type = ${idx}")
            params.append(entity_type)
            idx += 1

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        async with pool.acquire() as conn:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM assertion_history ah {join_clause} {where}",
                *params,
            )

            rows = await conn.fetch(
                f"""
                SELECT ah.assertion_id, ah.subject, ah.predicate,
                       ah.object_uri, ah.object_literal,
                       ah.valid_from, ah.valid_to, ah.tx_recorded_at
                FROM assertion_history ah
                {join_clause}
                {where}
                ORDER BY ah.valid_from ASC NULLS LAST, ah.tx_recorded_at DESC
                LIMIT ${idx} OFFSET ${idx + 1}
                """,
                *params, limit, offset,
            )

        entries = [
            TimelineEntry(
                assertion_id=str(r["assertion_id"]),
                subject=r["subject"],
                predicate=r["predicate"],
                object_uri=r["object_uri"],
                object_literal=r["object_literal"],
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                tx_recorded_at=r["tx_recorded_at"],
            )
            for r in rows
        ]

        return TimelineResponse(
            start=start, end=end, entity_type=entity_type,
            entries=entries, total=count,
        )

    return router
