"""Network coordinator endpoints (nodes, entities, health).

Coordinator-only aggregation endpoints that query across federated KOI-net
nodes.  Only included when caps.coordinator_endpoints is True.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field


# -- Response models ---------------------------------------------------------

class NodeInfo(BaseModel):
    """Summary of a federated KOI-net node."""
    node_rid: str
    name: Optional[str] = None
    endpoint_url: Optional[str] = None
    last_seen_at: Optional[str] = None
    entity_count: int = 0
    status: str = "unknown"  # "healthy", "degraded", "unreachable", "unknown"


class NetworkNodesResponse(BaseModel):
    """List of all known nodes in the federation."""
    nodes: List[NodeInfo]
    total: int


class NetworkEntitySummary(BaseModel):
    """Cross-node entity summary (aggregated view)."""
    entity_uri: str
    entity_type: Optional[str] = None
    name: Optional[str] = None
    present_on_nodes: List[str] = []


class NetworkEntitiesResponse(BaseModel):
    """Aggregated entity listing across nodes."""
    entities: List[NetworkEntitySummary]
    total: int


class NetworkHealthResponse(BaseModel):
    """Aggregate health of the federated network."""
    total_nodes: int = 0
    healthy_nodes: int = 0
    degraded_nodes: int = 0
    unreachable_nodes: int = 0
    total_entities_across_network: int = 0
    coordinator_node_rid: Optional[str] = None


# -- Router factory ----------------------------------------------------------

def create_router(pool, caps):
    """Return an APIRouter for network coordinator endpoints.

    Only included when caps.coordinator_endpoints is True.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities (coordinator_endpoints flag).
    """
    router = APIRouter(prefix="/network", tags=["network"])

    @router.get("/nodes", response_model=NetworkNodesResponse)
    async def network_nodes(
        status: Optional[str] = Query(
            None, description="Filter by node status"
        ),
    ):
        """List all known nodes in the KOI-net federation.

        Returns node RIDs, endpoint URLs, last-seen timestamps, and
        entity counts.  As coordinator, this node periodically polls
        peer health and caches the results.
        """
        # TODO: Query koi_net_peers table and aggregate node info
        # Each peer stores: node_rid, endpoint_url, last_seen, public_key
        raise HTTPException(
            status_code=501,
            detail="Network node listing not yet implemented",
        )

    @router.get("/entities", response_model=NetworkEntitiesResponse)
    async def network_entities(
        entity_type: Optional[str] = Query(
            None, description="Filter by entity type"
        ),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        """Aggregated entity listing across all federated nodes.

        Shows which entities exist on which nodes, enabling the coordinator
        to provide a unified view of the distributed knowledge graph.
        """
        # TODO: Query entity_registry with source_node_rid to determine
        # which entities were received from which peers
        raise HTTPException(
            status_code=501,
            detail="Network entity aggregation not yet implemented",
        )

    @router.get("/health", response_model=NetworkHealthResponse)
    async def network_health():
        """Aggregate health summary of the federated network.

        Returns counts of healthy, degraded, and unreachable nodes along
        with total entity counts across the network.
        """
        # TODO: Aggregate health from cached peer status
        return NetworkHealthResponse(
            total_nodes=0,
            healthy_nodes=0,
            degraded_nodes=0,
            unreachable_nodes=0,
            total_entities_across_network=0,
            coordinator_node_rid=None,
        )

    return router
