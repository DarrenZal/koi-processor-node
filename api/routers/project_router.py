"""Project briefing endpoint.

Assembles project context from the knowledge graph: spec hierarchy, open tasks,
and recent sessions. Designed for directory-agnostic operation — any agent from
any directory or channel can query this.

Routes are prefix-relative — prefix "/project" is applied at mount.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class SpecNode(BaseModel):
    doc_id: str
    doc_kind: str
    uri: str
    status: Optional[str] = None
    file_path: Optional[str] = None
    depends_on: List[str] = []
    primary_for: List[str] = []
    external: bool = False


class SpecHierarchy(BaseModel):
    root: Optional[SpecNode] = None
    nodes: List[SpecNode] = []
    edges: List[Dict[str, str]] = []


class TaskSummary(BaseModel):
    id: int
    title: str
    status: str
    priority: Optional[str] = None
    due_date: Optional[str] = None
    owner_uri: Optional[str] = None


class ProjectBriefing(BaseModel):
    project: Dict[str, Any]
    spec_hierarchy: Optional[SpecHierarchy] = None
    active_tasks: List[TaskSummary] = []
    recent_sessions: Optional[List[Dict[str, Any]]] = None
    external_dependencies: Optional[List[Dict[str, Any]]] = None


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_router(pool, caps=None) -> APIRouter:
    router = APIRouter(tags=["project"])

    @router.get("/projects")
    async def list_projects():
        """List all governed projects (those with a SpecDoc governs edge)."""
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT e.fuseki_uri, e.entity_text, e.metadata
                FROM entity_registry e
                JOIN entity_relationships r ON r.object_uri = e.fuseki_uri
                WHERE e.entity_type = 'Project'
                  AND r.predicate = 'governs'
                  AND r.subject_uri LIKE 'spec:%'
                ORDER BY e.entity_text
            """)
            seen = set()
            projects = []
            for row in rows:
                uri = row["fuseki_uri"]
                if uri in seen:
                    continue
                seen.add(uri)
                meta = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else (row["metadata"] or {})
                projects.append({
                    "name": row["entity_text"],
                    "uri": uri,
                    "project_id": meta.get("project_id"),
                    "tier": meta.get("tier", 0),
                    "docs_root": meta.get("docs_root"),
                    "repos": meta.get("repos", []),
                })
            return projects

    @router.get("/briefing", response_model=ProjectBriefing)
    async def project_briefing(
        project: str = Query(..., description="Project name or entity URI"),
        include_external_deps: bool = Query(False, description="Include cross-project dependencies"),
    ):
        """Assemble a project context briefing from the knowledge graph."""
        async with pool.acquire() as conn:
            # Step 1: Resolve project entity (read-only, no create-on-miss)
            project_entity = await _resolve_project(conn, project)
            if project_entity is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Project '{project}' not found in knowledge graph. "
                           f"Register it first or check the name."
                )

            project_uri = project_entity["fuseki_uri"]
            project_name = project_entity["entity_text"]
            metadata = project_entity.get("metadata") or {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            tier = metadata.get("tier", 0)

            # Step 2: Find spec DAG root(s) via governs predicate
            spec_hierarchy = await _build_spec_hierarchy(conn, project_uri, include_external=include_external_deps)

            # Step 3: Get active tasks
            active_tasks = await _get_active_tasks(conn, project_uri)

            # Step 4: Attempt session search (graceful degradation)
            recent_sessions = None  # Deferred — session search endpoint may 404

            # Step 5: Cross-project external dependencies
            external_dependencies = None
            if include_external_deps and spec_hierarchy:
                prefix = spec_hierarchy.root.doc_id.split(".", 1)[0] if spec_hierarchy.root else None
                if prefix:
                    external_dependencies = await _get_external_dependencies(conn, prefix)

            return ProjectBriefing(
                project={
                    "name": project_name,
                    "uri": project_uri,
                    "tier": tier,
                    "project_id": metadata.get("project_id"),
                    "docs_root": metadata.get("docs_root"),
                },
                spec_hierarchy=spec_hierarchy,
                active_tasks=active_tasks,
                recent_sessions=recent_sessions,
                external_dependencies=external_dependencies,
            )

    return router


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _resolve_project(conn, project_query: str) -> Optional[dict]:
    """Read-only project resolution. Does NOT create entities on miss.

    Tries in order:
    1. Exact URI match
    2. Exact normalized name match (type=Project)
    3. Case-insensitive LIKE match (type=Project)
    """
    # Try exact URI
    row = await conn.fetchrow(
        "SELECT fuseki_uri, entity_text, metadata FROM entity_registry "
        "WHERE fuseki_uri = $1 AND entity_type = 'Project'",
        project_query,
    )
    if row:
        return dict(row)

    # Try exact normalized name
    normalized = project_query.lower().strip().replace("_", " ").replace("-", " ")
    row = await conn.fetchrow(
        "SELECT fuseki_uri, entity_text, metadata FROM entity_registry "
        "WHERE entity_type = 'Project' AND normalized_text = $1",
        normalized,
    )
    if row:
        return dict(row)

    # Try case-insensitive partial match (for short names like "BKC")
    row = await conn.fetchrow(
        "SELECT fuseki_uri, entity_text, metadata FROM entity_registry "
        "WHERE entity_type = 'Project' AND "
        "(UPPER(entity_text) = UPPER($1) OR metadata->>'project_id' = LOWER($1))",
        project_query,
    )
    if row:
        return dict(row)

    return None


async def _build_spec_hierarchy(conn, project_uri: str, include_external: bool = False) -> Optional[SpecHierarchy]:
    """Build the spec hierarchy by walking governs + incoming depends_on edges."""
    # Find spec DAG root(s) — SpecDoc entities that govern this project
    roots = await conn.fetch(
        "SELECT subject_uri FROM entity_relationships "
        "WHERE predicate = 'governs' AND object_uri = $1",
        project_uri,
    )
    if not roots:
        return None

    root_uri = roots[0]["subject_uri"]

    # Get all SpecDoc entities for this project (by URI prefix)
    # Extract project prefix from root URI: spec:bkc.project-vision -> bkc
    prefix_parts = root_uri.replace("spec:", "").split(".", 1)
    project_prefix = prefix_parts[0] if prefix_parts else ""
    uri_pattern = f"spec:{project_prefix}.%"

    spec_rows = await conn.fetch(
        "SELECT fuseki_uri, entity_text, metadata FROM entity_registry "
        "WHERE entity_type = 'SpecDoc' AND fuseki_uri LIKE $1 "
        "ORDER BY fuseki_uri",
        uri_pattern,
    )

    if not spec_rows:
        return None

    # Build nodes
    nodes = []
    root_node = None
    for row in spec_rows:
        meta = row["metadata"] or {}
        if isinstance(meta, str):
            meta = json.loads(meta)

        doc_id = row["fuseki_uri"].replace("spec:", "")
        node = SpecNode(
            doc_id=doc_id,
            doc_kind=meta.get("doc_kind", "unknown"),
            uri=row["fuseki_uri"],
            status=meta.get("status"),
            file_path=meta.get("file_path"),
            depends_on=meta.get("depends_on", []),
            primary_for=meta.get("primary_for", []),
        )
        nodes.append(node)
        if row["fuseki_uri"] == root_uri:
            root_node = node

    # Get edges (depends_on between spec docs)
    if include_external:
        edges = await conn.fetch(
            "SELECT subject_uri, object_uri FROM entity_relationships "
            "WHERE predicate = 'depends_on' AND subject_uri LIKE $1 AND object_uri LIKE 'spec:%'",
            uri_pattern,
        )
    else:
        edges = await conn.fetch(
            "SELECT subject_uri, object_uri FROM entity_relationships "
            "WHERE predicate = 'depends_on' AND subject_uri LIKE $1 AND object_uri LIKE $1",
            uri_pattern,
        )

    if include_external:
        local_uris = {n.uri for n in nodes}
        external_uris = {e["object_uri"] for e in edges if e["object_uri"] not in local_uris}
        if external_uris:
            ext_rows = await conn.fetch(
                "SELECT fuseki_uri, entity_text, metadata FROM entity_registry "
                "WHERE fuseki_uri = ANY($1::text[])",
                list(external_uris),
            )
            for row in ext_rows:
                meta = row["metadata"] or {}
                if isinstance(meta, str):
                    meta = json.loads(meta)
                nodes.append(SpecNode(
                    doc_id=row["fuseki_uri"].replace("spec:", ""),
                    doc_kind=meta.get("doc_kind", "unknown"),
                    uri=row["fuseki_uri"],
                    status=meta.get("status"),
                    file_path=meta.get("file_path"),
                    depends_on=meta.get("depends_on", []),
                    primary_for=meta.get("primary_for", []),
                    external=True,
                ))

    edge_list = [
        {"from": e["subject_uri"].replace("spec:", ""),
         "to": e["object_uri"].replace("spec:", ""),
         "type": "depends_on"}
        for e in edges
    ]

    return SpecHierarchy(
        root=root_node,
        nodes=nodes,
        edges=edge_list,
    )


async def _get_active_tasks(conn, project_uri: str) -> List[TaskSummary]:
    """Get open tasks for a project."""
    rows = await conn.fetch(
        "SELECT id, title, status, priority, due_date, owner_uri "
        "FROM task_registry "
        "WHERE project_uri = $1 AND status NOT IN ('done', 'cancelled') "
        "ORDER BY CASE WHEN due_date IS NOT NULL THEN 0 ELSE 1 END, due_date, id",
        project_uri,
    )
    return [
        TaskSummary(
            id=r["id"],
            title=r["title"],
            status=r["status"],
            priority=r.get("priority"),
            due_date=str(r["due_date"]) if r.get("due_date") else None,
            owner_uri=r.get("owner_uri"),
        )
        for r in rows
    ]


async def _get_external_dependencies(conn, project_prefix: str) -> List[Dict[str, Any]]:
    """Find depends_on edges from this project's specs to other projects' specs."""
    local_pattern = f"spec:{project_prefix}.%"
    rows = await conn.fetch(
        "SELECT r.subject_uri, r.object_uri, e.metadata "
        "FROM entity_relationships r "
        "JOIN entity_registry e ON e.fuseki_uri = r.object_uri "
        "WHERE r.predicate = 'depends_on' "
        "  AND r.subject_uri LIKE $1 "
        "  AND r.object_uri LIKE 'spec:%' "
        "  AND r.object_uri NOT LIKE $1",
        local_pattern,
    )
    deps = []
    for row in rows:
        meta = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else (row["metadata"] or {})
        target_doc_id = row["object_uri"].replace("spec:", "")
        source_project = target_doc_id.split(".", 1)[0] if "." in target_doc_id else None
        deps.append({
            "doc_id": target_doc_id,
            "doc_kind": meta.get("doc_kind"),
            "uri": row["object_uri"],
            "source_project": source_project,
            "referenced_by": row["subject_uri"].replace("spec:", ""),
        })
    return deps
