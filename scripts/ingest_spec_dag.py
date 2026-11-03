#!/usr/bin/env python3
"""
Spec DAG Ingest Script

Reads a project's doc DAG (markdown files with YAML frontmatter) and upserts
SpecDoc entities + depends_on/governs relationships into the KOI knowledge graph.

Usage:
    python scripts/ingest_spec_dag.py --project-config /path/to/_meta/project.json --dry-run
    python scripts/ingest_spec_dag.py --project-config /path/to/_meta/project.json --apply
    python scripts/ingest_spec_dag.py --project myproject --docs-root /path/to/docs/ --apply
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

SPEC_SOURCE = "spec-dag-ingest"

VALID_DOC_KINDS = frozenset(
    ["vision", "foundation", "architecture", "spec", "operations", "research", "positioning", "pattern", "roadmap"]
)


@dataclass
class DocNode:
    doc_id: str
    doc_kind: str
    status: str
    depends_on: list[str]
    relates_to: list[str]
    file_path: str
    primary_for: list[str] = field(default_factory=list)
    research_subkind: str = ""


def parse_frontmatter(path: Path, docs_root: Path) -> Optional[dict[str, Any]]:
    """Extract YAML frontmatter from a markdown file, or return None."""
    try:
        import yaml
    except ImportError:
        log.error("PyYAML required: pip install pyyaml")
        sys.exit(1)

    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    block = text[3:end].strip()
    try:
        data = yaml.safe_load(block)
    except Exception:
        return None
    if not isinstance(data, dict) or "doc_id" not in data:
        return None
    try:
        data["_file_path"] = str(path.relative_to(docs_root))
    except ValueError:
        data["_file_path"] = str(path)
    return data


def collect_docs(docs_root: Path) -> tuple[dict[str, DocNode], list[str]]:
    """Scan docs_root for frontmattered markdown files. Returns (nodes, unclassified)."""
    nodes: dict[str, DocNode] = {}
    unclassified: list[str] = []

    for md_path in sorted(docs_root.rglob("*.md")):
        # Skip _meta directory and hidden files
        rel = md_path.relative_to(docs_root)
        if any(part.startswith("_") or part.startswith(".") for part in rel.parts):
            continue

        data = parse_frontmatter(md_path, docs_root)
        if data is None:
            unclassified.append(str(rel))
            continue

        doc_id = data["doc_id"]
        if doc_id in nodes:
            log.error(f"Duplicate doc_id '{doc_id}' in {data['_file_path']} and {nodes[doc_id].file_path}")
            sys.exit(1)

        nodes[doc_id] = DocNode(
            doc_id=doc_id,
            doc_kind=data.get("doc_kind", ""),
            status=data.get("status", "draft"),
            depends_on=data.get("depends_on", []),
            relates_to=data.get("relates_to", []),
            file_path=data["_file_path"],
            primary_for=data.get("primary_for", []),
            research_subkind=data.get("research_subkind", ""),
        )

    return nodes, unclassified


def validate_doc_dag(nodes: dict[str, DocNode], project_id: str) -> tuple[list[str], list[tuple[str, str]]]:
    """Validate the doc DAG. Returns (errors, external_refs).

    Phase A (local, pure): prefix checks, vision root, doc_kind, cycle detection.
    External depends_on targets (different project prefix) are collected into
    external_refs for Phase B validation against the DB.
    """
    errors: list[str] = []
    external_refs: list[tuple[str, str]] = []  # (doc_id, dep) tuples

    # Check doc_id prefix matches project_id
    for doc_id in nodes:
        if not doc_id.startswith(f"{project_id}."):
            errors.append(
                f"Doc '{doc_id}' does not start with project prefix '{project_id}.'"
            )

    # Check single vision root
    vision_roots = [
        doc_id for doc_id, node in nodes.items()
        if node.doc_kind == "vision" and not node.depends_on
    ]
    if len(vision_roots) == 0:
        errors.append("No vision root found (need exactly one doc with doc_kind=vision and depends_on=[])")
    elif len(vision_roots) > 1:
        errors.append(f"Multiple vision roots found: {vision_roots}")

    # Check doc_kind values
    for doc_id, node in nodes.items():
        if node.doc_kind not in VALID_DOC_KINDS:
            errors.append(
                f"Doc {doc_id}: invalid doc_kind '{node.doc_kind}' "
                f"(must be one of {sorted(VALID_DOC_KINDS)})"
            )

    # Check depends_on targets exist (local or external)
    for doc_id, node in nodes.items():
        for dep in node.depends_on:
            if dep not in nodes:
                dep_prefix = dep.split(".", 1)[0] if "." in dep else ""
                if dep_prefix and dep_prefix != project_id:
                    external_refs.append((doc_id, dep))
                else:
                    errors.append(f"Doc {doc_id}: depends_on target '{dep}' not found")

    # Cycle detection (DFS) — only on local edges
    graph: dict[str, list[str]] = defaultdict(list)
    for doc_id, node in nodes.items():
        for dep in node.depends_on:
            if dep in nodes:
                graph[doc_id].append(dep)

    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(nid: str) -> None:
        if nid in visited:
            return
        if nid in visiting:
            errors.append(f"Cycle detected in doc depends_on graph at: {nid}")
            return
        visiting.add(nid)
        for nxt in graph.get(nid, []):
            dfs(nxt)
        visiting.discard(nid)
        visited.add(nid)

    for nid in nodes:
        dfs(nid)

    return errors, external_refs


def validate_external_refs(external_refs: list[tuple[str, str]], args) -> list[str]:
    """Validate that external depends_on targets exist as SpecDoc entities in the DB."""
    errors = []
    conn = get_db_connection(args)
    cur = conn.cursor()
    try:
        for doc_id, dep in external_refs:
            cur.execute(
                "SELECT 1 FROM entity_registry WHERE entity_type = 'SpecDoc' "
                "AND fuseki_uri = %s", (f"spec:{dep}",)
            )
            if not cur.fetchone():
                errors.append(f"Doc {doc_id}: external depends_on target '{dep}' "
                              f"not found in knowledge graph (spec:{dep})")
            else:
                log.info(f"  Cross-project ref: {doc_id} --> {dep} (verified)")
    finally:
        conn.close()
    return errors


def spec_uri(doc_id: str) -> str:
    """Generate entity URI for a spec doc."""
    return f"spec:{doc_id}"


def normalize_text(text: str) -> str:
    """Normalize entity text for DB storage."""
    return text.lower().strip().replace("_", " ").replace("-", " ").replace("  ", " ")


def get_db_connection(args):
    """Create a psycopg2 connection."""
    import psycopg2
    return psycopg2.connect(
        host=args.host or os.getenv("POSTGRES_HOST", "localhost"),
        port=args.port or int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=args.db or os.getenv("POSTGRES_DB", "personal_koi"),
        user=args.user or os.getenv("POSTGRES_USER", ""),
        password=args.password or os.getenv("POSTGRES_PASSWORD", ""),
    )


def upsert_entity(cur, uri: str, entity_type: str, name: str, metadata: dict) -> bool:
    """Upsert an entity into entity_registry."""
    normalized = normalize_text(name)
    metadata_json = json.dumps(metadata)
    try:
        cur.execute("""
            INSERT INTO entity_registry (
                fuseki_uri, entity_text, entity_type, normalized_text,
                source, first_seen_rid, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (fuseki_uri) DO UPDATE SET
                entity_text = EXCLUDED.entity_text,
                entity_type = EXCLUDED.entity_type,
                normalized_text = EXCLUDED.normalized_text,
                metadata = EXCLUDED.metadata
        """, (uri, name, entity_type, normalized, SPEC_SOURCE,
              f"spec-dag:{metadata.get('doc_kind', 'unknown')}", metadata_json))
        return True
    except Exception as e:
        log.error(f"  Failed to upsert {uri}: {e}")
        return False


def create_relationship(cur, subject_uri: str, predicate: str, object_uri: str) -> bool:
    """Create a relationship in entity_relationships."""
    if subject_uri == object_uri:
        log.warning(f"  Skipping self-ref: {subject_uri} --{predicate}--> {object_uri}")
        return False
    try:
        cur.execute("""
            INSERT INTO entity_relationships (subject_uri, predicate, object_uri, source)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (subject_uri, predicate, object_uri) DO NOTHING
        """, (subject_uri, predicate, object_uri, SPEC_SOURCE))
        return True
    except Exception as e:
        log.warning(f"  Failed to create rel {subject_uri} --{predicate}--> {object_uri}: {e}")
        return False


def _project_metadata(project_config: dict) -> dict:
    """Build metadata dict from project config (all fields, not just tier)."""
    meta = {"tier": project_config.get("tier", 0)}
    for key in ("project_id", "project_name", "repos", "docs_root",
                "roadmap_path", "code_surfaces_path"):
        if key in project_config:
            meta[key] = project_config[key]
    return meta


def resolve_or_create_project(cur, project_config: dict) -> str:
    """Find or create the Project entity, return its URI."""
    project_uri = project_config["project_uri"]
    project_name = project_config["project_name"]
    meta = _project_metadata(project_config)
    normalized = normalize_text(project_name)

    # Check if it exists
    cur.execute("SELECT fuseki_uri FROM entity_registry WHERE fuseki_uri = %s", (project_uri,))
    row = cur.fetchone()
    if row:
        # Merge all project.json fields into existing metadata + sync display name
        cur.execute("""
            UPDATE entity_registry
            SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                entity_text = %s,
                normalized_text = %s
            WHERE fuseki_uri = %s
        """, (json.dumps(meta), project_name, normalized, project_uri))
        log.info(f"  Found existing Project entity: {project_uri} (updated name={project_name!r}, metadata: {list(meta.keys())})")
        return project_uri

    # Try to find by name
    cur.execute("""
        SELECT fuseki_uri FROM entity_registry
        WHERE entity_type = 'Project' AND normalized_text = %s
    """, (normalized,))
    row = cur.fetchone()
    if row:
        existing_uri = row[0]
        cur.execute("""
            UPDATE entity_registry
            SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                entity_text = %s,
                normalized_text = %s
            WHERE fuseki_uri = %s
        """, (json.dumps(meta), project_name, normalized, existing_uri))
        log.info(f"  Found Project by name: {existing_uri} (updated name={project_name!r}, metadata: {list(meta.keys())})")
        return existing_uri

    # Create new
    upsert_entity(cur, project_uri, "Project", project_name, meta)
    log.info(f"  Created new Project entity: {project_uri}")
    return project_uri


def detect_stale(cur, project_id: str, current_uris: set[str]) -> list[str]:
    """Find SpecDoc entities in KOI that are no longer in the DAG."""
    prefix = f"spec:{project_id}.%"
    cur.execute("""
        SELECT fuseki_uri FROM entity_registry
        WHERE entity_type = 'SpecDoc' AND fuseki_uri LIKE %s
    """, (prefix,))
    db_uris = {row[0] for row in cur.fetchall()}
    return sorted(db_uris - current_uris)


def run_ingest(project_config: dict, nodes: dict[str, DocNode], dry_run: bool, args) -> dict:
    """Ingest spec DAG into KOI. Returns stats dict."""
    stats = {
        "entities_upserted": 0, "entities_failed": 0,
        "rels_created": 0, "rels_failed": 0,
        "stale_found": 0,
    }

    project_id = project_config["project_id"]
    current_uris = {spec_uri(doc_id) for doc_id in nodes}

    # Find vision root
    vision_roots = [
        doc_id for doc_id, node in nodes.items()
        if node.doc_kind == "vision" and not node.depends_on
    ]
    vision_root_id = vision_roots[0] if vision_roots else None

    if dry_run:
        log.info("=== DRY RUN — no DB changes ===")
        for doc_id, node in sorted(nodes.items()):
            uri = spec_uri(doc_id)
            log.info(f"  [DRY RUN] Would upsert {uri} ({node.doc_kind}): {node.file_path}")
            stats["entities_upserted"] += 1

        for doc_id, node in sorted(nodes.items()):
            for dep in node.depends_on:
                log.info(f"  [DRY RUN] Would create: {spec_uri(doc_id)} --depends_on--> {spec_uri(dep)}")
                stats["rels_created"] += 1

        for doc_id, node in sorted(nodes.items()):
            for rel in node.relates_to:
                log.info(f"  [DRY RUN] Would create: {spec_uri(doc_id)} --related_to--> {spec_uri(rel)}")
                stats["rels_created"] += 1

        if vision_root_id:
            log.info(f"  [DRY RUN] Would create: {spec_uri(vision_root_id)} --governs--> {project_config['project_uri']}")
            stats["rels_created"] += 1

        log.info(f"\n  Would upsert {stats['entities_upserted']} entities, "
                 f"{stats['rels_created']} relationships")
        return stats

    # Apply mode
    import psycopg2
    conn = get_db_connection(args)
    try:
        cur = conn.cursor()

        # Resolve or create project entity
        project_uri = resolve_or_create_project(cur, project_config)

        # Upsert SpecDoc entities
        for doc_id, node in sorted(nodes.items()):
            uri = spec_uri(doc_id)
            metadata = {
                "doc_kind": node.doc_kind,
                "status": node.status,
                "file_path": node.file_path,
                "depends_on": node.depends_on,
                "primary_for": node.primary_for,
                "project_id": project_id,
                "research_subkind": node.research_subkind,
            }
            if upsert_entity(cur, uri, "SpecDoc", doc_id, metadata):
                stats["entities_upserted"] += 1
                log.info(f"  Upserted {uri} ({node.doc_kind})")
            else:
                stats["entities_failed"] += 1

        # Create depends_on relationships
        for doc_id, node in sorted(nodes.items()):
            for dep in node.depends_on:
                if create_relationship(cur, spec_uri(doc_id), "depends_on", spec_uri(dep)):
                    stats["rels_created"] += 1
                else:
                    stats["rels_failed"] += 1

        # Create related_to relationships from relates_to: frontmatter
        for doc_id, node in sorted(nodes.items()):
            for rel in node.relates_to:
                target_uri = spec_uri(rel)
                cur.execute(
                    "SELECT 1 FROM entity_registry WHERE fuseki_uri = %s", (target_uri,)
                )
                if not cur.fetchone():
                    log.warning(f"  relates_to target '{rel}' not found in KOI, skipping edge")
                    continue
                if create_relationship(cur, spec_uri(doc_id), "related_to", target_uri):
                    stats["rels_created"] += 1
                    log.info(f"  Created related_to: {spec_uri(doc_id)} --> {target_uri}")
                else:
                    stats["rels_failed"] += 1

        # Create governs relationship
        if vision_root_id:
            if create_relationship(cur, spec_uri(vision_root_id), "governs", project_uri):
                stats["rels_created"] += 1
                log.info(f"  Created governs: {spec_uri(vision_root_id)} --> {project_uri}")
            else:
                stats["rels_failed"] += 1

        # Detect stale entities
        stale = detect_stale(cur, project_id, current_uris)
        if stale:
            stats["stale_found"] = len(stale)
            log.warning(f"  Found {len(stale)} stale SpecDoc entities (not deleting):")
            for uri in stale:
                log.warning(f"    {uri}")

        conn.commit()
        log.info(f"\n  Upserted {stats['entities_upserted']} entities, "
                 f"created {stats['rels_created']} relationships"
                 f" ({stats['entities_failed']} entity failures, "
                 f"{stats['rels_failed']} rel failures)")

    except Exception as e:
        conn.rollback()
        log.error(f"Ingest failed, rolled back: {e}")
        raise
    finally:
        conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="Ingest spec DAG into KOI knowledge graph")
    parser.add_argument("--project-config", help="Path to _meta/project.json")
    parser.add_argument("--project", help="Project ID (e.g., bkc)")
    parser.add_argument("--docs-root", help="Path to docs directory")
    parser.add_argument("--dry-run", action="store_true", help="Validate and show what would be done")
    parser.add_argument("--apply", action="store_true", help="Actually write to DB")
    parser.add_argument("--host", default=None, help="DB host")
    parser.add_argument("--port", default=None, type=int, help="DB port")
    parser.add_argument("--db", default=None, help="DB name")
    parser.add_argument("--user", default=None, help="DB user")
    parser.add_argument("--password", default=None, help="DB password")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("Must specify --dry-run or --apply")

    # Load project config
    if args.project_config:
        config_path = Path(args.project_config).resolve()
        if not config_path.exists():
            log.error(f"Project config not found: {config_path}")
            sys.exit(1)
        with open(config_path) as f:
            project_config = json.load(f)
        # Resolve docs_root relative to the repo root.
        # Convention: _meta/ lives inside docs/, which lives inside the repo root.
        # So repo root is two levels above the config file: _meta/ -> docs/ -> repo/
        meta_dir = config_path.parent  # _meta/
        repo_root = meta_dir.parent.parent  # _meta/ -> docs/ -> repo root
        docs_root = (repo_root / project_config["docs_root"]).resolve()
    elif args.project and args.docs_root:
        docs_root = Path(args.docs_root).resolve()
        project_config = {
            "project_id": args.project,
            "project_name": args.project,
            "project_uri": f"project:{args.project}",
            "docs_root": str(docs_root),
            "tier": 0,
        }
    else:
        parser.error("Must specify --project-config or both --project and --docs-root")

    project_id = project_config["project_id"]

    if not docs_root.exists():
        log.error(f"Docs root not found: {docs_root}")
        sys.exit(1)

    log.info(f"Project: {project_config['project_name']} ({project_id})")
    log.info(f"Docs root: {docs_root}")

    # Collect and validate
    nodes, unclassified = collect_docs(docs_root)
    log.info(f"Found {len(nodes)} frontmattered docs, {len(unclassified)} unclassified")

    if not nodes:
        log.error("No frontmattered docs found")
        sys.exit(1)

    errors, external_refs = validate_doc_dag(nodes, project_id)
    if errors:
        log.error(f"Validation failed with {len(errors)} errors:")
        for err in errors:
            log.error(f"  - {err}")
        sys.exit(1)

    if external_refs:
        log.info(f"Found {len(external_refs)} cross-project reference(s), validating against DB...")
        ext_errors = validate_external_refs(external_refs, args)
        if ext_errors:
            log.error("External reference validation failed:")
            for err in ext_errors:
                log.error(f"  - {err}")
            sys.exit(1)
        log.info("External references validated")

    log.info("Validation passed")

    # Run ingest
    stats = run_ingest(project_config, nodes, args.dry_run, args)

    if args.dry_run:
        log.info("\nDry run complete. Use --apply to write changes.")


if __name__ == "__main__":
    main()
