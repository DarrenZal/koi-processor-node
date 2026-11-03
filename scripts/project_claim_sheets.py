#!/usr/bin/env python3
"""
Claim Sheet Projector — Tier B source-claim-only projection.

Projects claim sheets from the Johar corpus intake into KOI as source claims.
Simpler contract than project_bridge_notes.py:
  - Source claims only (never review claims)
  - No disposition semantics
  - No bridge-note frontmatter assumptions
  - Idempotent on rerun
  - Provenance links to corpus document entity

Usage:
  python scripts/project_claim_sheets.py --dry-run <path>...
  python scripts/project_claim_sheets.py --apply <path>...
"""

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncpg
import httpx
import yaml

KOI_BASE = "http://localhost:8351"
DB_DSN = "postgresql://localhost:5432/personal_koi"
CORPUS_ID = "indy-johar-substack"

log = logging.getLogger("claim_sheet_projector")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SourceClaim:
    c_id: str           # C1, C2, ...
    confidence: str      # high, medium, low
    anchor: str          # section/paragraph anchor
    statement: str       # claim text


@dataclass
class ClaimSheet:
    essay_id: str
    title: str
    url: str
    tier: str
    primary_theme: str
    secondary_themes: list
    batch_id: str
    cluster_targets: list
    review_status: str
    source_collection: str
    claims: list         # list of SourceClaim


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_claim_sheet(path: Path) -> ClaimSheet:
    """Parse a claim-sheet markdown file into a ClaimSheet."""
    text = path.read_text()

    # Split frontmatter
    fm_match = re.match(r'^---\n(.*?)\n---\n(.*)', text, re.DOTALL)
    if not fm_match:
        raise ValueError(f"No YAML frontmatter in {path}")

    fm = yaml.safe_load(fm_match.group(1))
    body = fm_match.group(2)

    # Parse claims from body
    claim_pattern = re.compile(
        r'\*\*(C\d+)\*\*\s+'
        r'\[confidence:\s*(high|medium|low)\]\s+'
        r'\[anchor:\s*([^\]]+)\]\s*\n'
        r'(.*?)(?=\n\*\*C\d+\*\*|\n##|\Z)',
        re.DOTALL
    )

    claims = []
    for m in claim_pattern.finditer(body):
        claims.append(SourceClaim(
            c_id=m.group(1),
            confidence=m.group(2),
            anchor=m.group(3).strip(),
            statement=m.group(4).strip(),
        ))

    if not claims:
        raise ValueError(f"No claims found in {path}")
    if len(claims) > 5:
        raise ValueError(f"Too many claims ({len(claims)}) in {path}, max 5")

    return ClaimSheet(
        essay_id=fm["essay_id"],
        title=fm["title"],
        url=fm["url"],
        tier=fm.get("tier", "B"),
        primary_theme=fm["primary_theme"],
        secondary_themes=fm.get("secondary_themes", []),
        batch_id=fm["batch_id"],
        cluster_targets=fm.get("cluster_targets", []),
        review_status=fm.get("review_status", "pending"),
        source_collection=fm.get("source_collection", CORPUS_ID),
        claims=claims,
    )


# ---------------------------------------------------------------------------
# Entity helpers (same conventions as project_bridge_notes.py)
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    return text.lower().strip()


def generate_entity_uri(name: str, entity_type: str) -> str:
    normalized = normalize_text(name)
    hash_input = f"{entity_type}:{normalized}"
    hash_id = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
    type_prefix = entity_type.lower()
    safe_name = normalized.replace(' ', '-').replace("'", '')[:50]
    return f"orn:personal-koi.entity:{type_prefix}-{safe_name}-{hash_id}"


async def resolve_or_create_concept(conn: asyncpg.Connection, concept_name: str) -> str:
    normalized = normalize_text(concept_name)
    row = await conn.fetchrow(
        "SELECT fuseki_uri FROM entity_registry "
        "WHERE normalized_text = $1 AND entity_type = 'Concept' LIMIT 1",
        normalized,
    )
    if row:
        return row["fuseki_uri"]

    normalized_spaced = normalized.replace("-", " ")
    if normalized_spaced != normalized:
        row = await conn.fetchrow(
            "SELECT fuseki_uri FROM entity_registry "
            "WHERE normalized_text = $1 AND entity_type = 'Concept' LIMIT 1",
            normalized_spaced,
        )
        if row:
            return row["fuseki_uri"]

    uri = generate_entity_uri(concept_name.replace("-", " "), "Concept")
    entity_text = concept_name.replace("-", " ").title()
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, metadata) "
        "VALUES ($1, $2, 'Concept', $3, $4::jsonb) "
        "ON CONFLICT (fuseki_uri) DO NOTHING",
        uri, entity_text, normalized_spaced,
        json.dumps({"source": "learning_field"}),
    )
    return uri


async def find_previous_source_claim(
    conn: asyncpg.Connection, source_document: str, c_id: str
) -> Optional[dict]:
    row = await conn.fetchrow(
        "SELECT claim_rid, statement FROM claims "
        "WHERE source_document = $1 "
        "  AND metadata->>'c_id' = $2 "
        "  AND metadata->>'source' = 'learning_field' "
        "  AND metadata->>'claim_layer' = 'source' "
        "ORDER BY created_at DESC LIMIT 1",
        source_document, c_id,
    )
    if row:
        return {"claim_rid": row["claim_rid"], "statement": row["statement"]}
    return None


async def find_primary_review_claim(
    conn: asyncpg.Connection, concept_slug: str, project_scope: str = "spore"
) -> Optional[str]:
    """Find the primary review claim for a concept within a project scope.

    Returns at most one review claim entity_uri. Prefers the earliest-created
    review claim in the project scope for this concept. This prevents a single
    source claim from inflating supports counts by linking to every governance
    cluster sharing the concept.
    """
    row = await conn.fetchrow(
        "SELECT entity_uri FROM claims "
        "WHERE metadata->>'claim_layer' = 'review' "
        "  AND metadata->>'source' = 'learning_field' "
        "  AND metadata->>'governance_cluster_key' LIKE $1 || '.%:' || $2 "
        "ORDER BY created_at ASC LIMIT 1",
        project_scope, concept_slug,
    )
    return row["entity_uri"] if row else None


async def insert_edge(
    conn: asyncpg.Connection,
    subject_uri: str,
    predicate: str,
    object_uri: str,
    source: str = "learning_field",
    confidence: float = 1.0,
    source_rid: Optional[str] = None,
) -> bool:
    try:
        result = await conn.execute(
            "INSERT INTO entity_relationships "
            "(subject_uri, predicate, object_uri, confidence, source, source_rid) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "ON CONFLICT DO NOTHING",
            subject_uri, predicate, object_uri, confidence, source, source_rid,
        )
        return result == "INSERT 0 1"
    except Exception as e:
        log.error(f"  Edge insert failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Projection
# ---------------------------------------------------------------------------

async def project_claim_sheet(
    sheet: ClaimSheet,
    conn: asyncpg.Connection,
    client: httpx.AsyncClient,
    dry_run: bool,
    projection_batch: str,
) -> dict:
    """Project a single claim sheet into KOI."""
    stats = {"source_claims": 0, "concepts": 0, "edges": 0}

    source_doc_id = f"claim-sheet:{sheet.source_collection}:{sheet.essay_id}"
    # Resolve document entity URI for provenance
    doc_uri = f"orn:personal-koi.entity:document-{sheet.source_collection}-{sheet.essay_id}"

    # Resolve project URI (use spore as default for Johar intake)
    project_uri = await conn.fetchval(
        "SELECT fuseki_uri FROM entity_registry "
        "WHERE entity_type = 'Project' AND metadata->>'project_id' = 'spore' LIMIT 1",
    )
    if not project_uri:
        log.warning("  No spore project entity found, using placeholder")
        project_uri = "project:forest-garden"

    claimant_uri = "org:spore-learning-field"

    log.info(f"\nProjecting claim sheet: {sheet.essay_id} ({len(sheet.claims)} claims)")
    log.info(f"  Cluster targets: {sheet.cluster_targets}")

    if dry_run:
        log.info("  [DRY RUN] Would create entities and edges")
        stats["source_claims"] = len(sheet.claims)
        return stats

    # 1. Resolve/create concept entities for cluster targets
    concept_uris = {}
    for ct in sheet.cluster_targets:
        uri = await resolve_or_create_concept(conn, ct)
        concept_uris[ct] = uri
        stats["concepts"] += 1

    # 2. Create source claims
    for claim in sheet.claims:
        # Primary about_uri: first cluster target
        primary_concept = sheet.cluster_targets[0] if sheet.cluster_targets else None
        about_uri = concept_uris.get(primary_concept, doc_uri)

        # Check for existing claim (idempotency)
        previous = await find_previous_source_claim(conn, source_doc_id, claim.c_id)
        entity_uri = None

        if previous and previous["statement"] == claim.statement:
            log.info(f"  {claim.c_id}: unchanged, reusing")
            # Resolve entity_uri for existing claim to create edges
            row = await conn.fetchrow(
                "SELECT entity_uri FROM claims WHERE claim_rid = $1",
                previous["claim_rid"],
            )
            entity_uri = row["entity_uri"] if row else None
        else:
            supersedes_rid = None
            if previous:
                supersedes_rid = previous["claim_rid"]
                log.info(f"  {claim.c_id}: supersedes {supersedes_rid}")

            payload = {
                "claimant_uri": claimant_uri,
                "statement": claim.statement,
                "claim_type": "governance",
                "about_uri": about_uri,
                "source_document": source_doc_id,
                "metadata": {
                    "c_id": claim.c_id,
                    "confidence": claim.confidence,
                    "evidence_anchor": claim.anchor,
                    "claim_layer": "source",
                    "extraction_status": "extracted",
                    "project_uri": project_uri,
                    "source": "learning_field",
                    "artifact_kind": "claim_sheet",
                    "essay_id": sheet.essay_id,
                    "corpus_id": sheet.source_collection,
                    "batch_id": sheet.batch_id,
                    "cluster_targets": sheet.cluster_targets,
                },
                "created_by": "darren",
            }
            if supersedes_rid:
                payload["supersedes_rid"] = supersedes_rid

            resp = await client.post(f"{KOI_BASE}/claims/", json=payload, timeout=30)
            if resp.status_code not in (200, 201):
                log.error(f"  Failed: {claim.c_id}: {resp.status_code} {resp.text}")
                continue

            data = resp.json()
            claim_rid = data.get("claim_rid", "?")
            entity_uri = data.get("entity_uri", "?")
            log.info(f"  {claim.c_id}: {claim_rid}")
            stats["source_claims"] += 1

            # Add projection_batch
            await conn.execute(
                "UPDATE claims SET metadata = metadata || $1::jsonb WHERE claim_rid = $2",
                json.dumps({"projection_batch": projection_batch}),
                claim_rid,
            )

        if not entity_uri:
            continue

        # 3. Create edges: claim → concept (about), claim → document (sourced_from)
        for ct in sheet.cluster_targets:
            if ct in concept_uris:
                inserted = await insert_edge(
                    conn, entity_uri, "about", concept_uris[ct],
                    source_rid=source_doc_id,
                )
                if inserted:
                    stats["edges"] += 1

            # 4. Link to primary review claim for convergence (one per concept)
            review_uri = await find_primary_review_claim(conn, ct)
            if review_uri:
                inserted = await insert_edge(
                    conn, entity_uri, "supports", review_uri,
                    source="learning_field",
                    source_rid=source_doc_id,
                )
                if inserted:
                    stats["edges"] += 1

        # Provenance edge: claim sourced_from document
        inserted = await insert_edge(
            conn, entity_uri, "sourced_from", doc_uri,
            source_rid=source_doc_id,
        )
        if inserted:
            stats["edges"] += 1

    return stats


async def main(paths: list[Path], dry_run: bool):
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    sheets = []
    for p in paths:
        try:
            sheet = parse_claim_sheet(p)
            if sheet.review_status != "approved":
                log.warning(f"Skipping {p.name}: review_status={sheet.review_status}")
                continue
            sheets.append(sheet)
        except Exception as e:
            log.error(f"Failed to parse {p}: {e}")
            sys.exit(1)

    log.info(f"Loaded {len(sheets)} claim sheets")

    conn = await asyncpg.connect(DB_DSN)
    async with httpx.AsyncClient() as client:
        totals = {"source_claims": 0, "concepts": 0, "edges": 0}
        for sheet in sheets:
            stats = await project_claim_sheet(sheet, conn, client, dry_run, batch_ts)
            for k in totals:
                totals[k] += stats.get(k, 0)

    await conn.close()

    log.info(f"\n{'=' * 60}")
    log.info("PROJECTION SUMMARY")
    log.info(f"  Sheets processed: {len(sheets)}")
    log.info(f"  Source claims:    {totals['source_claims']}")
    log.info(f"  Review claims:    0 (claim sheets never create review claims)")
    log.info(f"  Concepts:         {totals['concepts']}")
    log.info(f"  Edges:            {totals['edges']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project claim sheets into KOI graph")
    parser.add_argument("paths", nargs="+", type=Path, help="Claim sheet files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    if not args.apply and not args.dry_run:
        parser.error("Specify --apply or --dry-run")

    asyncio.run(main(args.paths, dry_run=args.dry_run))
