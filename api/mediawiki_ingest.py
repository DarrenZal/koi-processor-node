"""Shared MediaWiki ingest logic used by both the bulk importer and live sensor.

Extracted from scripts/mediawiki_bulk_import.py to provide a clean import path
for api/mediawiki_sensor.py without pulling in script-level side effects.

Dependency graph:
    api/mediawiki_ingest.py  <- api/mediawiki_sensor.py
                             <- scripts/mediawiki_bulk_import.py
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from api.federation_events import doclink_row_created
from api.mediawiki_parser import PARSER_VERSION
from api.personal_ingest_api import (
    resolve_entity,
    store_new_entity,
    ExtractedEntity,
    CanonicalEntity,
    normalize_entity_text,
    normalize_alias,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
HIGH_DEGREE_THRESHOLD = 50

# ---------------------------------------------------------------------------
# Slug helper (matches mediawiki_parse_dump.py)
# ---------------------------------------------------------------------------

_SLUG_UNSAFE_RE = re.compile(r"[^a-z0-9\s-]")
_SLUG_SPACE_RE = re.compile(r"[\s]+")


def _title_to_slug(title: str, max_len: int = 100) -> str:
    s = unicodedata.normalize("NFC", title.strip().lower())
    s = _SLUG_UNSAFE_RE.sub("", s)
    s = _SLUG_SPACE_RE.sub("-", s).strip("-")
    return s[:max_len] if s else "untitled"


# ---------------------------------------------------------------------------
# Page-level ingest functions
# ---------------------------------------------------------------------------

async def upsert_page_state(
    conn: asyncpg.Connection,
    wiki_id: int,
    page: Dict[str, Any],
) -> Tuple[int, bool]:
    """Upsert mediawiki_page_state. Returns (page_state_id, was_skipped).

    Skipped = content_hash unchanged.
    """
    existing = await conn.fetchrow("""
        SELECT id, content_hash FROM mediawiki_page_state
        WHERE wiki_id = $1 AND page_id = $2
    """, wiki_id, page["page_id"])

    if existing and existing["content_hash"] == page.get("content_hash"):
        return existing["id"], True

    row = await conn.fetchrow("""
        INSERT INTO mediawiki_page_state (
            wiki_id, page_id, title, normalized_title, source_rid,
            namespace, template_type, bkc_entity_type, page_class,
            is_redirect, redirect_target, content_hash, revision_id,
            word_count, wikilink_count, template_field_count,
            entity_density_score, ingest_confidence, promotion_priority,
            parse_version, status, scanned_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13,
            $14, $15, $16,
            $17, $18, $19,
            $20, 'pending', NOW()
        )
        ON CONFLICT (wiki_id, page_id) DO UPDATE SET
            title = EXCLUDED.title,
            normalized_title = EXCLUDED.normalized_title,
            source_rid = EXCLUDED.source_rid,
            namespace = EXCLUDED.namespace,
            template_type = EXCLUDED.template_type,
            bkc_entity_type = EXCLUDED.bkc_entity_type,
            page_class = EXCLUDED.page_class,
            is_redirect = EXCLUDED.is_redirect,
            redirect_target = EXCLUDED.redirect_target,
            content_hash = EXCLUDED.content_hash,
            revision_id = EXCLUDED.revision_id,
            word_count = EXCLUDED.word_count,
            wikilink_count = EXCLUDED.wikilink_count,
            template_field_count = EXCLUDED.template_field_count,
            entity_density_score = EXCLUDED.entity_density_score,
            ingest_confidence = EXCLUDED.ingest_confidence,
            promotion_priority = EXCLUDED.promotion_priority,
            parse_version = EXCLUDED.parse_version,
            status = 'pending',
            scanned_at = NOW()
        RETURNING id
    """,
        wiki_id,
        page["page_id"],
        page["title"],
        page.get("normalized_title", ""),
        page.get("source_rid", ""),
        page.get("namespace", 0),
        page.get("template_type"),
        page.get("bkc_entity_type"),
        page.get("page_class", "source_only"),
        page.get("is_redirect", False),
        page.get("redirect_target"),
        page.get("content_hash", ""),
        page.get("revision_id", 0),
        page.get("word_count", 0),
        len(page.get("wikilinks", [])),
        len(page.get("template_fields", {})),
        page.get("entity_density_score", 0.0),
        page.get("ingest_confidence", 0.0),
        page.get("promotion_priority", 0.0),
        page.get("parse_version", PARSER_VERSION),
    )
    return row["id"], False


async def store_page_links(
    conn: asyncpg.Connection,
    wiki_id: int,
    page_state_id: int,
    page: Dict[str, Any],
) -> int:
    """Upsert source-native edges into mediawiki_page_links. Returns count."""
    count = 0
    revision_id = page.get("revision_id", 0)

    for se in page.get("structural_edges", []):
        await conn.execute("""
            INSERT INTO mediawiki_page_links (
                wiki_id, source_page_id, target_title, normalized_target_title,
                predicate, edge_class, field_name, confidence,
                source_section, source_revision_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (wiki_id, source_page_id, target_title, predicate,
                         COALESCE(field_name, ''), COALESCE(source_section, ''))
            DO UPDATE SET
                confidence = EXCLUDED.confidence,
                source_revision_id = EXCLUDED.source_revision_id
        """,
            wiki_id,
            page_state_id,
            se["target_title"],
            se["target_title"].lower().strip(),
            se["predicate"],
            "structural",
            se.get("field_name"),
            se["confidence"],
            se.get("source_section"),
            revision_id,
        )
        count += 1

    for ee in page.get("editorial_edges", []):
        await conn.execute("""
            INSERT INTO mediawiki_page_links (
                wiki_id, source_page_id, target_title, normalized_target_title,
                predicate, edge_class, confidence,
                source_section, source_revision_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (wiki_id, source_page_id, target_title, predicate,
                         COALESCE(field_name, ''), COALESCE(source_section, ''))
            DO UPDATE SET
                confidence = EXCLUDED.confidence,
                source_revision_id = EXCLUDED.source_revision_id
        """,
            wiki_id,
            page_state_id,
            ee["target_title"],
            ee["target_title"].lower().strip(),
            "related_to",
            "editorial",
            ee["confidence"],
            ee.get("source_section"),
            revision_id,
        )
        count += 1

    return count


async def register_redirect_alias(
    conn: asyncpg.Connection,
    redirect_title: str,
    target_title: str,
) -> None:
    """If the redirect target resolves to an existing entity, add this title as alias."""
    normalized_alias = normalize_alias(redirect_title)
    normalized_target = normalize_entity_text(target_title)

    target_entity = await conn.fetchrow("""
        SELECT fuseki_uri FROM entity_registry
        WHERE normalized_text = $1
        LIMIT 1
    """, normalized_target)

    if target_entity:
        await conn.execute("""
            UPDATE entity_registry
            SET aliases = (
                SELECT ARRAY(
                    SELECT DISTINCT unnest(
                        array_cat(COALESCE(aliases, '{}'), $1::TEXT[])
                    )
                )
            )
            WHERE fuseki_uri = $2
        """, [normalized_alias], target_entity["fuseki_uri"])
        logger.debug(f"Registered alias '{redirect_title}' -> {target_entity['fuseki_uri']}")


async def process_entity_bearing_page(
    conn: asyncpg.Connection,
    page: Dict[str, Any],
    page_state_id: int,
    wiki_id: int,
    run_id: str,
    log_file=None,
) -> Dict[str, int]:
    """Process an entity_bearing page: resolve entities, promote edges.

    Returns counters dict with entities_created, entities_matched, edges_promoted.
    """
    counters = {"entities_created": 0, "entities_matched": 0, "edges_promoted": 0}
    # (document_rid, entity_uri, context) for doclink federation emits. Group B
    # sites (ON CONFLICT DO NOTHING) — only rows actually inserted are recorded.
    # Caller emits AFTER its connection block commits (2e rule).
    counters["doclink_emits"] = []
    title = page["title"]
    source_rid = page.get("source_rid", "")
    bkc_type = page.get("bkc_entity_type", "Concept")

    # Count weak edges (tier 3 structural + editorial) for high-degree quarantine
    weak_edge_count = 0
    for se in page.get("structural_edges", []):
        if se["confidence"] < 0.85:
            weak_edge_count += 1
    weak_edge_count += len(page.get("editorial_edges", []))

    if weak_edge_count > HIGH_DEGREE_THRESHOLD:
        logger.warning(
            f"High-degree quarantine: '{title}' has {weak_edge_count} weak edges, "
            f"setting review_status='needs_review'"
        )
        await conn.execute("""
            UPDATE mediawiki_page_state
            SET review_status = 'needs_review',
                status = 'quarantined',
                last_run_id = $2
            WHERE id = $1
        """, page_state_id, run_id)
        _write_log(log_file, source_rid, title, None, None, None,
                    "quarantined", None, run_id)
        return counters

    # Resolve the page's primary entity
    page_entity = ExtractedEntity(name=title, type=bkc_type)
    canonical, is_new = await resolve_entity(
        conn,
        page_entity,
        resolution_caller="mediawiki_ingest.page",
    )

    if is_new:
        await store_new_entity(conn, page_entity, canonical, source_rid, source="mediawiki_import", origin="import")
        counters["entities_created"] += 1
        _write_log(log_file, source_rid, title, None, canonical.uri, canonical.name,
                    "created", None, run_id)
    else:
        counters["entities_matched"] += 1
        _write_log(log_file, source_rid, title,
                    page.get("source_rid"), canonical.uri, canonical.name,
                    "matched", canonical.confidence, run_id)

    subject_uri = canonical.uri

    # Register aliases from the page
    for alias in page.get("aliases", []):
        normalized_alias = normalize_alias(alias)
        if normalized_alias:
            await conn.execute("""
                UPDATE entity_registry
                SET aliases = (
                    SELECT ARRAY(
                        SELECT DISTINCT unnest(
                            array_cat(COALESCE(aliases, '{}'), $1::TEXT[])
                        )
                    )
                )
                WHERE fuseki_uri = $2
            """, [normalized_alias], subject_uri)

    # Upsert document_entity_links for the page entity
    _dl_ctx = f"Primary entity from wiki page: {title}"
    _dl_status = await conn.execute("""
        INSERT INTO document_entity_links (document_rid, entity_uri, context)
        VALUES ($1, $2, $3)
        ON CONFLICT (document_rid, entity_uri) DO NOTHING
    """, source_rid, subject_uri, _dl_ctx)
    if doclink_row_created(_dl_status):
        counters["doclink_emits"].append((source_rid, subject_uri, _dl_ctx))

    # Delete existing mediawiki_import edges for this page (idempotent re-import)
    await conn.execute("""
        DELETE FROM entity_relationships
        WHERE source = 'mediawiki_import'
        AND source_rid = $1
    """, source_rid)

    # Resolve structural edges and promote to entity_relationships
    for se in page.get("structural_edges", []):
        target_type = se.get("target_type_hint", "Concept") or "Concept"
        target_entity = ExtractedEntity(name=se["target_title"], type=target_type)
        target_canonical, target_is_new = await resolve_entity(
            conn,
            target_entity,
            resolution_caller="mediawiki_ingest.structural_edge",
        )

        if target_is_new:
            await store_new_entity(
                conn, target_entity, target_canonical, source_rid,
                source="mediawiki_import", origin="import"
            )
            counters["entities_created"] += 1

        predicate = se["predicate"]
        predicate_ok = await conn.fetchval("""
            SELECT EXISTS (SELECT 1 FROM allowed_predicates WHERE predicate = $1)
        """, predicate)
        if not predicate_ok:
            logger.debug(f"Skipping edge with unknown predicate: {predicate}")
            continue

        if subject_uri == target_canonical.uri:
            continue

        try:
            await conn.execute("""
                INSERT INTO entity_relationships
                    (subject_uri, predicate, object_uri, confidence, source, source_rid)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (subject_uri, predicate, object_uri) DO UPDATE
                SET confidence = GREATEST(entity_relationships.confidence, EXCLUDED.confidence),
                    source = EXCLUDED.source,
                    source_rid = EXCLUDED.source_rid
            """,
                subject_uri,
                predicate,
                target_canonical.uri,
                se["confidence"],
                "mediawiki_import",
                source_rid,
            )
            counters["edges_promoted"] += 1
        except Exception as e:
            logger.debug(f"Edge insert failed ({title} -> {se['target_title']}): {e}")

        await conn.execute("""
            UPDATE mediawiki_page_links
            SET resolution_status = 'resolved',
                resolved_target_uri = $3,
                target_match_confidence = $4
            WHERE source_page_id = $1
            AND target_title = $2
            AND edge_class = 'structural'
        """, page_state_id, se["target_title"], target_canonical.uri, target_canonical.confidence)

        _dl_ctx = f"Referenced via {se['predicate']} from {title}"
        _dl_status = await conn.execute("""
            INSERT INTO document_entity_links (document_rid, entity_uri, context)
            VALUES ($1, $2, $3)
            ON CONFLICT (document_rid, entity_uri) DO NOTHING
        """, source_rid, target_canonical.uri, _dl_ctx)
        if doclink_row_created(_dl_status):
            counters["doclink_emits"].append(
                (source_rid, target_canonical.uri, _dl_ctx)
            )

    # Resolve editorial edges (lower confidence, promote if target resolves to existing)
    for ee in page.get("editorial_edges", []):
        target_entity = ExtractedEntity(name=ee["target_title"], type="Concept")
        target_canonical, target_is_new = await resolve_entity(
            conn,
            target_entity,
            resolution_caller="mediawiki_ingest.editorial_edge",
        )

        if target_is_new:
            await store_new_entity(
                conn, target_entity, target_canonical, source_rid,
                source="mediawiki_import", origin="import"
            )
            counters["entities_created"] += 1

        predicate = "related_to"
        if subject_uri == target_canonical.uri:
            continue

        predicate_ok = await conn.fetchval("""
            SELECT EXISTS (SELECT 1 FROM allowed_predicates WHERE predicate = $1)
        """, predicate)
        if not predicate_ok:
            continue

        try:
            await conn.execute("""
                INSERT INTO entity_relationships
                    (subject_uri, predicate, object_uri, confidence, source, source_rid)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (subject_uri, predicate, object_uri) DO UPDATE
                SET confidence = GREATEST(entity_relationships.confidence, EXCLUDED.confidence),
                    source = EXCLUDED.source,
                    source_rid = EXCLUDED.source_rid
            """,
                subject_uri,
                predicate,
                target_canonical.uri,
                ee["confidence"],
                "mediawiki_import",
                source_rid,
            )
            counters["edges_promoted"] += 1
        except Exception as e:
            logger.debug(f"Editorial edge insert failed ({title} -> {ee['target_title']}): {e}")

        await conn.execute("""
            UPDATE mediawiki_page_links
            SET resolution_status = 'resolved',
                resolved_target_uri = $3,
                target_match_confidence = $4
            WHERE source_page_id = $1
            AND target_title = $2
            AND edge_class = 'editorial'
        """, page_state_id, ee["target_title"], target_canonical.uri, target_canonical.confidence)

        _dl_ctx = f"Editorial link from {title}"
        _dl_status = await conn.execute("""
            INSERT INTO document_entity_links (document_rid, entity_uri, context)
            VALUES ($1, $2, $3)
            ON CONFLICT (document_rid, entity_uri) DO NOTHING
        """, source_rid, target_canonical.uri, _dl_ctx)
        if doclink_row_created(_dl_status):
            counters["doclink_emits"].append(
                (source_rid, target_canonical.uri, _dl_ctx)
            )

    # Update page state with results
    await conn.execute("""
        UPDATE mediawiki_page_state
        SET status = 'ingested',
            entity_uri = $2,
            entities_created = $3,
            relationships_created = $4,
            ingested_at = NOW(),
            last_run_id = $5
        WHERE id = $1
    """, page_state_id, subject_uri,
        counters["entities_created"], counters["edges_promoted"], run_id)

    return counters


def _write_log(
    log_file,
    source_rid: str,
    source_title: str,
    source_url: Optional[str],
    entity_uri: Optional[str],
    entity_name: Optional[str],
    action: str,
    match_tier: Optional[float],
    run_id: str,
) -> None:
    """Append one record to the import log JSONL."""
    if log_file is None:
        return
    record = {
        "source_rid": source_rid,
        "source_title": source_title,
        "source_url": source_url,
        "entity_uri": entity_uri,
        "entity_name": entity_name,
        "action": action,
        "match_tier": match_tier,
        "run_id": run_id,
    }
    log_file.write(json.dumps(record, ensure_ascii=False) + "\n")
    log_file.flush()
