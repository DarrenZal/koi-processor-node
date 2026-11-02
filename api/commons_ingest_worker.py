"""
Commons Ingest Worker

Background asyncio task that processes approved commons intake shares.
Uses PostgreSQL advisory locks + FOR UPDATE SKIP LOCKED for safe
multi-process/multi-worker concurrency.

State machine:
    staged → approved → ingesting → ingested | needs_merge_review | failed
    failed → ingesting (retry, up to 3x with exponential backoff)
    needs_merge_review → ingested (after admin resolves all merge candidates)

Usage:
    worker = CommonsIngestWorker(pool)
    await worker.start()
    ...
    await worker.stop()
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import asyncpg

logger = logging.getLogger("commons_ingest_worker")

# Advisory lock key — deterministic hash of worker name
ADVISORY_LOCK_KEY = 7_283_947_102  # hash('commons_ingest_worker') mod 2^31

# Polling and retry configuration
POLL_INTERVAL_S = 10
STALE_LEASE_CHECK_INTERVAL_S = 60
STALE_LEASE_TIMEOUT_S = 300  # 5 minutes

# Retry backoff: 1min, 5min, 30min, then stop
RETRY_BACKOFF_SECONDS = [60, 300, 1800]
MAX_RETRIES = 3

# Entity resolution confidence thresholds
CONFIDENCE_AUTO_MERGE = 0.95     # >0.95 → auto-merge with existing
CONFIDENCE_AMBIGUOUS_LOW = 0.85  # 0.85-0.95 → needs human review
# <0.85 → treat as new entity (no meaningful match)


class CommonsIngestWorker:
    """Background worker that ingests approved commons shares."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self._task: Optional[asyncio.Task] = None
        self._reaper_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start the background worker loop."""
        self._running = True
        self._task = asyncio.create_task(self._worker_loop())
        self._reaper_task = asyncio.create_task(self._stale_lease_reaper())
        logger.info("Commons ingest worker started")

    async def stop(self):
        """Stop the background worker loop."""
        self._running = False
        for task in (self._task, self._reaper_task):
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._task = None
        self._reaper_task = None
        logger.info("Commons ingest worker stopped")

    async def _worker_loop(self):
        """Main worker loop: acquire advisory lock, then poll for work."""
        while self._running:
            try:
                # Try to acquire advisory lock (singleton guard)
                async with self._pool.acquire() as conn:
                    acquired = await conn.fetchval(
                        "SELECT pg_try_advisory_lock($1)", ADVISORY_LOCK_KEY
                    )
                    if not acquired:
                        logger.debug("Another worker holds the advisory lock, sleeping")
                        await asyncio.sleep(POLL_INTERVAL_S)
                        continue

                    try:
                        await self._process_pending(conn)
                    finally:
                        await conn.execute(
                            "SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_KEY
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Worker loop error")

            await asyncio.sleep(POLL_INTERVAL_S)

    async def _process_pending(self, lock_conn: asyncpg.Connection):
        """Process all claimable rows in a single pass."""
        while self._running:
            row = await self._claim_next(lock_conn)
            if not row:
                break  # No more work

            share_id = row["id"]
            logger.info(f"Claimed share {share_id} for ingestion")
            try:
                await self._ingest_share(row)
            except Exception:
                logger.exception(f"Ingest failed for share {share_id}")
                await self._mark_failed(share_id)

    async def _claim_next(self, conn: asyncpg.Connection) -> Optional[asyncpg.Record]:
        """Atomically claim one approved or retry-eligible share."""
        # Use a separate connection for the claim to avoid holding the
        # advisory lock connection in a long transaction
        async with self._pool.acquire() as claim_conn:
            return await claim_conn.fetchrow("""
                UPDATE koi_shared_documents
                SET intake_status = 'ingesting',
                    ingest_started_at = NOW()
                WHERE id = (
                    SELECT id FROM koi_shared_documents
                    WHERE (intake_status = 'approved')
                       OR (intake_status = 'failed'
                           AND retry_count < $1
                           AND (next_retry_at IS NULL OR next_retry_at <= NOW()))
                    ORDER BY reviewed_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, event_id, document_rid, sender_node, manifest, contents
            """, MAX_RETRIES + 1)

    async def _ingest_share(self, row: asyncpg.Record):
        """Parse manifest, resolve entities, create graph entries."""
        share_id = row["id"]
        manifest = _parse_jsonb(row["manifest"])
        contents = _parse_jsonb(row["contents"])
        sender_node = row["sender_node"]

        if not manifest and not contents:
            logger.warning(f"Share {share_id} has no manifest or contents, marking ingested")
            await self._mark_ingested(share_id)
            return

        # Extract entities and relationships from the shared content
        entities = _extract_entities_from_share(manifest, contents)
        relationships = _extract_relationships_from_share(manifest, contents)

        if not entities and not relationships:
            logger.info(f"Share {share_id} has no entities or relationships to ingest")
            await self._mark_ingested(share_id)
            return

        # Import entity resolution at runtime to avoid circular imports
        from api.personal_ingest_api import (
            ExtractedEntity,
            CanonicalEntity,
            resolve_entity,
        )

        ambiguous_candidates = []
        ingested_entities = []

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for ent_data in entities:
                    entity = ExtractedEntity(
                        name=ent_data["name"],
                        type=ent_data.get("type", "Concept"),
                    )

                    canonical, is_new = await resolve_entity(
                        conn,
                        entity,
                        resolution_caller="commons_ingest_worker.ingest",
                    )

                    if is_new:
                        # New entity — no match found, already created by resolve_entity
                        ingested_entities.append(canonical)
                        logger.debug(
                            f"Share {share_id}: new entity '{canonical.name}' ({canonical.uri})"
                        )
                    elif canonical.confidence >= CONFIDENCE_AUTO_MERGE:
                        # High-confidence match — auto-merge
                        ingested_entities.append(canonical)
                        logger.debug(
                            f"Share {share_id}: auto-merged '{entity.name}' → "
                            f"'{canonical.name}' (confidence={canonical.confidence:.3f})"
                        )
                    elif canonical.confidence >= CONFIDENCE_AMBIGUOUS_LOW:
                        # Ambiguous match — queue for human review
                        ambiguous_candidates.append({
                            "share_id": share_id,
                            "remote_entity_label": entity.name,
                            "remote_entity_type": entity.type,
                            "local_entity_uri": canonical.uri,
                            "local_entity_label": canonical.name,
                            "confidence": canonical.confidence,
                        })
                        logger.info(
                            f"Share {share_id}: ambiguous match '{entity.name}' ↔ "
                            f"'{canonical.name}' (confidence={canonical.confidence:.3f})"
                        )
                    else:
                        # Low confidence — treat as new, resolve_entity already created it
                        ingested_entities.append(canonical)
                        logger.debug(
                            f"Share {share_id}: low match '{entity.name}' → new entity"
                        )

                # Process relationships for ingested entities
                for rel_data in relationships:
                    try:
                        await _create_relationship(
                            conn, rel_data, sender_node, ingested_entities
                        )
                    except Exception:
                        logger.warning(
                            f"Share {share_id}: failed to create relationship "
                            f"{rel_data.get('subject')} → {rel_data.get('object')}",
                            exc_info=True,
                        )

                # Record provenance in assertion_history for ingested entities
                for canonical in ingested_entities:
                    try:
                        await conn.execute("""
                            INSERT INTO assertion_history
                                (subject, predicate, object_literal, asserted_by_node_rid,
                                 provenance_doc_rid, source_node_rid)
                            VALUES ($1, 'commons_ingest', $2, $3, $4, $5)
                            ON CONFLICT DO NOTHING
                        """,
                            canonical.uri,
                            json.dumps({"name": canonical.name, "type": canonical.type}),
                            sender_node,
                            f"commons_share:{share_id}",
                            sender_node,
                        )
                    except asyncpg.PostgresError:
                        # assertion_history may not exist on all profiles
                        pass

                # Write merge candidates if any
                if ambiguous_candidates:
                    for mc in ambiguous_candidates:
                        await conn.execute("""
                            INSERT INTO koi_commons_merge_candidates
                                (share_id, remote_entity_label, remote_entity_type,
                                 local_entity_uri, local_entity_label, confidence)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (share_id, remote_entity_label, local_entity_uri)
                            DO UPDATE SET confidence = EXCLUDED.confidence
                        """,
                            mc["share_id"], mc["remote_entity_label"],
                            mc["remote_entity_type"], mc["local_entity_uri"],
                            mc["local_entity_label"], mc["confidence"],
                        )

        # Set terminal status
        if ambiguous_candidates:
            await self._mark_needs_merge_review(share_id)
            logger.info(
                f"Share {share_id}: {len(ingested_entities)} entities ingested, "
                f"{len(ambiguous_candidates)} need merge review"
            )
        else:
            await self._mark_ingested(share_id)
            logger.info(
                f"Share {share_id}: fully ingested ({len(ingested_entities)} entities)"
            )

    async def _mark_ingested(self, share_id: int):
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE koi_shared_documents SET intake_status = 'ingested' WHERE id = $1",
                share_id,
            )

    async def _mark_needs_merge_review(self, share_id: int):
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE koi_shared_documents SET intake_status = 'needs_merge_review' WHERE id = $1",
                share_id,
            )

    async def _mark_failed(self, share_id: int):
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT retry_count FROM koi_shared_documents WHERE id = $1",
                share_id,
            )
            retry_count = (row["retry_count"] or 0) if row else 0
            backoff_idx = min(retry_count, len(RETRY_BACKOFF_SECONDS) - 1)
            backoff_s = RETRY_BACKOFF_SECONDS[backoff_idx]

            await conn.execute("""
                UPDATE koi_shared_documents
                SET intake_status = 'failed',
                    retry_count = retry_count + 1,
                    next_retry_at = NOW() + make_interval(secs => $2)
                WHERE id = $1
            """, share_id, float(backoff_s))
            logger.warning(
                f"Share {share_id} marked failed (retry {retry_count + 1}, "
                f"next retry in {backoff_s}s)"
            )

    async def _stale_lease_reaper(self):
        """Reset shares stuck in 'ingesting' due to worker crash."""
        while self._running:
            try:
                await asyncio.sleep(STALE_LEASE_CHECK_INTERVAL_S)
                async with self._pool.acquire() as conn:
                    result = await conn.execute("""
                        UPDATE koi_shared_documents
                        SET intake_status = 'failed',
                            retry_count = retry_count + 1,
                            next_retry_at = NOW() + interval '60 seconds'
                        WHERE intake_status = 'ingesting'
                          AND ingest_started_at < NOW() - make_interval(secs => $1)
                    """, float(STALE_LEASE_TIMEOUT_S))
                    if result and "UPDATE 0" not in result:
                        logger.warning(f"Stale lease reaper reset rows: {result}")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Stale lease reaper error")


# =============================================================================
# Helper functions
# =============================================================================


def _parse_jsonb(val: Any) -> Optional[Dict]:
    """Parse a JSONB column that might be str, dict, or None."""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return None
    return val


def _extract_entities_from_share(
    manifest: Optional[Dict], contents: Optional[Dict]
) -> list:
    """Extract entity data from shared document manifest/contents.

    Supports multiple manifest formats:
    - manifest.entities: [{name, type, ...}]
    - contents.entities: [{name, type, ...}]
    - manifest.document with inline entity references
    """
    entities = []

    if manifest and "entities" in manifest:
        for e in manifest["entities"]:
            if isinstance(e, dict) and "name" in e:
                entities.append(e)

    if contents and "entities" in contents:
        for e in contents["entities"]:
            if isinstance(e, dict) and "name" in e:
                entities.append(e)

    return entities


def _extract_relationships_from_share(
    manifest: Optional[Dict], contents: Optional[Dict]
) -> list:
    """Extract relationship data from shared document manifest/contents."""
    relationships = []

    if manifest and "relationships" in manifest:
        for r in manifest["relationships"]:
            if isinstance(r, dict) and "subject" in r and "object" in r:
                relationships.append(r)

    if contents and "relationships" in contents:
        for r in contents["relationships"]:
            if isinstance(r, dict) and "subject" in r and "object" in r:
                relationships.append(r)

    return relationships


async def _create_relationship(
    conn: asyncpg.Connection,
    rel_data: Dict,
    sender_node: str,
    ingested_entities: list,
) -> None:
    """Create a relationship between resolved entities."""
    from api.personal_ingest_api import resolve_entity_to_uri

    subject_name = rel_data.get("subject", "")
    object_name = rel_data.get("object", "")
    predicate = rel_data.get("predicate", "related_to")

    # Try to find URIs from already-ingested entities
    subject_uri = None
    object_uri = None

    for ent in ingested_entities:
        if ent.name == subject_name or getattr(ent, "merged_with", None) == subject_name:
            subject_uri = ent.uri
        if ent.name == object_name or getattr(ent, "merged_with", None) == object_name:
            object_uri = ent.uri

    # Fall back to resolution
    if not subject_uri:
        subject_uri = await resolve_entity_to_uri(conn, subject_name)
    if not object_uri:
        object_uri = await resolve_entity_to_uri(conn, object_name)

    if not subject_uri or not object_uri:
        logger.debug(
            f"Skipping relationship: {subject_name} → {object_name} "
            f"(unresolved: subject={subject_uri is None}, object={object_uri is None})"
        )
        return

    # Check predicate is allowed
    allowed = await conn.fetchval(
        "SELECT 1 FROM allowed_predicates WHERE predicate = $1", predicate
    )
    if not allowed:
        predicate = "related_to"  # Fall back to generic

    await conn.execute("""
        INSERT INTO entity_relationships (subject_uri, predicate, object_uri, confidence, source)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (subject_uri, predicate, object_uri) DO UPDATE
        SET confidence = GREATEST(entity_relationships.confidence, EXCLUDED.confidence)
    """, subject_uri, predicate, object_uri, 0.8, f"commons:{sender_node}")
