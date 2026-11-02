"""MediaWiki Sensor — Background task polling registered wikis for changes.

Polls the MediaWiki RecentChanges API, detects updated/new pages via revision ID,
and runs the existing parse->resolve->embed pipeline incrementally.

Follows the GitHubSensor pattern: asyncio background task with start/stop lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import asyncpg

from api.mediawiki_api import MediaWikiClient, RecentChange, PageContent
from api.mediawiki_parser import parse_page, WikiPageParse
from api.mediawiki_ingest import (
    upsert_page_state,
    store_page_links,
    register_redirect_alias,
    process_entity_bearing_page,
)

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = int(os.getenv("MEDIAWIKI_POLL_INTERVAL", "300"))


def _mirror_path(local_path: str, title: str) -> Path:
    """MediaWiki title → local filesystem path, matching Jeff's wiki/ convention.

    Jeff's archive uses literal titles with spaces preserved. The only POSIX-unsafe
    char in MediaWiki titles is '/' (subpage separator); we map it to U+2215 DIVISION
    SLASH (visually similar, reversible, filesystem-safe). Titles already containing
    U+2215 are rejected (collision guard — astronomically rare).
    """
    if "\u2215" in title:
        raise ValueError(f"title contains U+2215 DIVISION SLASH — ambiguous: {title!r}")
    safe = title.replace("/", "\u2215")
    return Path(local_path) / "wiki" / f"{safe}.mediawiki"

# Backoff constants (same as KOIPoller)
_BACKOFF_BASE = 30
_BACKOFF_MAX = 600


class MediaWikiSensor:
    """Background task that polls registered MediaWiki wikis for recent changes."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        event_queue=None,
    ):
        self.pool = pool
        self.poll_interval = poll_interval
        self.event_queue = event_queue
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_scan: Optional[datetime] = None
        self._scan_count = 0
        self._failures: Dict[int, dict] = {}  # wiki_id -> {count, last_error, next_retry}
        self._embedder = None
        self._chunker = None

    async def start(self):
        """Start the background sync loop."""
        self._running = True
        # Lazy-init embedding provider
        try:
            from api.embedding_provider import create_embedding_provider
            self._embedder = create_embedding_provider()
            if self._embedder:
                logger.info(f"MediaWiki sensor embedding: {self._embedder.model_name}")
        except Exception as e:
            logger.warning(f"MediaWiki sensor: no embedder available: {e}")

        try:
            from api.chunker import SentenceAwareChunker
            self._chunker = SentenceAwareChunker(
                chunk_size=500, chunk_overlap=50, min_chunk_size=100
            )
        except Exception as e:
            logger.warning(f"MediaWiki sensor: no chunker available: {e}")

        self._task = asyncio.create_task(self._scan_loop())
        logger.info(f"MediaWiki sensor started (interval={self.poll_interval}s)")

    async def stop(self):
        """Stop the background sync loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("MediaWiki sensor stopped")

    async def _scan_loop(self):
        """Main loop: wait 30s, then sync all wikis on interval."""
        # Initial delay to let the API fully start
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            raise

        while self._running:
            try:
                await self._sync_all_wikis()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"MediaWiki sensor scan error: {e}", exc_info=True)

            try:
                await asyncio.sleep(self.poll_interval)
            except asyncio.CancelledError:
                raise

    async def _sync_all_wikis(self):
        """Query active wikis in poll mode and sync each."""
        async with self.pool.acquire() as conn:
            wikis = await conn.fetch(
                "SELECT * FROM mediawiki_wikis WHERE status = 'active' AND sync_mode = 'poll'"
            )

        if not wikis:
            logger.debug("MediaWiki sensor: no active poll-mode wikis")
            return

        for wiki_row in wikis:
            wiki = dict(wiki_row)
            wiki_id = wiki["id"]

            if self._should_skip(wiki_id):
                logger.debug(f"MediaWiki sensor: skipping wiki {wiki_id} (backoff)")
                continue

            try:
                result = await self._sync_wiki(wiki)
                self._record_success(wiki_id)
                logger.info(
                    f"MediaWiki sync {wiki.get('wiki_name', wiki_id)}: "
                    f"{result.get('changes_found', 0)} changes, "
                    f"{result.get('pages_processed', 0)} pages processed"
                )
            except Exception as e:
                self._record_failure(wiki_id, str(e))
                logger.error(
                    f"MediaWiki sync error for wiki {wiki_id}: {e}",
                    exc_info=True,
                )

        self._last_scan = datetime.now(timezone.utc)
        self._scan_count += 1

    async def _sync_wiki(self, wiki: dict) -> dict:
        """Sync a single wiki: fetch recent changes, process changed pages."""
        wiki_id = wiki["id"]
        api_url = wiki["api_url"]
        base_url = wiki["base_url"]
        wiki_domain = base_url.rstrip("/").split("//")[-1]

        # Determine "since" timestamp (1-second overlap guards against same-second
        # edits at the watermark boundary — MediaWiki rcstart is inclusive, so this
        # produces at most harmless duplicates handled by idempotent upserts).
        last_scan = wiki.get("last_scan_at")
        if last_scan:
            since = last_scan - timedelta(seconds=1)
        else:
            since = datetime.now(timezone.utc) - timedelta(hours=24)

        # Fetch recent changes
        client = MediaWikiClient(api_url=api_url, request_delay=1.0)
        try:
            changes = await client.fetch_recent_changes(since=since, limit=200)
        finally:
            await client.close()

        if not changes:
            # Update last_scan_at even with no changes
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE mediawiki_wikis SET last_scan_at = NOW() WHERE id = $1",
                    wiki_id,
                )
            return {"changes_found": 0, "pages_processed": 0}

        # Process log events (deletes, moves) BEFORE the edit-dedupe loop and remove
        # them from `changes` so they don't flow into `latest_by_page`. Deletes have no
        # fetchable content; moves carry their target_title in logparams.
        log_events = [rc for rc in changes if rc.change_type == "log"]
        changes = [rc for rc in changes if rc.change_type != "log"]
        await self._process_log_events(wiki, log_events)

        # Deduplicate by pageid (take latest revid per page)
        latest_by_page: Dict[int, RecentChange] = {}
        for rc in changes:
            existing = latest_by_page.get(rc.pageid)
            if existing is None or rc.revid > existing.revid:
                latest_by_page[rc.pageid] = rc

        # Batch-fetch page content
        pageids = list(latest_by_page.keys())
        client = MediaWikiClient(api_url=api_url, request_delay=1.0)
        try:
            page_contents = await client.fetch_page_batch(pageids)
        finally:
            await client.close()

        # Build lookup
        content_by_id: Dict[int, PageContent] = {pc.pageid: pc for pc in page_contents}

        # Process each changed page
        pages_processed = 0
        run_id = f"mw-sensor-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

        for pageid, rc in latest_by_page.items():
            pc = content_by_id.get(pageid)
            if pc is None:
                logger.debug(f"No content fetched for pageid {pageid}")
                continue

            change_type = rc.change_type
            try:
                await self._process_changed_page(
                    wiki_id, wiki_domain, pc, change_type, run_id
                )
                pages_processed += 1
                # Filesystem mirror: best-effort; never fails the sync
                try:
                    await self._write_local_mirror(wiki, pc.title, pc.wikitext, change_type)
                except Exception as fs_err:
                    logger.warning(f"Mirror write failed for '{pc.title}': {fs_err}")
            except Exception as e:
                logger.warning(
                    f"Error processing changed page '{rc.title}' (pageid={pageid}): {e}"
                )

        # Update last_scan_at
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE mediawiki_wikis SET last_scan_at = NOW() WHERE id = $1",
                wiki_id,
            )

        return {
            "changes_found": len(latest_by_page),
            "pages_processed": pages_processed,
            "run_id": run_id,
        }

    async def _process_changed_page(
        self,
        wiki_id: int,
        wiki_domain: str,
        page_content: PageContent,
        change_type: str,
        run_id: str,
    ):
        """Process a single changed/new page through the full pipeline."""
        # 1. Parse with mediawiki_parser
        parsed = parse_page(
            title=page_content.title,
            wikitext=page_content.wikitext,
            page_id=page_content.pageid,
            revision_id=page_content.revid,
            wiki_domain=wiki_domain,
        )

        # Convert to dict for the ingest functions
        page_dict = _parsed_to_dict(parsed)

        doclink_emits = []
        async with self.pool.acquire() as conn:
            # 2. Compare content_hash — upsert_page_state handles skip logic
            page_state_id, was_skipped = await upsert_page_state(
                conn, wiki_id, page_dict
            )

            if was_skipped:
                logger.debug(f"Skipped unchanged page: {page_content.title}")
                return

            # 3. Store page links
            await store_page_links(conn, wiki_id, page_state_id, page_dict)

            # 4. Route by page class
            page_class = parsed.page_class
            ingest_confidence = parsed.ingest_confidence

            if parsed.is_redirect and parsed.redirect_target:
                await register_redirect_alias(
                    conn, parsed.title, parsed.redirect_target
                )
                await conn.execute("""
                    UPDATE mediawiki_page_state
                    SET status = 'skipped', last_run_id = $2
                    WHERE id = $1
                """, page_state_id, run_id)

            elif page_class == "entity_bearing" and ingest_confidence >= 0.6:
                counters = await process_entity_bearing_page(
                    conn, page_dict, page_state_id, wiki_id, run_id
                )
                doclink_emits = counters.get("doclink_emits", [])

            else:
                # source_only or below threshold
                await conn.execute("""
                    UPDATE mediawiki_page_state
                    SET status = 'staged', last_run_id = $2
                    WHERE id = $1
                """, page_state_id, run_id)

        # Emit doclink federation events post-commit (after the connection
        # block above exits). Group B sites → already filtered to rows that
        # were actually inserted; mention_delta=1 each.
        from api.federation_events import emit_doclink_event
        for document_rid, entity_uri, ctx in doclink_emits:
            await emit_doclink_event(document_rid, entity_uri, 1, context=ctx)

        # 5. Re-chunk + re-embed (outside the main connection context)
        if parsed.word_count >= 30 and not parsed.is_redirect:
            try:
                await self._rechunk_and_embed(wiki_domain, parsed)
            except Exception as e:
                logger.warning(f"Rechunk/embed failed for '{parsed.title}': {e}")

        # 6. Emit KOI-net event
        if self.event_queue:
            event_type = "NEW" if change_type == "new" else "UPDATE"
            rid = parsed.source_rid
            try:
                await self.event_queue.add(
                    event_type=event_type,
                    rid=rid,
                    manifest={"title": parsed.title, "page_class": parsed.page_class},
                    contents={"content_hash": parsed.content_hash},
                )
            except Exception as e:
                logger.warning(f"Event emit failed for {rid}: {e}")

    async def _write_local_mirror(self, wiki: dict, title: str, wikitext: Optional[str], change_type: str):
        """Mirror a page to the filesystem clone. Best-effort; errors logged but not raised."""
        cfg = wiki.get("config") or {}
        if isinstance(cfg, str):
            cfg = json.loads(cfg)
        if not cfg.get("write_filesystem"):
            return
        local_path = cfg.get("local_path")
        if not local_path:
            return

        # Branch safety: only write when the clone is on the expected branch.
        # Prevents accidentally dirtying `main` if someone checks it out.
        expected_branch = cfg.get("git_branch", "live-sync")
        try:
            head = subprocess.check_output(
                ["git", "-C", local_path, "rev-parse", "--abbrev-ref", "HEAD"],
                stderr=subprocess.DEVNULL, timeout=2,
            ).decode().strip()
            if head != expected_branch:
                logger.warning(
                    f"Clone on branch {head!r}, expected {expected_branch!r} — skipping filesystem write for {title!r}"
                )
                return
        except Exception as e:
            logger.warning(f"Branch check failed ({e}) — skipping filesystem write for {title!r}")
            return

        try:
            path = _mirror_path(local_path, title)
        except ValueError as e:
            logger.warning(f"Skipping mirror write: {e}")
            return

        if change_type == "delete":
            if path.exists():
                try:
                    path.unlink()
                except OSError as e:
                    logger.warning(f"Mirror delete failed for {path}: {e}")
            return

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(wikitext or "", encoding="utf-8")
        except OSError as e:
            logger.warning(f"Mirror write failed for {path}: {e}")

    async def _process_log_events(self, wiki: dict, log_events: List[RecentChange]):
        """Handle delete and move log events (filesystem + DB tombstone/title update)."""
        wiki_id = wiki["id"]
        cfg = wiki.get("config") or {}
        if isinstance(cfg, str):
            cfg = json.loads(cfg)

        for rc in log_events:
            if rc.logtype == "delete" and rc.logaction == "delete":
                await self._write_local_mirror(wiki, rc.title, None, change_type="delete")
                async with self.pool.acquire() as conn:
                    if rc.pageid:
                        await conn.execute(
                            "UPDATE mediawiki_page_state SET status='deleted' WHERE wiki_id=$1 AND page_id=$2",
                            wiki_id, rc.pageid,
                        )
                    else:
                        await conn.execute(
                            "UPDATE mediawiki_page_state SET status='deleted' WHERE wiki_id=$1 AND title=$2",
                            wiki_id, rc.title,
                        )

            elif rc.logtype == "move" and rc.logaction in ("move", "move_redir", "move_noredir"):
                lp = rc.logparams or {}
                target = lp.get("target_title") or lp.get("target") or lp.get("4::target")
                if not target:
                    logger.warning(f"move event without target_title: {rc.title} (pageid={rc.pageid})")
                    continue

                # Filesystem rename (best-effort)
                if cfg.get("write_filesystem") and cfg.get("local_path"):
                    try:
                        old_path = _mirror_path(cfg["local_path"], rc.title)
                        new_path = _mirror_path(cfg["local_path"], target)
                        if old_path.exists():
                            new_path.parent.mkdir(parents=True, exist_ok=True)
                            old_path.rename(new_path)
                    except ValueError as e:
                        logger.warning(f"Skipping filesystem rename: {e}")
                    except OSError as e:
                        logger.warning(f"Filesystem rename failed: {e}")

                # DB title update always runs on successful move event.
                # source_rid is page-id-based (mediawiki:{domain}:{page_id}, stable across renames).
                if rc.pageid:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE mediawiki_page_state SET title=$1 WHERE wiki_id=$2 AND page_id=$3",
                            target, wiki_id, rc.pageid,
                        )

    async def _rechunk_and_embed(self, wiki_domain: str, parsed: WikiPageParse):
        """Delete existing chunks, re-chunk sections, embed, store."""
        if not self._chunker:
            return

        doc_rid = f"mediawiki:{wiki_domain}:{parsed.page_id}"
        wiki_url = f"https://{wiki_domain}/wiki/{parsed.title.replace(' ', '_')}"

        # Build section-aware chunks
        chunk_entries = []  # (text, embed_text, section_id, section_title, url)
        for section in parsed.sections:
            sec_text = section.text
            if not sec_text or len(sec_text.split()) < 10:
                continue
            sec_url = f"{wiki_url}#{section.id}"
            sec_title = section.title or "Introduction"
            if len(sec_text.split()) < 500:
                chunk_entries.append((
                    sec_text,
                    f"Page: {parsed.title} | Section: {sec_title}\n\n{sec_text}",
                    section.id,
                    sec_title,
                    sec_url,
                ))
            else:
                sub_chunks = self._chunker.chunk_text(sec_text)
                for sc in sub_chunks:
                    chunk_entries.append((
                        sc["text"],
                        f"Page: {parsed.title} | Section: {sec_title}\n\n{sc['text']}",
                        section.id,
                        sec_title,
                        sec_url,
                    ))

        if not chunk_entries:
            return

        # ── B8: Generate contextual retrieval snippets ──
        contexts = [""] * len(chunk_entries)
        try:
            from api.contextual_retriever import generate_contexts_for_document
            chunk_dicts = [{"text": ce[0]} for ce in chunk_entries]
            contexts = await generate_contexts_for_document(
                document_text=parsed.plain_text or "",
                chunks=chunk_dicts,
                document_title=parsed.title,
                concurrency=5,
            )
        except Exception as e:
            logger.warning(f"B8 context generation failed (non-fatal): {e}")

        # Rebuild embed texts with context prepended
        contextualized_entries = []
        for (text, _old_embed, sec_id, sec_title, sec_url), ctx in zip(chunk_entries, contexts):
            base_embed = f"Page: {parsed.title} | Section: {sec_title}\n\n{text}"
            embed_text = f"{ctx}\n\n{base_embed}" if ctx else base_embed
            contextualized_entries.append((text, embed_text, sec_id, sec_title, sec_url, ctx))

        # Embed
        embeddings = []
        if self._embedder:
            embed_texts = [ce[1] for ce in contextualized_entries]
            for batch_start in range(0, len(embed_texts), 100):
                batch = embed_texts[batch_start:batch_start + 100]
                # Pack 2 (2026-04-28): migrated to embed_batch_or_none for
                # B2/C4 token-tracking metric emission (one aggregate JSONL
                # record per batch with is_batch=true). Returns None on
                # whole-batch failure (matches embed_or_none semantics).
                batch_embs = await self._embedder.embed_batch_or_none(
                    batch, prompt_type="extraction"
                )
                if batch_embs is None:
                    logger.warning(
                        f"Embed batch failed (size={len(batch)}); "
                        "falling back to None embeddings for this batch."
                    )
                    embeddings.extend([None] * len(batch))
                else:
                    embeddings.extend(batch_embs)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Upsert koi_memories document
                doc_content = json.dumps({
                    "title": parsed.title,
                    "text": parsed.plain_text,
                    "wiki_url": wiki_url,
                    "template_type": parsed.template_type,
                    "page_class": parsed.page_class,
                })
                doc_metadata_dict = {
                    "source_rid": parsed.source_rid,
                    "page_id": parsed.page_id,
                    "word_count": parsed.word_count,
                    "revision_id": parsed.revision_id,
                }
                doc_metadata = json.dumps(doc_metadata_dict)

                # Privacy: promote metadata is_private/access_source to dedicated columns.
                # Sticky-OR on ON CONFLICT — once-private-stays-private (Phase 1 / tech-backlog #23).
                is_private = bool(doc_metadata_dict.get('is_private', False))
                access_source = doc_metadata_dict.get('access_source')

                await conn.execute("""
                    INSERT INTO koi_memories (id, rid, event_type, source_sensor, content, metadata, is_private, access_source)
                    VALUES ($1, $2, 'UPDATE', 'mediawiki-sensor', $3::jsonb, $4::jsonb, $5, $6)
                    ON CONFLICT (rid) DO UPDATE SET
                        event_type = 'UPDATE',
                        content = EXCLUDED.content,
                        metadata = EXCLUDED.metadata,
                        is_private = (koi_memories.is_private OR EXCLUDED.is_private),
                        access_source = COALESCE(koi_memories.access_source, EXCLUDED.access_source),
                        updated_at = NOW()
                """, uuid.uuid4(), doc_rid, doc_content, doc_metadata, is_private, access_source)

                # Delete old chunks
                await conn.execute(
                    "DELETE FROM koi_memory_chunks WHERE document_rid = $1", doc_rid
                )

                # Insert new chunks
                for idx, (text, _embed_text, sec_id, sec_title, sec_url, ctx) in enumerate(contextualized_entries):
                    chunk_rid = f"{doc_rid}#section:{sec_id}#chunk{idx}"
                    chunk_content = json.dumps({
                        "text": text,
                        "context": ctx,
                        "title": parsed.title,
                        "chunk_index": idx,
                        "section_id": sec_id,
                        "section_title": sec_title,
                        "wiki_url": sec_url,
                    })

                    embedding_str = None
                    if embeddings and idx < len(embeddings) and embeddings[idx] is not None:
                        embedding_str = '[' + ','.join(str(x) for x in embeddings[idx]) + ']'

                    if embedding_str:
                        # Writes to embedding_3072 (post-2026-04-23 OpenAI 3072-dim migration).
                        await conn.execute("""
                            INSERT INTO koi_memory_chunks
                                (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding_3072)
                            VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector(3072))
                            ON CONFLICT (chunk_rid) DO UPDATE SET
                                content = EXCLUDED.content,
                                embedding_3072 = EXCLUDED.embedding_3072
                        """, chunk_rid, doc_rid, idx, len(chunk_entries),
                            chunk_content, embedding_str)
                    else:
                        await conn.execute("""
                            INSERT INTO koi_memory_chunks
                                (chunk_rid, document_rid, chunk_index, total_chunks, content)
                            VALUES ($1, $2, $3, $4, $5::jsonb)
                            ON CONFLICT (chunk_rid) DO UPDATE SET content = EXCLUDED.content
                        """, chunk_rid, doc_rid, idx, len(chunk_entries), chunk_content)

    # ========== Backoff ==========

    def _should_skip(self, wiki_id: int) -> bool:
        info = self._failures.get(wiki_id)
        if not info:
            return False
        return datetime.now(timezone.utc) < info["next_retry"]

    def _record_failure(self, wiki_id: int, msg: str):
        info = self._failures.get(wiki_id, {"count": 0})
        info["count"] += 1
        info["last_error"] = msg
        delay = min(_BACKOFF_BASE * (2 ** (info["count"] - 1)), _BACKOFF_MAX)
        info["next_retry"] = datetime.now(timezone.utc) + timedelta(seconds=delay)
        self._failures[wiki_id] = info
        logger.warning(
            f"MediaWiki wiki {wiki_id}: failure #{info['count']}, "
            f"backoff {delay}s: {msg}"
        )

    def _record_success(self, wiki_id: int):
        if wiki_id in self._failures:
            count = self._failures[wiki_id]["count"]
            del self._failures[wiki_id]
            if count > 0:
                logger.info(
                    f"MediaWiki wiki {wiki_id}: recovered after {count} failures"
                )

    # ========== API support ==========

    async def trigger_scan(self, wiki_id: Optional[int] = None) -> dict:
        """Manually trigger a scan (called from API endpoint)."""
        async with self.pool.acquire() as conn:
            if wiki_id:
                wikis = await conn.fetch(
                    "SELECT * FROM mediawiki_wikis WHERE id = $1 AND status = 'active'",
                    wiki_id,
                )
            else:
                wikis = await conn.fetch(
                    "SELECT * FROM mediawiki_wikis WHERE status = 'active' AND sync_mode = 'poll'"
                )

        if not wikis:
            return {"status": "no_wikis", "message": "No active poll-mode wikis found"}

        results = []
        for wiki_row in wikis:
            try:
                result = await self._sync_wiki(dict(wiki_row))
                results.append({
                    "wiki_id": wiki_row["id"],
                    "wiki_name": wiki_row.get("wiki_name", ""),
                    **result,
                })
            except Exception as e:
                results.append({
                    "wiki_id": wiki_row["id"],
                    "error": str(e),
                })

        self._last_scan = datetime.now(timezone.utc)
        self._scan_count += 1

        return {"status": "completed", "results": results}

    async def get_status(self) -> dict:
        """Get sensor status."""
        async with self.pool.acquire() as conn:
            wikis = await conn.fetch(
                "SELECT id, wiki_name, base_url, sync_mode, status, last_scan_at "
                "FROM mediawiki_wikis ORDER BY wiki_name"
            )
            total_pages = await conn.fetchval(
                "SELECT COUNT(*) FROM mediawiki_page_state"
            )
            ingested_pages = await conn.fetchval(
                "SELECT COUNT(*) FROM mediawiki_page_state WHERE status = 'ingested'"
            )
            total_chunks = await conn.fetchval(
                "SELECT COUNT(*) FROM koi_memory_chunks mc "
                "JOIN koi_memories m ON m.rid = mc.document_rid "
                "WHERE m.source_sensor = 'mediawiki-sensor'"
            )

        return {
            "running": self._running,
            "poll_interval_seconds": self.poll_interval,
            "last_scan": self._last_scan.isoformat() if self._last_scan else None,
            "total_scans": self._scan_count,
            "total_pages": total_pages,
            "ingested_pages": ingested_pages,
            "total_chunks": total_chunks,
            "wikis": [dict(w) for w in wikis],
            "backoff_state": {
                str(wid): {
                    "failures": info["count"],
                    "last_error": info["last_error"],
                    "next_retry": info["next_retry"].isoformat(),
                }
                for wid, info in self._failures.items()
            },
        }


def _parsed_to_dict(parsed: WikiPageParse) -> dict:
    """Convert a WikiPageParse dataclass to a dict compatible with ingest functions."""
    return {
        "page_id": parsed.page_id,
        "title": parsed.title,
        "normalized_title": parsed.normalized_title,
        "source_rid": parsed.source_rid,
        "namespace": parsed.namespace,
        "template_type": parsed.template_type,
        "bkc_entity_type": parsed.bkc_entity_type,
        "page_class": parsed.page_class,
        "is_redirect": parsed.is_redirect,
        "redirect_target": parsed.redirect_target,
        "content_hash": parsed.content_hash,
        "revision_id": parsed.revision_id,
        "word_count": parsed.word_count,
        "wikilinks": [
            {"target": wl.target, "display_text": wl.display_text,
             "section": wl.section, "is_category": wl.is_category}
            for wl in parsed.wikilinks
        ],
        "template_fields": parsed.template_fields,
        "aliases": parsed.aliases,
        "entity_density_score": parsed.entity_density_score,
        "ingest_confidence": parsed.ingest_confidence,
        "promotion_priority": parsed.promotion_priority,
        "parse_version": parsed.parse_version,
        "structural_edges": [
            {"target_title": se.target_title, "predicate": se.predicate,
             "target_type_hint": se.target_type_hint, "field_name": se.field_name,
             "confidence": se.confidence, "source_section": se.source_section}
            for se in parsed.structural_edges
        ],
        "editorial_edges": [
            {"target_title": ee.target_title, "source_section": ee.source_section,
             "confidence": ee.confidence}
            for ee in parsed.editorial_edges
        ],
    }
