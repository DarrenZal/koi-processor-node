#!/usr/bin/env python3
"""
Local-path doc scanner for governed markdown docs.

Scans a local repo directory for .md files with YAML frontmatter,
chunks them, embeds with the KOI embedding provider, and stores in
koi_memories (source_sensor='doc-scanner') + koi_memory_chunks.

Can also run in sensor mode (`--watch`) with:
- low-latency watcher-triggered scans for fresh updates
- periodic reconcile scans that prune deleted/unindexed docs

Usage:
    cd /path/to/koi-processor
    source config/personal.env
    python scripts/doc_scanner.py /path/to/repo [--repo-name NAME] [--dry-run] [--force]
    python scripts/doc_scanner.py /path/to/repo --watch [--scan-interval 300] [--reconcile-interval 21600]

Options:
    --repo-name NAME   Override repo name (default: last path component)
    --dry-run          Parse and report without writing to DB
    --force            Re-index even if content hash unchanged
    --doc-id-only      Only index files that have a doc_id in frontmatter
    --watch            Run continuously as a local repo-doc sensor
    --scan-interval    Seconds between non-watcher scans in watch mode
    --reconcile-interval
                       Seconds between reconcile scans (deletion/drift cleanup)
    --no-watcher       Disable watchdog watcher and rely on timed scans only
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import yaml

# Add repo root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from api.embedding_provider import OpenAIEmbeddingProvider
from api.chunker import TextChunker

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
# OpenAI text-embedding-3-large @ 3072-dim (embedding_3072 column).
# Writes to koi_memory_chunks.embedding_3072.
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "3072"))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

EXCLUDE_DIRS = {"node_modules", "venv", ".venv", "__pycache__", ".git", "dist", "build", "archive"}
CHUNK_SIZE = 500
CHUNK_OVERLAP = 50
DEFAULT_SCAN_INTERVAL = int(os.getenv("REPO_DOC_SENSOR_SCAN_INTERVAL", "300"))
DEFAULT_RECONCILE_INTERVAL = int(os.getenv("REPO_DOC_SENSOR_RECONCILE_INTERVAL", "21600"))
WATCHER_DEBOUNCE_MS = int(os.getenv("REPO_DOC_SENSOR_WATCHER_DEBOUNCE_MS", "500"))


# ── Frontmatter parsing ───────────────────────────────────────────────────────

def parse_frontmatter(content: str) -> Tuple[Dict[str, Any], str]:
    """
    Extract YAML frontmatter from markdown content.
    Returns (frontmatter_dict, body_text).
    On any parse error, returns ({}, content).
    """
    if not content.startswith("---"):
        return {}, content

    try:
        end = content.index("\n---\n", 3)
        raw_yaml = content[3:end].strip()
        body = content[end + 5:].strip()
        data = yaml.safe_load(raw_yaml) or {}
        if not isinstance(data, dict):
            return {}, content
        return data, body
    except (ValueError, yaml.YAMLError):
        return {}, content


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def is_governed(fm: Dict[str, Any]) -> bool:
    """A doc is governed if it has a doc_id."""
    return bool(fm.get("doc_id"))


# ── DB helpers ────────────────────────────────────────────────────────────────

async def get_existing_docs(conn: asyncpg.Connection, repo_name: str) -> Dict[str, Dict[str, Any]]:
    """Returns {rel_path: {content_hash, rid}} for already-indexed docs in this repo."""
    rows = await conn.fetch("""
        SELECT rid,
               metadata->>'rel_path' AS rel_path,
               metadata->>'content_hash' AS content_hash
        FROM koi_memories
        WHERE source_sensor = 'doc-scanner'
          AND metadata->>'repo' = $1
    """, repo_name)
    return {
        r["rel_path"]: {"content_hash": r["content_hash"], "rid": r["rid"]}
        for r in rows
        if r["rel_path"]
    }


async def delete_docs(conn: asyncpg.Connection, repo_name: str, rel_paths: List[str]) -> int:
    """Delete docs and chunks for rel_paths in this repo. Returns deleted count."""
    if not rel_paths:
        return 0

    rids = [f"doc-scanner:{repo_name}:{rel_path}" for rel_path in rel_paths]
    await conn.execute("""
        DELETE FROM koi_memory_chunks
        WHERE document_rid = ANY($1::text[])
    """, rids)
    result = await conn.execute("""
        DELETE FROM koi_memories
        WHERE source_sensor = 'doc-scanner'
          AND metadata->>'repo' = $1
          AND metadata->>'rel_path' = ANY($2::text[])
    """, repo_name, rel_paths)
    return int(result.split()[-1]) if result.startswith("DELETE ") else 0


async def upsert_doc(
    conn: asyncpg.Connection,
    rid: str,
    repo_name: str,
    rel_path: str,
    frontmatter: Dict[str, Any],
    body_text: str,
    chash: str,
) -> str:
    """Upsert into koi_memories. Returns the memory UUID."""
    doc_content = {
        "title": frontmatter.get("doc_id") or rel_path,
        "text": body_text,
        "file_path": rel_path,
    }
    doc_metadata: Dict[str, Any] = {
        "repo": repo_name,
        "rel_path": rel_path,
        "content_hash": chash,
        "is_governed": is_governed(frontmatter),
    }
    # Capture governed doc fields
    for field in ("doc_id", "doc_kind", "status", "visibility"):
        val = frontmatter.get(field)
        if val:  # skip empty strings and None
            doc_metadata[field] = val
    if "depends_on" in frontmatter:
        deps = frontmatter["depends_on"]
        doc_metadata["depends_on"] = deps if isinstance(deps, list) else [deps]

    existing = await conn.fetchrow("SELECT id FROM koi_memories WHERE rid = $1", rid)
    event_type = "NEW" if existing is None else "UPDATE"

    memory_id = await conn.fetchval("""
        INSERT INTO koi_memories (id, rid, event_type, source_sensor, content, metadata)
        VALUES ($1, $2, $3, 'doc-scanner', $4::jsonb, $5::jsonb)
        ON CONFLICT (rid) DO UPDATE SET
            event_type = EXCLUDED.event_type,
            content = EXCLUDED.content,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING id
    """, uuid.uuid4(), rid, event_type,
        json.dumps(doc_content), json.dumps(doc_metadata))

    return str(memory_id)


async def upsert_chunks(
    conn: asyncpg.Connection,
    rid: str,
    chunks: List[Dict[str, Any]],
    embeddings: List[Optional[List[float]]],
    frontmatter: Dict[str, Any],
    repo_name: str,
    rel_path: str,
):
    """Delete old chunks and insert new ones with embeddings."""
    await conn.execute("DELETE FROM koi_memory_chunks WHERE document_rid = $1", rid)

    chunk_meta: Dict[str, Any] = {
        "repo": repo_name,
        "rel_path": rel_path,
        "is_governed": is_governed(frontmatter),
    }
    for field in ("doc_id", "doc_kind", "status", "visibility"):
        if field in frontmatter:
            chunk_meta[field] = frontmatter[field]

    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        chunk_rid = f"{rid}:chunk:{i}"
        chunk_content = {
            "text": chunk["text"],
            "context": frontmatter.get("doc_id") or rel_path,
        }
        chunk_meta_i = dict(chunk_meta)
        if emb is None:
            chunk_meta_i["embedding_failed"] = True
        emb_str = json.dumps(emb) if emb is not None else None
        # Writes to embedding_3072 (OpenAI 3072-dim).
        await conn.execute("""
            INSERT INTO koi_memory_chunks
                (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding_3072, metadata)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector(3072), $7::jsonb)
            ON CONFLICT (chunk_rid) DO UPDATE SET
                chunk_index = EXCLUDED.chunk_index,
                total_chunks = EXCLUDED.total_chunks,
                content = EXCLUDED.content,
                embedding_3072 = EXCLUDED.embedding_3072,
                metadata = EXCLUDED.metadata
        """, chunk_rid, rid, i, len(chunks),
            json.dumps(chunk_content), emb_str, json.dumps(chunk_meta_i))


# ── Scanner ───────────────────────────────────────────────────────────────────

async def scan_repo(
    repo_path: Path,
    repo_name: str,
    dry_run: bool,
    force: bool,
    doc_id_only: bool,
    delete_missing: bool = True,
) -> Dict[str, int]:
    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
    if not OPENAI_API_KEY:
        raise RuntimeError(
            "OPENAI_API_KEY not set — required for doc_scanner post-2026-04-23 "
            "OpenAI 3072-dim migration. Source config/personal.env or export the key."
        )
    embedder = OpenAIEmbeddingProvider(
        api_key=OPENAI_API_KEY,
        model=EMBEDDING_MODEL,
        dimension=EMBEDDING_DIMENSION,
    )
    chunker = TextChunker(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)

    async with pool.acquire() as conn:
        existing = await get_existing_docs(conn, repo_name)
    existing_hashes = {
        rel_path: row.get("content_hash")
        for rel_path, row in existing.items()
    }

    md_files = sorted([
        p for p in repo_path.rglob("*.md")
        if not any(part in EXCLUDE_DIRS for part in p.parts)
    ])

    logger.info("Found %d .md files in %s", len(md_files), repo_path)

    stats = {"scanned": 0, "indexed": 0, "skipped": 0, "errors": 0, "deleted": 0}
    current_indexable_paths = set()

    for fpath in md_files:
        rel_path = str(fpath.relative_to(repo_path))
        try:
            raw = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            logger.warning("Read error %s: %s", rel_path, e)
            stats["errors"] += 1
            continue

        fm, body = parse_frontmatter(raw)
        stats["scanned"] += 1

        if doc_id_only and not is_governed(fm):
            stats["skipped"] += 1
            continue
        current_indexable_paths.add(rel_path)

        chash = content_hash(raw)
        rid = f"doc-scanner:{repo_name}:{rel_path}"

        if not force and existing_hashes.get(rel_path) == chash:
            logger.debug("Unchanged %s", rel_path)
            stats["skipped"] += 1
            continue

        governed_marker = " [governed]" if is_governed(fm) else ""
        logger.info("Indexing %s%s", rel_path, governed_marker)

        if dry_run:
            print(f"  DRY-RUN: {rel_path} | doc_id={fm.get('doc_id')} | doc_kind={fm.get('doc_kind')} | status={fm.get('status')}")
            stats["indexed"] += 1
            continue

        # Chunk
        chunks = chunker.chunk_text(body or raw)
        if not chunks:
            logger.warning("No chunks produced for %s, skipping", rel_path)
            stats["skipped"] += 1
            continue

        # Embed chunks one at a time (batch calls can time out on large docs)
        embeddings: List[Optional[List[float]]] = []
        for chunk in chunks:
            try:
                emb = await embedder.embed(chunk["text"])
                embeddings.append(emb)
            except Exception as e:
                logger.warning("Embedding failed for %s chunk: %s", rel_path, e)
                embeddings.append(None)

        async with pool.acquire() as conn:
            await upsert_doc(conn, rid, repo_name, rel_path, fm, body, chash)
            await upsert_chunks(conn, rid, chunks, embeddings, fm, repo_name, rel_path)

        stats["indexed"] += 1

    if delete_missing:
        stale_paths = sorted(set(existing.keys()) - current_indexable_paths)
        if stale_paths:
            if dry_run:
                logger.info("DRY-RUN would prune %d stale docs from %s", len(stale_paths), repo_name)
                stats["deleted"] = len(stale_paths)
            else:
                async with pool.acquire() as conn:
                    stats["deleted"] = await delete_docs(conn, repo_name, stale_paths)
                logger.info("Pruned %d stale docs from %s", stats["deleted"], repo_name)

    await pool.close()

    print(f"\nScan complete: {stats}")
    print(f"  Scanned:  {stats['scanned']}")
    print(f"  Indexed:  {stats['indexed']}")
    print(f"  Skipped:  {stats['skipped']}")
    print(f"  Deleted:  {stats['deleted']}")
    print(f"  Errors:   {stats['errors']}")
    return stats


class RepoDocWatcher:
    """File watcher using watchdog — signals change_event on markdown modifications."""

    def __init__(self, repo_path: Path, change_event: asyncio.Event, loop: asyncio.AbstractEventLoop):
        self._repo_path = repo_path
        self._change_event = change_event
        self._loop = loop
        self._observer = None
        self._debounce_handle: Optional[asyncio.TimerHandle] = None
        self._events_received = 0
        self._events_coalesced = 0

    @property
    def events_received(self) -> int:
        return self._events_received

    @property
    def events_coalesced(self) -> int:
        return self._events_coalesced

    def start(self) -> bool:
        """Start watcher. Returns True on success, False if watchdog unavailable."""
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class _Handler(FileSystemEventHandler):
                def __init__(inner_self, watcher: "RepoDocWatcher"):
                    super().__init__()
                    inner_self._watcher = watcher

                def on_any_event(inner_self, event):
                    if event.is_directory:
                        return
                    candidates = [getattr(event, "src_path", ""), getattr(event, "dest_path", "")]
                    if any(inner_self._watcher._is_relevant_path(path) for path in candidates if path):
                        inner_self._watcher._loop.call_soon_threadsafe(inner_self._watcher._on_fs_event)

            self._observer = Observer()
            self._observer.schedule(_Handler(self), str(self._repo_path), recursive=True)
            self._observer.start()
            return True
        except (ImportError, OSError) as exc:
            logger.warning("repo_doc_sensor.watcher_unavailable reason=%s", exc)
            return False

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None
        if self._debounce_handle:
            self._debounce_handle.cancel()
            self._debounce_handle = None

    def _is_relevant_path(self, path: str) -> bool:
        if not path.endswith(".md") and not path.endswith(".md.tmp"):
            return False
        path_obj = Path(path)
        return not any(part in EXCLUDE_DIRS for part in path_obj.parts)

    def _on_fs_event(self):
        self._events_received += 1
        if self._debounce_handle:
            self._debounce_handle.cancel()
            self._events_coalesced += 1
        self._debounce_handle = self._loop.call_later(
            WATCHER_DEBOUNCE_MS / 1000.0,
            self._signal,
        )

    def _signal(self):
        self._change_event.set()
        self._debounce_handle = None


class RepoDocSensor:
    """Local repo-doc sensor: watcher-triggered scans + periodic reconcile."""

    def __init__(
        self,
        repo_path: Path,
        repo_name: str,
        dry_run: bool,
        doc_id_only: bool,
        scan_interval: int,
        reconcile_interval: int,
        watcher_enabled: bool,
    ):
        self.repo_path = repo_path
        self.repo_name = repo_name
        self.dry_run = dry_run
        self.doc_id_only = doc_id_only
        self.scan_interval = scan_interval
        self.reconcile_interval = reconcile_interval
        self.watcher_enabled = watcher_enabled
        self._change_event = asyncio.Event()
        self._watcher: Optional[RepoDocWatcher] = None
        self._last_scan_at: Optional[datetime] = None
        self._last_reconcile_at: Optional[datetime] = None
        self._scan_in_progress = False

    def start_watcher(self):
        if not self.watcher_enabled:
            logger.info("repo_doc_sensor.watcher_disabled")
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning("repo_doc_sensor.watcher_no_event_loop")
            return

        self._watcher = RepoDocWatcher(self.repo_path, self._change_event, loop)
        if self._watcher.start():
            logger.info("repo_doc_sensor.watcher_started path=%s", self.repo_path)
        else:
            self._watcher = None

    def stop_watcher(self):
        if self._watcher:
            self._watcher.stop()
            self._watcher = None

    async def run_cycle(self):
        if self._scan_in_progress:
            logger.debug("repo_doc_sensor.scan_already_in_progress repo=%s", self.repo_name)
            return

        now = datetime.now(timezone.utc)
        watcher_triggered = self._change_event.is_set()
        if watcher_triggered:
            self._change_event.clear()
            if self._watcher:
                logger.info(
                    "repo_doc_sensor.watcher_triggered repo=%s events=%d coalesced=%d",
                    self.repo_name,
                    self._watcher.events_received,
                    self._watcher.events_coalesced,
                )

        due_scan = (
            watcher_triggered
            or self._last_scan_at is None
            or (now - self._last_scan_at).total_seconds() >= self.scan_interval
        )
        due_reconcile = (
            self._last_reconcile_at is None
            or (now - self._last_reconcile_at).total_seconds() >= self.reconcile_interval
        )
        if not due_scan and not due_reconcile:
            return

        self._scan_in_progress = True
        reason_bits = []
        if watcher_triggered:
            reason_bits.append("watcher")
        if due_scan and not watcher_triggered:
            reason_bits.append("interval")
        if due_reconcile:
            reason_bits.append("reconcile")
        reason = "+".join(reason_bits) or "scan"
        logger.info("repo_doc_sensor.scan_start repo=%s reason=%s", self.repo_name, reason)

        try:
            await scan_repo(
                self.repo_path,
                self.repo_name,
                self.dry_run,
                force=False,
                doc_id_only=self.doc_id_only,
                delete_missing=due_reconcile,
            )
        finally:
            completed_at = datetime.now(timezone.utc)
            self._last_scan_at = completed_at
            if due_reconcile:
                self._last_reconcile_at = completed_at
            self._scan_in_progress = False

    async def serve_forever(self):
        self.start_watcher()
        logger.info(
            "repo_doc_sensor.started repo=%s scan_interval=%ss reconcile_interval=%ss watcher=%s",
            self.repo_name,
            self.scan_interval,
            self.reconcile_interval,
            bool(self._watcher),
        )
        try:
            while True:
                await self.run_cycle()
                now = datetime.now(timezone.utc)
                next_scan_in = (
                    0 if self._last_scan_at is None
                    else max(0.0, self.scan_interval - (now - self._last_scan_at).total_seconds())
                )
                next_reconcile_in = (
                    0 if self._last_reconcile_at is None
                    else max(0.0, self.reconcile_interval - (now - self._last_reconcile_at).total_seconds())
                )
                timeout = max(1.0, min(next_scan_in, next_reconcile_in, 60.0))
                try:
                    await asyncio.wait_for(self._change_event.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    pass
        finally:
            self.stop_watcher()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo_path", help="Path to repo root")
    parser.add_argument("--repo-name", help="Override repo name (default: dir name)")
    parser.add_argument("--dry-run", action="store_true", help="Parse without writing")
    parser.add_argument("--force", action="store_true", help="Re-index unchanged files")
    parser.add_argument("--doc-id-only", action="store_true",
                        help="Only index files with doc_id frontmatter")
    parser.add_argument("--watch", action="store_true",
                        help="Run continuously as a repo-doc sensor")
    parser.add_argument("--scan-interval", type=int, default=DEFAULT_SCAN_INTERVAL,
                        help=f"Timed scan interval in watch mode (default: {DEFAULT_SCAN_INTERVAL}s)")
    parser.add_argument("--reconcile-interval", type=int, default=DEFAULT_RECONCILE_INTERVAL,
                        help=f"Reconcile interval in watch mode (default: {DEFAULT_RECONCILE_INTERVAL}s)")
    parser.add_argument("--no-watcher", action="store_true",
                        help="Disable watchdog-based watcher in watch mode")
    args = parser.parse_args()

    repo_path = Path(args.repo_path).resolve()
    if not repo_path.is_dir():
        print(f"Error: {repo_path} is not a directory")
        sys.exit(1)

    repo_name = args.repo_name or repo_path.name
    if args.watch:
        if args.force:
            print("Error: --force is not supported with --watch")
            sys.exit(1)
        sensor = RepoDocSensor(
            repo_path=repo_path,
            repo_name=repo_name,
            dry_run=args.dry_run,
            doc_id_only=args.doc_id_only,
            scan_interval=args.scan_interval,
            reconcile_interval=args.reconcile_interval,
            watcher_enabled=not args.no_watcher,
        )
        try:
            asyncio.run(sensor.serve_forever())
        except KeyboardInterrupt:
            logger.info("repo_doc_sensor.stopped repo=%s", repo_name)
    else:
        asyncio.run(scan_repo(repo_path, repo_name, args.dry_run, args.force, args.doc_id_only))


if __name__ == "__main__":
    main()
