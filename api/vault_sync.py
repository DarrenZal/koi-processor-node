"""
Vault Sync Manager — bidirectional markdown sync between KOI-net peers.

Phase Sync-1: two peers, poll-based (~60s), conflict copies, markdown only.
Phase Sync-1.5: observability, backpressure, reconcile, file watcher.

Reuses the existing EventQueue, poller, and signing infrastructure.
"""

from __future__ import annotations

import asyncio
import dataclasses
import fnmatch
import hashlib
import itertools
import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from api.federation_blocks import FederationBlocksUnavailable, blocked_hashes

logger = logging.getLogger(__name__)

MAX_VAULT_FILE_BYTES = 1_048_576  # 1 MB
VAULT_SYNC_PATTERNS = ("*.md", "*.jsonl", "*.json", "*.jsonld", "*.txt", "*.csv", "*.yaml", "*.yml", "*.toml")

# Paths listed here are authoritative on this node — incoming UPDATE/FORGET events from
# peers are rejected so remote enrichment never overwrites locally-owned files.
# Comma-separated folder prefixes, e.g. "Meetings/,People/,Organizations/,Projects/"
_VAULT_READONLY_INCOMING_PATHS: list = [
    p.strip().rstrip("/") + "/"
    for p in os.getenv("KOI_VAULT_READONLY_PATHS", "").split(",")
    if p.strip()
]
# Paths excluded from vault sync entirely (all event types rejected).
# Hidden-directory components (any segment starting with ".") are always excluded.
# Add explicit overrides via KOI_VAULT_EXCLUDE_PATHS, comma-separated.
# Patterns with glob metacharacters (*, ?, [seq]) match the full relative_path
# via fnmatch — e.g. "*(conflict *).md" excludes vault-sync conflict copies.
# Patterns without metacharacters use legacy prefix semantics — e.g.
# "Shared/messages/" excludes everything under that folder.
_VAULT_EXCLUDE_PATTERNS: list = [
    p.strip()
    for p in os.getenv("KOI_VAULT_EXCLUDE_PATHS", "").split(",")
    if p.strip()
]
# Mirror paths — incoming events overwrite local content unconditionally
# (no conflict-copy generation). Use on the MIRROR-side node when a peer
# OWNS the path (via KOI_VAULT_READONLY_PATHS). Same parser semantics as
# KOI_VAULT_EXCLUDE_PATHS (fnmatch + startswith fallback).
_VAULT_MIRROR_PATTERNS: list = [
    p.strip()
    for p in os.getenv("KOI_VAULT_MIRROR_PATHS", "").split(",")
    if p.strip()
]
# Per-file-origin ownership (KOI_VAULT_OWNER_UPDATE_ACCEPT). When true, a readonly
# path accepts an incoming UPDATE iff the file's stored origin_node == the event's
# origin_node — the original author (e.g. a peer's pipeline) updating a note it
# created. Default OFF: deploying the code is inert until the owner opts in; rollback
# = unset + restart. FORGET is never accepted this way (the home node stays durable).
_VAULT_OWNER_UPDATE_ACCEPT: bool = os.getenv(
    "KOI_VAULT_OWNER_UPDATE_ACCEPT", "false"
).strip().lower() in ("1", "true", "yes", "on")
# Stub-collapse guard (0 disables): refuse an owner UPDATE whose content is
# < RATIO * current file size when current size >= MIN_BYTES. base_hash cannot
# catch a rich->stub regression; this can.
_VAULT_OWNER_UPDATE_SHRINK_RATIO: float = float(
    os.getenv("KOI_VAULT_OWNER_UPDATE_SHRINK_RATIO", "0.5") or 0
)
_VAULT_OWNER_UPDATE_SHRINK_MIN_BYTES: int = int(
    os.getenv("KOI_VAULT_OWNER_UPDATE_SHRINK_MIN_BYTES", "512") or 0
)
_GLOB_META_RE = re.compile(r"[*?\[]")


def _matches_pattern_set(rel_path: str, patterns: list) -> bool:
    for pat in patterns:
        if _GLOB_META_RE.search(pat):
            if fnmatch.fnmatchcase(rel_path, pat):
                return True
        else:
            prefix = pat.rstrip("/") + "/"
            if rel_path.startswith(prefix):
                return True
    return False


def _path_excluded(rel_path: str) -> bool:
    """Return True if rel_path matches any KOI_VAULT_EXCLUDE_PATHS pattern.

    Glob patterns (containing *, ?, [seq]) are matched via fnmatchcase
    against the full relative_path (case-sensitive, matching Linux/macOS
    APFS default behavior). Patterns without metacharacters are treated
    as folder prefixes (legacy semantics).
    """
    return _matches_pattern_set(rel_path, _VAULT_EXCLUDE_PATTERNS)


def _path_in_mirror(rel_path: str) -> bool:
    """Return True if rel_path matches any KOI_VAULT_MIRROR_PATHS pattern.

    Same matching semantics as _path_excluded (fnmatch + startswith).
    """
    return _matches_pattern_set(rel_path, _VAULT_MIRROR_PATTERNS)


# Startup overlap warning: paths in BOTH READONLY and MIRROR are
# ambiguous. READONLY wins (owner is safer default), but log so operator
# can clean up. We test by applying each MIRROR pattern to each READONLY
# prefix and vice versa.
def _check_readonly_mirror_overlap() -> list:
    """Return list of (readonly_pattern, mirror_pattern) overlapping pairs."""
    overlaps = []
    for ro_pat in _VAULT_READONLY_INCOMING_PATHS:
        # READONLY uses startswith semantics — synthesize a sample path
        # that any glob would have to match.
        sample = ro_pat + "sample.md"
        if _matches_pattern_set(sample, _VAULT_MIRROR_PATTERNS):
            for mp in _VAULT_MIRROR_PATTERNS:
                if _GLOB_META_RE.search(mp):
                    if fnmatch.fnmatchcase(sample, mp):
                        overlaps.append((ro_pat, mp))
                else:
                    prefix = mp.rstrip("/") + "/"
                    if sample.startswith(prefix) or prefix.startswith(ro_pat):
                        overlaps.append((ro_pat, mp))
    return overlaps


# Emit overlap warning at module load (READONLY-wins precedence still applies).
_RO_MIRROR_OVERLAPS = _check_readonly_mirror_overlap()
if _RO_MIRROR_OVERLAPS:
    logger.warning(
        "vault_sync.readonly_mirror_overlap pairs=%s — READONLY wins; clean up config",
        _RO_MIRROR_OVERLAPS,
    )
if _VAULT_MIRROR_PATTERNS:
    logger.info(
        "vault_sync.mirror_mode_active patterns=%s — incoming events for these paths "
        "will overwrite local content unconditionally (no conflict copies generated)",
        _VAULT_MIRROR_PATTERNS,
    )

_VAULT_SYNC_EXTENSIONS = tuple("." + p.lstrip("*.") for p in VAULT_SYNC_PATTERNS)  # (".md", ".jsonl", ...)
DEFAULT_SCAN_INTERVAL = 60  # seconds
DEFAULT_RECONCILE_INTERVAL = 6 * 3600  # 6 hours
TOMBSTONE_CLEANUP_DAYS = 30
DEDUP_CLEANUP_DAYS = 90
EVENT_QUEUE_RETENTION_DAYS = int(os.getenv("EVENT_QUEUE_RETENTION_DAYS", "90"))

# Auto-repair constants
AUTO_REPAIR_ENABLED = os.getenv("VAULT_SYNC_AUTO_REPAIR", "false").strip().lower() in ("1", "true", "yes", "on")
AUTO_REPAIR_MAX_DRIFT = int(os.getenv("VAULT_SYNC_AUTO_REPAIR_MAX_DRIFT", "10"))
AUTO_REPAIR_BACKOFF_LIMIT = 3  # consecutive failures before disabling
WRITE_DEBOUNCE_MS = 500

# Backpressure constants (WP3)
MAX_FILES_PER_SCAN = int(os.getenv("VAULT_SYNC_MAX_FILES_PER_SCAN", "100"))
MAX_BYTES_PER_SCAN = int(os.getenv("VAULT_SYNC_MAX_BYTES_PER_SCAN", str(10 * 1024 * 1024)))  # 10 MB
MAX_EVENTS_PER_SCAN = max(1, int(os.getenv("VAULT_SYNC_MAX_EVENTS_PER_SCAN", "200")))
DELETE_EVENT_RESERVE = int(os.getenv("VAULT_SYNC_DELETE_EVENT_RESERVE", "50"))
DELETE_EVENT_RESERVE = max(0, min(DELETE_EVENT_RESERVE, MAX_EVENTS_PER_SCAN))

# Watcher constants (WP4)
WATCHER_DEBOUNCE_MS = int(os.getenv("VAULT_SYNC_WATCHER_DEBOUNCE_MS", "500"))

# Metrics persistence
METRICS_PERSIST_EVERY_N_SCANS = 5
METRICS_PERSIST_EVERY_SECS = 300


class VaultUnavailableError(Exception):
    """Raised when vault/shared folder is not accessible."""
    pass


@dataclasses.dataclass
class SyncMetrics:
    schema_version: int = 1
    # Scan-side
    files_scanned: int = 0
    events_queued: int = 0
    bytes_queued: int = 0
    scans_completed: int = 0
    # Apply-side
    events_applied: int = 0
    events_skipped_dedup: int = 0
    conflicts_created: int = 0
    # Rejections (replaces _rejected_counts)
    rejected_path_traversal: int = 0
    rejected_oversize: int = 0
    rejected_missing_fields: int = 0
    rejected_invalid_type: int = 0
    rejected_integrity_mismatch: int = 0
    rejected_stale_event: int = 0
    rejected_unauthorized_source: int = 0
    rejected_path_authoritative: int = 0
    rejected_path_excluded: int = 0
    # Backpressure (WP3)
    scans_capped: int = 0
    # Watcher (WP4)
    watcher_enabled: bool = False
    watcher_events_received: int = 0
    watcher_triggered_scans: int = 0
    watcher_events_coalesced: int = 0
    # Auto-repair
    auto_repairs_attempted: int = 0
    auto_repairs_succeeded: int = 0
    auto_repairs_failed: int = 0
    # Event queue cleanup
    event_queue_cleaned: int = 0
    # Transient apply-side errors (race conditions etc.) — events deferred for retry
    transient_errors: int = 0
    # Mirror-mode counters (KOI_VAULT_MIRROR_PATHS)
    mirror_overwrite: int = 0
    mirror_forget: int = 0
    mirror_authorship_change: int = 0
    mirror_forget_rejected_no_state: int = 0
    mirror_forget_rejected_non_owner: int = 0
    # Per-file-origin ownership (KOI_VAULT_OWNER_UPDATE_ACCEPT)
    readonly_owner_update_accepted: int = 0
    readonly_owner_update_shrink_guarded: int = 0
    # Timestamps
    last_scan_started_at: Optional[str] = None
    last_scan_completed_at: Optional[str] = None
    last_scan_duration_ms: int = 0
    last_apply_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SyncMetrics":
        if d.get("schema_version") != 1:
            return cls()
        fields = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in fields})


_REJECT_FIELD_MAP = {
    "path_traversal": "rejected_path_traversal",
    "oversize": "rejected_oversize",
    "missing_fields": "rejected_missing_fields",
    "invalid_type": "rejected_invalid_type",
    "integrity_mismatch": "rejected_integrity_mismatch",
    "stale_event": "rejected_stale_event",
    "unauthorized_source": "rejected_unauthorized_source",
    "path_authoritative_local": "rejected_path_authoritative",
    "path_excluded": "rejected_path_excluded",
}


class VaultWatcher:
    """File system watcher using watchdog — signals change_event on *.md modifications."""

    def __init__(self, vault_path: Path, shared_folder: str, change_event: asyncio.Event, loop: asyncio.AbstractEventLoop):
        self._vault_path = vault_path
        self._shared_folder = shared_folder
        self._change_event = change_event
        self._loop = loop
        self._events_received = 0
        self._events_coalesced = 0
        self._debounce_handle: Optional[asyncio.TimerHandle] = None
        self._observer = None

    @property
    def events_received(self) -> int:
        return self._events_received

    @property
    def events_coalesced(self) -> int:
        return self._events_coalesced

    def start(self) -> bool:
        """Start watching. Returns True on success, False if watchdog unavailable."""
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            watch_dir = str(self._vault_path / self._shared_folder)

            class _Handler(FileSystemEventHandler):
                def __init__(inner_self, watcher):
                    super().__init__()
                    inner_self._watcher = watcher

                def on_any_event(inner_self, event):
                    if event.is_directory:
                        return
                    src = getattr(event, "src_path", "")
                    if any(src.endswith(ext) or src.endswith(ext + ".tmp") for ext in _VAULT_SYNC_EXTENSIONS):
                        inner_self._watcher._loop.call_soon_threadsafe(inner_self._watcher._on_fs_event)

            self._observer = Observer()
            self._observer.schedule(_Handler(self), watch_dir, recursive=True)
            self._observer.start()
            return True
        except (ImportError, OSError) as e:
            logger.warning("vault_sync.watcher_unavailable reason=%s", e)
            return False

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    def _on_fs_event(self):
        """Called on the event loop thread via call_soon_threadsafe."""
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


# ---------------------------------------------------------------------------
# Vault-file RID namespace (divergence 1)
# ---------------------------------------------------------------------------
#
# `rid_lib.types` reserves the `koi-net.*` namespace for exactly two protocol
# objects — `koi-net.node` and `koi-net.edge`. `orn:koi-net.vault-file:` is
# application data wearing protocol clothes, and it squats a namespace we do not
# own.
#
# We are NOT migrating to upstream's `obsidian.note`, and the reason is measured
# rather than aesthetic: `koi-net-obsidian-manager-node`'s `ObsidianNote`
# requires a reference of exactly two '/'-separated components
# (`<vault_id>/<note_id>`), and **a vault with folders has paths deeper than
# that** — `Shared/Projects/Example.md` raises ValueError. Upstream's grammar
# cannot express a nested vault, so adopting it would break every note below the
# top folder level (about 30% of the paths in the vault this was measured on).
# That is a deliberate fork with a reason, which is the bar.
#
# Instead we move to a namespace we actually own, matching the
# `personal-koi.*` convention already used for entity/doclink/knowledge-episode.
#
# MIGRATION IS STAGED (expand → migrate → contract). Today we ACCEPT both and
# still EMIT the legacy one, so no peer breaks. Flip KOI_VAULT_FILE_NAMESPACE
# only once every peer accepts both.
#
# The cutover is safe for edge scoping because both namespaces resolve to the
# same rid type, `Vault-file` (pinned by
# tests/test_rid_type_extraction.py::test_legacy_and_migrated_namespaces_agree).
VAULT_FILE_NS_LEGACY = "koi-net.vault-file"
VAULT_FILE_NS_OWNED = "personal-koi.vault-file"

#: Namespace used for OUTBOUND RIDs. Legacy until every peer accepts both.
VAULT_FILE_NS_EMIT = os.getenv("KOI_VAULT_FILE_NAMESPACE", VAULT_FILE_NS_LEGACY)

#: Namespaces accepted on INBOUND events. Always both.
VAULT_FILE_RID_PREFIXES = (
    f"orn:{VAULT_FILE_NS_LEGACY}:",
    f"orn:{VAULT_FILE_NS_OWNED}:",
)


def vault_file_rid(relative_path: str) -> str:
    """Build the outbound RID for a vault file."""
    return f"orn:{VAULT_FILE_NS_EMIT}:{relative_path}"


def parse_vault_file_rid(rid: Any) -> Optional[str]:
    """Return the relative path from a vault-file RID in EITHER namespace.

    Returns None if the RID is not a vault-file RID at all, so callers can tell
    "not mine" from "mine but empty".
    """
    if not isinstance(rid, str):
        return None
    for prefix in VAULT_FILE_RID_PREFIXES:
        if rid.startswith(prefix):
            return rid[len(prefix):]
    return None


def vault_file_rid_sql_clause(column: str = "rid") -> str:
    """SQL matching a vault-file RID in either namespace.

    A single LIKE on the legacy prefix silently stops matching the moment
    emission flips, which would make every vault row invisible to cleanup,
    doctor and metrics queries at once.
    """
    return "(" + " OR ".join(f"{column} LIKE '{p}%'" for p in VAULT_FILE_RID_PREFIXES) + ")"


def _build_vault_manifest(
    *, relative_path: str, content_hash: str, base_hash: Optional[str],
    origin_node: str, origin_seq: int, file_size: int, event_type: str, now,
) -> Dict[str, Any]:
    """Build a vault-file manifest that satisfies the upstream KOI-net contract.

    `rid_lib.ext.Manifest` requires exactly {rid, timestamp, sha256_hash}, and
    `koi_net.protocol.Event.manifest` is typed as that model. We previously
    emitted `content_hash` and no `rid`, so a stock KOI-net node rejected every
    vault-file event we sent (verified against koi-net 2.1.2 / rid-lib 3.3.0).

    We already had all three values, so `rid` and `sha256_hash` are added rather
    than computed. Everything else is our extension and is carried alongside:
    pydantic ignores unknown keys, so upstream reads the three it needs and our
    own apply path keeps using the rest. Do not remove `content_hash` — the
    inbound path at _apply_new_or_update still reads it, and peers running the
    older shape still send it.
    """
    return {
        # --- upstream KOI-net Manifest contract ---
        "rid": vault_file_rid(relative_path),
        "timestamp": now,
        "sha256_hash": content_hash,
        # --- our extensions (additive; ignored by upstream parsers) ---
        "relative_path": relative_path,
        "content_hash": content_hash,
        "base_hash": base_hash,
        "origin_node": origin_node,
        "origin_seq": origin_seq,
        "bytes": file_size,
        "deleted": event_type == "FORGET",
    }


class VaultSyncManager:
    """Manages bidirectional markdown file sync over KOI-net events."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        node_rid: str,
        event_queue,
        vault_path: str,
        scan_interval: int = DEFAULT_SCAN_INTERVAL,
        reconcile_interval: int = DEFAULT_RECONCILE_INTERVAL,
        encryption_private_key=None,
    ):
        self.pool = pool
        self.node_rid = node_rid
        self.event_queue = event_queue
        self.vault_path = Path(vault_path).expanduser().resolve()
        self.scan_interval = scan_interval
        self.reconcile_interval = reconcile_interval
        self._scan_in_progress = False
        self._last_scan_at: Optional[datetime] = None
        self._last_reconcile_at: Optional[datetime] = None
        # Cache of (relative_path -> (mtime_ns, size, content_hash))
        self._stat_cache: Dict[str, Tuple[int, int, str]] = {}
        # Metrics (replaces _rejected_counts)
        self._metrics = SyncMetrics()
        self._metrics_scans_since_persist = 0
        self._metrics_last_persist_at: Optional[datetime] = None
        # Callables fired after each completed scan cycle; async or sync, no args.
        self.post_cycle_hooks: list = []
        # Concurrency lock
        self._write_lock = asyncio.Lock()
        # Watcher
        self._change_event = asyncio.Event()
        self._watcher: Optional[VaultWatcher] = None
        # E2EE
        self._encryption_private_key = encryption_private_key
        self._shared_key_cache: Dict[str, bytes] = {}  # peer_node_rid -> derived key
        # Auto-repair state
        self._auto_repair_enabled = AUTO_REPAIR_ENABLED
        self._auto_repair_consecutive_failures = 0
        self._last_reconcile_drift: int = 0

    # ------------------------------------------------------------------
    # Backward-compat property: _rejected_counts → metrics
    # ------------------------------------------------------------------

    @property
    def _rejected_counts(self) -> Dict[str, int]:
        return {
            "path_traversal": self._metrics.rejected_path_traversal,
            "oversize": self._metrics.rejected_oversize,
            "missing_fields": self._metrics.rejected_missing_fields,
            "invalid_type": self._metrics.rejected_invalid_type,
            "integrity_mismatch": self._metrics.rejected_integrity_mismatch,
            "stale_event": self._metrics.rejected_stale_event,
            "unauthorized_source": self._metrics.rejected_unauthorized_source,
        }

    # ------------------------------------------------------------------
    # Metrics persistence
    # ------------------------------------------------------------------

    async def load_metrics(self):
        """Load persisted metrics from DB on startup."""
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("SELECT metrics_json FROM vault_sync_metrics WHERE id=1")
                if row and row["metrics_json"]:
                    data = row["metrics_json"]
                    if isinstance(data, str):
                        data = json.loads(data)
                    self._metrics = SyncMetrics.from_dict(data)
                    logger.info("vault_sync.metrics_loaded")
        except Exception as e:
            logger.warning("vault_sync.metrics_load_failed reason=%s", e)

    async def persist_metrics(self):
        """Persist metrics to DB."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO vault_sync_metrics (id, metrics_json, updated_at)
                       VALUES (1, $1::jsonb, NOW())
                       ON CONFLICT (id) DO UPDATE SET
                           metrics_json = EXCLUDED.metrics_json,
                           updated_at = NOW()""",
                    json.dumps(self._metrics.to_dict()),
                )
            self._metrics_scans_since_persist = 0
            self._metrics_last_persist_at = datetime.now(timezone.utc)
        except Exception as e:
            logger.warning("vault_sync.metrics_persist_failed reason=%s", e)

    async def _maybe_persist_metrics(self):
        """Persist if threshold reached."""
        self._metrics_scans_since_persist += 1
        now = datetime.now(timezone.utc)
        should_persist = (
            self._metrics_scans_since_persist >= METRICS_PERSIST_EVERY_N_SCANS
            or self._metrics_last_persist_at is None
            or (now - self._metrics_last_persist_at).total_seconds() >= METRICS_PERSIST_EVERY_SECS
        )
        if should_persist:
            await self.persist_metrics()

    # ------------------------------------------------------------------
    # Watcher lifecycle
    # ------------------------------------------------------------------

    def start_watcher(self):
        """Start file watcher if enabled. Fail-open."""
        watcher_enabled = os.getenv("VAULT_SYNC_WATCHER", "true").strip().lower() in ("1", "true", "yes", "on")
        if not watcher_enabled:
            logger.info("vault_sync.watcher_disabled")
            self._metrics.watcher_enabled = False
            return

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            logger.warning("vault_sync.watcher_no_event_loop")
            self._metrics.watcher_enabled = False
            return

        peer_config = None  # We'll use default "Shared" for watcher path
        shared_folder = os.getenv("VAULT_SYNC_FOLDER", "Shared")
        self._watcher = VaultWatcher(self.vault_path, shared_folder, self._change_event, loop)
        if self._watcher.start():
            self._metrics.watcher_enabled = True
            logger.info("vault_sync.watcher_started path=%s/%s", self.vault_path, shared_folder)
        else:
            self._metrics.watcher_enabled = False
            self._watcher = None

    def stop_watcher(self):
        """Stop file watcher."""
        if self._watcher:
            self._watcher.stop()
            self._watcher = None
            self._metrics.watcher_enabled = False

    # ------------------------------------------------------------------
    # Public interface (called from poller)
    # ------------------------------------------------------------------

    async def run_cycle(self):
        """Run a scan cycle if interval has elapsed and no scan is in progress."""
        now = datetime.now(timezone.utc)

        # Scan cycle
        if self._scan_in_progress:
            logger.debug("vault_sync: scan already in progress, skipping")
            return

        # Check watcher signal
        watcher_triggered = self._change_event.is_set()
        if watcher_triggered:
            self._change_event.clear()
            self._metrics.watcher_triggered_scans += 1
            if self._watcher:
                self._metrics.watcher_events_received = self._watcher.events_received
                self._metrics.watcher_events_coalesced = self._watcher.events_coalesced

        should_scan = (
            watcher_triggered
            or self._last_scan_at is None
            or (now - self._last_scan_at).total_seconds() >= self.scan_interval
        )
        if should_scan:
            self._scan_in_progress = True
            scan_start = datetime.now(timezone.utc)
            self._metrics.last_scan_started_at = scan_start.isoformat()
            try:
                await self._scan_async()
            except Exception as e:
                logger.error("vault_sync.scan_error error=%s", e)
            finally:
                self._scan_in_progress = False
                self._last_scan_at = now
                scan_end = datetime.now(timezone.utc)
                self._metrics.last_scan_completed_at = scan_end.isoformat()
                self._metrics.last_scan_duration_ms = int((scan_end - scan_start).total_seconds() * 1000)
                self._metrics.scans_completed += 1
                await self._maybe_persist_metrics()
                for hook in self.post_cycle_hooks:
                    try:
                        import asyncio as _asyncio
                        result = hook()
                        if _asyncio.iscoroutine(result):
                            _asyncio.create_task(result)
                    except Exception as _e:
                        logger.warning("vault_sync post_cycle_hook error: %s", _e)

        # Reconcile cycle
        should_reconcile = (
            self._last_reconcile_at is None
            or (now - self._last_reconcile_at).total_seconds() >= self.reconcile_interval
        )
        if should_reconcile:
            try:
                await self._reconcile()
            except Exception as e:
                logger.error("vault_sync.reconcile_error error=%s", e)
            finally:
                self._last_reconcile_at = now

    async def apply_event(
        self,
        rid: str,
        event_type: str,
        contents: Dict[str, Any],
        manifest: Dict[str, Any],
        source_node: str,
        event_id: Optional[str] = None,
    ):
        """Apply an incoming vault-sync event from a peer."""
        # Input validation (no lock needed)
        if not isinstance(contents, dict):
            self._reject("invalid_type", rid, source_node, event_id, "contents is not a dict")
            return
        if not event_id:
            self._reject("missing_fields", rid, source_node, event_id, "event_id is missing")
            return
        if not isinstance(manifest, dict):
            self._reject("missing_fields", rid, source_node, event_id, "manifest is not a dict")
            return
        # Accept either spelling. Upstream KOI-net names this field sha256_hash
        # (rid_lib.ext.Manifest); our own emitter has always written
        # content_hash and still does, so peers on the older shape keep working.
        # Without this a stock KOI-net node's events are rejected outright.
        if not (manifest.get("content_hash") or manifest.get("sha256_hash")):
            self._reject("missing_fields", rid, source_node, event_id,
                         "manifest hash missing (neither content_hash nor sha256_hash)")
            return

        # Derive relative_path from the RID when the peer only sent the upstream
        # three-field manifest, which has no relative_path.
        relative_path = (
            manifest.get("relative_path")
            or contents.get("relative_path")
            or parse_vault_file_rid(rid)
        )
        if not relative_path:
            self._reject("missing_fields", rid, source_node, event_id, "relative_path missing")
            return

        # Validate file extension
        if not any(relative_path.endswith(ext) for ext in _VAULT_SYNC_EXTENSIONS):
            self._reject("invalid_type", rid, source_node, event_id, f"unsupported file type: {relative_path}")
            return

        # Source allowlist — must be a configured vault sync peer with matching folder
        peer = await self._get_peer_by_source(source_node, rel_path=relative_path)
        if not peer:
            self._reject("unauthorized_source", rid, source_node, event_id,
                         f"source {source_node} not a configured vault sync peer for path {relative_path}")
            return

        # Path traversal check — use THIS peer's shared_folder
        shared_folder = peer["shared_folder"]
        base_dir = (self.vault_path / shared_folder).resolve()
        target_path = (base_dir / relative_path.split("/", 1)[-1] if "/" in relative_path else base_dir / relative_path).resolve()

        # Actually, relative_path is like "Shared/Design Doc.md" — rebuild properly
        target_path = (self.vault_path / relative_path).resolve()
        if not target_path.is_relative_to(base_dir):
            self._reject("path_traversal", rid, source_node, event_id, f"relative_path escapes shared folder: {relative_path}")
            return

        # Exclude hidden directories (e.g. Shared/.obsidian/, Shared/.letta/) and
        # explicit KOI_VAULT_EXCLUDE_PATHS. These generate constant conflicts from
        # machine-local state (Obsidian workspace.json, app caches, etc.).
        _path_parts = relative_path.split("/")
        if any(part.startswith(".") for part in _path_parts[:-1]):
            self._reject("path_excluded", rid, source_node, event_id,
                         f"hidden directory in path: {relative_path}")
            return
        if _path_excluded(relative_path):
            self._reject("path_excluded", rid, source_node, event_id,
                         f"path excluded by config: {relative_path}")
            return

        # Locally-authoritative path guard MOVED inside the write lock (after the
        # stale-event guard) so it can be origin-aware — see KOI_VAULT_OWNER_UPDATE_ACCEPT.
        # Non-owner readonly UPDATE/FORGET is still rejected there; those events now run
        # E2EE-decrypt + dedup first (negligible extra work, correctness unchanged).

        # Size check
        manifest_bytes = manifest.get("bytes", 0)
        if manifest_bytes > MAX_VAULT_FILE_BYTES:
            self._reject("oversize", rid, source_node, event_id, f"manifest.bytes={manifest_bytes}")
            return

        # E2EE: decrypt if ciphertext present
        if contents.get("ciphertext") and contents.get("e2ee_version") == 1:
            shared_key = await self._get_shared_key(source_node)
            if shared_key is None:
                self._reject("missing_fields", rid, source_node, event_id,
                             "encrypted event but no shared E2EE key available")
                return
            try:
                from api.koi_encryption import decrypt_content
                markdown = decrypt_content(contents["ciphertext"], shared_key, rid)
            except Exception as e:
                self._reject("integrity_mismatch", rid, source_node, event_id,
                             f"E2EE decryption failed: {e}")
                return
        else:
            markdown = contents.get("markdown", "")
            if contents.get("ciphertext"):
                logger.warning("vault_sync.e2ee_unknown_version version=%s", contents.get("e2ee_version"))

        if not manifest.get("deleted", False):
            actual_bytes = len(markdown.encode("utf-8"))
            if actual_bytes > MAX_VAULT_FILE_BYTES:
                self._reject("oversize", rid, source_node, event_id, f"actual bytes={actual_bytes}")
                return
            # Integrity check
            if manifest_bytes and actual_bytes != manifest_bytes:
                self._reject("integrity_mismatch", rid, source_node, event_id,
                             f"manifest.bytes={manifest_bytes} != actual={actual_bytes}")
                return

        # Event-ID dedup / loop detection — check if we already applied this event_id
        # (from ANY source, not just this one — catches forwarded loops)
        async with self.pool.acquire() as conn:
            already = await conn.fetchval(
                "SELECT 1 FROM vault_sync_applied_events WHERE event_id=$1::uuid LIMIT 1",
                event_id,
            )
            if already:
                logger.debug("vault_sync.loop_detected event_id=%s source=%s", event_id, source_node)
                self._metrics.events_skipped_dedup += 1
                return

        content_hash = manifest.get("content_hash") or manifest["sha256_hash"]
        base_hash = manifest.get("base_hash")
        origin_seq = manifest.get("origin_seq", 1)
        origin_node = manifest.get("origin_node", source_node)
        deleted = manifest.get("deleted", False)
        timestamp = manifest.get("timestamp", datetime.now(timezone.utc).isoformat())

        # Mutation section — acquire lock
        async with self._write_lock:
            # Get local state under lock
            async with self.pool.acquire() as conn:
                local_row = await conn.fetchrow(
                    "SELECT * FROM vault_sync_state WHERE relative_path=$1",
                    relative_path,
                )

            # Stale-event guard: same origin, older or equal seq
            if local_row and not local_row["is_deleted"]:
                if origin_node == local_row["origin_node"] and origin_seq <= local_row["origin_seq"]:
                    self._reject("stale_event", rid, source_node, event_id,
                                 f"origin_seq {origin_seq} <= local {local_row['origin_seq']}")
                    return

            # ── PER-FILE-ORIGIN OWNERSHIP (KOI_VAULT_OWNER_UPDATE_ACCEPT) ──
            # Readonly paths reject incoming UPDATE/FORGET so remote enrichment can't
            # overwrite files THIS node authored. Exception (flag-gated, UPDATE-only):
            # accept when the file's stored origin_node == the incoming origin_node — the
            # peer that ORIGINALLY created the file (its NEW event) updating its own file
            # (e.g. a peer's pipeline enriching a Meetings/ note it authored). A note
            # this node authored (origin=self) still rejects any peer-origin UPDATE.
            if event_type in ("UPDATE", "FORGET") and _VAULT_READONLY_INCOMING_PATHS \
                    and any(relative_path.startswith(p) for p in _VAULT_READONLY_INCOMING_PATHS):
                owner_updating_own = (
                    _VAULT_OWNER_UPDATE_ACCEPT
                    and event_type == "UPDATE"        # UPDATE-only; FORGET stays rejected
                    and local_row is not None
                    and not local_row["is_deleted"]
                    and local_row["origin_node"] == origin_node
                    # A note this node authored is never updated from outside. An honest echo of
                    # our own event stops at the stale-event guard above, so a claim of our own
                    # origin that reaches this point is not an honest event. A relayed event
                    # naming a third node as origin is unaffected.
                    and origin_node != self.node_rid
                )
                if owner_updating_own and _VAULT_OWNER_UPDATE_SHRINK_RATIO > 0:
                    # Stub-collapse guard: reject a rich->stub owner UPDATE (base_hash
                    # cannot catch it — a stub legitimately bases on the prior rich hash).
                    local_len = local_row["file_size_bytes"] or 0
                    incoming_len = len(markdown.encode("utf-8"))
                    if local_len >= _VAULT_OWNER_UPDATE_SHRINK_MIN_BYTES \
                            and incoming_len < local_len * _VAULT_OWNER_UPDATE_SHRINK_RATIO:
                        self._metrics.readonly_owner_update_shrink_guarded += 1
                        self._reject("path_authoritative_local", rid, source_node, event_id,
                                     f"owner UPDATE to {relative_path} shrinks "
                                     f"{local_len}->{incoming_len} bytes; refusing stub-collapse")
                        return
                if not owner_updating_own:
                    self._reject("path_authoritative_local", rid, source_node, event_id,
                                 f"incoming {event_type} rejected: {relative_path} is locally authoritative")
                    return
                self._metrics.readonly_owner_update_accepted += 1
            # ── end per-file-origin ownership ──

            if event_type == "FORGET":
                await self._apply_forget(
                    target_path, relative_path, local_row, base_hash,
                    origin_node, origin_seq, source_node, event_id, rid,
                )
            elif event_type in ("NEW", "UPDATE"):
                await self._apply_new_or_update(
                    target_path, relative_path, local_row, event_type,
                    markdown, content_hash, base_hash, origin_node, origin_seq,
                    source_node, event_id, rid, manifest_bytes, timestamp,
                )
            else:
                self._reject("invalid_type", rid, source_node, event_id, f"unknown event_type: {event_type}")
                return

        self._metrics.events_applied += 1
        self._metrics.last_apply_at = datetime.now(timezone.utc).isoformat()
        logger.info("vault_sync.apply event_type=%s path=%s source=%s", event_type, relative_path, source_node)

        # Forward to other peers (mesh relay)
        await self._forward_to_peers(
            event_id, event_type, rid, manifest,
            markdown if event_type != "FORGET" else None,
            source_node,
        )

    async def get_status(self) -> Dict[str, Any]:
        """Return sync dashboard info."""
        peers = await self._get_all_peers()
        first_peer = peers[0] if peers else None
        async with self.pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM vault_sync_state WHERE is_deleted=FALSE")
            tombstones = await conn.fetchval("SELECT COUNT(*) FROM vault_sync_state WHERE is_deleted=TRUE")
            pending = await conn.fetchval(
                f"""SELECT COUNT(*) FROM koi_net_events
                   WHERE {vault_file_rid_sql_clause()}
                   AND expires_at > NOW()
                   AND array_length(delivered_to, 1) IS NULL""",
            )
        # Compute next reconcile time
        next_reconcile_at = None
        if self._last_reconcile_at:
            next_dt = self._last_reconcile_at + timedelta(seconds=self.reconcile_interval)
            next_reconcile_at = next_dt.isoformat()

        return {
            "enabled": len(peers) > 0,
            # Backward compat: root-level peer/shared_folder show first enabled peer
            "peer": first_peer["peer_node_rid"] if first_peer else None,
            "shared_folder": first_peer["shared_folder"] if first_peer else None,
            "peer_count": len(peers),
            "peers": [
                {
                    "peer_node_rid": p["peer_node_rid"],
                    "shared_folder": p["shared_folder"],
                    "enabled": p.get("enabled", True),
                    "last_full_sync_at": p["last_full_sync_at"].isoformat() if p.get("last_full_sync_at") else None,
                }
                for p in peers
            ],
            "files_tracked": total or 0,
            "tombstones": tombstones or 0,
            "pending_events": pending or 0,
            "last_scan_at": self._last_scan_at.isoformat() if self._last_scan_at else None,
            "last_reconcile_at": self._last_reconcile_at.isoformat() if self._last_reconcile_at else None,
            "rejected_events": dict(self._rejected_counts),
            "metrics": self._metrics.to_dict(),
            "reconcile": {
                "last_run_at": self._last_reconcile_at.isoformat() if self._last_reconcile_at else None,
                "mode": "auto-repair" if self._auto_repair_enabled else "detect",
                "drift_count": self._last_reconcile_drift,
                "next_scheduled_at": next_reconcile_at,
                "auto_repair_enabled": self._auto_repair_enabled,
                "auto_repair_consecutive_failures": self._auto_repair_consecutive_failures,
            },
        }

    async def configure(self, peer_name: str, shared_folder: str = "Shared", enabled: bool = True) -> Dict[str, Any]:
        """Configure vault sync for a peer. Resolves peer name to node_rid."""
        async with self.pool.acquire() as conn:
            # Resolve peer name to node_rid
            node_rid = await conn.fetchval(
                "SELECT node_rid FROM koi_net_peer_aliases WHERE LOWER(alias) = LOWER($1)",
                peer_name,
            )
            if not node_rid:
                node_rid = await conn.fetchval(
                    "SELECT node_rid FROM koi_net_nodes WHERE LOWER(node_name) = LOWER($1) AND status = 'active'",
                    peer_name,
                )
            if not node_rid:
                if peer_name.startswith("orn:koi-net.node:"):
                    node_rid = peer_name
                else:
                    return {"error": f"Peer '{peer_name}' not found"}

            await conn.execute(
                """INSERT INTO vault_sync_peers (peer_node_rid, shared_folder, enabled)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (peer_node_rid, shared_folder) DO UPDATE SET
                       enabled = EXCLUDED.enabled""",
                node_rid, shared_folder, enabled,
            )

            # Ensure shared folder exists
            folder_path = self.vault_path / shared_folder
            folder_path.mkdir(parents=True, exist_ok=True)

            return {
                "peer_node_rid": node_rid,
                "shared_folder": shared_folder,
                "enabled": enabled,
            }

    async def unconfigure(self, peer_name: str, shared_folder: Optional[str] = None) -> Dict[str, Any]:
        """Disable vault sync for a peer (optionally for a specific folder only)."""
        async with self.pool.acquire() as conn:
            node_rid = await conn.fetchval(
                "SELECT node_rid FROM koi_net_peer_aliases WHERE LOWER(alias) = LOWER($1)",
                peer_name,
            )
            if not node_rid:
                node_rid = await conn.fetchval(
                    "SELECT node_rid FROM koi_net_nodes WHERE LOWER(node_name) = LOWER($1) AND status = 'active'",
                    peer_name,
                )
            if not node_rid:
                if peer_name.startswith("orn:koi-net.node:"):
                    node_rid = peer_name
                else:
                    return {"error": f"Peer '{peer_name}' not found"}

            if shared_folder:
                result = await conn.execute(
                    "UPDATE vault_sync_peers SET enabled=FALSE WHERE peer_node_rid=$1 AND shared_folder=$2",
                    node_rid, shared_folder,
                )
            else:
                result = await conn.execute(
                    "UPDATE vault_sync_peers SET enabled=FALSE WHERE peer_node_rid=$1",
                    node_rid,
                )
            count = int(result.split()[-1])
            if count == 0:
                return {"error": f"Peer '{peer_name}' not configured for vault sync"}
            return {"peer_node_rid": node_rid, "shared_folder": shared_folder, "enabled": False}

    async def trigger_sync(self) -> Dict[str, Any]:
        """Force an immediate sync cycle."""
        self._last_scan_at = None  # Reset timer to force scan
        await self.run_cycle()
        return {"triggered": True, "last_scan_at": self._last_scan_at.isoformat() if self._last_scan_at else None}

    # ------------------------------------------------------------------
    # Reconciliation (WP2a detect, WP2b repair)
    # ------------------------------------------------------------------

    async def reconcile(self, mode: str = "detect", confirm: bool = False,
                        paths: Optional[List[str]] = None, max_actions: int = 50,
                        peer_rid: Optional[str] = None) -> Dict[str, Any]:
        """Detect or repair drift between DB state and filesystem.

        mode="detect": read-only drift report.
        mode="repair": queue events to fix drift (requires confirm=True).
        peer_rid: optional filter to reconcile for a specific peer only.
        """
        if peer_rid:
            peers = await self._get_all_peers_by_source(peer_rid)
        else:
            peers = await self._get_all_peers()
        if not peers:
            return {"error": "vault sync not configured or disabled"}

        # Collect unique shared folders from peers
        folder_set: set = set()
        for p in peers:
            folder_set.add(p["shared_folder"])

        # Vault root safety check — all folders must be accessible
        for folder in folder_set:
            folder_dir = self.vault_path / folder
            if not folder_dir.is_dir():
                raise VaultUnavailableError(f"shared folder not accessible: {folder_dir}")

        # Snapshot DB under brief lock — scoped to folder prefixes
        async with self._write_lock:
            async with self.pool.acquire() as conn:
                db_rows = []
                for folder in folder_set:
                    rows = await conn.fetch(
                        "SELECT relative_path, content_hash FROM vault_sync_state "
                        "WHERE is_deleted=FALSE AND relative_path LIKE $1",
                        folder + "/%",
                    )
                    db_rows.extend(rows)

        # Compare against filesystem (no lock) — scan each folder
        # A3: exclude the same paths the disk side skips, or an excluded file
        # lands in missing_on_disk and the repair path queues a FORGET for it.
        db_map = {
            r["relative_path"]: r["content_hash"]
            for r in db_rows
            if not _path_excluded(r["relative_path"])
        }
        disk_files: Dict[str, str] = {}
        # A5: a path we could not examine is UNKNOWN, not ABSENT. rglob already
        # yielded it, so the file exists; the only thing that happened is that
        # this process could not read it (permissions, transient I/O, an
        # unmaterialised cloud-sync placeholder, a concurrent writer). Without
        # this set those paths fall straight through to missing_on_disk and the
        # repair loop tombstones them and FORGETs them to every peer — a
        # permanent cross-peer delete caused by a temporary read failure.
        # Deliberate skips (symlink, excluded, hidden dir) are NOT unknown:
        # they are decisions, and they are handled by the db_map exclusion above.
        unknown_paths: set = set()
        for shared_folder in folder_set:
            base_dir = self.vault_path / shared_folder
            for md_file in itertools.chain(*(base_dir.rglob(p) for p in VAULT_SYNC_PATTERNS)):
                if md_file.is_symlink():
                    continue
                try:
                    _inner = md_file.relative_to(base_dir)
                    rel_path = f"{shared_folder}/{_inner}"
                except ValueError:
                    # Cannot even name it, so cannot judge its absence.
                    continue
                if any(part.startswith(".") for part in _inner.parts[:-1]):
                    continue
                if _path_excluded(rel_path):
                    continue
                if not md_file.is_file():
                    # Enumerated but not a regular file right now — a race with
                    # a writer, or a type change. Unknown, not absent.
                    unknown_paths.add(rel_path)
                    continue
                try:
                    content = md_file.read_bytes()
                    disk_files[rel_path] = hashlib.sha256(content).hexdigest()
                except OSError as e:
                    unknown_paths.add(rel_path)
                    logger.warning(
                        "vault_sync.reconcile_unreadable path=%s error=%s — "
                        "recorded UNKNOWN, will not be treated as deleted",
                        rel_path, e,
                    )

        missing_on_disk = []
        hash_mismatch = []
        missing_in_db = []

        # Filter to paths if specified
        check_paths = set(paths) if paths else None

        for rel_path, db_hash in db_map.items():
            if check_paths and rel_path not in check_paths:
                continue
            if rel_path in unknown_paths:
                # A5: absence was never established for this path.
                continue
            if rel_path not in disk_files:
                missing_on_disk.append(rel_path)
            elif disk_files[rel_path] != db_hash:
                hash_mismatch.append(rel_path)

        for rel_path in disk_files:
            if check_paths and rel_path not in check_paths:
                continue
            if rel_path not in db_map:
                missing_in_db.append(rel_path)

        total_drift = len(missing_on_disk) + len(hash_mismatch) + len(missing_in_db)

        if unknown_paths:
            logger.warning(
                "vault_sync.reconcile_unknown_paths count=%d — these were enumerated but "
                "could not be examined, and are excluded from missing_on_disk",
                len(unknown_paths),
            )

        result: Dict[str, Any] = {
            "mode": mode,
            "missing_on_disk": missing_on_disk,
            "missing_in_db": missing_in_db,
            "hash_mismatch": hash_mismatch,
            "total_drift": total_drift,
            # A5: report coverage, so a caller can tell a clean reconcile from
            # one that simply could not see part of the vault.
            "unknown_on_disk": sorted(unknown_paths),
            "unknown_count": len(unknown_paths),
        }

        if mode == "repair":
            if not confirm:
                result["repair_requires_confirm"] = True
                return result

            actions_taken = 0
            remaining = 0

            # Build folder→peers lookup so repair only queues to matching peers
            folder_peers: Dict[str, List[Dict[str, Any]]] = {}
            for p in peers:
                folder_peers.setdefault(p["shared_folder"], []).append(p)

            def _peers_for_path(rel_path: str) -> List[Dict[str, Any]]:
                """Return peers whose shared_folder is a prefix of rel_path."""
                matched = []
                for folder, plist in folder_peers.items():
                    if rel_path.startswith(folder + "/"):
                        matched.extend(plist)
                return matched

            # Repair missing_in_db — queue NEW events for files on disk but not in DB
            for rel_path in missing_in_db:
                if actions_taken >= max_actions:
                    remaining += 1
                    continue
                file_path = self.vault_path / rel_path
                try:
                    file_bytes = file_path.read_bytes()
                except OSError:
                    continue
                content_hash = hashlib.sha256(file_bytes).hexdigest()
                size = len(file_bytes)
                new_seq = 1
                async with self._write_lock:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            """INSERT INTO vault_sync_state
                               (relative_path, content_hash, origin_node, origin_seq, local_edit_seq, file_size_bytes, last_modified_at)
                               VALUES ($1, $2, $3, $4, $4, $5, NOW())
                               ON CONFLICT (relative_path) DO UPDATE SET
                                   content_hash = EXCLUDED.content_hash,
                                   origin_node = EXCLUDED.origin_node,
                                   origin_seq = EXCLUDED.origin_seq,
                                   local_edit_seq = EXCLUDED.local_edit_seq,
                                   file_size_bytes = EXCLUDED.file_size_bytes,
                                   is_deleted = FALSE, deleted_at = NULL,
                                   updated_at = NOW()""",
                            rel_path, content_hash, self.node_rid, new_seq, size,
                        )
                    for peer in _peers_for_path(rel_path):
                        await self._queue_event(
                            "NEW", rel_path, content_hash, None, new_seq,
                            file_bytes.decode("utf-8", errors="replace"),
                            size, peer["peer_node_rid"],
                        )
                actions_taken += 1

            # Repair hash_mismatch — queue UPDATE events (disk wins)
            for rel_path in hash_mismatch:
                if actions_taken >= max_actions:
                    remaining += 1
                    continue
                file_path = self.vault_path / rel_path
                try:
                    file_bytes = file_path.read_bytes()
                except OSError:
                    continue
                content_hash = hashlib.sha256(file_bytes).hexdigest()
                prev_hash = db_map.get(rel_path)
                size = len(file_bytes)
                async with self._write_lock:
                    async with self.pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT origin_node, origin_seq, local_edit_seq FROM vault_sync_state WHERE relative_path=$1",
                            rel_path,
                        )
                        new_seq = (row["local_edit_seq"] + 1) if row else 1
                        await conn.execute(
                            """UPDATE vault_sync_state
                               SET content_hash=$2, origin_node=$3, origin_seq=$4, local_edit_seq=$4,
                                   file_size_bytes=$5, updated_at=NOW()
                               WHERE relative_path=$1""",
                            rel_path, content_hash, self.node_rid, new_seq, size,
                        )
                    for peer in _peers_for_path(rel_path):
                        await self._queue_event(
                            "UPDATE", rel_path, content_hash, prev_hash, new_seq,
                            file_bytes.decode("utf-8", errors="replace"),
                            size, peer["peer_node_rid"],
                        )
                actions_taken += 1

            # Repair missing_on_disk — queue FORGET events
            for rel_path in missing_on_disk:
                if actions_taken >= max_actions:
                    remaining += 1
                    continue
                # A4: the enumeration above and this write are not atomic, and
                # the enumeration can miss a file that is present.
                if not self._absence_confirmed(rel_path, source="reconcile"):
                    continue
                prev_hash = db_map.get(rel_path)
                async with self._write_lock:
                    async with self.pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "SELECT origin_seq, local_edit_seq FROM vault_sync_state WHERE relative_path=$1",
                            rel_path,
                        )
                        new_seq = (row["local_edit_seq"] + 1) if row else 1
                        await conn.execute(
                            """UPDATE vault_sync_state
                               SET is_deleted=TRUE, deleted_at=NOW(), updated_at=NOW(),
                                   origin_seq=$2, local_edit_seq=$2
                               WHERE relative_path=$1""",
                            rel_path, new_seq,
                        )
                    for peer in _peers_for_path(rel_path):
                        await self._queue_event(
                            "FORGET", rel_path, prev_hash, prev_hash,
                            new_seq, None, 0, peer["peer_node_rid"],
                        )
                actions_taken += 1

            result["actions_taken"] = actions_taken
            if remaining > 0:
                result["capped"] = True
                result["remaining"] = remaining

        return result

    # ------------------------------------------------------------------
    # Internal: Local scan
    # ------------------------------------------------------------------

    async def _scan_async(self):
        """Async scan implementation — scans per-peer folders."""
        peers = await self._get_all_peers()
        if not peers:
            return

        # A scan records vault_sync_state before it queues, and the federation guard (migration 137)
        # refuses to queue without its block list. Scanning then would mark changes as sent that never
        # were, so skip the scan until the table exists.
        async with self.pool.acquire() as conn:
            try:
                await blocked_hashes(conn)
            except FederationBlocksUnavailable:
                logger.error("vault_sync.scan_skipped reason=federation_rid_blocks_missing")
                return

        # Group peers by shared_folder to scan each folder once
        folder_peers: Dict[str, List[Dict[str, Any]]] = {}
        for p in peers:
            folder_peers.setdefault(p["shared_folder"], []).append(p)

        for shared_folder, peer_list in folder_peers.items():
            await self._scan_folder(shared_folder, peer_list)

    async def _scan_folder(self, shared_folder: str, peer_list: List[Dict[str, Any]]):
        """Scan a single shared folder and queue events for matching peers."""
        base_dir = self.vault_path / shared_folder

        if not base_dir.exists():
            base_dir.mkdir(parents=True, exist_ok=True)
            logger.info("vault_sync.scan_created_folder path=%s", base_dir)

        # Initial sync check — any peer lacking last_full_sync_at counts
        is_initial = any(p.get("last_full_sync_at") is None for p in peer_list)

        # Phase 1: glob + hash (NO lock) — collect changed files
        changed_files: List[Tuple[str, str, int, float, bytes]] = []  # (rel_path, content_hash, size, mtime, file_bytes)
        seen_paths: set = set()
        files_scanned = 0
        files_changed = 0
        bytes_this_cycle = 0
        # A1: set when the glob below exits early on backpressure. A truncated
        # enumeration leaves present files out of `seen_paths`, and the deletion
        # pass treats "not in seen_paths" as "deleted" — so a partial scan must
        # never be allowed to justify a delete.
        scan_truncated = False
        create_update_budget = max(0, MAX_EVENTS_PER_SCAN - DELETE_EVENT_RESERVE)

        # A2: the mtime+size pre-check below is backed by an in-memory cache that
        # starts empty on every process start, so after a restart EVERY file is a
        # cache miss and counts toward MAX_FILES_PER_SCAN regardless of whether its
        # content actually changed — which guarantees truncation. Compare against
        # the hashes already in the DB so only genuinely-changed files consume the
        # cap. One query per folder scan, taken without the write lock.
        db_hashes: Dict[str, str] = {}
        try:
            async with self.pool.acquire() as conn:
                for r in await conn.fetch(
                    "SELECT relative_path, content_hash FROM vault_sync_state "
                    "WHERE is_deleted=FALSE AND relative_path LIKE $1",
                    shared_folder + "/%",
                ):
                    db_hashes[r["relative_path"]] = r["content_hash"]
        except Exception as e:  # never let this optimisation break a scan
            logger.warning("vault_sync.scan_hash_preload_failed folder=%s error=%s", shared_folder, e)

        for md_file in itertools.chain(*(base_dir.rglob(p) for p in VAULT_SYNC_PATTERNS)):
            if md_file.is_symlink():
                continue
            if not md_file.is_file():
                continue

            try:
                _inner = md_file.relative_to(base_dir)
                rel_path = f"{shared_folder}/{_inner}"
                _inner_parts = _inner.parts
            except ValueError:
                continue

            # Skip hidden directories (e.g. .obsidian/, .letta/) and explicit exclude paths
            if any(part.startswith(".") for part in _inner_parts[:-1]):
                continue
            if _path_excluded(rel_path):
                continue

            seen_paths.add(rel_path)
            files_scanned += 1

            # stat for mtime + size
            try:
                stat = md_file.stat()
            except OSError:
                continue

            mtime_ns = stat.st_mtime_ns
            size = stat.st_size

            # Size check
            if size > MAX_VAULT_FILE_BYTES:
                logger.warning("vault_sync.scan_skip_oversize path=%s bytes=%d", rel_path, size)
                continue

            # Fast pre-check: skip if mtime+size unchanged
            cached = self._stat_cache.get(rel_path)
            if cached and cached[0] == mtime_ns and cached[1] == size:
                continue

            # Write debounce: wait and re-check before hashing
            await asyncio.sleep(WRITE_DEBOUNCE_MS / 1000)
            try:
                stat2 = md_file.stat()
            except OSError:
                continue
            if stat2.st_mtime_ns != mtime_ns:
                continue  # File still being written

            # Read and hash
            try:
                file_bytes = md_file.read_bytes()
            except OSError as e:
                logger.warning("vault_sync.scan_read_error path=%s error=%s", rel_path, e)
                continue

            content_hash = hashlib.sha256(file_bytes).hexdigest()

            # Update stat cache
            self._stat_cache[rel_path] = (mtime_ns, size, content_hash)

            # A2: content is byte-identical to what the DB already has, so this
            # file is not "changed" — warm the cache and move on WITHOUT charging
            # it against the scan budget. Without this, a cold cache spends the
            # entire cap on unchanged files and the scan truncates every time.
            if db_hashes.get(rel_path) == content_hash:
                continue

            # Backpressure checks (WP3)
            if files_changed >= MAX_FILES_PER_SCAN:
                self._metrics.scans_capped += 1
                scan_truncated = True
                logger.info("vault_sync.scan_capped reason=max_files cap=%d", MAX_FILES_PER_SCAN)
                break
            if bytes_this_cycle + size > MAX_BYTES_PER_SCAN:
                self._metrics.scans_capped += 1
                scan_truncated = True
                logger.info("vault_sync.scan_capped reason=max_bytes cap=%d", MAX_BYTES_PER_SCAN)
                break

            changed_files.append((rel_path, content_hash, size, stat.st_mtime, file_bytes))
            files_changed += 1
            bytes_this_cycle += size

        # Phase 2: DB mutations (lock held)
        events_this_cycle = 0
        async with self._write_lock:
            for rel_path, content_hash, size, mtime, file_bytes in changed_files:
                if events_this_cycle >= create_update_budget:
                    self._metrics.scans_capped += 1
                    logger.info("vault_sync.scan_capped reason=event_budget cap=%d", create_update_budget)
                    break

                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT * FROM vault_sync_state WHERE relative_path=$1",
                        rel_path,
                    )

                    if not row:
                        # New file
                        new_seq = 1
                        await conn.execute(
                            """INSERT INTO vault_sync_state
                               (relative_path, content_hash, origin_node, origin_seq, local_edit_seq, file_size_bytes, last_modified_at)
                               VALUES ($1, $2, $3, $4, $4, $5, $6)""",
                            rel_path, content_hash, self.node_rid, new_seq, size,
                            datetime.fromtimestamp(mtime, tz=timezone.utc),
                        )
                        for peer in peer_list:
                            await self._queue_event(
                                "NEW", rel_path, content_hash, None, new_seq,
                                file_bytes.decode("utf-8", errors="replace"),
                                size, peer["peer_node_rid"],
                            )
                        events_this_cycle += 1
                        self._metrics.events_queued += 1
                        self._metrics.bytes_queued += size
                    elif row["is_deleted"]:
                        # File reappeared after deletion — treat as new
                        new_seq = row["local_edit_seq"] + 1
                        await conn.execute(
                            """UPDATE vault_sync_state
                               SET content_hash=$2, origin_node=$3, origin_seq=$4, local_edit_seq=$4,
                                   file_size_bytes=$5, last_modified_at=$6,
                                   is_deleted=FALSE, deleted_at=NULL, updated_at=NOW()
                               WHERE relative_path=$1""",
                            rel_path, content_hash, self.node_rid, new_seq, size,
                            datetime.fromtimestamp(mtime, tz=timezone.utc),
                        )
                        for peer in peer_list:
                            await self._queue_event(
                                "NEW", rel_path, content_hash, None, new_seq,
                                file_bytes.decode("utf-8", errors="replace"),
                                size, peer["peer_node_rid"],
                            )
                        events_this_cycle += 1
                        self._metrics.events_queued += 1
                        self._metrics.bytes_queued += size
                    elif row["content_hash"] != content_hash:
                        # Modified file
                        prev_hash = row["content_hash"]
                        new_seq = row["local_edit_seq"] + 1
                        await conn.execute(
                            """UPDATE vault_sync_state
                               SET content_hash=$2, origin_node=$3, origin_seq=$4, local_edit_seq=$4,
                                   file_size_bytes=$5, last_modified_at=$6, updated_at=NOW()
                               WHERE relative_path=$1""",
                            rel_path, content_hash, self.node_rid, new_seq, size,
                            datetime.fromtimestamp(mtime, tz=timezone.utc),
                        )
                        for peer in peer_list:
                            await self._queue_event(
                                "UPDATE", rel_path, content_hash, prev_hash, new_seq,
                                file_bytes.decode("utf-8", errors="replace"),
                                size, peer["peer_node_rid"],
                            )
                        events_this_cycle += 1
                        self._metrics.events_queued += 1
                        self._metrics.bytes_queued += size

            # Deletion loop — uses remaining budget up to MAX_EVENTS_PER_SCAN
            # Only check files in THIS shared_folder
            if scan_truncated:
                logger.warning(
                    "vault_sync.deletions_skipped reason=scan_truncated folder=%s files_seen=%d — "
                    "a partial enumeration cannot distinguish 'absent' from 'not reached'",
                    shared_folder, len(seen_paths),
                )
            async with self.pool.acquire() as conn:
                rows = [] if scan_truncated else await conn.fetch(
                    "SELECT relative_path, content_hash, origin_seq, local_edit_seq FROM vault_sync_state "
                    "WHERE is_deleted=FALSE AND relative_path LIKE $1",
                    shared_folder + "/%",
                )
                # A3: the glob skips excluded paths BEFORE adding them to
                # seen_paths, but this query does not exclude them — so any
                # tracked path matching an exclude pattern is in the DB set and
                # absent from seen_paths by construction, and gets tombstoned.
                # That is why every conflict copy self-destructed seconds after
                # creation (KOI_VAULT_EXCLUDE_PATHS matches '*(conflict *).md')
                # and why most of one peer's false tombstones were conflict paths.
                rows = [r for r in rows if not _path_excluded(r["relative_path"])]
                for row in rows:
                    if row["relative_path"] not in seen_paths:
                        if events_this_cycle >= MAX_EVENTS_PER_SCAN:
                            self._metrics.scans_capped += 1
                            logger.info("vault_sync.scan_capped reason=event_cap_deletes total=%d", events_this_cycle)
                            break
                        rel_path = row["relative_path"]
                        # A4: confirm absence against the filesystem before the
                        # destructive write, independently of why this path was
                        # selected as a deletion candidate.
                        if not self._absence_confirmed(rel_path, source="scan"):
                            continue
                        new_seq = row["local_edit_seq"] + 1
                        # Remove from stat cache
                        self._stat_cache.pop(rel_path, None)
                        await conn.execute(
                            """UPDATE vault_sync_state
                               SET is_deleted=TRUE, deleted_at=NOW(), updated_at=NOW(),
                                   origin_seq=$2, local_edit_seq=$2
                               WHERE relative_path=$1""",
                            rel_path, new_seq,
                        )
                        for peer in peer_list:
                            await self._queue_event(
                                "FORGET", rel_path, row["content_hash"], row["content_hash"],
                                new_seq, None, 0, peer["peer_node_rid"],
                            )
                        events_this_cycle += 1
                        self._metrics.events_queued += 1

        self._metrics.files_scanned += files_scanned
        logger.info("vault_sync.scan_complete folder=%s files_changed=%d events_queued=%d duration_ms=%d",
                     shared_folder, files_changed, events_this_cycle, self._metrics.last_scan_duration_ms)

        # Mark initial sync complete — per peer
        if is_initial:
            async with self.pool.acquire() as conn:
                for peer in peer_list:
                    await conn.execute(
                        "UPDATE vault_sync_peers SET last_full_sync_at=NOW() WHERE peer_node_rid=$1 AND shared_folder=$2",
                        peer["peer_node_rid"], peer["shared_folder"],
                    )

        # Periodic cleanup
        await self._cleanup_tombstones()
        await self._cleanup_event_queue()

    # ------------------------------------------------------------------
    # Internal: Apply incoming events
    # ------------------------------------------------------------------

    async def _apply_new_or_update(
        self, target_path: Path, relative_path: str,
        local_row, event_type: str, markdown: str,
        content_hash: str, base_hash: Optional[str],
        origin_node: str, origin_seq: int,
        source_node: str, event_id: str, rid: str,
        file_size: int, timestamp: str,
    ):
        # Mirror mode: if path is in KOI_VAULT_MIRROR_PATHS, accept the event
        # unconditionally (skip conflict-copy branches). The line-591 stale-event
        # guard in apply_event() has already run before we get here, so old
        # events from the same origin can't slip through.
        if _path_in_mirror(relative_path):
            if local_row and local_row.get("origin_node") and local_row["origin_node"] != origin_node:
                logger.info(
                    "vault_sync.mirror_authorship_change path=%s old_origin=%s new_origin=%s",
                    relative_path, local_row["origin_node"], origin_node,
                )
                self._metrics.mirror_authorship_change += 1
            await self._atomic_write(target_path, markdown)
            now = datetime.now(timezone.utc)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO vault_sync_state
                       (relative_path, content_hash, origin_node, origin_seq, file_size_bytes,
                        last_synced_at, last_modified_at, is_deleted, deleted_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, NULL)
                       ON CONFLICT (relative_path) DO UPDATE SET
                           content_hash = EXCLUDED.content_hash,
                           origin_node = EXCLUDED.origin_node,
                           origin_seq = EXCLUDED.origin_seq,
                           file_size_bytes = EXCLUDED.file_size_bytes,
                           last_synced_at = EXCLUDED.last_synced_at,
                           last_modified_at = EXCLUDED.last_modified_at,
                           is_deleted = FALSE,
                           deleted_at = NULL,
                           updated_at = NOW()""",
                    relative_path, content_hash, origin_node, origin_seq, file_size, now, now,
                )
            try:
                st = target_path.stat()
                self._stat_cache[relative_path] = (st.st_mtime_ns, st.st_size, content_hash)
            except OSError:
                pass
            await self._record_applied(source_node, event_id, rid)
            self._metrics.mirror_overwrite += 1
            return

        file_exists = target_path.exists() and not target_path.is_symlink()

        if event_type == "NEW" and file_exists:
            # Compute on-disk hash so we can short-circuit even when local_row is missing
            # (Branch #1 fix: NEW event for path with no vault_sync_state row was creating
            # false conflicts even when on-disk content matched the incoming hash).
            if local_row:
                local_hash = local_row["content_hash"]
            else:
                try:
                    local_hash = hashlib.sha256(target_path.read_bytes()).hexdigest()
                except (FileNotFoundError, PermissionError, OSError) as e:
                    # Race: file vanished or became unreadable between file_exists check
                    # and read. Do NOT record applied — event will be retried on next poll.
                    self._metrics.transient_errors += 1
                    logger.warning(
                        "vault_sync.new_branch_hash_race path=%s error=%s event=%s — deferring",
                        relative_path, e, event_id,
                    )
                    return
            if local_hash == content_hash:
                # Idempotent — same content. Backfill vault_sync_state row if missing
                # so subsequent NEW events don't re-trigger this code path.
                if not local_row:
                    now = datetime.now(timezone.utc)
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            """INSERT INTO vault_sync_state
                               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes,
                                last_synced_at, last_modified_at, is_deleted, deleted_at)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, NULL)
                               ON CONFLICT (relative_path) DO UPDATE SET
                                   content_hash = EXCLUDED.content_hash,
                                   origin_node = EXCLUDED.origin_node,
                                   origin_seq = EXCLUDED.origin_seq,
                                   file_size_bytes = EXCLUDED.file_size_bytes,
                                   last_synced_at = EXCLUDED.last_synced_at,
                                   last_modified_at = EXCLUDED.last_modified_at,
                                   is_deleted = FALSE,
                                   deleted_at = NULL,
                                   updated_at = NOW()""",
                            relative_path, content_hash, origin_node, origin_seq, file_size, now, now,
                        )
                await self._record_applied(source_node, event_id, rid)
                return
            # Conflict: file exists with different content
            self._metrics.conflicts_created += 1
            await self._create_conflict_copy(target_path, relative_path, markdown, content_hash,
                                             origin_node, origin_seq, source_node, event_id, rid, file_size, timestamp)
            return

        if event_type == "UPDATE":
            if not file_exists and (not local_row or local_row["is_deleted"]):
                # File was deleted locally — edit wins over delete, recreate
                pass  # Fall through to write
            elif file_exists and local_row:
                if local_row["content_hash"] == content_hash:
                    # Content already matches incoming — idempotent, no conflict needed
                    await self._record_applied(source_node, event_id, rid)
                    return
                if not base_hash:
                    # No causal proof — treat as conflict
                    self._metrics.conflicts_created += 1
                    await self._create_conflict_copy(target_path, relative_path, markdown, content_hash,
                                                     origin_node, origin_seq, source_node, event_id, rid, file_size, timestamp)
                    return
                if local_row["content_hash"] == base_hash:
                    # Safe update — we haven't edited since peer's base
                    pass  # Fall through to write
                else:
                    # We also edited — conflict
                    self._metrics.conflicts_created += 1
                    await self._create_conflict_copy(target_path, relative_path, markdown, content_hash,
                                                     origin_node, origin_seq, source_node, event_id, rid, file_size, timestamp)
                    return

        # Write file atomically
        await self._atomic_write(target_path, markdown)

        # Update sync state
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO vault_sync_state
                   (relative_path, content_hash, origin_node, origin_seq, file_size_bytes,
                    last_synced_at, last_modified_at, is_deleted, deleted_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE, NULL)
                   ON CONFLICT (relative_path) DO UPDATE SET
                       content_hash = EXCLUDED.content_hash,
                       origin_node = EXCLUDED.origin_node,
                       origin_seq = EXCLUDED.origin_seq,
                       file_size_bytes = EXCLUDED.file_size_bytes,
                       last_synced_at = EXCLUDED.last_synced_at,
                       last_modified_at = EXCLUDED.last_modified_at,
                       is_deleted = FALSE,
                       deleted_at = NULL,
                       updated_at = NOW()""",
                relative_path, content_hash, origin_node, origin_seq, file_size, now, now,
            )

        # Update stat cache to prevent re-scanning the file we just wrote
        try:
            st = target_path.stat()
            self._stat_cache[relative_path] = (st.st_mtime_ns, st.st_size, content_hash)
        except OSError:
            pass

        await self._record_applied(source_node, event_id, rid)

    def _absence_confirmed(self, rel_path: str, source: str) -> bool:
        """Re-stat the path at the moment of the tombstone write.

        A4. Every other guard in this file reasons about coverage: was the scan
        truncated (A1), did the budget run out (A2), could we read it (A5). This
        one does not reason. It asks the filesystem, at the point of the
        destructive write, whether the file is there.

        That independence is the point. On 2026-08-25 the storm tombstoned 1,339
        paths of which 1,139 were still on disk — this check alone would have
        blocked all 1,139 no matter what the coverage logic concluded.

        Applies ONLY where WE decide a file is absent. An inbound FORGET is a
        decision a peer already made and the file existing locally is the normal
        case; guarding that on exists() would break every legitimate remote
        deletion, which is the mirror image of the storm.

        Returns True when the file really is gone and the tombstone may proceed.
        """
        try:
            present = (self.vault_path / rel_path).exists()
        except OSError as e:
            # Cannot even stat it, so absence is not established. A5's rule.
            logger.warning(
                "vault_sync.absence_unverifiable path=%s source=%s error=%s — refusing to tombstone",
                rel_path, source, e,
            )
            return False
        if present:
            self._metrics.tombstones_blocked_present = getattr(
                self._metrics, "tombstones_blocked_present", 0) + 1
            logger.warning(
                "vault_sync.tombstone_blocked path=%s source=%s reason=file_present_on_disk — "
                "a deletion candidate that is still on disk is a bug upstream of here, not a delete",
                rel_path, source,
            )
            return False
        return True

    async def _apply_forget(
        self, target_path: Path, relative_path: str,
        local_row, base_hash: Optional[str],
        origin_node: str, origin_seq: int,
        source_node: str, event_id: str, rid: str,
    ):
        # Mirror mode: same-origin stale check + owner-only restriction, then accept.
        if _path_in_mirror(relative_path):
            # Stale check (same author, older or equal seq)
            if local_row and local_row["origin_node"] == origin_node \
                    and origin_seq <= local_row["origin_seq"]:
                logger.info(
                    "vault_sync.mirror_forget_stale path=%s origin_seq=%s <= local=%s",
                    relative_path, origin_seq, local_row["origin_seq"],
                )
                await self._record_applied(source_node, event_id, rid)
                return
            # Owner-only restriction: no state row → reject for safety
            if local_row is None:
                logger.warning(
                    "vault_sync.mirror_forget_rejected_no_state path=%s event=%s — "
                    "cannot verify owner; dropping",
                    relative_path, event_id,
                )
                self._metrics.mirror_forget_rejected_no_state += 1
                return
            # Owner-only restriction: state row exists but origin differs → reject
            if local_row["origin_node"] != origin_node:
                logger.warning(
                    "vault_sync.mirror_forget_rejected_non_owner path=%s "
                    "non_owner=%s real_owner=%s",
                    relative_path, origin_node, local_row["origin_node"],
                )
                self._metrics.mirror_forget_rejected_non_owner += 1
                return
            # Accept: delete file + mark deleted
            try:
                if target_path.exists():
                    target_path.unlink()
            except OSError as e:
                logger.error("vault_sync.mirror_forget_delete_error path=%s error=%s",
                             target_path, e)
            self._stat_cache.pop(relative_path, None)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """UPDATE vault_sync_state
                       SET is_deleted=TRUE, deleted_at=NOW(), updated_at=NOW()
                       WHERE relative_path=$1""",
                    relative_path,
                )
            await self._record_applied(source_node, event_id, rid)
            self._metrics.mirror_forget += 1
            return

        if not local_row or local_row["is_deleted"]:
            # Already deleted — no-op
            await self._record_applied(source_node, event_id, rid)
            return

        if not base_hash:
            # No causal proof — refuse destructive action
            logger.info("vault_sync.apply_forget_no_base path=%s", relative_path)
            await self._record_applied(source_node, event_id, rid)
            return

        if local_row["content_hash"] != base_hash:
            # We edited after peer's base — stale delete, ignore
            logger.info("vault_sync.apply_forget_stale path=%s", relative_path)
            await self._record_applied(source_node, event_id, rid)
            return

        # Safe to delete
        try:
            if target_path.exists():
                target_path.unlink()
        except OSError as e:
            logger.error("vault_sync.delete_error path=%s error=%s", target_path, e)

        # Remove from stat cache
        self._stat_cache.pop(relative_path, None)

        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE vault_sync_state
                   SET is_deleted=TRUE, deleted_at=NOW(), updated_at=NOW()
                   WHERE relative_path=$1""",
                relative_path,
            )

        await self._record_applied(source_node, event_id, rid)

    async def _create_conflict_copy(
        self, target_path: Path, relative_path: str,
        markdown: str, content_hash: str,
        origin_node: str, origin_seq: int,
        source_node: str, event_id: str, rid: str,
        file_size: int, timestamp: str,
    ):
        """Create a conflict copy of the incoming file."""
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y-%m-%d %H-%M-%S")
        stem = target_path.stem
        suffix = target_path.suffix
        conflict_name = f"{stem} (conflict {ts}){suffix}"
        conflict_path = target_path.parent / conflict_name

        # Derive conflict relative path
        parts = relative_path.rsplit("/", 1)
        if len(parts) == 2:
            conflict_rel = f"{parts[0]}/{conflict_name}"
        else:
            conflict_rel = conflict_name

        await self._atomic_write(conflict_path, markdown)

        # A3: do not register a path the scanner is configured to ignore. The
        # live config excludes '*(conflict *).md', so registering the copy here
        # created a row the glob would never see again — tracked but
        # unscannable. The file itself is still written; only the state row is
        # skipped.
        if _path_excluded(conflict_rel):
            await self._record_applied(source_node, event_id, rid)
            logger.warning(
                "vault_sync.conflict_created path=%s state=unregistered "
                "reason=path_excluded", conflict_rel,
            )
            return

        # Insert sync state for the conflict copy
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO vault_sync_state
                   (relative_path, content_hash, origin_node, origin_seq, file_size_bytes,
                    last_synced_at, last_modified_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (relative_path) DO UPDATE SET
                       content_hash = EXCLUDED.content_hash,
                       origin_node = EXCLUDED.origin_node,
                       origin_seq = EXCLUDED.origin_seq,
                       file_size_bytes = EXCLUDED.file_size_bytes,
                       last_synced_at = EXCLUDED.last_synced_at,
                       last_modified_at = EXCLUDED.last_modified_at,
                       updated_at = NOW()""",
                conflict_rel, content_hash, origin_node, origin_seq,
                file_size, now, now,
            )

        await self._record_applied(source_node, event_id, rid)
        logger.warning("vault_sync.conflict_created path=%s", conflict_rel)

    # ------------------------------------------------------------------
    # Internal: Reconciliation (periodic manifest exchange)
    # ------------------------------------------------------------------

    async def _reconcile(self):
        """Send manifest to all peers for drift detection, with optional auto-repair."""
        peers = await self._get_all_peers()
        if not peers:
            return

        # --- Local drift detection (DB vs filesystem) ---
        drift_result = await self.reconcile(mode="detect")
        drift_count = drift_result.get("total_drift", 0)
        self._last_reconcile_drift = drift_count

        if drift_count > 0:
            logger.info("vault_sync.reconcile_drift_detected count=%d", drift_count)

            # Auto-repair if enabled, within threshold, and not backed off
            if (self._auto_repair_enabled
                    and drift_count <= AUTO_REPAIR_MAX_DRIFT
                    and self._auto_repair_consecutive_failures < AUTO_REPAIR_BACKOFF_LIMIT):
                self._metrics.auto_repairs_attempted += 1
                logger.info("vault_sync.auto_repair_starting drift=%d", drift_count)
                try:
                    repair_result = await self.reconcile(mode="repair", confirm=True)
                    actions = repair_result.get("actions_taken", 0)
                    self._metrics.auto_repairs_succeeded += 1
                    self._auto_repair_consecutive_failures = 0
                    logger.info("vault_sync.auto_repair_succeeded actions=%d", actions)
                except Exception as e:
                    self._metrics.auto_repairs_failed += 1
                    self._auto_repair_consecutive_failures += 1
                    logger.error(
                        "vault_sync.auto_repair_failed error=%s consecutive_failures=%d",
                        e, self._auto_repair_consecutive_failures,
                    )
                    if self._auto_repair_consecutive_failures >= AUTO_REPAIR_BACKOFF_LIMIT:
                        self._auto_repair_enabled = False
                        logger.warning(
                            "vault_sync.auto_repair_disabled_backoff failures=%d",
                            self._auto_repair_consecutive_failures,
                        )
            elif self._auto_repair_enabled and drift_count > AUTO_REPAIR_MAX_DRIFT:
                logger.warning(
                    "vault_sync.auto_repair_skipped_threshold drift=%d max=%d",
                    drift_count, AUTO_REPAIR_MAX_DRIFT,
                )
        else:
            # Reset failure counter on clean reconcile
            if self._auto_repair_consecutive_failures > 0:
                self._auto_repair_consecutive_failures = 0

        # --- Send manifest to peer (existing behavior) ---
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT relative_path, content_hash, origin_node, origin_seq
                   FROM vault_sync_state WHERE is_deleted=FALSE"""
            )

        manifest_entries = [
            {
                "relative_path": r["relative_path"],
                "content_hash": r["content_hash"],
                "origin_node": r["origin_node"],
                "origin_seq": r["origin_seq"],
            }
            for r in rows
        ]

        # Queue reconcile event — one per peer
        now = datetime.now(timezone.utc).isoformat()
        for peer in peers:
            await self.event_queue.add(
                event_type="UPDATE",
                # Keep reconcile events under the vault-file RID type so edge rid_types
                # filters allow delivery without requiring an extra capability type.
                rid=vault_file_rid(f"reconcile/{self.node_rid}"),
                manifest={"type": "reconcile", "timestamp": now, "entry_count": len(manifest_entries)},
                contents={"_vault_sync": True, "_reconcile": True, "entries": manifest_entries},
                ttl_hours=24,
                target_node=peer["peer_node_rid"],
            )
        logger.info("vault_sync.reconcile_sent entries=%d peers=%d", len(manifest_entries), len(peers))

    async def apply_reconcile(self, contents: Dict[str, Any], source_node: str):
        """Process a reconcile manifest from a peer."""
        entries = contents.get("entries", [])
        peer = await self._get_peer_by_source(source_node)
        if not peer:
            logger.warning("vault_sync.reconcile_unknown_source source=%s", source_node)
            return

        shared_folder = peer["shared_folder"]
        base_dir = self.vault_path / shared_folder

        for entry in entries:
            rel_path = entry.get("relative_path")
            remote_hash = entry.get("content_hash")
            if not rel_path or not remote_hash:
                continue

            async with self.pool.acquire() as conn:
                local_row = await conn.fetchrow(
                    "SELECT * FROM vault_sync_state WHERE relative_path=$1",
                    rel_path,
                )

            if not local_row:
                # Peer has file we don't — it will arrive as a NEW event from normal scan
                logger.debug("vault_sync.reconcile_peer_has path=%s", rel_path)
            elif local_row["is_deleted"]:
                logger.debug("vault_sync.reconcile_peer_has_deleted path=%s", rel_path)
            elif local_row["content_hash"] != remote_hash:
                logger.info("vault_sync.reconcile_hash_mismatch path=%s", rel_path)
                # Mismatch will be resolved by normal scan/event cycle

    # ------------------------------------------------------------------
    # Internal: Helpers
    # ------------------------------------------------------------------

    async def _get_peer_config(self) -> Optional[Dict[str, Any]]:
        """Get the first enabled peer config (backward compat)."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM vault_sync_peers WHERE enabled=TRUE ORDER BY peer_node_rid LIMIT 1"
            )
            if not row:
                return None
            return dict(row)

    async def _get_all_peers(self) -> List[Dict[str, Any]]:
        """Get all enabled vault sync peers."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM vault_sync_peers WHERE enabled=TRUE ORDER BY peer_node_rid")
            return [dict(r) for r in rows]

    async def _get_all_peers_by_source(self, source_node: str) -> List[Dict[str, Any]]:
        """Get all enabled peer configs (all folders) for a specific source node."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM vault_sync_peers WHERE peer_node_rid=$1 AND enabled=TRUE", source_node)
            return [dict(r) for r in rows]

    async def _get_peer_by_source(self, source_node: str, rel_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get peer config for a specific source node.

        If rel_path is provided, returns the peer whose shared_folder matches
        the path prefix (supports multi-folder per peer). Otherwise returns
        the first enabled peer for auth-only checks.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM vault_sync_peers WHERE peer_node_rid=$1 AND enabled=TRUE", source_node)
            if not rows:
                return None
            if rel_path:
                for row in rows:
                    folder = row["shared_folder"]
                    if rel_path == folder or rel_path.startswith(folder + "/"):
                        return dict(row)
                return None  # no folder matches this path
            return dict(rows[0])

    async def _get_shared_key(self, peer_node_rid: str) -> Optional[bytes]:
        """Get or derive the shared E2EE key for a peer. Returns None if E2EE unavailable."""
        if not self._encryption_private_key:
            return None

        cached = self._shared_key_cache.get(peer_node_rid)
        if cached is not None:
            return cached

        # Look up peer's encryption public key from DB
        async with self.pool.acquire() as conn:
            enc_key_b64 = await conn.fetchval(
                "SELECT encryption_key FROM koi_net_nodes WHERE node_rid = $1",
                peer_node_rid,
            )
        if not enc_key_b64:
            return None

        try:
            from api.koi_encryption import load_public_key_from_b64, derive_shared_key
            peer_public = load_public_key_from_b64(enc_key_b64)
            shared = derive_shared_key(
                self._encryption_private_key, peer_public,
                self.node_rid, peer_node_rid,
            )
            self._shared_key_cache[peer_node_rid] = shared
            return shared
        except Exception as e:
            logger.warning("vault_sync.e2ee_key_derivation_failed peer=%s error=%s", peer_node_rid, e)
            return None

    def invalidate_shared_key(self, peer_node_rid: str) -> None:
        """Invalidate cached shared key (call on handshake / key rotation)."""
        self._shared_key_cache.pop(peer_node_rid, None)

    async def _queue_event(
        self, event_type: str, relative_path: str,
        content_hash: str, base_hash: Optional[str],
        origin_seq: int, markdown: Optional[str],
        file_size: int, peer_node_rid: str,
    ):
        """Queue a vault-sync event for delivery."""
        now = datetime.now(timezone.utc).isoformat()
        rid = vault_file_rid(relative_path)

        contents = {
            "relative_path": relative_path,
            "_vault_sync": True,
        }

        # E2EE: encrypt markdown if shared key available
        shared_key = await self._get_shared_key(peer_node_rid) if markdown is not None else None
        if shared_key is not None and markdown is not None:
            try:
                from api.koi_encryption import encrypt_content
                contents["ciphertext"] = encrypt_content(markdown, shared_key, rid)
                contents["e2ee_version"] = 1
            except Exception as e:
                logger.warning("vault_sync.e2ee_encrypt_failed path=%s error=%s, falling back to plaintext", relative_path, e)
                contents["markdown"] = markdown
        elif markdown is not None:
            contents["markdown"] = markdown
            if self._encryption_private_key:
                logger.debug("vault_sync.e2ee_unavailable peer=%s (no peer encryption key)", peer_node_rid)

        manifest = _build_vault_manifest(
            relative_path=relative_path,
            content_hash=content_hash,
            base_hash=base_hash,
            origin_node=self.node_rid,
            origin_seq=origin_seq,
            file_size=file_size,
            event_type=event_type,
            now=now,
        )

        await self.event_queue.add(
            event_type=event_type,
            rid=rid,
            manifest=manifest,
            contents=contents,
            ttl_hours=168,  # 7 days
            target_node=peer_node_rid,
        )

    async def _forward_to_peers(
        self, event_id: str, event_type: str, rid: str,
        manifest: Dict[str, Any], plaintext_markdown: Optional[str],
        source_node: str,
    ):
        """Forward applied event to all other configured peers. Re-encrypts per target."""
        peers = await self._get_all_peers()
        rel_path = manifest.get("relative_path", "")

        for peer in peers:
            if peer["peer_node_rid"] == source_node:
                continue  # Don't echo back

            # Check if file is in this peer's shared_folder (exact boundary match)
            folder = peer["shared_folder"]
            if rel_path != folder and not rel_path.startswith(folder + "/"):
                continue

            # Build contents — re-encrypt for this specific target
            contents: Dict[str, Any] = {"_vault_sync": True, "relative_path": rel_path}
            shared_key = await self._get_shared_key(peer["peer_node_rid"])
            if shared_key and plaintext_markdown is not None:
                try:
                    from api.koi_encryption import encrypt_content
                    contents["ciphertext"] = encrypt_content(plaintext_markdown, shared_key, rid)
                    contents["e2ee_version"] = 1
                except Exception as e:
                    logger.warning("vault_sync.forward_encrypt_failed peer=%s error=%s", peer["peer_node_rid"], e)
                    contents["markdown"] = plaintext_markdown
            elif plaintext_markdown is not None:
                contents["markdown"] = plaintext_markdown

            # Queue with SAME event_id for loop detection on remote
            await self.event_queue.add(
                event_type=event_type,
                rid=rid,
                manifest=manifest,
                contents=contents,
                event_id=event_id,
                ttl_hours=168,
                target_node=peer["peer_node_rid"],
            )
            logger.info("vault_sync.forwarded event_id=%s to=%s", event_id, peer["peer_node_rid"])

    async def _atomic_write(self, path: Path, content: str):
        """Write file atomically via tmp + rename."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.parent / f"{path.name}.tmp.{uuid.uuid4().hex[:8]}"
        try:
            tmp_path.write_text(content, encoding="utf-8")
            tmp_path.rename(path)
        except Exception:
            # Clean up tmp on failure
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    async def _record_applied(self, source_node: str, event_id: str, rid: str):
        """Record that we applied an event (for idempotency)."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO vault_sync_applied_events (source_node, event_id, rid)
                   VALUES ($1, $2::UUID, $3)
                   ON CONFLICT (source_node, event_id, rid) DO NOTHING""",
                source_node, event_id, rid,
            )

    async def _cleanup_tombstones(self):
        """Purge old tombstones and dedup entries."""
        async with self.pool.acquire() as conn:
            deleted = await conn.execute(
                f"DELETE FROM vault_sync_state WHERE is_deleted=TRUE AND deleted_at < NOW() - INTERVAL '{TOMBSTONE_CLEANUP_DAYS} days'"
            )
            purged = await conn.execute(
                f"DELETE FROM vault_sync_applied_events WHERE applied_at < NOW() - INTERVAL '{DEDUP_CLEANUP_DAYS} days'"
            )

    async def _cleanup_event_queue(self):
        """Purge delivered events older than retention period. Never deletes undelivered events."""
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    f"""DELETE FROM koi_net_events
                        WHERE {vault_file_rid_sql_clause()}
                        AND array_length(delivered_to, 1) IS NOT NULL
                        AND queued_at < NOW() - INTERVAL '{EVENT_QUEUE_RETENTION_DAYS} days'"""
                )
                # result is like "DELETE N"
                count = int(result.split()[-1]) if result else 0
                if count > 0:
                    self._metrics.event_queue_cleaned += count
                    logger.info("vault_sync.event_queue_cleanup removed=%d retention_days=%d", count, EVENT_QUEUE_RETENTION_DAYS)
        except Exception as e:
            logger.warning("vault_sync.event_queue_cleanup_failed error=%s", e)

    def _reject(self, reason: str, rid: str, source_node: str, event_id: Optional[str], detail: str):
        """Log and count a rejected event."""
        field = _REJECT_FIELD_MAP.get(reason)
        if field:
            setattr(self._metrics, field, getattr(self._metrics, field) + 1)
        logger.warning(
            "vault_sync.rejected reason=%s path=%s detail=%s", reason, rid, detail,
        )
