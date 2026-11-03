#!/usr/bin/env python3
"""Collect vault-sync diagnostics without mutating API, DB, or vault files.

This is intended for version-skew recovery work between KOI-net peers. It uses
only the Python standard library plus `psql` if available. Secrets are not
printed; the local admin token is used only for localhost status endpoints.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = os.getenv("KOI_DB_NAME") or os.getenv("POSTGRES_DB") or "personal_koi"
DEFAULT_API = os.getenv("KOI_API_BASE", "http://127.0.0.1:8351")
DEFAULT_VAULT = Path(os.getenv("KOI_VAULT_PATH", "~/Documents/Notes")).expanduser()
DEFAULT_TOKEN_PATH = Path("~/.config/personal-koi/koi-state/admin_token").expanduser()

VAULT_PATTERNS = {".md", ".jsonl", ".json", ".jsonld", ".txt", ".csv", ".yaml", ".yml", ".toml"}
ENV_ALLOWLIST = {
    "KOI_BASE_URL",
    "KOI_NODE_NAME",
    "KOI_NODE_TYPE",
    "KOI_ONTOLOGY_URI",
    "KOI_ONTOLOGY_VERSION",
    "KOI_REQUIRE_SIGNED_REQUESTS",
    "KOI_REQUIRE_SIGNED_RESPONSES",
    "KOI_REQUIRE_SIGNED_BROADCAST",
    "KOI_REQUIRE_SIGNED_POLL",
    "KOI_REQUIRE_SIGNED_CONFIRM",
    "KOI_ENFORCE_ENVELOPE_TARGET",
    "KOI_VAULT_EXCLUDE_PATHS",
    "KOI_VAULT_MIRROR_PATHS",
    "KOI_VAULT_READONLY_PATHS",
    "VAULT_SYNC_AUTO_REPAIR",
    "VAULT_SYNC_ENABLED",
    "VAULT_SYNC_FOLDER",
    "VAULT_SYNC_MAX_BYTES_PER_SCAN",
    "VAULT_SYNC_MAX_EVENTS_PER_SCAN",
    "VAULT_SYNC_MAX_FILES_PER_SCAN",
    "VAULT_SYNC_REPAIR_ENABLED",
    "VAULT_SYNC_WATCHER",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(cmd: list[str], cwd: Path | None = None, timeout: int = 20) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        return {
            "cmd": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except FileNotFoundError as exc:
        return {"cmd": cmd, "returncode": None, "stdout": "", "stderr": f"not found: {exc}"}
    except subprocess.TimeoutExpired as exc:
        return {
            "cmd": cmd,
            "returncode": None,
            "stdout": (exc.stdout or "").strip() if isinstance(exc.stdout, str) else "",
            "stderr": f"timeout after {timeout}s",
        }


def psql_json(db: str, sql: str, timeout: int = 20) -> dict[str, Any]:
    result = run(
        ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-d", db, "-c", sql],
        timeout=timeout,
    )
    if result["returncode"] != 0:
        return {"ok": False, "error": result["stderr"], "stdout": result["stdout"]}
    raw = result["stdout"]
    if not raw:
        return {"ok": True, "value": None}
    try:
        return {"ok": True, "value": json.loads(raw)}
    except json.JSONDecodeError:
        return {"ok": False, "error": "psql output was not JSON", "stdout": raw}


# Vault-file RIDs exist in two namespaces during the divergence-1 migration:
# the legacy squatted `koi-net.vault-file` and the owned `personal-koi.vault-file`.
# A doctor that only matches the legacy one reports a healthy zero after the
# cutover, which is the worst possible failure mode for a diagnostic tool.
_VAULT_RID_PREFIXES = ("orn:koi-net.vault-file:", "orn:personal-koi.vault-file:")
_VAULT_RID_CLAUSE = "(" + " OR ".join(f"rid LIKE '{p}%'" for p in _VAULT_RID_PREFIXES) + ")"


def sql_literal(value: str) -> str:
    """Return a safely quoted SQL string literal for psql snippets."""
    return "'" + value.replace("'", "''") + "'"


def http_json(base_url: str, path: str, token: str | None = None, timeout: int = 8) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                parsed: Any = json.loads(body)
            except json.JSONDecodeError:
                parsed = body
            return {"ok": True, "status": resp.status, "url": url, "body": parsed}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"ok": False, "status": exc.code, "url": url, "body": body[:4000]}
    except Exception as exc:
        return {"ok": False, "status": None, "url": url, "error": repr(exc)}


def read_admin_token(path: Path) -> str | None:
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return token or None


def load_safe_env(config_path: Path) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key in ENV_ALLOWLIST:
        if key in os.environ:
            values[key] = os.environ[key]

    if config_path.exists():
        try:
            for line in config_path.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                key, _, val = stripped.partition("=")
                key = key.strip()
                if key in ENV_ALLOWLIST and key not in values:
                    values[key] = val.strip().strip("'\"")
        except OSError as exc:
            values["_config_read_error"] = repr(exc)

    return values


def git_info() -> dict[str, Any]:
    return {
        "branch": run(["git", "branch", "--show-current"], cwd=REPO_ROOT),
        "head": run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT),
        "last_commit": run(["git", "log", "-1", "--format=%H%n%h%n%ci%n%s"], cwd=REPO_ROOT),
        "status": run(["git", "status", "--short", "--branch"], cwd=REPO_ROOT),
        "dirty_files": run(["git", "diff", "--name-only"], cwd=REPO_ROOT),
        "remote": run(["git", "remote", "-v"], cwd=REPO_ROOT),
    }


def file_inventory(vault: Path, path_prefix: str | None, include_files: bool) -> dict[str, Any]:
    if not path_prefix:
        return {"enabled": False, "reason": "no path prefix supplied"}

    root = (vault / path_prefix).expanduser().resolve()
    result: dict[str, Any] = {
        "enabled": True,
        "vault": str(vault),
        "path_prefix": path_prefix,
        "root": str(root),
        "exists": root.exists(),
        "is_dir": root.is_dir(),
        "count": 0,
        "total_bytes": 0,
        "files": [],
    }
    if not root.is_dir():
        return result

    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        if path.suffix not in VAULT_PATTERNS:
            continue
        try:
            data = path.read_bytes()
        except OSError as exc:
            files.append({"path": str(path), "error": repr(exc)})
            continue
        rel_path = path.relative_to(vault).as_posix()
        item = {
            "relative_path": rel_path,
            "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        files.append(item)
        result["count"] += 1
        result["total_bytes"] += len(data)

    if include_files:
        result["files"] = files
    else:
        result["files_omitted"] = len(files)
        result.pop("files", None)
    return result


def db_diagnostics(db: str, peer_rid: str | None, path_prefix: str | None) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}

    diagnostics["structural_status"] = psql_json(db, r"""
SELECT jsonb_build_object(
  'tables', jsonb_build_object(
    'koi_net_events', to_regclass('public.koi_net_events') IS NOT NULL,
    'koi_net_edges', to_regclass('public.koi_net_edges') IS NOT NULL,
    'koi_net_nodes', to_regclass('public.koi_net_nodes') IS NOT NULL,
    'vault_sync_state', to_regclass('public.vault_sync_state') IS NOT NULL,
    'vault_sync_peers', to_regclass('public.vault_sync_peers') IS NOT NULL,
    'vault_sync_applied_events', to_regclass('public.vault_sync_applied_events') IS NOT NULL,
    'vault_sync_metrics', to_regclass('public.vault_sync_metrics') IS NOT NULL,
    'schema_migrations', to_regclass('public.schema_migrations') IS NOT NULL
  ),
  'columns', jsonb_build_object(
    'koi_net_events_target_node', EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='koi_net_events' AND column_name='target_node'
    ),
    'koi_net_nodes_encryption_key', EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='koi_net_nodes' AND column_name='encryption_key'
    ),
    'vault_sync_state_local_edit_seq', EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='vault_sync_state' AND column_name='local_edit_seq'
    ),
    'vault_sync_peers_id', EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema='public' AND table_name='vault_sync_peers' AND column_name='id'
    )
  ),
  'vault_sync_peers_pk_columns', COALESCE((
    SELECT jsonb_agg(column_name ORDER BY ordinal_position)
    FROM information_schema.key_column_usage
    WHERE table_schema='public'
      AND table_name='vault_sync_peers'
      AND constraint_name='vault_sync_peers_pkey'
  ), '[]'::jsonb),
  'event_dedup_indexes', COALESCE((
    SELECT jsonb_agg(indexname ORDER BY indexname)
    FROM pg_indexes
    WHERE schemaname='public'
      AND tablename='koi_net_events'
      AND indexname LIKE '%source_event%'
  ), '[]'::jsonb)
);
""")

    diagnostics["schema_columns"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'table', table_name,
  'column', column_name,
  'type', data_type,
  'nullable', is_nullable,
  'default', column_default
) ORDER BY table_name, ordinal_position), '[]'::jsonb)
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name IN (
    'koi_net_events',
    'koi_net_edges',
    'koi_net_nodes',
    'vault_sync_state',
    'vault_sync_peers',
    'vault_sync_applied_events',
    'vault_sync_metrics'
  );
""")

    diagnostics["constraints"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'table', c.relname,
  'constraint', con.conname,
  'type', con.contype,
  'definition', pg_get_constraintdef(con.oid)
) ORDER BY c.relname, con.conname), '[]'::jsonb)
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname='public'
  AND c.relname IN ('koi_net_events','vault_sync_state','vault_sync_peers','vault_sync_applied_events');
""")

    diagnostics["indexes"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'table', tablename,
  'index', indexname,
  'definition', indexdef
) ORDER BY tablename, indexname), '[]'::jsonb)
FROM pg_indexes
WHERE schemaname='public'
  AND tablename IN ('koi_net_events','vault_sync_state','vault_sync_peers','vault_sync_applied_events');
""")

    diagnostics["schema_migrations"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'version', version,
  'applied_at', applied_at
) ORDER BY version), '[]'::jsonb)
FROM schema_migrations;
""")

    diagnostics["nodes"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'node_name', node_name,
  'node_rid', node_rid,
  'node_type', node_type,
  'base_url', base_url,
  'status', status,
  'last_seen', last_seen,
  'has_public_key', public_key IS NOT NULL,
  'has_encryption_key', encryption_key IS NOT NULL,
  'ontology_uri', ontology_uri,
  'ontology_version', ontology_version
) ORDER BY node_name NULLS LAST, node_rid), '[]'::jsonb)
FROM koi_net_nodes;
""")

    diagnostics["edges"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'source_node', source_node,
  'target_node', target_node,
  'edge_type', edge_type,
  'status', status,
  'rid_types', rid_types,
  'metadata', metadata
) ORDER BY source_node, target_node, edge_type), '[]'::jsonb)
FROM koi_net_edges;
""")

    diagnostics["vault_sync_peers"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'peer_node_rid', peer_node_rid,
  'shared_folder', shared_folder,
  'enabled', enabled,
  'last_full_sync_at', last_full_sync_at,
  'created_at', created_at
) ORDER BY peer_node_rid, shared_folder), '[]'::jsonb)
FROM vault_sync_peers;
""")

    diagnostics["vault_event_summary"] = psql_json(db, r"""
SELECT COALESCE(jsonb_agg(jsonb_build_object(
  'target_node', target_node,
  'event_type', event_type,
  'count', count,
  'delivered_count', delivered_count,
  'confirmed_count', confirmed_count,
  'oldest_queued_at', oldest_queued_at,
  'newest_queued_at', newest_queued_at
) ORDER BY target_node NULLS FIRST, event_type), '[]'::jsonb)
FROM (
  SELECT
    target_node,
    event_type,
    COUNT(*)::int AS count,
    COUNT(*) FILTER (WHERE array_length(delivered_to, 1) IS NOT NULL)::int AS delivered_count,
    COUNT(*) FILTER (WHERE array_length(confirmed_by, 1) IS NOT NULL)::int AS confirmed_count,
    MIN(queued_at) AS oldest_queued_at,
    MAX(queued_at) AS newest_queued_at
  FROM koi_net_events
  WHERE {_VAULT_RID_CLAUSE}
    AND expires_at > NOW()
  GROUP BY target_node, event_type
) s;
""")

    if peer_rid:
        peer_sql = sql_literal(peer_rid)
        diagnostics["vault_events_for_peer"] = psql_json(db, f"""
SELECT jsonb_build_object(
  'peer_rid', {peer_sql}::text,
  'active_total', COUNT(*)::int,
  'delivered', COUNT(*) FILTER (WHERE {peer_sql}::text = ANY(delivered_to))::int,
  'confirmed', COUNT(*) FILTER (WHERE {peer_sql}::text = ANY(confirmed_by))::int,
  'targeted_active', COUNT(*) FILTER (WHERE target_node = {peer_sql}::text)::int,
  'targeted_undelivered', COUNT(*) FILTER (
    WHERE target_node = {peer_sql}::text
      AND NOT ({peer_sql}::text = ANY(delivered_to))
  )::int
)
FROM koi_net_events
WHERE {_VAULT_RID_CLAUSE}
  AND expires_at > NOW();
""")

    if peer_rid and path_prefix:
        suffix = path_prefix.rstrip("/") + "/%"
        rid_like_sql = "(" + " OR ".join(
            f"rid LIKE {sql_literal(pfx + suffix)}" for pfx in _VAULT_RID_PREFIXES
        ) + ")"
        peer_sql = sql_literal(peer_rid)
        diagnostics["path_events_for_peer"] = psql_json(db, f"""
SELECT jsonb_build_object(
  'peer_rid', {peer_sql}::text,
  'rid_like', {rid_like_sql}::text,
  'total', COUNT(*)::int,
  'delivered', COUNT(*) FILTER (WHERE {peer_sql}::text = ANY(delivered_to))::int,
  'confirmed', COUNT(*) FILTER (WHERE {peer_sql}::text = ANY(confirmed_by))::int,
  'undelivered_targeted', COUNT(*) FILTER (
    WHERE target_node = {peer_sql}::text
      AND NOT ({peer_sql}::text = ANY(delivered_to))
  )::int,
  'by_type', COALESCE((
    SELECT jsonb_object_agg(event_type, n)
    FROM (
      SELECT event_type, COUNT(*)::int AS n
      FROM koi_net_events
      WHERE rid LIKE {rid_like_sql}::text
        AND target_node = {peer_sql}::text
        AND expires_at > NOW()
      GROUP BY event_type
    ) by_type
  ), '{{}}'::jsonb)
)
FROM koi_net_events
WHERE rid LIKE {rid_like_sql}::text
  AND expires_at > NOW();
""")

    return diagnostics


def collect(args: argparse.Namespace) -> dict[str, Any]:
    token = read_admin_token(Path(args.token_path).expanduser())
    config_path = REPO_ROOT / "config" / "personal.env"

    report: dict[str, Any] = {
        "generated_at": now_iso(),
        "host": {
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python": sys.version.split()[0],
        },
        "inputs": {
            "repo_root": str(REPO_ROOT),
            "db": args.db,
            "api_base": args.api_base,
            "vault": str(Path(args.vault).expanduser()),
            "path_prefix": args.path_prefix,
            "peer_rid": args.peer_rid,
            "include_file_list": args.include_file_list,
        },
        "git": git_info(),
        "safe_env": load_safe_env(config_path),
        "admin_token_present": token is not None,
        "http": {
            "health": http_json(args.api_base, "/health", token=None),
            "koi_net_health": http_json(args.api_base, "/koi-net/health", token=None),
            "vault_sync_status": http_json(args.api_base, "/koi-net/vault-sync/status", token=token),
        },
        "db": db_diagnostics(args.db, args.peer_rid, args.path_prefix),
        "files": file_inventory(Path(args.vault).expanduser(), args.path_prefix, args.include_file_list),
    }
    return report


def print_summary(report: dict[str, Any]) -> None:
    structural = report.get("db", {}).get("structural_status", {}).get("value") or {}
    pk_cols = structural.get("vault_sync_peers_pk_columns")
    columns = structural.get("columns", {})
    path_events = report.get("db", {}).get("path_events_for_peer", {}).get("value") or {}
    files = report.get("files", {})
    http_status = report.get("http", {}).get("vault_sync_status", {})

    print("vault-sync-doctor summary", file=sys.stderr)
    print(f"  generated_at: {report.get('generated_at')}", file=sys.stderr)
    print(f"  git_head: {report.get('git', {}).get('head', {}).get('stdout', '')[:12]}", file=sys.stderr)
    print(f"  peers_pk: {pk_cols}", file=sys.stderr)
    print(f"  local_edit_seq: {columns.get('vault_sync_state_local_edit_seq')}", file=sys.stderr)
    print(f"  target_node_column: {columns.get('koi_net_events_target_node')}", file=sys.stderr)
    print(f"  vault_status_ok: {http_status.get('ok')} status={http_status.get('status')}", file=sys.stderr)
    print(f"  files[{files.get('path_prefix')}]: {files.get('count')} total_bytes={files.get('total_bytes')}", file=sys.stderr)
    if path_events:
        print(
            "  path_events_for_peer: "
            f"total={path_events.get('total')} delivered={path_events.get('delivered')} "
            f"confirmed={path_events.get('confirmed')} undelivered_targeted={path_events.get('undelivered_targeted')}",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect KOI vault-sync diagnostics.")
    parser.add_argument("--db", default=DEFAULT_DB, help=f"PostgreSQL database name (default: {DEFAULT_DB})")
    parser.add_argument("--api-base", default=DEFAULT_API, help=f"Local API base URL (default: {DEFAULT_API})")
    parser.add_argument("--vault", default=str(DEFAULT_VAULT), help=f"Vault root (default: {DEFAULT_VAULT})")
    parser.add_argument("--token-path", default=str(DEFAULT_TOKEN_PATH), help="Local admin token path")
    parser.add_argument("--path-prefix", default=None, help="Vault relative folder to inventory, e.g. 'Shared/Regen AI/Meetings'")
    parser.add_argument("--peer-rid", default=None, help="Peer node RID for focused event counters")
    parser.add_argument("--include-file-list", action="store_true", help="Include per-file sha256 inventory")
    parser.add_argument("--output", default=None, help="Write JSON report to this path")
    parser.add_argument("--no-summary", action="store_true", help="Do not print human summary to stderr")
    args = parser.parse_args()

    report = collect(args)
    encoded = json.dumps(report, indent=2, sort_keys=True, default=str)

    if args.output:
        out = Path(args.output).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(encoded + "\n", encoding="utf-8")
        print(f"wrote {out}", file=sys.stderr)
    else:
        print(encoded)

    if not args.no_summary:
        print_summary(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
