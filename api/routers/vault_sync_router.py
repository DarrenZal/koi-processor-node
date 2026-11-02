"""Vault sync management endpoints.

Provides REST control over bidirectional markdown sync between KOI-net peers.
Mounted at /koi-net/vault-sync when caps.vault_sync is True.

Endpoints:
    GET  /status     — Dashboard info (metrics, config, rejected events)
    POST /trigger    — Force immediate sync cycle
    POST /configure  — Configure vault sync for a peer
    POST /reconcile  — Detect or repair drift between DB and filesystem
    POST /pause      — Pause the sync polling loop
    POST /resume     — Resume the sync polling loop
"""

import asyncio
import logging
import os
import subprocess
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.auth import enforce_local_admin

logger = logging.getLogger(__name__)

# Reference to the VaultSyncManager instance, set by setup_koi_net().
_vault_sync_manager = None


def set_vault_sync_manager(manager):
    """Called by setup_koi_net() to inject the VaultSyncManager instance."""
    global _vault_sync_manager
    _vault_sync_manager = manager


def _bool_env_raw(name: str, default: bool = False) -> bool:
    """Check env var as bool (standalone, no side effects)."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _unavailable(msg: str = "Vault sync is not enabled") -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": msg})


async def _rebuild_pageindex_bg():
    """Rebuild BM25 vault pageindex after sync so newly-synced files are searchable."""
    venv_python = os.path.expanduser(
        "~/.claude/local/darren-workflow/pageindex/venv/bin/python3"
    )
    script = os.path.expanduser("~/projects/darren-workflow/scripts/pageindex.py")
    vault_path = os.path.expanduser("~/Documents/Notes")
    if not os.path.exists(venv_python) or not os.path.exists(script):
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            venv_python, script, "index", "--vault", vault_path,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        await proc.wait()
        logger.info("vault_sync: pageindex rebuilt")
    except Exception as e:
        logger.warning("vault_sync: pageindex rebuild failed: %s", e)


def create_router(pool, caps):
    """Return an APIRouter for vault sync endpoints.

    Only included when caps.vault_sync is True. Mounted at
    /koi-net/vault-sync by the app wiring layer.
    """
    router = APIRouter(prefix="/koi-net/vault-sync", tags=["vault-sync"])

    @router.get("/status")
    async def vault_sync_status(request: Request):
        """Get vault sync dashboard info."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return JSONResponse(content={"enabled": False, "reason": "VAULT_SYNC_ENABLED not set"})

        status = await _vault_sync_manager.get_status()
        return JSONResponse(content=status)

    @router.post("/trigger")
    async def vault_sync_trigger(request: Request):
        """Force an immediate sync cycle."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return _unavailable()

        result = await _vault_sync_manager.trigger_sync()
        # Rebuild BM25 pageindex in background so newly synced files are searchable
        asyncio.create_task(_rebuild_pageindex_bg())
        return JSONResponse(content=result)

    @router.post("/configure")
    async def vault_sync_configure(request: Request):
        """Configure vault sync for a peer."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return _unavailable("Vault sync is not enabled (set VAULT_SYNC_ENABLED=true)")

        body = await request.json()
        peer = body.get("peer")
        shared_folder = body.get("shared_folder", "Shared")
        enabled = body.get("enabled", True)

        if not peer:
            return JSONResponse(status_code=400, content={"error": "peer is required"})

        result = await _vault_sync_manager.configure(peer, shared_folder, enabled)
        if "error" in result:
            return JSONResponse(status_code=404, content=result)
        return JSONResponse(content=result)

    @router.delete("/peers/{peer_name}")
    async def vault_sync_remove_peer(request: Request, peer_name: str):
        """Disable/remove a vault sync peer."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return _unavailable("Vault sync is not enabled (set VAULT_SYNC_ENABLED=true)")

        result = await _vault_sync_manager.unconfigure(peer_name)
        if "error" in result:
            return JSONResponse(status_code=404, content=result)
        return JSONResponse(content=result)

    @router.post("/reconcile")
    async def vault_sync_reconcile(request: Request):
        """Run reconciliation: detect or repair drift between DB and filesystem."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return _unavailable()

        body = await request.json()
        mode = body.get("mode", "detect")

        if mode == "repair":
            if not _bool_env_raw("VAULT_SYNC_REPAIR_ENABLED", False):
                return JSONResponse(
                    status_code=403,
                    content={"error": "repair mode disabled"},
                )

        if mode not in ("detect", "repair"):
            return JSONResponse(
                status_code=400,
                content={"error": f"invalid mode: {mode}. Use 'detect' or 'repair'."},
            )

        try:
            from api.vault_sync import VaultUnavailableError
            result = await _vault_sync_manager.reconcile(
                mode=mode,
                confirm=body.get("confirm", False),
                paths=body.get("paths"),
                max_actions=body.get("max_actions", 50),
                peer_rid=body.get("peer"),
            )
        except Exception as e:
            if type(e).__name__ == "VaultUnavailableError":
                return JSONResponse(status_code=503, content={"error": str(e)})
            raise

        return JSONResponse(content=result)

    @router.post("/pause")
    async def vault_sync_pause(request: Request):
        """Pause the vault sync polling loop."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return _unavailable()

        _vault_sync_manager._paused = True
        return JSONResponse(content={"status": "paused"})

    @router.post("/resume")
    async def vault_sync_resume(request: Request):
        """Resume the vault sync polling loop after a pause."""
        err = enforce_local_admin(request)
        if err:
            return err

        if _vault_sync_manager is None:
            return _unavailable()

        _vault_sync_manager._paused = False
        return JSONResponse(content={"status": "resumed"})

    return router
