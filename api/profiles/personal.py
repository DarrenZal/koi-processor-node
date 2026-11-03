"""Personal deployment startup profile.

Starts vault sync manager and optionally the TerminusDB adapter.
This profile is used for local/personal KOI instances (e.g. personal_koi).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def on_startup(app, pool, caps):
    """Start vault sync and TerminusDB outbox if enabled.

    Parameters
    ----------
    app : FastAPI
        The application instance (for storing references on app.state).
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities from the capabilities registry.
    """
    if caps.vault_sync:
        try:
            from api.vault_sync import VaultSyncManager
            from api.routers.vault_sync_router import set_vault_sync_manager

            manager = VaultSyncManager(pool=pool)
            await manager.start()
            app.state.vault_sync_manager = manager
            set_vault_sync_manager(manager)

            # After each sync cycle, rebuild the vault BM25 pageindex so
            # newly-synced files appear in unified_search's vault surface.
            from api.routers.vault_sync_router import _rebuild_pageindex_bg
            manager.post_cycle_hooks.append(_rebuild_pageindex_bg)
            logger.info("Vault sync manager started")
        except Exception:
            logger.exception("Failed to start vault sync manager")

    if caps.terminusdb:
        try:
            from api.terminusdb_adapter import TerminusDBAdapter

            adapter = TerminusDBAdapter()
            app.state.terminusdb_adapter = adapter
            logger.info("TerminusDB adapter initialized")
        except Exception:
            logger.exception("Failed to initialize TerminusDB adapter")

    if caps.mediawiki_sensor:
        try:
            from api.mediawiki_sensor import MediaWikiSensor
            sensor = MediaWikiSensor(
                pool=pool,
                event_queue=getattr(app.state, "event_queue", None),
            )
            await sensor.start()
            app.state.mediawiki_sensor = sensor

            # Register graceful shutdown — app-level shutdown only closes the DB pool.
            @app.on_event("shutdown")
            async def _stop_mediawiki_sensor():
                s = getattr(app.state, "mediawiki_sensor", None)
                if s:
                    await s.stop()

            logger.info("MediaWiki sensor started")
        except Exception:
            logger.exception("Failed to start MediaWiki sensor")
