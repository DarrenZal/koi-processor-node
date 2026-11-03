"""BKC leaf node deployment startup profile.

Minimal profile -- federation is handled by the main app and KOI-net router.
Leaf nodes (e.g. Greater Victoria, Cowichan Valley) participate in the
network but do not run sensors or pipeline handlers.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def on_startup(app, pool, caps):
    """No additional startup needed for leaf nodes.

    Federation event handling is managed by the KOI-net router mounted
    on the main app.  Leaf nodes receive and apply events from coordinators
    but do not initiate sensor scans or pipeline processing.

    Parameters
    ----------
    app : FastAPI
        The application instance.
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities from the capabilities registry.
    """
    logger.info(
        "BKC leaf node startup complete (profile=%s)",
        caps.deployment_profile,
    )
