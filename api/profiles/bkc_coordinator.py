"""BKC coordinator deployment startup profile.

Registers pipeline handlers and starts web/GitHub sensors.
This profile is used for the Octo coordinator node that manages
the bioregional knowledge commons federation.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def on_startup(app, pool, caps):
    """Register pipeline handlers and start web/GitHub sensors.

    Parameters
    ----------
    app : FastAPI
        The application instance (for storing references on app.state).
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities from the capabilities registry.
    """
    if caps.pipeline:
        try:
            # TODO: Import and register pipeline handlers from api/pipeline/
            # from api.pipeline import register_handlers
            # register_handlers(app, pool)
            logger.info("Pipeline handlers registered (stub)")
        except Exception:
            logger.exception("Failed to register pipeline handlers")

    if caps.web_sensor:
        try:
            # TODO: Import and start the web sensor background task
            # from api.web_sensor import WebSensor
            # sensor = WebSensor(pool=pool)
            # await sensor.start()
            # app.state.web_sensor = sensor
            logger.info("Web sensor started (stub)")
        except Exception:
            logger.exception("Failed to start web sensor")

    if caps.github_sensor:
        try:
            from api.github_sensor import GitHubSensor
            from api.personal_ingest_api import generate_document_embedding
            sensor = GitHubSensor(
                pool=pool,
                event_queue=getattr(app.state, 'event_queue', None),
            )
            sensor._embed_fn = generate_document_embedding
            await sensor.start()
            app.state.github_sensor = sensor
            logger.info("GitHub sensor started")
        except Exception:
            logger.exception("Failed to start GitHub sensor")

    if caps.mediawiki_sensor:
        try:
            from api.mediawiki_sensor import MediaWikiSensor
            sensor = MediaWikiSensor(
                pool=pool,
                event_queue=getattr(app.state, 'event_queue', None),
            )
            await sensor.start()
            app.state.mediawiki_sensor = sensor
            logger.info("MediaWiki sensor started")
        except Exception:
            logger.exception("Failed to start MediaWiki sensor")
