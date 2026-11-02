"""MediaWiki sensor endpoints (scan, status, wikis).

Wraps the MediaWikiSensor to provide REST endpoints for manual scan triggers,
status monitoring, and wiki registration.
Only included when caps.mediawiki_sensor is True.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


class MediaWikiScanRequest(BaseModel):
    wiki_id: Optional[int] = Field(None, description="Specific wiki to scan (all if omitted)")


class MediaWikiWikiRegister(BaseModel):
    base_url: str = Field(..., description="Wiki base URL (e.g. https://salishsearestoration.org)")
    wiki_name: Optional[str] = Field(None, description="Human-readable wiki name")
    sync_mode: str = Field("poll", description="Sync mode: poll or dump")


def create_router(pool, sensor):
    """Return an APIRouter for MediaWiki sensor endpoints.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    sensor : MediaWikiSensor or None
        The running sensor instance.
    """
    router = APIRouter(prefix="/mediawiki", tags=["mediawiki"])

    @router.post("/scan")
    async def mediawiki_scan(body: MediaWikiScanRequest):
        """Trigger a manual scan of registered wikis."""
        if sensor is None:
            raise HTTPException(status_code=503, detail="MediaWiki sensor not running")
        result = await sensor.trigger_scan(wiki_id=body.wiki_id)
        return result

    @router.get("/status")
    async def mediawiki_status():
        """Return MediaWiki sensor health and per-wiki sync state."""
        if sensor is None:
            return {"running": False, "message": "MediaWiki sensor not configured"}
        return await sensor.get_status()

    @router.get("/wikis")
    async def mediawiki_wikis():
        """List registered wikis."""
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, wiki_name, base_url, api_url, sync_mode, status, "
                "last_scan_at, config FROM mediawiki_wikis ORDER BY wiki_name"
            )
        return [dict(r) for r in rows]

    @router.post("/wikis")
    async def mediawiki_register_wiki(body: MediaWikiWikiRegister):
        """Register a new wiki for polling."""
        base_url = body.base_url.rstrip("/")
        api_url = base_url + "/api.php"
        wiki_name = body.wiki_name or base_url.split("//")[-1]

        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO mediawiki_wikis (base_url, api_url, wiki_name, sync_mode)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (base_url) DO UPDATE SET
                    sync_mode = EXCLUDED.sync_mode,
                    wiki_name = EXCLUDED.wiki_name
                RETURNING id, wiki_name, base_url, sync_mode, status
            """, base_url, api_url, wiki_name, body.sync_mode)

        return dict(row)

    return router
