"""GitHub sensor endpoints (scan, status, repos, code-query, artifacts).

Wraps the github_sensor module to provide a REST API for GitHub repository
scanning, code artifact extraction, and entity linking.
Only included when caps.github_sensor is True.
"""

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field


# -- Request / Response models -----------------------------------------------

class GitHubScanRequest(BaseModel):
    """Request to trigger a GitHub scan."""
    repo_name: Optional[str] = Field(
        None, description="Specific repo to scan (all repos if omitted)"
    )


class GitHubScanResponse(BaseModel):
    """Result of triggering a GitHub scan."""
    status: str  # "started", "already_running", "error"
    repos_queued: int = 0
    message: str = ""


class GitHubRepoStatus(BaseModel):
    """Status of a single monitored repository."""
    name: str
    url: str
    last_scanned_at: Optional[str] = None
    head_sha: Optional[str] = None
    files_tracked: int = 0
    entities_linked: int = 0


class GitHubStatusResponse(BaseModel):
    """Overall GitHub sensor status."""
    enabled: bool
    scanning: bool = False
    repos_monitored: int = 0
    last_scan_at: Optional[str] = None
    total_artifacts: int = 0


class GitHubCodeQueryRequest(BaseModel):
    """Request to search code artifacts by content or entity references."""
    query: str = Field(..., description="Search query text")
    repo_name: Optional[str] = Field(
        None, description="Limit search to a specific repo"
    )
    file_pattern: Optional[str] = Field(
        None, description="Glob pattern for file paths (e.g. '*.py')"
    )
    limit: int = Field(20, ge=1, le=100)


class CodeArtifact(BaseModel):
    """A code artifact extracted from a GitHub repository."""
    repo_name: str
    file_path: str
    language: Optional[str] = None
    last_modified: Optional[str] = None
    head_sha: Optional[str] = None
    entities_linked: List[str] = []
    snippet: Optional[str] = None


class GitHubCodeQueryResponse(BaseModel):
    """Results of a code artifact search."""
    query: str
    results: List[CodeArtifact]
    total: int


# -- Router factory ----------------------------------------------------------

def create_router(pool, caps, app=None):
    """Return an APIRouter for GitHub sensor endpoints.

    Only included when caps.github_sensor is True.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    caps : Capabilities
        Runtime capabilities (github_sensor flag).
    app : FastAPI, optional
        Application instance for accessing app.state.github_sensor.
    """
    router = APIRouter(prefix="/github", tags=["github"])

    def _get_sensor():
        if app and hasattr(app.state, 'github_sensor'):
            return app.state.github_sensor
        return None

    @router.post("/scan")
    async def github_scan(body: GitHubScanRequest):
        """Trigger a scan of monitored GitHub repositories.

        If repo_name is provided, only that repo is scanned; otherwise
        all configured repos are scanned.
        """
        sensor = _get_sensor()
        if not sensor:
            raise HTTPException(status_code=503, detail="GitHub sensor not running")
        result = await sensor.trigger_scan(repo_name=body.repo_name)
        return result

    @router.get("/status")
    async def github_status():
        """Return GitHub sensor health and scan status."""
        sensor = _get_sensor()
        if sensor:
            return await sensor.get_status()
        return GitHubStatusResponse(
            enabled=caps.github_sensor,
            scanning=False,
            repos_monitored=0,
            last_scan_at=None,
            total_artifacts=0,
        )

    @router.get("/repos")
    async def github_repos(
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        """List monitored GitHub repositories and their scan status."""
        # TODO: Query github_repos table and return GitHubRepoStatus list
        raise HTTPException(
            status_code=501,
            detail="GitHub repo listing not yet implemented",
        )

    @router.post("/code-query", response_model=GitHubCodeQueryResponse)
    async def github_code_query(body: GitHubCodeQueryRequest):
        """Search code artifacts by content or linked entities.

        Searches the code_artifacts table for files matching the query text.
        Optionally filter by repo name or file path pattern.
        """
        # TODO: Wire to full-text search over code_artifacts
        raise HTTPException(
            status_code=501,
            detail="Code query not yet implemented",
        )

    @router.get("/artifacts/{repo}")
    async def github_artifacts(
        repo: str,
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        """List code artifacts for a specific repository.

        Returns files tracked by the GitHub sensor for the given repo,
        including linked entity URIs and last-modified timestamps.
        """
        # TODO: Query code_artifacts table filtered by repo
        raise HTTPException(
            status_code=501,
            detail="GitHub artifacts listing not yet implemented",
        )

    return router
