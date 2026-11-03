#!/usr/bin/env python3
"""
GitHub Webhook Handler for Code Graph Updates.

Receives push events from GitHub and triggers code extraction for changed repos.

Setup:
1. Start the server: uvicorn api.github_webhook:app --host 0.0.0.0 --port 8360
2. Configure webhooks in GitHub repo settings:
   - Payload URL: http://your-server:8360/webhook/github
   - Content type: application/json
   - Secret: Set GITHUB_WEBHOOK_SECRET env var
   - Events: Just the push event
"""

import os
import sys
import hmac
import hashlib
import subprocess

from api.async_proc import run_async
from pathlib import Path
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from loguru import logger

# Configuration
WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "")
# Default to /opt/projects; override with CODE_GRAPH_BASE_PATH
BASE_PATH = Path(os.environ.get("CODE_GRAPH_BASE_PATH", "/opt/projects"))

# Map GitHub repo names to local paths
# Some repos live in a regen-repos/ subdirectory of BASE_PATH (REGEN_REPOS_PATH)
REGEN_REPOS_PATH = Path(os.environ.get("REGEN_REPOS_PATH", str(BASE_PATH / "regen-repos")))
REPO_PATHS = {
    # Regen repos (under REGEN_REPOS_PATH)
    "regen-ledger": REGEN_REPOS_PATH / "regen-ledger",
    "regen-web": REGEN_REPOS_PATH / "regen-web",
    "regen-data-standards": REGEN_REPOS_PATH / "regen-data-standards",
    # KOI repos (directly under base path)
    "koi-processor": BASE_PATH / "koi-processor",
    "koi-sensors": BASE_PATH / "koi-sensors",
    "koi-research": BASE_PATH / "koi-research",
    "regen-koi-mcp": BASE_PATH / "regen-koi-mcp",
    "personal-koi-mcp": BASE_PATH / "personal-koi-mcp",
    # Batch 1 - High priority new repos
    "regen-compute": REGEN_REPOS_PATH / "regen-compute",
    "revenue-hunter-cec": REGEN_REPOS_PATH / "revenue-hunter-cec",
    "regen-ai-core": REGEN_REPOS_PATH / "regen-ai-core",
    "regen-ai-claude": REGEN_REPOS_PATH / "regen-ai-claude",
    # Batch 2 - Additional repos
    "regen-js": REGEN_REPOS_PATH / "regen-js",
    "agentic-tokenomics": REGEN_REPOS_PATH / "agentic-tokenomics",
    "protocol-politicians": REGEN_REPOS_PATH / "protocol-politicians",
    "regen-claude-config": REGEN_REPOS_PATH / "regen-claude-config",
    "regenie-corpus": REGEN_REPOS_PATH / "regenie-corpus",
    "mcp": REGEN_REPOS_PATH / "mcp",
    "pacto-framework": REGEN_REPOS_PATH / "pacto-framework",
    "koi-gov": REGEN_REPOS_PATH / "koi-gov",
    # Already in sensor but need webhook mapping
    "regen-registry-handbook": REGEN_REPOS_PATH / "regen-registry-handbook",
    "regen-registry-methodology-library": REGEN_REPOS_PATH / "regen-registry-methodology-library",
}

TRACKED_BRANCHES = tuple(
    branch.strip()
    for branch in os.environ.get("WEBHOOK_TRACKED_BRANCHES", "main,master,regen-prod").split(",")
    if branch.strip()
)
if not TRACKED_BRANCHES:
    TRACKED_BRANCHES = ("main", "master", "regen-prod")
TRACKED_REFS = {f"refs/heads/{branch}" for branch in TRACKED_BRANCHES}

# Track running extractions
running_extractions: dict[str, datetime] = {}

app = FastAPI(
    title="Code Graph Webhook",
    description="GitHub webhook handler for automatic code graph updates",
)


def verify_signature(payload: bytes, signature: str) -> bool:
    """Verify GitHub webhook signature."""
    if not WEBHOOK_SECRET:
        logger.warning("GITHUB_WEBHOOK_SECRET not set, skipping signature verification")
        return True

    if not signature or not signature.startswith("sha256="):
        return False

    expected = hmac.new(
        WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(f"sha256={expected}", signature)


async def run_extraction(repo_name: str, repo_path: Path, trigger: str = "webhook"):
    """Run extraction for a single repo."""
    if repo_name in running_extractions:
        logger.info(f"Extraction already running for {repo_name}, skipping")
        return

    try:
        running_extractions[repo_name] = datetime.now()
        logger.info(f"Starting extraction for {repo_name} (triggered by {trigger})")

        if not repo_path.exists():
            logger.error(f"Repository path does not exist for {repo_name}: {repo_path}")
            return

        # Pull latest changes
        # #15-class defect: this is an `async def`, so a bare subprocess.run() froze the
        # WHOLE shared :8351 event loop — 60s for the pull and up to 600s for the
        # extraction below, i.e. ten minutes of every other request stalling on one
        # GitHub webhook. See api/async_proc.py.
        pull_result = await run_async(
            ["git", "pull", "--ff-only"],
            cwd=repo_path,
            timeout=60,
        )

        if pull_result.returncode != 0:
            logger.warning(
                f"Git pull for {repo_name} returned non-zero: {pull_result.stderr.strip()}"
            )
        else:
            logger.info(f"Git pull for {repo_name}: {pull_result.stdout.strip()}")

        # Run extraction
        koi_processor = BASE_PATH / "koi-processor"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(koi_processor)

        extract_result = await run_async(
            [
                sys.executable,
                str(koi_processor / "scripts" / "load_to_staging.py"),
                "--repo",
                repo_name,
                "--path",
                str(repo_path),
            ],
            cwd=koi_processor,
            env=env,
            timeout=600,
        )

        if extract_result.returncode != 0:
            logger.error(f"Extraction failed for {repo_name}: {extract_result.stderr.strip()}")
        else:
            logger.info(f"Extraction completed for {repo_name}")
            for line in extract_result.stdout.split("\n"):
                if "entities" in line.lower() or "edges" in line.lower():
                    logger.info(f"  {line.strip()}")

    except subprocess.TimeoutExpired:
        logger.error(f"Extraction timed out for {repo_name}")
    except Exception as exc:
        logger.error(f"Extraction error for {repo_name}: {exc}")
    finally:
        running_extractions.pop(repo_name, None)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "service": "code-graph-webhook",
        "status": "healthy",
        "configured_repos": list(REPO_PATHS.keys()),
        "running_extractions": {
            repo: started_at.isoformat() for repo, started_at in running_extractions.items()
        },
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "running_extractions": list(running_extractions.keys()),
    }


@app.post("/webhook/github")
async def github_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle GitHub webhook push events."""
    body = await request.body()

    signature = request.headers.get("X-Hub-Signature-256", "")
    if not verify_signature(body, signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {exc}")

    event_type = request.headers.get("X-GitHub-Event", "")
    if event_type != "push":
        return {"status": "ignored", "reason": f"Event type '{event_type}' not handled"}

    repo_full_name = payload.get("repository", {}).get("full_name", "")
    repo_name = payload.get("repository", {}).get("name", "")
    ref = payload.get("ref", "")

    logger.info(f"Received push event for {repo_full_name} on {ref}")

    if ref not in TRACKED_REFS:
        return {
            "status": "ignored",
            "reason": f"Branch '{ref}' not tracked (tracked: {sorted(TRACKED_BRANCHES)})",
        }

    if repo_name not in REPO_PATHS:
        return {
            "status": "ignored",
            "reason": f"Repository '{repo_name}' not configured",
        }

    if repo_name in running_extractions:
        return {
            "status": "skipped",
            "reason": f"Extraction already running for {repo_name}",
            "started_at": running_extractions[repo_name].isoformat(),
        }

    commits = payload.get("commits", [])
    commit_count = len(commits)
    head_commit = payload.get("head_commit", {})
    commit_msg = head_commit.get("message", "")[:100]
    pusher = payload.get("pusher", {}).get("name", "unknown")

    logger.info(f"Processing {commit_count} commit(s) by {pusher}: {commit_msg}")

    repo_path = REPO_PATHS[repo_name]
    background_tasks.add_task(run_extraction, repo_name, repo_path, f"push by {pusher}")

    return {
        "status": "accepted",
        "repo": repo_name,
        "ref": ref,
        "commits": commit_count,
        "pusher": pusher,
        "message": f"Extraction scheduled for {repo_name}",
    }


@app.post("/webhook/extract/{repo_name}")
async def manual_extract(repo_name: str, background_tasks: BackgroundTasks):
    """Manually trigger extraction for a repo."""
    if repo_name not in REPO_PATHS:
        raise HTTPException(
            status_code=404,
            detail=f"Repository '{repo_name}' not configured. Available: {list(REPO_PATHS.keys())}",
        )

    if repo_name in running_extractions:
        return {
            "status": "skipped",
            "reason": f"Extraction already running for {repo_name}",
            "started_at": running_extractions[repo_name].isoformat(),
        }

    repo_path = REPO_PATHS[repo_name]
    background_tasks.add_task(run_extraction, repo_name, repo_path, "manual trigger")

    return {
        "status": "accepted",
        "repo": repo_name,
        "message": f"Manual extraction scheduled for {repo_name}",
    }


@app.post("/webhook/extract-all")
async def extract_all(background_tasks: BackgroundTasks):
    """Trigger extraction for all configured repos."""
    scheduled = []
    skipped = []

    for repo_name, repo_path in REPO_PATHS.items():
        if repo_name in running_extractions:
            skipped.append(repo_name)
            continue
        background_tasks.add_task(run_extraction, repo_name, repo_path, "extract-all")
        scheduled.append(repo_name)

    return {
        "status": "accepted",
        "scheduled": scheduled,
        "skipped": skipped,
        "message": f"Scheduled extraction for {len(scheduled)} repos",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8360)
