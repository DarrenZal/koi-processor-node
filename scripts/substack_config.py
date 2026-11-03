"""Load the operator's Substack publication list.

WHICH publications to ingest is *personal config*, not code — kept out of the
repo so forks configure their own. Read from:
  $SUBSTACK_PUBLICATIONS_CONFIG, else config/substack_publications.yaml
(copy config/substack_publications.example.yaml to create it).

Read by substack_sensor.py (free + cookie-auth paid posts via the API) and by the
Gmail paid-post bridge, ingest_substack_from_gmail.py (run by run_substack_gmail_bridge.sh).
"""

import os
from pathlib import Path
from typing import Any, Dict, List

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent


def config_path() -> Path:
    override = os.getenv("SUBSTACK_PUBLICATIONS_CONFIG")
    return Path(override) if override else _REPO_ROOT / "config" / "substack_publications.yaml"


def load_config() -> Dict[str, Any]:
    p = config_path()
    if not p.exists():
        raise FileNotFoundError(
            f"Substack publications config not found at {p}.\n"
            f"Copy config/substack_publications.example.yaml to "
            f"config/substack_publications.yaml and add your own publications "
            f"(this file is personal — it is gitignored, not committed)."
        )
    with p.open() as f:
        return yaml.safe_load(f) or {}


def load_publications() -> List[Dict[str, Any]]:
    """Normalized publication dicts. Each has feed_slug, base, author, domain,
    tags, plus derived host + author_entity, and optional email_sender."""
    pubs: List[Dict[str, Any]] = []
    for raw in load_config().get("publications") or []:
        if not raw.get("feed_slug") or not raw.get("base"):
            raise ValueError(f"substack publication missing feed_slug/base: {raw}")
        pub = dict(raw)
        pub.setdefault("author", pub["feed_slug"])
        pub.setdefault("domain", "other")
        pub.setdefault("tags", [])
        pub["base"] = pub["base"].rstrip("/")
        pub["host"] = pub["base"].split("://", 1)[-1]
        pub["author_entity"] = {"name": pub["author"], "type": "Person"}
        pubs.append(pub)
    return pubs


def gmail_user() -> str:
    """Inbox for the Gmail paid-post bridge (falls back to $SUBSTACK_GMAIL_USER)."""
    return str(load_config().get("gmail_user") or os.getenv("SUBSTACK_GMAIL_USER", "")).strip()
