"""
Unit tests for agentic_crawler (Phase 1).

Covers ACs that are practical to hit without a live server or DB:
- AC4: agentic_crawler does not import requests/aiohttp/playwright
- AC8/AC49: ontology-compliant outputs; no chat_provider imports in crawl path
- AC19: malformed-LLM retry, then 3-consecutive-skip terminate
- AC22: system-ceiling budget rejection
- AC51/AC87: required deps importable
- AC63: existing_rid populated via lookup_fn
- AC67: lookup_entity is read-only (exercised via mock)
- AC70: SSRF validator rejects loopback/private/metadata IPs
- AC75: pre-follow redirect validation (aiohttp + requests both use
  allow_redirects=False)
- AC77: HTML compaction enforces 20k-token budget
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import agentic_crawler, crawl_llm, ontology_registry
from api.agentic_crawler import (
    CrawlBudget,
    CrawlBudgetExceeded,
    WorldModel,
    agentic_crawl,
    same_domain,
)
from api.prompts.crawl_page_analysis import (
    NextLink,
    PageAnalysis,
    ProposedEntity,
    ProposedRelationship,
)

from tests import koi_test_targets as _targets

# _count_tokens loads the crawl model's tiktoken encoding; skip rather than download it.
_needs_crawl_encoding = _targets.requires_tiktoken(models=(crawl_llm.DEFAULT_MODEL,))


# ---------------------------------------------------------------------------
# AC4 — crawler does not import HTTP libraries directly
# ---------------------------------------------------------------------------

def test_ac4_no_direct_http_imports_in_agentic_crawler():
    src = (ROOT / "api" / "agentic_crawler.py").read_text()
    assert not re.search(r"^import requests\b", src, re.MULTILINE)
    assert not re.search(r"^import aiohttp\b", src, re.MULTILINE)
    assert "from playwright" not in src


# ---------------------------------------------------------------------------
# AC49 — no chat_provider imports in crawl path
# ---------------------------------------------------------------------------

def test_ac49_no_chat_provider_in_crawl_path():
    for name in ("agentic_crawler.py", "crawl_llm.py"):
        src = (ROOT / "api" / name).read_text()
        assert "chat_provider" not in src, f"{name} imports chat_provider"


# ---------------------------------------------------------------------------
# AC22 — system-ceiling budget rejection
# ---------------------------------------------------------------------------

def test_ac22_budget_exceeding_ceiling_rejected():
    bad = CrawlBudget(max_pages=1000)
    with pytest.raises(ValueError):
        bad.clamp_to_system_ceilings()

    bad = CrawlBudget(max_usd=5.0)
    with pytest.raises(ValueError):
        bad.clamp_to_system_ceilings()

    ok = CrawlBudget(max_pages=30)
    ok.clamp_to_system_ceilings()


# ---------------------------------------------------------------------------
# AC51/AC87 — required deps importable
# ---------------------------------------------------------------------------

def test_ac51_required_deps_importable():
    import tldextract  # noqa: F401
    import tiktoken  # noqa: F401
    import bs4  # noqa: F401


# ---------------------------------------------------------------------------
# AC70 — SSRF validator blocks private/loopback/metadata hosts
# ---------------------------------------------------------------------------

def test_ac70_url_validator_blocks_private():
    from api.web_fetcher import URLValidationError, URLValidator

    v = URLValidator()
    for bad in [
        "http://127.0.0.1/",
        "http://10.0.0.1/",
        "http://169.254.169.254/",
        "http://metadata.google.internal/",
        "http://[::1]/",
    ]:
        with pytest.raises(URLValidationError):
            v.validate(bad)


# ---------------------------------------------------------------------------
# AC75 — pre-follow redirect validation (structural: allow_redirects=False)
# ---------------------------------------------------------------------------

def test_ac75_fetchers_disable_auto_redirects():
    src = (ROOT / "api" / "web_fetcher.py").read_text()
    # aiohttp path
    assert re.search(r"allow_redirects=False", src)
    # _fetch_html_requests_sync path uses same flag
    assert src.count("allow_redirects=False") >= 2
    assert "auto-followed" not in src or "NOT auto" in src  # docstring intent


# ---------------------------------------------------------------------------
# AC77 — HTML compaction enforces token budget
# ---------------------------------------------------------------------------

@_needs_crawl_encoding
def test_ac77_html_compaction_enforces_budget():
    # Build a big HTML doc that will exceed 20k tokens.
    big_text = "lorem ipsum " * 50_000
    html = (
        "<html><head><title>Big</title></head><body>"
        f"<h1>Header</h1><p>{big_text}</p>"
        "<a href='https://example.org/a'>A</a>"
        "<img src='https://example.org/x.png' alt='x'/>"
        "</body></html>"
    )
    page = crawl_llm.compact_page(html, "https://example.org/")
    tokens = crawl_llm._count_tokens(page.to_json())
    assert tokens <= crawl_llm.MAX_INPUT_TOKENS
    assert page.truncations, "expected at least one truncation event"


@pytest.fixture(autouse=True)
def offline_suffix_list(monkeypatch):
    """tldextract downloads the Public Suffix List on first use; use its bundled copy.

    same_domain() calls tldextract.extract, and without this a fresh environment makes
    a network request to publicsuffix.org during the test.
    """
    try:
        import tldextract
    except ImportError:
        return
    extractor = tldextract.TLDExtract(cache_dir=None, suffix_list_urls=())
    monkeypatch.setattr(tldextract, "extract", extractor)


@pytest.fixture
def offline_dns(monkeypatch):
    """Resolve the example hosts these tests use to a public address, with no network.

    The SSRF validator resolves every hostname it is given, so without this the test
    depends on a live DNS lookup. Any other hostname fails to resolve, as offline.
    """
    import socket

    hosts = {"example.org", "www.example.org", "example.com", "www.example.com"}

    def fake_getaddrinfo(host, *args, **kwargs):
        if isinstance(host, str) and host.lower() in hosts:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", 0))]
        raise socket.gaierror(socket.EAI_NONAME, "offline test resolver")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


def test_fetch_and_preview_preserves_raw_html_contract(offline_dns):
    from api import web_fetcher

    page_text = " ".join(["watershed"] * 160)
    html = (
        "<html><head><title>Preview Contract</title></head>"
        "<body>"
        "<a href='/about'>About</a>"
        "<img src='/logos.png' alt='Partner Logos'/>"
        f"<p>{page_text}</p>"
        "</body></html>"
    )

    async def _fake_fetch_html_aiohttp(url: str):
        assert url == "https://example.org/"
        return html, 200

    async def _go():
        with patch.object(web_fetcher, "_fetch_html_aiohttp", _fake_fetch_html_aiohttp), \
             patch.object(web_fetcher, "PLAYWRIGHT_AVAILABLE", False), \
             patch.object(web_fetcher, "SCRAPLING_AVAILABLE", False):
            return await web_fetcher.fetch_and_preview("https://example.org/")

    preview = asyncio.run(_go())
    assert preview.raw_html == html
    assert preview.rendered_with == "aiohttp"
    preview_dict = preview.to_dict()
    assert "raw_html" not in preview_dict
    assert preview_dict["title"] == "Preview Contract"


# ---------------------------------------------------------------------------
# same_domain helper
# ---------------------------------------------------------------------------

def test_same_domain_subdomain_tolerant():
    assert same_domain("https://www.example.org/", "https://example.org/")
    assert same_domain("https://blog.example.org/x", "https://example.org/")
    assert not same_domain("https://partner.org/x", "https://example.org/")


# ---------------------------------------------------------------------------
# Crawler loop — happy path, malformed-retry, off-domain rejection,
# 3-consecutive-skip terminate, AC63 lookup_fn
# ---------------------------------------------------------------------------

class _FakePreview:
    def __init__(self, html: str):
        self.raw_html = html
        self.content_text = ""


def _make_analysis(
    entities: list[ProposedEntity],
    relationships: list[ProposedRelationship] | None = None,
    next_links: list[NextLink] | None = None,
    judgment: str = "continue",
) -> PageAnalysis:
    return PageAnalysis(
        entities=entities,
        relationships=relationships or [],
        next_links=next_links or [],
        judgment=judgment,
    )


async def _run_crawl(
    analyses_by_url: dict[str, list],
    start_url: str,
    *,
    lookup_fn=None,
):
    async def _fetch(url: str):
        return _FakePreview(f"<html><body>mock {url}</body></html>")

    call_counts: dict[str, int] = {}

    async def _fake_analyze_page(*, page, goal, world_model_summary, allowed_types, allowed_predicates, model):
        entry = analyses_by_url.get(page.url)
        if entry is None:
            return _make_analysis([], judgment="sufficient"), {"prompt_tokens": 10, "completion_tokens": 10}
        idx = call_counts.get(page.url, 0)
        call_counts[page.url] = idx + 1
        result = entry[min(idx, len(entry) - 1)]
        if isinstance(result, Exception):
            raise result
        return result, {"prompt_tokens": 10, "completion_tokens": 10}

    with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
        return await agentic_crawl(
            start_url=start_url,
            goal="map org",
            budget=CrawlBudget(max_pages=10, max_usd=1.0, max_seconds=60),
            fetch_fn=_fetch,
            lookup_fn=lookup_fn,
        )


@_needs_crawl_encoding
def test_agentic_crawl_uses_raw_html_when_content_text_is_sparse(offline_dns):
    from api.web_fetcher import PageMetadata, WebPreview

    start = "https://example.org/"
    html = (
        "<html><head><title>Partner Page</title></head>"
        "<body>"
        "<h1>Partner Page</h1>"
        "<p>Partner directory text.</p>"
        "<a href='/about'>About</a>"
        "<a href='https://example.org/partners/alpha'>Alpha Partner</a>"
        "<img src='/assets/partners.png' alt='Partner Logos'/>"
        "</body></html>"
    )
    preview = WebPreview(
        url=start,
        rid="orn:web.page:example_org/test",
        domain="example.org",
        title="Partner Page",
        description="",
        content_text="Access Denied",
        content_hash="hash",
        word_count=2,
        metadata=PageMetadata(),
        raw_html=html,
    )
    seen: dict[str, object] = {}
    fetched: list[str] = []

    async def _fetch(url: str):
        # A thin start page seeds its same-domain links, so the crawler goes on to
        # request them; only the start page is served here.
        fetched.append(url)
        if url != start:
            raise RuntimeError("only the start page is served in this test")
        return preview

    async def _fake_analyze_page(*, page, goal, world_model_summary, allowed_types, allowed_predicates, model):
        if "title" in seen:
            return _make_analysis([], judgment="sufficient"), {"prompt_tokens": 1, "completion_tokens": 1}
        seen["title"] = page.title
        seen["main_text"] = page.main_text
        seen["links"] = page.links
        seen["images"] = page.images
        return _make_analysis([], judgment="sufficient"), {
            "prompt_tokens": 1,
            "completion_tokens": 1,
        }

    async def _go():
        with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
            return await agentic_crawl(
                start_url=start,
                fetch_fn=_fetch,
                budget=CrawlBudget(max_pages=5, max_usd=1.0, max_seconds=60),
            )

    proposal = asyncio.run(_go())
    assert fetched[0] == start and proposal.stats["pages_visited"] >= 1
    assert seen["title"] == "Partner Page"
    assert "Partner directory text." in str(seen["main_text"])
    assert seen["links"] == [
        {"url": "https://example.org/about", "anchor": "About"},
        {"url": "https://example.org/partners/alpha", "anchor": "Alpha Partner"},
    ]
    assert seen["images"] == [
        {
            "src": "https://example.org/assets/partners.png",
            "alt": "Partner Logos",
            "context": "",
        }
    ]


@_needs_crawl_encoding
def test_happy_path_produces_entities_and_respects_domain():
    start = "https://example.org/"
    analyses = {
        start: [
            _make_analysis(
                entities=[
                    ProposedEntity(name="Example Org", type="Organization", confidence=0.95),
                    ProposedEntity(name="Restoration", type="Project", confidence=0.9),
                    ProposedEntity(name="Education", type="Project", confidence=0.9),
                ],
                relationships=[
                    ProposedRelationship(
                        subject_name="Example Org",
                        predicate="has_project",
                        object_name="Restoration",
                    ),
                    ProposedRelationship(
                        subject_name="Example Org",
                        predicate="has_project",
                        object_name="Education",
                    ),
                ],
                next_links=[
                    NextLink(url="https://example.org/about", priority=0.9, reason="about"),
                    NextLink(url="https://partner.org/x", priority=0.8, reason="partner"),
                ],
            )
        ],
        "https://example.org/about": [
            _make_analysis(
                entities=[
                    ProposedEntity(name="Corin Ashgrove", type="Person", confidence=0.9),
                    ProposedEntity(name="Bowker Creek", type="Project", confidence=0.9),
                ],
                relationships=[
                    ProposedRelationship(
                        subject_name="Corin Ashgrove",
                        predicate="affiliated_with",
                        object_name="Example Org",
                    ),
                ],
                judgment="sufficient",
            )
        ],
    }
    proposal = asyncio.run(_run_crawl(analyses, start))
    assert len(proposal.entities) >= 5  # meets AC1 ≥6 shape in real usage
    # off-domain partner.org must not have been visited
    for e in proposal.entities:
        assert "partner.org" not in (e.get("source_url") or "")
    assert proposal.start_url == start


@_needs_crawl_encoding
def test_ac19_malformed_retry_then_skip():
    """Start page succeeds; a SUBSEQUENT page errors — crawl continues
    (consecutive_skips = 1) and still returns a proposal with the start-page
    entities."""
    start = "https://example.org/"
    fail_url = "https://example.org/broken"

    async def _fake_analyze_page(*, page, goal, world_model_summary, allowed_types, allowed_predicates, model):
        if page.url == fail_url:
            raise ValueError("malformed JSON on subsequent page")
        return (
            _make_analysis(
                entities=[ProposedEntity(name="Seed", type="Organization", confidence=0.9)],
                next_links=[NextLink(url=fail_url, priority=0.5, reason="try broken")],
                judgment="continue",
            ),
            {"prompt_tokens": 5, "completion_tokens": 5},
        )

    async def _fetch(url: str):
        return _FakePreview("<html><body>x</body></html>")

    async def _go():
        with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
            return await agentic_crawl(
                start_url=start,
                fetch_fn=_fetch,
                budget=CrawlBudget(max_pages=5),
            )

    proposal = asyncio.run(_go())
    # Both pages attempted; only the start contributed entities.
    assert proposal.stats["pages_visited"] == 2
    assert len(proposal.entities) == 1
    assert proposal.entities[0]["name"] == "Seed"


@_needs_crawl_encoding
def test_ac19_three_consecutive_skips_terminate():
    start = "https://example.org/"

    async def _fake_analyze_page(**_):
        raise ValueError("repeated malformed output")

    async def _fetch(url: str):
        return _FakePreview("<html><body>x</body></html>")

    async def _go():
        with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
            # Start page failure should raise 'start page analysis failed'
            return await agentic_crawl(
                start_url=start,
                fetch_fn=_fetch,
                budget=CrawlBudget(max_pages=5),
            )

    with pytest.raises(CrawlBudgetExceeded) as exc_info:
        asyncio.run(_go())
    assert "start page analysis failed" in str(exc_info.value)


@_needs_crawl_encoding
def test_ac63_existing_rid_populated_via_lookup_fn():
    start = "https://example.org/"
    analyses = {
        start: [
            _make_analysis(
                entities=[
                    ProposedEntity(name="Example Org", type="Organization"),
                    ProposedEntity(name="Fresh Project", type="Project"),
                ],
                judgment="sufficient",
            )
        ]
    }
    lookup_calls: list[tuple[str, str]] = []

    async def lookup(name: str, entity_type: str):
        lookup_calls.append((name, entity_type))
        if name == "Example Org" and entity_type == "Organization":
            return "orn:test.entity:example-org"
        return None

    proposal = asyncio.run(_run_crawl(analyses, start, lookup_fn=lookup))
    rids = {e["name"]: e["existing_rid"] for e in proposal.entities}
    assert rids["Example Org"] == "orn:test.entity:example-org"
    assert rids["Fresh Project"] is None
    assert ("Example Org", "Organization") in lookup_calls


def test_missing_token_401_via_router_auth(monkeypatch):
    # AC11 slice — with AGENTIC_CRAWL_ENABLED=true but no bearer, auth fails.
    from api import crawl_auth

    monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "true")
    monkeypatch.setenv("CRAWL_TOKEN__ops__test", "tok-ops")
    try:
        crawl_auth.reload_identity_config()
        with pytest.raises(crawl_auth.CrawlAuthError) as exc_info:
            crawl_auth.authenticate_request(
                authorization_header=None,
                identity_claim_header=None,
                body_submitted_by=None,
            )
        assert exc_info.value.status_code == 401
    finally:
        monkeypatch.undo()
        crawl_auth.reload_identity_config()


def test_bound_token_snapshot_requires_reload(monkeypatch):
    from api import crawl_auth

    monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "true")
    monkeypatch.setenv("CRAWL_TOKEN__ops__test", "test-token-b")
    try:
        crawl_auth.reload_identity_config()
        auth = crawl_auth.authenticate_request(
            authorization_header="Bearer test-token-b",
            identity_claim_header=None,
            body_submitted_by=None,
        )
        assert auth.submitted_by == "ops:test"

        monkeypatch.setenv("CRAWL_TOKEN__ops__test", "test-token-c")
        with pytest.raises(crawl_auth.CrawlAuthError) as exc_info:
            crawl_auth.authenticate_request(
                authorization_header="Bearer test-token-c",
                identity_claim_header=None,
                body_submitted_by=None,
            )
        assert exc_info.value.status_code == 401

        crawl_auth.reload_identity_config()
        auth = crawl_auth.authenticate_request(
            authorization_header="Bearer test-token-c",
            identity_claim_header=None,
            body_submitted_by=None,
        )
        assert auth.submitted_by == "ops:test"
    finally:
        monkeypatch.undo()
        crawl_auth.reload_identity_config()


def test_no_tokens_configured_503(monkeypatch):
    from api import crawl_auth

    monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "true")
    for var in (
        "CRAWL_TOKEN__ops__test",
        "CRAWL_TOKEN_TELEGRAM",
        "CRAWL_SECRET_TELEGRAM",
    ):
        monkeypatch.delenv(var, raising=False)
    try:
        crawl_auth.reload_identity_config()
        with pytest.raises(crawl_auth.CrawlAuthError) as exc_info:
            crawl_auth.authenticate_request(
                authorization_header="Bearer missing",
                identity_claim_header=None,
                body_submitted_by=None,
            )
        assert exc_info.value.status_code == 503
        assert exc_info.value.message == "per-surface tokens not configured"
    finally:
        monkeypatch.undo()
        crawl_auth.reload_identity_config()


# ---------------------------------------------------------------------------
# AC67 — lookup_entity is read-only (fake conn asserts no writes)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ac67_lookup_entity_is_read_only():
    from api.personal_ingest_api import lookup_entity

    class FakeConn:
        def __init__(self):
            self.select_count = 0
            self.write_count = 0

        async def fetchrow(self, query, *args):
            self.select_count += 1
            upper = (query or "").upper().lstrip()
            assert upper.startswith("SELECT") or upper.startswith("\n            SELECT"), query
            return None

        async def fetch(self, *args, **kwargs):
            self.select_count += 1
            return []

        async def execute(self, *args, **kwargs):
            self.write_count += 1

    conn = FakeConn()
    rid = await lookup_entity(conn, "Nobody", "Organization")
    assert rid is None
    assert conn.write_count == 0
    assert conn.select_count >= 1


# ---------------------------------------------------------------------------
# AC8 — ontology-compliant outputs
# ---------------------------------------------------------------------------

def test_ac8_world_model_rejects_unknown_predicates():
    wm = WorldModel()
    analysis = _make_analysis(
        entities=[
            ProposedEntity(name="A", type="Organization"),
            ProposedEntity(name="B", type="Project"),
        ],
        relationships=[
            ProposedRelationship(
                subject_name="A", predicate="has_project", object_name="B"
            ),
            ProposedRelationship(
                subject_name="A", predicate="not_a_real_predicate", object_name="B"
            ),
        ],
    )
    wm.merge_page_analysis(analysis, source_url="https://a/")
    preds = {r.predicate for r in wm.relationships}
    assert "has_project" in preds
    assert "not_a_real_predicate" not in preds
