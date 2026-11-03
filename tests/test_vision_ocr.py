"""
Phase 2 tests — vision OCR routing, SSRF protection, confidence filter.

ACs covered:
- AC29 — confidence routing (0.95 include, 0.75 review, 0.55 dropped)
- AC45 — role-to-predicate routing (partner_grid, team_photo, infographic,
  generic_decoration/unknown)
- AC70 part 2 — SSRF blocks on image URLs
- AC49 — vision path has zero chat_provider references
- Content-type skip: non-image/* rejected
- Size skip: declared Content-Length >5 MB rejected
- Retry-skip: malformed LLM JSON → single retry → second failure returns
  empty list (never terminates crawl)
"""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import agentic_crawler, crawl_llm, vision_ocr
from api.agentic_crawler import (
    CrawlBudget,
    WorldModel,
    _apply_vision_orgs,
    _ROLE_POLICY,
    agentic_crawl,
)
from api.prompts.crawl_page_analysis import (
    ImageRegion,
    NextLink,
    PageAnalysis,
    ProposedEntity as PEModel,
)

from tests import koi_test_targets as _targets

# _count_tokens loads the crawl model's tiktoken encoding; skip rather than download it.
_needs_crawl_encoding = _targets.requires_tiktoken(models=(crawl_llm.DEFAULT_MODEL,))


# ---------------------------------------------------------------------------
# AC49 — no chat_provider references in vision path
# ---------------------------------------------------------------------------

def test_ac49_vision_path_has_no_chat_provider():
    for name in ("vision_ocr.py", "crawl_llm.py", "agentic_crawler.py"):
        src = (ROOT / "api" / name).read_text()
        assert "chat_provider" not in src, f"{name} references chat_provider"


# ---------------------------------------------------------------------------
# AC29 — confidence routing
# ---------------------------------------------------------------------------

def test_ac29_confidence_routing():
    """0.95 included clean; 0.75 included with requires_review=True; 0.55 dropped."""
    world = WorldModel()
    # Seed a root Organization so vision policy has an anchor.
    world.upsert_entity(
        agentic_crawler.ProposedEntity(
            name="Root Org", type="Organization", confidence=1.0
        )
    )

    _apply_vision_orgs(
        world,
        orgs=[
            {"name": "Clear Logo Org", "confidence": 0.95},
            {"name": "Blurry Logo Org", "confidence": 0.75},
            {"name": "Barely Visible Org", "confidence": 0.55},
        ],
        role="partner_grid",
        source_url="https://root.org/partners",
        source_image="https://www.example.com/partners.png",
    )

    names = {e.name: e for e in world.entities}
    assert "Clear Logo Org" in names
    assert "Blurry Logo Org" in names
    assert "Barely Visible Org" not in names, "sub-0.7 should be dropped"

    assert names["Clear Logo Org"].requires_review is False
    assert names["Blurry Logo Org"].requires_review is True

    # Source image propagated
    assert names["Clear Logo Org"].source_image == "https://www.example.com/partners.png"


# ---------------------------------------------------------------------------
# AC45 — role-to-predicate routing
# ---------------------------------------------------------------------------

def _world_with_root(root_type: str = "Organization") -> WorldModel:
    world = WorldModel()
    world.upsert_entity(
        agentic_crawler.ProposedEntity(name="Root", type=root_type, confidence=1.0)
    )
    return world


def test_ac45_partner_grid_emits_collaborates_with():
    world = _world_with_root()
    _apply_vision_orgs(
        world,
        orgs=[{"name": "Partner A", "confidence": 0.9}],
        role="partner_grid",
        source_url="u",
        source_image="i",
    )
    preds = [(r.subject_index, r.predicate, r.object_index) for r in world.relationships]
    # Root (0) -> collaborates_with -> Partner A (1)
    assert (0, "collaborates_with", 1) in preds
    assert world.entities[1].type == "Organization"


def test_ac45_sponsor_list_adds_both_predicates():
    world = _world_with_root()
    _apply_vision_orgs(
        world,
        orgs=[{"name": "Sponsor Co", "confidence": 0.9}],
        role="sponsor_list",
        source_url="u",
        source_image="i",
    )
    preds = {(r.subject_index, r.predicate, r.object_index) for r in world.relationships}
    # collaborates_with (out): Root → Sponsor
    assert (0, "collaborates_with", 1) in preds
    # affiliated_with (in): Sponsor → Root
    assert (1, "affiliated_with", 0) in preds


def test_ac45_team_photo_emits_person_affiliated_with():
    world = _world_with_root()
    _apply_vision_orgs(
        world,
        orgs=[{"name": "Corin Ashgrove", "confidence": 0.9}],
        role="team_photo",
        source_url="u",
        source_image="i",
    )
    assert world.entities[1].type == "Person"
    preds = [(r.subject_index, r.predicate, r.object_index) for r in world.relationships]
    assert (1, "affiliated_with", 0) in preds


def test_ac45_infographic_emits_concept_about():
    world = _world_with_root()
    _apply_vision_orgs(
        world,
        orgs=[{"name": "Watershed Health", "confidence": 0.9}],
        role="infographic",
        source_url="u",
        source_image="i",
    )
    assert world.entities[1].type == "Concept"
    preds = [(r.subject_index, r.predicate, r.object_index) for r in world.relationships]
    assert (1, "about", 0) in preds


def test_ac45_generic_decoration_skipped():
    world = _world_with_root()
    _apply_vision_orgs(
        world,
        orgs=[{"name": "Stock Photo Co", "confidence": 0.95}],
        role="generic_decoration",
        source_url="u",
        source_image="i",
    )
    # Only root should remain
    assert len(world.entities) == 1
    assert not world.relationships


def test_ac45_unknown_role_skipped():
    world = _world_with_root()
    _apply_vision_orgs(
        world,
        orgs=[{"name": "Mystery Co", "confidence": 0.95}],
        role="unknown",
        source_url="u",
        source_image="i",
    )
    assert len(world.entities) == 1
    assert not world.relationships


# ---------------------------------------------------------------------------
# AC70 part 2 — SSRF blocks on image URLs
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/logo.png",
        "http://10.0.0.1/logo.png",
        "http://169.254.169.254/creds.png",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://[::1]/x.png",
    ],
)
def test_ac70_vision_fetch_blocks_private_urls(url):
    async def _run():
        return await vision_ocr.fetch_image_bytes(url)

    result = asyncio.run(_run())
    assert result is None, f"expected SSRF block for {url}"


# ---------------------------------------------------------------------------
# Content-type + size skip paths
# ---------------------------------------------------------------------------

class _FakeResponse:
    def __init__(self, *, status=200, headers=None, body=b"", chunks=None):
        self.status = status
        self.headers = headers or {}
        self._body = body
        self._chunks = list(chunks) if chunks is not None else None
        self._content = self._Content(body, chunks=self._chunks)

    class _Content:
        def __init__(self, body, *, chunks=None):
            self._body = body
            self._chunks = list(chunks) if chunks is not None else None
            self._sent_body = False

        async def read(self, cap):
            if self._chunks is not None:
                if not self._chunks:
                    return b""
                return self._chunks.pop(0)[: cap]
            if self._sent_body:
                return b""
            self._sent_body = True
            return self._body[: cap]

    @property
    def content(self):
        return self._content

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


class _FakeSession:
    def __init__(self, response):
        self._response = response
        self.last_url = None
        self.last_allow_redirects = None
        self.last_proxy = None

    def get(self, url, *, headers=None, allow_redirects=True, proxy=None):
        self.last_url = url
        self.last_allow_redirects = allow_redirects
        self.last_proxy = proxy
        return self._response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


def _patched_session(monkeypatch, fake_session):
    def _factory(*_args, **_kwargs):
        return fake_session

    monkeypatch.setattr(vision_ocr.aiohttp, "ClientSession", _factory)
    # Clear _PROXY_URL for any test that uses a single _FakeResponse —
    # fetch_image_bytes retries with the proxy when the first fetch
    # returns None, and the retry would consume a now-drained chunks
    # list and assert False on (b'', ...) tuples instead of None.
    # Tests that specifically exercise the proxy path re-set _PROXY_URL
    # via their own monkeypatch call (e.g. test_proxy_support).
    monkeypatch.setattr(vision_ocr, "_PROXY_URL", "")


def test_non_image_content_type_rejected(monkeypatch, offline_dns):
    resp = _FakeResponse(
        status=200,
        headers={"Content-Type": "text/html; charset=utf-8"},
        body=b"<html></html>",
    )
    _patched_session(monkeypatch, _FakeSession(resp))

    async def _run():
        return await vision_ocr.fetch_image_bytes("https://www.example.com/x.html")

    assert asyncio.run(_run()) is None


def test_oversize_declared_length_rejected(monkeypatch, offline_dns):
    resp = _FakeResponse(
        status=200,
        headers={
            "Content-Type": "image/png",
            "Content-Length": str(vision_ocr.MAX_IMAGE_BYTES + 1),
        },
        body=b"x",
    )
    _patched_session(monkeypatch, _FakeSession(resp))

    async def _run():
        return await vision_ocr.fetch_image_bytes("https://www.example.com/huge.png")

    assert asyncio.run(_run()) is None


def test_oversize_actual_body_rejected(monkeypatch, offline_dns):
    big = b"\x00" * (vision_ocr.MAX_IMAGE_BYTES + 10)
    resp = _FakeResponse(
        status=200,
        headers={"Content-Type": "image/jpeg"},  # No Content-Length header
        chunks=[big[i: i + 64 * 1024] for i in range(0, len(big), 64 * 1024)] + [b""],
    )
    _patched_session(monkeypatch, _FakeSession(resp))

    async def _run():
        return await vision_ocr.fetch_image_bytes("https://www.example.com/big.jpg")

    assert asyncio.run(_run()) is None


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


def test_good_image_fetch_returns_bytes(monkeypatch, offline_dns):
    body = b"\x89PNG\r\n\x1a\nfake image data"
    resp = _FakeResponse(
        status=200,
        headers={"Content-Type": "image/png; charset=binary"},
        body=body,
    )
    _patched_session(monkeypatch, _FakeSession(resp))

    async def _run():
        return await vision_ocr.fetch_image_bytes("https://www.example.com/ok.png")

    result = asyncio.run(_run())
    assert result is not None
    raw, content_type, final_url = result
    assert raw == body
    assert content_type == "image/png"


def test_good_image_fetch_reads_until_eof(monkeypatch, offline_dns):
    body = b"\x89PNG\r\n\x1a\nchunked image data"
    resp = _FakeResponse(
        status=200,
        headers={"Content-Type": "image/png"},
        chunks=[body[:7], body[7:15], body[15:], b""],
    )
    _patched_session(monkeypatch, _FakeSession(resp))

    async def _run():
        return await vision_ocr.fetch_image_bytes("https://www.example.com/chunked.png")

    result = asyncio.run(_run())
    assert result is not None
    raw, content_type, final_url = result
    assert raw == body
    assert content_type == "image/png"


def test_blocked_image_retries_via_proxy(monkeypatch, offline_dns):
    body = b"\x89PNG\r\n\x1a\nproxy image data"
    direct_session = _FakeSession(
        _FakeResponse(status=403, headers={"Content-Type": "text/plain"}, body=b"blocked")
    )
    proxy_session = _FakeSession(
        _FakeResponse(status=200, headers={"Content-Type": "image/png"}, body=body)
    )
    sessions = [direct_session, proxy_session]

    def _factory(*_args, **_kwargs):
        return sessions.pop(0)

    monkeypatch.setattr(vision_ocr.aiohttp, "ClientSession", _factory)
    monkeypatch.setattr(vision_ocr, "_PROXY_URL", "http://proxy.internal:8080")

    async def _run():
        return await vision_ocr.fetch_image_bytes("https://www.example.com/logo.png")

    result = asyncio.run(_run())
    assert result is not None
    raw, content_type, final_url = result
    assert raw == body
    assert content_type == "image/png"
    assert direct_session.last_proxy is None
    assert proxy_session.last_proxy == "http://proxy.internal:8080"


# ---------------------------------------------------------------------------
# crawl_llm.extract_orgs_from_image retry-then-skip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_vision_llm_retry_then_skip_returns_empty():
    """Malformed JSON first reply → retry → second reply also malformed → (
    [], usage)."""
    calls = {"n": 0}

    async def _fake_chat_completion(messages, *, model, json_mode=True, max_tokens=1024):
        calls["n"] += 1
        return "not json", {"prompt_tokens": 5, "completion_tokens": 5}

    with patch.object(crawl_llm, "_chat_completion", _fake_chat_completion):
        orgs, usage = await crawl_llm.extract_orgs_from_image(
            image_bytes=b"\x89PNG", content_type="image/png", context="ctx", role="partner_grid"
        )

    assert orgs == []
    assert calls["n"] == 2, "expected initial call + one retry"
    assert usage["prompt_tokens"] == 10  # accumulated across both attempts


@pytest.mark.asyncio
async def test_vision_llm_happy_path_returns_orgs():
    canned = '{"orgs": [{"name": "Foo Partner", "confidence": 0.92}, {"name": "Bar Sponsor", "confidence": 0.66}]}'

    async def _fake_chat_completion(messages, *, model, json_mode=True, max_tokens=1024):
        return canned, {"prompt_tokens": 20, "completion_tokens": 10}

    with patch.object(crawl_llm, "_chat_completion", _fake_chat_completion):
        orgs, _ = await crawl_llm.extract_orgs_from_image(
            image_bytes=b"\x89PNG", content_type="image/png", role="partner_grid"
        )
    names = {o["name"]: o["confidence"] for o in orgs}
    assert names["Foo Partner"] == 0.92
    assert names["Bar Sponsor"] == 0.66


@pytest.mark.asyncio
async def test_vision_llm_rejects_non_image_content_type():
    with pytest.raises(ValueError, match="non-image content_type"):
        await crawl_llm.extract_orgs_from_image(
            image_bytes=b"<html>",
            content_type="text/html",
            role="partner_grid",
        )


# ---------------------------------------------------------------------------
# End-to-end loop: vision_fn wired, budget enforced
# ---------------------------------------------------------------------------

@_needs_crawl_encoding
def test_vision_budget_stops_further_calls():
    """With max_vision_calls=1, only the first flagged image should trigger
    vision_fn even if the page flags 3."""
    start = "https://example.org/"

    async def _fetch(url):
        class P:
            raw_html = "<html><body>x</body></html>"
            content_text = ""

        return P()

    fake_analysis = PageAnalysis(
        entities=[PEModel(name="Example Org", type="Organization", confidence=0.95)],
        worth_ocr_images=[
            ImageRegion(image_url=f"https://cdn.example.org/p{i}.png", role="partner_grid", context="Our Partners")
            for i in range(3)
        ],
        judgment="sufficient",
    )

    async def _fake_analyze_page(**_):
        return fake_analysis, {"prompt_tokens": 10, "completion_tokens": 10}

    vision_calls = {"n": 0}

    async def _fake_vision(image_url, role, context):
        vision_calls["n"] += 1
        return (
            [{"name": f"Partner {vision_calls['n']}", "confidence": 0.9}],
            {"prompt_tokens": 10, "completion_tokens": 5},
            image_url,
        )

    async def _go():
        with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
            return await agentic_crawl(
                start_url=start,
                budget=CrawlBudget(max_pages=2, max_vision_calls=1),
                fetch_fn=_fetch,
                vision_fn=_fake_vision,
            )

    proposal = asyncio.run(_go())
    assert vision_calls["n"] == 1, f"expected budget to cap vision at 1, got {vision_calls['n']}"
    # 1 root + 1 vision-extracted partner
    assert len(proposal.entities) == 2
    partner = [e for e in proposal.entities if e["name"].startswith("Partner")]
    assert len(partner) == 1
    assert partner[0]["source_image"].startswith("https://cdn.example.org/")
    assert partner[0]["metadata"]["vision_role"] == "partner_grid"
    assert proposal.stats["vision_calls"] == 1


@_needs_crawl_encoding
def test_vision_orgs_defer_until_root_entity_exists():
    start = "https://example.org/partners"
    about = "https://example.org/about"

    async def _fetch(url):
        class P:
            raw_html = "<html><body>x</body></html>"
            content_text = ""

        return P()

    analyses = {
        start: PageAnalysis(
            entities=[],
            worth_ocr_images=[
                ImageRegion(
                    image_url="https://cdn.example.org/p.png",
                    role="partner_grid",
                    context="Our Partners",
                )
            ],
            next_links=[NextLink(url=about, priority=0.9, reason="about")],
            judgment="continue",
        ),
        about: PageAnalysis(
            entities=[PEModel(name="Example Org", type="Organization", confidence=0.95)],
            judgment="sufficient",
        ),
    }

    async def _fake_analyze_page(*, page, **_kwargs):
        return analyses[page.url], {"prompt_tokens": 10, "completion_tokens": 10}

    async def _fake_vision(image_url, role, context):
        return (
            [{"name": "Partner One", "confidence": 0.9}],
            {"prompt_tokens": 10, "completion_tokens": 5},
            image_url,
        )

    async def _go():
        with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
            return await agentic_crawl(
                start_url=start,
                budget=CrawlBudget(max_pages=2, max_vision_calls=5),
                fetch_fn=_fetch,
                vision_fn=_fake_vision,
            )

    proposal = asyncio.run(_go())
    names = {e["name"]: e for e in proposal.entities}
    assert "Example Org" in names
    assert "Partner One" in names
    assert names["Partner One"]["source_image"] == "https://cdn.example.org/p.png"
    rels = {
        (
            proposal.entities[r["subject_index"]]["name"],
            r["predicate"],
            proposal.entities[r["object_index"]]["name"],
        )
        for r in proposal.relationships
    }
    assert ("Example Org", "collaborates_with", "Partner One") in rels


@_needs_crawl_encoding
def test_vision_fn_none_disables_step_cleanly():
    """Passing vision_fn=None (e.g., Phase 1 path) skips vision entirely."""
    start = "https://example.org/"

    async def _fetch(url):
        class P:
            raw_html = "<html><body>x</body></html>"
            content_text = ""

        return P()

    fake_analysis = PageAnalysis(
        entities=[PEModel(name="Example Org", type="Organization", confidence=0.95)],
        worth_ocr_images=[
            ImageRegion(image_url="https://cdn.example.org/p.png", role="partner_grid")
        ],
        judgment="sufficient",
    )

    async def _fake_analyze_page(**_):
        return fake_analysis, {"prompt_tokens": 10, "completion_tokens": 10}

    async def _go():
        with patch.object(crawl_llm, "analyze_page", _fake_analyze_page):
            return await agentic_crawl(
                start_url=start,
                budget=CrawlBudget(max_pages=2, max_vision_calls=5),
                fetch_fn=_fetch,
                vision_fn=None,
            )

    proposal = asyncio.run(_go())
    assert proposal.stats["vision_calls"] == 0
    assert len(proposal.entities) == 1
