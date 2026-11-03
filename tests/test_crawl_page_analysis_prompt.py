from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api import crawl_llm, ontology_registry
from tests import koi_test_targets as _targets


# Live crawl page analysis calls the OpenAI API: KOI_TEST_LLM=1 and OPENAI_API_KEY.
pytestmark = [pytest.mark.live, _targets.requires_live_llm("OPENAI_API_KEY")]


def _fixture_path(name: str) -> Path:
    return ROOT / "tests" / "fixtures" / name


async def _analyze_fixture(name: str, base_url: str):
    html = _fixture_path(name).read_text(encoding="utf-8")
    page = crawl_llm.compact_page(html, base_url)
    return await crawl_llm.analyze_page(
        page=page,
        goal="Map the organization and identify useful internal pages to crawl next.",
        world_model_summary="",
        allowed_types=sorted(ontology_registry.ALLOWED_ENTITY_TYPES),
        allowed_predicates=sorted(ontology_registry.ALLOWED_PREDICATES),
    )


def test_ac1_peninsula_fixture_surfaces_about_or_partners_links():
    # A saved copy of a third-party site's home page; a checkout may not carry it.
    if not _fixture_path("peninsula_homepage.html").exists():
        pytest.skip("peninsula_homepage.html fixture not in this checkout")
    analysis, _usage = asyncio.run(
        _analyze_fixture("peninsula_homepage.html", "https://peninsulastreams.ca/")
    )
    urls = [link.url for link in analysis.next_links]
    assert any("/about" in url or "/partners" in url for url in urls), urls


def test_ac2_generic_nonprofit_fixture_returns_visible_subpage_nav_links():
    analysis, _usage = asyncio.run(
        _analyze_fixture("generic_nonprofit_homepage.html", "https://harborwatershed.org/")
    )
    urls = {link.url for link in analysis.next_links}
    expected = {
        "https://harborwatershed.org/about/",
        "https://harborwatershed.org/partners/",
        "https://harborwatershed.org/contact/",
    }
    assert expected.issubset(urls), urls


def test_ac3_content_page_does_not_invent_missing_subpages():
    analysis, _usage = asyncio.run(
        _analyze_fixture("content_page_no_subpages.html", "https://example.org/articles/wetland-update")
    )
    urls = {link.url for link in analysis.next_links}
    visible_links = {"https://example.org/articles/spring-field-notes"}

    assert urls.issubset(visible_links), urls
    assert not any("/about" in url or "/partners" in url for url in urls), urls
