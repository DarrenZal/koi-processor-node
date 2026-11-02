"""Unit tests for api.mediawiki_parser — no DB dependencies."""

import os
import sys
import tempfile
import textwrap

import pytest

# Ensure the project root is on sys.path so `api.mediawiki_parser` resolves.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.mediawiki_parser import (
    WikiPageParse,
    normalize_title,
    normalize_section_id,
    derive_aliases,
    classify_page,
    compute_ingest_confidence,
    compute_promotion_priority,
    build_structural_edges,
    build_editorial_edges,
    parse_page,
    parse_dump,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WIKI_DOMAIN = "test.salishsea.wiki"


# ---------------------------------------------------------------------------
# Fixture wikitext strings
# ---------------------------------------------------------------------------

REDIRECT_PAGE = "#REDIRECT [[Target Page]]"

DISAMBIGUATION_PAGE = textwrap.dedent("""\
    '''Herring''' may refer to:

    * [[Pacific Herring]]
    * [[Atlantic Herring]]
    * [[Herring (fish)]]
""")

PARENTHETICAL_TITLE = "Herring (fish)"

DUPLICATE_SECTIONS_PAGE = textwrap.dedent("""\
    == History ==
    First history section content goes here.

    == Geography ==
    Some geography content here.

    == History ==
    Second history section with more content.
""")

FULL_TEMPLATE_PAGE = textwrap.dedent("""\
    {{Topic
    |Places=Salish Sea, Puget Sound
    |Jurisdictions=British Columbia
    |AnthroTopics=Fisheries Management
    |EcoTopics=Marine Ecology
    |RelatedEfforts=Herring Recovery Project
    |RelatedTopics=Salmon, Kelp Forests
    |Products=Field Guide
    |Workgroups=Marine Working Group
    }}

    The '''Herring Monitoring''' program tracks herring populations across the Salish Sea bioregion.
    It relies on [[citizen science]] and [[acoustic monitoring]] to gather data.
    The results inform [[DFO]] management decisions and are shared with [[First Nations]] communities.

    == Methods ==
    Various methods are used including [[seine netting]] and [[sonar surveys]].
    Data is collected seasonally from March to June.

    == Results ==
    Population estimates suggest recovery in some areas. See also [[Pacific Herring]].
""")

MALFORMED_PAGE = textwrap.dedent("""\
    This page has <b>unclosed bold tag
    and a {{broken|template with
    missing closing braces
    plus a [[broken link
    and another [[Good Link]] that works.
    Some more text to pad out the page with enough words for testing purposes.
    This line is just filler to ensure we have content to parse and extract.
""")


# ===================================================================
# Normalization tests
# ===================================================================

class TestNormalization:

    def test_normalize_title_basic(self):
        assert normalize_title("  Herring_Monitoring  ") == "herring monitoring"

    def test_normalize_title_preserves_parentheticals(self):
        assert normalize_title("Herring (fish)") == "herring (fish)"

    def test_normalize_title_strips_namespace(self):
        assert normalize_title("Category:Foo") == "foo"

    def test_normalize_section_id(self):
        assert normalize_section_id("Landscape Change") == "landscape-change"

    def test_normalize_section_id_special_chars(self):
        result = normalize_section_id("Methods & '''Results'''!")
        # Should strip markup and non-ASCII-safe chars, lowercase, hyphenate
        assert "'" not in result
        assert "&" not in result
        assert "!" not in result
        assert result == "methods-results"


# ===================================================================
# Parser tests
# ===================================================================

class TestParser:

    def test_parse_redirect(self):
        result = parse_page("Some Redirect", REDIRECT_PAGE, 1, 1, WIKI_DOMAIN)
        assert result.is_redirect is True
        assert result.redirect_target == "Target Page"
        assert result.page_class == "alias_only"

    def test_parse_disambiguation(self):
        result = parse_page("Herring", DISAMBIGUATION_PAGE, 2, 1, WIKI_DOMAIN)
        # Disambiguation: no template, short-ish text -> source_only
        assert result.page_class in ("alias_only", "source_only")
        # Should have multiple wikilinks
        assert len(result.wikilinks) >= 3

    def test_parse_parenthetical_title(self):
        result = parse_page(
            PARENTHETICAL_TITLE,
            "This is a short stub about herring fish species.",
            3, 1, WIKI_DOMAIN,
        )
        assert "herring" in result.aliases

    def test_parse_duplicate_sections(self):
        result = parse_page("Test Page", DUPLICATE_SECTIONS_PAGE, 4, 1, WIKI_DOMAIN)
        section_ids = [s.id for s in result.sections]
        assert "history" in section_ids
        assert "history-2" in section_ids

    def test_parse_full_template(self):
        result = parse_page("Herring Monitoring", FULL_TEMPLATE_PAGE, 5, 1, WIKI_DOMAIN)
        assert result.template_type == "Topic"
        assert result.bkc_entity_type == "Concept"
        assert len(result.template_fields) > 0
        assert "Places" in result.template_fields
        assert len(result.structural_edges) > 0
        # Check tier diversity
        confidences = {e.confidence for e in result.structural_edges}
        assert 0.95 in confidences  # Tier 1 (Places)

    def test_parse_malformed(self):
        result = parse_page("Broken Page", MALFORMED_PAGE, 6, 1, WIKI_DOMAIN)
        # Should complete without raising
        assert result.title == "Broken Page"
        # Good Link should still be extracted
        link_targets = [l.target for l in result.wikilinks]
        assert "Good Link" in link_targets


# ===================================================================
# Classification tests
# ===================================================================

class TestClassification:

    def test_classify_entity_bearing(self):
        for tpl, bkc_type in [
            ("Topic", "Concept"),
            ("Effort", "Project"),
            ("Workgroup", "Organization"),
            ("Place", "Location"),
        ]:
            cls = classify_page(tpl, False, bkc_type, 200)
            assert cls == "entity_bearing", f"Expected entity_bearing for {tpl}"

    def test_classify_source_only(self):
        cls = classify_page("Product", False, None, 200)
        assert cls == "source_only"

    def test_classify_alias_only(self):
        cls = classify_page(None, True, None, 0)
        assert cls == "alias_only"


# ===================================================================
# Scoring tests
# ===================================================================

class TestScoring:

    def test_ingest_confidence_high(self):
        # Full template with fields, lots of text, links, no warnings
        score = compute_ingest_confidence(
            template_type="Topic",
            field_count=5,
            word_count=400,
            wikilink_count=10,
            warning_count=0,
        )
        assert score >= 0.6

    def test_ingest_confidence_low(self):
        # No template, short text, no links, warnings
        score = compute_ingest_confidence(
            template_type=None,
            field_count=0,
            word_count=20,
            wikilink_count=0,
            warning_count=2,
        )
        assert score < 0.4

    def test_promotion_priority_ordering(self):
        rich = compute_promotion_priority(
            template_type="Topic",
            word_count=500,
            wikilink_count=15,
            structural_edge_count=8,
        )
        sparse = compute_promotion_priority(
            template_type=None,
            word_count=30,
            wikilink_count=1,
            structural_edge_count=0,
        )
        assert rich > sparse


# ===================================================================
# Edge tests
# ===================================================================

class TestEdges:

    def test_structural_edges_tier1(self):
        fields = {"Places": ["Salish Sea"]}
        edges = build_structural_edges(fields)
        assert len(edges) == 1
        assert edges[0].confidence == 0.95
        assert edges[0].predicate == "located_in"

    def test_structural_edges_tier2(self):
        fields = {"Jurisdictions": ["British Columbia"]}
        edges = build_structural_edges(fields)
        assert len(edges) == 1
        assert edges[0].confidence == 0.85

    def test_structural_edges_tier3(self):
        fields = {"RelatedTopics": ["Salmon"]}
        edges = build_structural_edges(fields)
        assert len(edges) == 1
        assert edges[0].confidence == 0.7
        assert edges[0].predicate == "related_to"

    def test_editorial_edges(self):
        result = parse_page("Herring Monitoring", FULL_TEMPLATE_PAGE, 5, 1, WIKI_DOMAIN)
        # Editorial edges should exist for body wikilinks not in structural edges
        assert len(result.editorial_edges) > 0
        for edge in result.editorial_edges:
            assert edge.confidence == 0.6
            # Default predicate on EditorialEdge is implicit (related_to) — just check confidence

    def test_editorial_edges_dedup(self):
        result = parse_page("Herring Monitoring", FULL_TEMPLATE_PAGE, 5, 1, WIKI_DOMAIN)
        structural_targets = {e.target_title for e in result.structural_edges}
        editorial_targets = {e.target_title for e in result.editorial_edges}
        # No editorial edge should duplicate a structural target
        assert structural_targets.isdisjoint(editorial_targets)


# ===================================================================
# parse_dump test
# ===================================================================

class TestParseDump:

    def test_parse_dump_minimal(self):
        xml_content = textwrap.dedent("""\
            <mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" version="0.10">
              <page>
                <title>Test Article</title>
                <ns>0</ns>
                <id>42</id>
                <revision>
                  <id>100</id>
                  <text>This is a simple test article with a [[wikilink]].</text>
                </revision>
              </page>
              <page>
                <title>Talk:Ignore Me</title>
                <ns>1</ns>
                <id>43</id>
                <revision>
                  <id>101</id>
                  <text>Talk page content</text>
                </revision>
              </page>
            </mediawiki>
        """)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".xml", delete=False,
        ) as f:
            f.write(xml_content)
            f.flush()
            tmp_path = f.name

        try:
            results = list(parse_dump(tmp_path, WIKI_DOMAIN))
            # Only NS-0 pages should be yielded
            assert len(results) == 1
            page = results[0]
            assert isinstance(page, WikiPageParse)
            assert page.title == "Test Article"
            assert page.page_id == 42
            assert page.revision_id == 100
            assert len(page.wikilinks) >= 1
        finally:
            os.unlink(tmp_path)
