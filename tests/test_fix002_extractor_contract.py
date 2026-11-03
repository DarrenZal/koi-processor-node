"""
FIX-002: Extractor Contract Tests

Tests for:
- normalize_type() function
- build_extraction_prompt() function
- _parse_extraction() contract (called with stub dicts, no LLM calls)

No LLM API calls are made in these tests.
"""

import importlib.util
import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from core.entity_types import (
    normalize_type,
    is_llm_allowed_type,
    ALL_CANONICAL_TYPES,
    LLM_ALLOWED_TYPES,
    TYPE_ALIASES_TO_CANONICAL,
)
from extraction.prompt_builder import build_extraction_prompt, get_system_message


# =============================================================================
# normalize_type() Tests
# =============================================================================

class TestNormalizeType:
    """Tests for the normalize_type() function."""

    def test_humanactor_to_person(self):
        """HumanActor should normalize to PERSON."""
        assert normalize_type("HumanActor") == "PERSON"

    def test_human_actor_underscore_to_person(self):
        """HUMAN_ACTOR should normalize to PERSON."""
        assert normalize_type("HUMAN_ACTOR") == "PERSON"

    def test_humanactor_lowercase_to_person(self):
        """humanactor should normalize to PERSON."""
        assert normalize_type("humanactor") == "PERSON"

    def test_gpe_to_location(self):
        """GPE (Geo-Political Entity) should normalize to LOCATION."""
        assert normalize_type("GPE") == "LOCATION"

    def test_place_to_location(self):
        """PLACE should normalize to LOCATION."""
        assert normalize_type("PLACE") == "LOCATION"

    def test_city_to_location(self):
        """CITY should normalize to LOCATION."""
        assert normalize_type("city") == "LOCATION"

    def test_meeting_to_event(self):
        """MEETING should normalize to EVENT."""
        assert normalize_type("MEETING") == "EVENT"

    def test_conference_to_event(self):
        """CONFERENCE should normalize to EVENT."""
        assert normalize_type("conference") == "EVENT"

    def test_org_to_organization(self):
        """ORG should normalize to ORGANIZATION."""
        assert normalize_type("ORG") == "ORGANIZATION"

    def test_company_to_organization(self):
        """COMPANY should normalize to ORGANIZATION."""
        assert normalize_type("COMPANY") == "ORGANIZATION"

    def test_individual_to_person(self):
        """INDIVIDUAL should normalize to PERSON."""
        assert normalize_type("INDIVIDUAL") == "PERSON"

    def test_repo_to_project(self):
        """REPO should normalize to PROJECT."""
        assert normalize_type("REPO") == "PROJECT"

    def test_idea_to_concept(self):
        """IDEA should normalize to CONCEPT."""
        assert normalize_type("IDEA") == "CONCEPT"

    def test_tech_to_technology(self):
        """TECH should normalize to TECHNOLOGY."""
        assert normalize_type("TECH") == "TECHNOLOGY"

    def test_tool_to_technology(self):
        """TOOL should normalize to TECHNOLOGY."""
        assert normalize_type("TOOL") == "TECHNOLOGY"

    def test_method_to_function(self):
        """METHOD should normalize to FUNCTION."""
        assert normalize_type("METHOD") == "FUNCTION"

    def test_canonical_types_pass_through(self):
        """Canonical types should pass through unchanged (uppercase)."""
        for t in ALL_CANONICAL_TYPES:
            assert normalize_type(t) == t
            assert normalize_type(t.lower()) == t

    def test_none_returns_entity(self):
        """None should return ENTITY."""
        assert normalize_type(None) == "ENTITY"

    def test_empty_string_returns_entity(self):
        """Empty string should return ENTITY."""
        assert normalize_type("") == "ENTITY"
        assert normalize_type("   ") == "ENTITY"

    def test_unknown_type_returns_entity(self):
        """Unknown/garbage types should return ENTITY."""
        assert normalize_type("GARBAGE_TYPE") == "ENTITY"
        assert normalize_type("foobar") == "ENTITY"
        assert normalize_type("12345") == "ENTITY"

    def test_uri_stripping_full_url(self):
        """Full URIs should be stripped."""
        assert normalize_type("https://regen.network/ontology#Person") == "PERSON"
        assert normalize_type("http://schema.org/Organization") == "ORGANIZATION"

    def test_prefix_stripping(self):
        """Namespace prefixes should be stripped."""
        assert normalize_type("regen:Person") == "PERSON"
        assert normalize_type("koi#PERSON") == "PERSON"
        assert normalize_type("schema:Organization") == "ORGANIZATION"


# =============================================================================
# is_llm_allowed_type() Tests
# =============================================================================

class TestIsLLMAllowedType:
    """Tests for the is_llm_allowed_type() function."""

    def test_person_is_allowed(self):
        """PERSON is LLM-allowed."""
        assert is_llm_allowed_type("PERSON") is True

    def test_organization_is_allowed(self):
        """ORGANIZATION is LLM-allowed."""
        assert is_llm_allowed_type("ORGANIZATION") is True

    def test_technology_is_allowed(self):
        """TECHNOLOGY is LLM-allowed."""
        assert is_llm_allowed_type("TECHNOLOGY") is True

    def test_location_is_allowed(self):
        """LOCATION is LLM-allowed."""
        assert is_llm_allowed_type("LOCATION") is True

    def test_event_is_allowed(self):
        """EVENT is LLM-allowed."""
        assert is_llm_allowed_type("EVENT") is True

    def test_function_not_allowed(self):
        """FUNCTION is NOT LLM-allowed (code graph only)."""
        assert is_llm_allowed_type("FUNCTION") is False

    def test_entity_not_allowed(self):
        """ENTITY is NOT LLM-allowed (fallback default)."""
        assert is_llm_allowed_type("ENTITY") is False

    def test_all_llm_allowed_types(self):
        """All LLM_ALLOWED_TYPES should return True."""
        for t in LLM_ALLOWED_TYPES:
            assert is_llm_allowed_type(t) is True

    def test_case_insensitive(self):
        """Should handle case variations."""
        assert is_llm_allowed_type("person") is True
        assert is_llm_allowed_type("Person") is True
        assert is_llm_allowed_type("PERSON") is True


# =============================================================================
# build_extraction_prompt() Tests
# =============================================================================

class TestBuildExtractionPrompt:
    """Tests for the build_extraction_prompt() function."""

    def test_prompt_includes_location(self):
        """Prompt should include LOCATION as an allowed type."""
        prompt = build_extraction_prompt(
            content="Test content",
            source_type="discourse"
        )
        assert "LOCATION" in prompt

    def test_prompt_includes_event(self):
        """Prompt should include EVENT as an allowed type."""
        prompt = build_extraction_prompt(
            content="Test content",
            source_type="discourse"
        )
        assert "EVENT" in prompt

    def test_prompt_forbids_generic_events(self):
        """Prompt should forbid generic event words."""
        prompt = build_extraction_prompt(
            content="Test content",
            source_type="discourse"
        )
        # Should contain guidance about not extracting generic events
        assert "meeting" in prompt.lower() and "NOT" in prompt

    def test_prompt_ai_as_technology(self):
        """Prompt should specify AI systems are TECHNOLOGY, not PERSON."""
        prompt = build_extraction_prompt(
            content="Test content",
            source_type="discourse"
        )
        assert "ChatGPT" in prompt or "AI" in prompt
        assert "TECHNOLOGY" in prompt

    def test_prompt_truncates_content(self):
        """Prompt should truncate long content."""
        long_content = "x" * 5000
        prompt = build_extraction_prompt(
            content=long_content,
            source_type="discourse",
            max_content_length=100
        )
        # The content portion should only be 100 chars, not full 5000
        # The template adds significant overhead (~6000 chars), but content itself is truncated
        assert "x" * 100 in prompt
        assert "x" * 5000 not in prompt  # Full content should not be present

    def test_prompt_uses_source_type(self):
        """Prompt should include the source type."""
        prompt = build_extraction_prompt(
            content="Test content",
            source_type="github"
        )
        assert "github" in prompt.lower()

    def test_prompt_includes_confidence(self):
        """Prompt should mention confidence scores."""
        prompt = build_extraction_prompt(
            content="Test content",
            source_type="discourse"
        )
        assert "confidence" in prompt.lower()


# =============================================================================
# get_system_message() Tests
# =============================================================================

class TestGetSystemMessage:
    """Tests for the get_system_message() function."""

    def test_system_message_exists(self):
        """System message should be non-empty."""
        msg = get_system_message()
        assert msg is not None
        assert len(msg) > 0

    def test_system_message_mentions_json(self):
        """System message should mention JSON output."""
        msg = get_system_message()
        assert "JSON" in msg or "json" in msg.lower()


# =============================================================================
# _parse_extraction() Contract Tests (using stub data)
# =============================================================================

@pytest.mark.skipif(importlib.util.find_spec("rdflib") is None,
                    reason="rdflib is not installed (an optional dependency of these modules)")
class TestParseExtractionContract:
    """Tests for extractor _parse_extraction() contract using stub data.

    These tests verify that _parse_extraction() correctly:
    - Normalizes entity types to canonical uppercase
    - Preserves confidence scores
    - Drops entities with non-LLM-allowed types

    No actual LLM API calls are made.
    """

    def setup_method(self):
        """Set up test extractors."""
        # Import extractors
        from extraction.openai_extractor import OpenAIExtractor
        from extraction.llm_extractor import OntologyLLMExtractor

        # Create extractor instances (no API key needed for parsing tests)
        self.openai_extractor = OpenAIExtractor(api_key="test-key")
        self.llm_extractor = OntologyLLMExtractor()

    def test_openai_parse_normalizes_humanactor(self):
        """OpenAI extractor should normalize HumanActor to PERSON."""
        stub_extraction = {
            "entities": [
                {"type": "HumanActor", "name": "Gregory Landua", "confidence": 0.95}
            ]
        }

        result = self.openai_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0]["type"] == "PERSON"
        assert entities[0]["name"] == "Gregory Landua"

    def test_openai_parse_preserves_confidence(self):
        """OpenAI extractor should preserve confidence scores."""
        stub_extraction = {
            "entities": [
                {"type": "PERSON", "name": "Test Person", "confidence": 0.87}
            ]
        }

        result = self.openai_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0].get("confidence") == 0.87

    def test_openai_parse_drops_function_type(self):
        """OpenAI extractor should drop entities with FUNCTION type."""
        stub_extraction = {
            "entities": [
                {"type": "FUNCTION", "name": "process_data", "confidence": 0.90},
                {"type": "PERSON", "name": "Valid Person", "confidence": 0.85}
            ]
        }

        result = self.openai_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0]["name"] == "Valid Person"

    def test_openai_parse_drops_entity_fallback_type(self):
        """OpenAI extractor should drop entities that normalize to ENTITY."""
        stub_extraction = {
            "entities": [
                {"type": "GARBAGE_TYPE", "name": "Bad Entity", "confidence": 0.90},
                {"type": "PERSON", "name": "Valid Person", "confidence": 0.85}
            ]
        }

        result = self.openai_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        # GARBAGE_TYPE normalizes to ENTITY, which is not LLM-allowed
        assert len(entities) == 1
        assert entities[0]["name"] == "Valid Person"

    def test_llm_parse_normalizes_humanactor(self):
        """LLM extractor should normalize HumanActor to PERSON."""
        stub_extraction = {
            "entities": [
                {"type": "HumanActor", "name": "Mara Bax", "confidence": 0.92}
            ]
        }

        result = self.llm_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0]["type"] == "PERSON"

    def test_llm_parse_preserves_confidence(self):
        """LLM extractor should preserve confidence scores (was a bug before FIX-002)."""
        stub_extraction = {
            "entities": [
                {"type": "ORGANIZATION", "name": "Regen Network", "confidence": 0.93}
            ]
        }

        result = self.llm_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0].get("confidence") == 0.93

    def test_llm_parse_drops_function_type(self):
        """LLM extractor should drop entities with FUNCTION type."""
        stub_extraction = {
            "entities": [
                {"type": "METHOD", "name": "calculate_score", "confidence": 0.88},
                {"type": "CONCEPT", "name": "Carbon Credits", "confidence": 0.90}
            ]
        }

        result = self.llm_extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        # METHOD normalizes to FUNCTION, which is not LLM-allowed
        assert len(entities) == 1
        assert entities[0]["name"] == "Carbon Credits"

    def test_parse_normalizes_gpe_to_location(self):
        """Both extractors should normalize GPE to LOCATION."""
        stub_extraction = {
            "entities": [
                {"type": "GPE", "name": "Boulder, Colorado", "confidence": 0.95}
            ]
        }

        result1 = self.openai_extractor._parse_extraction(stub_extraction, "discourse")
        result2 = self.llm_extractor._parse_extraction(stub_extraction, "discourse")

        assert result1["extracted_entities"][0]["type"] == "LOCATION"
        assert result2["extracted_entities"][0]["type"] == "LOCATION"

    def test_parse_normalizes_meeting_to_event(self):
        """Both extractors should normalize MEETING alias to EVENT."""
        stub_extraction = {
            "entities": [
                {"type": "CONFERENCE", "name": "COP28", "confidence": 0.95}
            ]
        }

        result1 = self.openai_extractor._parse_extraction(stub_extraction, "discourse")
        result2 = self.llm_extractor._parse_extraction(stub_extraction, "discourse")

        assert result1["extracted_entities"][0]["type"] == "EVENT"
        assert result2["extracted_entities"][0]["type"] == "EVENT"

    def test_relationship_types_normalized(self):
        """Relationship subject_type and object_type should be normalized."""
        stub_extraction = {
            "entities": [],
            "relationships": [
                {
                    "subject": "Gregory Landua",
                    "predicate": "works_at",
                    "object": "Regen Network",
                    "subject_type": "HumanActor",
                    "object_type": "ORG",
                    "confidence": 0.90
                }
            ]
        }

        result = self.openai_extractor._parse_extraction(stub_extraction, "discourse")

        rels = result.get("extracted_relationships", [])
        assert len(rels) == 1
        assert rels[0]["subject_type"] == "PERSON"
        assert rels[0]["object_type"] == "ORGANIZATION"
        assert rels[0]["confidence"] == 0.90


# =============================================================================
# Type Alias Coverage Tests
# =============================================================================

class TestTypeAliasCoverage:
    """Verify that all important type aliases are covered."""

    def test_person_aliases(self):
        """All PERSON aliases should normalize correctly."""
        person_aliases = ["HumanActor", "HUMAN_ACTOR", "HUMAN", "INDIVIDUAL", "ACTOR", "AUTHOR"]
        for alias in person_aliases:
            assert normalize_type(alias) == "PERSON", f"{alias} should map to PERSON"

    def test_organization_aliases(self):
        """All ORGANIZATION aliases should normalize correctly."""
        org_aliases = ["ORG", "COMPANY", "FOUNDATION", "INSTITUTION", "DAO"]
        for alias in org_aliases:
            assert normalize_type(alias) == "ORGANIZATION", f"{alias} should map to ORGANIZATION"

    def test_location_aliases(self):
        """All LOCATION aliases should normalize correctly."""
        loc_aliases = ["PLACE", "CITY", "COUNTRY", "REGION", "GPE", "GEO"]
        for alias in loc_aliases:
            assert normalize_type(alias) == "LOCATION", f"{alias} should map to LOCATION"

    def test_event_aliases(self):
        """All EVENT aliases should normalize correctly."""
        event_aliases = ["MEETING", "CONFERENCE", "WORKSHOP", "SUMMIT", "WEBINAR"]
        for alias in event_aliases:
            assert normalize_type(alias) == "EVENT", f"{alias} should map to EVENT"

    def test_technology_aliases(self):
        """All TECHNOLOGY aliases should normalize correctly."""
        tech_aliases = ["TECH", "TOOL", "SYSTEM", "SOFTWARE", "PLATFORM", "AI"]
        for alias in tech_aliases:
            assert normalize_type(alias) == "TECHNOLOGY", f"{alias} should map to TECHNOLOGY"


# =============================================================================
# GeminiExtractor Contract Tests (no API calls)
# =============================================================================
#
# google-generativeai is listed in requirements.txt but is an optional
# dependency for the Gemini extractor path, which no known deployment
# uses (they use OpenAI for extraction). The gemini_extractor module unconditionally imports
# google.generativeai at module load, so these tests get skipped
# cleanly when the package is absent from the active venv.

_gemini_available = True
try:  # pragma: no cover — import-time availability check
    from extraction.gemini_extractor import GeminiExtractor  # noqa: F401
except (ImportError, ModuleNotFoundError):
    _gemini_available = False

_skip_if_no_gemini = pytest.mark.skipif(
    not _gemini_available,
    reason="google-generativeai not installed in this venv; Gemini extractor path not in use on any production node",
)


@_skip_if_no_gemini
class TestGeminiExtractorContract:
    """Tests for GeminiExtractor._parse_extraction() contract.

    No actual Gemini API calls are made.
    """

    def test_gemini_parse_extraction_output_shape(self):
        """GeminiExtractor._parse_extraction produces correct output shape."""
        import json
        from extraction.gemini_extractor import GeminiExtractor

        # Mock raw response (no API call)
        raw_response = json.dumps({
            "entities": [
                {"name": "Regen Network", "type": "Organization", "confidence": 0.95},
                {"name": "Gregory Landua", "type": "person", "confidence": 0.9},
                {"name": "SomeFunction", "type": "FUNCTION", "confidence": 0.8},  # Not LLM-allowed, should be dropped
            ],
            "relationships": [
                {"subject": "Gregory Landua", "predicate": "founded", "object": "Regen Network", "confidence": 0.85}
            ]
        })

        # Create extractor without __init__ (skip API check)
        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"
        extractor.logger = __import__('logging').getLogger(__name__)

        # Parse raw JSON into dict
        extraction = json.loads(raw_response)
        result = extractor._parse_extraction(extraction, "web")

        # Verify wrapper keys exist
        assert result["semantic_extraction"] == True
        assert result["source_type"] == "web"
        assert "llm_extraction" in result
        assert result["llm_extraction"]["provider"] == "google"
        assert result["llm_extraction"]["model"] == "gemini-3-flash-preview"

        # Verify entities normalized and filtered
        entities = result["extracted_entities"]
        assert len(entities) == 2  # FUNCTION dropped
        assert all(e["type"] == e["type"].upper() for e in entities)
        assert all(e["type"] in {"PERSON", "ORGANIZATION", "PROJECT", "CONCEPT", "TECHNOLOGY", "CLAIM", "EVIDENCE", "QUESTION", "LOCATION", "EVENT"} for e in entities)

        # Verify relationships have required keys
        rels = result["extracted_relationships"]
        assert len(rels) == 1
        assert all(k in rels[0] for k in ["subject", "predicate", "object"])

    def test_gemini_parse_normalizes_humanactor(self):
        """GeminiExtractor should normalize HumanActor to PERSON."""
        from extraction.gemini_extractor import GeminiExtractor

        stub_extraction = {
            "entities": [
                {"type": "HumanActor", "name": "Gregory Landua", "confidence": 0.95}
            ]
        }

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"
        extractor.logger = __import__('logging').getLogger(__name__)

        result = extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0]["type"] == "PERSON"
        assert entities[0]["name"] == "Gregory Landua"

    def test_gemini_parse_preserves_confidence(self):
        """GeminiExtractor should preserve confidence scores."""
        from extraction.gemini_extractor import GeminiExtractor

        stub_extraction = {
            "entities": [
                {"type": "PERSON", "name": "Test Person", "confidence": 0.87}
            ]
        }

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"
        extractor.logger = __import__('logging').getLogger(__name__)

        result = extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0].get("confidence") == 0.87

    def test_gemini_parse_drops_function_type(self):
        """GeminiExtractor should drop entities with FUNCTION type."""
        from extraction.gemini_extractor import GeminiExtractor

        stub_extraction = {
            "entities": [
                {"type": "FUNCTION", "name": "process_data", "confidence": 0.90},
                {"type": "METHOD", "name": "calculate_score", "confidence": 0.88},
                {"type": "PERSON", "name": "Valid Person", "confidence": 0.85}
            ]
        }

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"
        extractor.logger = __import__('logging').getLogger(__name__)

        result = extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        # Both FUNCTION and METHOD (which maps to FUNCTION) should be dropped
        assert len(entities) == 1
        assert entities[0]["name"] == "Valid Person"

    def test_gemini_parse_normalizes_gpe_to_location(self):
        """GeminiExtractor should normalize GPE to LOCATION."""
        from extraction.gemini_extractor import GeminiExtractor

        stub_extraction = {
            "entities": [
                {"type": "GPE", "name": "Boulder, Colorado", "confidence": 0.95}
            ]
        }

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"
        extractor.logger = __import__('logging').getLogger(__name__)

        result = extractor._parse_extraction(stub_extraction, "discourse")

        entities = result.get("extracted_entities", [])
        assert len(entities) == 1
        assert entities[0]["type"] == "LOCATION"

    def test_gemini_parse_normalizes_relationship_types(self):
        """GeminiExtractor relationship subject_type and object_type should be normalized."""
        from extraction.gemini_extractor import GeminiExtractor

        stub_extraction = {
            "entities": [],
            "relationships": [
                {
                    "subject": "Gregory Landua",
                    "predicate": "works_at",
                    "object": "Regen Network",
                    "subject_type": "HumanActor",
                    "object_type": "ORG",
                    "confidence": 0.90
                }
            ]
        }

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"
        extractor.logger = __import__('logging').getLogger(__name__)

        result = extractor._parse_extraction(stub_extraction, "discourse")

        rels = result.get("extracted_relationships", [])
        assert len(rels) == 1
        assert rels[0]["subject_type"] == "PERSON"
        assert rels[0]["object_type"] == "ORGANIZATION"
        assert rels[0]["confidence"] == 0.90


@_skip_if_no_gemini
class TestGeminiExtractorJsonExtraction:
    """Tests for GeminiExtractor._extract_json() method."""

    def test_plain_json(self):
        """Should parse plain JSON."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.logger = __import__('logging').getLogger(__name__)

        result = extractor._extract_json('{"entities": []}')
        assert result == {"entities": []}

    def test_markdown_wrapped_json(self):
        """Should extract JSON from markdown code blocks."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.logger = __import__('logging').getLogger(__name__)

        wrapped = '```json\n{"entities": []}\n```'
        result = extractor._extract_json(wrapped)
        assert result == {"entities": []}

    def test_markdown_without_json_tag(self):
        """Should extract JSON from markdown code blocks without json tag."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.logger = __import__('logging').getLogger(__name__)

        wrapped = '```\n{"entities": []}\n```'
        result = extractor._extract_json(wrapped)
        assert result == {"entities": []}

    def test_json_with_text_before(self):
        """Should extract JSON when there's text before it."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.logger = __import__('logging').getLogger(__name__)

        text = 'Here is the extraction:\n{"entities": []}'
        result = extractor._extract_json(text)
        assert result == {"entities": []}

    def test_empty_text_returns_empty_dict(self):
        """Should return empty dict for empty text."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.logger = __import__('logging').getLogger(__name__)

        assert extractor._extract_json("") == {}
        assert extractor._extract_json(None) == {}


@_skip_if_no_gemini
class TestGeminiExtractorEmptyResult:
    """Tests for GeminiExtractor._build_empty_result() method."""

    def test_empty_result_has_wrapper_keys(self):
        """Empty result should have all required wrapper keys."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"

        result = extractor._build_empty_result("discourse")

        assert result["semantic_extraction"] == True
        assert result["source_type"] == "discourse"
        assert "llm_extraction" in result
        assert result["llm_extraction"]["provider"] == "google"
        assert result["extracted_entities"] == []
        assert result["extracted_relationships"] == []

    def test_empty_result_with_error(self):
        """Empty result with error should include error key."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"

        result = extractor._build_empty_result("discourse", error="API timeout")

        assert result["error"] == "API timeout"

    def test_empty_result_merges_existing_metadata(self):
        """Empty result should merge with existing metadata."""
        from extraction.gemini_extractor import GeminiExtractor

        extractor = object.__new__(GeminiExtractor)
        extractor.model = "gemini-3-flash-preview"

        existing = {"author": "test_user", "post_id": "123"}
        result = extractor._build_empty_result("discourse", existing_metadata=existing)

        assert result["author"] == "test_user"
        assert result["post_id"] == "123"
        assert result["semantic_extraction"] == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
