"""
Unit tests for EntityQualityFilter

Tests all seven filter types:
1. Stop words (pronouns, generic nouns)
2. Numeric-only entities
3. Tautological entities
4. Lowercase single-word PERSON entities
5. Generic person patterns
6. Sentence-like entities
7. Length limit violations

Run with: pytest src/knowledge_graph/improvements/tests/test_entity_quality_filter.py -v
"""

import pytest
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.knowledge_graph.improvements.entity_quality_filter import (
    EntityQualityFilter,
    FilterConfig,
    filter_entities,
    ENTITY_WHITELIST,
    COUNTRY_CODES,
    ORGANIZATIONS,
    TECH_SCIENCE,
    CURRENCY_CODES,
    PERSON_NAMES_WHITELIST,
)


class TestStopWordFilter:
    """Tests for stop word filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Pronouns should be blocked
    @pytest.mark.parametrize("pronoun", [
        "we", "they", "it", "she", "he", "i", "you", "us", "them",
        "We", "They", "IT",  # Case insensitive
    ])
    def test_blocks_pronouns(self, pronoun):
        assert self.filter.is_stop_word(pronoun) is True

    # Generic nouns should be blocked
    @pytest.mark.parametrize("generic", [
        "people", "person", "user", "member", "participant",
        "validator", "delegator", "team", "community",
        "Users", "Members",  # Case insensitive
    ])
    def test_blocks_generic_nouns(self, generic):
        assert self.filter.is_stop_word(generic) is True

    # Real entity names should pass
    @pytest.mark.parametrize("valid_name", [
        "Gregory Landua",
        "Regen Network",
        "Avery Penrose",
        "Toucan Protocol",
        "Cosmos SDK",
    ])
    def test_allows_valid_names(self, valid_name):
        assert self.filter.is_stop_word(valid_name) is False

    def test_full_filter_with_stop_word(self):
        entity = {"name": "we", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "stop_word"


class TestNumericFilter:
    """Tests for numeric-only entity filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Pure numbers should be blocked
    @pytest.mark.parametrize("numeric", [
        "2030", "35", "1956", "0", "12345",
    ])
    def test_blocks_pure_numbers(self, numeric):
        assert self.filter.is_numeric_only(numeric) is True

    # Numbers with text should pass
    @pytest.mark.parametrize("valid", [
        "Episode 120",
        "Project 2025",
        "CO2",
        "Web3",
        "2024 Report",
        "v2.0",
    ])
    def test_allows_numbers_with_text(self, valid):
        assert self.filter.is_numeric_only(valid) is False

    def test_full_filter_with_numeric(self):
        entity = {"name": "2030", "type": "DATE"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "numeric_only"


class TestTautologicalFilter:
    """Tests for tautological entity filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Name equals type should be blocked
    @pytest.mark.parametrize("name,entity_type", [
        ("organization", "ORGANIZATION"),
        ("Organization", "organization"),
        ("places", "PLACE"),
        ("PERSON", "person"),
        ("concept", "CONCEPT"),
        ("formal_organization", "FORMAL_ORGANIZATION"),
    ])
    def test_blocks_tautological(self, name, entity_type):
        assert self.filter.is_tautological(name, entity_type) is True

    # Different name and type should pass
    @pytest.mark.parametrize("name,entity_type", [
        ("Regen Network", "ORGANIZATION"),
        ("Boulder", "PLACE"),
        ("Gregory Landua", "PERSON"),
        ("Sustainability", "CONCEPT"),
    ])
    def test_allows_different_name_type(self, name, entity_type):
        assert self.filter.is_tautological(name, entity_type) is False

    def test_full_filter_with_tautological(self):
        # Use "place" which is not in stop words but is tautological
        entity = {"name": "place", "type": "PLACE"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "tautological"


class TestLowercasePersonFilter:
    """Tests for lowercase single-word PERSON filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Lowercase single-word PERSON should be blocked
    @pytest.mark.parametrize("name", [
        "mom", "dad", "friend", "guy", "farmer",
    ])
    def test_blocks_lowercase_person(self, name):
        assert self.filter.is_lowercase_person(name, "PERSON") is True

    # Capitalized names should pass
    @pytest.mark.parametrize("name", [
        "Aaron", "Gregory", "Alice", "Bob",
    ])
    def test_allows_capitalized_single_word(self, name):
        assert self.filter.is_lowercase_person(name, "PERSON") is False

    # Multi-word names should pass (even if lowercase)
    @pytest.mark.parametrize("name", [
        "gregory landua",  # Multi-word, even lowercase
        "John Smith",
        "Dr. Jane",
    ])
    def test_allows_multi_word(self, name):
        assert self.filter.is_lowercase_person(name, "PERSON") is False

    # Non-PERSON types should pass
    def test_allows_non_person_type(self):
        assert self.filter.is_lowercase_person("concept", "CONCEPT") is False
        assert self.filter.is_lowercase_person("project", "PROJECT") is False

    def test_full_filter_with_lowercase_person(self):
        # Use "bob" which is lowercase but not in stop words
        entity = {"name": "bob", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "lowercase_person"


class TestGenericPatternFilter:
    """Tests for generic person pattern filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Generic patterns should be blocked
    @pytest.mark.parametrize("pattern", [
        "the character",
        "our friends",
        "the speaker",
        "some people",
        "many participants",
        "those who believe",
        "everyone involved",
        "a user",
        "the members",
    ])
    def test_blocks_generic_patterns(self, pattern):
        assert self.filter.matches_generic_pattern(pattern) is True

    # Proper names should pass
    # Note: Names starting with "The/A/An" + CAPITALIZED word now pass
    # because the pattern distinguishes proper names from generic descriptions
    @pytest.mark.parametrize("valid_name", [
        "Dr. Jane Goodall",
        "Gregory Landua",
        "Avery William Penrose",
        "The Y on Earth Podcast",  # "The" + capitalized = proper name
        "The Ministry for the Future",  # "The" + capitalized = proper name
        "The World Bank",  # "The" + capitalized = proper name
        "A Novel Approach",  # "A" + capitalized = proper name
        "Regen Network",
    ])
    def test_allows_proper_names(self, valid_name):
        assert self.filter.matches_generic_pattern(valid_name) is False

    def test_full_filter_with_generic_pattern(self):
        entity = {"name": "the character", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "generic_pattern"


class TestSentenceLikeFilter:
    """Tests for sentence-like entity filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Sentence fragments should be blocked
    # Note: "can" is excluded from verb pattern because "Can" is a common name
    @pytest.mark.parametrize("sentence", [
        "the most important thing is to act",
        "according to the latest research",
        "this is what we need",
        "has been working on",
        "why does this matter?",
        "what is the solution.",
    ])
    def test_blocks_sentence_fragments(self, sentence):
        assert self.filter.is_sentence_like(sentence) is True

    def test_can_is_excluded_to_allow_names(self):
        """'can' is excluded from sentence pattern because Can is a name."""
        # "can help" is not blocked because "can" is excluded
        assert self.filter.is_sentence_like("can help with sustainability") is False

    # Valid entity names should pass
    @pytest.mark.parametrize("valid_name", [
        "Regenerative Agriculture",
        "Carbon Credit Methodology",
        "Regen Network Development",
        "Voluntary Carbon Market",
    ])
    def test_allows_entity_names(self, valid_name):
        assert self.filter.is_sentence_like(valid_name) is False

    def test_full_filter_with_sentence_like(self):
        entity = {"name": "this is what we need to do", "type": "CONCEPT"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "sentence_like"


class TestLengthLimitFilter:
    """Tests for length limit filtering."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_blocks_too_long_character_count(self):
        long_name = "x" * 100  # Exceeds 80 char limit
        assert self.filter.exceeds_length_limits(long_name) is True

    def test_blocks_too_many_words(self):
        many_words = "one two three four five six seven eight nine ten"  # 10 words > 8
        assert self.filter.exceeds_length_limits(many_words) is True

    def test_allows_reasonable_length(self):
        normal_name = "Regen Network Development Inc."
        assert self.filter.exceeds_length_limits(normal_name) is False

    def test_full_filter_with_too_long(self):
        entity = {"name": "x" * 100, "type": "CONCEPT"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "too_long"


class TestBatchFiltering:
    """Tests for batch filtering functionality."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_batch_filtering(self):
        entities = [
            {"name": "Gregory Landua", "type": "PERSON"},  # Pass
            {"name": "we", "type": "PERSON"},  # Fail: stop_word
            {"name": "Regen Network", "type": "ORGANIZATION"},  # Pass
            {"name": "2030", "type": "DATE"},  # Fail: numeric
            {"name": "organization", "type": "ORGANIZATION"},  # Fail: tautological
        ]

        passed = self.filter.filter_batch(entities)

        assert len(passed) == 2
        assert passed[0]["name"] == "Gregory Landua"
        assert passed[1]["name"] == "Regen Network"

    def test_batch_updates_stats(self):
        entities = [
            {"name": "Gregory Landua", "type": "PERSON"},
            {"name": "we", "type": "PERSON"},
            {"name": "they", "type": "PERSON"},
        ]

        self.filter.filter_batch(entities)
        stats = self.filter.get_stats()

        assert stats["total_checked"] == 3
        assert stats["total_passed"] == 1
        assert stats["total_filtered"] == 2
        assert stats["reasons"]["stop_word"] == 2

    def test_reset_stats(self):
        entities = [{"name": "we", "type": "PERSON"}]
        self.filter.filter_batch(entities)

        stats_before = self.filter.get_stats()
        assert stats_before["total_checked"] == 1

        self.filter.reset_stats()
        stats_after = self.filter.get_stats()
        assert stats_after["total_checked"] == 0


class TestFilterConfig:
    """Tests for filter configuration."""

    def test_custom_stop_words(self):
        config = FilterConfig(additional_stop_words={"customterm"})
        filter_instance = EntityQualityFilter(config)

        assert filter_instance.is_stop_word("customterm") is True
        assert filter_instance.is_stop_word("we") is True  # Default still works

    def test_whitelist(self):
        config = FilterConfig(whitelist={"the organization"})
        filter_instance = EntityQualityFilter(config)

        # Would normally be blocked by generic pattern
        entity = {"name": "the organization", "type": "CONCEPT"}
        passes, reason = filter_instance.filter_entity(entity)
        assert passes is True

    def test_custom_length_limits(self):
        config = FilterConfig(max_name_length=50, max_word_count=5)
        filter_instance = EntityQualityFilter(config)

        # Would pass default but fail custom
        long_name = "x" * 60
        assert filter_instance.exceeds_length_limits(long_name) is True

        # 6 words fails with max 5
        six_words = "one two three four five six"
        assert filter_instance.exceeds_length_limits(six_words) is True


class TestFilteredWithReasons:
    """Tests for get_filtered_with_reasons functionality."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_returns_both_passed_and_filtered(self):
        entities = [
            {"name": "Gregory Landua", "type": "PERSON"},
            {"name": "we", "type": "PERSON"},
            {"name": "2030", "type": "DATE"},
        ]

        passed, filtered = self.filter.get_filtered_with_reasons(entities)

        assert len(passed) == 1
        assert passed[0]["name"] == "Gregory Landua"

        assert len(filtered) == 2
        # Check filtered entries have reasons
        filtered_dict = {e["name"]: reason for e, reason in filtered}
        assert filtered_dict["we"] == "stop_word"
        assert filtered_dict["2030"] == "numeric_only"


class TestConvenienceFunction:
    """Tests for the filter_entities convenience function."""

    def test_filter_entities_function(self):
        entities = [
            {"name": "Gregory Landua", "type": "PERSON"},
            {"name": "we", "type": "PERSON"},
        ]

        passed = filter_entities(entities)

        assert len(passed) == 1
        assert passed[0]["name"] == "Gregory Landua"


class TestReportGeneration:
    """Tests for report generation."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_generate_report(self):
        entities = [
            {"name": "Gregory Landua", "type": "PERSON"},
            {"name": "we", "type": "PERSON"},
            {"name": "they", "type": "PERSON"},
            {"name": "2030", "type": "DATE"},
        ]

        report = self.filter.generate_report(entities)

        assert "ENTITY QUALITY FILTER REPORT" in report
        assert "Total entities analyzed: 4" in report
        assert "Passed filters: 1" in report
        assert "Filtered out: 3" in report
        assert "stop_word" in report
        assert "numeric_only" in report


class TestFilterWithReasons:
    """Tests for filter_with_reasons that returns all applicable reasons."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_returns_all_reasons_for_multiple_violations(self):
        """Entity can trigger multiple filters - all reasons should be returned."""
        # "app.regen.claim" matches technical_pattern (package path)
        # It also matches sentence_like because it contains periods
        is_valid, reasons = self.filter.filter_with_reasons("app.regen.claim", "Project")
        assert is_valid is False
        assert "technical_pattern" in reasons
        # May have other reasons depending on which patterns match

    def test_returns_single_reason_for_single_violation(self):
        """Entity with single violation returns single reason."""
        # "2030" is numeric only - single reason
        is_valid, reasons = self.filter.filter_with_reasons("2030", "DATE")
        assert is_valid is False
        assert "numeric_only" in reasons
        assert len(reasons) == 1

    def test_returns_empty_list_for_valid_entity(self):
        """Valid entity returns empty reasons list."""
        is_valid, reasons = self.filter.filter_with_reasons("Regen Network", "ORGANIZATION")
        assert is_valid is True
        assert reasons == []

    def test_us_now_passes_due_to_whitelist(self):
        """'US' is now whitelisted as a country code.
        Previously was blocked as stop_word because lowercase 'us' is a pronoun.
        Fixed by adding built-in whitelist for country codes."""
        is_valid, reasons = self.filter.filter_with_reasons("US", "Organization")
        assert is_valid is True
        assert reasons == []


class TestRealWorldExamples:
    """Tests with real-world entity examples from Regen data."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_valid_regen_entities(self):
        """Entities that should pass all filters."""
        valid_entities = [
            {"name": "Regen Network", "type": "ORGANIZATION"},
            {"name": "Gregory Landua", "type": "PERSON"},
            {"name": "Ecocredit Module", "type": "MODULE"},
            {"name": "Voluntary Carbon Market", "type": "CONCEPT"},
            {"name": "Toucan Protocol", "type": "ORGANIZATION"},
            {"name": "Carbon Credit", "type": "PRODUCT"},
            {"name": "Cosmos SDK", "type": "SOFTWARE"},
            {"name": "Boulder, Colorado", "type": "PLACE"},
            {"name": "Mainnet Launch", "type": "EVENT"},
            {"name": "MsgCreateBatch", "type": "FUNCTION"},
        ]

        for entity in valid_entities:
            passes, reason = self.filter.filter_entity(entity)
            assert passes is True, f"Entity '{entity['name']}' should pass but was blocked by {reason}"

    def test_invalid_regen_entities(self):
        """Entities that should be filtered out."""
        invalid_entities = [
            ({"name": "we", "type": "PERSON"}, "stop_word"),
            ({"name": "user", "type": "PERSON"}, "stop_word"),
            ({"name": "validator", "type": "PERSON"}, "stop_word"),
            ({"name": "the community", "type": "ORGANIZATION"}, "generic_pattern"),
            ({"name": "some projects", "type": "PROJECT"}, "generic_pattern"),
            ({"name": "place", "type": "PLACE"}, "tautological"),  # Changed from organization
            ({"name": "2025", "type": "DATE"}, "numeric_only"),
            ({"name": "what is the best approach?", "type": "QUESTION"}, "sentence_like"),
        ]

        for entity, expected_reason in invalid_entities:
            passes, reason = self.filter.filter_entity(entity)
            assert passes is False, f"Entity '{entity['name']}' should be blocked"
            assert reason == expected_reason, f"Entity '{entity['name']}' blocked for wrong reason: {reason} != {expected_reason}"


class TestTemplateAndPlaceholderFilters:
    """Tests for template/placeholder pattern filters."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_blocks_jira_issue_id(self):
        """JIRA-style issue IDs should be blocked."""
        passes, reason = self.filter.filter_entity({"name": "APP-776", "type": "CLAIM"})
        assert passes is False
        assert reason == "jira_issue_id"

    def test_blocks_erc_standard(self):
        """ERC standards should be blocked."""
        passes, reason = self.filter.filter_entity({"name": "ERC-20", "type": "TECHNOLOGY"})
        assert passes is False
        assert reason == "erc_standard"

    def test_blocks_boilerplate_phrase(self):
        """Known boilerplate phrases should be blocked."""
        passes, reason = self.filter.filter_entity({"name": "Testing Instructions", "type": "CLAIM"})
        assert passes is False
        assert reason == "boilerplate"

    def test_blocks_placeholder_person(self):
        """Placeholder PERSON entities should be blocked."""
        passes, reason = self.filter.filter_entity({"name": "Public Users", "type": "PERSON"})
        assert passes is False
        # May be blocked by either placeholder (FIX-003) or placeholder_person
        assert reason in ("placeholder_person", "placeholder")


class TestBuiltInWhitelist:
    """Tests for the built-in entity whitelist functionality."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # ===== Country Code Tests =====

    @pytest.mark.parametrize("country_code", [
        "US", "UK", "EU", "CA", "AU", "NZ", "FR", "DE", "IT", "JP", "CN", "IN", "BR"
    ])
    def test_country_codes_are_whitelisted(self, country_code):
        """Country codes should not be blocked even if they match stop words."""
        assert self.filter.is_whitelisted(country_code) is True

    @pytest.mark.parametrize("country_code", ["US", "us", "Us", "uS"])
    def test_whitelist_is_case_insensitive(self, country_code):
        """Whitelist check should be case-insensitive."""
        assert self.filter.is_whitelisted(country_code) is True

    def test_us_passes_filter_entity(self):
        """'US' should pass filter_entity (not blocked as pronoun)."""
        entity = {"name": "US", "type": "ORGANIZATION"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True
        assert reason == ""

    def test_us_passes_filter_with_reasons(self):
        """'US' should pass filter_with_reasons (not blocked as pronoun)."""
        is_valid, reasons = self.filter.filter_with_reasons("US", "ORGANIZATION")
        assert is_valid is True
        assert reasons == []

    def test_lowercase_us_also_passes(self):
        """Even lowercase 'us' should pass when it's a country reference."""
        # Note: This is a policy decision - we whitelist regardless of case
        entity = {"name": "us", "type": "LOCATION"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True

    # ===== Organization Tests =====

    @pytest.mark.parametrize("org", [
        "UN", "NATO", "WHO", "UNESCO", "UNICEF", "NASA", "EPA", "FDA", "MIT"
    ])
    def test_organizations_are_whitelisted(self, org):
        """International organizations and agencies should not be blocked."""
        assert self.filter.is_whitelisted(org) is True
        entity = {"name": org, "type": "ORGANIZATION"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True, f"{org} should pass but was blocked by {reason}"

    # ===== Tech/Science Tests =====

    @pytest.mark.parametrize("tech_term", [
        "AI", "ML", "API", "DNA", "RNA", "CO2", "GPS", "IoT", "CPU", "GPU"
    ])
    def test_tech_science_terms_whitelisted(self, tech_term):
        """Technical and scientific abbreviations should not be blocked."""
        assert self.filter.is_whitelisted(tech_term) is True
        entity = {"name": tech_term, "type": "CONCEPT"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True, f"{tech_term} should pass but was blocked by {reason}"

    # ===== Currency Tests =====

    @pytest.mark.parametrize("currency", [
        "USD", "EUR", "GBP", "JPY", "BTC", "ETH", "ATOM", "REGEN"
    ])
    def test_currency_codes_whitelisted(self, currency):
        """Currency codes should not be blocked."""
        assert self.filter.is_whitelisted(currency) is True
        entity = {"name": currency, "type": "CURRENCY"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True, f"{currency} should pass but was blocked by {reason}"

    # ===== Whitelist Configuration =====

    def test_user_whitelist_extends_builtin(self):
        """User-provided whitelist should extend the built-in whitelist."""
        config = FilterConfig(whitelist={"custom_entity"})
        filter_instance = EntityQualityFilter(config)

        # User whitelist works
        assert filter_instance.is_whitelisted("custom_entity") is True

        # Built-in whitelist still works
        assert filter_instance.is_whitelisted("US") is True
        assert filter_instance.is_whitelisted("NASA") is True

    # ===== Edge Cases =====

    def test_non_whitelisted_stop_words_still_blocked(self):
        """Stop words not in whitelist should still be blocked."""
        # "we" (not "US") should still be blocked
        entity = {"name": "we", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "stop_word"

    def test_whitelist_has_expected_size(self):
        """Verify whitelist contains expected number of entries."""
        # Should have substantial coverage
        assert len(ENTITY_WHITELIST) > 200
        assert len(COUNTRY_CODES) >= 80
        assert len(ORGANIZATIONS) >= 50
        assert len(TECH_SCIENCE) >= 50
        assert len(CURRENCY_CODES) >= 30

    def test_regen_specific_terms_whitelisted(self):
        """Regen-specific terms should be in the whitelist."""
        regen_terms = ["MRV", "GHG", "CO2", "REGEN", "ATOM"]
        for term in regen_terms:
            assert self.filter.is_whitelisted(term) is True, f"{term} should be whitelisted"


class TestWhitelistIntegrationWithPipeline:
    """Tests for whitelist behavior in pipeline context."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    def test_batch_filter_respects_whitelist(self):
        """Batch filtering should respect the whitelist."""
        entities = [
            {"name": "US", "type": "LOCATION"},  # Should pass (whitelisted)
            {"name": "we", "type": "PERSON"},     # Should fail (stop word, not whitelisted)
            {"name": "NASA", "type": "ORGANIZATION"},  # Should pass (whitelisted)
            {"name": "they", "type": "PERSON"},  # Should fail (stop word, not whitelisted)
        ]

        passed = self.filter.filter_batch(entities)

        assert len(passed) == 2
        passed_names = [e["name"] for e in passed]
        assert "US" in passed_names
        assert "NASA" in passed_names
        assert "we" not in passed_names
        assert "they" not in passed_names

    def test_filtered_with_reasons_respects_whitelist(self):
        """get_filtered_with_reasons should respect the whitelist."""
        entities = [
            {"name": "US", "type": "LOCATION"},
            {"name": "us", "type": "PERSON"},     # lowercase should also pass
            {"name": "we", "type": "PERSON"},     # Should fail
        ]

        passed, filtered = self.filter.get_filtered_with_reasons(entities)

        assert len(passed) == 2
        assert len(filtered) == 1
        assert filtered[0][0]["name"] == "we"

    def test_is_whitelisted_method(self):
        """Test the is_whitelisted helper method directly."""
        filter_instance = EntityQualityFilter()

        # Whitelisted items
        assert filter_instance.is_whitelisted("US") is True
        assert filter_instance.is_whitelisted("us") is True  # Case insensitive
        assert filter_instance.is_whitelisted("NASA") is True
        assert filter_instance.is_whitelisted("REGEN") is True

        # Non-whitelisted items
        assert filter_instance.is_whitelisted("we") is False
        assert filter_instance.is_whitelisted("randomword") is False
        assert filter_instance.is_whitelisted("") is False


class TestE325FirstNameOrgNameArtifact:
    """Tests for E325 FirstName-OrgName artifact detection."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Known E325 artifacts that should be blocked
    @pytest.mark.parametrize("name", [
        "Will-Regen Foundation",
        "Chris-Chainflow",
        "Curtis-Meme_Network",
        "John-Acme Labs",
        "Mary-Green DAO",
        "Bob-Tech Protocol",
        "Alice-Crypto Inc",
        "Tom-Network Fund",
    ])
    def test_blocks_firstname_orgname_artifacts(self, name):
        """FirstName-OrgName patterns should be blocked when typed as PERSON."""
        assert self.filter.is_firstname_orgname_artifact(name, "PERSON") is True
        passes, reason = self.filter.filter_entity({"name": name, "type": "PERSON"})
        assert passes is False
        assert reason == "firstname_orgname_artifact"

    # Legitimate hyphenated names that should NOT be blocked
    @pytest.mark.parametrize("name", [
        "Mary-Jane",           # Hyphenated first name
        "Jean-Pierre",         # French name
        "Mary-Anne",           # Double first name
        "Smith-Jones",         # Hyphenated surname
        "Ana-Maria",           # Spanish double name
        "Sarah-Beth",          # Double first name
        "Kim-Jong",            # Asian name
    ])
    def test_allows_legitimate_hyphenated_names(self, name):
        """Legitimate hyphenated names should pass."""
        assert self.filter.is_firstname_orgname_artifact(name, "PERSON") is False
        passes, reason = self.filter.filter_entity({"name": name, "type": "PERSON"})
        # Should pass (or be blocked for a different reason, not firstname_orgname_artifact)
        if not passes:
            assert reason != "firstname_orgname_artifact"

    # Wrong type should not be blocked
    @pytest.mark.parametrize("entity_type", [
        "ORGANIZATION", "PROJECT", "CONCEPT", "TECHNOLOGY", "EVENT"
    ])
    def test_allows_non_person_types(self, entity_type):
        """FirstName-OrgName pattern with non-PERSON type should pass this filter."""
        assert self.filter.is_firstname_orgname_artifact("Will-Regen Foundation", entity_type) is False

    # Normal names without hyphen should pass
    @pytest.mark.parametrize("name", [
        "Gregory Landua",
        "Will Szal",
        "Regen Foundation",
        "Christian Shearer",
    ])
    def test_allows_normal_names(self, name):
        """Normal names without hyphen pattern should pass."""
        assert self.filter.is_firstname_orgname_artifact(name, "PERSON") is False

    def test_full_filter_with_e325_artifact(self):
        """Complete filter should block E325 artifacts."""
        entity = {"name": "Will-Regen Foundation", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "firstname_orgname_artifact"

    def test_filter_with_reasons_includes_e325(self):
        """filter_with_reasons should include firstname_orgname_artifact reason."""
        is_valid, reasons = self.filter.filter_with_reasons("Chris-Chainflow", "PERSON")
        assert is_valid is False
        assert "firstname_orgname_artifact" in reasons


class TestFIX011BlockchainAsLocation:
    """Tests for FIX-011: Block blockchain names as LOCATION."""

    def setup_method(self):
        self.filter = EntityQualityFilter()

    # Known blockchain names that should be blocked when typed as LOCATION
    @pytest.mark.parametrize("name", [
        "Ethereum",
        "ethereum",
        "Polygon",
        "polygon",
        "Solana",
        "solana",
        "Arbitrum",
        "arbitrum",
        "Optimism",
        "optimism",
        "Base",
        "base",
        "Bitcoin",
        "Cardano",
        "Avalanche",
        "Cosmos",
        "Near",
        "Fantom",
    ])
    def test_blocks_blockchain_as_location(self, name):
        """Blockchain names should be blocked when typed as LOCATION."""
        assert self.filter.is_blockchain_as_location(name, "LOCATION") is True
        passes, reason = self.filter.filter_entity({"name": name, "type": "LOCATION"})
        assert passes is False
        assert reason == "blockchain_as_location"

    # Blockchain names with correct type should NOT be blocked
    @pytest.mark.parametrize("entity_type", [
        "TECHNOLOGY", "PROJECT", "ORGANIZATION", "CONCEPT"
    ])
    def test_allows_blockchain_with_correct_type(self, entity_type):
        """Blockchain names with correct type should pass this filter."""
        assert self.filter.is_blockchain_as_location("Ethereum", entity_type) is False

    # Legitimate locations should pass
    @pytest.mark.parametrize("name", [
        "Boulder",
        "Colorado",
        "New York",
        "San Francisco",
        "Berlin",
        "London",
        "Global South",
        "Europe",
        "United States",
        "Amazon",  # River/rainforest - legitimate location
    ])
    def test_allows_legitimate_locations(self, name):
        """Legitimate location names should pass."""
        assert self.filter.is_blockchain_as_location(name, "LOCATION") is False
        # These may still fail other filters, but not blockchain_as_location
        passes, reason = self.filter.filter_entity({"name": name, "type": "LOCATION"})
        if not passes:
            assert reason != "blockchain_as_location"

    # L2 chains and scaling solutions
    @pytest.mark.parametrize("name", [
        "zkSync",
        "StarkNet",
        "Loopring",
        "Metis",
        "Mantle",
        "Linea",
        "Scroll",
    ])
    def test_blocks_l2_chains_as_location(self, name):
        """L2 chains should be blocked when typed as LOCATION."""
        assert self.filter.is_blockchain_as_location(name, "LOCATION") is True

    # Cosmos ecosystem chains
    @pytest.mark.parametrize("name", [
        "Osmosis",
        "Juno",
        "Evmos",
        "Injective",
        "Kava",
        "Akash",
        "Secret",
        "Terra",
    ])
    def test_blocks_cosmos_chains_as_location(self, name):
        """Cosmos ecosystem chains should be blocked when typed as LOCATION."""
        assert self.filter.is_blockchain_as_location(name, "LOCATION") is True

    # Network terms
    @pytest.mark.parametrize("name", [
        "mainnet",
        "testnet",
        "devnet",
        "Mainnet",
        "Testnet",
    ])
    def test_blocks_network_terms_as_location(self, name):
        """Network terms should be blocked when typed as LOCATION."""
        assert self.filter.is_blockchain_as_location(name, "LOCATION") is True

    # Compound names with blockchain prefix/suffix
    @pytest.mark.parametrize("name", [
        "ethereum mainnet",
        "polygon network",
        "solana testnet",
        "arbitrum one",
    ])
    def test_blocks_compound_blockchain_names(self, name):
        """Compound names with blockchain prefix should be blocked."""
        assert self.filter.is_blockchain_as_location(name, "LOCATION") is True

    def test_full_filter_with_blockchain_as_location(self):
        """Complete filter should block blockchain as LOCATION."""
        entity = {"name": "Polygon", "type": "LOCATION"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "blockchain_as_location"

    def test_filter_with_reasons_includes_blockchain_as_location(self):
        """filter_with_reasons should include blockchain_as_location reason."""
        is_valid, reasons = self.filter.filter_with_reasons("Ethereum", "LOCATION")
        assert is_valid is False
        assert "blockchain_as_location" in reasons

    def test_batch_filter_respects_blockchain_check(self):
        """Batch filtering should respect blockchain as LOCATION check."""
        entities = [
            {"name": "Boulder", "type": "LOCATION"},  # Should pass
            {"name": "Ethereum", "type": "LOCATION"},  # Should fail
            {"name": "Ethereum", "type": "TECHNOLOGY"},  # Should pass
            {"name": "Polygon", "type": "LOCATION"},  # Should fail
        ]

        passed = self.filter.filter_batch(entities)

        assert len(passed) == 2
        passed_names = [(e["name"], e["type"]) for e in passed]
        assert ("Boulder", "LOCATION") in passed_names
        assert ("Ethereum", "TECHNOLOGY") in passed_names
        assert ("Ethereum", "LOCATION") not in passed_names
        assert ("Polygon", "LOCATION") not in passed_names


class TestFIX013CodeModuleAsProcess:
    """
    Tests for FIX-013: Code module names mis-typed as PROCESS.

    Code modules like EntityQualityFilter, CanonicalResolver are software
    components (TECHNOLOGY), not processes.
    """

    @pytest.fixture(autouse=True)
    def setup_filter(self):
        """Create fresh filter for each test."""
        self.filter = EntityQualityFilter()

    # Known code module names should be blocked as PROCESS
    @pytest.mark.parametrize("name", [
        "EntityQualityFilter",
        "entityqualityfilter",
        "CanonicalResolver",
        "canonicalresolver",
        "ConfidenceFilter",
        "confidencefilter",
        "DocumentLevelDeduplicator",
        "OntologyNormalizer",
        "ListSplitter",
        "DataLoader",
        "ConfigParser",
        "RequestHandler",
    ])
    def test_blocks_code_modules_as_process(self, name):
        """Code module names should be blocked when typed as PROCESS."""
        assert self.filter.is_code_module_as_process(name, "PROCESS") is True

    # CamelCase class names should be blocked as PROCESS
    @pytest.mark.parametrize("name", [
        "DataProcessor",
        "FileHandler",
        "EventDispatcher",
        "MessageBroker",
        "TaskScheduler",
    ])
    def test_blocks_camelcase_classes_as_process(self, name):
        """CamelCase class names should be blocked when typed as PROCESS."""
        assert self.filter.is_code_module_as_process(name, "PROCESS") is True

    # Same names should pass with correct TECHNOLOGY type
    @pytest.mark.parametrize("name", [
        "EntityQualityFilter",
        "CanonicalResolver",
        "DataLoader",
    ])
    def test_allows_code_modules_as_technology(self, name):
        """Code modules should pass when correctly typed as TECHNOLOGY."""
        assert self.filter.is_code_module_as_process(name, "TECHNOLOGY") is False

    # Legitimate process names should pass
    @pytest.mark.parametrize("name", [
        "verification",
        "validation",
        "data ingestion",
        "entity extraction",
        "knowledge discovery",
        "carbon sequestration",
        "manufacturing",
    ])
    def test_allows_legitimate_processes(self, name):
        """Legitimate process names should pass."""
        assert self.filter.is_code_module_as_process(name, "PROCESS") is False

    def test_full_filter_with_code_module_as_process(self):
        """Complete filter should block code module as PROCESS."""
        entity = {"name": "EntityQualityFilter", "type": "PROCESS"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "code_module_as_process"

    def test_filter_with_reasons_includes_code_module_as_process(self):
        """filter_with_reasons should include code_module_as_process reason."""
        is_valid, reasons = self.filter.filter_with_reasons("CanonicalResolver", "PROCESS")
        assert is_valid is False
        assert "code_module_as_process" in reasons


class TestFIX014AbstractConceptAsMaterial:
    """
    Tests for FIX-014: Abstract concepts mis-typed as MATERIAL.

    Abstract environmental/ecological concepts like biodiversity, carbon
    sequestration should be CONCEPT, not MATERIAL.
    """

    @pytest.fixture(autouse=True)
    def setup_filter(self):
        """Create fresh filter for each test."""
        self.filter = EntityQualityFilter()

    # Abstract ecological concepts should be blocked as MATERIAL
    @pytest.mark.parametrize("name", [
        "biodiversity",
        "ecosystem",
        "ecology",
        "sustainability",
        "regeneration",
        "conservation",
        "resilience",
    ])
    def test_blocks_ecological_concepts_as_material(self, name):
        """Ecological concepts should be blocked when typed as MATERIAL."""
        assert self.filter.is_abstract_concept_as_material(name, "MATERIAL") is True

    # Carbon-related concepts should be blocked as MATERIAL
    @pytest.mark.parametrize("name", [
        "carbon sequestration",
        "carbon capture",
        "carbon offset",
        "sequestration",
    ])
    def test_blocks_carbon_concepts_as_material(self, name):
        """Carbon-related concepts should be blocked as MATERIAL."""
        assert self.filter.is_abstract_concept_as_material(name, "MATERIAL") is True

    # Economic/credit concepts should be blocked as MATERIAL
    @pytest.mark.parametrize("name", [
        "ecological assets",
        "natural capital",
        "carbon credits",
        "biodiversity credits",
        "ecosystem services",
    ])
    def test_blocks_economic_concepts_as_material(self, name):
        """Economic/credit concepts should be blocked as MATERIAL."""
        assert self.filter.is_abstract_concept_as_material(name, "MATERIAL") is True

    # Same names should pass with correct CONCEPT type
    @pytest.mark.parametrize("name", [
        "biodiversity",
        "carbon sequestration",
        "ecological assets",
        "sustainability",
    ])
    def test_allows_abstract_concepts_as_concept(self, name):
        """Abstract concepts should pass when correctly typed as CONCEPT."""
        assert self.filter.is_abstract_concept_as_material(name, "CONCEPT") is False

    # Physical materials should pass
    @pytest.mark.parametrize("name", [
        "wood",
        "steel",
        "concrete",
        "soil",
        "water",
        "biochar",
        "compost",
        "fertilizer",
        "plastic",
        "glass",
    ])
    def test_allows_physical_materials(self, name):
        """Physical materials should pass as MATERIAL."""
        assert self.filter.is_abstract_concept_as_material(name, "MATERIAL") is False

    def test_full_filter_with_abstract_concept_as_material(self):
        """Complete filter should block abstract concept as MATERIAL."""
        entity = {"name": "biodiversity", "type": "MATERIAL"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "abstract_concept_as_material"

    def test_filter_with_reasons_includes_abstract_concept_as_material(self):
        """filter_with_reasons should include abstract_concept_as_material reason."""
        is_valid, reasons = self.filter.filter_with_reasons("carbon sequestration", "MATERIAL")
        assert is_valid is False
        assert "abstract_concept_as_material" in reasons

    def test_batch_filter_respects_abstract_concept_check(self):
        """Batch filtering should respect abstract concept as MATERIAL check."""
        entities = [
            {"name": "wood", "type": "MATERIAL"},  # Should pass
            {"name": "biodiversity", "type": "MATERIAL"},  # Should fail
            {"name": "biodiversity", "type": "CONCEPT"},  # Should pass
            {"name": "carbon sequestration", "type": "MATERIAL"},  # Should fail
        ]

        passed = self.filter.filter_batch(entities)

        assert len(passed) == 2
        passed_names = [(e["name"], e["type"]) for e in passed]
        assert ("wood", "MATERIAL") in passed_names
        assert ("biodiversity", "CONCEPT") in passed_names
        assert ("biodiversity", "MATERIAL") not in passed_names
        assert ("carbon sequestration", "MATERIAL") not in passed_names


class TestSingleTokenPersonGuard:
    """
    Tests for FIX-016: Single-token PERSON guard.

    Blocks single-token PERSON names (e.g., "Max", "Will") unless:
    - Has explicit cue prefix (Dr., CEO, Chairman, etc.)
    - Is multi-token (full name)
    - Is hyphenated/underscored (treated as multi-token)
    """

    @pytest.fixture(autouse=True)
    def setup_filter(self):
        self.filter = EntityQualityFilter()

    # Block: Single-token without cue
    @pytest.mark.parametrize("name", ["Max", "Will", "Mark", "Alice", "Bob"])
    def test_blocks_single_token_person(self, name):
        """Single-token PERSON names should be blocked."""
        is_valid, reasons = self.filter.filter_with_reasons(name, "PERSON")
        assert is_valid is False
        assert "single_token_person" in reasons

    # Allow: With title/role prefix (various punctuation styles)
    @pytest.mark.parametrize("name", [
        "Dr. Jane",      # Standard with period
        "Dr Jane",       # Without period
        "CEO Alice",     # Role prefix
        "CEO: Alice",    # With colon
        "Chairman Bob",
        "President Max",
        "Director Sarah",
        "Prof. Smith",
        "Mr. Jones",
    ])
    def test_allows_with_cue_prefix(self, name):
        """Names with cue prefixes should be allowed."""
        assert self.filter.is_single_token_person(name, "PERSON") is False

    # Allow: Multi-token full names
    @pytest.mark.parametrize("name", [
        "Max Semenchuk",
        "Will Szal",
        "Mark Johnson",
        "Gregory Landua",
    ])
    def test_allows_full_names(self, name):
        """Multi-token full names should be allowed."""
        assert self.filter.is_single_token_person(name, "PERSON") is False

    # Allow: Hyphenated and underscored names (multi-token by tokenization)
    @pytest.mark.parametrize("name", [
        "Mary-Jane",       # Hyphenated first name
        "Max_Semenchuk",   # Underscored full name
        "Jean-Pierre",     # French hyphenated name
        "Smith-Jones",     # Hyphenated surname
    ])
    def test_allows_hyphenated_underscored_names(self, name):
        """Hyphenated/underscored names should be treated as multi-token and allowed."""
        assert self.filter.is_single_token_person(name, "PERSON") is False

    # Non-PERSON types should pass
    @pytest.mark.parametrize("entity_type", ["ORGANIZATION", "PROJECT", "CONCEPT"])
    def test_allows_non_person_types(self, entity_type):
        """Single-token entities with non-PERSON types should pass."""
        assert self.filter.is_single_token_person("Max", entity_type) is False

    # Verify whitelisted names are still blocked by this guard
    def test_blocks_whitelisted_single_token(self):
        """Will and Mark are whitelisted but should still be blocked by single-token guard."""
        is_valid, reasons = self.filter.filter_with_reasons("Will", "PERSON")
        assert "single_token_person" in reasons

    def test_full_filter_blocks_single_token_person(self):
        """filter_entity should block single-token PERSON."""
        entity = {"name": "Max", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is False
        assert reason == "single_token_person"

    def test_full_filter_allows_full_name(self):
        """filter_entity should allow full names."""
        entity = {"name": "Max Semenchuk", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True

    def test_full_filter_allows_cue_prefix(self):
        """filter_entity should allow names with cue prefixes."""
        entity = {"name": "Dr. Jane", "type": "PERSON"}
        passes, reason = self.filter.filter_entity(entity)
        assert passes is True

    # Verify lowercase single-token names are not blocked by this guard
    # (they are handled by is_lowercase_person instead)
    @pytest.mark.parametrize("name", ["bob", "friend", "guy"])
    def test_lowercase_not_blocked_by_this_guard(self, name):
        """Lowercase single-token names should be handled by is_lowercase_person, not this guard."""
        assert self.filter.is_single_token_person(name, "PERSON") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
