"""
FIX-004: Role/Group Detection Tests

Tests for the enhanced role/group detection in EntityQualityFilter:
- Singular + plural form coverage
- Multi-word role patterns (Department Lead, Team, etc.)
- Cosmos SDK terms (Keeper, Relayer, Depositor)
- Proper person names NOT blocked
"""

import pytest

# Add src to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter


class TestFix004RoleDetection:
    """FIX-004: Role/Group detection tests."""

    @pytest.fixture
    def filter(self):
        return EntityQualityFilter()

    # ========================================================================
    # Test singular/plural forms
    # ========================================================================

    def test_blocks_singular_role_terms(self, filter):
        """Singular forms should be blocked."""
        assert filter.is_generic_group("buyer", "PERSON") is True
        assert filter.is_generic_group("validator", "PERSON") is True
        assert filter.is_generic_group("developer", "PERSON") is True
        assert filter.is_generic_group("team", "PERSON") is True
        assert filter.is_generic_group("keeper", "PERSON") is True
        assert filter.is_generic_group("member", "PERSON") is True
        assert filter.is_generic_group("contributor", "PERSON") is True

    def test_blocks_plural_role_terms(self, filter):
        """Plural forms should be blocked."""
        assert filter.is_generic_group("buyers", "PERSON") is True
        assert filter.is_generic_group("validators", "PERSON") is True
        assert filter.is_generic_group("developers", "PERSON") is True
        assert filter.is_generic_group("teams", "PERSON") is True
        assert filter.is_generic_group("keepers", "PERSON") is True
        assert filter.is_generic_group("members", "PERSON") is True
        assert filter.is_generic_group("contributors", "PERSON") is True

    # ========================================================================
    # Test multi-word patterns
    # ========================================================================

    def test_blocks_compound_role_terms(self, filter):
        """Compound terms ending in role word should be blocked."""
        assert filter.is_generic_group("carbon credit buyers", "PERSON") is True
        assert filter.is_generic_group("water utilities", "PERSON") is True
        assert filter.is_generic_group("network validators", "PERSON") is True
        assert filter.is_generic_group("community members", "PERSON") is True

    def test_blocks_role_patterns(self, filter):
        """Multi-word role patterns should be blocked."""
        assert filter.is_generic_group("Development Team", "PERSON") is True
        assert filter.is_generic_group("Partnerships Lead", "PERSON") is True
        assert filter.is_generic_group("Comms Lead", "PERSON") is True
        assert filter.is_generic_group("Governance Committee", "PERSON") is True
        assert filter.is_generic_group("Core Contributors", "PERSON") is True
        assert filter.is_generic_group("Working Group", "PERSON") is True
        assert filter.is_generic_group("Task Force", "PERSON") is True

    def test_blocks_department_title_patterns(self, filter):
        """Department + Title patterns should be blocked."""
        assert filter.is_generic_group("Engineering Manager", "PERSON") is True
        assert filter.is_generic_group("Product Lead", "PERSON") is True
        assert filter.is_generic_group("Operations Director", "PERSON") is True
        assert filter.is_generic_group("Finance Head", "PERSON") is True
        assert filter.is_generic_group("Security Chief", "PERSON") is True

    # ========================================================================
    # Test proper names are NOT blocked
    # ========================================================================

    def test_allows_proper_person_names(self, filter):
        """Proper person names should NOT be blocked."""
        assert filter.is_generic_group("Gregory Landua", "PERSON") is False
        assert filter.is_generic_group("Alice Johnson", "PERSON") is False
        assert filter.is_generic_group("Satoshi Nakamoto", "PERSON") is False
        assert filter.is_generic_group("Will Szal", "PERSON") is False
        assert filter.is_generic_group("Michael Head", "PERSON") is False  # surname collision guard
        assert filter.is_generic_group("John Smith", "PERSON") is False

    def test_allows_non_person_types(self, filter):
        """Role terms as other types should NOT be blocked."""
        assert filter.is_generic_group("validators", "TECHNOLOGY") is False
        assert filter.is_generic_group("Development Team", "ORGANIZATION") is False
        assert filter.is_generic_group("buyers", "CONCEPT") is False
        assert filter.is_generic_group("keeper", "TECHNOLOGY") is False

    # ========================================================================
    # Test Cosmos SDK terms
    # ========================================================================

    def test_blocks_cosmos_sdk_terms(self, filter):
        """Cosmos SDK role terms should be blocked as PERSON."""
        assert filter.is_generic_group("Keeper", "PERSON") is True
        assert filter.is_generic_group("keepers", "PERSON") is True
        assert filter.is_generic_group("relayer", "PERSON") is True
        assert filter.is_generic_group("relayers", "PERSON") is True
        assert filter.is_generic_group("depositor", "PERSON") is True
        assert filter.is_generic_group("depositors", "PERSON") is True
        assert filter.is_generic_group("proposer", "PERSON") is True
        assert filter.is_generic_group("proposers", "PERSON") is True

    # ========================================================================
    # Test matches_role_pattern directly
    # ========================================================================

    def test_matches_role_pattern(self, filter):
        """Test regex pattern matching for roles."""
        assert filter.matches_role_pattern("Project Lead", "PERSON") is True
        assert filter.matches_role_pattern("Program Manager", "PERSON") is True
        assert filter.matches_role_pattern("Working Group", "PERSON") is True
        assert filter.matches_role_pattern("Task Force", "PERSON") is True
        assert filter.matches_role_pattern("Core Contributors", "PERSON") is True
        assert filter.matches_role_pattern("Community Contributors", "PERSON") is True

    def test_matches_role_pattern_proper_names_not_matched(self, filter):
        """Proper names should not match role patterns."""
        assert filter.matches_role_pattern("Gregory Landua", "PERSON") is False
        assert filter.matches_role_pattern("Regen Network", "ORGANIZATION") is False
        assert filter.matches_role_pattern("Alice Smith", "PERSON") is False

    def test_matches_role_pattern_single_word_not_matched(self, filter):
        """Single word entities should not match role patterns (handled by GENERIC_GROUP_TERMS)."""
        assert filter.matches_role_pattern("validator", "PERSON") is False
        assert filter.matches_role_pattern("developer", "PERSON") is False

    # ========================================================================
    # Integration tests: filter_with_reasons
    # ========================================================================

    def test_filter_with_reasons_blocks_roles(self, filter):
        """Full filter flow should block role terms."""
        # Should be blocked with "generic_group" reason
        is_valid, reasons = filter.filter_with_reasons("Development Team", "PERSON")
        assert is_valid is False
        assert "generic_group" in reasons

        is_valid, reasons = filter.filter_with_reasons("validator", "PERSON")
        assert is_valid is False
        assert "generic_group" in reasons

        is_valid, reasons = filter.filter_with_reasons("Partnerships Lead", "PERSON")
        assert is_valid is False
        assert "generic_group" in reasons

        # Proper name should pass
        is_valid, reasons = filter.filter_with_reasons("Gregory Landua", "PERSON")
        assert is_valid is True
        assert reasons == []

    def test_filter_entity_blocks_roles(self, filter):
        """filter_entity should block role terms."""
        # Note: lowercase single-word terms may be blocked by lowercase_person first
        passes, reason = filter.filter_entity({"name": "buyers", "type": "PERSON"})
        assert passes is False
        assert reason in ("generic_group", "lowercase_person", "stop_word")

        passes, reason = filter.filter_entity({"name": "keeper", "type": "PERSON"})
        assert passes is False
        assert reason in ("generic_group", "lowercase_person")

        # Multi-word role patterns should be blocked with generic_group
        passes, reason = filter.filter_entity({"name": "Core Contributors", "type": "PERSON"})
        assert passes is False
        assert reason == "generic_group"

        # Capitalized singular terms should be blocked. FIX-016's
        # single_token_person guard may fire before the generic_group
        # role detector since "Buyer" is both a single-token PERSON and a
        # role term; either blocked-reason is acceptable as long as the
        # entity is rejected.
        passes, reason = filter.filter_entity({"name": "Buyer", "type": "PERSON"})
        assert passes is False
        assert reason in ("generic_group", "single_token_person")

        # Proper name should pass
        passes, reason = filter.filter_entity({"name": "Will Szal", "type": "PERSON"})
        assert passes is True
        assert reason == ""

    # ========================================================================
    # Edge cases
    # ========================================================================

    def test_case_insensitive_matching(self, filter):
        """Role detection should be case-insensitive."""
        assert filter.is_generic_group("BUYER", "PERSON") is True
        assert filter.is_generic_group("Buyer", "PERSON") is True
        assert filter.is_generic_group("buyer", "PERSON") is True
        assert filter.is_generic_group("DEVELOPMENT TEAM", "PERSON") is True
        assert filter.is_generic_group("development team", "PERSON") is True

    def test_whitespace_handling(self, filter):
        """Should handle leading/trailing whitespace."""
        assert filter.is_generic_group("  buyer  ", "PERSON") is True
        assert filter.is_generic_group("  Development Team  ", "PERSON") is True

    def test_humanactor_and_entity_types(self, filter):
        """Should also apply to HUMANACTOR and ENTITY types."""
        assert filter.is_generic_group("buyers", "HUMANACTOR") is True
        assert filter.is_generic_group("Development Team", "HUMANACTOR") is True
        assert filter.is_generic_group("validators", "ENTITY") is True
        assert filter.is_generic_group("Working Group", "ENTITY") is True
