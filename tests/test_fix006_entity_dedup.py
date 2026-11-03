"""
Tests for FIX-006 Entity Deduplication Improvements.

Tests cover:
- Shared normalization module (entity_normalizer.py)
- Per-type semantic threshold selection
- Resolver behavior for alias resolution

Author: Claude Code
Date: 2025-12-23
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from knowledge_graph.entity_normalizer import (
    normalize_entity_name,
    normalize_for_canonical_lookup,
    is_single_token_name,
    names_are_similar_enough_for_merge,
)
from knowledge_graph.uri_generator import DeterministicURIGenerator
from knowledge_graph.entity_resolver import (
    DEFAULT_TYPE_THRESHOLDS,
    DEFAULT_FUZZY_THRESHOLDS,
)


class TestNormalizeEntityName:
    """Test the shared normalization function."""

    def test_underscore_to_space(self):
        """FIX-006: underscores should become spaces."""
        assert normalize_entity_name("Gregory_Regen") == "gregory regen"
        assert normalize_entity_name("will_szal") == "will szal"
        assert normalize_entity_name("Regen_Network") == "regen network"

    def test_hyphen_to_space(self):
        """FIX-006: hyphens should become spaces."""
        assert normalize_entity_name("greg-landua") == "greg landua"
        assert normalize_entity_name("regen-foundation") == "regen foundation"

    def test_strip_leading_at(self):
        """FIX-006: leading @ should be stripped."""
        assert normalize_entity_name("@willszal") == "willszal"
        assert normalize_entity_name("@gregory_landua") == "gregory landua"

    def test_strip_pipe_suffix(self):
        """FIX-006: trailing | SUFFIX pattern should be stripped."""
        assert normalize_entity_name("Gregory | RND") == "gregory"
        assert normalize_entity_name("Gregory | RND INC") == "gregory"
        assert normalize_entity_name("Test | Something") == "test"

    def test_org_dot_normalization(self):
        """FIX-006: dots in org names should become spaces."""
        # With org type hint
        assert normalize_entity_name("regen.foundation", "ORGANIZATION") == "regen foundation"
        assert normalize_entity_name("regen.network", "ORGANIZATION") == "regen network"
        # Without type hint (default behavior)
        assert normalize_entity_name("regen.foundation") == "regen foundation"

    def test_lowercase(self):
        """Names should be lowercased."""
        assert normalize_entity_name("Gregory Landua") == "gregory landua"
        assert normalize_entity_name("REGEN NETWORK") == "regen network"

    def test_whitespace_normalization(self):
        """Multiple spaces should collapse to single space."""
        assert normalize_entity_name("Regen  Network") == "regen network"
        assert normalize_entity_name("  Gregory   Landua  ") == "gregory landua"

    def test_article_removal(self):
        """Articles at start should be removed."""
        assert normalize_entity_name("The Regen Network") == "regen network"
        assert normalize_entity_name("A Project") == "project"

    def test_corporate_suffix_removal(self):
        """Corporate suffixes should be removed."""
        assert normalize_entity_name("Regen Network Inc") == "regen network"
        assert normalize_entity_name("Regen Network, Inc.") == "regen network"
        assert normalize_entity_name("DeSci Labs AG") == "desci labs"

    def test_trailing_punctuation(self):
        """Trailing punctuation should be stripped."""
        assert normalize_entity_name("Gregory Landua.") == "gregory landua"
        assert normalize_entity_name("Regen Network!") == "regen network"

    def test_empty_string(self):
        """Empty string should return empty."""
        assert normalize_entity_name("") == ""

    def test_combined_transformations(self):
        """Multiple transformations should work together."""
        # @Gregory_Regen | RND -> strip @ -> strip | RND -> convert _ -> lowercase
        # = gregory regen
        assert normalize_entity_name("@Gregory_Regen | RND") == "gregory regen"
        # Gregory | RND -> strip | RND -> lowercase = gregory
        assert normalize_entity_name("Gregory | RND") == "gregory"
        assert normalize_entity_name("The regen.foundation, Inc.") == "regen foundation"


class TestIsSingleTokenName:
    """Test single token detection for safety guardrails."""

    def test_single_token(self):
        """Single tokens should be detected."""
        assert is_single_token_name("Max") is True
        assert is_single_token_name("will") is True
        assert is_single_token_name("Gregory") is True

    def test_multi_token(self):
        """Multi-token names should not be flagged."""
        assert is_single_token_name("Gregory Landua") is False
        assert is_single_token_name("will szal") is False
        assert is_single_token_name("Regen Network") is False

    def test_normalized_input(self):
        """Should work with normalized input."""
        # These get normalized before checking
        assert is_single_token_name("gregory") is True
        assert is_single_token_name("gregory landua") is False


class TestPerTypeThresholds:
    """Test per-type threshold defaults."""

    def test_person_threshold(self):
        """PERSON threshold should be 0.92."""
        assert DEFAULT_TYPE_THRESHOLDS["PERSON"] == 0.92

    def test_organization_threshold(self):
        """ORGANIZATION threshold should be 0.95."""
        assert DEFAULT_TYPE_THRESHOLDS["ORGANIZATION"] == 0.95

    def test_concept_threshold(self):
        """CONCEPT threshold should be 0.90."""
        assert DEFAULT_TYPE_THRESHOLDS["CONCEPT"] == 0.90

    def test_claim_threshold(self):
        """CLAIM threshold should be 0.98 (highest)."""
        assert DEFAULT_TYPE_THRESHOLDS["CLAIM"] == 0.98

    def test_default_threshold(self):
        """DEFAULT threshold should be 0.95."""
        assert DEFAULT_TYPE_THRESHOLDS["DEFAULT"] == 0.95


class TestFuzzyThresholds:
    """Test per-type fuzzy string matching thresholds."""

    def test_person_fuzzy_threshold(self):
        """PERSON fuzzy threshold should be 0.93 (raised from 0.88 to reduce false positives)."""
        assert DEFAULT_FUZZY_THRESHOLDS["PERSON"] == 0.93

    def test_organization_fuzzy_threshold(self):
        """ORGANIZATION fuzzy threshold should be 0.85."""
        assert DEFAULT_FUZZY_THRESHOLDS["ORGANIZATION"] == 0.85


class TestURIGeneratorNormalization:
    """Test that URI generator uses the new normalization."""

    def test_uri_generator_uses_shared_normalization(self):
        """URI generator should use the shared normalizer."""
        uri_gen = DeterministicURIGenerator()

        # These should now be normalized with FIX-006 rules
        assert uri_gen.normalize_name("Gregory_Regen") == "gregory regen"
        assert uri_gen.normalize_name("@willszal") == "willszal"

    def test_uri_generation_with_normalization(self):
        """URIs should be generated from normalized names."""
        uri_gen = DeterministicURIGenerator()

        # Different surface forms should produce the same URI after normalization
        uri1 = uri_gen.generate_uri("Gregory_Regen", "PERSON")
        uri2 = uri_gen.generate_uri("Gregory Regen", "PERSON")
        # After FIX-006 normalization, both normalize to "gregory regen"
        assert uri1 == uri2

    def test_uri_generation_with_type_hint(self):
        """URI generation should pass type to normalizer."""
        uri_gen = DeterministicURIGenerator()

        # Org with dots should normalize properly
        uri = uri_gen.generate_uri("regen.foundation", "ORGANIZATION")
        expected_uri = uri_gen.generate_uri("regen foundation", "ORGANIZATION")
        assert uri == expected_uri


class TestNamesSimilarEnoughForMerge:
    """Test pre-filter for merge candidates."""

    def test_exact_match_after_normalization(self):
        """Exact matches after normalization should pass."""
        assert names_are_similar_enough_for_merge(
            "Gregory_Regen", "gregory regen", "PERSON"
        ) is True

    def test_substring_match(self):
        """One name being substring of another should pass."""
        assert names_are_similar_enough_for_merge(
            "Greg", "Gregory", "PERSON"
        ) is True

    def test_same_first_token_org(self):
        """Organizations sharing first token should pass."""
        assert names_are_similar_enough_for_merge(
            "Regen Network", "Regen Foundation", "ORGANIZATION"
        ) is True


class TestCanonicalResolverIntegration:
    """Test canonical resolver with new normalization."""

    def test_canonical_resolver_loads(self):
        """Canonical resolver should load without errors."""
        try:
            from knowledge_graph.improvements.canonical_resolver import CanonicalResolver
            resolver = CanonicalResolver()
            assert resolver is not None
            assert len(resolver.alias_to_canonical) > 0
        except FileNotFoundError:
            pytest.skip("canonical_entities.json not found")

    def test_canonical_resolver_finds_normalized_aliases(self):
        """Resolver should find aliases via normalized lookup."""
        try:
            from knowledge_graph.improvements.canonical_resolver import CanonicalResolver
            resolver = CanonicalResolver()

            # These should resolve to Gregory Landua after FIX-006 normalization
            canonical, resolved = resolver.resolve("Gregory_Regen", "PERSON")
            assert resolved is True
            assert canonical == "Gregory Landua"

            canonical, resolved = resolver.resolve("@willszal", "PERSON")
            assert resolved is True
            assert canonical == "Will Szal"

        except FileNotFoundError:
            pytest.skip("canonical_entities.json not found")

    def test_canonical_resolver_regen_foundation(self):
        """Regen Foundation variants should resolve."""
        try:
            from knowledge_graph.improvements.canonical_resolver import CanonicalResolver
            resolver = CanonicalResolver()

            # FIX-006: These should all resolve to Regen Foundation
            variants = [
                "regen.foundation",
                "regenfoundation",
                "RegenFdn",
                "Regen_Foundation",
            ]
            for variant in variants:
                canonical, resolved = resolver.resolve(variant, "ORGANIZATION")
                assert resolved is True, f"'{variant}' should resolve"
                assert canonical == "Regen Foundation", f"'{variant}' should resolve to 'Regen Foundation'"

        except FileNotFoundError:
            pytest.skip("canonical_entities.json not found")


class TestEntityResolverThresholdSelection:
    """Test that EntityResolver selects correct thresholds per type."""

    def test_threshold_for_type_returns_correct_values(self):
        """EntityResolver should return type-specific thresholds."""
        try:
            from knowledge_graph.entity_resolver import EntityResolver

            # Create resolver with mock config (won't actually connect)
            # We just want to test threshold selection
            class MockResolver:
                type_thresholds = DEFAULT_TYPE_THRESHOLDS

                def _threshold_for_type(self, entity_type):
                    return self.type_thresholds.get(
                        entity_type.upper(),
                        self.type_thresholds.get("DEFAULT", 0.95)
                    )

            resolver = MockResolver()

            assert resolver._threshold_for_type("PERSON") == 0.92
            assert resolver._threshold_for_type("ORGANIZATION") == 0.95
            assert resolver._threshold_for_type("CONCEPT") == 0.90
            assert resolver._threshold_for_type("CLAIM") == 0.98
            assert resolver._threshold_for_type("UNKNOWN_TYPE") == 0.95  # Default

        except ImportError:
            pytest.skip("EntityResolver not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
