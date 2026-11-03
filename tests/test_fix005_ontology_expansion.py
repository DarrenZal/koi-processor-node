"""
FIX-005: Ontology Granularity Expansion Tests

Tests for the new domain-specific and general entity types:
- Domain types: CREDIT_CLASS, GOVERNANCE_PROPOSAL, VALIDATOR, MODULE, API_MESSAGE, KEEPER
- General types: LICENSE, STANDARD, PROCESS, MATERIAL

Validates:
- New types in ALL_CANONICAL_TYPES and LLM_ALLOWED_TYPES
- Type normalization and aliases
- URI generation with correct prefixes
- Entity quality filter allows domain identifiers
- Entity quality filter blocks generic domain type words
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestFix005OntologyExpansion:
    """FIX-005: Ontology granularity expansion tests."""

    # ========================================================================
    # Test new canonical types exist
    # ========================================================================

    def test_domain_types_in_canonical(self):
        """Domain types should be in ALL_CANONICAL_TYPES."""
        from core.entity_types import ALL_CANONICAL_TYPES

        domain_types = {
            "CREDIT_CLASS", "GOVERNANCE_PROPOSAL", "VALIDATOR",
            "MODULE", "API_MESSAGE", "KEEPER"
        }
        for t in domain_types:
            assert t in ALL_CANONICAL_TYPES, f"{t} not in ALL_CANONICAL_TYPES"

    def test_general_types_in_canonical(self):
        """General types should be in ALL_CANONICAL_TYPES."""
        from core.entity_types import ALL_CANONICAL_TYPES

        general_types = {"LICENSE", "STANDARD", "PROCESS", "MATERIAL"}
        for t in general_types:
            assert t in ALL_CANONICAL_TYPES, f"{t} not in ALL_CANONICAL_TYPES"

    def test_new_types_llm_allowed(self):
        """New types should be in LLM_ALLOWED_TYPES."""
        from core.entity_types import LLM_ALLOWED_TYPES, is_llm_allowed_type

        new_types = {
            "CREDIT_CLASS", "GOVERNANCE_PROPOSAL", "VALIDATOR",
            "MODULE", "API_MESSAGE", "KEEPER",
            "LICENSE", "STANDARD", "PROCESS", "MATERIAL"
        }
        for t in new_types:
            assert t in LLM_ALLOWED_TYPES, f"{t} not in LLM_ALLOWED_TYPES"
            assert is_llm_allowed_type(t), f"is_llm_allowed_type({t}) returned False"

    # ========================================================================
    # Test type normalization
    # ========================================================================

    def test_normalize_credit_class_aliases(self):
        """Credit class aliases should normalize correctly."""
        from core.entity_types import normalize_type

        assert normalize_type("CREDITCLASS") == "CREDIT_CLASS"
        assert normalize_type("creditclass") == "CREDIT_CLASS"
        assert normalize_type("ECOCREDIT") == "CREDIT_CLASS"
        assert normalize_type("eco_credit") == "CREDIT_CLASS"

    def test_normalize_governance_proposal_aliases(self):
        """Governance proposal aliases should normalize correctly."""
        from core.entity_types import normalize_type

        assert normalize_type("GOVERNANCEPROPOSAL") == "GOVERNANCE_PROPOSAL"
        assert normalize_type("PROPOSAL") == "GOVERNANCE_PROPOSAL"
        assert normalize_type("gov_proposal") == "GOVERNANCE_PROPOSAL"

    def test_normalize_api_message_aliases(self):
        """API message aliases should normalize correctly."""
        from core.entity_types import normalize_type

        assert normalize_type("MESSAGE") == "API_MESSAGE"
        assert normalize_type("MSG") == "API_MESSAGE"
        assert normalize_type("protobuf_message") == "API_MESSAGE"

    def test_normalize_module_aliases(self):
        """Module aliases should normalize correctly."""
        from core.entity_types import normalize_type

        assert normalize_type("MODULE") == "MODULE"
        assert normalize_type("COSMOS_MODULE") == "MODULE"
        assert normalize_type("SDK_MODULE") == "MODULE"

    def test_normalize_license_not_concept(self):
        """LICENSE should be its own type, not CONCEPT."""
        from core.entity_types import normalize_type

        assert normalize_type("LICENSE") == "LICENSE"
        assert normalize_type("license") == "LICENSE"
        assert normalize_type("LICENSE") != "CONCEPT"

    def test_normalize_standard_not_concept(self):
        """STANDARD should be its own type, not CONCEPT."""
        from core.entity_types import normalize_type

        assert normalize_type("STANDARD") == "STANDARD"
        assert normalize_type("standard") == "STANDARD"
        assert normalize_type("STANDARD") != "CONCEPT"

    def test_normalize_process_aliases(self):
        """Process aliases should normalize correctly."""
        from core.entity_types import normalize_type

        assert normalize_type("PROCESS") == "PROCESS"
        assert normalize_type("WORKFLOW") == "PROCESS"
        assert normalize_type("PROCEDURE") == "PROCESS"

    def test_normalize_material_aliases(self):
        """Material aliases should normalize correctly."""
        from core.entity_types import normalize_type

        assert normalize_type("MATERIAL") == "MATERIAL"
        assert normalize_type("RESOURCE") == "MATERIAL"
        assert normalize_type("SUBSTANCE") == "MATERIAL"

    # ========================================================================
    # Test type descriptions
    # ========================================================================

    def test_new_types_have_descriptions(self):
        """New types should have descriptions."""
        from core.entity_types import get_canonical_description

        new_types = [
            "CREDIT_CLASS", "GOVERNANCE_PROPOSAL", "VALIDATOR",
            "MODULE", "API_MESSAGE", "KEEPER",
            "LICENSE", "STANDARD", "PROCESS", "MATERIAL"
        ]
        for t in new_types:
            desc = get_canonical_description(t)
            assert desc != "Unknown entity type", f"{t} has no description"
            assert len(desc) > 10, f"{t} description too short: {desc}"

    # ========================================================================
    # Test URI generation
    # ========================================================================

    def test_uri_prefixes_for_new_types(self):
        """New types should have URI prefixes (not fallback to 'entity')."""
        from knowledge_graph.uri_generator import DeterministicURIGenerator

        gen = DeterministicURIGenerator()

        # Check domain types have prefixes (not fallback to "entity")
        assert "credit-class" in gen.generate_uri("C01", "CREDIT_CLASS")
        assert "proposal" in gen.generate_uri("Proposal 47", "GOVERNANCE_PROPOSAL")
        assert "validator" in gen.generate_uri("Chorus One", "VALIDATOR")
        assert "module" in gen.generate_uri("x/ecocredit", "MODULE")
        assert "msg" in gen.generate_uri("MsgSend", "API_MESSAGE")
        assert "keeper" in gen.generate_uri("EcocreditKeeper", "KEEPER")

        # Check general types
        assert "license" in gen.generate_uri("Apache 2.0", "LICENSE")
        assert "standard" in gen.generate_uri("ISO 14064", "STANDARD")
        assert "process" in gen.generate_uri("MRV Process", "PROCESS")
        assert "material" in gen.generate_uri("Biochar", "MATERIAL")

    def test_uri_no_entity_fallback_for_new_types(self):
        """New types should NOT fall back to 'entity' prefix."""
        from knowledge_graph.uri_generator import DeterministicURIGenerator

        gen = DeterministicURIGenerator()

        new_types = [
            ("C01", "CREDIT_CLASS"),
            ("Proposal 47", "GOVERNANCE_PROPOSAL"),
            ("Chorus One", "VALIDATOR"),
            ("x/ecocredit", "MODULE"),
            ("MsgSend", "API_MESSAGE"),
            ("EcocreditKeeper", "KEEPER"),
            ("Apache 2.0", "LICENSE"),
            ("ISO 14064", "STANDARD"),
            ("MRV Process", "PROCESS"),
            ("Biochar", "MATERIAL"),
        ]

        for name, entity_type in new_types:
            uri = gen.generate_uri(name, entity_type)
            assert "/entity/" not in uri, f"{entity_type} should not use /entity/ prefix: {uri}"

    # ========================================================================
    # Test ontology normalizer
    # ========================================================================

    def test_ontology_normalizer_new_mappings(self):
        """OntologyNormalizer should handle new type mappings."""
        from knowledge_graph.postprocessing.modules.ontology_normalizer_module import (
            OntologyNormalizerModule
        )

        normalizer = OntologyNormalizerModule()

        assert normalizer.get_canonical_type("CREDITCLASS") == "CREDIT_CLASS"
        assert normalizer.get_canonical_type("GOVERNANCEPROPOSAL") == "GOVERNANCE_PROPOSAL"
        assert normalizer.get_canonical_type("MESSAGE") == "API_MESSAGE"
        assert normalizer.get_canonical_type("WORKFLOW") == "PROCESS"

    def test_ontology_normalizer_module_not_project(self):
        """MODULE should normalize to MODULE, not PROJECT (critical fix)."""
        from knowledge_graph.postprocessing.modules.ontology_normalizer_module import (
            OntologyNormalizerModule
        )

        normalizer = OntologyNormalizerModule()

        # This is the critical fix - MODULE was previously mapped to PROJECT
        assert normalizer.get_canonical_type("MODULE") == "MODULE"
        assert normalizer.get_canonical_type("MODULE") != "PROJECT"

    # ========================================================================
    # Test entity quality filter allows domain identifiers
    # ========================================================================

    def test_allows_msg_types_as_api_message(self):
        """Msg* types should be allowed when typed as API_MESSAGE."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        f = EntityQualityFilter()

        # MsgSend as API_MESSAGE should pass
        passes, reason = f.filter_entity({"name": "MsgSend", "type": "API_MESSAGE"})
        assert passes is True, f"MsgSend blocked: {reason}"

        passes, reason = f.filter_entity({"name": "MsgCreateBatch", "type": "API_MESSAGE"})
        assert passes is True, f"MsgCreateBatch blocked: {reason}"

        passes, reason = f.filter_entity({"name": "MsgRetire", "type": "API_MESSAGE"})
        assert passes is True, f"MsgRetire blocked: {reason}"

    def test_allows_credit_class_ids(self):
        """Credit class IDs should be allowed when typed as CREDIT_CLASS."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        f = EntityQualityFilter()

        passes, reason = f.filter_entity({"name": "C01", "type": "CREDIT_CLASS"})
        assert passes is True, f"C01 blocked: {reason}"

        passes, reason = f.filter_entity({"name": "C02", "type": "CREDIT_CLASS"})
        assert passes is True, f"C02 blocked: {reason}"

    def test_allows_module_paths(self):
        """Module paths should be allowed when typed as MODULE."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        f = EntityQualityFilter()

        passes, reason = f.filter_entity({"name": "x/ecocredit", "type": "MODULE"})
        assert passes is True, f"x/ecocredit blocked: {reason}"

        passes, reason = f.filter_entity({"name": "x/group", "type": "MODULE"})
        assert passes is True, f"x/group blocked: {reason}"

    def test_blocks_generic_domain_words_even_when_typed(self):
        """Generic role words should be blocked even if mis-typed as domain types."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        f = EntityQualityFilter()

        # Generic words should be blocked even with correct domain type
        passes, reason = f.filter_entity({"name": "validators", "type": "VALIDATOR"})
        assert passes is False, "validators as VALIDATOR should be blocked"

        passes, reason = f.filter_entity({"name": "keepers", "type": "KEEPER"})
        assert passes is False, "keepers as KEEPER should be blocked"

        passes, reason = f.filter_entity({"name": "modules", "type": "MODULE"})
        assert passes is False, "modules as MODULE should be blocked"

        passes, reason = f.filter_entity({"name": "proposal", "type": "GOVERNANCE_PROPOSAL"})
        assert passes is False, "proposal as GOVERNANCE_PROPOSAL should be blocked"

    def test_allows_specific_domain_entities(self):
        """Specific named entities should be allowed for domain types."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        f = EntityQualityFilter()

        # Specific validator names should pass
        passes, reason = f.filter_entity({"name": "Chorus One", "type": "VALIDATOR"})
        assert passes is True, f"Chorus One blocked: {reason}"

        passes, reason = f.filter_entity({"name": "Figment", "type": "VALIDATOR"})
        assert passes is True, f"Figment blocked: {reason}"

        # Specific credit class names should pass
        passes, reason = f.filter_entity({"name": "CarbonPlus Grasslands", "type": "CREDIT_CLASS"})
        assert passes is True, f"CarbonPlus Grasslands blocked: {reason}"

        # Specific governance proposals should pass
        passes, reason = f.filter_entity({"name": "Proposal 47", "type": "GOVERNANCE_PROPOSAL"})
        assert passes is True, f"Proposal 47 blocked: {reason}"


class TestFix005IntegrationPromptBuilder:
    """Integration tests for prompt builder with new types."""

    def test_prompt_contains_new_types(self):
        """Extraction prompt should mention new types."""
        from extraction.prompt_builder import build_extraction_prompt, LLM_ALLOWED_TYPES

        prompt = build_extraction_prompt("Test content", "discourse")

        # Check LLM_ALLOWED_TYPES includes new types
        new_types = ["CREDIT_CLASS", "LICENSE", "STANDARD", "API_MESSAGE", "MODULE"]
        for t in new_types:
            assert t in LLM_ALLOWED_TYPES, f"{t} not in LLM_ALLOWED_TYPES"

    def test_prompt_has_type_guidance(self):
        """Prompt should include guidance for domain types."""
        from extraction.prompt_builder import build_extraction_prompt

        prompt = build_extraction_prompt("Test content about Regen Network", "discourse")

        # Check for FIX-005 type guidance (case-insensitive search)
        prompt_lower = prompt.lower()
        assert "credit_class" in prompt_lower or "credit class" in prompt_lower
        assert "module" in prompt_lower
        assert "license" in prompt_lower
        assert "standard" in prompt_lower


class TestFix005NonRegression:
    """Non-regression tests - ensure existing functionality still works."""

    def test_existing_types_still_work(self):
        """Existing types should still normalize correctly."""
        from core.entity_types import normalize_type

        # These should still work
        assert normalize_type("PERSON") == "PERSON"
        assert normalize_type("ORGANIZATION") == "ORGANIZATION"
        assert normalize_type("PROJECT") == "PROJECT"
        assert normalize_type("CONCEPT") == "CONCEPT"
        assert normalize_type("TECHNOLOGY") == "TECHNOLOGY"
        assert normalize_type("LOCATION") == "LOCATION"
        assert normalize_type("EVENT") == "EVENT"

    def test_existing_aliases_still_work(self):
        """Existing type aliases should still work."""
        from core.entity_types import normalize_type

        # These should still work
        assert normalize_type("ORG") == "ORGANIZATION"
        assert normalize_type("GPE") == "LOCATION"
        assert normalize_type("HUMAN") == "PERSON"
        assert normalize_type("MEETING") == "EVENT"

    def test_existing_filter_rules_still_work(self):
        """Existing filter rules should still work."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        f = EntityQualityFilter()

        # Pronouns should still be blocked
        passes, _ = f.filter_entity({"name": "we", "type": "PERSON"})
        assert passes is False

        # Generic groups should still be blocked
        passes, _ = f.filter_entity({"name": "buyers", "type": "PERSON"})
        assert passes is False

        # AI as PERSON should still be blocked
        passes, _ = f.filter_entity({"name": "ChatGPT", "type": "PERSON"})
        assert passes is False

        # Proper names should still pass
        passes, _ = f.filter_entity({"name": "Gregory Landua", "type": "PERSON"})
        assert passes is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
