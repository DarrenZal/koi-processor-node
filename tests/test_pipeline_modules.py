"""
Tests for pipeline modules.

Tests all available pipeline modules:
- ConfidenceFilterModule
- EntityQualityFilterModule
- CanonicalResolverModule
- ListSplitterModule
- OntologyNormalizerModule
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from knowledge_graph.postprocessing.context import Entity, Relationship, ProcessingContext
from knowledge_graph.postprocessing.modules.confidence_filter_module import ConfidenceFilterModule
from knowledge_graph.postprocessing.modules.entity_quality_module import EntityQualityFilterModule
from knowledge_graph.postprocessing.modules.list_splitter_module import ListSplitterModule
from knowledge_graph.postprocessing.modules.ontology_normalizer_module import OntologyNormalizerModule

# CanonicalResolver may not be available in test environment
try:
    from knowledge_graph.postprocessing.modules.canonical_resolver_module import CanonicalResolverModule
    HAS_RESOLVER = True
except (ImportError, FileNotFoundError):
    HAS_RESOLVER = False


# =============================================================================
# CanonicalResolverModule Tests
# =============================================================================

if HAS_RESOLVER:
    class TestCanonicalResolverModule:
        """Tests for CanonicalResolverModule."""

        def test_aligns_type_using_canonical_registry(self):
            """Type should be replaced by canonical type when available."""
            module = CanonicalResolverModule()
            entity = Entity(name="Regen Network", type="PERSON")
            context = ProcessingContext(entities=[entity])

            result = module.process(context)

            assert len(result.entities) == 1
            assert result.entities[0].type == "ORGANIZATION"
            assert result.entities[0].metadata.get("original_type") == "PERSON"

        def test_allows_type_mismatch_resolution(self):
            """Should resolve even when provided type differs from canonical."""
            module = CanonicalResolverModule()
            entity = Entity(name="Regen Ledger", type="PROJECT")
            context = ProcessingContext(entities=[entity])

            result = module.process(context)

            assert len(result.entities) == 1
            assert result.entities[0].name == "Regen Ledger"
            assert result.entities[0].type == "TECHNOLOGY"


# =============================================================================
# ConfidenceFilterModule Tests
# =============================================================================

class TestConfidenceFilterModule:
    """Tests for ConfidenceFilterModule."""

    def test_module_name(self):
        """Test module name."""
        module = ConfidenceFilterModule()
        assert module.get_name() == "ConfidenceFilter"

    def test_default_thresholds(self):
        """Test default threshold values."""
        module = ConfidenceFilterModule()
        assert module.entity_threshold == 0.70
        assert module.relationship_threshold == 0.80

    def test_custom_thresholds(self):
        """Test custom threshold values."""
        module = ConfidenceFilterModule({
            "entity_threshold": 0.5,
            "relationship_threshold": 0.6
        })
        assert module.entity_threshold == 0.5
        assert module.relationship_threshold == 0.6

    def test_invalid_threshold(self):
        """Test invalid threshold raises error."""
        with pytest.raises(ValueError):
            ConfidenceFilterModule({"entity_threshold": 1.5})

    def test_high_confidence_entity_passes(self):
        """Test high confidence entity passes filter."""
        module = ConfidenceFilterModule()
        entity = Entity(name="Test", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1
        assert len(result.blocked_entities) == 0

    def test_low_confidence_entity_blocked(self):
        """Test low confidence entity is blocked."""
        module = ConfidenceFilterModule({"entity_threshold": 0.7})
        entity = Entity(name="Test", type="PERSON", confidence=0.5)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_null_confidence_allowed_by_default(self):
        """Test null confidence allowed when allow_null is True."""
        module = ConfidenceFilterModule({"allow_null": True})
        entity = Entity(name="Test", type="PERSON", confidence=None)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1

    def test_null_confidence_blocked_when_disabled(self):
        """Test null confidence blocked when allow_null is False."""
        module = ConfidenceFilterModule({"allow_null": False})
        entity = Entity(name="Test", type="PERSON", confidence=None)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_strict_mode(self):
        """Test strict mode requires confidence."""
        module = ConfidenceFilterModule({"strict_mode": True})
        entity = Entity(name="Test", type="PERSON", confidence=None)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_relationship_filtering(self):
        """Test relationship confidence filtering."""
        module = ConfidenceFilterModule({"relationship_threshold": 0.8})

        high_conf = Relationship(source="A", predicate="knows", target="B", confidence=0.9)
        low_conf = Relationship(source="C", predicate="knows", target="D", confidence=0.5)

        context = ProcessingContext(relationships=[high_conf, low_conf])

        result = module.process(context)

        assert len(result.relationships) == 1
        assert len(result.blocked_relationships) == 1

    def test_statistics_tracking(self):
        """Test statistics are tracked."""
        module = ConfidenceFilterModule({"entity_threshold": 0.7})

        entities = [
            Entity(name="High", type="PERSON", confidence=0.9),
            Entity(name="Low", type="PERSON", confidence=0.5)
        ]
        context = ProcessingContext(entities=entities)

        module.process(context)
        stats = module.get_statistics()

        assert stats["entities_blocked"] == 1


# =============================================================================
# EntityQualityFilterModule Tests
# =============================================================================

class TestEntityQualityFilterModule:
    """Tests for EntityQualityFilterModule."""

    def test_module_name(self):
        """Test module name."""
        module = EntityQualityFilterModule()
        assert module.get_name() == "EntityQualityFilter"

    def test_valid_entity_passes(self):
        """Test valid entity passes filter."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Gregory Landua", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1

    def test_pronoun_blocked(self):
        """Test pronouns are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="we", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_generic_noun_blocked(self):
        """Test generic nouns are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="user", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_numeric_only_blocked(self):
        """Test numeric-only entities are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="2030", type="CONCEPT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_tautological_blocked(self):
        """Test tautological entities (name=type) are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="organization", type="ORGANIZATION")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_lowercase_person_blocked(self):
        """Test lowercase single-word PERSON entities are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="mom", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_capitalized_person_allowed(self):
        """Test capitalized multi-token person name is allowed.

        Since FIX-016 (single-token capitalized PERSON without cue prefix
        is blocked), this test uses a multi-token name. The module's
        intent here is that a well-formed PERSON entity passes through —
        single-token filtering is covered directly in
        test_entity_quality_filter.test_blocks_single_token_person.
        """
        module = EntityQualityFilterModule()
        entity = Entity(name="Alice Smith", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1

    def test_jira_issue_blocked(self):
        """Test JIRA-style issue identifiers are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="APP-776", type="CLAIM")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_boilerplate_blocked(self):
        """Test known boilerplate phrases are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Testing Instructions", type="CLAIM")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_placeholder_person_blocked(self):
        """Test placeholder PERSON values are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Public Users", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_generic_pattern_blocked(self):
        """Test generic patterns (the X, our X) are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="the community", type="ORGANIZATION")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_sentence_like_blocked(self):
        """Test sentence-like entities are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="this is a sentence", type="CONCEPT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_url_blocked(self):
        """Test URLs are blocked."""
        module = EntityQualityFilterModule()
        entity = Entity(name="https://example.com", type="CONCEPT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0

    def test_multiple_entities(self):
        """Test filtering multiple entities."""
        module = EntityQualityFilterModule()
        entities = [
            Entity(name="Gregory Landua", type="PERSON"),
            Entity(name="we", type="PERSON"),
            Entity(name="Regen Network", type="ORGANIZATION"),
            Entity(name="user", type="PERSON")
        ]
        context = ProcessingContext(entities=entities)

        result = module.process(context)

        assert len(result.entities) == 2
        assert len(result.blocked_entities) == 2

    def test_statistics_by_reason(self):
        """Test statistics track reasons."""
        module = EntityQualityFilterModule()
        entities = [
            Entity(name="we", type="PERSON"),  # stop_word
            Entity(name="2030", type="CONCEPT")  # numeric_only
        ]
        context = ProcessingContext(entities=entities)

        module.process(context)
        stats = module.get_statistics()

        assert stats.get("blocked_stop_word", 0) >= 1

    # =========================================================================
    # FIX-002: Git/Changelog, AI mistyped, EVENT guardrails tests
    # =========================================================================

    def test_git_commit_blocked_as_claim(self):
        """FIX-002: Git commit patterns blocked when typed as CLAIM."""
        module = EntityQualityFilterModule()
        entity = Entity(name="feat(api): add endpoint", type="CLAIM")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_git_merge_blocked_as_claim(self):
        """FIX-002: Git merge commits blocked when typed as CLAIM."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Merge pull request #123 from feature/auth", type="CLAIM")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_version_string_blocked_as_evidence(self):
        """FIX-002: Version strings blocked when typed as EVIDENCE."""
        module = EntityQualityFilterModule()
        entity = Entity(name="v1.2.3", type="EVIDENCE")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_git_commit_allowed_as_concept(self):
        """FIX-002: Git commit pattern allowed when NOT CLAIM/EVIDENCE/QUESTION."""
        module = EntityQualityFilterModule()
        # This would be weird but we only block for specific types
        entity = Entity(name="feat(api): add endpoint", type="TECHNOLOGY")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        # Should pass because type is not CLAIM/EVIDENCE/QUESTION
        assert len(result.entities) == 1

    def test_ai_system_blocked_as_person(self):
        """FIX-002: AI systems blocked when mis-typed as PERSON."""
        module = EntityQualityFilterModule()
        entity = Entity(name="ChatGPT", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_ai_system_allowed_as_technology(self):
        """FIX-002: AI systems allowed when correctly typed as TECHNOLOGY."""
        module = EntityQualityFilterModule()
        entity = Entity(name="ChatGPT", type="TECHNOLOGY")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1
        assert len(result.blocked_entities) == 0

    def test_claude_blocked_as_person(self):
        """FIX-002: Claude (AI) blocked when typed as PERSON."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Claude", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_generic_event_blocked(self):
        """FIX-002: Generic event words blocked when typed as EVENT."""
        module = EntityQualityFilterModule()
        entity = Entity(name="meeting", type="EVENT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_community_call_blocked_as_event(self):
        """FIX-002: 'community call' blocked as generic EVENT."""
        module = EntityQualityFilterModule()
        entity = Entity(name="community call", type="EVENT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_named_event_allowed(self):
        """FIX-002: Named events with specific titles allowed."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Regen Network Community Call", type="EVENT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        # "Regen Network Community Call" is a specific named event, should pass
        assert len(result.entities) == 1
        assert len(result.blocked_entities) == 0

    def test_cop28_event_allowed(self):
        """FIX-002: Specific conference names allowed as EVENT."""
        module = EntityQualityFilterModule()
        entity = Entity(name="COP28", type="EVENT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1
        assert len(result.blocked_entities) == 0

    def test_regen_gathering_2024_allowed(self):
        """FIX-002: Named gathering events allowed."""
        module = EntityQualityFilterModule()
        entity = Entity(name="Regen Gathering 2024", type="EVENT")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1
        assert len(result.blocked_entities) == 0


# =============================================================================
# ListSplitterModule Tests
# =============================================================================

class TestListSplitterModule:
    """Tests for ListSplitterModule."""

    def test_module_name(self):
        """Test module name."""
        module = ListSplitterModule()
        assert module.get_name() == "ListSplitter"

    def test_non_list_entity_unchanged(self):
        """Test non-list entity is unchanged."""
        module = ListSplitterModule()
        entity = Entity(name="Gregory Landua", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1
        assert result.entities[0].name == "Gregory Landua"

    def test_split_and_pattern(self):
        """Test splitting 'A and B' pattern."""
        module = ListSplitterModule()
        entity = Entity(name="Alice and Bob", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 2
        names = [e.name for e in result.entities]
        assert "Alice" in names
        assert "Bob" in names

    def test_split_ampersand_pattern(self):
        """Test splitting 'A & B' pattern."""
        module = ListSplitterModule()
        entity = Entity(name="Alice & Bob", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 2

    def test_split_comma_and_pattern(self):
        """Test splitting 'A, B, and C' pattern."""
        module = ListSplitterModule()
        entity = Entity(name="Alice, Bob, and Charlie", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 3
        names = [e.name for e in result.entities]
        assert "Alice" in names
        assert "Bob" in names
        assert "Charlie" in names

    def test_split_simple_comma_list(self):
        """Test splitting 'A, B, C' pattern."""
        module = ListSplitterModule()
        entity = Entity(name="Alice, Bob, Charlie", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 3

    def test_low_confidence_not_split(self):
        """Test low confidence entities are not split."""
        module = ListSplitterModule({"min_confidence_to_split": 0.8})
        entity = Entity(name="Alice and Bob", type="PERSON", confidence=0.5)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1
        assert result.entities[0].name == "Alice and Bob"

    def test_disabled_type_not_split(self):
        """Test disabled types are not split."""
        module = ListSplitterModule({"enabled_types": ["PERSON"]})
        entity = Entity(name="Project A and Project B", type="CONCEPT", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert len(result.entities) == 1

    def test_max_items_limit(self):
        """Test max items limit."""
        module = ListSplitterModule({"max_items": 2})
        entity = Entity(name="A, B, C", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        # Should not split because result would exceed max_items
        assert len(result.entities) == 1

    def test_split_preserves_metadata(self):
        """Test split preserves metadata."""
        module = ListSplitterModule()
        entity = Entity(
            name="Alice and Bob",
            type="PERSON",
            confidence=0.9,
            metadata={"source": "twitter"}
        )
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        for e in result.entities:
            assert e.metadata.get("split_from") == "Alice and Bob"
            assert e.confidence == 0.9

    def test_statistics_tracking(self):
        """Test statistics tracking."""
        module = ListSplitterModule()
        entity = Entity(name="Alice and Bob", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        module.process(context)
        stats = module.get_statistics()

        assert stats["lists_split"] == 1
        assert stats["entities_created"] == 2

    def test_would_split_helper(self):
        """Test would_split helper method."""
        module = ListSplitterModule()

        assert module.would_split("Alice and Bob") is True
        assert module.would_split("Alice") is False
        assert module.would_split("A, B, and C") is True


# =============================================================================
# OntologyNormalizerModule Tests
# =============================================================================

class TestOntologyNormalizerModule:
    """Tests for OntologyNormalizerModule."""

    def test_module_name(self):
        """Test module name."""
        module = OntologyNormalizerModule()
        assert module.get_name() == "OntologyNormalizer"

    def test_no_change_for_canonical_type(self):
        """Test no change for already canonical type."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "PERSON"

    def test_normalize_individual_to_person(self):
        """Test INDIVIDUAL normalizes to PERSON."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="INDIVIDUAL")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "PERSON"

    def test_normalize_org_to_organization(self):
        """Test ORG normalizes to ORGANIZATION."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="ORG")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "ORGANIZATION"

    def test_normalize_company_to_organization(self):
        """Test COMPANY normalizes to ORGANIZATION."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="COMPANY")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "ORGANIZATION"

    def test_normalize_repo_to_project(self):
        """Test REPO normalizes to PROJECT."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="REPO")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "PROJECT"

    def test_normalize_idea_to_concept(self):
        """Test IDEA normalizes to CONCEPT."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="IDEA")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "CONCEPT"

    def test_normalize_place_to_location(self):
        """Test PLACE normalizes to LOCATION."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="PLACE")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "LOCATION"

    def test_normalize_predicate_employed_by(self):
        """Test employed_by normalizes to works_at."""
        module = OntologyNormalizerModule()
        rel = Relationship(source="Alice", predicate="employed_by", target="Regen")
        context = ProcessingContext(relationships=[rel])

        result = module.process(context)

        assert result.relationships[0].predicate == "works_at"

    def test_normalize_predicate_member_of(self):
        """Test member_of normalizes to part_of."""
        module = OntologyNormalizerModule()
        rel = Relationship(source="Alice", predicate="member_of", target="Team")
        context = ProcessingContext(relationships=[rel])

        result = module.process(context)

        assert result.relationships[0].predicate == "part_of"

    def test_normalize_predicate_created(self):
        """Test created normalizes to founded."""
        module = OntologyNormalizerModule()
        rel = Relationship(source="Alice", predicate="created", target="Project")
        context = ProcessingContext(relationships=[rel])

        result = module.process(context)

        assert result.relationships[0].predicate == "founded"

    def test_custom_type_mapping(self):
        """Test custom type mapping."""
        module = OntologyNormalizerModule({
            "type_mappings": {"CUSTOM_TYPE": "CUSTOM_CANONICAL"}
        })
        entity = Entity(name="Test", type="CUSTOM_TYPE")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].type == "CUSTOM_CANONICAL"

    def test_custom_predicate_mapping(self):
        """Test custom predicate mapping."""
        module = OntologyNormalizerModule({
            "predicate_mappings": {"custom_pred": "canonical_pred"}
        })
        rel = Relationship(source="A", predicate="custom_pred", target="B")
        context = ProcessingContext(relationships=[rel])

        result = module.process(context)

        assert result.relationships[0].predicate == "canonical_pred"

    def test_preserves_original_type_in_metadata(self):
        """Test original type is preserved in metadata."""
        module = OntologyNormalizerModule()
        entity = Entity(name="Test", type="INDIVIDUAL")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].metadata.get("original_type") == "INDIVIDUAL"

    def test_preserves_original_predicate_in_metadata(self):
        """Test original predicate is preserved in metadata."""
        module = OntologyNormalizerModule()
        rel = Relationship(source="A", predicate="employed_by", target="B")
        context = ProcessingContext(relationships=[rel])

        result = module.process(context)

        assert result.relationships[0].metadata.get("original_predicate") == "employed_by"

    def test_statistics_tracking(self):
        """Test statistics tracking."""
        module = OntologyNormalizerModule()
        entities = [
            Entity(name="A", type="INDIVIDUAL"),
            Entity(name="B", type="ORG")
        ]
        rels = [
            Relationship(source="A", predicate="employed_by", target="B")
        ]
        context = ProcessingContext(entities=entities, relationships=rels)

        module.process(context)
        stats = module.get_statistics()

        assert stats["types_normalized"] == 2
        assert stats["predicates_normalized"] == 1

    def test_get_canonical_type_helper(self):
        """Test get_canonical_type helper method."""
        module = OntologyNormalizerModule()

        assert module.get_canonical_type("INDIVIDUAL") == "PERSON"
        assert module.get_canonical_type("PERSON") == "PERSON"
        assert module.get_canonical_type("ORG") == "ORGANIZATION"


# =============================================================================
# CanonicalResolverModule Tests (conditional)
# =============================================================================

@pytest.mark.skipif(not HAS_RESOLVER, reason="CanonicalResolver not available")
class TestCanonicalResolverModule:
    """Tests for CanonicalResolverModule."""

    def test_module_name(self):
        """Test module name."""
        module = CanonicalResolverModule()
        assert module.get_name() == "CanonicalResolver"

    def test_unknown_entity_unchanged(self):
        """Test unknown entity is unchanged."""
        module = CanonicalResolverModule()
        entity = Entity(name="Unknown Entity XYZ", type="PERSON")
        context = ProcessingContext(entities=[entity])

        result = module.process(context)

        assert result.entities[0].name == "Unknown Entity XYZ"


# =============================================================================
# Module Integration Tests
# =============================================================================

class TestModuleIntegration:
    """Integration tests for multiple modules working together."""

    def test_confidence_then_quality_filter(self):
        """Test confidence filter then quality filter."""
        from knowledge_graph.postprocessing.pipeline import PipelineOrchestrator

        pipeline = PipelineOrchestrator([
            ConfidenceFilterModule({"entity_threshold": 0.6}),
            EntityQualityFilterModule()
        ])

        entities = [
            Entity(name="Gregory Landua", type="PERSON", confidence=0.9),  # passes both
            Entity(name="we", type="PERSON", confidence=0.9),  # passes confidence, fails quality
            Entity(name="Test", type="PERSON", confidence=0.3),  # fails confidence
        ]
        context = ProcessingContext(entities=entities)

        result = pipeline.process(context)

        assert len(result.entities) == 1
        assert result.entities[0].name == "Gregory Landua"

    def test_normalizer_then_quality_filter(self):
        """Test normalizer runs before quality filter."""
        from knowledge_graph.postprocessing.pipeline import PipelineOrchestrator

        pipeline = PipelineOrchestrator([
            OntologyNormalizerModule(),
            EntityQualityFilterModule()
        ])

        entities = [
            Entity(name="Alice Smith", type="INDIVIDUAL"),  # INDIVIDUAL -> PERSON, multi-token passes filter
            Entity(name="we", type="INDIVIDUAL"),  # should still be blocked (stop word)
        ]
        context = ProcessingContext(entities=entities)

        result = pipeline.process(context)

        assert len(result.entities) == 1
        assert result.entities[0].type == "PERSON"

    def test_splitter_creates_valid_entities(self):
        """Test splitter creates entities that pass quality filter."""
        from knowledge_graph.postprocessing.pipeline import PipelineOrchestrator

        pipeline = PipelineOrchestrator([
            ListSplitterModule(),
            EntityQualityFilterModule()
        ])

        entity = Entity(name="Alice Smith and Bob Jones", type="PERSON", confidence=0.9)
        context = ProcessingContext(entities=[entity])

        result = pipeline.process(context)

        # Both split entities should pass quality filter — multi-token so
        # they clear the FIX-016 single-token-person block.
        assert len(result.entities) == 2

    def test_full_module_chain(self):
        """Test full chain: confidence -> normalizer -> quality -> splitter."""
        from knowledge_graph.postprocessing.pipeline import PipelineOrchestrator

        pipeline = PipelineOrchestrator([
            ConfidenceFilterModule({"entity_threshold": 0.6}),
            OntologyNormalizerModule(),
            EntityQualityFilterModule(),
            ListSplitterModule()
        ])

        entities = [
            Entity(name="Alice and Bob", type="INDIVIDUAL", confidence=0.9),
            Entity(name="we", type="PERSON", confidence=0.9),
            Entity(name="Test Corp", type="COMPANY", confidence=0.3),
        ]
        context = ProcessingContext(entities=entities)

        result = pipeline.process(context)

        # "Alice and Bob" -> split into "Alice" and "Bob" with type PERSON
        # "we" -> blocked by quality filter
        # "Test Corp" -> blocked by confidence filter
        assert len(result.entities) == 2
        names = [e.name for e in result.entities]
        assert "Alice" in names
        assert "Bob" in names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
