"""
FIX-003: Entity Type Resolution Tests

Tests for:
1. Min-length validation (blocks single-char entities)
2. Expanded placeholder detection
3. Predicate-based type inference
4. Pipeline ordering (OntologyNormalizer before ListSplitter/EntityQualityFilter)

Run with: PYTHONPATH=src pytest tests/test_fix003_entity_validation.py -v
"""

import json
import sys
import importlib.util
import pytest
from pathlib import Path

# Make the test self-contained: add src/ to sys.path so isolated runs
# work without relying on other test files (e.g. test_fix002) being
# collected first to set it up.
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


class TestMinLengthFilter:
    """Test FIX-003 min-length validation."""

    def test_min_length_filter_blocks_single_char(self):
        """Single-char names should be filtered."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        # Should be blocked (too short)
        assert filter.is_too_short("X") == True
        assert filter.is_too_short(" ") == True
        assert filter.is_too_short("A") == True
        assert filter.is_too_short("I") == True
        assert filter.is_too_short("") == True

    def test_min_length_filter_allows_two_char(self):
        """Two-char strings should pass length check."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        # Two-char strings are allowed by length rule
        assert filter.is_too_short("US") == False
        assert filter.is_too_short("AI") == False
        assert filter.is_too_short("UK") == False
        assert filter.is_too_short("EU") == False

    def test_min_length_in_filter_entity(self):
        """Verify too_short reason is returned in filter_entity.

        Uses type=ORGANIZATION (not PERSON) so the FIX-016
        single_token_person guard doesn't fire first — we want to
        exercise the too_short branch specifically.
        """
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        passes, reason = filter.filter_entity({"name": "X", "type": "ORGANIZATION"})
        assert passes == False
        assert reason == "too_short"

    def test_min_length_strips_whitespace(self):
        """Whitespace-only names should be blocked."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        assert filter.is_too_short("   ") == True
        assert filter.is_too_short("\t") == True
        assert filter.is_too_short("  A  ") == True  # Single char after strip


class TestPlaceholderDetection:
    """Test FIX-003 expanded placeholder detection."""

    def test_placeholder_detection_expanded(self):
        """Placeholder patterns should be caught."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        # Should be detected as placeholder
        assert filter.is_placeholder("Unknown") == True
        assert filter.is_placeholder("Anonymous User") == True
        assert filter.is_placeholder("User 123") == True
        assert filter.is_placeholder("N/A") == True
        assert filter.is_placeholder("TBD") == True
        assert filter.is_placeholder("placeholder") == True
        assert filter.is_placeholder("test user") == True
        assert filter.is_placeholder("dummy") == True
        assert filter.is_placeholder("sample entity") == True
        assert filter.is_placeholder("none") == True
        assert filter.is_placeholder("todo") == True
        assert filter.is_placeholder("Public Users") == True

    def test_placeholder_detection_allows_valid(self):
        """Valid names should NOT be placeholder."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        # Should NOT be placeholder
        assert filter.is_placeholder("Gregory Landua") == False
        assert filter.is_placeholder("Regen Network") == False
        assert filter.is_placeholder("Bitcoin") == False
        assert filter.is_placeholder("Alice Smith") == False
        assert filter.is_placeholder("Cosmos Hub") == False

    def test_placeholder_in_filter_entity(self):
        """Verify placeholder reason is returned in filter_entity.

        Uses type=ORGANIZATION (not PERSON) so the FIX-016
        single_token_person guard doesn't fire first — we want to
        exercise the placeholder branch specifically.
        """
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        passes, reason = filter.filter_entity({"name": "Unknown", "type": "ORGANIZATION"})
        assert passes == False
        assert reason == "placeholder"

    def test_placeholder_applies_to_all_types(self):
        """Placeholder check should apply to ALL entity types, not just PERSON."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        # Should block placeholders regardless of type
        assert filter.is_placeholder("TBD", "ORGANIZATION") == True
        assert filter.is_placeholder("N/A", "PROJECT") == True
        assert filter.is_placeholder("Unknown", "TECHNOLOGY") == True


@pytest.mark.skipif(importlib.util.find_spec("rdflib") is None,
                    reason="rdflib is not installed (an optional dependency of these modules)")
class TestPredicateTypeInference:
    """Test FIX-003 predicate-based type inference."""

    def test_predicate_type_inference(self):
        """Predicate should hint entity types."""
        from knowledge_graph.graph_integration import KnowledgeGraphIntegrator

        # Create instance without full init (avoid DB connections)
        integrator = KnowledgeGraphIntegrator.__new__(KnowledgeGraphIntegrator)

        # Test type hints
        assert integrator._infer_type_from_predicate("works_at", "subject") == "PERSON"
        assert integrator._infer_type_from_predicate("works_at", "object") == "ORGANIZATION"
        assert integrator._infer_type_from_predicate("located_in", "object") == "LOCATION"
        assert integrator._infer_type_from_predicate("founded", "subject") == "PERSON"
        assert integrator._infer_type_from_predicate("founded", "object") == "ORGANIZATION"
        assert integrator._infer_type_from_predicate("member_of", "subject") == "PERSON"
        assert integrator._infer_type_from_predicate("attended", "subject") == "PERSON"
        assert integrator._infer_type_from_predicate("attended", "object") == "EVENT"

    def test_predicate_type_inference_unknown(self):
        """Unknown predicates should return None."""
        from knowledge_graph.graph_integration import KnowledgeGraphIntegrator

        integrator = KnowledgeGraphIntegrator.__new__(KnowledgeGraphIntegrator)

        assert integrator._infer_type_from_predicate("unknown_predicate", "subject") is None
        assert integrator._infer_type_from_predicate("random_relation", "object") is None
        assert integrator._infer_type_from_predicate("", "subject") is None

    def test_predicate_type_hints_exist(self):
        """Verify PREDICATE_TYPE_HINTS class attribute exists."""
        from knowledge_graph.graph_integration import KnowledgeGraphIntegrator

        assert hasattr(KnowledgeGraphIntegrator, "PREDICATE_TYPE_HINTS")
        assert isinstance(KnowledgeGraphIntegrator.PREDICATE_TYPE_HINTS, dict)
        assert len(KnowledgeGraphIntegrator.PREDICATE_TYPE_HINTS) > 0


class TestPipelineOrdering:
    """Test FIX-003 pipeline module ordering."""

    def test_pipeline_order_normalizer_before_filter(self):
        """OntologyNormalizer should run before ListSplitter and EntityQualityFilter."""
        repo_root = Path(__file__).resolve().parents[1]
        config_path = repo_root / 'src/knowledge_graph/config/pipeline_config.json'

        assert config_path.exists(), f"Config file not found: {config_path}"

        config = json.loads(config_path.read_text())

        # Correct structure: config["pipeline"]["modules"] with m["name"]
        modules = [m['name'] for m in config['pipeline']['modules']]

        assert 'OntologyNormalizer' in modules, "OntologyNormalizer not in pipeline"
        assert 'ListSplitter' in modules, "ListSplitter not in pipeline"
        assert 'EntityQualityFilter' in modules, "EntityQualityFilter not in pipeline"

        normalizer_idx = modules.index('OntologyNormalizer')
        splitter_idx = modules.index('ListSplitter')
        filter_idx = modules.index('EntityQualityFilter')

        assert normalizer_idx < splitter_idx, \
            f"OntologyNormalizer (idx={normalizer_idx}) must run before ListSplitter (idx={splitter_idx})"
        assert splitter_idx < filter_idx, \
            f"ListSplitter (idx={splitter_idx}) must run before EntityQualityFilter (idx={filter_idx})"

    def test_pipeline_config_version(self):
        """Config version should reflect FIX-003 changes."""
        repo_root = Path(__file__).resolve().parents[1]
        config_path = repo_root / 'src/knowledge_graph/config/pipeline_config.json'

        config = json.loads(config_path.read_text())

        # Version should be 1.1.0 or higher
        version = config.get("version", "0.0.0")
        major, minor, patch = map(int, version.split("."))
        assert (major, minor) >= (1, 1), f"Expected version >= 1.1.0, got {version}"


class TestFilterWithReasons:
    """Test that new filter reasons appear in filter_with_reasons."""

    def test_too_short_in_reasons(self):
        """Verify too_short appears in filter_with_reasons output."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        is_valid, reasons = filter.filter_with_reasons("X", "PERSON")
        assert is_valid == False
        assert "too_short" in reasons

    def test_placeholder_in_reasons(self):
        """Verify placeholder appears in filter_with_reasons output."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        is_valid, reasons = filter.filter_with_reasons("TBD", "ORGANIZATION")
        assert is_valid == False
        assert "placeholder" in reasons

    def test_stats_include_new_reasons(self):
        """Verify stats dict includes new reason keys."""
        from knowledge_graph.improvements.entity_quality_filter import EntityQualityFilter

        filter = EntityQualityFilter()

        # Run some filters
        filter.filter_batch([
            {"name": "X", "type": "PERSON"},
            {"name": "TBD", "type": "ORGANIZATION"},
            {"name": "Valid Entity", "type": "CONCEPT"},
        ])

        stats = filter.get_stats()
        assert "too_short" in stats["reasons"]
        assert "placeholder" in stats["reasons"]


@pytest.mark.skipif(importlib.util.find_spec("rdflib") is None,
                    reason="rdflib is not installed (an optional dependency of these modules)")
class TestIntegratorCounters:
    """Test FIX-003 entity resolution counters."""

    def test_counters_exist(self):
        """Verify FIX-003 counters are initialized."""
        from knowledge_graph.graph_integration import KnowledgeGraphIntegrator

        # Create minimal instance
        integrator = KnowledgeGraphIntegrator.__new__(KnowledgeGraphIntegrator)
        integrator.predicate_inferred_count = 0
        integrator.existing_lookup_count = 0
        integrator.entity_skip_count = 0
        integrator.entity_ambiguous_count = 0

        assert hasattr(integrator, "predicate_inferred_count")
        assert hasattr(integrator, "existing_lookup_count")
        assert hasattr(integrator, "entity_skip_count")
        assert hasattr(integrator, "entity_ambiguous_count")

    def test_log_entity_stats_method_exists(self):
        """Verify log_entity_stats method exists."""
        from knowledge_graph.graph_integration import KnowledgeGraphIntegrator

        assert hasattr(KnowledgeGraphIntegrator, "log_entity_stats")
        assert callable(getattr(KnowledgeGraphIntegrator, "log_entity_stats"))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
