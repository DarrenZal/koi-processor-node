"""
Tests for the post-processing pipeline framework.

Tests the core framework components:
- ProcessingContext
- PostProcessingModule base class
- PipelineOrchestrator
- PipelineBuilder
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from knowledge_graph.postprocessing.context import Entity, Relationship, ProcessingContext
from knowledge_graph.postprocessing.base import PostProcessingModule, PassthroughModule, FilterModule, TransformModule
from knowledge_graph.postprocessing.pipeline import PipelineOrchestrator, PipelineBuilder


# =============================================================================
# Entity Tests
# =============================================================================

class TestEntity:
    """Tests for Entity dataclass."""

    def test_entity_creation(self):
        """Test basic entity creation."""
        entity = Entity(name="Test Entity", type="CONCEPT")
        assert entity.name == "Test Entity"
        assert entity.type == "CONCEPT"
        assert entity.confidence is None
        assert entity.metadata == {}

    def test_entity_with_confidence(self):
        """Test entity with confidence score."""
        entity = Entity(name="Test", type="PERSON", confidence=0.85)
        assert entity.confidence == 0.85

    def test_entity_with_metadata(self):
        """Test entity with metadata."""
        entity = Entity(name="Test", type="PERSON", metadata={"source": "twitter"})
        assert entity.metadata["source"] == "twitter"

    def test_entity_hash(self):
        """Test entity hashing for set operations."""
        e1 = Entity(name="Test", type="PERSON")
        e2 = Entity(name="Test", type="PERSON")
        e3 = Entity(name="Test", type="ORGANIZATION")

        assert hash(e1) == hash(e2)
        assert hash(e1) != hash(e3)

    def test_entity_equality(self):
        """Test entity equality."""
        e1 = Entity(name="Test", type="PERSON", confidence=0.9)
        e2 = Entity(name="Test", type="PERSON", confidence=0.5)
        e3 = Entity(name="Different", type="PERSON")

        assert e1 == e2  # Same name and type
        assert e1 != e3  # Different name

    def test_entity_to_dict(self):
        """Test entity to dictionary conversion."""
        entity = Entity(name="Test", type="PERSON", confidence=0.85, metadata={"key": "value"})
        d = entity.to_dict()

        assert d["name"] == "Test"
        assert d["type"] == "PERSON"
        assert d["confidence"] == 0.85
        assert d["metadata"]["key"] == "value"

    def test_entity_from_dict(self):
        """Test entity creation from dictionary."""
        d = {"name": "Test", "type": "PERSON", "confidence": 0.85}
        entity = Entity.from_dict(d)

        assert entity.name == "Test"
        assert entity.type == "PERSON"
        assert entity.confidence == 0.85


# =============================================================================
# Relationship Tests
# =============================================================================

class TestRelationship:
    """Tests for Relationship dataclass."""

    def test_relationship_creation(self):
        """Test basic relationship creation."""
        rel = Relationship(source="Alice", predicate="works_at", target="Regen")
        assert rel.source == "Alice"
        assert rel.predicate == "works_at"
        assert rel.target == "Regen"

    def test_relationship_with_confidence(self):
        """Test relationship with confidence."""
        rel = Relationship(source="A", predicate="knows", target="B", confidence=0.9)
        assert rel.confidence == 0.9

    def test_relationship_to_dict(self):
        """Test relationship to dictionary conversion."""
        rel = Relationship(source="A", predicate="knows", target="B", confidence=0.9)
        d = rel.to_dict()

        assert d["source"] == "A"
        assert d["predicate"] == "knows"
        assert d["target"] == "B"
        assert d["confidence"] == 0.9


# =============================================================================
# ProcessingContext Tests
# =============================================================================

class TestProcessingContext:
    """Tests for ProcessingContext."""

    def test_context_creation(self):
        """Test basic context creation."""
        context = ProcessingContext()
        assert len(context.entities) == 0
        assert len(context.relationships) == 0
        assert len(context.blocked_entities) == 0

    def test_context_with_entities(self):
        """Test context with entities."""
        entities = [
            Entity(name="Alice", type="PERSON"),
            Entity(name="Bob", type="PERSON")
        ]
        context = ProcessingContext(entities=entities)
        assert len(context.entities) == 2

    def test_block_entity(self):
        """Test blocking an entity."""
        entity = Entity(name="test", type="PERSON")
        context = ProcessingContext(entities=[entity])

        context.block_entity(entity, "low_quality", "TestModule")

        assert len(context.entities) == 0
        assert len(context.blocked_entities) == 1
        assert context.blocked_entities[0].metadata["blocked_by"] == "TestModule"
        assert context.blocked_entities[0].metadata["blocked_reason"] == "low_quality"

    def test_block_relationship(self):
        """Test blocking a relationship."""
        rel = Relationship(source="A", predicate="knows", target="B")
        context = ProcessingContext(relationships=[rel])

        context.block_relationship(rel, "low_confidence", "TestModule")

        assert len(context.relationships) == 0
        assert len(context.blocked_relationships) == 1

    def test_modify_entity(self):
        """Test modifying an entity."""
        original = Entity(name="alice", type="PERSON")
        modified = Entity(name="Alice", type="PERSON")
        context = ProcessingContext(entities=[original])

        context.modify_entity(original, modified, "Normalizer")

        assert len(context.entities) == 1
        assert context.entities[0].name == "Alice"
        assert "alice" in context.modified_entities

    def test_merge_entities(self):
        """Test merging entities."""
        e1 = Entity(name="Regen", type="ORG")
        e2 = Entity(name="Regen Network", type="ORG")
        canonical = Entity(name="Regen Network", type="ORGANIZATION")

        context = ProcessingContext(entities=[e1, e2])
        context.merge_entities([e1, e2], canonical, "Deduplicator")

        assert len(context.entities) == 1
        assert context.entities[0].name == "Regen Network"
        assert len(context.merged_groups) == 1

    def test_add_entities(self):
        """Test adding entities."""
        context = ProcessingContext()
        new_entities = [
            Entity(name="Alice", type="PERSON"),
            Entity(name="Bob", type="PERSON")
        ]

        context.add_entities(new_entities, "Splitter")

        assert len(context.entities) == 2
        assert context.statistics.get("Splitter_added") == 2

    def test_halt(self):
        """Test halting the pipeline."""
        context = ProcessingContext()
        context.halt("Test error")

        assert context.should_halt is True
        assert len(context.errors) == 1

    def test_get_summary(self):
        """Test getting context summary."""
        entities = [Entity(name="Test", type="PERSON")]
        context = ProcessingContext(entities=entities)

        summary = context.get_summary()

        assert summary["total_entities"] == 1
        assert "timestamp" in summary

    def test_clone(self):
        """Test cloning context."""
        original = ProcessingContext(
            entities=[Entity(name="Test", type="PERSON")],
            statistics={"count": 5}
        )

        cloned = original.clone()

        # Modify original
        original.entities.append(Entity(name="New", type="PERSON"))
        original.statistics["count"] = 10

        # Clone should be unchanged
        assert len(cloned.entities) == 1
        assert cloned.statistics["count"] == 5


# =============================================================================
# PostProcessingModule Tests
# =============================================================================

class TestPostProcessingModule:
    """Tests for base module classes."""

    def test_passthrough_module(self):
        """Test passthrough module."""
        module = PassthroughModule()
        context = ProcessingContext(
            entities=[Entity(name="Test", type="PERSON")]
        )

        result = module.process(context)

        assert len(result.entities) == 1
        assert module.get_name() == "Passthrough"

    def test_module_enabled_disabled(self):
        """Test module enable/disable."""
        enabled = PassthroughModule({"enabled": True})
        disabled = PassthroughModule({"enabled": False})

        assert enabled.enabled is True
        assert disabled.enabled is False

    def test_module_statistics(self):
        """Test module statistics tracking."""
        module = PassthroughModule()
        context = ProcessingContext(
            entities=[Entity(name="Test", type="PERSON")]
        )

        module.process(context)
        stats = module.get_statistics()

        assert stats["entities_processed"] == 1
        assert stats["enabled"] is True

    def test_module_reset(self):
        """Test module reset."""
        module = PassthroughModule()
        module.stats["test"] = 5

        module.reset()

        assert len(module.stats) == 0


# =============================================================================
# Custom Test Module
# =============================================================================

class BlockAllModule(FilterModule):
    """Test module that blocks all entities."""

    def get_name(self):
        return "BlockAll"

    def should_block_entity(self, entity):
        return True, "blocked_by_test"


class UppercaseModule(TransformModule):
    """Test module that uppercases entity names."""

    def get_name(self):
        return "Uppercase"

    def transform_entity(self, entity):
        if entity.name != entity.name.upper():
            new_entity = Entity(
                name=entity.name.upper(),
                type=entity.type,
                confidence=entity.confidence,
                metadata=entity.metadata
            )
            return True, new_entity
        return False, entity


# =============================================================================
# PipelineOrchestrator Tests
# =============================================================================

class TestPipelineOrchestrator:
    """Tests for PipelineOrchestrator."""

    def test_empty_pipeline(self):
        """Test pipeline with no modules."""
        pipeline = PipelineOrchestrator([])
        context = ProcessingContext(entities=[Entity(name="Test", type="PERSON")])

        result = pipeline.process(context)

        assert len(result.entities) == 1

    def test_single_module_pipeline(self):
        """Test pipeline with single module."""
        pipeline = PipelineOrchestrator([PassthroughModule()])
        context = ProcessingContext(entities=[Entity(name="Test", type="PERSON")])

        result = pipeline.process(context)

        assert len(result.entities) == 1

    def test_multi_module_pipeline(self):
        """Test pipeline with multiple modules."""
        pipeline = PipelineOrchestrator([
            PassthroughModule({"name": "Pass1"}),
            PassthroughModule({"name": "Pass2"}),
            PassthroughModule({"name": "Pass3"})
        ])
        context = ProcessingContext(entities=[Entity(name="Test", type="PERSON")])

        result = pipeline.process(context)

        assert len(result.entities) == 1
        stats = pipeline.get_statistics()
        assert len(stats["modules"]) == 3

    def test_filter_module_in_pipeline(self):
        """Test filter module in pipeline."""
        pipeline = PipelineOrchestrator([BlockAllModule()])
        context = ProcessingContext(
            entities=[Entity(name="Test", type="PERSON")]
        )

        result = pipeline.process(context)

        assert len(result.entities) == 0
        assert len(result.blocked_entities) == 1

    def test_transform_module_in_pipeline(self):
        """Test transform module in pipeline."""
        pipeline = PipelineOrchestrator([UppercaseModule()])
        context = ProcessingContext(
            entities=[Entity(name="test", type="PERSON")]
        )

        result = pipeline.process(context)

        assert result.entities[0].name == "TEST"

    def test_disabled_module_skipped(self):
        """Test that disabled modules are skipped."""
        pipeline = PipelineOrchestrator([
            PassthroughModule({"enabled": False, "name": "Disabled"}),
            PassthroughModule({"name": "Enabled"})
        ])
        context = ProcessingContext(entities=[Entity(name="Test", type="PERSON")])

        result = pipeline.process(context)
        stats = pipeline.get_statistics()

        # Only enabled module should have stats
        assert "Enabled" in stats["modules"]
        assert "Disabled" not in stats["modules"]

    def test_pipeline_halt(self):
        """Test pipeline halt functionality."""
        class HaltModule(PostProcessingModule):
            def get_name(self):
                return "Halt"
            def process(self, context):
                context.halt("Test halt")
                return context

        pipeline = PipelineOrchestrator([
            HaltModule(),
            PassthroughModule()  # Should not run
        ])
        context = ProcessingContext()

        result = pipeline.process(context)

        assert result.should_halt is True
        # Only one module should have run
        assert len(pipeline.get_statistics()["modules"]) == 1

    def test_pipeline_stop_on_error(self):
        """Test pipeline stop on error."""
        class ErrorModule(PostProcessingModule):
            def get_name(self):
                return "Error"
            def process(self, context):
                raise ValueError("Test error")

        pipeline = PipelineOrchestrator([ErrorModule()], stop_on_error=True)
        context = ProcessingContext()

        result = pipeline.process(context)

        assert len(result.errors) > 0
        assert result.should_halt is True

    def test_pipeline_continue_on_error(self):
        """Test pipeline continues on error when not stop_on_error."""
        class ErrorModule(PostProcessingModule):
            def get_name(self):
                return "Error"
            def process(self, context):
                raise ValueError("Test error")

        pipeline = PipelineOrchestrator([
            ErrorModule(),
            PassthroughModule()
        ], stop_on_error=False)
        context = ProcessingContext(entities=[Entity(name="Test", type="PERSON")])

        result = pipeline.process(context)

        assert len(result.errors) > 0
        assert result.should_halt is False
        # Both modules should have attempted to run
        assert len(pipeline.get_statistics()["modules"]) == 1  # Only passthrough succeeded

    def test_add_module(self):
        """Test adding module to pipeline."""
        pipeline = PipelineOrchestrator()
        pipeline.add_module(PassthroughModule())

        assert len(pipeline) == 1

    def test_remove_module(self):
        """Test removing module from pipeline."""
        pipeline = PipelineOrchestrator([PassthroughModule()])

        removed = pipeline.remove_module("Passthrough")

        assert removed is True
        assert len(pipeline) == 0

    def test_get_module(self):
        """Test getting module by name."""
        module = PassthroughModule()
        pipeline = PipelineOrchestrator([module])

        found = pipeline.get_module("Passthrough")

        assert found is module

    def test_process_entities_convenience(self):
        """Test process_entities convenience method."""
        pipeline = PipelineOrchestrator([PassthroughModule()])

        entities = [
            {"name": "Test", "type": "PERSON", "confidence": 0.9}
        ]

        result = pipeline.process_entities(entities)

        assert len(result.entities) == 1
        assert result.entities[0].name == "Test"

    def test_pipeline_execution_times(self):
        """Test that execution times are tracked."""
        pipeline = PipelineOrchestrator([PassthroughModule()])
        context = ProcessingContext()

        pipeline.process(context)
        stats = pipeline.get_statistics()

        assert "execution_times" in stats
        assert "Passthrough" in stats["execution_times"]
        assert stats["total_time"] >= 0

    def test_pipeline_reset(self):
        """Test pipeline reset."""
        pipeline = PipelineOrchestrator([PassthroughModule()])
        context = ProcessingContext()

        pipeline.process(context)
        pipeline.reset()

        stats = pipeline.get_statistics()
        assert len(stats["modules"]) == 0


# =============================================================================
# PipelineBuilder Tests
# =============================================================================

class TestPipelineBuilder:
    """Tests for PipelineBuilder."""

    def test_register_module(self):
        """Test module registration."""
        builder = PipelineBuilder()
        builder.register_module("Test", PassthroughModule)

        assert "Test" in builder.get_registered_modules()

    def test_from_dict(self):
        """Test building pipeline from dictionary."""
        builder = PipelineBuilder()
        builder.register_module("Passthrough", PassthroughModule)

        config = {
            "pipeline": {
                "stop_on_error": True,
                "modules": [
                    {"name": "Passthrough", "enabled": True, "config": {}}
                ]
            }
        }

        pipeline = builder.from_dict(config)

        assert len(pipeline) == 1
        assert pipeline.stop_on_error is True

    def test_from_dict_unknown_module(self):
        """Test building with unknown module (should skip)."""
        builder = PipelineBuilder()

        config = {
            "pipeline": {
                "modules": [
                    {"name": "Unknown", "enabled": True}
                ]
            }
        }

        pipeline = builder.from_dict(config)

        assert len(pipeline) == 0


# =============================================================================
# Integration Tests
# =============================================================================

class TestPipelineIntegration:
    """Integration tests for the pipeline framework."""

    def test_full_pipeline_flow(self):
        """Test a complete pipeline flow with multiple modules."""
        pipeline = PipelineOrchestrator([
            UppercaseModule(),
            PassthroughModule()
        ])

        context = ProcessingContext(
            entities=[
                Entity(name="alice", type="PERSON", confidence=0.9),
                Entity(name="bob", type="PERSON", confidence=0.8)
            ],
            relationships=[
                Relationship(source="alice", predicate="knows", target="bob")
            ]
        )

        result = pipeline.process(context)

        assert len(result.entities) == 2
        assert result.entities[0].name == "ALICE"
        assert len(result.modified_entities) == 2

    def test_pipeline_preserves_metadata(self):
        """Test that pipeline preserves entity metadata."""
        pipeline = PipelineOrchestrator([PassthroughModule()])

        context = ProcessingContext(
            entities=[
                Entity(name="Test", type="PERSON", metadata={"source": "twitter"})
            ]
        )

        result = pipeline.process(context)

        assert result.entities[0].metadata["source"] == "twitter"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
