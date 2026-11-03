"""
Tests for OntologyLoader — fixture-based tests for TTL parsing and hierarchy.

WS3: Verifies that the ontology loader can parse TTL files, extract
rdfs:subClassOf hierarchies, and handle failure gracefully.
"""

import sys
import importlib.util
import os
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.ontology_loader import OntologyLoader

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), 'fixtures', 'rds-ontology-fixture.ttl')


@pytest.mark.skipif(importlib.util.find_spec("rdflib") is None,
                    reason="rdflib is not installed (an optional dependency of these modules)")
class TestOntologyLoaderFromFile:
    """Test loading ontology from local TTL file."""

    def test_load_from_file_succeeds(self):
        loader = OntologyLoader()
        result = loader.load_from_file(FIXTURE_PATH)
        assert result is True
        assert loader.is_loaded is True

    def test_triple_count(self):
        loader = OntologyLoader()
        loader.load_from_file(FIXTURE_PATH)
        assert loader.triple_count > 0

    def test_agent_subclass_of_entity(self):
        loader = OntologyLoader()
        loader.load_from_file(FIXTURE_PATH)
        parents = loader.get_parent_types("Agent")
        assert "Entity" in parents

    def test_work_order_no_parent(self):
        loader = OntologyLoader()
        loader.load_from_file(FIXTURE_PATH)
        parents = loader.get_parent_types("WorkOrder")
        assert parents == []


class TestOntologyLoaderGracefulFailure:
    """Test that failures are handled gracefully."""

    def test_load_nonexistent_file(self):
        loader = OntologyLoader()
        result = loader.load_from_file("/nonexistent/path/ontology.ttl")
        assert result is False
        assert loader.is_loaded is False

    def test_load_from_fuseki_bad_url(self):
        loader = OntologyLoader()
        result = loader.load_from_fuseki(
            "http://localhost:99999",  # Unreachable
            "koi",
            "urn:regen:graph:ontology",
        )
        assert result is False
        assert loader.is_loaded is False


class TestOntologyLoaderAliases:
    """Test hardcoded alias map."""

    def test_get_koi_type_agent(self):
        loader = OntologyLoader()
        assert loader.get_koi_type("Agent") == "AGENT"

    def test_get_koi_type_work_order(self):
        loader = OntologyLoader()
        assert loader.get_koi_type("WorkOrder") == "WORK_ORDER"

    def test_get_koi_type_individual(self):
        loader = OntologyLoader()
        assert loader.get_koi_type("Individual") == "PERSON"

    def test_get_koi_type_unknown_returns_none(self):
        loader = OntologyLoader()
        assert loader.get_koi_type("NonexistentType") is None

    def test_get_type_aliases_returns_dict(self):
        loader = OntologyLoader()
        aliases = loader.get_type_aliases()
        assert isinstance(aliases, dict)
        assert len(aliases) > 0
        assert "Agent" in aliases
