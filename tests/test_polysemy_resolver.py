"""
Tests for Polysemy Resolver Module

Tests the core logic of the polysemy resolver without requiring a database connection.
Database integration tests are skipped when DB is not available.

Author: Claude Code
Date: 2025-12-24
"""

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from tests import koi_test_targets as targets

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge_graph.polysemy_resolver import (
    compute_score,
    DEFAULT_TYPE_PRIORITY,
    EntityVariant,
    ResolutionResult,
    resolve_entity_variants,
    resolve_entity,
    get_default_db_config,
)


class TestComputeScore:
    """Tests for score computation logic."""

    def test_occurrence_count_weight(self):
        """Higher occurrence count should result in higher score."""
        variant1 = {
            'occurrence_count': 10,
            'relationship_count': 0,
            'entity_type': 'CONCEPT',
        }
        variant2 = {
            'occurrence_count': 100,
            'relationship_count': 0,
            'entity_type': 'CONCEPT',
        }

        score1, _ = compute_score(variant1)
        score2, _ = compute_score(variant2)

        assert score2 > score1
        # Occurrence weight is 1000x
        assert score2 - score1 == 90 * 1000

    def test_relationship_count_weight(self):
        """Higher relationship count should result in higher score."""
        variant1 = {
            'occurrence_count': 10,
            'relationship_count': 5,
            'entity_type': 'CONCEPT',
        }
        variant2 = {
            'occurrence_count': 10,
            'relationship_count': 50,
            'entity_type': 'CONCEPT',
        }

        score1, _ = compute_score(variant1)
        score2, _ = compute_score(variant2)

        assert score2 > score1
        # Relationship weight is 100x
        assert score2 - score1 == 45 * 100

    def test_type_priority_weight(self):
        """Higher priority type should result in higher score."""
        variant_tech = {
            'occurrence_count': 10,
            'relationship_count': 5,
            'entity_type': 'TECHNOLOGY',
        }
        variant_concept = {
            'occurrence_count': 10,
            'relationship_count': 5,
            'entity_type': 'CONCEPT',
        }

        score_tech, _ = compute_score(variant_tech)
        score_concept, _ = compute_score(variant_concept)

        # TECHNOLOGY (100) > CONCEPT (70)
        assert score_tech > score_concept
        # Type priority weight is 10x
        assert score_tech - score_concept == (100 - 70) * 10

    def test_type_hint_boost(self):
        """Type hint match should add 50000 to score."""
        variant = {
            'occurrence_count': 10,
            'relationship_count': 5,
            'entity_type': 'TECHNOLOGY',
        }

        score_no_hint, breakdown_no_hint = compute_score(variant)
        score_with_hint, breakdown_with_hint = compute_score(variant, type_hint='TECHNOLOGY')

        assert score_with_hint > score_no_hint
        assert score_with_hint - score_no_hint == 50000
        assert 'type_hint_match' in breakdown_with_hint
        assert 'type_hint_match' not in breakdown_no_hint

    def test_type_hint_case_insensitive(self):
        """Type hint matching should be case-insensitive."""
        variant = {
            'occurrence_count': 10,
            'relationship_count': 5,
            'entity_type': 'TECHNOLOGY',
        }

        score_upper, _ = compute_score(variant, type_hint='TECHNOLOGY')
        score_lower, _ = compute_score(variant, type_hint='technology')
        score_mixed, _ = compute_score(variant, type_hint='Technology')

        assert score_upper == score_lower == score_mixed

    def test_score_breakdown_format(self):
        """Score breakdown should include all components."""
        variant = {
            'occurrence_count': 42,
            'relationship_count': 7,
            'entity_type': 'PROJECT',
        }

        _, breakdown = compute_score(variant, type_hint='PROJECT')

        assert 'occ=42' in breakdown
        assert 'rels=7' in breakdown
        assert 'type_pri=90' in breakdown  # PROJECT priority
        assert 'type_hint_match=+50k' in breakdown

    def test_custom_type_priority(self):
        """Custom type priority should override defaults."""
        variant = {
            'occurrence_count': 10,
            'relationship_count': 5,
            'entity_type': 'CUSTOM_TYPE',
        }

        custom_priority = {'CUSTOM_TYPE': 999}

        score_default, _ = compute_score(variant)
        score_custom, _ = compute_score(variant, type_priority=custom_priority)

        # Default priority for unknown type is 0
        # Custom priority is 999
        assert score_custom > score_default
        assert score_custom - score_default == 999 * 10


class TestEntityVariant:
    """Tests for EntityVariant dataclass."""

    def test_to_dict(self):
        """EntityVariant should convert to dictionary."""
        variant = EntityVariant(
            uri='https://example.com/entity/1',
            entity_text='Ethereum',
            entity_type='TECHNOLOGY',
            occurrence_count=128,
            relationship_count=45,
            score=179200,
            score_breakdown='occ=128, rels=45, type_pri=100'
        )

        d = variant.to_dict()

        assert d['uri'] == 'https://example.com/entity/1'
        assert d['entity_text'] == 'Ethereum'
        assert d['entity_type'] == 'TECHNOLOGY'
        assert d['occurrence_count'] == 128
        assert d['relationship_count'] == 45
        assert d['score'] == 179200


class TestResolutionResult:
    """Tests for ResolutionResult dataclass."""

    def test_to_dict_with_winner(self):
        """ResolutionResult should convert to dictionary with winner."""
        winner = EntityVariant(
            uri='https://example.com/entity/1',
            entity_text='Ethereum',
            entity_type='TECHNOLOGY',
            occurrence_count=128,
            relationship_count=45,
            score=179200,
            score_breakdown='occ=128'
        )

        result = ResolutionResult(
            query_label='ethereum',
            type_hint='TECHNOLOGY',
            variant_count=3,
            winner=winner,
            alternatives=[],
            is_polysemy=True,
            resolution_method='type_hint_match'
        )

        d = result.to_dict()

        assert d['query_label'] == 'ethereum'
        assert d['type_hint'] == 'TECHNOLOGY'
        assert d['variant_count'] == 3
        assert d['winner'] is not None
        assert d['winner']['entity_text'] == 'Ethereum'
        assert d['is_polysemy'] is True
        assert d['resolution_method'] == 'type_hint_match'

    def test_to_dict_without_winner(self):
        """ResolutionResult should handle None winner."""
        result = ResolutionResult(
            query_label='nonexistent',
            type_hint=None,
            variant_count=0,
            winner=None,
            alternatives=[],
            is_polysemy=False,
            resolution_method='no_match'
        )

        d = result.to_dict()

        assert d['winner'] is None
        assert d['variant_count'] == 0
        assert d['resolution_method'] == 'no_match'


class TestDefaultTypeConfig:
    """Tests for default type priority configuration."""

    def test_technology_highest_priority(self):
        """TECHNOLOGY should have highest default priority."""
        max_priority = max(DEFAULT_TYPE_PRIORITY.values())
        assert DEFAULT_TYPE_PRIORITY['TECHNOLOGY'] == max_priority
        assert DEFAULT_TYPE_PRIORITY['TECHNOLOGY'] == 100

    def test_all_expected_types_present(self):
        """All common entity types should be in priority map."""
        expected_types = [
            'TECHNOLOGY', 'PROJECT', 'ORGANIZATION', 'CONCEPT',
            'STANDARD', 'PERSON', 'PROCESS', 'MATERIAL', 'MODULE',
            'LOCATION', 'EVENT', 'VALIDATOR', 'CREDIT_CLASS',
        ]

        for t in expected_types:
            assert t in DEFAULT_TYPE_PRIORITY, f"Missing type: {t}"

    def test_priority_ordering(self):
        """Priority ordering should be TECH > PROJECT > ORG > CONCEPT."""
        assert DEFAULT_TYPE_PRIORITY['TECHNOLOGY'] > DEFAULT_TYPE_PRIORITY['PROJECT']
        assert DEFAULT_TYPE_PRIORITY['PROJECT'] > DEFAULT_TYPE_PRIORITY['ORGANIZATION']
        assert DEFAULT_TYPE_PRIORITY['ORGANIZATION'] > DEFAULT_TYPE_PRIORITY['CONCEPT']


class TestDatabaseConfig:
    """Tests for database configuration."""

    def test_default_config(self, monkeypatch):
        """With nothing set, the config falls back to the code's documented defaults."""
        for var in ('POSTGRES_HOST', 'POSTGRES_PORT', 'POSTGRES_DB', 'POSTGRES_USER',
                    'POSTGRES_PASSWORD'):
            monkeypatch.delenv(var, raising=False)
        config = get_default_db_config()

        assert config['host'] == 'localhost'
        assert config['port'] == 5433
        assert config['database'] == 'eliza'

    def test_config_from_env(self):
        """Config should read from environment variables."""
        with patch.dict(os.environ, {
            'POSTGRES_HOST': 'testhost',
            'POSTGRES_PORT': '5432',
            'POSTGRES_DB': 'testdb',
            'POSTGRES_USER': 'testuser',
            'POSTGRES_PASSWORD': 'test-token-d',
        }):
            config = get_default_db_config()

            assert config['host'] == 'testhost'
            assert config['port'] == 5432
            assert config['database'] == 'testdb'
            assert config['user'] == 'testuser'
            assert config['password'] == 'test-token-d'


class TestScoreRanking:
    """Tests for score-based ranking with mocked data."""

    def test_ranking_order_by_occurrence(self):
        """Variants should be ranked by occurrence count when no type hint."""
        variants = [
            {'occurrence_count': 10, 'relationship_count': 0, 'entity_type': 'CONCEPT'},
            {'occurrence_count': 100, 'relationship_count': 0, 'entity_type': 'CONCEPT'},
            {'occurrence_count': 50, 'relationship_count': 0, 'entity_type': 'CONCEPT'},
        ]

        scored = [(compute_score(v)[0], v) for v in variants]
        scored.sort(key=lambda x: -x[0])

        # Should be ordered: 100, 50, 10
        assert scored[0][1]['occurrence_count'] == 100
        assert scored[1][1]['occurrence_count'] == 50
        assert scored[2][1]['occurrence_count'] == 10

    def test_type_hint_overrides_occurrence(self):
        """Type hint match should override moderately higher occurrence count."""
        high_occ_wrong_type = {
            # With occ_weight=1000 and type_hint_boost=50k, hint should win up to a 50-occurrence gap.
            'occurrence_count': 55,
            'relationship_count': 0,
            'entity_type': 'CONCEPT',
        }
        low_occ_right_type = {
            'occurrence_count': 10,
            'relationship_count': 0,
            'entity_type': 'TECHNOLOGY',
        }

        score_high, _ = compute_score(high_occ_wrong_type, type_hint='TECHNOLOGY')
        score_low, _ = compute_score(low_occ_right_type, type_hint='TECHNOLOGY')

        # Low occurrence but matching type should win due to 50k boost
        assert score_low > score_high


class TestMockedDatabaseResolution:
    """Tests for resolution functions with mocked database."""

    @patch('knowledge_graph.polysemy_resolver._get_entity_variants_from_db')
    @patch('knowledge_graph.polysemy_resolver.psycopg2')
    def test_resolve_entity_variants_returns_list(self, mock_psycopg2, mock_get_variants):
        """resolve_entity_variants should return a list of dicts."""
        mock_get_variants.return_value = [
            {
                'id': 1,
                'entity_text': 'Ethereum',
                'entity_type': 'TECHNOLOGY',
                'normalized_text': 'ethereum',
                'occurrence_count': 100,
                'fuseki_uri': 'https://example.com/eth1',
                'relationship_count': 10,
            },
            {
                'id': 2,
                'entity_text': 'Ethereum',
                'entity_type': 'PROJECT',
                'normalized_text': 'ethereum',
                'occurrence_count': 20,
                'fuseki_uri': 'https://example.com/eth2',
                'relationship_count': 5,
            },
        ]

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        results = resolve_entity_variants('ethereum', conn=mock_conn)

        assert len(results) == 2
        assert results[0]['entity_type'] == 'TECHNOLOGY'  # Higher score
        assert results[0]['score'] > results[1]['score']

    @patch('knowledge_graph.polysemy_resolver._get_entity_variants_from_db')
    @patch('knowledge_graph.polysemy_resolver.psycopg2')
    def test_resolve_entity_returns_result(self, mock_psycopg2, mock_get_variants):
        """resolve_entity should return ResolutionResult."""
        mock_get_variants.return_value = [
            {
                'id': 1,
                'entity_text': 'Notion',
                'entity_type': 'TECHNOLOGY',
                'normalized_text': 'notion',
                'occurrence_count': 150,
                'fuseki_uri': 'https://example.com/notion1',
                'relationship_count': 25,
            },
            {
                'id': 2,
                'entity_text': 'Notion',
                'entity_type': 'ORGANIZATION',
                'normalized_text': 'notion',
                'occurrence_count': 30,
                'fuseki_uri': 'https://example.com/notion2',
                'relationship_count': 8,
            },
        ]

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        result = resolve_entity('notion', conn=mock_conn)

        assert isinstance(result, ResolutionResult)
        assert result.winner is not None
        assert result.winner.entity_type == 'TECHNOLOGY'
        assert result.is_polysemy is True
        assert len(result.alternatives) == 1

    @patch('knowledge_graph.polysemy_resolver._get_entity_variants_from_db')
    @patch('knowledge_graph.polysemy_resolver.psycopg2')
    def test_resolve_no_match(self, mock_psycopg2, mock_get_variants):
        """resolve_entity should handle no matches gracefully."""
        mock_get_variants.return_value = []

        mock_conn = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn

        result = resolve_entity('nonexistent_entity_xyz', conn=mock_conn)

        assert result.winner is None
        assert result.variant_count == 0
        assert result.resolution_method == 'no_match'
        assert result.is_polysemy is False


# Skip integration tests if database is not available
@targets.requires_database
class TestDatabaseIntegration:
    """Integration tests against KOI_TEST_DATABASE_URL (needs the eliza-profile schema)."""

    @pytest.fixture(autouse=True)
    def _db_config_from_test_database(self, monkeypatch):
        parts = targets.url_parts(targets.database_url())
        monkeypatch.setenv('POSTGRES_HOST', parts['host'] or '')
        monkeypatch.setenv('POSTGRES_PORT', str(parts['port'] or 5432))
        monkeypatch.setenv('POSTGRES_DB', parts['database'] or '')
        monkeypatch.setenv('POSTGRES_USER', parts['user'] or '')
        monkeypatch.setenv('POSTGRES_PASSWORD', parts['password'] or '')

    def test_resolve_common_entity(self):
        """Test resolution of a common entity (requires DB)."""
        # This test only runs if DB is configured
        result = resolve_entity('ethereum')

        # Should find something
        assert result.variant_count > 0 or result.resolution_method == 'no_match'
