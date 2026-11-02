from datetime import datetime, timezone

import pytest

from api.routers.commitment_router import (
    RoutingSuggestionRequest,
    _draft_level_routing_diagnostics,
    _score_pools,
)


class _FakeConn:
    def __init__(self, pool_rows):
        self.pool_rows = pool_rows

    async def fetch(self, query, *args):
        if "FROM commitment_pools" in query:
            return self.pool_rows
        return []

    async def fetchval(self, query, *args):
        return None


class _FakeAcquire:
    def __init__(self, pool_rows):
        self.conn = _FakeConn(pool_rows)

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, pool_rows):
        self.pool_rows = pool_rows

    def acquire(self):
        return _FakeAcquire(self.pool_rows)


def test_draft_level_diagnostics_distinguish_input_gaps_from_score_degradation():
    draft = RoutingSuggestionRequest(metadata={})

    diagnostics = _draft_level_routing_diagnostics(draft)
    by_code = {d.code: d for d in diagnostics}

    assert by_code["missing_bioregion_uri"].kind == "input_gap"
    assert by_code["missing_routing_tags"].kind == "input_gap"
    assert by_code["missing_estimated_value"].kind == "score_degradation"
    assert by_code["missing_timeframe"].kind == "score_degradation"


@pytest.mark.asyncio
async def test_pool_diagnostics_emit_hard_excludes_without_default_governance_noise():
    pool = _FakePool([
        {
            "pool_rid": "orn:koi-net.commitment-pool:test",
            "name": "Test Pool",
            "bioregion_uri": "orn:koi-net.bioregion:victoria",
            "metadata": {
                "need_tags": ["restoration"],
                "capacity_usd": 1000,
                "remaining_capacity_usd": 0,
            },
        }
    ])
    draft = RoutingSuggestionRequest(
        validity_start=datetime(2026, 4, 1, tzinfo=timezone.utc),
        validity_end=datetime(2026, 9, 30, tzinfo=timezone.utc),
        metadata={
            "bioregion_uri": "orn:koi-net.bioregion:victoria",
            "routing_tags": ["restoration"],
            "estimated_value_usd": 500,
        },
    )

    suggestions = await _score_pools(pool, draft)

    assert len(suggestions) == 1
    suggestion = suggestions[0]
    by_code = {d.code: d for d in suggestion.diagnostics}
    assert suggestion.hard_excludes == ["no_capacity"]
    assert by_code["no_capacity"].kind == "hard_exclude"
    assert "governance_unscored" not in by_code


@pytest.mark.asyncio
async def test_no_score_factors_matched_is_review_gap_not_hard_exclude():
    pool = _FakePool([
        {
            "pool_rid": "orn:koi-net.commitment-pool:test",
            "name": "Test Pool",
            "bioregion_uri": "orn:koi-net.bioregion:cascadia",
            "metadata": {
                "need_tags": ["equipment"],
                "capacity_usd": 0,
                "remaining_capacity_usd": 0,
            },
        }
    ])
    draft = RoutingSuggestionRequest(
        metadata={
            "bioregion_uri": "orn:koi-net.bioregion:victoria",
            "routing_tags": ["restoration"],
        },
    )

    suggestions = await _score_pools(pool, draft)

    assert len(suggestions) == 1
    suggestion = suggestions[0]
    by_code = {d.code: d for d in suggestion.diagnostics}
    assert suggestion.total_score == 0
    assert suggestion.hard_excludes == []
    assert by_code["no_score_factors_matched"].kind == "review_gap"


@pytest.mark.asyncio
async def test_governance_unscored_only_fires_when_governance_fields_present():
    pool = _FakePool([
        {
            "pool_rid": "orn:koi-net.commitment-pool:test",
            "name": "Test Pool",
            "bioregion_uri": "orn:koi-net.bioregion:victoria",
            "metadata": {
                "need_tags": ["restoration"],
                "governance_membrane": "steward-review",
            },
        }
    ])
    draft = RoutingSuggestionRequest(
        metadata={
            "bioregion_uri": "orn:koi-net.bioregion:victoria",
            "routing_tags": ["restoration"],
        },
    )

    suggestions = await _score_pools(pool, draft)

    assert len(suggestions) == 1
    by_code = {d.code: d for d in suggestions[0].diagnostics}
    assert by_code["governance_unscored"].kind == "review_gap"
