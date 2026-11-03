"""Tests for community label generation and cohesion calculation.

Pure-function tests — no database required.
"""

import pytest
from collections import Counter


def generate_label(file_paths: list) -> str:
    """Generate a heuristic label from the most common parent directory.

    Mirrors the logic in scripts/community_detection.py.
    """
    if not file_paths:
        return "unknown"

    # Extract parent directories
    parents = []
    for fp in file_paths:
        parts = fp.strip('/').split('/')
        if len(parts) > 1:
            parents.append(parts[0])
        else:
            parents.append('root')

    # Find most common parent
    counter = Counter(parents)
    most_common = counter.most_common(1)[0][0]

    # If the most common parent covers less than 40% of members,
    # try one level deeper for more specificity
    if counter[most_common] / len(file_paths) < 0.4 and len(file_paths) > 3:
        deeper_parents = []
        for fp in file_paths:
            parts = fp.strip('/').split('/')
            if len(parts) > 2:
                deeper_parents.append('/'.join(parts[:2]))
            elif len(parts) > 1:
                deeper_parents.append(parts[0])
            else:
                deeper_parents.append('root')
        deeper_counter = Counter(deeper_parents)
        deeper_common = deeper_counter.most_common(1)[0][0]
        if deeper_counter[deeper_common] / len(file_paths) >= 0.3:
            return deeper_common

    return most_common


def calculate_cohesion(member_count: int, internal_edges: int) -> float:
    """Calculate community cohesion (internal edge density).

    Mirrors the logic in scripts/community_detection.py.
    """
    if member_count < 2:
        return 0.0
    max_possible = member_count * (member_count - 1) / 2
    if max_possible == 0:
        return 0.0
    return min(internal_edges / max_possible, 1.0)


def filter_singletons(communities: list) -> list:
    """Filter out singleton communities (<2 members).

    Mirrors the logic in scripts/community_detection.py.
    """
    return [c for c in communities if len(c.get('members', [])) >= 2]


# --- Tests ---

class TestLabelGeneration:
    """Test heuristic label generation from file paths."""

    def test_single_directory_dominates(self):
        paths = ['api/auth.py', 'api/users.py', 'api/routes.py', 'utils/helper.py']
        assert generate_label(paths) == 'api'

    def test_root_files(self):
        paths = ['main.py', 'setup.py', 'config.py']
        assert generate_label(paths) == 'root'

    def test_mixed_directories_picks_majority(self):
        paths = ['scripts/extract.py', 'scripts/load.py', 'api/serve.py']
        assert generate_label(paths) == 'scripts'

    def test_empty_paths(self):
        assert generate_label([]) == 'unknown'

    def test_single_file(self):
        paths = ['src/main.py']
        assert generate_label(paths) == 'src'

    def test_deeper_path_when_scattered(self):
        # When top-level is too scattered, should try deeper
        paths = [
            'api/v1/auth.py', 'api/v1/users.py',
            'scripts/load.py', 'tests/test_api.py',
            'utils/helpers.py', 'config/settings.py',
        ]
        label = generate_label(paths)
        # 'api' covers 2/6 = 33% < 40%, but api/v1 covers 2/6 = 33% >= 30%
        assert label in ('api', 'api/v1')

    def test_uniform_directory(self):
        paths = ['keeper/msg_server.go', 'keeper/query_server.go',
                 'keeper/keeper.go', 'keeper/invariants.go']
        assert generate_label(paths) == 'keeper'


class TestCohesionCalculation:
    """Test internal edge density calculation."""

    def test_fully_connected(self):
        # 4 members, 6 edges = fully connected
        assert calculate_cohesion(4, 6) == pytest.approx(1.0)

    def test_no_internal_edges(self):
        assert calculate_cohesion(4, 0) == pytest.approx(0.0)

    def test_half_connected(self):
        # 4 members, 3 of 6 possible edges
        assert calculate_cohesion(4, 3) == pytest.approx(0.5)

    def test_single_member(self):
        # Singleton should return 0
        assert calculate_cohesion(1, 0) == pytest.approx(0.0)

    def test_two_members_connected(self):
        # 2 members, 1 edge = fully connected
        assert calculate_cohesion(2, 1) == pytest.approx(1.0)

    def test_two_members_disconnected(self):
        assert calculate_cohesion(2, 0) == pytest.approx(0.0)

    def test_clamp_to_one(self):
        # Edge count exceeding theoretical max (shouldn't happen, but clamp)
        assert calculate_cohesion(3, 10) == pytest.approx(1.0)

    def test_large_community(self):
        # 10 members, 15 edges, max possible = 45
        assert calculate_cohesion(10, 15) == pytest.approx(15 / 45)


class TestSingletonFiltering:
    """Test filtering of singleton communities."""

    def test_filter_singletons(self):
        communities = [
            {'name': 'api', 'members': ['a', 'b', 'c']},
            {'name': 'orphan', 'members': ['x']},
            {'name': 'utils', 'members': ['d', 'e']},
        ]
        filtered = filter_singletons(communities)
        assert len(filtered) == 2
        assert filtered[0]['name'] == 'api'
        assert filtered[1]['name'] == 'utils'

    def test_all_singletons(self):
        communities = [
            {'name': 'a', 'members': ['x']},
            {'name': 'b', 'members': ['y']},
        ]
        assert filter_singletons(communities) == []

    def test_no_singletons(self):
        communities = [
            {'name': 'big', 'members': ['a', 'b', 'c', 'd']},
        ]
        filtered = filter_singletons(communities)
        assert len(filtered) == 1

    def test_empty_members(self):
        communities = [
            {'name': 'empty', 'members': []},
            {'name': 'valid', 'members': ['a', 'b']},
        ]
        filtered = filter_singletons(communities)
        assert len(filtered) == 1
        assert filtered[0]['name'] == 'valid'
