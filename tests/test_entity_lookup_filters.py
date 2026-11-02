"""The /chat entity ANN must return only LIVE entities, on both paths.

Two defects this pins, both found 2026-08-16:

1. Tombstones were in the candidate pool. /entities/merge tombstones via
   merged_into rather than deleting, and deliberately keeps embedding_3072, so
   every merge ever performed added a decoy to the ANN. Measured before the fix:
   202 tombstones in the pool, 167 sharing a normalized_text with a live,
   also-embedded survivor. "Polis" had two dead rows (Concept and Project) both
   merged into the live "Pol.is", so the model was handed one entity three times
   and could cite the dead fuseki_uri.

2. The first fix defined the filter as a LOCAL in entity_lookup, leaving the name
   unresolvable in _keyword_entity_search. That is the fallback path, reached only
   when vector search has already failed, so the NameError would have turned a
   soft degradation into a hard one and only in an already-bad state.

These are SQL-shape tests on purpose: the queries are f-strings, so a dropped
filter is a text change and can be caught without a database.

Run: ~/venvs/koi-server/bin/python -m pytest tests/test_entity_lookup_filters.py -v
"""
from __future__ import annotations

import inspect
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api import retrieval_executors as rx  # noqa: E402


def _sql_blocks(fn) -> list[str]:
    """Every f-string SELECT ... FROM entity_registry in a function's source."""
    src = inspect.getsource(fn)
    return [m.group(0) for m in
            re.finditer(r'"""\s*SELECT.*?FROM entity_registry.*?"""', src, re.S)]


def test_live_filter_is_module_level_not_a_local():
    """Regression: a local here is invisible to _keyword_entity_search."""
    assert hasattr(rx, "LIVE_FILTER"), (
        "LIVE_FILTER must be module-level; _keyword_entity_search is a separate "
        "function and cannot see a local defined in entity_lookup")
    assert "merged_into IS NULL" in rx.LIVE_FILTER


@pytest.mark.parametrize("fn_name", ["entity_lookup", "_keyword_entity_search"])
def test_both_entity_query_paths_exclude_tombstones(fn_name):
    """The vector path AND the keyword fallback. The fallback is the one that got
    missed, and it is the one that runs when things are already going wrong."""
    fn = getattr(rx, fn_name)
    blocks = _sql_blocks(fn)
    assert blocks, f"no entity_registry SELECT found in {fn_name}"
    for b in blocks:
        assert "{LIVE_FILTER}" in b, (
            f"{fn_name} queries entity_registry without interpolating LIVE_FILTER; "
            f"tombstoned duplicates re-enter the candidate pool")


def test_the_keyword_fallback_can_actually_resolve_the_name():
    """The NameError that the first fix would have shipped.

    Compiling the f-string is not enough: the name must be resolvable from that
    function's scope. Checking the module globals is what distinguishes a local
    from a module constant.
    """
    src = inspect.getsource(rx._keyword_entity_search)
    assert "{LIVE_FILTER}" in src
    assert "LIVE_FILTER" in vars(rx), (
        "referenced in _keyword_entity_search but not resolvable from module scope")
    assert "LIVE_FILTER" not in rx._keyword_entity_search.__code__.co_varnames


def test_roadmap_seed_query_also_excludes_tombstones():
    """Same table, different call site. A merged WorkItem is not a work item."""
    src = inspect.getsource(rx)
    m = re.search(r"FROM entity_registry er\b.*?ORDER BY", src, re.S)
    assert m, "roadmap seed query not found"
    assert "er.merged_into IS NULL" in m.group(0)


def test_chat_exclude_types_still_carries_the_noise_types():
    """Guard the earlier fix while we are in here."""
    for t in ("Document", "Event"):
        assert t in rx.CHAT_EXCLUDE_TYPES
