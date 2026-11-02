"""Tests for alias normalization (plan alias-normalization-fix).

Pins the invariant that every alias-write path stores `normalize_alias`-form
values. Pure unit + AST tests (no app import, no DB) so they run in CI. The
_apply_entity upsert path and the /entities/merge path are proven by one-shot
integration checks during deploy (AC3 / AC5), not here, since they need a live
DB + event loop.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

from api.resolution_primitives import normalize_alias, normalize_alias_list


# --- Unit: normalize_alias_list --------------------------------------------

def test_normalize_alias_list_lowercases_strips_dedupes():
    got = normalize_alias_list(
        ["Greg Landua", "[[People/Foo|Foo]]", "Org/Bar", "greg landua", "  Spaced  ", "BAZ"]
    )
    assert got == ["greg landua", "foo", "bar", "spaced", "baz"]


def test_normalize_alias_list_is_idempotent():
    once = normalize_alias_list(["Greg Landua", "[[X|Y]]", "A/B"])
    twice = normalize_alias_list(once)
    assert once == twice
    assert all(a == normalize_alias(a) for a in once)


def test_normalize_alias_list_handles_none_and_empty():
    # Regression: None must be dropped, NOT stringified to "none"
    # (normalize_alias does str(x) internally).
    assert normalize_alias_list([None, "", "  ", None]) == []
    assert normalize_alias_list(None) == []
    assert normalize_alias_list("Solo Caps") == ["solo caps"]
    assert "none" not in normalize_alias_list(["Real", None])


def test_normalize_alias_list_preserves_first_seen_order():
    assert normalize_alias_list(["B", "a", "b", "A"]) == ["b", "a"]


# --- Drift guard: the two normalize_alias copies must stay identical --------
# AST source extraction — NO app import (importing personal_ingest_api triggers
# FastAPI app side effects).

def _extract_func_logic(py_path: Path, func_name: str) -> str:
    """Return the function's executable logic as normalized source, with the
    leading docstring stripped (docstring wording — e.g. ascii `->` vs unicode
    `→` — is not logic drift and must not fail the guard)."""
    tree = ast.parse(py_path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(getattr(body[0], "value", None), ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                body = body[1:]  # drop docstring
            return ast.unparse(ast.Module(body=body, type_ignores=[]))
    raise AssertionError(f"{func_name} not found in {py_path}")


def test_normalize_alias_definitions_do_not_drift():
    a = _extract_func_logic(ROOT / "api" / "resolution_primitives.py", "normalize_alias")
    b = _extract_func_logic(ROOT / "api" / "personal_ingest_api.py", "normalize_alias")
    assert a == b, (
        "normalize_alias LOGIC has drifted between resolution_primitives.py and "
        "personal_ingest_api.py — keep them behaviorally identical or consolidate to one."
    )


# --- Fixture table: pin normalize_alias output shape -----------------------

def test_normalize_alias_fixture_table():
    cases = {
        "Greg Landua": "greg landua",
        "[[People/Name|Display]]": "name",
        "Organizations/Acme": "acme",
        "  Trimmed  ": "trimmed",
        "Already lower": "already lower",
        "MixedCASE": "mixedcase",
    }
    for raw, expected in cases.items():
        assert normalize_alias(raw) == expected, raw
