"""Unit tests for ingest_spec_dag.py pure functions.

Tests validate_doc_dag and collect_docs without any database dependency.
Uses tmp_path for filesystem tests and inline DocNode construction for validation.

Run:  pytest tests/test_ingest_spec_dag.py -v
"""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.ingest_spec_dag import DocNode, collect_docs, validate_doc_dag


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_doc(tmp_path: Path, rel_path: str, frontmatter: str) -> None:
    """Write a markdown file with YAML frontmatter under tmp_path."""
    p = tmp_path / rel_path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f"---\n{frontmatter}\n---\n\n# Content\n")


def _make_nodes(specs: list[tuple[str, str, list[str]]]) -> dict[str, DocNode]:
    """Build a nodes dict from (doc_id, doc_kind, depends_on) tuples."""
    return {
        doc_id: DocNode(
            doc_id=doc_id,
            doc_kind=doc_kind,
            status="active",
            depends_on=deps,
            relates_to=[],
            file_path=f"{doc_id.replace('.', '/')}.md",
        )
        for doc_id, doc_kind, deps in specs
    }


# ---------------------------------------------------------------------------
# collect_docs tests
# ---------------------------------------------------------------------------

@pytest.mark.core
def test_collect_docs_from_fixture(tmp_path):
    """3 .md files with valid frontmatter → 3 DocNode objects."""
    _write_doc(tmp_path, "project-vision.md",
               'doc_id: tp.project-vision\ndoc_kind: vision\nstatus: active\ndepends_on: []')
    _write_doc(tmp_path, "foundations/arch.md",
               'doc_id: tp.arch\ndoc_kind: foundation\nstatus: active\ndepends_on:\n  - tp.project-vision')
    _write_doc(tmp_path, "specs/feat.md",
               'doc_id: tp.feat\ndoc_kind: spec\nstatus: active\ndepends_on:\n  - tp.arch')

    nodes, unclassified = collect_docs(tmp_path)

    assert len(nodes) == 3
    assert "tp.project-vision" in nodes
    assert "tp.arch" in nodes
    assert "tp.feat" in nodes
    assert nodes["tp.arch"].depends_on == ["tp.project-vision"]
    assert len(unclassified) == 0


@pytest.mark.core
def test_collect_docs_skips_meta_directory(tmp_path):
    """Files under _meta/ are excluded."""
    _write_doc(tmp_path, "project-vision.md",
               'doc_id: tp.project-vision\ndoc_kind: vision\nstatus: active\ndepends_on: []')
    _write_doc(tmp_path, "_meta/project.json.md",
               'doc_id: tp.meta\ndoc_kind: spec\nstatus: active\ndepends_on: []')

    nodes, unclassified = collect_docs(tmp_path)

    assert len(nodes) == 1
    assert "tp.project-vision" in nodes
    assert "tp.meta" not in nodes


# ---------------------------------------------------------------------------
# validate_doc_dag tests
# ---------------------------------------------------------------------------

@pytest.mark.core
def test_validate_dag_passes_clean_dag():
    """Vision root + 2 specs → 0 errors, 0 external_refs."""
    nodes = _make_nodes([
        ("tp.project-vision", "vision", []),
        ("tp.arch", "foundation", ["tp.project-vision"]),
        ("tp.feat", "spec", ["tp.arch"]),
    ])

    errors, external_refs = validate_doc_dag(nodes, "tp")

    assert errors == []
    assert external_refs == []


@pytest.mark.core
def test_validate_dag_cross_project_ref():
    """depends_on with a different project prefix → external_refs populated, 0 errors."""
    nodes = _make_nodes([
        ("tp.project-vision", "vision", []),
        ("tp.alignment", "spec", ["tp.project-vision", "other.some-spec"]),
    ])

    errors, external_refs = validate_doc_dag(nodes, "tp")

    assert errors == []
    assert len(external_refs) == 1
    assert external_refs[0] == ("tp.alignment", "other.some-spec")


@pytest.mark.core
def test_validate_dag_detects_cycle():
    """A → B → A → cycle error."""
    nodes = _make_nodes([
        ("tp.project-vision", "vision", []),
        ("tp.a", "spec", ["tp.project-vision", "tp.b"]),
        ("tp.b", "spec", ["tp.a"]),
    ])

    errors, external_refs = validate_doc_dag(nodes, "tp")

    assert any("Cycle" in e or "cycle" in e.lower() for e in errors)


@pytest.mark.core
def test_validate_dag_missing_vision_root():
    """No vision doc → error."""
    nodes = _make_nodes([
        ("tp.arch", "foundation", []),
        ("tp.feat", "spec", ["tp.arch"]),
    ])

    errors, external_refs = validate_doc_dag(nodes, "tp")

    assert any("vision" in e.lower() for e in errors)


@pytest.mark.core
def test_validate_dag_bad_doc_kind():
    """doc_kind: 'bogus' → error."""
    nodes = _make_nodes([
        ("tp.project-vision", "vision", []),
        ("tp.bad", "bogus", ["tp.project-vision"]),
    ])

    errors, external_refs = validate_doc_dag(nodes, "tp")

    assert any("bogus" in e for e in errors)
