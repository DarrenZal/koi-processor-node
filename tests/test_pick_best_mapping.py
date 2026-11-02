"""Pure-unit tests for _pick_best_mapping — duplicate entity_rid_mappings (2026-07-16).

No DB, no embeddings. Exercises the tiebreak that decides which vault note wins
when several entity_rid_mappings rows share one canonical_uri.

Why this exists — the Mabs regression:

    canonical person-mabel-patel-0a1b2c3d4e5f had TWO mappings:
      id 1001  Notes/Person/mabs           -> People/Mabs.md          (2026-03-13)
      id 1002  Notes/Person/mabel-patel  -> People/Mabel Patel.md (2026-07-16)

    People/Mabs.md was a duplicate that had been deleted and then restored by
    Obsidian Sync from a stale device. _annotate_vault_fields used a bare
    `LIMIT 1` with no ORDER BY, so Postgres returned the older row and every
    resolve_entity('Mabel Patel') reported the loser's path — which meant
    /process-note --propagate wrote mentionedIn into the wrong note.

Duplicates are created by /register-entity itself: it upserts
`ON CONFLICT (vault_rid)`, so registering a second note for an existing
canonical INSERTs a new row instead of repointing the old one.
"""

import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from api import personal_ingest_api as api


def row(vault_path, name, canonical_name):
    return {"vault_path": vault_path, "name": name, "canonical_name": canonical_name}


@pytest.fixture
def only_survivor_on_disk(monkeypatch):
    """People/Mabel Patel.md exists; the restored duplicate does not."""
    monkeypatch.setattr(
        api, "_vault_note_exists", lambda p: p == "People/Mabel Patel.md"
    )


@pytest.fixture
def both_on_disk(monkeypatch):
    """The sync-restored duplicate is back on disk alongside the survivor."""
    monkeypatch.setattr(api, "_vault_note_exists", lambda p: True)


def test_empty_rows_returns_none():
    assert api._pick_best_mapping([]) is None
    assert api._pick_best_mapping(None) is None


def test_single_row_wins_even_if_missing_on_disk(monkeypatch):
    """A lone mapping is still the answer — vault_note_exists reports the gap."""
    monkeypatch.setattr(api, "_vault_note_exists", lambda p: False)
    rows = [row("People/Ghost.md", "Ghost", "Ghost")]
    assert api._pick_best_mapping(rows) == "People/Ghost.md"


def test_prefers_note_that_exists_on_disk(only_survivor_on_disk):
    """Existence beats recency: caller orders newest-first, loser is listed first."""
    rows = [
        row("People/Mabs.md", "Mabs", "Mabel Patel"),
        row("People/Mabel Patel.md", "Mabel Patel", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Mabel Patel.md"


def test_mabs_regression_both_on_disk_name_match_wins(both_on_disk):
    """THE regression: duplicate restored by sync, both files present.

    Existence can't separate them, so the mapping whose name matches the
    canonical entity name must win.
    """
    rows = [
        row("People/Mabs.md", "Mabs", "Mabel Patel"),
        row("People/Mabel Patel.md", "Mabel Patel", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Mabel Patel.md"


def test_name_match_is_normalized(both_on_disk):
    """Case/whitespace differences must not defeat the name match."""
    rows = [
        row("People/Mabs.md", "Mabs", "Mabel Patel"),
        row("People/Mabel Patel.md", "  mabel   PATEL ", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Mabel Patel.md"


def test_existence_outranks_name_match(monkeypatch):
    """A name-matching mapping to a deleted file loses to a live one.

    Otherwise resolution points at a path that isn't there — the phantom class.
    """
    monkeypatch.setattr(api, "_vault_note_exists", lambda p: p == "People/Mabs.md")
    rows = [
        row("People/Mabel Patel.md", "Mabel Patel", "Mabel Patel"),
        row("People/Mabs.md", "Mabs", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Mabs.md"


def test_recency_breaks_ties_when_nothing_else_separates(both_on_disk):
    """Neither name matches the canonical → first row (newest) wins."""
    rows = [
        row("People/Newest.md", "Nope", "Mabel Patel"),
        row("People/Older.md", "Also Nope", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Newest.md"


def test_vault_unmounted_is_fail_safe(monkeypatch):
    """_vault_note_exists returns True for everything when the vault is absent.

    The ranking must degrade to name-match, not discard every candidate.
    """
    monkeypatch.setattr(api, "_vault_note_exists", lambda p: True)
    rows = [
        row("People/Mabs.md", "Mabs", "Mabel Patel"),
        row("People/Mabel Patel.md", "Mabel Patel", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Mabel Patel.md"


def test_missing_canonical_name_does_not_crash(both_on_disk):
    """LEFT JOIN can yield canonical_name=None; must not raise."""
    rows = [
        row("People/A.md", "A", None),
        row("People/B.md", "B", None),
    ]
    assert api._pick_best_mapping(rows) == "People/A.md"


def test_missing_mapping_name_does_not_crash(both_on_disk):
    rows = [
        row("People/A.md", None, "Mabel Patel"),
        row("People/Mabel Patel.md", "Mabel Patel", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows) == "People/Mabel Patel.md"


# --- expected_folder: same-name cross-type pairs -----------------------------
#
# Caught by measuring the fix against live data before shipping it: ranking on
# (exists, name) alone TIED these pairs — both notes match the name — and fell
# through to recency, which silently flipped Regenerate Cascadia from
# Organizations/ (the survivor the 2026-06-30 consolidation chose) to Projects/.
# The entity's own type is the tiebreak.


def test_folder_match_beats_recency_regenerate_cascadia(both_on_disk):
    """Regression: Organization entity must resolve to Organizations/.

    Both notes name-match, and Projects/ is the more recently synced row (listed
    first), so without expected_folder recency wins and picks the wrong note.
    """
    rows = [
        row("Projects/Regenerate Cascadia.md", "Regenerate Cascadia", "Regenerate Cascadia"),
        row("Organizations/Regenerate Cascadia.md", "Regenerate Cascadia", "Regenerate Cascadia"),
    ]
    assert (
        api._pick_best_mapping(rows, expected_folder="Organizations")
        == "Organizations/Regenerate Cascadia.md"
    )


def test_folder_match_koi_project_not_the_unrelated_person(both_on_disk):
    """KOI (Project) must not resolve to People/Koi.md — a person in Brazil.

    The live registry had all three mapped to canonical project-koi-c6f39042f013.
    """
    rows = [
        row("People/Koi.md", "Koi", "KOI"),
        row("Concepts/KOI.md", "KOI", "KOI"),
        row("Projects/KOI.md", "KOI", "KOI"),
    ]
    assert (
        api._pick_best_mapping(rows, expected_folder="Projects") == "Projects/KOI.md"
    )


def test_folder_match_pkols_organization(both_on_disk):
    rows = [
        row("Locations/PKOLS.md", "PKOLS", "PKOLS"),
        row("Organizations/PKOLS.md", "PKOLS", "PKOLS"),
    ]
    assert (
        api._pick_best_mapping(rows, expected_folder="Organizations")
        == "Organizations/PKOLS.md"
    )


def test_expected_folder_none_falls_back_to_name_match(both_on_disk):
    """No type hint → skip the folder signal, don't crash or discard rows."""
    rows = [
        row("People/Mabs.md", "Mabs", "Mabel Patel"),
        row("People/Mabel Patel.md", "Mabel Patel", "Mabel Patel"),
    ]
    assert api._pick_best_mapping(rows, expected_folder=None) == "People/Mabel Patel.md"


def test_folder_prefix_is_not_a_substring_match(both_on_disk):
    """'Projects' must not match 'ProjectsArchive/...'; only a real path segment."""
    rows = [
        row("ProjectsArchive/Thing.md", "Thing", "Thing"),
        row("Projects/Thing.md", "Thing", "Thing"),
    ]
    assert api._pick_best_mapping(rows, expected_folder="Projects") == "Projects/Thing.md"


def test_existence_still_outranks_folder_match(monkeypatch):
    """A type-correct mapping to a deleted file loses to a live one."""
    monkeypatch.setattr(
        api, "_vault_note_exists", lambda p: p == "Organizations/Kelpwood.md"
    )
    rows = [
        row("Projects/Kelpwood.md", "Kelpwood", "Kelpwood"),
        row("Organizations/Kelpwood.md", "Kelpwood", "Kelpwood"),
    ]
    assert (
        api._pick_best_mapping(rows, expected_folder="Projects")
        == "Organizations/Kelpwood.md"
    )
