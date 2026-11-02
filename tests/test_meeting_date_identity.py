"""Two meetings on different dates must not resolve to the same entity.

THE DEFECT THIS PINS
--------------------
In `entity_rid_mappings`, most Meeting-typed rows had collapsed onto a few
canonical URIs: a meeting note lost its identity into an earlier meeting of the
same series, and `attended` edges pointed at a meeting whose date disagreed with
their source note. The worst case was cross-series: a note like
`Meetings/2025-03-06 Fernhill Meeting.md` was the `source_rid` of edges pointing
at an unrelated *2025-03-04 Ilsa Brandt Meeting*.

Cause: `normalize_entity_text` does `.replace('-', ' ')`, so `2025-03-04`
becomes three ordinary tokens. The one field that distinguishes two meetings in
a series is thereby converted into tokens that INFLATE the Jaro-Winkler prefix
bonus and count toward token overlap — the discriminator became the thing
making them look alike. The collision warning that would have surfaced it is
disabled for Meeting by name (`suppress_types = {'Meeting', 'Task'}`).

WHY NOT `passes_distinctive_token_check`
----------------------------------------
Adding "Meeting" to that guard's type tuple was the obvious fix and IT DOES NOT
WORK — verified before writing this. That check catches one name being a
qualified EXTENSION of another ("DWeb Camp Cascadia" vs "DWeb Camp"); two
titles differing only by date have distinct tokens on both sides, so it never
fires. It accepts all three collapse pairs below. `test_distinctive_token_check_
would_not_have_caught_this` pins that, so nobody re-proposes the one-liner.
"""

from __future__ import annotations

import pytest

from api.personal_ingest_api import (
    _extract_leading_date,
    normalize_entity_text,
)
from api.resolution_primitives import (
    passes_distinctive_token_check,
    passes_token_overlap_strict,
)


# Invented pairs with the shapes of the collapsed ones: a three-word series, a
# two-word series across a year boundary, and two unrelated meetings two days apart.
COLLAPSE_PAIRS = [
    ("2025-03-04 Riverbend Watershed Council Meeting",
     "2025-03-18 Riverbend Watershed Council Meeting", "same series, different date"),
    ("2024-12-10 Garden Guild Meeting",
     "2025-01-14 Garden Guild Meeting", "same series, different date"),
    ("2025-03-06 Fernhill Meeting",
     "2025-03-04 Ilsa Brandt Meeting", "unrelated meetings, wrong edges"),
]


@pytest.mark.parametrize("a,b,label", COLLAPSE_PAIRS)
def test_different_dates_do_not_merge(a, b, label):
    assert not passes_token_overlap_strict(
        normalize_entity_text(a), normalize_entity_text(b), "Meeting"
    ), f"{label}: these are different meetings and must not merge"


def test_the_same_meeting_still_merges():
    """The accept-case. A guard that rejects everything also passes a
    rejection-only suite, which is why this test is not optional."""
    t = normalize_entity_text("2025-03-04 Ilsa Brandt Meeting")
    assert passes_token_overlap_strict(t, t, "Meeting")


def test_same_date_different_titles_falls_through():
    """The guard keys on DATE disagreement only. Same date, different subject
    must reach the normal matching logic rather than being auto-accepted or
    auto-rejected by this guard."""
    a = normalize_entity_text("2025-03-04 Alpha Meeting")
    b = normalize_entity_text("2025-03-04 Beta Meeting")
    assert passes_token_overlap_strict(a, b, "Meeting")


@pytest.mark.parametrize("a,b,label", COLLAPSE_PAIRS)
def test_guard_is_scoped_to_meeting(a, b, label):
    """Non-Meeting types must be unaffected. Without this, a regression that
    applied the date guard everywhere would look like a pass above."""
    na, nb = normalize_entity_text(a), normalize_entity_text(b)
    assert passes_token_overlap_strict(na, nb, "Concept"), (
        "the date guard leaked into Concept resolution"
    )


@pytest.mark.parametrize("a,b,label", COLLAPSE_PAIRS)
def test_distinctive_token_check_would_not_have_caught_this(a, b, label):
    """Pins WHY Meeting is not simply added to the distinctive-token tuple.

    If this ever starts failing, that guard's semantics changed and the date
    guard should be re-evaluated — not left in place by inertia.
    """
    assert passes_distinctive_token_check(
        normalize_entity_text(a), normalize_entity_text(b)
    ), "passes_distinctive_token_check now rejects this; re-evaluate the date guard"


@pytest.mark.parametrize("text,expected", [
    ("2025-03-04 Ilsa Brandt Meeting", "2025-03-04"),
    ("2025 03 04 ilsa brandt meeting", "2025-03-04"),   # post-normalization form
    ("Watershed Gathering", None),
    ("2026 budget review", None),                      # year alone is not a date
    ("2026-13-45 Bad Date Meeting", None),             # impossible month/day
    ("", None),
    (None, None),
])
def test_extract_leading_date(text, expected):
    assert _extract_leading_date(text) == expected


def test_absent_date_never_rejects():
    """Fall through on absence, reject only on disagreement.

    A Meeting pair where neither side carries a date must not be rejected BY
    THIS GUARD. Asserted against a same-type baseline rather than an absolute,
    because the surrounding logic may legitimately reject for other reasons.
    """
    a, b = normalize_entity_text("Project Sync"), normalize_entity_text("Project Standup")
    assert passes_token_overlap_strict(a, b, "Meeting") == passes_token_overlap_strict(a, b, "Concept")


def test_one_sided_date_behaves_identically_across_types():
    """When only one side has a date the guard cannot fire, so Meeting must
    behave exactly like every other type. This isolates the guard from
    pre-existing rejection logic — the check that stopped me misreading an
    unrelated rejection as my own bug."""
    a, b = normalize_entity_text("2025-03-04 Sync"), normalize_entity_text("Sync")
    results = {t: passes_token_overlap_strict(a, b, t)
               for t in ("Meeting", "Concept", "Project", "Person")}
    assert len(set(results.values())) == 1, f"Meeting diverged from its peers: {results}"
