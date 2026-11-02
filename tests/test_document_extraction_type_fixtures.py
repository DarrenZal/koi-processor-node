"""Fixtures for the three mis-types issue #68 was filed about.

Each fixture is a per-window extraction payload shaped exactly as the extractor
emits one. They are run through the real schema validator and the real
`merge_extractions`, not through a stand-in, because the defect these cover lived
in the seam between the schema and the merge and a stand-in for either would have
hidden it.

The three cases, from the 2026-09-14 michaelgarfield extraction:

  DWeb Berlin           a dated five-day gathering, emitted as Organization
                        because no schema admitted `Event`, then bound to a
                        Project. Retyped and merged by the operator (merge 326/327).

  The End of Hoop       named essays, emitted as Project because no schema
  Jumping / How To      admitted `Document`, then fuzzily collapsed into unrelated
  Think About Science   Project entities.

  Freedom               a website-blocking app that shares its name with the
                        abstract concept the same corpus discusses. Bound by EXACT
                        match to the Concept.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import document_extraction_contract as contract  # noqa: E402
from api import ingest_identity as ident  # noqa: E402
from api.resolution_primitives import normalize_entity_text  # noqa: E402

V1_SCHEMA = json.loads(
    (REPO_ROOT / "scripts/schemas/deep_extraction_doc_v1.schema.json").read_text())
V2_SCHEMA = json.loads(
    (REPO_ROOT / "scripts/schemas/deep_extraction_doc_v2.schema.json").read_text())

#: The enum as it stood before #68, used as a positive control: every fixture that
#: matters here must be REJECTED by it, or the fixture is not exercising the fix.
LEGACY_ENUM = ["Person", "Organization", "Project", "Concept",
               "Location", "Protocol", "CaseStudy"]


def _legacy_schema() -> dict:
    s = json.loads(json.dumps(V2_SCHEMA))
    s["properties"]["entities"]["items"]["properties"]["type"]["enum"] = LEGACY_ENUM
    return s


def _extractor():
    """Import the extractor by path — `scripts/` is not a package on sys.path."""
    spec = importlib.util.spec_from_file_location(
        "_extract_deep_documents", REPO_ROOT / "scripts/extract_deep_documents.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXTRACTOR = _extractor()


# ── Fixture builders ─────────────────────────────────────────────────────────

def _window(entities, facts, *, discourse=None, span=(0, 2), name="A Document"):
    w = {
        "document": {"name": name, "summary": "A summary.", "doc_kind": "essay",
                     "chunk_span": list(span)},
        "entities": [
            {"name": n, "type": t, "first_seen_chunk": c, "mention_count": m}
            for n, t, c, m in entities
        ],
        "facts": [
            {"subject": s, "predicate": p, "object": o, "object_literal": lit,
             "fact_text": f"{s} {p} {o or lit}.", "chunk_range": list(span),
             "confidence": "high"}
            for s, p, o, lit in facts
        ],
    }
    # Always present. v2 REQUIRES `discourse`; v1's top-level
    # `additionalProperties: true` tolerates it, so one fixture shape validates
    # against both schemas and a v1/v2 divergence cannot hide behind two shapes.
    w["discourse"] = discourse if discourse is not None else [{
        "move_type": "claim", "title": f"{name} makes a claim.",
        "detail": None, "status": "asserted", "supports": None,
        "chunk_range": list(span),
    }]
    return w


def _validate(window: dict, schema: dict) -> list:
    return sorted(Draft202012Validator(schema).iter_errors(window),
                  key=lambda e: list(e.path))


def _merged_types(windows) -> dict:
    merged = EXTRACTOR.merge_extractions(windows, [])
    return {normalize_entity_text(e["name"]): e["type"] for e in merged["entities"]}, merged


# ── AC1: a dated gathering is an Event ───────────────────────────────────────

DWEB_WINDOWS = [
    _window(
        entities=[("DWeb Berlin", "Event", 0, 3),
                  ("Michael Garfield", "Person", 0, 2)],
        facts=[("DWeb Berlin", "HAS_DATE", None, "09 July 02026"),
               ("DWeb Berlin", "HAS_LOCATION", None, "Berlin")],
        name="Scenius at Scale",
    ),
    _window(
        entities=[("DWeb Camp 2026: Root Systems", "Event", 2, 2),
                  ("Internet Archive", "Organization", 2, 1)],
        facts=[("DWeb Camp 2026: Root Systems", "HAS_TIMEFRAME", None, "five days")],
        span=(2, 4), name="Scenius at Scale",
    ),
]


@pytest.mark.parametrize("schema,label", [(V1_SCHEMA, "v1"), (V2_SCHEMA, "v2")])
def test_a_dated_gathering_validates_as_event(schema, label) -> None:
    for w in DWEB_WINDOWS:
        assert not _validate(w, schema), (
            f"{label} rejects an Event-typed gathering: "
            f"{[e.message for e in _validate(w, schema)]}")


def test_the_legacy_enum_rejected_it_which_is_why_this_fixture_exists() -> None:
    """Positive control. If this ever passes, the fixture stopped testing the fix."""
    errors = _validate(DWEB_WINDOWS[0], _legacy_schema())
    assert errors and "'Event' is not one of" in errors[0].message


def test_dweb_names_survive_the_merge_as_event() -> None:
    types, _ = _merged_types(DWEB_WINDOWS)
    assert types["dweb berlin"] == "Event"
    assert types["dweb camp 2026: root systems"] == "Event"


def test_an_organization_window_cannot_overrule_the_event_window() -> None:
    """The actual 2026-09-14 shape: one window sees the convening body, another
    sees the gathering. Before #68 `Event` was not in TYPE_PRIORITY at all, so it
    scored 0 and lost to everything — including `Concept`."""
    windows = [
        _window(entities=[("DWeb Berlin", "Organization", 0, 1)],
                facts=[("DWeb Berlin", "HAS_STATUS", None, "annual")]),
        _window(entities=[("DWeb Berlin", "Event", 1, 4)],
                facts=[("DWeb Berlin", "HAS_DATE", None, "09 July 02026")], span=(1, 3)),
    ]
    types, merged = _merged_types(windows)
    assert types["dweb berlin"] == "Event"
    assert merged["type_conflicts"] == [
        {"normalized": "dweb berlin", "name": "DWeb Berlin", "kept": "Event",
         "dropped": ["Organization"], "occurrences": 1}
    ], "the coercion must be recorded, not silent (issue #68 requirement 4)"


# ── AC2: a named essay is a Document ─────────────────────────────────────────

ESSAY_WINDOWS = [
    _window(
        entities=[("The End of Hoop Jumping", "Document", 0, 2),
                  ("Daniel Thorson", "Person", 0, 1)],
        facts=[("The End of Hoop Jumping", "AUTHORED_BY", "Daniel Thorson", None)],
        name="Scenius at Scale",
    ),
    _window(
        entities=[("How To Think About Science", "Document", 3, 2),
                  ("David Cayley", "Person", 3, 1),
                  ("CBC", "Organization", 3, 1)],
        facts=[("How To Think About Science", "PUBLISHED_BY", "CBC", None),
               ("How To Think About Science", "AUTHORED_BY", "David Cayley", None)],
        span=(3, 5), name="RIP Wendell Berry",
    ),
]


@pytest.mark.parametrize("schema,label", [(V1_SCHEMA, "v1"), (V2_SCHEMA, "v2")])
def test_named_essays_validate_as_document(schema, label) -> None:
    for w in ESSAY_WINDOWS:
        assert not _validate(w, schema), f"{label} rejects a Document-typed essay"


def test_the_legacy_enum_rejected_document_too() -> None:
    errors = _validate(ESSAY_WINDOWS[0], _legacy_schema())
    assert errors and "'Document' is not one of" in errors[0].message


def test_named_essays_survive_the_merge_as_document_not_project() -> None:
    types, _ = _merged_types(ESSAY_WINDOWS)
    assert types["the end of hoop jumping"] == "Document"
    assert types["how to think about science"] == "Document"


def test_a_project_window_cannot_overrule_a_document_window() -> None:
    """`Project` was the only available answer before #68 and is still the answer a
    window will fall back to. It must not win against a window that got it right."""
    windows = [
        _window(entities=[("The End of Hoop Jumping", "Project", 0, 1)],
                facts=[("The End of Hoop Jumping", "HAS_STATUS", None, "published")]),
        _window(entities=[("The End of Hoop Jumping", "Document", 1, 2)],
                facts=[("The End of Hoop Jumping", "HAS_URL", None, "https://example.test")],
                span=(1, 2)),
    ]
    types, merged = _merged_types(windows)
    assert types["the end of hoop jumping"] == "Document"
    assert merged["type_conflicts"][0]["kept"] == "Document"
    assert merged["type_conflicts"][0]["dropped"] == ["Project"]


def test_essay_endpoints_are_typed_document_by_the_identity_contract() -> None:
    """The type the identity contract would PIN — the thing PR #66 makes permanent
    by hashing it into the canonical URI."""
    merged = EXTRACTOR.merge_extractions(ESSAY_WINDOWS, [])
    eps = {e.name: e.entity_type
           for e in ident.collect_endpoints(merged, normalize=normalize_entity_text)}
    assert eps["The End of Hoop Jumping"] == "Document"
    assert eps["How To Think About Science"] == "Document"


# ── AC3: the Freedom app is not the abstract freedom ─────────────────────────

FREEDOM_WINDOWS = [
    _window(
        entities=[("Freedom (internet-blocking software)", "Project", 0, 2),
                  ("freedom", "Concept", 0, 3),
                  ("Tim Leary", "Person", 0, 1)],
        facts=[("Freedom (internet-blocking software)", "HAS_DESCRIPTION", None,
                "blocks distracting websites"),
               ("freedom", "RELATES_TO", "Tim Leary", None)],
        name="Advertisement Is Psychedelic Art",
    ),
]


@pytest.mark.parametrize("schema,label", [(V1_SCHEMA, "v1"), (V2_SCHEMA, "v2")])
def test_the_freedom_fixture_validates(schema, label) -> None:
    assert not _validate(FREEDOM_WINDOWS[0], schema)


def test_the_software_and_the_concept_stay_two_entities() -> None:
    types, merged = _merged_types(FREEDOM_WINDOWS)
    assert types["freedom (internet blocking software)"] == "Project"
    assert types["freedom"] == "Concept"
    assert len(merged["entities"]) == 3
    assert merged["type_conflicts"] == [], (
        "disambiguated surface forms must not register as a conflict"
    )


def test_they_remain_two_distinct_pinnable_identities() -> None:
    merged = EXTRACTOR.merge_extractions(FREEDOM_WINDOWS, [])
    eps = ident.collect_endpoints(merged, normalize=normalize_entity_text)
    keys = {e.key for e in eps}
    assert ("freedom (internet blocking software)", "Project") in keys
    assert ("freedom", "Concept") in keys
    assert len({e.normalized for e in eps}) == len(keys), (
        "two endpoints share a normalized label; they would compete for one identity"
    )


def test_the_bare_name_collision_is_recorded_rather_than_resolved_in_silence() -> None:
    """The failure mode the prompt guidance exists to prevent.

    If a window types the app `Freedom` and another types the idea `freedom`, they
    normalize identically and the merge — which keys on the normalized name so that
    one entity typed two ways becomes ONE entity — folds them together. That is not
    fixable by reordering priorities: whichever type wins, something is wrong.

    What IS fixable is the silence. The coercion is now reported, so the identity
    gate and the run receipt show that a decision was made on the caller's behalf.
    """
    windows = [
        _window(entities=[("Freedom", "Project", 0, 2)],
                facts=[("Freedom", "HAS_DESCRIPTION", None, "an app")]),
        _window(entities=[("freedom", "Concept", 1, 5)],
                facts=[("freedom", "RELATES_TO", None, "autonomy")], span=(1, 2)),
    ]
    _types, merged = _merged_types(windows)
    assert len(merged["type_conflicts"]) == 1
    conflict = merged["type_conflicts"][0]
    assert conflict["normalized"] == "freedom"
    assert {conflict["kept"], *conflict["dropped"]} == {"Project", "Concept"}


# ── Requirement 6: cached historical payloads keep working ───────────────────

LEGACY_ERA_WINDOW = _window(
    entities=[("Ivan Illich", "Person", 0, 4),
              ("Convivial Tools", "Concept", 0, 2),
              ("Kentucky", "Location", 1, 1),
              ("Long Now Foundation", "Organization", 1, 1),
              ("Auroville", "CaseStudy", 2, 1),
              ("HTTPS", "Protocol", 2, 1),
              ("Whole Earth Catalog", "Project", 2, 2)],
    facts=[("Convivial Tools", "DERIVES_FROM", "Ivan Illich", None)],
)


@pytest.mark.parametrize("schema,label", [(V1_SCHEMA, "v1"), (V2_SCHEMA, "v2")])
def test_a_payload_cached_under_the_old_seven_type_enum_still_validates(schema, label) -> None:
    """The schema change is purely ADDITIVE, so nothing already cached is invalidated.

    This is why `Document` and `Event` are appended to the enum rather than
    inserted: 356 documents sit in the deep-extract backlog and an unknown number
    of cached window extractions predate the change.
    """
    assert not _validate(LEGACY_ERA_WINDOW, schema), f"{label} rejects a v1-era payload"


def test_every_legacy_type_is_still_admitted() -> None:
    for t in LEGACY_ENUM:
        assert contract.is_extractable(t)
        assert t in contract.TYPE_PRIORITY


def test_an_audited_type_override_in_a_cached_payload_now_validates() -> None:
    """The operator's 2026-09-14 correction of the cached Scenius extraction —
    `Organization` -> `Event`, applied by hand to `document_window_extractions`.

    Under the old enum that hand-corrected payload could not be re-validated at all:
    the only error was `'Event' is not one of [...]`. A repair that makes a cached
    payload unparseable is not a repair.
    """
    corrected = _window(
        entities=[("DWeb Berlin", "Event", 0, 3)],
        facts=[("DWeb Berlin", "HAS_DATE", None, "09 July 02026")])
    assert not _validate(corrected, V2_SCHEMA)
    legacy_errors = _validate(corrected, _legacy_schema())
    assert len(legacy_errors) == 1 and "'Event' is not one of" in legacy_errors[0].message


def test_an_off_contract_type_is_reported_not_swallowed() -> None:
    """A cached payload carrying a type this build does not admit must be visible.

    `TYPE_PRIORITY.get(etype, 0)` ranked such a type below `Concept` and said
    nothing — which is precisely how Document and Event behaved for months.
    """
    windows = [
        _window(entities=[("Weekly Standup", "Meeting", 0, 1)],
                facts=[("Weekly Standup", "HAS_DATE", None, "Monday")]),
    ]
    merged = EXTRACTOR.merge_extractions(windows, [])
    assert merged["unknown_types"] == {"Meeting": 1}
    assert merged["type_contract_version"] == contract.DOCUMENT_TYPE_CONTRACT_VERSION


def test_a_clean_payload_reports_no_conflicts_and_no_unknowns() -> None:
    """Negative control for the two reporters above: they must be able to be empty,
    or 'nothing reported' would carry no information."""
    merged = EXTRACTOR.merge_extractions([LEGACY_ERA_WINDOW], [])
    assert merged["type_conflicts"] == []
    assert merged["unknown_types"] == {}


# ── The startup half of requirement 5: what a RUN loads, not what is committed ──

def _live_surfaces(tier: str = "thorough") -> tuple:
    prompt = (REPO_ROOT / f"scripts/prompts/deep_extraction_doc_"
                          f"{'v2' if tier == 'thorough' else 'v1'}.md").read_text()
    schema = V2_SCHEMA if tier == "thorough" else V1_SCHEMA
    return prompt, json.loads(json.dumps(schema))


@pytest.mark.parametrize("tier", ["standard", "thorough"])
def test_the_extractor_accepts_the_surfaces_in_this_checkout(tier: str) -> None:
    prompt, schema = _live_surfaces(tier)
    EXTRACTOR.assert_contract_surfaces(prompt, schema, Path("prompt"), Path("schema"))


def test_a_stale_schema_from_another_checkout_is_refused() -> None:
    """The deployment gap this covers is real: a scheduled extraction job may run
    from a separate clone refreshed by `git pull`, and every
    prompt/schema path is env-overridable. A test over the files in THIS repository
    says nothing about what an unattended run sent to a model."""
    prompt, schema = _live_surfaces()
    schema["properties"]["entities"]["items"]["properties"]["type"]["enum"] = LEGACY_ENUM
    with pytest.raises(EXTRACTOR.ExtractionError) as exc:
        EXTRACTOR.assert_contract_surfaces(prompt, schema, Path("prompt"), Path("schema"))
    assert exc.value.terminal is True
    assert exc.value.reason == "contract_drift"


def test_a_stale_prompt_is_refused_even_when_the_schema_is_current() -> None:
    """The nastiest shape: schema current, prompt stale. The model is never told
    `Document` exists, emits `Project`, and the output validates perfectly — the
    drift is invisible to every downstream check."""
    prompt, schema = _live_surfaces()
    stale = prompt.replace("`Document`", "`Project`")
    with pytest.raises(EXTRACTOR.ExtractionError) as exc:
        EXTRACTOR.assert_contract_surfaces(stale, schema, Path("prompt"), Path("schema"))
    assert "Document" in str(exc.value)


def test_a_schema_with_no_entity_type_enum_is_refused_not_skipped() -> None:
    prompt, schema = _live_surfaces()
    del schema["properties"]["entities"]["items"]["properties"]["type"]["enum"]
    with pytest.raises(EXTRACTOR.ExtractionError, match="no entities\\[\\].type enum"):
        EXTRACTOR.assert_contract_surfaces(prompt, schema, Path("prompt"), Path("schema"))


def test_off_contract_counts_records_not_comparisons() -> None:
    """The count must mean 'entity records carrying this type', not 'times it lost
    a priority comparison'. The latter depends on how windows happened to be cut,
    so the same payload windowed differently would report different numbers."""
    windows = [
        _window(entities=[("Weekly Standup", "Meeting", 0, 1),
                          ("Quarterly Review", "Meeting", 0, 1)],
                facts=[("Weekly Standup", "HAS_DATE", None, "Monday")]),
        _window(entities=[("Weekly Standup", "Meeting", 1, 1)],
                facts=[("Weekly Standup", "HAS_STATUS", None, "recurring")], span=(1, 2)),
        _window(entities=[("Weekly Standup", "Event", 2, 1)],   # one conflict
                facts=[("Weekly Standup", "HAS_LOCATION", None, "room 2")], span=(2, 3)),
    ]
    merged = EXTRACTOR.merge_extractions(windows, [])
    assert merged["unknown_types"] == {"Meeting": 3}, (
        "3 records carry Meeting: two for Weekly Standup, one for Quarterly Review"
    )
    assert merged["type_conflicts"][0]["kept"] == "Event", (
        "an admitted type must beat an off-contract one"
    )
