"""The deep-document extraction type contract, and the drift it exists to catch.

Issue #68. Before this module the entity-type vocabulary was written out by hand in
six places with nothing comparing them, and `Document` and `Event` were missing from
all six while the live registry had already marked both `extractable = true`. The
result was not a bad answer — it was a STRUCTURALLY FORBIDDEN one: a dated gathering
could only come out `Organization`, a named essay could only come out `Project`.

These tests are the comparison that did not exist. Every one of them fails if a
surface is edited by hand, and each has a positive control proving it can fail — a
drift test that cannot fail is the same shape of nothing as the enum it replaced.

None of the contract tests need a database, a network, or an API key. A test that
skips is a test that does not exist, and this vocabulary is exactly the kind of
thing that would drift for months inside a skipped test.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import document_extraction_contract as contract  # noqa: E402
from api.entity_schema import UNKNOWN_TYPE_SCHEMA, get_schema_for_type  # noqa: E402
from scripts import render_extraction_contract as renderer  # noqa: E402
from tests import koi_test_targets as targets  # noqa: E402

#: The vocabulary as it stood before #68. Written out literally, on purpose: if the
#: contract ever silently reverts, comparing it to a constant derived from the
#: contract would compare it to itself and notice nothing.
LEGACY_SEVEN = ("Person", "Organization", "Project", "Concept",
                "Location", "Protocol", "CaseStudy")

ADDED_BY_68 = ("Document", "Event")

SCHEMA_FILES = (
    REPO_ROOT / "scripts/schemas/deep_extraction_doc_v1.schema.json",
    REPO_ROOT / "scripts/schemas/deep_extraction_doc_v2.schema.json",
)
PROMPT_FILES = (
    REPO_ROOT / "scripts/prompts/deep_extraction_doc_v1.md",
    REPO_ROOT / "scripts/prompts/deep_extraction_doc_v2.md",
)


# ── The contract's own shape ─────────────────────────────────────────────────

def test_the_contract_admits_the_legacy_seven_plus_document_and_event() -> None:
    assert contract.DOCUMENT_ENTITY_TYPE_NAMES[: len(LEGACY_SEVEN)] == LEGACY_SEVEN, (
        "the original seven must keep their original enum positions so the schema "
        "change stays purely additive and v1-era cached payloads still validate"
    )
    for t in ADDED_BY_68:
        assert t in contract.DOCUMENT_ENTITY_TYPE_NAMES, f"{t} missing — issue #68"
    assert len(contract.DOCUMENT_ENTITY_TYPE_NAMES) == 9


def test_every_type_is_declared_exactly_once_with_a_distinct_priority() -> None:
    names = [t.name for t in contract.DOCUMENT_ENTITY_TYPES]
    assert len(names) == len(set(names)), f"duplicate type declaration: {names}"
    priorities = [t.priority for t in contract.DOCUMENT_ENTITY_TYPES]
    assert len(priorities) == len(set(priorities)), (
        f"two types share a priority, so cross-window conflict resolution between "
        f"them is decided by dict order rather than by the contract: {priorities}"
    )
    assert all(p > contract.UNKNOWN_TYPE_PRIORITY for p in priorities), (
        "an admitted type must outrank an unadmitted one"
    )


def test_adding_types_did_not_reorder_the_legacy_seven() -> None:
    """The relative order of the original seven is load-bearing and unchanged.

    Adding a type must not silently re-resolve conflicts that already resolved
    correctly. Before #68 the order was Person > Organization > Project > Location
    > Protocol > CaseStudy > Concept.
    """
    ranked = sorted(LEGACY_SEVEN, key=lambda n: contract.TYPE_PRIORITY[n], reverse=True)
    assert tuple(ranked) == ("Person", "Organization", "Project", "Location",
                             "Protocol", "CaseStudy", "Concept")


@pytest.mark.parametrize("added", ADDED_BY_68)
def test_document_and_event_outrank_project(added: str) -> None:
    """`Project` is the generic 'named thing' sink both 2026-09-14 mis-types fell
    into. A window that recognises an essay as a Document or a gathering as an
    Event must not be overruled by one that fell back to Project."""
    assert contract.TYPE_PRIORITY[added] > contract.TYPE_PRIORITY["Project"]
    assert contract.TYPE_PRIORITY[added] > contract.TYPE_PRIORITY["Concept"]


def test_event_outranks_organization_because_that_is_the_observed_failure() -> None:
    """`DWeb Berlin`, a five-day gathering, was captured as an Organization. The
    reverse has not been observed. See the ordering rationale in the contract."""
    assert contract.TYPE_PRIORITY["Event"] > contract.TYPE_PRIORITY["Organization"]


def test_priority_for_reports_unknown_rather_than_silently_returning_zero() -> None:
    """Issue #68 requirement 4. `TYPE_PRIORITY.get(x, 0)` is how Document and Event
    ranked below Concept for months without anything saying so."""
    pri, known = contract.priority_for("Event")
    assert known and pri == contract.TYPE_PRIORITY["Event"]
    pri, known = contract.priority_for("Meeting")
    assert not known and pri == contract.UNKNOWN_TYPE_PRIORITY
    assert contract.priority_for(None) == (contract.UNKNOWN_TYPE_PRIORITY, False)


# ── Every derived surface matches the contract ───────────────────────────────

@pytest.mark.parametrize("path", SCHEMA_FILES, ids=lambda p: p.name)
def test_schema_enum_matches_the_contract(path: Path) -> None:
    schema = json.loads(path.read_text(encoding="utf-8"))
    enum = schema["properties"]["entities"]["items"]["properties"]["type"]["enum"]
    assert tuple(enum) == contract.DOCUMENT_ENTITY_TYPE_NAMES


@pytest.mark.parametrize("path", PROMPT_FILES, ids=lambda p: p.name)
def test_prompt_states_the_same_type_set_in_all_three_places(path: Path) -> None:
    """The typing rules, the appendix schema, and HARD OUTPUT RULE 2 — a model that
    obeys one and ignores another produces exactly the drift this issue is about."""
    text = path.read_text(encoding="utf-8")

    backticked = re.search(
        r"\*\*Type each entity as exactly one of:\*\*(.*?)\. Canonicalization",
        text, re.S)
    assert backticked, f"{path.name}: typing-rules list not found"
    assert tuple(re.findall(r"`([A-Za-z]+)`", backticked.group(1))) == \
        contract.DOCUMENT_ENTITY_TYPE_NAMES

    appendix = re.search(r"## APPENDIX: Schema\n\n```json\n(.*?)\n```", text, re.S)
    assert appendix, f"{path.name}: appendix schema block not found"
    enum = json.loads(appendix.group(1))["properties"]["entities"]["items"][
        "properties"]["type"]["enum"]
    assert tuple(enum) == contract.DOCUMENT_ENTITY_TYPE_NAMES

    hard = re.search(r"`entities\[\]\.type` MUST be exactly one of:\n\s*(.+?)\.\n",
                     text, re.S)
    assert hard, f"{path.name}: HARD OUTPUT RULE 2 not found"
    assert tuple(x.strip() for x in hard.group(1).split(",")) == \
        contract.DOCUMENT_ENTITY_TYPE_NAMES


@pytest.mark.parametrize("path", PROMPT_FILES, ids=lambda p: p.name)
def test_prompt_teaches_document_and_event_with_the_worked_examples(path: Path) -> None:
    """Admitting a type in an enum without saying when to use it is half a fix."""
    # Whitespace-normalized: the generated bullets are wrapped to 88 columns, so a
    # worked example can legitimately straddle a line break. What must hold is that
    # the guidance is PRESENT, not how it happens to be folded.
    text = re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))
    for needle in ("A NAMED WRITTEN WORK is `Document`",
                   "A DATED HAPPENING is `Event`",
                   "The End of Hoop Jumping",
                   "How To Think About Science",
                   "DWeb Camp 2026: Root Systems",
                   "Freedom (internet-blocking software)"):
        assert needle in text, f"{path.name} does not mention {needle!r}"


def test_the_extractor_uses_the_contracts_priority_table() -> None:
    """`scripts/extract_deep_documents.py` must not hold its own copy."""
    src = (REPO_ROOT / "scripts/extract_deep_documents.py").read_text(encoding="utf-8")
    assert "TYPE_PRIORITY = contract.TYPE_PRIORITY" in src
    assert not re.search(r'TYPE_PRIORITY\s*=\s*\{\s*"Person"', src), (
        "the extractor has grown a hand-written priority table again"
    )


def test_the_renderer_reports_the_repository_in_sync() -> None:
    """The check the whole derivation rests on, run exactly as an operator would."""
    r = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts/render_extraction_contract.py"), "--check"],
        capture_output=True, text=True, cwd=REPO_ROOT)
    assert r.returncode == 0, (
        f"contract surfaces have drifted from {contract.DOCUMENT_TYPE_CONTRACT_VERSION}.\n"
        f"{r.stdout}\n{r.stderr}")
    assert "all 4 contract surfaces match" in r.stdout


# ── Positive controls: prove each check CAN fail ─────────────────────────────

def _sandbox(tmp_path: Path, monkeypatch) -> tuple:
    """Copy the four surfaces somewhere writable and point the renderer at them."""
    schemas = tuple(shutil.copy2(p, tmp_path / p.name) for p in SCHEMA_FILES)
    prompts = tuple(shutil.copy2(p, tmp_path / p.name) for p in PROMPT_FILES)
    schemas = tuple(Path(p) for p in schemas)
    prompts = tuple(Path(p) for p in prompts)
    monkeypatch.setattr(renderer, "SCHEMA_FILES", schemas)
    monkeypatch.setattr(renderer, "PROMPT_FILES", prompts)
    return schemas, prompts


def test_control_a_hand_edited_schema_enum_is_detected(tmp_path, monkeypatch) -> None:
    schemas, _ = _sandbox(tmp_path, monkeypatch)
    victim = schemas[0]
    text = victim.read_text(encoding="utf-8")
    victim.write_text(text.replace('"Event"]', "]"), encoding="utf-8")   # drop Event
    assert renderer.render_file(victim) != victim.read_text(encoding="utf-8"), (
        "removing a type from a schema enum went undetected"
    )


def test_control_a_hand_edited_prompt_rule_is_detected(tmp_path, monkeypatch) -> None:
    _, prompts = _sandbox(tmp_path, monkeypatch)
    victim = prompts[0]
    text = victim.read_text(encoding="utf-8")
    assert "A DATED HAPPENING is `Event`" in text
    victim.write_text(text.replace("A DATED HAPPENING is `Event`",
                                   "A DATED HAPPENING is `Organization`"),
                      encoding="utf-8")
    assert renderer.render_file(victim) != victim.read_text(encoding="utf-8")


def test_control_a_missing_marker_raises_instead_of_skipping(tmp_path, monkeypatch) -> None:
    """A generator that quietly leaves a region it cannot find is indistinguishable
    from one with nothing to do. That false pass is this issue in miniature."""
    _, prompts = _sandbox(tmp_path, monkeypatch)
    victim = prompts[0]
    text = victim.read_text(encoding="utf-8")
    victim.write_text(
        text.replace(renderer._marker("entity-type-hard-rule", "begin"), ""),
        encoding="utf-8")
    with pytest.raises(renderer.GeneratorError, match="missing begin marker"):
        renderer.render_file(victim)


def test_control_an_ambiguous_enum_line_raises(tmp_path, monkeypatch) -> None:
    schemas, _ = _sandbox(tmp_path, monkeypatch)
    victim = schemas[0]
    text = victim.read_text(encoding="utf-8")
    line = [ln for ln in text.splitlines() if '"type": {"enum":' in ln][0]
    victim.write_text(text.replace(line, line + "\n" + line), encoding="utf-8")
    with pytest.raises(renderer.GeneratorError, match="expected exactly 1"):
        renderer.render_file(victim)


# ── The registry: code side, then the live database ──────────────────────────

@pytest.mark.parametrize("type_key", contract.DOCUMENT_ENTITY_TYPE_NAMES)
def test_every_admitted_type_is_a_registered_entity_type(type_key: str) -> None:
    """Issue #68 requirement 5, first half: the extractor may not emit a type the
    resolver has no schema for — that routes the entity to a Misc vault folder and
    to UNKNOWN_TYPE_SCHEMA's thresholds."""
    schema = get_schema_for_type(type_key)
    assert schema.type_key != UNKNOWN_TYPE_SCHEMA.type_key, (
        f"the extractor may emit {type_key} but api/entity_schema.py has no schema "
        f"for it, so it falls back to UNKNOWN_TYPE_SCHEMA"
    )


def test_the_live_registry_and_the_contract_designate_the_same_extractable_set() -> None:
    """Issue #68 requirement 5, second half — against the registry at KOI_TEST_DATABASE_URL
    (a database built by the migrations carries the seeded vocabulary).

    This is the check whose absence WAS the bug: `allowed_entity_types` has carried
    `Document` and `Event` with `extractable = true` while three code surfaces said
    seven. Read-only.
    """
    import asyncpg

    dsn = targets.database_url()

    async def fetch() -> tuple:
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch(
                "SELECT entity_type, extractable, deprecated_at FROM allowed_entity_types")
            return ({r["entity_type"] for r in rows},
                    {r["entity_type"] for r in rows
                     if r["extractable"] and r["deprecated_at"] is None})
        finally:
            await conn.close()

    registered, extractable = asyncio.run(fetch())
    assert registered, "allowed_entity_types is empty; a silent skip is the thing we prevent"

    admitted = set(contract.DOCUMENT_ENTITY_TYPE_NAMES)
    assert not admitted - registered, (
        f"the extractor may emit type(s) the registry does not register: "
        f"{sorted(admitted - registered)}")
    assert not extractable - admitted, (
        f"the registry designates type(s) extractable that NEITHER extraction schema "
        f"can represent: {sorted(extractable - admitted)}")
    assert not admitted - extractable, (
        f"the extractor may emit type(s) the registry does not designate extractable: "
        f"{sorted(admitted - extractable)}")
