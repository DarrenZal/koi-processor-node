"""The untyped wikilink tier must be able to run, and nested vault folders must keep their type.

THE DEFECT THIS PINS
--------------------
`batch_resolve_entities` resolves wikilink targets in three tiers. The middle tier — for
targets with no type hint — ordered by `occurrence_count`, a column that exists in
migrations/020_entity_registry.sql (RegenAI-era) and in ZERO tables of `personal_koi`.
So that tier did not merely rank badly: it raised `UndefinedColumnError` every time it was
reached, before any relationship was written and before the Step 3b vault-path fallback
that resolves exactly these targets correctly.

`POST /register-entity` catches the exception, logs a warning and returns success, so a
note whose frontmatter contained one untyped target silently lost ALL of its relationships.

Reached how: `parse_wikilink` does `folder, name = path.rsplit('/', 1)`, so a nested path
`Meetings/Example Group/2026-01-15 Example Group Meeting` yields the folder key
`"meetings/example group"`, which is not in `folder_type_map` -> type_hint None. Nested is the
vault's actual convention, so essentially every meeting wikilink took the broken tier.
Measured 2026-08-23: 1,984 of 6,112 vault notes with a sync target carried at least one
untyped target, and `sourced_from` had 0 edges from a nested path against 1,944 declared
in the vault.

WHY NOT "just delete the ORDER BY"
----------------------------------
`DISTINCT ON` without a total order picks an arbitrary row per name, so the tier would
become nondeterministic instead of broken. The replacement must both exist and rank:
`(merged_into IS NOT NULL), id` prefers a live row over a tombstone and then breaks ties
stably. The pre-existing comment claiming this tier "actively PREFERS the tombstone,
which typically has the higher count" described behaviour that never once executed.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path

import asyncpg
import pytest

from tests import koi_test_targets as targets

from api.vault_parser import FIELD_TO_PREDICATE, parse_wikilink

REPO = Path(__file__).resolve().parents[1]
VAULT_PARSER = REPO / "api" / "vault_parser.py"


def _source() -> str:
    return VAULT_PARSER.read_text()


# --------------------------------------------------------------------------------------
# The column must exist. This is the whole defect, stated as a property of the schema.
# --------------------------------------------------------------------------------------

async def _columns(table: str) -> set[str]:
    conn = await asyncpg.connect(targets.database_url())
    try:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1",
            table,
        )
        return {r["column_name"] for r in rows}
    finally:
        await conn.close()


def test_untyped_tier_orders_by_a_column_that_exists() -> None:
    """The ORDER BY of the untyped tier may only name real entity_registry columns."""
    src = _source()
    m = re.search(
        r"SELECT DISTINCT ON \(normalized_text\).*?ORDER BY ([^\n]+)\n",
        src,
        re.DOTALL,
    )
    assert m, "the untyped tier's DISTINCT ON query vanished — re-anchor this test"
    order_by = m.group(1)

    cols = asyncio.run(_columns("entity_registry"))
    assert cols, "information_schema returned nothing; a silent skip is what we are preventing"

    referenced = set(re.findall(r"[a-z_][a-z0-9_]*", order_by)) - {
        "desc", "asc", "nulls", "last", "first", "is", "not", "null", "and", "or"
    }
    missing = sorted(referenced - cols)
    assert not missing, (
        f"ORDER BY names column(s) absent from entity_registry: {missing}. "
        "This raises UndefinedColumnError and the caller swallows it."
    )


def test_untyped_tier_query_actually_executes() -> None:
    """Run the tier's real SQL. Before the fix this raises UndefinedColumnError."""
    src = _source()
    m = re.search(
        r'(SELECT DISTINCT ON \(normalized_text\).*?)\n\s*"""',
        src,
        re.DOTALL,
    )
    assert m, "could not extract the untyped-tier SQL — re-anchor this test"
    sql = m.group(1)

    async def run() -> None:
        conn = await asyncpg.connect(targets.database_url())
        try:
            await conn.fetch(sql, ["a name that resolves to nothing"])
        finally:
            await conn.close()

    asyncio.run(run())


def test_untyped_tier_is_deterministic() -> None:
    """DISTINCT ON without a total order returns an arbitrary row; forbid that regression."""
    src = _source()
    m = re.search(
        r"SELECT DISTINCT ON \(normalized_text\).*?ORDER BY ([^\n]+)\n", src, re.DOTALL
    )
    assert m, "the untyped tier's DISTINCT ON query vanished — re-anchor this test"
    order_by = m.group(1).strip()
    assert "normalized_text" in order_by, "DISTINCT ON key must lead the ORDER BY"
    tail = order_by.split("normalized_text", 1)[1]
    assert tail.strip(" ,"), (
        "ORDER BY has no tiebreak after normalized_text: DISTINCT ON would pick "
        "an arbitrary row per name"
    )
    assert "id" in tail, "no stable unique tiebreak (expected id) in the ORDER BY"


def test_occurrence_count_never_returns_to_vault_parser() -> None:
    """A RegenAI-era column. Re-introducing it re-breaks the tier silently."""
    offenders = [
        f"{VAULT_PARSER.name}:{i}: {line.strip()}"
        for i, line in enumerate(_source().split("\n"), 1)
        if "occurrence_count" in line and not line.lstrip().startswith("#")
    ]
    assert not offenders, "occurrence_count is back in executable code:\n" + "\n".join(offenders)


# --------------------------------------------------------------------------------------
# Nested vault folders must keep their type hint (fix (a)).
# --------------------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw,expected_type",
    [
        ("[[Meetings/2026-06-30 Example Meeting]]", "Meeting"),
        ("[[Meetings/Example Group/2026-06-30 Example Group Meeting]]", "Meeting"),
        ("[[Meetings/Another Group/2026-02-09 Another Group Meeting]]", "Meeting"),
        ("[[Meetings/The Example Project/2026-01-15 Meeting]]", "Meeting"),
        ("[[People/Shawn Anderson]]", "Person"),
        ("[[People/Cascadia/Shawn Anderson]]", "Person"),
        ("[[Organizations/Regen Network]]", "Organization"),
    ],
)
def test_nested_folders_keep_their_type_hint(raw: str, expected_type: str) -> None:
    _, hint = parse_wikilink(raw)
    assert hint == expected_type, (
        f"{raw} lost its type hint. rsplit('/', 1) makes the folder key the whole prefix, "
        "so a nested path never matches folder_type_map and falls into the untyped tier."
    )


def test_bare_wikilink_still_has_no_type_hint() -> None:
    """The untyped tier must stay reachable — this is not a licence to type everything."""
    assert parse_wikilink("[[Regen Network]]") == ("Regen Network", None)


def test_nested_meeting_wikilink_survives_the_full_target_build() -> None:
    """End-to-end on the sourceNote field: the shape that dominated the vault."""
    predicate, direction, default_hint = FIELD_TO_PREDICATE["sourcenote"]
    assert default_hint is None, "sourceNote relies on the folder prefix for its type"
    name, hint = parse_wikilink("[[Meetings/Example Group/2026-01-15 Example Group Meeting]]")
    assert (hint or default_hint) == "Meeting", (
        "sourceNote wikilinks to nested meeting folders resolve untyped, which is how "
        "1,944 declared sourced_from edges became 0 in the graph"
    )


# --------------------------------------------------------------------------------------
# The swallow must be visible (fix (c)).
#
# sync_vault_relationships resolves every target in ONE batch, so a single unresolvable
# target loses every relationship in the note — a whole-note failure. The handler caught
# it, logged "Failed to sync relationships: <exc>" with no note name, and returned
# success=True. Nothing downstream could tell a note with no relationships from a note
# whose relationships were dropped.
# --------------------------------------------------------------------------------------

INGEST_API = REPO / "api" / "personal_ingest_api.py"


def test_register_entity_response_can_report_a_relationship_sync_failure() -> None:
    from api.personal_ingest_api import RegisterEntityResponse

    fields = RegisterEntityResponse.model_fields
    assert "relationship_sync_error" in fields, (
        "RegisterEntityResponse cannot express partial failure, so a note whose "
        "relationships were all dropped is indistinguishable from a clean success"
    )
    ok = RegisterEntityResponse(
        success=True, canonical_uri="orn:x", is_new=False, vault_rid="v"
    )
    assert ok.relationship_sync_error is None, "must default to None on the clean path"


def test_the_relationship_sync_handler_records_and_attributes_its_failure() -> None:
    src = INGEST_API.read_text()
    idx = src.find("await sync_vault_relationships(")
    assert idx != -1, "call site vanished — re-anchor this test"
    block = src[idx: idx + 2000]

    handler = block[block.find("except Exception"):]
    assert handler, "the call is no longer wrapped — check whether that is intended"

    assert "relationship_sync_error =" in handler, (
        "the handler logs but does not record the failure, so the response cannot "
        "report it and the caller still sees an unqualified success"
    )
    assert "eff_vault_path" in handler, (
        "the warning does not name the note. 'Failed to sync relationships: <exc>' is "
        "unattributable across thousands of notes"
    )


def test_the_response_actually_carries_the_error() -> None:
    """Recording it into a local that never reaches the response would be worse than nothing."""
    src = INGEST_API.read_text()
    idx = src.find("result = RegisterEntityResponse(")
    assert idx != -1, "response construction vanished — re-anchor this test"
    ctor = src[idx: src.find(")", src.find("koi_rid=", idx)) + 1]
    assert "relationship_sync_error=relationship_sync_error" in ctor, (
        "the error is recorded but not returned"
    )


# --------------------------------------------------------------------------------------
# The failure must not poison the enclosing transaction.
#
# `except Exception` does not undo a failed statement's effect on a PostgreSQL
# transaction. /register-entity runs its whole body inside one `conn.transaction()`, so
# when the untyped tier raised, catching it let the handler continue while every later
# statement failed with InFailedSQLTransactionError and the closing COMMIT became a
# silent ROLLBACK. The endpoint returned HTTP 200 success=True having discarded the
# entity_registry and entity_rid_mappings writes it had just made.
# --------------------------------------------------------------------------------------

def test_catching_an_error_without_a_savepoint_poisons_the_transaction() -> None:
    """Positive control. If this ever stops failing, the test below proves nothing."""
    async def run() -> str:
        conn = await asyncpg.connect(targets.database_url())
        try:
            tr = conn.transaction()
            await tr.start()
            try:
                try:
                    await conn.fetch("SELECT 1 FROM entity_registry ORDER BY no_such_column")
                except Exception:
                    pass
                try:
                    await conn.fetchval("SELECT count(*) FROM entity_registry")
                    return "usable"
                except Exception as e:
                    return type(e).__name__
            finally:
                await tr.rollback()
        finally:
            await conn.close()

    assert asyncio.run(run()) == "InFailedSQLTransactionError", (
        "a caught error no longer aborts the transaction — re-derive the savepoint "
        "reasoning in personal_ingest_api before trusting the test below"
    )


def test_a_savepoint_makes_the_transaction_usable_again() -> None:
    """The containment the handler relies on."""
    async def run() -> str:
        conn = await asyncpg.connect(targets.database_url())
        try:
            tr = conn.transaction()
            await tr.start()
            try:
                await conn.execute("SAVEPOINT vault_rel_sync")
                try:
                    await conn.fetch("SELECT 1 FROM entity_registry ORDER BY no_such_column")
                except Exception:
                    await conn.execute("ROLLBACK TO SAVEPOINT vault_rel_sync")
                await conn.fetchval("SELECT count(*) FROM entity_registry")
                return "usable"
            finally:
                await tr.rollback()
        finally:
            await conn.close()

    assert asyncio.run(run()) == "usable"


def test_the_relationship_sync_call_is_wrapped_in_a_savepoint() -> None:
    src = INGEST_API.read_text()
    idx = src.find("await sync_vault_relationships(")
    assert idx != -1, "call site vanished — re-anchor this test"
    block = src[max(0, idx - 1800): idx + 2200]

    assert "SAVEPOINT vault_rel_sync" in block, (
        "sync_vault_relationships is not wrapped in a savepoint. Its exception is caught, "
        "but the enclosing transaction stays aborted and the handler's COMMIT silently "
        "becomes a ROLLBACK — discarding the registration while returning success=True"
    )
    handler = block[block.find("except Exception", idx - max(0, idx - 1800)):]
    assert "ROLLBACK TO SAVEPOINT vault_rel_sync" in handler, (
        "the savepoint is set but never rolled back to, so the transaction stays aborted"
    )
