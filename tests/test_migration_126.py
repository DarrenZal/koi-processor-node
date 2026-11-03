"""Migration 126 (document_extraction_type_registry) — house pattern and dry run.

WHAT THIS GUARDS (PR #66 review finding M8)
-------------------------------------------
The first draft of 126 had no transaction, no ``koi_migrations`` ledger row and a
COUNT assertion (``IF n <> 9``). Each of those is a way for the migration to report
success while its postcondition does not hold:

- no ``BEGIN;``/``COMMIT;`` — under a bare ``psql -f`` the upsert autocommits before
  a failing ``DO`` block, leaving a half-applied state no file describes;
- no ledger row — a database rebuilt from ``migrations/`` has no record it ran, so
  nothing can tell 126-applied from 126-never-applied;
- a count — 'Meeting' wrongly extractable *plus* 'CaseStudy' wrongly missing still
  counts 9, and the assertion passes.

Two layers, both with controls:

1. STRUCTURAL — parse the up and down files and assert the house pattern that 124
   and 125 already follow. 124/125 are the positive control (the check passes on
   them); a doctored copy of 126-up with the ledger line removed is the negative
   control (the check fails on it).
2. DRY RUN — execute the migration bodies on the scratch database inside ONE asyncpg
   transaction that is rolled back at teardown (fixture pattern copied from
   ``tests/test_ingest_identity.py::conn``, including the live-database guard). The
   scratch ``allowed_entity_types`` is empty, so each test seeds the rows migration
   111 seeds, read from ``migrations/111_entity_type_registry.sql`` itself. The
   negative control seeds the count-preserving corruption above and requires the
   up body to RAISE, naming both offenders.

ISOLATION. Nothing here talks to localhost:8351 or to ``personal_koi``. Every
statement runs on the scratch connection and is rolled back.
"""

from __future__ import annotations

import pathlib
import re

import asyncpg
import pytest
from tests import koi_test_targets as targets

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
MIGRATIONS = REPO_ROOT / "migrations"

UP_126 = MIGRATIONS / "126_document_extraction_type_registry.sql"
DOWN_126 = MIGRATIONS / "126_document_extraction_type_registry_down.sql"
SEED_111 = MIGRATIONS / "111_entity_type_registry.sql"

LEDGER_ID_126 = "personal:126_document_extraction_type_registry"

# The contract set (api/document_extraction_contract.py, doc-entity-types-v2-2026-09-14)
# and migration 111's legacy seven. Written out rather than imported so a test of the
# SQL cannot be satisfied by the Python contract drifting in step with it.
CONTRACT_NINE = frozenset({
    "Person", "Organization", "Project", "Concept", "Location", "Protocol",
    "CaseStudy", "Document", "Event",
})
LEGACY_SEVEN = CONTRACT_NINE - {"Document", "Event"}



@pytest.fixture
def anyio_backend():
    return "asyncio"


# ═══════════════════════════════════════════════════════════════════════════
# 1. Structural: the house pattern of 124/125
# ═══════════════════════════════════════════════════════════════════════════

_BEGIN = re.compile(r"^\s*BEGIN\s*;\s*$", re.M)
_COMMIT = re.compile(r"^\s*COMMIT\s*;\s*$", re.M)
_ON_ERROR_STOP = re.compile(r"^\s*\\set\s+ON_ERROR_STOP\s+on\s*$", re.M)


def _ledger_insert(text: str, migration_id: str) -> bool:
    pat = re.compile(
        r"INSERT\s+INTO\s+koi_migrations\s*\(\s*migration_id\s*,\s*checksum\s*\)\s*"
        r"VALUES\s*\(\s*'" + re.escape(migration_id) + r"'\s*,\s*'[^']+'\s*\)\s*"
        r"ON\s+CONFLICT\s*\(\s*migration_id\s*\)\s+DO\s+NOTHING\s*;",
        re.S | re.I,
    )
    return pat.search(text) is not None


def _ledger_delete(text: str, migration_id: str) -> bool:
    pat = re.compile(
        r"DELETE\s+FROM\s+koi_migrations\s+WHERE\s+migration_id\s*=\s*'"
        + re.escape(migration_id) + r"'\s*;",
        re.S | re.I,
    )
    return pat.search(text) is not None


def _ledger_id_for(path: pathlib.Path) -> str:
    stem = path.name[: -len(".sql")]
    if stem.endswith("_down"):
        stem = stem[: -len("_down")]
    return f"personal:{stem}"


def house_pattern_violations(path: pathlib.Path, text: str | None = None) -> list[str]:
    """Return every way `path` departs from the 124/125 house pattern.

    Empty list == conforms. `text` overrides the file contents so a doctored copy
    can be checked without writing it to disk.
    """
    text = path.read_text() if text is None else text
    is_down = path.name.endswith("_down.sql")
    mid = _ledger_id_for(path)
    bad: list[str] = []
    if not _BEGIN.search(text):
        bad.append("no `BEGIN;`")
    if not _COMMIT.search(text):
        bad.append("no `COMMIT;`")
    if is_down:
        if not _ON_ERROR_STOP.search(text):
            bad.append("no `\\set ON_ERROR_STOP on`")
        if not _ledger_delete(text, mid):
            bad.append(f"no `DELETE FROM koi_migrations WHERE migration_id = '{mid}';`")
    else:
        if not _ledger_insert(text, mid):
            bad.append(f"no `INSERT INTO koi_migrations (...) VALUES ('{mid}', ...) "
                       f"ON CONFLICT (migration_id) DO NOTHING;`")
    # Ordering that matters: a ledger row recorded BEFORE the assertion would survive
    # an assertion failure only in a non-transactional file, but the inverse order is
    # what 124/125 do and what keeps "recorded" == "postcondition held".
    if not is_down and not bad:
        ins = text.rfind("INSERT INTO koi_migrations")
        do_block = text.rfind("END $$;")
        if do_block != -1 and ins < do_block:
            bad.append("ledger INSERT precedes the final assertion block")
    return bad


class TestHousePattern:

    @pytest.mark.parametrize("name", [
        "124_entity_normalization_compat.sql",
        "124_entity_normalization_compat_down.sql",
        "125_extraction_provenance_and_quality.sql",
        "125_extraction_provenance_and_quality_down.sql",
    ])
    def test_positive_control_124_and_125_conform(self, name):
        """The checker must PASS on the migrations the pattern is copied from.

        If this fails, the checker is wrong, not the migration — and every
        assertion below about 126 is meaningless.
        """
        assert house_pattern_violations(MIGRATIONS / name) == []

    def test_negative_control_checker_rejects_a_missing_ledger_line(self):
        """Doctored 126-up with the ledger INSERT removed must be REJECTED."""
        text = UP_126.read_text()
        doctored = re.sub(
            r"INSERT\s+INTO\s+koi_migrations.*?DO\s+NOTHING\s*;", "", text, flags=re.S | re.I)
        bad = house_pattern_violations(UP_126, doctored)
        assert any("INSERT INTO koi_migrations" in b for b in bad), (
            f"checker accepted a 126-up with no ledger row; violations reported: {bad}")

    def test_126_up_conforms(self):
        assert house_pattern_violations(UP_126) == [], house_pattern_violations(UP_126)

    def test_126_down_conforms(self):
        assert house_pattern_violations(DOWN_126) == [], house_pattern_violations(DOWN_126)

    def test_126_up_records_the_agreed_checksum_label(self):
        text = UP_126.read_text()
        assert re.search(
            r"'personal:126_document_extraction_type_registry'\s*,\s*'v1_extractable_document_event'",
            text), "ledger row is present but not with checksum 'v1_extractable_document_event'"


# ═══════════════════════════════════════════════════════════════════════════
# 2. Dry run on the scratch database, rolled back
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    db = await c.fetchval("SELECT current_database()")
    assert db != "personal_koi", (
        f"refusing to run: connected to {db!r}. These tests write allowed_entity_types "
        f"and koi_migrations; the live graph is not an acceptable target.")
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()
    await c.close()


_META_LINE = re.compile(r"^\s*(BEGIN|COMMIT)\s*;\s*$|^\s*\\(set|copy)\b")


def _body(path: pathlib.Path) -> str:
    """The migration with its transaction wrapper and psql meta-commands removed.

    We are already inside the fixture's transaction, so the file's own BEGIN/COMMIT
    must go (a nested BEGIN only warns, but COMMIT would end the fixture's
    transaction and defeat the rollback). ``\\set``/``\\copy`` are psql-only and
    asyncpg would reject them. plpgsql's bare ``BEGIN`` (no semicolon) is untouched.
    """
    lines = path.read_text().splitlines()
    kept = [ln for ln in lines if not _META_LINE.match(ln)]
    return "\n".join(kept)


def _seed_sql_from_111() -> str:
    """The exact INSERT migration 111 seeds — 28 rows, 7 extractable."""
    text = SEED_111.read_text()
    m = re.search(
        r"INSERT\s+INTO\s+allowed_entity_types\s*\(entity_type,\s*description,\s*extractable\)\s*VALUES.*?"
        r"ON\s+CONFLICT\s*\(entity_type\)\s+DO\s+NOTHING\s*;",
        text, flags=re.S)
    assert m, "could not find migration 111's seed INSERT; the seed source moved"
    return m.group(0)


async def _seed_111(conn) -> None:
    await conn.execute(_seed_sql_from_111())
    # Control on the seed itself: if 111 no longer seeds 28/7, every assertion below
    # is about a different starting state than the one the migration documents.
    n_all = await conn.fetchval("SELECT count(*) FROM allowed_entity_types")
    assert n_all == 28, f"111 seeded {n_all} rows, expected 28"
    assert await _extractable(conn) == LEGACY_SEVEN


async def _extractable(conn) -> frozenset[str]:
    rows = await conn.fetch(
        "SELECT entity_type FROM allowed_entity_types "
        "WHERE extractable AND deprecated_at IS NULL")
    return frozenset(r["entity_type"] for r in rows)


async def _ledger_rows(conn) -> list[asyncpg.Record]:
    return await conn.fetch(
        "SELECT migration_id, checksum FROM koi_migrations WHERE migration_id = $1",
        LEDGER_ID_126)


@pytest.mark.anyio
class TestDryRun:

    async def test_up_establishes_the_nine_and_records_the_ledger_row(self, conn):
        await _seed_111(conn)
        assert await _ledger_rows(conn) == [], "scratch DB already has the 126 ledger row"

        await conn.execute(_body(UP_126))

        assert await _extractable(conn) == CONTRACT_NINE
        ledger = await _ledger_rows(conn)
        assert [dict(r) for r in ledger] == [
            {"migration_id": LEDGER_ID_126, "checksum": "v1_extractable_document_event"}]

    async def test_up_is_idempotent(self, conn):
        """Second application changes nothing and does not duplicate the ledger row."""
        await _seed_111(conn)
        await conn.execute(_body(UP_126))
        before = await conn.fetch(
            "SELECT entity_type, extractable, deprecated_at FROM allowed_entity_types "
            "ORDER BY entity_type")
        await conn.execute(_body(UP_126))
        after = await conn.fetch(
            "SELECT entity_type, extractable, deprecated_at FROM allowed_entity_types "
            "ORDER BY entity_type")
        assert [dict(r) for r in after] == [dict(r) for r in before]
        assert len(await _ledger_rows(conn)) == 1

    async def test_up_is_a_noop_on_a_registry_that_already_has_the_nine(self, conn):
        """A node where Document/Event are already extractable. Must not error."""
        await _seed_111(conn)
        await conn.execute(
            "INSERT INTO allowed_entity_types (entity_type, description, extractable) "
            "VALUES ('Document','x',true), ('Event','x',true)")
        assert await _extractable(conn) == CONTRACT_NINE
        await conn.execute(_body(UP_126))
        assert await _extractable(conn) == CONTRACT_NINE
        assert len(await _ledger_rows(conn)) == 1

    async def test_down_restores_the_seven_and_removes_the_ledger_row(self, conn):
        await _seed_111(conn)
        await conn.execute(_body(UP_126))
        assert await _extractable(conn) == CONTRACT_NINE          # precondition
        assert len(await _ledger_rows(conn)) == 1, (
            "up recorded no ledger row, so 'removes the ledger row' below is vacuous")

        await conn.execute(_body(DOWN_126))

        assert await _extractable(conn) == LEGACY_SEVEN
        assert await _ledger_rows(conn) == []
        # The rows themselves survive — 390 live entities carry these types.
        kept = await conn.fetch(
            "SELECT entity_type FROM allowed_entity_types "
            "WHERE entity_type IN ('Document','Event') ORDER BY 1")
        assert [r["entity_type"] for r in kept] == ["Document", "Event"]

    async def test_negative_control_the_up_assertion_is_a_set_check_not_a_count(self, conn):
        """'Meeting' wrongly extractable + 'CaseStudy' wrongly not: count stays 9.

        A COUNT assertion passes this. A SET assertion must RAISE and name both.
        """
        await _seed_111(conn)
        await conn.execute(
            "UPDATE allowed_entity_types SET extractable = true  WHERE entity_type = 'Meeting'")
        await conn.execute(
            "UPDATE allowed_entity_types SET extractable = false WHERE entity_type = 'CaseStudy'")
        # Prove the corruption really is count-preserving after the upsert would run:
        # 7 - CaseStudy + Meeting = 7, + Document + Event = 9.
        n_now = await conn.fetchval(
            "SELECT count(*) FROM allowed_entity_types WHERE extractable AND deprecated_at IS NULL")
        assert n_now == 7

        with pytest.raises(asyncpg.exceptions.PostgresError) as exc_info:
            await conn.execute(_body(UP_126))

        msg = str(exc_info.value)
        assert "Meeting" in msg and "CaseStudy" in msg, (
            f"the assertion fired but did not name both offenders: {msg!r}")

    async def test_negative_control_the_down_assertion_is_a_set_check_too(self, conn):
        """Down with a foreign designation present must RAISE and name it."""
        await _seed_111(conn)
        await conn.execute(_body(UP_126))
        await conn.execute(
            "UPDATE allowed_entity_types SET extractable = true  WHERE entity_type = 'Meeting'")
        await conn.execute(
            "UPDATE allowed_entity_types SET extractable = false WHERE entity_type = 'Protocol'")

        with pytest.raises(asyncpg.exceptions.PostgresError) as exc_info:
            await conn.execute(_body(DOWN_126))

        msg = str(exc_info.value)
        assert "Meeting" in msg and "Protocol" in msg, (
            f"the rollback assertion fired but did not name both offenders: {msg!r}")

    async def test_body_stripping_actually_removed_a_wrapper(self):
        """If the files carry no BEGIN;/COMMIT;, the DRY RUN above ran the raw file
        and the structural tests are the only thing noticing — say so here too.

        Counted as matched wrapper LINES, not bytes: "\\n".join(splitlines())
        always drops the trailing newline, so a byte comparison passed on the
        unwrapped first draft (and a line-count diff miscounts a trailing blank).
        """
        for p, want in ((UP_126, 2), (DOWN_126, 3)):     # BEGIN;/COMMIT; (+ \set)
            removed = [ln for ln in p.read_text().splitlines() if _META_LINE.match(ln)]
            assert len(removed) == want, (
                f"{p.name}: stripped {len(removed)} wrapper lines {removed!r}, expected {want}")
            assert not any(_META_LINE.match(ln) for ln in _body(p).splitlines())
