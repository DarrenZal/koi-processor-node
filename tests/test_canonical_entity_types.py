"""The canonical entity-type vocabulary, and the drift it is supposed to prevent.

WHY Document AND Event ARE CANONICAL (2026-08-24)
-------------------------------------------------
They were 390 of the 421 rows carrying a type outside the canonical 28, and the obvious
reading — "non-canonical means wrong, retype them" — did not survive measurement:

  - They are not typos. They are schema.org types written deliberately by two intake
    pipelines (`johar-corpus-intake-v1` -> Document, `extract-session-entities` -> Event).
  - They are load-bearing: Document carries 598 relationship edges, Event carries 498
    document links. Retyping rewires all of it.
  - Retrieval never cared. A Document entity probed through /knowledge/unified-search
    ranks FIRST, identically to a canonical control, and all 390 rows carry embeddings.
  - Resolution never failed either: unknown types fall back to UNKNOWN_TYPE_SCHEMA, which
    is STRICTER (0.90/0.95) than most canonical types. They resolved conservatively.
  - No mapping into the existing 28 is defensible. An `Event` extracted from a session
    transcript is not a `Meeting` with attendees.

So the cost of them being "non-canonical" was a `Misc` vault folder and a log warning,
and the honest fix is to admit what the graph already contains.

WHAT THIS DOES NOT DO
---------------------
Admitting a type is not the same as enforcing the vocabulary. `allowed_entity_types`
(migration 111) holds the canonical list and is read by NOTHING — no foreign key on
entity_registry, no create-path guard. Until that changes, a cleanup regenerates: the
16-type tail accumulated exactly that way. Tracked as its own decision.
"""

from __future__ import annotations


import asyncpg
import asyncio
import pytest

from tests import koi_test_targets as targets

from api.entity_schema import (
    DEFAULT_SCHEMAS,
    UNKNOWN_TYPE_SCHEMA,
    get_entity_schemas,
    get_schema_for_type,
)

ADMITTED = ("Document", "Event")


@pytest.mark.parametrize("type_key", ADMITTED)
def test_admitted_types_have_a_real_schema(type_key: str) -> None:
    schema = get_schema_for_type(type_key)
    assert schema.type_key != UNKNOWN_TYPE_SCHEMA.type_key, (
        f"{type_key} still falls back to UNKNOWN_TYPE_SCHEMA. 390 live rows carry these "
        f"two types, with 598 edges and 498 document links between them."
    )
    assert schema.folder and schema.folder != UNKNOWN_TYPE_SCHEMA.folder, (
        f"{type_key} has no vault folder of its own, so it routes to Misc"
    )


@pytest.mark.parametrize("type_key", ADMITTED)
def test_admitted_types_are_in_the_database_allowlist(type_key: str) -> None:
    """The code list and allowed_entity_types must not diverge.

    Two sources of truth for one vocabulary is how a registry stops describing reality.
    """
    dsn = targets.database_url()

    async def fetch() -> set[str]:
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch("SELECT entity_type FROM allowed_entity_types")
            return {r["entity_type"] for r in rows}
        finally:
            await conn.close()

    allowed = asyncio.run(fetch())
    assert allowed, "allowed_entity_types is empty; a silent skip is what we are preventing"
    assert type_key in allowed, (
        f"{type_key} is canonical in code but absent from allowed_entity_types"
    )


def test_no_folder_collisions_across_the_vocabulary() -> None:
    """load_entity_schemas DROPS a type whose folder is already taken, with only a log
    line. A collision would silently shrink the vocabulary rather than fail."""
    folders: dict[str, str] = {}
    for key, schema in get_entity_schemas().items():
        assert schema.folder not in folders, (
            f"folder {schema.folder!r} claimed by both {folders[schema.folder]!r} "
            f"and {key!r}; one of them will be dropped at load"
        )
        folders[schema.folder] = key


def test_the_code_defaults_and_the_database_allowlist_agree() -> None:
    """Whole-vocabulary version of the per-type check, so a THIRD type cannot drift in."""
    dsn = targets.database_url()

    async def fetch() -> set[str]:
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch("SELECT entity_type FROM allowed_entity_types")
            return {r["entity_type"] for r in rows}
        finally:
            await conn.close()

    allowed = asyncio.run(fetch())
    in_code = set(DEFAULT_SCHEMAS)
    only_code = sorted(in_code - allowed)
    only_db = sorted(allowed - in_code)
    assert not only_code and not only_db, (
        f"vocabulary drift — only in DEFAULT_SCHEMAS: {only_code}; "
        f"only in allowed_entity_types: {only_db}"
    )
