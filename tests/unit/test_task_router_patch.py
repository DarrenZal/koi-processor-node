"""Unit tests for task_router.py PATCH/ingest behaviour added 2026-10-09.

Covers:
  * PATCH {"notes": ...} appends to metadata->'notes' and GET returns it, without
    disturbing other metadata keys (49 rows already carry closed_* keys there);
  * notes longer than MAX_NOTE_CHARS are truncated, not rejected;
  * unknown PATCH / ingest fields are reported (`ignored_fields`) instead of
    vanishing, and do not break the update;
  * a non-empty owner/project link that does not resolve leaves the stored link
    alone and is reported as `unresolved` (it used to write NULL and wipe it);
  * an explicit empty string still clears the link.

Style mirrors tests/unit/test_knowledge_router.py: one asyncpg connection inside
a transaction that is rolled back at teardown, in KOI_TEST_DATABASE_URL; skipped
when that is unset.
"""

import json
import uuid

import asyncpg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from tests import koi_test_targets as targets



class _Pool:
    """Wraps one asyncpg.Connection so it quacks like asyncpg.Pool."""

    def __init__(self, conn):
        self._conn = conn

    class _CM:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *exc):
            return False

    def acquire(self):
        return self._CM(self._conn)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def harness():
    from api.routers.task_router import create_router

    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    app = FastAPI()
    app.include_router(create_router(_Pool(conn), caps=None), prefix="/tasks")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, conn

    await tx.rollback()
    await conn.close()


def _key(suffix):
    return f"unit-test-{suffix}-{uuid.uuid4().hex[:8]}"


async def _ingest(client, key, **extra):
    r = await client.post("/tasks/ingest", json={"taskKey": key, "title": "unit test task", "sourceType": "test", **extra})
    assert r.status_code == 200, r.text
    return r.json()


async def _get(client, key):
    r = await client.get(f"/tasks/{key}")
    assert r.status_code == 200, r.text
    return r.json()


async def _add_project(conn, name):
    """A Project entity the tier-3 resolver can find by normalized_text."""
    uri = f"orn:test.entity:project-{uuid.uuid4().hex[:10]}"
    await conn.execute(
        """
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
        VALUES ($1, $2, 'Project', $3)
        """,
        uri, name, name.lower(),
    )
    return uri


class TestNotes:
    @pytest.mark.anyio
    async def test_notes_append_and_are_returned(self, harness):
        client, conn = harness
        key = _key("notes")
        await _ingest(client, key)
        assert (await _get(client, key))["notes"] == []

        r1 = await client.patch(f"/tasks/{key}", json={"status": "cancelled", "notes": "Superseded by the sixteen meeting tasks."})
        assert r1.status_code == 200, r1.text
        assert "ignored_fields" not in r1.json()
        r2 = await client.patch(f"/tasks/{key}", json={"notes": "Second note, status untouched."})
        assert r2.status_code == 200, r2.text

        task = await _get(client, key)
        assert task["status"] == "cancelled"
        assert [n["text"] for n in task["notes"]] == [
            "Superseded by the sixteen meeting tasks.",
            "Second note, status untouched.",
        ]
        assert task["notes"][0]["status"] == "cancelled"
        assert task["notes"][0]["at"].endswith("Z")
        assert "metadata" not in task

    @pytest.mark.anyio
    async def test_notes_keep_other_metadata_keys(self, harness):
        client, conn = harness
        key = _key("meta")
        await _ingest(client, key)
        await conn.execute(
            "UPDATE task_registry SET metadata = $1::jsonb WHERE task_key = $2",
            json.dumps({"closed_reason": "legacy", "closed_by": "raw-sql"}), key,
        )
        r = await client.patch(f"/tasks/{key}", json={"notes": "kept alongside"})
        assert r.status_code == 200, r.text
        meta = json.loads(await conn.fetchval("SELECT metadata FROM task_registry WHERE task_key = $1", key))
        assert meta["closed_reason"] == "legacy"
        assert meta["closed_by"] == "raw-sql"
        assert [n["text"] for n in meta["notes"]] == ["kept alongside"]

    @pytest.mark.anyio
    async def test_notes_on_a_non_array_value_start_a_fresh_array(self, harness):
        client, conn = harness
        key = _key("badnotes")
        await _ingest(client, key)
        await conn.execute(
            "UPDATE task_registry SET metadata = $1::jsonb WHERE task_key = $2",
            json.dumps({"notes": "a string, not an array"}), key,
        )
        r = await client.patch(f"/tasks/{key}", json={"notes": "fresh"})
        assert r.status_code == 200, r.text
        assert [n["text"] for n in (await _get(client, key))["notes"]] == ["fresh"]

    @pytest.mark.anyio
    async def test_long_notes_are_truncated_not_rejected(self, harness):
        from api.routers.task_router import MAX_NOTE_CHARS

        client, conn = harness
        key = _key("long")
        await _ingest(client, key)
        r = await client.patch(f"/tasks/{key}", json={"notes": "x" * (MAX_NOTE_CHARS + 500)})
        assert r.status_code == 200, r.text
        assert len((await _get(client, key))["notes"][0]["text"]) == MAX_NOTE_CHARS

    @pytest.mark.anyio
    async def test_blank_notes_write_nothing(self, harness):
        client, conn = harness
        key = _key("blank")
        await _ingest(client, key)
        r = await client.patch(f"/tasks/{key}", json={"notes": "   ", "priority": "high"})
        assert r.status_code == 200, r.text
        task = await _get(client, key)
        assert task["notes"] == []
        assert task["priority"] == "high"


class TestUnknownFields:
    @pytest.mark.anyio
    async def test_patch_reports_unknown_fields_and_still_applies_known_ones(self, harness):
        client, conn = harness
        key = _key("extra")
        await _ingest(client, key)
        r = await client.patch(f"/tasks/{key}", json={"status": "done", "completedAt": "2026-10-09", "taskKey": key})
        assert r.status_code == 200, r.text
        assert r.json()["ignored_fields"] == ["completedAt", "taskKey"]
        assert (await _get(client, key))["status"] == "done"

    @pytest.mark.anyio
    async def test_ingest_with_unknown_field_still_creates(self, harness):
        client, conn = harness
        key = _key("ingextra")
        r = await client.post("/tasks/ingest", json={"taskKey": key, "title": "t", "sourceType": "test", "surprise": 1})
        assert r.status_code == 200, r.text
        assert r.json()["action"] == "created"
        assert (await _get(client, key))["task_key"] == key


class TestUnresolvedLinks:
    @pytest.mark.anyio
    async def test_unresolved_project_keeps_the_existing_project(self, harness):
        client, conn = harness
        name = f"Unit Test Project {uuid.uuid4().hex[:8]}"
        uri = await _add_project(conn, name)
        key = _key("proj")
        await _ingest(client, key, projectWikilink=f"[[Projects/{name}]]")
        assert (await _get(client, key))["project_uri"] == uri

        r = await client.patch(f"/tasks/{key}", json={"projectWikilink": "[[Projects/No Such Project Anywhere]]", "priority": "low"})
        assert r.status_code == 200, r.text
        assert r.json()["unresolved"] == ["projectWikilink"]
        task = await _get(client, key)
        assert task["project_uri"] == uri
        assert task["priority"] == "low"

    @pytest.mark.anyio
    async def test_unresolved_owner_keeps_the_existing_owner(self, harness):
        client, conn = harness
        key = _key("owner")
        await _ingest(client, key)
        await conn.execute("UPDATE task_registry SET owner_uri = 'orn:test.entity:person-kept' WHERE task_key = $1", key)
        r = await client.patch(f"/tasks/{key}", json={"ownerWikilink": "[[People/Nobody Resolvable Zz]]"})
        assert r.status_code == 200, r.text
        assert r.json()["unresolved"] == ["ownerWikilink"]
        assert (await _get(client, key))["owner_uri"] == "orn:test.entity:person-kept"

    @pytest.mark.anyio
    async def test_resolved_project_is_still_updated(self, harness):
        client, conn = harness
        first = await _add_project(conn, f"Unit Test A {uuid.uuid4().hex[:8]}")
        name_b = f"Unit Test B {uuid.uuid4().hex[:8]}"
        second = await _add_project(conn, name_b)
        key = _key("move")
        await _ingest(client, key)
        await conn.execute("UPDATE task_registry SET project_uri = $1 WHERE task_key = $2", first, key)
        r = await client.patch(f"/tasks/{key}", json={"projectWikilink": f"[[Projects/{name_b}]]"})
        assert r.status_code == 200, r.text
        assert "unresolved" not in r.json()
        assert (await _get(client, key))["project_uri"] == second

    @pytest.mark.anyio
    async def test_empty_string_still_clears_the_link(self, harness):
        client, conn = harness
        uri = await _add_project(conn, f"Unit Test C {uuid.uuid4().hex[:8]}")
        key = _key("clear")
        await _ingest(client, key)
        await conn.execute("UPDATE task_registry SET project_uri = $1 WHERE task_key = $2", uri, key)
        r = await client.patch(f"/tasks/{key}", json={"projectWikilink": ""})
        assert r.status_code == 200, r.text
        assert (await _get(client, key))["project_uri"] is None
