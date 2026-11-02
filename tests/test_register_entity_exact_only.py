"""The Meeting backfill's exact-only registration switch is opt-in."""

import pytest

from api import personal_ingest_api as pia


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_exc):
        return False


class _Conn:
    def transaction(self):
        return _AsyncContext(self)

    async def fetchrow(self, query, *_args):
        if "private_count" in query:
            return {"total": 0, "private_count": 0}
        return None

    async def execute(self, *_args):
        return "UPDATE 1"


class _Pool:
    def __init__(self):
        self.conn = _Conn()

    def acquire(self):
        return _AsyncContext(self.conn)


async def _register(monkeypatch, *, exact_only=None):
    calls = []

    async def fake_resolve(
        conn,
        entity,
        context=None,
        skip_fuzzy=False,
        skip_cross_type=False,
        resolution_caller=None,
    ):
        calls.append({
            "skip_fuzzy": skip_fuzzy,
            "skip_cross_type": skip_cross_type,
            "resolution_caller": resolution_caller,
            "name": entity.name,
            "type": entity.type,
        })
        return pia.CanonicalEntity(
            name=entity.name,
            uri="orn:personal-koi.entity:meeting-existing",
            type=entity.type,
            is_new=False,
            confidence=1.0,
        ), False

    monkeypatch.setattr(pia, "db_pool", _Pool())
    monkeypatch.setattr(pia, "resolve_entity", fake_resolve)
    payload = {
        "name": "2026-08-22 Alpha Meeting",
        "entity_type": "Meeting",
        "force_type": True,
    }
    if exact_only is not None:
        payload["exact_only"] = exact_only
    response = await pia.register_vault_entity(pia.RegisterEntityRequest(**payload))
    assert response.success is True
    return calls


@pytest.mark.asyncio
async def test_negative_default_registration_keeps_full_resolution(monkeypatch):
    calls = await _register(monkeypatch)
    assert calls == [{
        "skip_fuzzy": False,
        "skip_cross_type": True,
        "resolution_caller": "personal_ingest_api.register_vault_entity",
        "name": "2026-08-22 Alpha Meeting",
        "type": "Meeting",
    }]


@pytest.mark.asyncio
async def test_exact_only_registration_sets_existing_skip_fuzzy_switch(monkeypatch):
    calls = await _register(monkeypatch, exact_only=True)
    assert calls[0]["skip_fuzzy"] is True
    assert calls[0]["skip_cross_type"] is True
    assert calls[0]["resolution_caller"] == "personal_ingest_api.register_vault_entity"
