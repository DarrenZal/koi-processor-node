"""/manifests/fetch rejects a request whose `rids` is not a list of strings.

Without the check, a null RID reaches `not_found` and fails response validation (a 500
on this branch before the check; `regen-prod`, which leaves `not_found` empty, answers
200). The endpoint now answers 400 before the membrane gate or the database is touched.
"""
import json

import pytest

import api.koi_net_router as knr


@pytest.fixture
def call(monkeypatch):
    monkeypatch.setattr(knr, "_security_policy", lambda: {"require_approved_edge_for_poll": False})

    async def no_db(*_args, **_kwargs):
        raise AssertionError("a malformed request must not reach the database")

    monkeypatch.setattr(knr, "_db_pool", type("NoPool", (), {"acquire": no_db})())

    async def _call(payload):
        async def unwrap(_request, allow_unsigned=False):
            return payload, "orn:koi-net.node:peer+0", False

        monkeypatch.setattr(knr, "_unwrap_request", unwrap)
        response = await knr.manifests_fetch(None)
        return response.status_code, json.loads(response.body)

    return _call


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"rids": [None]},
        {"rids": ["orn:koi-net.node:x+1", None]},
        {"rids": [42]},
        {"rids": "orn:koi-net.node:x+1"},
        {"rids": None},
    ],
)
async def test_malformed_rids_get_400_not_500(call, payload):
    status, body = await call(payload)
    assert status == 400
    assert body["type"] == "error_response"
    assert body["error_code"] == "INVALID_PAYLOAD"


def test_requested_rids_accepts_strings_and_empty():
    assert knr._requested_rids({"rids": ["a", "b"]}) == ["a", "b"]
    assert knr._requested_rids({}) == []
    assert knr._requested_rids(None) == []
    assert knr._requested_rids(["not", "a", "dict"]) is None
