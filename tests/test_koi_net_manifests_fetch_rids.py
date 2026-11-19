"""/manifests/fetch and /bundles/fetch reject a request whose `rids` is not a list of strings.

Without the check, a null RID reaches `not_found` and fails response validation (a 500
on this branch before the check; `regen-prod`, which leaves `not_found` empty, answers
200). The endpoint now answers 400 before the membrane gate or the database is touched.
/bundles/fetch gained the same check on 2026-11-05, when it began serving our own node
bundle ahead of the membrane gate (divergence 15).

Also here: our own node is served while the membrane refuses everything else, with no
database involved (divergence 15).
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

    async def _call(payload, endpoint=knr.manifests_fetch):
        async def unwrap(_request, allow_unsigned=False):
            return payload, "orn:koi-net.node:peer+0", False

        monkeypatch.setattr(knr, "_unwrap_request", unwrap)
        response = await endpoint(None)
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
@pytest.mark.parametrize("endpoint", [knr.manifests_fetch, knr.bundles_fetch], ids=["manifests", "bundles"])
async def test_malformed_rids_get_400_not_500(call, payload, endpoint):
    status, body = await call(payload, endpoint)
    assert status == 400
    assert body["type"] == "error_response"
    assert body["error_code"] == "INVALID_PAYLOAD"


def test_requested_rids_accepts_strings_and_empty():
    assert knr._requested_rids({"rids": ["a", "b"]}) == ["a", "b"]
    assert knr._requested_rids({}) == []
    assert knr._requested_rids(None) == []
    assert knr._requested_rids(["not", "a", "dict"]) is None


OWN = "orn:koi-net.node:conformance-own+" + "a" * 64


@pytest.fixture
def strict_refusal(monkeypatch):
    """The membrane is on and the caller has no approved edge; no database is reachable."""
    from api.koi_protocol import NodeProfile, NodeProvides

    monkeypatch.setattr(knr, "_security_policy", lambda: {"require_approved_edge_for_poll": True})
    monkeypatch.setattr(knr, "_node_profile", NodeProfile(
        node_rid=OWN, node_name="conformance-own", node_type="FULL", base_url="http://127.0.0.1:9/koi-net",
        provides=NodeProvides(event=["orn:koi-net.node"], state=["orn:koi-net.node"]), public_key="MFkw"))

    async def no_edge(*_args, **_kwargs):
        return False

    monkeypatch.setattr(knr, "_check_approved_edge", no_edge)

    async def no_db(*_args, **_kwargs):
        raise AssertionError("a refused request must not reach the database")

    monkeypatch.setattr(knr, "_db_pool", type("NoPool", (), {"acquire": no_db})())

    async def _call(payload, endpoint):
        async def unwrap(_request, allow_unsigned=False):
            return payload, "orn:koi-net.node:peer+" + "b" * 64, True

        monkeypatch.setattr(knr, "_unwrap_request", unwrap)
        monkeypatch.setattr(knr, "_wrap_response", lambda payload, _target, _signed: payload)
        response = await endpoint(None)
        return response.status_code, json.loads(response.body)

    return _call


@pytest.mark.asyncio
async def test_own_bundle_is_served_while_the_membrane_refuses(strict_refusal):
    other = "orn:koi-net.vault-file:Shared/x.md"
    status, body = await strict_refusal({"rids": [OWN, other]}, knr.bundles_fetch)
    assert status == 200
    assert [b["manifest"]["rid"] for b in body["bundles"]] == [OWN]
    assert other not in json.dumps(body)


@pytest.mark.asyncio
async def test_own_manifest_is_served_while_the_membrane_refuses(strict_refusal):
    status, body = await strict_refusal({"rids": [OWN]}, knr.manifests_fetch)
    assert status == 200
    assert [m["rid"] for m in body["manifests"]] == [OWN]


@pytest.mark.asyncio
async def test_own_rid_is_listed_while_the_membrane_refuses(strict_refusal):
    status, body = await strict_refusal({"rid_types": ["orn:koi-net.node"]}, knr.rids_fetch)
    assert status == 200
    assert body["rids"] == [OWN]
