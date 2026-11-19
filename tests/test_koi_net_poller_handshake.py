"""Our poller signs its handshakes and falls back to an unsigned one only for first contact.

A peer running koi-processor-node PR #15 lets a known node's profile change only under that
node's signature, so an unsigned handshake can no longer carry our new base_url or
encryption_key. A peer that does not know our key cannot verify a signature and answers
unknown_node: that, and only that, is retried unsigned.
"""
import json

import httpx
import pytest

import api.koi_poller as kp
from api.koi_protocol import NodeProfile, NodeProvides
from api.node_identity import derive_node_rid, generate_keypair, get_public_key_der_b64

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _poller():
    key = generate_keypair()
    rid = derive_node_rid("poller-test", key.public_key())
    profile = NodeProfile(node_rid=rid, node_name="poller-test", node_type="FULL", base_url="http://127.0.0.1:9/koi-net",
                          provides=NodeProvides(event=[], state=[]), public_key=get_public_key_der_b64(key))
    return kp.KOIPoller(pool=None, node_rid=rid, private_key=key, node_profile=profile)


def _route(monkeypatch, answers):
    seen = []

    def handler(request):
        body = json.loads(request.content)
        seen.append("signed" if "signature" in body else "unsigned")
        status, payload = answers[len(seen) - 1]
        return httpx.Response(status, json=payload)

    real = httpx.AsyncClient
    monkeypatch.setattr(kp.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    return seen


async def test_a_peer_that_knows_us_gets_only_the_signed_handshake(monkeypatch):
    seen = _route(monkeypatch, [(200, {"accepted": True})])
    assert await _poller()._send_handshake("orn:koi-net.node:peer+" + "a" * 64, "http://127.0.0.1:10/koi-net")
    assert seen == ["signed"]


async def test_first_contact_falls_back_to_unsigned(monkeypatch):
    seen = _route(monkeypatch, [(400, {"type": "error_response", "error": "unknown_node", "error_code": "UNKNOWN_SOURCE_NODE"}),
                                (200, {"accepted": True})])
    assert await _poller()._send_handshake("orn:koi-net.node:peer+" + "a" * 64, "http://127.0.0.1:10/koi-net")
    assert seen == ["signed", "unsigned"]


async def test_any_other_refusal_is_not_retried_unsigned(monkeypatch):
    """An unsigned retry after, say, KEY_MISMATCH or NODE_REJECTED would only try to slip past it."""
    seen = _route(monkeypatch, [(403, {"type": "error_response", "error": "invalid_signature", "error_code": "NODE_REJECTED"})])
    assert not await _poller()._send_handshake("orn:koi-net.node:peer+" + "a" * 64, "http://127.0.0.1:10/koi-net")
    assert seen == ["signed"]


async def test_the_signed_handshake_carries_a_current_issued_at_in_the_profile(monkeypatch):
    """Register 26, R1: the receiver refuses a replay of an older signed handshake by it. It
    rides in the profile, which an older peer's NodeProfile ignores (its HandshakeRequest
    forbids unknown top-level fields), and the signature covers it."""
    from datetime import datetime, timezone
    from api.koi_protocol import HandshakeRequest
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"accepted": True})

    real = httpx.AsyncClient
    monkeypatch.setattr(kp.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    assert await _poller()._send_handshake("orn:koi-net.node:peer+" + "a" * 64, "http://127.0.0.1:10/koi-net")
    (signed,) = bodies
    issued = datetime.fromisoformat(signed["payload"]["profile"]["issued_at"].replace("Z", "+00:00"))
    assert abs((datetime.now(timezone.utc) - issued).total_seconds()) < 60
    HandshakeRequest(**signed["payload"])  # still a valid handshake for a peer on older code
