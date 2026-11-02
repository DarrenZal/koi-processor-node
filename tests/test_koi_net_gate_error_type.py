"""Pins the error type the membrane gate sends with a refused KOI-net request.

UNAPPROVED_EDGE and IDENTITY_REQUIRED must keep the default "invalid_signature". koi-net
2.0.x clients re-handshake when they receive "unknown_node", and their handshake broadcast
meets the same gate, so that mapping makes them loop.
See divergence 18 in docs/architecture/koi-protocol-conformance.md.
"""
import json

from api.koi_net_router import _protocol_error


def test_gate_refusals_keep_invalid_signature():
    for code in ("UNAPPROVED_EDGE", "IDENTITY_REQUIRED"):
        body = json.loads(_protocol_error(403, code, "refused").body)
        assert body["type"] == "error_response"
        assert body["error"] == "invalid_signature", (code, body["error"])
        assert body["error_code"] == code
