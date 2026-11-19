#!/usr/bin/env python3
"""Fingerprints an operator compares out of band before approving a peer (register entry 26).

An unsigned first handshake can register a RID nobody has used yet with the caller's own
address and encryption key. So before approving, the operator compares what this node stored
for the peer (`row`) with what the peer reads off its own node (`own`), over Signal, as in
the SAS step.

A fingerprint is "SHA256:" + base64 (no padding) of SHA-256 over the key's decoded bytes,
the format of lib.sh's compute_koi_fingerprint. api.koi_net_router computes the same value
for the /koi-net/edges/approve response (tests/test_fingerprints_script.py pins both).

    fingerprints.py own [--port 8351]   this node's values, from its own /koi-net/health
    fingerprints.py row                 a stored node row given as JSON on stdin

Standard library only: it runs on a peer before anything else is installed.
"""
import argparse
import base64
import binascii
import hashlib
import json
import sys
import urllib.request


def key_fingerprint(b64):
    if not b64:
        return "(none)"
    try:
        raw = base64.b64decode("".join(str(b64).split()), validate=True)
    except (binascii.Error, ValueError):
        return "(unparseable)"
    return "SHA256:" + base64.b64encode(hashlib.sha256(raw).digest()).decode().rstrip("=")


def describe(values):
    return "\n".join([
        f"  node_rid:                   {values.get('node_rid') or '(none)'}",
        f"  base_url:                   {values.get('base_url') or '(none)'}",
        f"  public key fingerprint:     {key_fingerprint(values.get('public_key'))}",
        f"  encryption key fingerprint: {key_fingerprint(values.get('encryption_key'))}",
    ])


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    own = sub.add_parser("own", help="this node's values, from its /koi-net/health")
    own.add_argument("--port", type=int, default=8351)
    sub.add_parser("row", help="a stored koi_net_nodes row as JSON on stdin")
    args = ap.parse_args(argv)
    if args.cmd == "own":
        with urllib.request.urlopen(f"http://127.0.0.1:{args.port}/koi-net/health", timeout=10) as resp:
            values = (json.load(resp) or {}).get("node") or {}
    else:
        values = json.load(sys.stdin)
    print(describe(values))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
