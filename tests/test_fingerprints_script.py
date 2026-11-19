"""scripts/federation/fingerprints.py, run as the operator and the peer run it (register 26):
the stored row on the approving node, `own` on the peer's node.

`own` reads a stand-in /koi-net/health on loopback. One fingerprint format everywhere: the
script, the /koi-net/edges/approve response (api.koi_net_router._key_fingerprint) and
lib.sh's compute_koi_fingerprint.
"""
import base64
import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from api import koi_net_router as knr
from api.koi_encryption import generate_encryption_keypair, get_encryption_public_key_b64
from api.node_identity import generate_keypair, get_public_key_der_b64

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "federation" / "fingerprints.py"


def _values():
    return {"node_rid": "orn:koi-net.node:fp-test+00", "base_url": "http://10.100.0.9:8400/koi-net",
            "public_key": get_public_key_der_b64(generate_keypair()),
            "encryption_key": get_encryption_public_key_b64(generate_encryption_keypair()[1])}


def _run(args, stdin=""):
    r = subprocess.run([sys.executable, "-I", str(SCRIPT), *args], input=stdin, capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    return r.stdout


def test_row_prints_the_stored_values_and_both_fingerprints():
    v = _values()
    out = _run(["row"], json.dumps(v))
    assert v["base_url"] in out and v["node_rid"] in out
    assert f"public key fingerprint:     {knr._key_fingerprint(v['public_key'])}" in out
    assert f"encryption key fingerprint: {knr._key_fingerprint(v['encryption_key'])}" in out
    assert "(none)" in _run(["row"], json.dumps({"node_rid": "x"}))  # a keyless row says so


def test_own_reads_this_nodes_health_on_loopback():
    v = _values()

    class Health(BaseHTTPRequestHandler):
        def do_GET(self):
            body = json.dumps({"status": "healthy", "node": v}).encode() if self.path == "/koi-net/health" else b"{}"
            self.send_response(200 if self.path == "/koi-net/health" else 404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    server = HTTPServer(("127.0.0.1", 0), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        out = _run(["own", "--port", str(server.server_address[1])])
    finally:
        server.shutdown()
    assert out == _run(["row"], json.dumps(v))  # the peer's own view equals the stored view


def test_one_fingerprint_format_in_the_script_the_api_and_lib_sh():
    v = _values()
    lib = subprocess.run(["bash", "-c", f"source {ROOT}/scripts/federation/lib.sh; compute_koi_fingerprint \"$1\"", "_",
                          v["public_key"]], capture_output=True, text=True, timeout=30)
    assert lib.returncode == 0, lib.stderr
    assert lib.stdout.strip() == knr._key_fingerprint(v["public_key"])
    assert knr._key_fingerprint(base64.b64encode(b"x" * 32).decode()).startswith("SHA256:")
    assert knr._key_fingerprint("not base64!") == "(unparseable)" and knr._key_fingerprint(None) is None
