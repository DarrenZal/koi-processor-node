"""scripts/federation/approve-peer-edges.sh, run with bash as the operator runs it, with psql
and curl replaced on PATH by recorders (no database, no network).

Register 26: the script shows the stored address and fingerprints
and approves only after the operator confirms they match the peer's own values. Also the
same rule as connect-peers.sh: the admin token never reaches curl's
argv, and the peer name never becomes SQL text.
"""
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "federation" / "approve-peer-edges.sh"
TOKEN = "approve-script-test-token"
LOCAL = "orn:koi-net.node:local-test+11"
PEER = "orn:koi-net.node:peer-test+22"
ROW = {"node_rid": PEER, "node_name": "peer-test", "base_url": "http://10.100.0.9:8400/koi-net",
       "public_key": "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE", "encryption_key": "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg="}

PSQL = r'''#!/usr/bin/env python3
import json, os, sys
sql = sys.stdin.read()
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(json.dumps({"tool": "psql", "argv": sys.argv[1:], "stdin": sql}) + "\n")
if "koi_net_peer_aliases" in sql:
    print(os.environ["STUB_PEER"])
elif "json_build_object" in sql:
    print(os.environ["STUB_ROW"])
elif "FROM koi_net_edges" in sql:
    print("orn:koi-net.edge:test-1")
'''
CURL = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
headers = []
for i, a in enumerate(args):
    if a == "-H" and args[i + 1].startswith("@"):
        headers.append(open(args[i + 1][1:]).read())
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(json.dumps({"tool": "curl", "argv": args, "header_files": headers}) + "\n")
url = [a for a in args if a.startswith("http")][-1]
if url.endswith("/koi-net/health"):
    print(json.dumps({"node": {"node_rid": os.environ["STUB_LOCAL"]}}))
else:
    print(json.dumps({"status": "approved"}))
'''


def _run(tmp_path, answer, peer_id="x'; DROP TABLE koi_net_nodes; --"):
    stubs = tmp_path / "bin"
    stubs.mkdir()
    for name, body in (("psql", PSQL), ("curl", CURL)):
        (stubs / name).write_text(body)
        (stubs / name).chmod(0o755)
    log = tmp_path / "calls.jsonl"
    env = {"PATH": f"{stubs}:{os.environ['PATH']}", "HOME": str(tmp_path), "KOI_STATE_DIR": str(tmp_path / "state"),
           "KOI_ADMIN_TOKEN": TOKEN, "STUB_LOG": str(log), "STUB_PEER": PEER, "STUB_LOCAL": LOCAL,
           "STUB_ROW": json.dumps(ROW), "KOI_API_PORT": "9"}
    r = subprocess.run(["bash", str(SCRIPT), peer_id], input=answer, capture_output=True, text=True, env=env, timeout=60)
    calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
    return r, calls


def test_it_shows_the_stored_values_and_approves_after_a_yes(tmp_path):
    r, calls = _run(tmp_path, "y\n")
    assert r.returncode == 0, r.stdout + r.stderr
    assert ROW["base_url"] in r.stdout and "public key fingerprint:     SHA256:" in r.stdout
    assert "encryption key fingerprint: SHA256:" in r.stdout
    approves = [c for c in calls if c["tool"] == "curl" and any(a.endswith("/edges/approve") for a in c["argv"])]
    assert len(approves) == 1
    assert approves[0]["header_files"] == [f"Authorization: Bearer {TOKEN}\n"]  # through a file descriptor
    assert "Approved: 1" in r.stdout


def test_a_no_approves_nothing(tmp_path):
    r, calls = _run(tmp_path, "n\n")
    assert r.returncode == 1
    assert not [c for c in calls if c["tool"] == "curl" and any(a.endswith("/edges/approve") for a in c["argv"])]


def test_the_token_never_reaches_argv_and_the_name_never_becomes_sql(tmp_path):
    peer_id = "x'; DROP TABLE koi_net_nodes; --"
    r, calls = _run(tmp_path, "y\n", peer_id)
    assert r.returncode == 0, r.stdout + r.stderr
    assert calls and not any(TOKEN in a for c in calls for a in c["argv"])
    psql = [c for c in calls if c["tool"] == "psql"]
    assert psql and not any(peer_id in c["stdin"] or PEER in c["stdin"] or LOCAL in c["stdin"] for c in psql)
    assert ["-v", f"id={peer_id}"] == psql[0]["argv"][3:5]
