"""connect-peers.sh treats the remote node's /health as untrusted data.

The node_rid and node_name come from the remote. A crafted value must not run code on the operator's machine, and a
node_rid that is not a well-formed node RID makes the script refuse before any further step. The script runs for
real with bash, against a fake curl (no network), a stub psycopg2 that only records its parameters, and temp
directories for its state, registry and config: nothing outside the test's temp dir is read as config or written.
Run: python -B -m pytest -q -p no:cacheprovider tests/peer_guards
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "federation" / "connect-peers.sh"
LIB = REPO / "scripts" / "federation" / "lib.sh"
HEX16 = "0" * 16
GOOD_RID = "orn:koi-net.node:remote-peer+" + "a" * 64
KEY = "AAAAB3NzaC1lZDI1NTE5AAAA"  # any base64: only its fingerprint is printed

FAKE_CURL = r"""#!/bin/bash
# Fake curl: log every call, answer health and handshake from files, never touch the network.
printf '%s\n' "$*" >> "$NC_CURL_LOG"
url="${@: -1}"
case "$url" in
  *127.0.0.1*/koi-net/health) cat "$NC_LOCAL_HEALTH" ;;
  */koi-net/health) cat "$NC_REMOTE_HEALTH" ;;
  */koi-net/handshake)
    # The remote handshake asks only for the status code (-o /dev/null -w '%{http_code}').
    if [[ "$*" == *'%{http_code}'* ]]; then printf '200'
    else echo '{"type": "handshake_response", "accepted": true, "edge_status": "PROPOSED"}'; fi ;;
  */koi-net/edges/approve) echo '{"status": "approved"}' ;;
  *) echo '{}' ;;
esac
exit 0
"""

STUB_PSYCOPG2 = """import json, os
class _C:
    def cursor(self): return self
    def execute(self, sql, params=None):
        with open(os.environ["NC_PSY_LOG"], "a") as f:
            f.write(json.dumps(list(params or [])) + "\\n")
    def fetchone(self): return None
    def close(self): pass
    autocommit = False
def connect(url): return _C()
"""


def run_script(tmp_path, rid, name="remote-peer", alias="remote"):
    """Run connect-peers.sh against a remote whose /health reports `rid` and `name`. Returns (proc, markers, logs)."""
    bin_dir, stub, cfg, state = (tmp_path / d for d in ("bin", "stub", "kp", "state"))
    for d in (bin_dir, stub, cfg / "config", state, tmp_path / "home"):
        d.mkdir(parents=True, exist_ok=True)
    (bin_dir / "curl").write_text(FAKE_CURL)
    (bin_dir / "curl").chmod(0o755)
    (bin_dir / "python3").symlink_to(sys.executable)
    (stub / "psycopg2.py").write_text(STUB_PSYCOPG2)
    (cfg / "config" / "personal.env").write_text(
        "KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=true\nKOI_ENFORCE_SOURCE_KEY_RID_BINDING=true\n"
        "KOI_BASE_URL=http://192.0.2.10:8351\n")
    (state / "admin_token").write_text("test-admin-token-not-a-real-secret\n")
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps([{"peer_name": "someone-else", "node_rid": GOOD_RID, "koi_pubkey": ""}]))
    remote = tmp_path / "remote.json"
    remote.write_text(json.dumps({"node": {"node_rid": rid, "node_name": name, "node_type": "FULL",
                                           "public_key": KEY, "provides": {"event": [], "state": []}}}))
    local = tmp_path / "local.json"
    local.write_text(json.dumps({"node": {"node_rid": "orn:koi-net.node:local+" + "b" * 64, "node_name": "local",
                                          "node_type": "FULL", "public_key": KEY}}))
    markers = [tmp_path / f"mark{i}" for i in (1, 2, 3)]
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path / "home"), "PYTHONPATH": str(stub),
        "PYTHONDONTWRITEBYTECODE": "1", "KOI_PROCESSOR_DIR": str(cfg), "KOI_STATE_DIR": str(state),
        "PEER_REGISTRY": str(registry), "NC_CURL_LOG": str(tmp_path / "curl.log"), "NC_PSY_LOG": str(tmp_path / "psy.log"),
        "NC_REMOTE_HEALTH": str(remote), "NC_LOCAL_HEALTH": str(local),
        "NC_MARK1": str(markers[0]), "NC_MARK2": str(markers[1]), "NC_MARK3": str(markers[2]),
    }
    proc = subprocess.run(["/bin/bash", str(SCRIPT), "http://peer.invalid:8351", alias], input="y\ny\n", env=env,
                          capture_output=True, text=True, timeout=60)
    logs = {k: (tmp_path / f).read_text() if (tmp_path / f).exists() else "" for k, f in
            (("curl", "curl.log"), ("psy", "psy.log"))}
    return proc, [m.exists() for m in markers], logs


def test_quote_breakout_in_node_rid_runs_nothing_and_is_refused(tmp_path):
    rid = f"orn:koi-net.node:x+{HEX16}'), __import__('pathlib').Path(__import__('os').environ['NC_MARK1']).touch(), ('"
    proc, ran, logs = run_script(tmp_path, rid)
    assert ran == [False, False, False], ("code from the node_rid ran", proc.stderr[-400:])
    assert proc.returncode != 0 and "Refusing" in proc.stderr, (proc.returncode, proc.stderr[-400:])
    assert "/koi-net/handshake" not in logs["curl"], logs["curl"]


def test_command_substitution_in_node_rid_is_refused(tmp_path):
    rid = f"orn:koi-net.node:x+{HEX16}$(touch \"$NC_MARK2\")"
    proc, ran, logs = run_script(tmp_path, rid)
    assert ran == [False, False, False], proc.stderr[-400:]
    assert proc.returncode != 0 and "Refusing" in proc.stderr, (proc.returncode, proc.stderr[-400:])
    assert "/koi-net/handshake" not in logs["curl"], logs["curl"]


def test_crafted_node_name_runs_nothing_in_the_registry_lookup(tmp_path):
    name = "x' or __import__('pathlib').Path(__import__('os').environ['NC_MARK3']).touch() or '"
    proc, ran, logs = run_script(tmp_path, GOOD_RID, name=name)
    assert ran == [False, False, False], ("code from the node_name ran", proc.stderr[-400:])
    assert proc.returncode == 0, (proc.returncode, proc.stderr[-400:])


def test_well_formed_peer_still_connects(tmp_path):
    """Control: a well-formed node_rid passes, both handshakes are sent, and the RID reaches SQL as a parameter."""
    proc, ran, logs = run_script(tmp_path, GOOD_RID)
    assert proc.returncode == 0, (proc.returncode, proc.stderr[-400:])
    assert logs["curl"].count("/koi-net/handshake") == 2, logs["curl"]
    assert GOOD_RID in logs["psy"], logs["psy"]
    assert ran == [False, False, False]


INLINE_PY = re.compile(r'python3 -c "(.*?)"(?=\s|\)|$)', re.S)
SHELL_VAR = re.compile(r"\$\{?[A-Za-z_][A-Za-z0-9_]*")


def test_no_shell_value_inside_inline_python():
    """Every inline Python block in connect-peers.sh, and the registry lookup it calls, takes values from the
    environment, never pasted into its source."""
    src = SCRIPT.read_text()
    lib = LIB.read_text()
    lookup = lib[lib.index("peer_registry_lookup() {"):lib.index("\n}\n", lib.index("peer_registry_lookup() {"))]
    bodies = [("connect-peers.sh", src[:m.start()].count("\n") + 1, m.group(1)) for m in INLINE_PY.finditer(src)]
    bodies += [("lib.sh peer_registry_lookup", 0, m.group(1)) for m in INLINE_PY.finditer(lookup)]
    assert len(bodies) >= 10  # the pattern must find the blocks at all
    offenders = [(f, line, SHELL_VAR.findall(b)) for f, line, b in bodies if SHELL_VAR.search(b)]
    assert offenders == [], offenders
