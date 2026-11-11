"""Federation scripts treat values that another party wrote as data, never as Python source.

These scripts and lib.sh functions used to paste values another party wrote into `python3 -c`
source:

- approve-peer.sh --from-file reads a join request that the joining peer wrote. Its
  koi_public_key and node_rid were never validated, and the registry entry was built by pasting
  them into Python source, on every platform.
- lib.sh peer_registry_add and peer_registry_update_status paste their values into source in
  the branch taken when `flock` exists (Linux). The entry carries the join request's values,
  and remove-peer.sh passes a peer name that can be a remote node's own node_name.
- lib.sh decode_invite_token pasted the invite token, which the inviting side wrote, into
  source on the joining peer's machine, before any check of its format.
- remove-peer.sh pasted the node_rid, from the registry (a join request) or the database (a
  peer's handshake), into its retraction and revocation blocks.

Each script runs for real with bash: a fake `ssh` that only records and exits 97, a fake `flock`
where the Linux branch is wanted, a psycopg2 stub that records its parameters, a POSTGRES_URL
that cannot resolve, and temp directories for state, registry and config.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FED = REPO / "scripts" / "federation"
# bootstrap-node.sh is not in every copy of this repository; its tests skip where it is absent.
needs_bootstrap = pytest.mark.skipif(not (FED / "bootstrap-node.sh").exists(),
                                     reason="scripts/federation/bootstrap-node.sh is not in this tree")
LIB = FED / "lib.sh"
TOUCH = "__import__('pathlib').Path(__import__('os').environ['NC_MARK']).touch()"
WG_KEY = "A" * 43 + "="  # 44-character base64, as validate_wg_pubkey requires
GOOD_RID = "orn:koi-net.node:peer-x+" + "a" * 64

FAKE_SSH = '#!/bin/bash\nprintf "%s\\n" "$*" >> "$NC_SSH_LOG"\nexit 97\n'
FAKE_FLOCK = "#!/bin/bash\nexit 0\n"  # lets lib.sh take its `command -v flock` branch
STUB_PSYCOPG2 = """import json, os
class _C:
    rowcount = 0
    autocommit = False
    def cursor(self): return self
    def execute(self, sql, params=None):
        with open(os.environ["NC_PSY_LOG"], "a") as f:
            f.write(json.dumps(list(params or [])) + "\\n")
    def fetchone(self): return (False,)
    def fetchall(self): return []
    def close(self): pass
def connect(url): return _C()
"""


def _env(tmp_path: Path, flock: bool) -> dict:
    bin_dir, stub, kp, state = (tmp_path / d for d in ("bin", "stub", "kp", "state"))
    for d in (bin_dir, stub, kp / "config", state, tmp_path / "home"):
        d.mkdir(parents=True, exist_ok=True)
    (bin_dir / "ssh").write_text(FAKE_SSH)
    (bin_dir / "ssh").chmod(0o755)
    if flock:
        (bin_dir / "flock").write_text(FAKE_FLOCK)
        (bin_dir / "flock").chmod(0o755)
    (bin_dir / "python3").symlink_to(sys.executable)
    (stub / "psycopg2.py").write_text(STUB_PSYCOPG2)
    (kp / "config" / "personal.env").write_text("POSTGRES_URL=postgresql://koi-test.invalid/none\nKOI_NODE_NAME=local-test\n")
    return {
        "PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path / "home"), "PYTHONPATH": str(stub),
        "PYTHONDONTWRITEBYTECODE": "1", "KOI_PROCESSOR_DIR": str(kp), "KOI_STATE_DIR": str(state),
        "PEER_REGISTRY": str(tmp_path / "registry.json"), "NC_MARK": str(tmp_path / "ran"),
        "NC_SSH_LOG": str(tmp_path / "ssh.log"), "NC_PSY_LOG": str(tmp_path / "psy.log"),
        "POSTGRES_URL": "postgresql://koi-test.invalid/none",
    }


def _bash(args: list[str], env: dict, stdin: str = "") -> subprocess.CompletedProcess:
    # In the test's temp dir: approve-peer.sh writes federation-invite-<peer>/ into the current directory.
    return subprocess.run(["/bin/bash", *args], env=env, input=stdin, capture_output=True, text=True, timeout=60,
                          cwd=Path(env["NC_MARK"]).parent)


def _lib(call: str, env: dict, **values: str) -> subprocess.CompletedProcess:
    """Call one lib.sh function the way the scripts do: source lib.sh, then call it with
    arguments that reach it as shell variables."""
    return _bash(["-c", f'source "$NC_LIB"; {call}'], {**env, "NC_LIB": str(LIB), **values})


def _registry(env: dict) -> list:
    return json.loads(Path(env["PEER_REGISTRY"]).read_text())


def _join_request(tmp_path: Path, koi: str, rid: str) -> Path:
    join = tmp_path / "join.txt"
    join.write_text("=== KOI-net Join Request ===\n"
                    f"peer_name: peer-x\nwg_public_key: {WG_KEY}\nkoi_public_key: {koi}\nnode_rid: {rid}\n"
                    "==========================\n")
    return join


def _approve_from_file(tmp_path: Path, env: dict, koi: str, rid: str) -> subprocess.CompletedProcess:
    join = _join_request(tmp_path, koi, rid)
    return _bash([str(FED / "approve-peer.sh"), "--dry-run", "--from-file", str(join), "5", "user@relay.invalid"], env)


def test_a_crafted_join_request_runs_nothing_in_approve_peer(tmp_path):
    env = _env(tmp_path, flock=False)
    koi = f"x' + str({TOUCH}) + '"
    rid = f"orn:koi-net.node:x+{'0' * 16}' + str({TOUCH}) + '"
    r = _approve_from_file(tmp_path, env, koi, rid)
    assert not Path(env["NC_MARK"]).exists(), ("code from the join request ran", r.stdout[-600:], r.stderr[-600:])
    assert r.returncode == 1 and "Refusing" in r.stderr and "Would add" not in r.stdout, (r.stdout[-600:], r.stderr[-600:])


P256_DER = bytes.fromhex("3059301306072a8648ce3d020106082a8648ce3d03010703420004") + bytes(range(64))
P256_KEY = base64.b64encode(P256_DER).decode()
B64_64 = hashlib.sha256(P256_KEY.encode()).hexdigest()


def test_approve_peer_refuses_a_join_request_whose_identity_does_not_check_out(tmp_path):
    """Hardening: the node RID's shape, the key's encoding, and the RID derived from the key."""
    env = _env(tmp_path, flock=False)
    other = base64.b64encode(P256_DER[:-1] + b"\xff").decode()
    cases = {
        "rid with a space": (P256_KEY, f"orn:koi-net.node:peer x+{B64_64}"),
        "rid hash too short": (P256_KEY, f"orn:koi-net.node:peer-x+{B64_64[:15]}"),
        "rid not a node RID": (P256_KEY, f"orn:koi-net.edge:peer-x+{B64_64}"),
        "key not base64": ("not base64!", f"orn:koi-net.node:peer-x+{B64_64}"),
        "key not P-256 DER": (base64.b64encode(b"\x30" * 91).decode(), f"orn:koi-net.node:peer-x+{B64_64}"),
        "key not canonical": (P256_KEY[:-3] + "B==", f"orn:koi-net.node:peer-x+{B64_64}"),
        "rid from another key": (other, f"orn:koi-net.node:peer-x+{B64_64}"),
        "no key": ("", f"orn:koi-net.node:peer-x+{B64_64}"),
    }
    for case, (koi, rid) in cases.items():
        r = _approve_from_file(tmp_path, env, koi, rid)
        assert r.returncode == 1 and "Refusing" in r.stderr and "Would add" not in r.stdout, (case, r.stderr[-400:])


def test_a_well_formed_join_request_is_approved_with_its_identity(tmp_path):
    """Control: each RID form api/node_identity.py accepts passes, and reaches the entry as written."""
    env = _env(tmp_path, flock=False)
    for suffix in (B64_64, B64_64[:16], hashlib.sha256(P256_DER).hexdigest()):
        rid = f"orn:koi-net.node:peer-x+{suffix}"
        r = _approve_from_file(tmp_path, env, P256_KEY, rid)
        assert r.returncode == 0, (suffix, r.stderr[-600:])
        shown = r.stdout[r.stdout.index("Would add to peer registry:"):]
        entry = json.loads(shown[shown.index("{"):shown.index("}") + 1])
        assert (entry["koi_pubkey"], entry["node_rid"], entry["peer_number"]) == (P256_KEY, rid, 5), entry


def _files(root: Path) -> dict:
    return {str(f.relative_to(root)): f.read_bytes() for f in sorted(root.rglob("*")) if f.is_file()}


def test_approve_peer_dry_run_writes_nothing(tmp_path):
    """Each mode under --dry-run leaves the test's temp dir (its working directory) unchanged."""
    env = _env(tmp_path, flock=False)
    join = _join_request(tmp_path, P256_KEY, f"orn:koi-net.node:peer-x+{B64_64}")
    before = _files(tmp_path)
    r = _bash([str(FED / "approve-peer.sh"), "--dry-run", "peer-x", "5", WG_KEY, "user@relay.invalid"], env)
    assert r.returncode == 0 and "[DRY-RUN] Would write federation-invite-peer-x/wg-koi.conf:" in r.stdout, r.stderr[-400:]
    assert "PrivateKey = <PASTE_YOUR_PRIVATE_KEY_HERE>\n" in r.stdout
    r = _bash([str(FED / "approve-peer.sh"), "--dry-run", "--from-file", str(join), "5", "user@relay.invalid"], env)
    assert r.returncode == 0, r.stderr[-400:]
    r = _bash([str(FED / "approve-peer.sh"), "--dry-run", "--pubkey-only", WG_KEY, "5", "user@relay.invalid"], env)
    assert r.returncode == 1 and "No invite found" in r.stderr, r.stderr[-400:]  # no registry: no match, none created
    assert _files(tmp_path) == before, sorted(set(_files(tmp_path)) ^ set(before))


def test_pubkey_only_checks_the_number_and_key_before_using_them(tmp_path):
    env = _env(tmp_path, flock=False)
    Path(env["PEER_REGISTRY"]).write_text(json.dumps([{"peer_name": "peer-x", "peer_number": 5, "status": "approving"}]))
    r = _bash([str(FED / "approve-peer.sh"), "--pubkey-only", WG_KEY, f"5 or str({TOUCH})", "user@relay.invalid"], env)
    assert not Path(env["NC_MARK"]).exists(), r.stderr[-600:]
    assert r.returncode == 1 and "Invalid peer number" in r.stderr, r.stderr[-600:]
    r = _bash([str(FED / "approve-peer.sh"), "--pubkey-only", "AAAA$(touch $NC_MARK)AAAA", "5", "user@relay.invalid"], env)
    assert not Path(env["NC_MARK"]).exists(), r.stderr[-600:]
    assert r.returncode == 1 and "Invalid WireGuard public key" in r.stderr, r.stderr[-600:]
    assert not Path(env["NC_SSH_LOG"]).exists()  # the stale-approving branch queries the relay only after the checks


def test_wg_pubkey_check_takes_only_32_byte_canonical_base64(tmp_path):
    env = _env(tmp_path, flock=False)
    good = [WG_KEY, base64.b64encode(bytes(range(32))).decode(), base64.b64encode(b"\xff" * 32).decode()]
    bad = ["A" * 42 + "==", "A" * 44, "AAAA AAAA" + "A" * 34 + "=", "A" * 20 + "\n" + "A" * 22 + "=", "A" * 42 + "B=",
           "A" * 43, "A" * 20 + "$(id)" + "A" * 18 + "="]
    for key in good + bad:
        r = _lib('validate_wg_pubkey "$NC_KEY"', env, NC_KEY=key)
        assert (r.returncode == 0) == (key in good), (repr(key), r.returncode, r.stderr[-300:])


def test_a_crafted_entry_runs_nothing_in_peer_registry_add_with_flock(tmp_path):
    env = _env(tmp_path, flock=True)
    Path(env["PEER_REGISTRY"]).write_text("[]")
    entry = {"peer_name": "peer-x", "koi_pubkey": f"x''' + str({TOUCH}) + '''", "status": "active"}
    r = _lib('peer_registry_add "$NC_ENTRY"', env, NC_ENTRY=json.dumps(entry))
    assert not Path(env["NC_MARK"]).exists(), ("code from the entry ran", r.stderr[-600:])
    assert r.returncode == 0, r.stderr[-600:]
    assert _registry(env) == [entry]


def test_a_crafted_name_runs_nothing_in_peer_registry_update_status_with_flock(tmp_path):
    env = _env(tmp_path, flock=True)
    name = f"p' or str({TOUCH}) or '"
    stamp = f"t' + str({TOUCH}) + '"
    Path(env["PEER_REGISTRY"]).write_text(json.dumps([{"peer_name": name, "status": "active"}]))
    r = _lib('peer_registry_update_status "$NC_NAME" removed removed_at "$NC_STAMP"', env, NC_NAME=name, NC_STAMP=stamp)
    assert not Path(env["NC_MARK"]).exists(), ("code from the name ran", r.stderr[-600:])
    assert r.returncode == 0, r.stderr[-600:]
    assert _registry(env) == [{"peer_name": name, "status": "removed", "removed_at": stamp}]


@needs_bootstrap
def test_a_crafted_invite_token_runs_nothing_in_bootstrap(tmp_path):
    env = _env(tmp_path, flock=False)
    token = f"x') if str({TOUCH}) else decode_token('"
    r = _bash([str(FED / "bootstrap-node.sh"), "--dry-run", "--invite", token], env)
    assert not Path(env["NC_MARK"]).exists(), ("code from the invite token ran", r.stderr[-600:])
    assert r.returncode != 0 and "Failed to decode invite token" in r.stderr, (r.returncode, r.stderr[-600:])


def test_a_well_formed_invite_token_still_decodes(tmp_path):
    env = _env(tmp_path, flock=False)
    payload = {"peer_name": "peer-x", "peer_number": 5, "expires_at": 4102444800}
    sys.path.insert(0, str(FED))
    try:
        from invite_token import create_token
    finally:
        sys.path.remove(str(FED))
    token = create_token(payload, b"test-secret-not-real")
    r = _lib('decode_invite_token "$NC_TOKEN"', env, NC_TOKEN=token)
    assert r.returncode == 0, r.stderr[-600:]
    assert json.loads(r.stdout) == payload


def _invite(**overrides) -> str:
    sys.path.insert(0, str(FED))
    try:
        from invite_token import create_token
    finally:
        sys.path.remove(str(FED))
    payload = {"v": 1, "jti": "j", "peer_name": "peer-x", "peer_number": 5, "relay_pubkey": WG_KEY,
               "relay_endpoint": "relay.invalid:51820", "admin_base_url": "http://10.100.0.2:8400",
               "admin_wg_ip": "10.100.0.2", "vault_sync_folder": "Shared", "issued_at": 0, "expires_at": 4102444800}
    return create_token({**payload, **overrides}, b"test-secret-not-real")


@needs_bootstrap
def test_bootstrap_refuses_invite_values_that_would_reach_root_config_or_the_local_api(tmp_path):
    """The relay key and endpoint go into wg-koi.conf (read by wg-quick as root), the admin URL to curl, and the
    folder into a JSON body sent to this node's own API with its admin token."""
    env = _env(tmp_path, flock=False)
    cases = {
        "relay key": {"relay_pubkey": "AAAA$(touch $NC_MARK)AAAA"},
        "relay endpoint newline": {"relay_endpoint": "relay.invalid:51820\n[Interface]\nPostUp = touch /tmp/x"},
        "relay endpoint no port": {"relay_endpoint": "relay.invalid"},
        "admin URL scheme": {"admin_base_url": "file:///etc/passwd"},
        "admin URL path": {"admin_base_url": "http://10.100.0.2:8400/x?y"},
        "folder traversal": {"vault_sync_folder": "../x"},
        "folder JSON": {"vault_sync_folder": 'Shared", "peer": "admin", "shared_folder": "."'},
        "folder root": {"vault_sync_folder": "."},
    }
    for case, values in cases.items():
        r = _bash([str(FED / "bootstrap-node.sh"), "--dry-run", "--invite", _invite(**values)], env)
        assert not Path(env["NC_MARK"]).exists(), (case, r.stderr[-400:])
        assert r.returncode == 1 and "Refusing the invite" in r.stderr, (case, r.returncode, r.stderr[-400:])


@needs_bootstrap
def test_bootstrap_accepts_a_well_formed_invite(tmp_path):
    """Control: well-formed values pass the checks, and the run stops at the --dry-run refusal that follows."""
    env = _env(tmp_path, flock=False)
    for values in ({}, {"vault_sync_folder": None}, {"vault_sync_folder": "Team notes_2"},
                   {"admin_base_url": "https://admin.invalid"}):
        r = _bash([str(FED / "bootstrap-node.sh"), "--dry-run", "--invite", _invite(**values)], env)
        assert "Refusing" not in r.stderr and "--dry-run is not supported with --invite" in r.stderr, (values, r.stderr[-400:])


def test_create_invite_refuses_a_malformed_relay_key(tmp_path):
    env = _env(tmp_path, flock=False)
    bin_dir = Path(env["PATH"].split(":")[0])
    (bin_dir / "ssh").write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$NC_SSH_LOG"\necho "not-a-key"\n')
    (bin_dir / "curl").write_text("#!/bin/bash\nexit 1\n")  # the local health check is best effort
    (bin_dir / "curl").chmod(0o755)
    r = _bash([str(FED / "create-invite.sh"), "--relay-ssh", "user@relay.invalid", "peer-x", "7"], env)
    assert r.returncode == 1 and "Invalid WireGuard public key" in r.stderr, (r.returncode, r.stderr[-400:])
    assert "KOI-INVITE-1:" not in r.stdout and json.loads(Path(env["PEER_REGISTRY"]).read_text()) == []


def test_a_crafted_node_rid_runs_nothing_in_remove_peer(tmp_path):
    env = _env(tmp_path, flock=True)
    rid = f"x' + str({TOUCH}) + '"
    Path(env["PEER_REGISTRY"]).write_text(json.dumps([{"peer_name": "peer-x", "node_rid": rid, "status": "active"}]))
    r = _bash([str(FED / "remove-peer.sh"), "peer-x", "user@relay.invalid"], env)
    assert not Path(env["NC_MARK"]).exists(), ("code from the node_rid ran", r.stdout[-600:], r.stderr[-600:])
    assert r.returncode == 0, (r.stdout[-600:], r.stderr[-600:])
    params = [json.loads(line) for line in Path(env["NC_PSY_LOG"]).read_text().splitlines()]
    assert [rid, rid] in params and [rid] in params, params  # revocation got the node_rid as parameters
    assert _registry(env)[0]["status"] == "removed"
    assert not Path(env["NC_SSH_LOG"]).exists()  # no WireGuard key in the entry, so no relay call
    assert "WireGuard:       MANUAL removal needed\n" in r.stdout, r.stdout[-600:]


def test_remove_peer_refuses_to_send_a_malformed_registry_key_to_the_relay(tmp_path):
    """Hardening: the registry's WireGuard key is checked again before the relay edit; the rest of
    the removal goes ahead and the exit status says the relay still has the peer."""
    env = _env(tmp_path, flock=True)
    key = "A" * 20 + "$(touch $NC_MARK)" + "A" * 6 + "="
    Path(env["PEER_REGISTRY"]).write_text(json.dumps([{"peer_name": "peer-x", "node_rid": GOOD_RID, "wg_pubkey": key,
                                                       "status": "active"}]))
    r = _bash([str(FED / "remove-peer.sh"), "peer-x", "user@relay.invalid"], env)
    assert not Path(env["NC_MARK"]).exists(), (r.stdout[-600:], r.stderr[-600:])
    assert r.returncode == 1 and "refusing to send it to the relay" in r.stderr, (r.returncode, r.stderr[-600:])
    assert "MANUAL removal needed (registry key refused)" in r.stdout, r.stdout[-600:]
    assert not Path(env["NC_SSH_LOG"]).exists()
    assert _registry(env)[0]["status"] == "removed"
    params = [json.loads(line) for line in Path(env["NC_PSY_LOG"]).read_text().splitlines()]
    assert [GOOD_RID, GOOD_RID] in params  # the database revocation still ran


def test_a_well_formed_peer_is_still_approved_into_the_registry(tmp_path):
    """Control: the direct approve flow still writes the entry with the flock branch."""
    env = _env(tmp_path, flock=True)
    Path(env["PEER_REGISTRY"]).write_text("[]")
    r = _bash([str(FED / "approve-peer.sh"), "peer-x", "5", WG_KEY, "user@relay.invalid"], env)
    reg = _registry(env)
    assert len(reg) == 1 and reg[0]["peer_name"] == "peer-x" and reg[0]["peer_number"] == 5, (reg, r.stderr[-600:])
    assert reg[0]["wg_pubkey"] == WG_KEY and reg[0]["status"] == "active"


def inline_python_bodies(src: str) -> list[tuple[int, str]]:
    """Every `python3 -c "..."` body: from the opening quote to the next unescaped quote."""
    out = []
    for m in re.finditer(r'python3 -c "', src):
        i = m.end()
        while i < len(src) and src[i] != '"':
            i += 2 if src[i] == "\\" else 1
        out.append((src[:m.start()].count("\n") + 1, src[m.end():i]))
    return out


SHELL_VAR = re.compile(r"\$\{?[A-Za-z_][A-Za-z0-9_]*|\$\(")
LIB_FUNCTIONS = ("peer_registry_lookup", "peer_registry_add", "peer_registry_update_status", "decode_invite_token")


def test_no_shell_value_inside_inline_python_on_remote_paths():
    bodies = []
    for name in ("approve-peer.sh", "remove-peer.sh", "connect-peers.sh"):
        bodies += [(name, line, body) for line, body in inline_python_bodies((FED / name).read_text())]
    lib = LIB.read_text()
    for fn in LIB_FUNCTIONS:
        start = lib.index(f"\n{fn}() {{")
        body = lib[start:lib.index("\n}\n", start)]
        bodies += [(f"lib.sh {fn}", line, b) for line, b in inline_python_bodies(body)]
    assert len(bodies) >= 25, len(bodies)  # the extractor must find the blocks at all
    offenders = [(f, line, SHELL_VAR.findall(b)) for f, line, b in bodies if SHELL_VAR.search(b)]
    assert offenders == [], offenders


FAKE_PEER_SSH = r"""#!/bin/bash
# Fake ssh for soak-check.sh: the peer's token read and its two vault-sync answers, from files.
printf '%s\n' "$*" >> "$NC_SSH_LOG"
case "$*" in
  *admin_token*) echo "peer-token-not-real" ;;
  */koi-net/vault-sync/status*) cat "$NC_PEER_STATUS" ;;
  */koi-net/vault-sync/reconcile*) echo '{"total_drift": 0}' ;;
esac
exit 0
"""
FAKE_LOCAL_CURL = r"""#!/bin/bash
case "$*" in
  */koi-net/vault-sync/status*) echo '{"pending_events": 0, "metrics": {"scans_completed": 1, "watcher_enabled": true}}' ;;
  */koi-net/vault-sync/reconcile*) echo '{"total_drift": 0}' ;;
esac
exit 0
"""


def test_a_crafted_peer_status_runs_nothing_in_soak_check(tmp_path):
    """soak-check.sh pasted the peer's own vault-sync status values into Python source."""
    env = _env(tmp_path, flock=False)
    project = tmp_path / "project"
    (project / "scripts" / "federation").mkdir(parents=True)
    script = project / "scripts" / "federation" / "soak-check.sh"
    script.write_text((FED / "soak-check.sh").read_text())  # its durable log lands under this temp project
    bin_dir = Path(env["PATH"].split(":")[0])
    (bin_dir / "ssh").write_text(FAKE_PEER_SSH)
    (bin_dir / "curl").write_text(FAKE_LOCAL_CURL)
    (bin_dir / "curl").chmod(0o755)
    pending = f"x' + str({TOUCH}) + '"
    status = tmp_path / "peer-status.json"
    status.write_text(json.dumps({"pending_events": pending, "metrics": {"scans_completed": 2, "watcher_enabled": True}}))
    soak_log = tmp_path / "soak.jsonl"
    r = _bash([str(script)], {**env, "NC_PEER_STATUS": str(status), "SOAK_LOG": str(soak_log), "API_PORT": "8400",
                              "PEER_SSH": "peer@peer.invalid"})
    assert not Path(env["NC_MARK"]).exists(), ("code from the peer's status ran", r.stdout[-600:], r.stderr[-600:])
    assert r.returncode == 0, (r.stdout[-600:], r.stderr[-600:])
    record = json.loads(soak_log.read_text().splitlines()[-1])
    assert record["peer"]["pending_events"] == pending and record["local"]["scans_completed"] == "1", record
    assert (project / "docs" / "soak-results" / "vault-sync-soak.jsonl").read_text().splitlines()[-1] == json.dumps(record)


def test_no_shell_value_inside_soak_check_inline_python():
    offenders = [(line, SHELL_VAR.findall(b)) for line, b in inline_python_bodies((FED / "soak-check.sh").read_text())
                 if SHELL_VAR.search(b)]
    assert offenders == [], offenders
