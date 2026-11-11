"""Federation scripts pass every value to inline Python as data, never pasted into its source.

The values here come from the operator (arguments, paths, a database URL), from the relay (its
WireGuard key), or from this node's own identity module. Pasted into `python3 -c "..."` source, a
quote in any of them ended the string literal and the rest ran as code. The relay's config edit is
included too: a key was spliced into a script that runs as root on the relay.

Each test runs the real script or lib.sh function with bash and a crafted value, and checks that
the value ran nothing and arrived intact. Fakes stand in for ssh, flock, wg, wg-quick, curl,
psycopg2 and koi-processor's node_identity module. State, registry and config live in the test's
temp directory. Nothing reaches the network or a database.
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

REPO = Path(__file__).resolve().parents[1]
FED = REPO / "scripts" / "federation"
LIB = FED / "lib.sh"
TOUCH = "__import__('pathlib').Path(__import__('os').environ['NC_MARK']).touch()"
Q = f"'+str({TOUCH})+'"  # ends a '...' literal in pasted source, runs TOUCH, reopens it
WG_KEY = "A" * 43 + "="
P256_DER = bytes.fromhex("3059301306072a8648ce3d020106082a8648ce3d03010703420004") + bytes(range(64))

FAKE_RELAY_SSH = r"""#!/bin/bash
# Fake ssh to the relay: record the call and the script, then run the script here with the
# relay's fixed paths moved into the test's temp directory (no sudo).
printf '%s\n' "$*" >> "$NC_SSH_LOG"
script=$(cat)
printf '%s' "$script" > "$NC_REMOTE_SCRIPT"
script=${script//\/etc\/wireguard\/wg-koi.conf/$NC_WG_CONF}
script=${script//\/run\/wg-koi.lock/$NC_WG_LOCK}
exec bash -s <<< "$script"
"""
FAKE_WG_QUICK = '#!/bin/bash\n[[ "$1" == strip ]] && exec cat\nexit 0\n'
FAKE_WG = '#!/bin/bash\ncase "$1" in genkey) echo "' + "B" * 43 + '=" ;; pubkey) echo "' + "C" * 43 + '=" ;; *) exit 1 ;; esac\n'
FAKE_OK = "#!/bin/bash\nexit 0\n"
FAKE_FAIL = "#!/bin/bash\nexit 1\n"
FAKE_INVITE_SSH = f'#!/bin/bash\nprintf "%s\\n" "$*" >> "$NC_SSH_LOG"\ncase "$*" in *"wg show wg-koi public-key"*) echo "{WG_KEY}" ;; *) exit 97 ;; esac\n'
STUB_PSYCOPG2 = """import os
def connect(url):
    with open(os.environ["NC_PSY_LOG"], "a") as f:
        f.write(url + "\\n")
    raise RuntimeError("stub psycopg2: no database in this test")
"""
FAKE_NODE_IDENTITY = f"""import base64, json, os
DER = bytes.fromhex({P256_DER.hex()!r})
class _Profile:
    def __init__(self, name):
        self.node_rid = 'orn:koi-net.node:peer-x+' + 'b' * 64
def load_or_create_identity(name):
    with open(os.environ['NC_IDENTITY_LOG'], 'a') as f:
        f.write(json.dumps({{'name': name, 'state': os.environ.get('KOI_STATE_DIR')}}) + '\\n')
    return (object(), _Profile(name))
def get_public_key_der_b64(private_key):
    return base64.b64encode(DER).decode()
"""


def _tool(bin_dir: Path, name: str, body: str) -> None:
    (bin_dir / name).write_text(body)
    (bin_dir / name).chmod(0o755)


def _env(tmp_path: Path) -> dict:
    bin_dir, stub, kp, state = (tmp_path / d for d in ("bin", "stub", "kp", "state"))
    for d in (bin_dir, stub, kp / "config", state, tmp_path / "home"):
        d.mkdir(parents=True, exist_ok=True)
    # A wrapper, not a symlink: a symlink outside the venv loses its site-packages (cryptography).
    _tool(bin_dir, "python3", f'#!/bin/bash\nexec "{sys.executable}" "$@"\n')
    for name, body in (("ssh", FAKE_RELAY_SSH), ("flock", FAKE_OK), ("wg-quick", FAKE_WG_QUICK), ("wg", FAKE_WG),
                       ("curl", FAKE_FAIL), ("pip3", FAKE_FAIL), ("sudo", FAKE_FAIL)):
        _tool(bin_dir, name, body)
    (stub / "psycopg2.py").write_text(STUB_PSYCOPG2)
    return {
        "PATH": f"{bin_dir}:/usr/bin:/bin", "HOME": str(tmp_path / "home"), "PYTHONPATH": str(stub),
        "PYTHONDONTWRITEBYTECODE": "1", "PIP_NO_INDEX": "1", "KOI_PROCESSOR_DIR": str(kp),
        "KOI_STATE_DIR": str(state), "PEER_REGISTRY": str(tmp_path / "registry.json"),
        "NC_MARK": str(tmp_path / "ran"), "NC_SSH_LOG": str(tmp_path / "ssh.log"),
        "NC_PSY_LOG": str(tmp_path / "psy.log"), "NC_IDENTITY_LOG": str(tmp_path / "identity.log"),
        "NC_REMOTE_SCRIPT": str(tmp_path / "remote.sh"), "NC_WG_CONF": str(tmp_path / "wg-koi.conf"),
        "NC_WG_LOCK": str(tmp_path / "wg-koi.lock"),
    }


def _run(args: list[str], env: dict, cwd: Path, stdin: str = "") -> subprocess.CompletedProcess:
    return subprocess.run(["/bin/bash", *args], env=env, cwd=cwd, input=stdin, capture_output=True, text=True,
                          timeout=60)


def _lib(call: str, env: dict, cwd: Path, **values: str) -> subprocess.CompletedProcess:
    """Call one lib.sh function the way the scripts do, with values that reach it as shell variables."""
    return _run(["-c", f'source "$NC_LIB"; {call}'], {**env, "NC_LIB": str(LIB), **values}, cwd)


def _ran(env: dict) -> bool:
    return Path(env["NC_MARK"]).exists()


def _fake_koi_processor(root: Path) -> Path:
    (root / "api").mkdir(parents=True)
    (root / "api" / "__init__.py").write_text("")
    (root / "api" / "node_identity.py").write_text(FAKE_NODE_IDENTITY)
    return root


# lib.sh relay_config_edit: the key and IP run as root on the relay.

def test_relay_config_edit_refuses_a_crafted_key_and_ip(tmp_path):
    env = _env(tmp_path)
    Path(env["NC_WG_CONF"]).write_text("[Interface]\nAddress = 10.100.0.1/24\n")
    for action, key, ip in (("add-peer", "AAAA$(touch $NC_MARK)AAAA", "10.100.0.5"),
                            ("remove-peer", "AAAA$(touch $NC_MARK)AAAA", ""),
                            ("add-peer", WG_KEY, "10.100.0.5$(touch $NC_MARK)")):
        r = _lib(f'relay_config_edit user@relay.invalid {action} "$NC_KEY" "$NC_IP"', env, tmp_path, NC_KEY=key, NC_IP=ip)
        assert not _ran(env), (action, "code from the value ran on the relay", r.stderr[-600:])
        assert r.returncode == 1 and "Relay config edit refused" in r.stderr, (action, r.returncode, r.stderr[-600:])
    assert not Path(env["NC_SSH_LOG"]).exists()


def test_relay_config_edit_adds_and_removes_a_peer_with_the_key_as_data(tmp_path):
    """Control: the remote script runs (fake relay) and the key reaches it only as an assignment."""
    env = _env(tmp_path)
    conf = Path(env["NC_WG_CONF"])
    conf.write_text("[Interface]\nAddress = 10.100.0.1/24\n")
    key = "Zm9v+/" + "A" * 37 + "="
    r = _lib('relay_config_edit user@relay.invalid add-peer "$NC_KEY" 10.100.0.7', env, tmp_path, NC_KEY=key)
    assert r.returncode == 0 and "OK: Peer added" in r.stdout, (r.stdout[-600:], r.stderr[-600:])
    assert f"[Peer]\nPublicKey = {key}\nAllowedIPs = 10.100.0.7/32" in conf.read_text()
    script = Path(env["NC_REMOTE_SCRIPT"]).read_text()
    assert script.startswith(f"PUBKEY={key}\nPEER_IP=10.100.0.7\n") and script.count(key) == 1, script[:300]
    assert 'LOCKFILE="/run/wg-koi.lock"' in script and "/tmp/" not in script
    added = conf.read_text()
    r = _lib('relay_config_edit user@relay.invalid add-peer "$NC_KEY" 10.100.0.8', env, tmp_path, NC_KEY=key)
    assert r.returncode == 1 and "already exists" in r.stderr, (r.stdout[-600:], r.stderr[-600:])  # duplicate check
    assert conf.read_text() == added
    r = _lib('relay_config_edit user@relay.invalid remove-peer "$NC_KEY"', env, tmp_path, NC_KEY=key)
    assert r.returncode == 0 and "OK: Peer removed" in r.stdout, (r.stdout[-600:], r.stderr[-600:])
    assert key not in conf.read_text() and "[Interface]" in conf.read_text()
    assert Path(env["NC_REMOTE_SCRIPT"]).read_text().count(key) == 1
    assert not _ran(env)


def test_relay_config_edit_refuses_a_symlinked_lock(tmp_path):
    """The lock is opened by root with 200>, which would truncate a symlink's target."""
    env = _env(tmp_path)
    conf = Path(env["NC_WG_CONF"])
    conf.write_text("[Interface]\nAddress = 10.100.0.1/24\n")
    target = tmp_path / "target"
    target.write_text("keep\n")
    Path(env["NC_WG_LOCK"]).symlink_to(target)
    for call in ('add-peer "$NC_KEY" 10.100.0.7', 'remove-peer "$NC_KEY"'):
        r = _lib(f"relay_config_edit user@relay.invalid {call}", env, tmp_path, NC_KEY=WG_KEY)
        assert r.returncode == 1 and "is a symlink" in r.stderr, (call, r.stdout[-400:], r.stderr[-400:])
    assert target.read_text() == "keep\n" and conf.read_text() == "[Interface]\nAddress = 10.100.0.1/24\n"


# lib.sh run_migrations, parse_join_request, ensure_python_module, peer_registry_lookup_by_number.

def test_run_migrations_takes_the_url_and_paths_as_data(tmp_path):
    env = _env(tmp_path)
    url, mig_dir = f"postgresql://koi-test.invalid/db{Q}", str(tmp_path / f"mig{Q}")
    r = _lib('run_migrations "$NC_URL" "$NC_DIR"', env, tmp_path, NC_URL=url, NC_DIR=mig_dir)
    assert not _ran(env), ("code from the URL or path ran", r.stderr[-600:])
    assert r.returncode != 0 and "stub psycopg2" in r.stderr, r.stderr[-600:]
    assert Path(env["NC_PSY_LOG"]).read_text() == url + "\n"


def test_parse_join_request_takes_its_path_as_data(tmp_path):
    env = _env(tmp_path)
    join = tmp_path / f"join{Q}.txt"
    join.write_text("=== KOI-net Join Request ===\npeer_name: peer-x\nwg_public_key: k\n==========================\n")
    r = _lib('parse_join_request "$NC_FILE"', env, tmp_path, NC_FILE=str(join))
    assert not _ran(env), ("code from the path ran", r.stderr[-600:])
    assert r.returncode == 0 and r.stdout.splitlines() == ["peer_name=peer-x", "wg_public_key=k"], (r.stdout, r.stderr)


def test_ensure_python_module_refuses_a_crafted_name(tmp_path):
    env = _env(tmp_path)
    r = _lib('ensure_python_module "$NC_MODULE"', env, tmp_path, NC_MODULE=f"os; {TOUCH}")
    assert not _ran(env), ("code from the module name ran", r.stderr[-600:])
    assert r.returncode == 1 and "Invalid Python module name" in r.stderr, r.stderr[-600:]
    for module in ("json", "os.path", "psycopg2"):  # control: real names still import
        r = _lib('ensure_python_module "$NC_MODULE"', env, tmp_path, NC_MODULE=module)
        assert r.returncode == 0, (module, r.stderr[-600:])


def test_peer_registry_lookup_by_number_takes_the_number_as_data(tmp_path):
    env = _env(tmp_path)
    entry = {"peer_name": "peer-x", "peer_number": 3, "status": "invited"}
    Path(env["PEER_REGISTRY"]).write_text(json.dumps([entry]))
    r = _lib('peer_registry_lookup_by_number "$NC_NUM"', env, tmp_path, NC_NUM=f"5 or str({TOUCH})")
    assert not _ran(env), ("code from the number ran", r.stderr[-600:])
    assert r.returncode == 1 and r.stdout == "", (r.returncode, r.stdout)
    r = _lib('peer_registry_lookup_by_number 3', env, tmp_path)
    assert r.returncode == 0 and json.loads(r.stdout) == entry, (r.stdout, r.stderr)


# koi_node_identity: one helper for join-request.sh, bootstrap-node.sh and setup-node.sh.

def test_koi_node_identity_takes_paths_and_name_as_data(tmp_path):
    env = _env(tmp_path)
    kp = _fake_koi_processor(tmp_path / f"kp{Q}")
    state, name = str(tmp_path / f"state{Q}"), f"peer{Q}"
    r = _lib('koi_node_identity "$NC_KP" "$NC_STATE" "$NC_NAME"', env, tmp_path, NC_KP=str(kp), NC_STATE=state,
             NC_NAME=name)
    assert not _ran(env), ("code from a path or the name ran", r.stderr[-600:])
    assert r.returncode == 0, r.stderr[-600:]
    assert json.loads(Path(env["NC_IDENTITY_LOG"]).read_text()) == {"name": name, "state": state}
    key = base64.b64encode(P256_DER).decode()
    fp = "SHA256:" + base64.b64encode(hashlib.sha256(P256_DER).digest()).decode().rstrip("=")
    assert r.stdout.splitlines() == [f"node_rid=orn:koi-net.node:peer-x+{'b' * 64}", f"koi_public_key={key}",
                                     f"fingerprint={fp}"]


def test_join_request_runs_nothing_from_its_paths(tmp_path):
    """join-request.sh end to end, with a koi-processor dir and a state dir whose names carry a quote."""
    env = _env(tmp_path)
    kp = _fake_koi_processor(tmp_path / f"kp{Q}")
    state = tmp_path / f"state{Q}"
    r = _run([str(FED / "join-request.sh"), "peer-x", str(state)], {**env, "KOI_PROCESSOR_DIR": str(kp)}, tmp_path)
    assert not _ran(env), ("code from a path ran", r.stderr[-600:])
    assert r.returncode == 0, (r.stdout[-600:], r.stderr[-600:])
    assert json.loads(Path(env["NC_IDENTITY_LOG"]).read_text()) == {"name": "peer-x", "state": str(state / "koi-state")}
    text = (state / "wireguard" / "join-request.txt").read_text()
    assert f"koi_public_key: {base64.b64encode(P256_DER).decode()}\n" in text
    assert f"node_rid: orn:koi-net.node:peer-x+{'b' * 64}\n" in text


# create-invite.sh: registry path, state dir, folder and TTL from the operator; relay key from the relay.

def _decode(token: str) -> dict:
    sys.path.insert(0, str(FED))
    try:
        from invite_token import decode_token
    finally:
        sys.path.remove(str(FED))
    return decode_token(token)


def test_create_invite_dry_run_takes_its_values_as_data(tmp_path):
    env = _env(tmp_path)
    registry, folder, admin_ip = tmp_path / f"reg{Q}.json", f"Shared{Q}", f"10.100.0.9{Q}"
    r = _run([str(FED / "create-invite.sh"), "--dry-run", "--vault-sync-folder", folder, "--relay-ssh",
              "user@relay.invalid", "peer-x"], {**env, "PEER_REGISTRY": str(registry), "ADMIN_WG_IP": admin_ip}, tmp_path)
    assert not _ran(env), ("code from a value ran", r.stdout[-600:], r.stderr[-600:])
    assert r.returncode == 0, (r.stdout[-600:], r.stderr[-600:])
    lines = r.stdout.splitlines()
    payload = _decode(lines[lines.index("[DRY-RUN] Token would be:") + 1])
    assert (payload["peer_name"], payload["peer_number"], payload["vault_sync_folder"], payload["admin_wg_ip"]) == \
        ("peer-x", 3, folder, admin_ip), payload
    assert payload["relay_endpoint"] == "relay.invalid:51820" and payload["expires_at"] - payload["issued_at"] == 86400


def test_create_invite_refuses_a_ttl_that_is_not_a_whole_number(tmp_path):
    env = _env(tmp_path)
    for ttl in (f"1) + str({TOUCH}) + (1", "0", "1.5", "-3"):
        r = _run([str(FED / "create-invite.sh"), "--dry-run", "--relay-ssh", "user@relay.invalid", "--ttl", ttl, "peer-x"],
                 env, tmp_path)
        assert not _ran(env), (ttl, r.stderr[-600:])
        assert r.returncode == 1 and "Invalid --ttl" in r.stderr, (ttl, r.returncode, r.stderr[-600:])


def test_create_invite_live_takes_its_values_as_data(tmp_path):
    """The live path: a secret under a state dir whose name carries a quote, the registry entry, the JTI."""
    env = _env(tmp_path)
    _tool(Path(env["PATH"].split(":")[0]), "ssh", FAKE_INVITE_SSH)
    state, folder = tmp_path / f"state{Q}", f"Shared{Q}"
    r = _run([str(FED / "create-invite.sh"), "--vault-sync-folder", folder, "--relay-ssh", "user@relay.invalid",
              "peer-x", "7"], {**env, "KOI_STATE_DIR": str(state)}, tmp_path)
    assert not _ran(env), ("code from a value ran", r.stdout[-600:], r.stderr[-600:])
    assert r.returncode == 0, (r.stdout[-600:], r.stderr[-600:])
    token = next(line.strip() for line in r.stdout.splitlines() if line.strip().startswith("KOI-INVITE-1:"))
    sys.path.insert(0, str(FED))
    try:
        from invite_token import verify_token
    finally:
        sys.path.remove(str(FED))
    payload = verify_token(token, (state / "invite_secret").read_bytes())
    assert (payload["peer_number"], payload["relay_pubkey"], payload["vault_sync_folder"]) == (7, WG_KEY, folder)
    [entry] = json.loads(Path(env["PEER_REGISTRY"]).read_text())
    assert {k: entry[k] for k in ("peer_name", "peer_number", "wg_ip", "jti", "status", "consumed")} == \
        {"peer_name": "peer-x", "peer_number": 7, "wg_ip": "10.100.0.7", "jti": payload["jti"], "status": "invited",
         "consumed": False}


# Every inline Python block in scripts/federation.

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


def test_no_shell_value_inside_any_inline_python_in_federation_scripts():
    bodies = [(f.name, line, body) for f in sorted(FED.glob("*.sh")) for line, body in inline_python_bodies(f.read_text())]
    assert len(bodies) >= 60, len(bodies)  # the extractor must find the blocks at all
    offenders = [(f, line, SHELL_VAR.findall(b)) for f, line, b in bodies if SHELL_VAR.search(b)]
    assert offenders == [], offenders


def test_the_identity_scripts_use_the_shared_helper():
    for name in ("join-request.sh", "bootstrap-node.sh", "setup-node.sh"):
        if name == "bootstrap-node.sh" and not (FED / name).exists():
            continue  # not in every copy of this repository
        src = (FED / name).read_text()
        assert "$(koi_node_identity " in src and "load_or_create_identity" not in src, name
