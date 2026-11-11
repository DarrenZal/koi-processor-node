"""Values reach SQL in scripts/federation as psql variables, never as SQL text (after the
python3 -c sweep in tests/test_federation_script_values.py).

- A static scan: no `psql -c` argument holds a shell expansion, every heredoc feeding psql has a
  quoted delimiter, and every psql that reads SQL from stdin sets ON_ERROR_STOP=1. Without it
  psql exits 0 on a SQL error in stdin input, where `-c` exits 1 (measured with psql 14.15), so
  a `|| log_fatal` after a converted call would never fire.
- compute-sas.sh and approve-peer-edges.sh run with bash, psql and curl replaced on PATH by
  recorders. The psql recorder answers a SQL error the way psql does: exit 3 with
  ON_ERROR_STOP=1, else exit 0 with no output.
- bootstrap-node.sh needs WireGuard and two live nodes, so its ensure_postgres function and its
  admin-profile lines are cut from the script text and run on their own with bash.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FED = REPO / "scripts" / "federation"
LIB = FED / "lib.sh"
BOOTSTRAP = FED / "bootstrap-node.sh"
# bootstrap-node.sh is not in every copy of this repository; its tests skip where it is absent.
needs_bootstrap = pytest.mark.skipif(not BOOTSTRAP.exists(), reason="scripts/federation/bootstrap-node.sh is not in this tree")
# A known smoke tool that interpolates RIDs into its own SQL; left as is for now.
EXCLUDED = {"smoke-vault-sync.sh"}


# The scanner.

SHELL_VALUE = re.compile(r"\$\{?[A-Za-z_][A-Za-z0-9_]*|\$\(|`")
END = set(" \t\n;|&)<>")


def _skip(src: str, i: int) -> int:
    while i < len(src) and src[i] in " \t":
        i += 1
    return i


def _word(src: str, i: int) -> tuple[list[tuple[str, str]], int]:
    """One shell word from i, as [(quoting, text)]: '"' expands, "'" does not, '' is bare text."""
    segs = []
    while i < len(src) and src[i] not in END:
        if src.startswith('\\"', i):  # \"...\" inside an outer "...", as in a string run_or_print evals
            j = src.index('\\"', i + 2)
            segs.append(('"', src[i + 2:j]))
            i = j + 2
        elif src.startswith("\\\n", i):
            break
        elif src[i] == '"':
            j = i + 1
            while src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            segs.append(('"', src[i + 1:j]))
            i = j + 1
        elif src[i] == "'":
            j = src.index("'", i + 1)
            segs.append(("'", src[i + 1:j]))
            i = j + 1
        else:
            j = i
            while j < len(src) and src[j] not in END and src[j] not in "\"'\\":
                j += 1
            if j == i:  # a lone backslash escape
                j = i + 2
            segs.append(("", src[i:j]))
            i = j
    return segs, i


def psql_commands(src: str) -> list[tuple[int, list, list]]:
    """Each psql command outside a comment: (line, words, stdin), stdin being [(kind, word)]."""
    out = []
    for m in re.finditer(r"(?<![\w$.-])psql(?![\w.-])", src):
        line_start = src.rfind("\n", 0, m.start()) + 1
        if src[line_start:m.start()].lstrip().startswith("#"):
            continue
        i, words, stdin = m.end(), [], []
        while i < len(src):
            if src[i] in " \t":
                i += 1
            elif src.startswith("\\\n", i):
                i += 2
            elif src.startswith("<<<", i):
                w, i = _word(src, _skip(src, i + 3))
                stdin.append(("herestring", w))
            elif src.startswith("<<", i):
                j = i + 3 if src.startswith("<<-", i) else i + 2
                w, i = _word(src, _skip(src, j))
                stdin.append(("heredoc", w))
            elif src[i] == "<":
                w, i = _word(src, _skip(src, i + 1))
                stdin.append(("file", w))
            elif src[i] == ">" or src.startswith("&>", i):
                j = i + 1
                if j < len(src) and src[j] in ">&":
                    j += 1
                _, i = _word(src, _skip(src, j))
            elif src[i] in "\n;|&)":
                break
            else:
                w, i = _word(src, i)
                words.append(w)
        out.append((src.count("\n", 0, m.start()) + 1, words, stdin))
    return out


def _bare(word) -> str | None:
    return "".join(t for _, t in word) if all(q == "" for q, _ in word) else None


def _expands(word) -> bool:
    return any(q != "'" and SHELL_VALUE.search(t) for q, t in word)


def sql_arguments(words) -> list:
    """The SQL a psql command takes on its command line: the argument of -c / --command."""
    out = []
    for k, w in enumerate(words):
        bare = _bare(w)
        if bare is not None and (re.fullmatch(r"-[A-Za-z]*c", bare) or bare == "--command") and k + 1 < len(words):
            out.append(words[k + 1])
        elif w and w[0][0] == "" and w[0][1].startswith("--command="):
            out.append([("", w[0][1][len("--command="):])] + w[1:])
    return out


def _stops_on_error(words) -> bool:
    return any(_bare(a) == "-v" and _bare(b) == "ON_ERROR_STOP=1" for a, b in zip(words, words[1:]))


def offenders(src: str) -> tuple[list, int, int]:
    """(offenders, -c sites seen, stdin sites seen) for one script."""
    bad, c_sites, stdin_sites = [], 0, 0
    for line, words, stdin in psql_commands(src):
        for arg in sql_arguments(words):
            c_sites += 1
            if _expands(arg):
                bad.append((line, "-c argument holds a shell value"))
        if stdin:
            stdin_sites += 1
            if not _stops_on_error(words):
                bad.append((line, "reads SQL from stdin without -v ON_ERROR_STOP=1"))
        for kind, w in stdin:
            quoted = any(q in ("'", '"') for q, _ in w) or (_bare(w) or "").startswith("\\")
            if kind == "heredoc" and not quoted:
                bad.append((line, "heredoc delimiter is not quoted"))
            if kind == "herestring" and _expands(w):
                bad.append((line, "here-string holds a shell value"))
    return bad, c_sites, stdin_sites


SCANNER_FIXTURE = r'''
a=$(psql -d personal_koi -Atc "SELECT 1 FROM t WHERE x = '$A'")
run_or_print "sudo -u postgres psql -tAc \"SELECT 1 FROM pg_roles WHERE rolname='${B}'\" | grep -q 1"
psql "$URL" -c "CREATE EXTENSION IF NOT EXISTS vector;" >/dev/null 2>&1
n=$(psql -tAq "$DB" -v ON_ERROR_STOP=1 -c "
    SELECT count(*) FROM t WHERE d > $(date +%s)")
psql --command="SELECT '$C'"
psql -c 'SELECT 1'"$D"
psql -c 'SELECT $literal_in_single_quotes'
x=$(psql -d db -At -v id="$X" -v ON_ERROR_STOP=1 <<'SQL'
SELECT :'id'
SQL
)
y=$(psql -d db -At -v ON_ERROR_STOP=1 <<SQL
SELECT '$Y'
SQL
)
psql -d db -At -v ON_ERROR_STOP=1 <<< "SELECT '$E'"
z=$(psql -d db -At -v id="$Z" <<'SQL'
SELECT :'id'
SQL
)
# psql -c "SELECT '$IN_A_COMMENT'"
command -v psql >/dev/null 2>&1 && log_fatal "Lookup failed (psql -d $DB)"
'''


def test_the_scanner_flags_the_forms_it_must_and_passes_the_safe_ones():
    bad, c_sites, stdin_sites = offenders(SCANNER_FIXTURE)
    assert (c_sites, stdin_sites) == (7, 4)
    assert bad == [
        (2, "-c argument holds a shell value"),       # pasted into -c
        (3, "-c argument holds a shell value"),       # inside a string that is eval'd
        (5, "-c argument holds a shell value"),       # $(...) after -v ON_ERROR_STOP=1
        (7, "-c argument holds a shell value"),       # --command=
        (8, "-c argument holds a shell value"),       # '...' joined to "$D"
        (14, "heredoc delimiter is not quoted"),
        (18, "here-string holds a shell value"),
        (19, "reads SQL from stdin without -v ON_ERROR_STOP=1"),
    ]


def test_no_shell_value_reaches_psql_sql_in_federation_scripts():
    found, c_total, stdin_total = [], 0, 0
    for f in sorted(FED.glob("*.sh")):
        if f.name in EXCLUDED:
            continue
        bad, c_sites, stdin_sites = offenders(f.read_text())
        found += [(f.name, *b) for b in bad]
        c_total, stdin_total = c_total + c_sites, stdin_total + stdin_sites
    # The scan must see the sites at all: 5 -c calls (bootstrap 1, setup-node 3, validate-node 1) and
    # 6 stdin calls (bootstrap 3, compute-sas 1, approve-peer-edges 2) at the time of writing.
    # bootstrap-node.sh is not in every copy of this repository.
    c_floor, stdin_floor = (5, 6) if BOOTSTRAP.exists() else (4, 3)
    assert c_total >= c_floor and stdin_total >= stdin_floor, (c_total, stdin_total)
    assert found == [], found


def test_the_one_excluded_script_is_still_one_the_scan_would_flag():
    bad, c_sites, _ = offenders((FED / "smoke-vault-sync.sh").read_text())
    assert c_sites >= 10 and bad, (c_sites, bad)  # drop it from EXCLUDED once it is converted


# Recorders for the scripts that run with bash.

PSQL = r'''#!/usr/bin/env python3
import json, os, sys
sql = sys.stdin.read()
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(json.dumps({"tool": "psql", "argv": sys.argv[1:], "stdin": sql}) + "\n")
error = os.environ.get("STUB_SQL_ERROR")
if error and error in sql:
    sys.exit(3 if "ON_ERROR_STOP=1" in sys.argv[1:] else 0)  # as psql answers an error in stdin input
for key, reply in json.loads(os.environ.get("STUB_REPLIES", "{}")).items():
    if key in sql:
        print(reply)
        break
sys.exit(int(os.environ.get("STUB_PSQL_EXIT", "0")))
'''
CURL = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(json.dumps({"tool": "curl", "argv": sys.argv[1:]}) + "\n")
print(os.environ["STUB_HEALTH"])
'''
RECORDER = r'''#!/usr/bin/env python3
import json, os, sys
sql = sys.stdin.read()
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(json.dumps({"tool": os.path.basename(sys.argv[0]), "argv": sys.argv[1:], "stdin": sql}) + "\n")
'''
P256_KEY = "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE" + "A" * 88
GOOD_RID = "orn:koi-net.node:admin-node+0123456789abcdef"


def _bin(tmp_path: Path, tools: dict) -> Path:
    b = tmp_path / "bin"
    b.mkdir(exist_ok=True)
    # A wrapper, not a symlink: a symlink outside the venv loses its site-packages.
    tools = {"python3": f'#!/bin/bash\nexec "{sys.executable}" "$@"\n', **tools}
    for name, body in tools.items():
        (b / name).write_text(body.replace("#!/usr/bin/env python3", f"#!{sys.executable}", 1))
        (b / name).chmod(0o755)
    return b


def _calls(log: Path) -> list[dict]:
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def _env(tmp_path: Path, tools: dict, **extra: str) -> dict:
    return {"PATH": f"{_bin(tmp_path, tools)}:/usr/bin:/bin", "HOME": str(tmp_path), "STUB_LOG": str(tmp_path / "calls.jsonl"),
            "PYTHONDONTWRITEBYTECODE": "1", "KOI_API_PORT": "9", **extra}


# compute-sas.sh.

HOSTILE_ID = "x'); DROP TABLE koi_net_nodes; SELECT ('"


def _compute_sas(tmp_path, peer_id, **extra):
    env = _env(tmp_path, {"psql": PSQL, "curl": CURL}, STUB_HEALTH=json.dumps({"node": {"public_key": P256_KEY}}),
               STUB_REPLIES=json.dumps({"koi_net_peer_aliases": "PEER-KEY"}), **extra)
    r = subprocess.run(["bash", str(FED / "compute-sas.sh"), peer_id], env=env, cwd=tmp_path, capture_output=True,
                       text=True, timeout=60, stdin=subprocess.DEVNULL)
    return r, [c for c in _calls(tmp_path / "calls.jsonl") if c["tool"] == "psql"]


def test_compute_sas_takes_the_peer_id_as_a_psql_variable(tmp_path):
    r, psql = _compute_sas(tmp_path, HOSTILE_ID)
    assert r.returncode == 0, r.stdout + r.stderr
    assert re.search(r"SAS:  \d{6}", r.stdout)
    [call] = psql
    assert call["argv"] == ["-d", "personal_koi", "-At", "-v", f"id={HOSTILE_ID}", "-v", "ON_ERROR_STOP=1"]
    assert HOSTILE_ID not in call["stdin"] and call["stdin"].count(":'id'") == 4


def test_compute_sas_stops_on_a_sql_error(tmp_path):
    r, _ = _compute_sas(tmp_path, "peer-b", STUB_SQL_ERROR="koi_net_peer_aliases")
    assert r.returncode == 1 and "Cannot find peer 'peer-b' in database" in r.stdout + r.stderr


# approve-peer-edges.sh: its converted lookups stop on a SQL error instead of reporting success.

def test_approve_peer_edges_stops_on_a_sql_error_in_the_edge_lookup(tmp_path):
    local, peer = "orn:koi-net.node:local-test+11", "orn:koi-net.node:peer-test+22"
    state = tmp_path / "state"
    state.mkdir()
    (state / "admin_token").write_text("t\n")
    env = _env(tmp_path, {"psql": PSQL, "curl": CURL}, STUB_HEALTH=json.dumps({"node": {"node_rid": local}}),
               STUB_REPLIES=json.dumps({"koi_net_peer_aliases": peer}),
               STUB_SQL_ERROR="FROM koi_net_edges", KOI_STATE_DIR=str(state))
    r = subprocess.run(["bash", str(FED / "approve-peer-edges.sh"), "peer-test"], env=env, cwd=tmp_path,
                       capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL)
    assert r.returncode == 1, r.stdout + r.stderr
    assert "Edge lookup failed" in r.stdout + r.stderr and "already approved" not in r.stdout


# bootstrap-node.sh: ensure_postgres, run on its own.

def _function(src: str, name: str) -> str:
    start = src.index(f"\n{name}() {{\n") + 1
    return src[start:src.index("\n}\n", start) + 3]


def _ensure_postgres(tmp_path, os_name, dry_run, db_user):
    src = BOOTSTRAP.read_text()
    script = tmp_path / "ensure_postgres.sh"
    # DRY_RUN is set after lib.sh, which starts it at false (bootstrap-node.sh then parses --dry-run).
    script.write_text(f'source "{LIB}"\nDRY_RUN={dry_run}\n' + _function(src, "run_or_print") + _function(src, "ensure_postgres")
                      + "ensure_postgres\n")
    tools = {"uname": f"#!/bin/bash\necho {os_name}\n", "psql": "#!/bin/bash\nexit 2\n"}
    tools.update({t: RECORDER for t in ("sudo", "createuser", "createdb", "brew")})
    env = _env(tmp_path, tools, DB_USER=db_user)
    r = subprocess.run(["bash", str(script)], env=env, cwd=tmp_path, capture_output=True, text=True, timeout=60,
                       stdin=subprocess.DEVNULL)
    return r, _calls(tmp_path / "calls.jsonl")


HOSTILE_USER = "x'; touch ran; echo '"


@needs_bootstrap
def test_ensure_postgres_on_linux_takes_the_role_name_as_data(tmp_path):
    r, calls = _ensure_postgres(tmp_path, "Linux", "false", HOSTILE_USER)
    assert r.returncode == 0, r.stdout + r.stderr
    assert not (tmp_path / "ran").exists()
    sudo = [c["argv"] for c in calls if c["tool"] == "sudo"]
    assert sudo == [
        ["systemctl", "enable", "--now", "postgresql"],
        ["-u", "postgres", "psql", "-tA", "-v", f"name={HOSTILE_USER}", "-v", "ON_ERROR_STOP=1"],
        ["-u", "postgres", "createuser", "-s", HOSTILE_USER],
        ["-u", "postgres", "psql", "-tA", "-v", "ON_ERROR_STOP=1"],
        ["-u", "postgres", "createdb", "-O", HOSTILE_USER, "personal_koi"],
    ]
    role_sql = [c["stdin"] for c in calls if c["tool"] == "sudo" and "pg_roles" in c["stdin"]]
    assert role_sql == ["SELECT 1 FROM pg_roles WHERE rolname = :'name'\n"]


@needs_bootstrap
def test_ensure_postgres_on_macos_takes_the_role_name_as_data(tmp_path):
    r, calls = _ensure_postgres(tmp_path, "Darwin", "false", HOSTILE_USER)
    assert r.returncode == 0, r.stdout + r.stderr
    assert not (tmp_path / "ran").exists()
    assert [c["argv"] for c in calls if c["tool"] == "createuser"] == [["-s", HOSTILE_USER]]


@needs_bootstrap
def test_ensure_postgres_dry_run_runs_nothing(tmp_path):
    for os_name in ("Linux", "Darwin"):
        r, calls = _ensure_postgres(tmp_path, os_name, "true", HOSTILE_USER)
        assert r.returncode == 0, r.stdout + r.stderr
        assert calls == [] and not (tmp_path / "ran").exists()
        assert "[DRY-RUN]" in r.stdout and "x\\'\\;\\ touch\\ ran\\;\\ echo\\ \\'" in r.stdout


# bootstrap-node.sh refuses a malformed node RID in the admin's /health.

def _admin_profile(tmp_path, node_rid):
    src = BOOTSTRAP.read_text()
    block = src[src.index("    # Get admin's profile\n"):src.index("    # Get local profile\n")]
    script = tmp_path / "admin_profile.sh"
    script.write_text(f'source "{LIB}"\nINVITE_ADMIN_BASE_URL=http://10.100.0.1:8351\n{block}echo "REACHED $ADMIN_NODE_RID"\n')
    health = json.dumps({"node": {"node_rid": node_rid, "public_key": P256_KEY}})
    env = _env(tmp_path, {"curl": CURL}, STUB_HEALTH=health)
    return subprocess.run(["bash", str(script)], env=env, cwd=tmp_path, capture_output=True, text=True, timeout=60,
                          stdin=subprocess.DEVNULL)


@needs_bootstrap
def test_bootstrap_refuses_a_malformed_admin_node_rid(tmp_path):
    for rid in (GOOD_RID + "'); DROP TABLE koi_net_edges; --", GOOD_RID + "\nx", "", "orn:koi-net.node:a b+0123456789abcdef",
                "orn:koi-net.node:admin-node+0123"):
        r = _admin_profile(tmp_path, rid)
        assert r.returncode == 1 and "REACHED" not in r.stdout, (rid, r.stdout + r.stderr)
        assert "Refusing the admin's profile" in r.stdout + r.stderr, rid


@needs_bootstrap
def test_bootstrap_accepts_a_well_formed_admin_node_rid(tmp_path):
    r = _admin_profile(tmp_path, GOOD_RID)
    assert r.returncode == 0 and f"REACHED {GOOD_RID}" in r.stdout, r.stdout + r.stderr
