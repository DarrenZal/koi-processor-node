"""KOI-net interop gate: our node against stock koi-net nodes, end to end, on loopback.

    tools/sandboxed.sh -- python3 tests/interop/run_gate.py --node-python <python>

Standard library only, so any python3 can run it. For each case it:

1. starts a throwaway PostgreSQL cluster in a scratch directory (TCP on 127.0.0.1 only,
   no Unix socket), builds `personal_koi` there once with scripts/run_migrations.sh, and
   clones it per case as `koi_test_interop_<case>`;
2. starts our node from this checkout with `<node-python> -m uvicorn`, from an empty
   environment in which every target is named: the case database, a scratch HOME, state
   dir and vault, 127.0.0.1 and a free port;
3. starts one stock koi-net node (tests/interop/stock_node.py, koi-net-node-template at a
   pinned commit) whose first contact is our node, or none for the conformance case;
4. runs pytest (tests/interop/test_gate.py, or tests/conformance for the conformance
   case) in .venv-koinet, with targets in KOI_TEST_* variables and case details in
   KOI_GATE_* variables;
5. stops every process it started, by the PID it recorded, and the cluster.

Cases: `conformance` (no stock node), and `<full|partial>-<off|on>`, where full/partial is
the stock node's type and off/on is KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL (the membrane).

--db-tests runs main-suite tests that need a database (KOI_TEST_DATABASE_URL) with
<node-python>, against a clone of the same throwaway `personal_koi`, named
`koi_test_interop_db_tests`. With --db-tests and no --cases, only those tests run.

Time limits: every pytest run loads tests/interop/gate_timeout.py, which fails a test phase
(setup, call or teardown) that runs past --test-timeout seconds and names the asyncio tasks
still waiting, so one hung test fails instead of blocking the run. --wall-clock bounds the
whole gate: past it, the gate stops every process it started and the cluster, keeps the
scratch directory and exits 1. 0 turns either limit off.

Exit status: 0 when every pytest run exits 0 (strict xfails count as passing), 1 when a
run fails or the wall clock runs out, 2 for a usage error or a refused target, 3 when a
prerequisite is missing.
Prerequisites: tests/interop/setup_stock_nodes.sh (the only step that uses the network).

Nothing here reads a live target. The scratch directory is kept on failure, or always
with --keep, and its path is printed; it holds every node's log.
"""
from __future__ import annotations

import argparse
import atexit
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
VENV_PYTHON = REPO_ROOT / ".venv-koinet" / "bin" / "python"
TEMPLATE_SRC = REPO_ROOT / ".interop" / "template-c45b188" / "src"
SETUP = REPO_ROOT / "tests" / "interop" / "setup_stock_nodes.sh"
STOCK_NODE = REPO_ROOT / "tests" / "interop" / "stock_node.py"
TIMEOUT_PLUGIN = "tests.interop.gate_timeout"
LIVE_PORT = 8351
PG_USER = "koi_gate"
ALL_CASES = ("conformance", "full-off", "full-on", "partial-off", "partial-on")
GATE_INTEREST = "orn:koi-net.node"
# Run against a throwaway clone in every gate run that does not name its own --db-tests.
DEFAULT_DB_TESTS = ["tests/interop/test_negotiation_db.py", "tests/interop/test_peer_rejection_db.py",
                    "tests/test_koi_net_peer_identity.py", "tests/test_koi_net_bootstrap_not_known.py"]  # D4: the gate sets our node's interest; the default is empty

# Stock nodes subscribe to node objects (the template default) and to vault files, the
# application RID type our node federates with conformant manifests (divergence 11).
STOCK_INTEREST = ["orn:koi-net.node", "orn:koi-net.vault-file"]


class GateRefusal(Exception):
    """A target or path the gate will not use. Exit 2."""


class GateMissing(Exception):
    """A prerequisite is absent. Exit 3."""


class GateWallClock(BaseException):
    """The gate ran past --wall-clock. A BaseException, so the `except Exception` around each
    case does not take it for one case's failure and go on to the next. subprocess.run kills
    a pytest it is waiting on when this is raised."""


def _on_wall_clock(_signum, _frame):
    raise GateWallClock()


def free_port() -> int:
    for _ in range(50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        if port != LIVE_PORT:
            return port
    raise GateRefusal("could not find a free loopback port other than 8351")


def base_env(home: Path) -> dict[str, str]:
    """An empty environment plus PATH, a scratch HOME and no bytecode writes."""
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(home),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TMPDIR": str(home),
    }


def check_work_dir(work: Path) -> None:
    real_home = Path(os.path.expanduser("~")).resolve()
    resolved = work.resolve()
    for forbidden in (real_home / "Documents", real_home / ".config", real_home / "Library"):
        if resolved == forbidden or forbidden in resolved.parents:
            raise GateRefusal(f"refusing work dir {resolved}: inside {forbidden}")


def check_prerequisites(node_python: str | None) -> Path:
    if not node_python:
        raise GateRefusal("--node-python (or KOI_GATE_NODE_PYTHON) is required: a deps-complete interpreter for our node")
    py = Path(node_python).expanduser()
    if not py.is_file() or not os.access(py, os.X_OK):
        raise GateMissing(f"node interpreter {py} is not an executable file")
    if not VENV_PYTHON.is_file():
        raise GateMissing(f"{VENV_PYTHON} is missing: run {SETUP.relative_to(REPO_ROOT)}")
    verify = subprocess.run(
        ["bash", str(SETUP), "--verify-only", str(TEMPLATE_SRC.parent)],
        capture_output=True, text=True,
    )
    if verify.returncode != 0:
        raise GateMissing(f"stock node template: {verify.stderr.strip()}; run {SETUP.relative_to(REPO_ROOT)}")
    for tool in ("initdb", "pg_ctl", "psql"):
        if shutil.which(tool) is None:
            raise GateMissing(f"{tool} is not on PATH")
    return py


@dataclass
class Proc:
    name: str
    popen: subprocess.Popen
    log: Path


@dataclass
class Gate:
    work: Path
    node_python: Path
    test_timeout: int = 0
    procs: list[Proc] = field(default_factory=list)
    pg_dir: Path | None = None
    pg_port: int | None = None

    # -- processes ---------------------------------------------------------------

    def spawn(self, name: str, argv: list[str], env: dict[str, str], cwd: Path) -> Proc:
        log = self.work / f"{name}.log"
        fh = open(log, "wb")
        popen = subprocess.Popen(argv, env=env, cwd=cwd, stdin=subprocess.DEVNULL, stdout=fh, stderr=subprocess.STDOUT, start_new_session=True)
        proc = Proc(name, popen, log)
        self.procs.append(proc)
        with open(self.work / "pids.txt", "a") as pids:
            pids.write(f"{popen.pid} {name}\n")
        print(f"gate: started {name} pid={popen.pid} log={log}", flush=True)
        return proc

    def stop(self, proc: Proc) -> None:
        p = proc.popen
        if p.poll() is None:
            # Signal the recorded process group only (start_new_session made it its own).
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(p.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    print(f"gate: {proc.name} pid={p.pid} did not exit after SIGKILL", file=sys.stderr, flush=True)
        if proc in self.procs:
            self.procs.remove(proc)

    def stop_all(self) -> None:
        for proc in list(reversed(self.procs)):
            try:
                self.stop(proc)
            except Exception as exc:  # keep going: the cluster must still stop
                print(f"gate: stopping {proc.name}: {exc!r}", file=sys.stderr, flush=True)
        if self.pg_dir is not None:
            subprocess.run(["pg_ctl", "-D", str(self.pg_dir), "-m", "fast", "-w", "stop"], capture_output=True)
            self.pg_dir = None

    # -- database ----------------------------------------------------------------

    def dsn(self, db: str) -> str:
        return f"postgresql://{PG_USER}@127.0.0.1:{self.pg_port}/{db}"

    def psql(self, db: str, sql: str) -> None:
        r = subprocess.run(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-q", self.dsn(db), "-c", sql], capture_output=True, text=True, env=base_env(self.work))
        if r.returncode != 0:
            raise RuntimeError(f"psql {db}: {r.stderr.strip()}")

    def start_cluster(self) -> None:
        self.pg_dir = self.work / "pg"
        self.pg_port = free_port()
        env = base_env(self.work)
        subprocess.run(["initdb", "-D", str(self.pg_dir), "-U", PG_USER, "-A", "trust", "-E", "UTF8", "--locale=C"], check=True, capture_output=True, env=env)
        opts = f"-c listen_addresses=127.0.0.1 -c port={self.pg_port} -c unix_socket_directories='' -c fsync=off -c synchronous_commit=off"
        r = subprocess.run(["pg_ctl", "-D", str(self.pg_dir), "-l", str(self.work / "postgres.log"), "-o", opts, "-w", "start"], capture_output=True, text=True, env=env)
        if r.returncode != 0:
            raise RuntimeError(f"pg_ctl start: {r.stderr.strip() or r.stdout.strip()}")
        print(f"gate: throwaway cluster on 127.0.0.1:{self.pg_port} ({self.pg_dir})", flush=True)

    def build_template_db(self) -> None:
        self.psql("postgres", "CREATE DATABASE personal_koi")
        self.psql("personal_koi", 'CREATE EXTENSION IF NOT EXISTS vector; CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
        env = base_env(self.work)
        env["DATABASE_URL"] = self.dsn("personal_koi")
        out = self.work / "migrations.log"
        with open(out, "wb") as fh:
            r = subprocess.run(["bash", str(REPO_ROOT / "scripts" / "run_migrations.sh")], env=env, cwd=REPO_ROOT, stdin=subprocess.DEVNULL, stdout=fh, stderr=subprocess.STDOUT)
        if r.returncode != 0:
            raise RuntimeError(f"scripts/run_migrations.sh exited {r.returncode}; see {out}")
        print(f"gate: migrated personal_koi ({out})", flush=True)

    def case_db(self, case: str) -> str:
        name = "koi_test_interop_" + case.replace("-", "_")
        self.psql("postgres", f"CREATE DATABASE {name} TEMPLATE personal_koi")
        return name

    # -- nodes -------------------------------------------------------------------

    def start_our_node(self, case: str, db: str, membrane_on: bool) -> dict:
        root = self.work / case / "ours"
        for sub in ("home", "state", "vault/Shared"):
            (root / sub).mkdir(parents=True, exist_ok=True)
        port = free_port()
        admin_token = secrets.token_hex(16)
        env = base_env(root / "home")
        env.update({
            "POSTGRES_URL": self.dsn(db),
            "KOI_NET_ENABLED": "true",
            "KOI_NODE_NAME": f"conformance-gate-{case}",
            "KOI_STATE_DIR": str(root / "state"),
            "KOI_BASE_URL": f"http://127.0.0.1:{port}/koi-net",
            "KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL": "true" if membrane_on else "false",
            "KOI_FEDERATE_KNOWLEDGE": "true",
            "KOI_POLL_INTERVAL": "2",
            "KOI_NET_RID_TYPES_OF_INTEREST": GATE_INTEREST,
            # D1: membrane off, the gate auto-approves subscriptions; membrane on, the test
            # approves them as an operator would (/koi-net/edges/approve).
            "KOI_NET_AUTO_APPROVE_EDGES": "false" if membrane_on else "true",
            "VAULT_SYNC_ENABLED": "true",
            "VAULT_SYNC_WATCHER": "false",
            "VAULT_SYNC_FOLDER": "Shared",
            "OBSIDIAN_VAULT_PATH": str(root / "vault"),
            "KOI_ADMIN_TOKEN": admin_token,
        })
        argv = [str(self.node_python), "-B", "-m", "uvicorn", "api.personal_ingest_api:app", "--host", "127.0.0.1", "--port", str(port)]
        proc = self.spawn(f"{case}-ours", argv, env, REPO_ROOT)
        url = f"http://127.0.0.1:{port}"
        health = wait_http_json(f"{url}/koi-net/health", proc, timeout=120)
        node = health["node"]
        return {"url": url, "rid": node["node_rid"], "log": proc.log, "admin_token": admin_token, "vault": root / "vault", "proc": proc}

    def start_stock_node(self, case: str, kind: str, ours: dict) -> dict:
        root = self.work / case / "stock"
        (root / "home").mkdir(parents=True, exist_ok=True)
        profile: dict = {"node_type": kind.upper(), "provides": {"event": [], "state": []}}
        config: dict = {
            "koi_net": {
                "node_name": f"stock-{case}",
                "node_profile": profile,
                "rid_types_of_interest": STOCK_INTEREST,
                "cache_directory_path": ".rid_cache",
                "private_key_pem_path": "priv_key.pem",
                "first_contact": {"rid": ours["rid"], "url": f"{ours['url']}/koi-net"},
            },
        }
        port = None
        if kind == "full":
            port = free_port()
            profile["base_url"] = f"http://127.0.0.1:{port}/koi-net"
            config["server"] = {"host": "127.0.0.1", "port": port, "path": "/koi-net"}
        else:
            config["poller"] = {"polling_interval": 1}
        # JSON is YAML; koi-net loads config.yaml with ruamel.
        (root / "config.yaml").write_text(json.dumps(config, indent=2))
        env = base_env(root / "home")
        env["PRIV_KEY_PASSWORD"] = secrets.token_hex(16)
        env["NO_COLOR"] = "1"
        env["KOI_GATE_TEMPLATE_SRC"] = str(TEMPLATE_SRC)
        argv = [str(VENV_PYTHON), "-I", "-B", str(STOCK_NODE), kind, str(root)]
        proc = self.spawn(f"{case}-stock", argv, env, root)
        if port is not None:
            wait_tcp(port, proc, timeout=60)
        else:
            wait_file(root / "priv_key.pem", proc, timeout=60)
        return {"root": root, "url": f"http://127.0.0.1:{port}" if port else None, "log": proc.log, "proc": proc}

    # -- tests -------------------------------------------------------------------

    def run_pytest(self, case: str, targets: dict[str, str], tests: list[str], junit_dir: Path) -> int:
        env = base_env(self.work / case)
        env.update(targets)
        step_log = self.work / case / "steps.ndjson"
        env["KOI_GATE_STEP_LOG"] = str(step_log)
        junit = junit_dir / f"{case}.xml"
        env["KOI_GATE_TEST_TIMEOUT"] = str(self.test_timeout)
        argv = [str(VENV_PYTHON), "-B", "-m", "pytest", *tests, "-p", "no:cacheprovider", "-p", TIMEOUT_PLUGIN,
                "-rxXfEs", f"--junitxml={junit}"]
        print(f"gate: pytest {' '.join(tests)} ({case})", flush=True)
        r = subprocess.run(argv, env=env, cwd=REPO_ROOT, stdin=subprocess.DEVNULL)
        if step_log.exists():
            for line in step_log.read_text().splitlines():
                rec = json.loads(line)
                if rec["outcome"] != "reached":
                    print(f"gate: {case} {rec['test']}: stopped at '{rec['step']}' ({rec['outcome']})", flush=True)
        print(f"gate: case {case} pytest exit {r.returncode} (junit {junit})", flush=True)
        skipped = junit_skips(junit)
        if skipped:
            print(f"gate: case {case} skipped {len(skipped)} test(s); the gate allows none:", flush=True)
            for line in skipped[:20]:
                print(f"gate:   {line}", flush=True)
            return r.returncode or 1
        return r.returncode

    def run_db_tests(self, tests: list[str], junit_dir: Path, allow_skips: bool) -> int:
        db = self.case_db("db-tests")
        env = base_env(self.work / "db-tests-home")
        (self.work / "db-tests-home").mkdir(exist_ok=True)
        env["KOI_TEST_DATABASE_URL"] = self.dsn(db)
        junit = junit_dir / "db-tests.xml"
        env["KOI_GATE_TEST_TIMEOUT"] = str(self.test_timeout)
        argv = [str(self.node_python), "-B", "-m", "pytest", *tests, "-p", "no:cacheprovider", "-p", TIMEOUT_PLUGIN,
                "-rfEs", f"--junitxml={junit}"]
        print(f"gate: pytest {' '.join(tests)} (db-tests, {db})", flush=True)
        r = subprocess.run(argv, env=env, cwd=REPO_ROOT, stdin=subprocess.DEVNULL)
        print(f"gate: db-tests pytest exit {r.returncode} (junit {junit})", flush=True)
        skipped = junit_skips(junit)
        if skipped:
            print(f"gate: db-tests skipped {len(skipped)} test(s)" + ("" if allow_skips else "; the gate's own DB tests allow none:"), flush=True)
            for line in skipped[:20]:
                print(f"gate:   {line}", flush=True)
            if not allow_skips:
                return r.returncode or 1
        return r.returncode

    def run_case(self, case: str, junit_dir: Path) -> int:
        started = len(self.procs)
        try:
            return self._run_case(case, junit_dir)
        finally:
            for proc in list(reversed(self.procs[started:])):
                self.stop(proc)

    def _run_case(self, case: str, junit_dir: Path) -> int:
        db = self.case_db(case)
        if case == "conformance":
            ours = self.start_our_node(case, db, membrane_on=False)
            return self.run_pytest(case, {"KOI_TEST_API_URL": ours["url"], "KOI_GATE_ADMIN_TOKEN": ours["admin_token"]},
                                   ["tests/conformance"], junit_dir)
        kind, membrane = case.split("-")
        ours = self.start_our_node(case, db, membrane_on=(membrane == "on"))
        stock = self.start_stock_node(case, kind, ours)
        targets = {
            "KOI_TEST_API_URL": ours["url"],
            "KOI_TEST_DATABASE_URL": self.dsn(db),
            "KOI_GATE_CASE": case,
            "KOI_GATE_STOCK_KIND": kind,
            "KOI_GATE_MEMBRANE": membrane,
            "KOI_GATE_STOCK_ROOT": str(stock["root"]),
            "KOI_GATE_STOCK_LOG": str(stock["log"]),
            "KOI_GATE_STOCK_PID": str(stock["proc"].popen.pid),
            "KOI_GATE_OURS_LOG": str(ours["log"]),
            "KOI_GATE_OURS_RID": ours["rid"],
            "KOI_GATE_VAULT": str(ours["vault"]),
            "KOI_GATE_ADMIN_TOKEN": ours["admin_token"],
        }
        if stock["url"]:
            targets["KOI_TEST_PEER_API_URL"] = stock["url"]
        return self.run_pytest(case, targets, ["tests/interop/test_gate.py"], junit_dir)


def junit_skips(junit: Path) -> list[str]:
    """Tests a JUnit file records as skipped, other than xfails (JUnit records those as skips too)."""
    import xml.etree.ElementTree as ET
    try:
        root = ET.parse(junit).getroot()
    except (OSError, ET.ParseError) as exc:
        return [f"<unreadable junit {junit}: {exc}>"]
    out = []
    for tc in root.iter("testcase"):
        for child in tc:
            if child.tag == "skipped" and child.get("type") != "pytest.xfail":
                out.append(f"{tc.get('classname')}::{tc.get('name')}: {(child.get('message') or '')[:120]}")
    return out


def wait_http_json(url: str, proc: Proc, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        if proc.popen.poll() is not None:
            raise RuntimeError(f"{proc.name} exited {proc.popen.returncode} before {url} answered; see {proc.log}")
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return json.loads(resp.read())
        except (urllib.error.URLError, ConnectionError, TimeoutError, json.JSONDecodeError) as exc:
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"{url} did not answer in {timeout:.0f}s ({last!r}); see {proc.log}")


def wait_tcp(port: int, proc: Proc, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.popen.poll() is not None:
            raise RuntimeError(f"{proc.name} exited {proc.popen.returncode} before listening on {port}; see {proc.log}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.3)
    raise RuntimeError(f"{proc.name} did not listen on {port} in {timeout:.0f}s; see {proc.log}")


def wait_file(path: Path, proc: Proc, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.popen.poll() is not None:
            raise RuntimeError(f"{proc.name} exited {proc.popen.returncode} before writing {path.name}; see {proc.log}")
        if path.exists():
            return
        time.sleep(0.3)
    raise RuntimeError(f"{proc.name} did not write {path} in {timeout:.0f}s; see {proc.log}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--node-python", default=os.environ.get("KOI_GATE_NODE_PYTHON"), help="deps-complete interpreter for our node")
    ap.add_argument("--cases", default=None, help=f"comma-separated subset of {', '.join(ALL_CASES)} (default: all, or none with --db-tests)")
    ap.add_argument("--db-tests", nargs="+", default=[], metavar="PATH", help="main-suite tests to run against a throwaway database")
    ap.add_argument("--work-dir", help="scratch directory to create; it must not exist (default: a new temporary directory)")
    ap.add_argument("--junit-dir", help="where to write <case>.xml (default: <work-dir>/junit, removed with the work dir after a passing run)")
    ap.add_argument("--keep", action="store_true", help="keep the scratch directory after a passing run")
    ap.add_argument("--check", action="store_true", help="check prerequisites and arguments, start nothing")
    ap.add_argument("--test-timeout", type=int, default=120, metavar="SECONDS",
                    help="fail a test phase that runs longer than this (default: 120; 0: no limit)")
    ap.add_argument("--wall-clock", type=int, default=1800, metavar="SECONDS",
                    help="stop the whole gate after this long, as a failure (default: 1800; 0: no limit)")
    return ap.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.cases is None:
        args.cases = "" if args.db_tests else ",".join(ALL_CASES)
    if not args.db_tests and "conformance" in args.cases.split(","):
        # A full gate run also proves the negotiation paths stock nodes do not reach.
        args.db_tests = list(DEFAULT_DB_TESTS)
    cases = [c.strip() for c in args.cases.split(",") if c.strip()]
    try:
        for path in args.db_tests:
            if not (REPO_ROOT / path).resolve().is_relative_to(REPO_ROOT / "tests"):
                raise GateRefusal(f"--db-tests path {path} is not under tests/")
        unknown = [c for c in cases if c not in ALL_CASES]
        if (not cases and not args.db_tests) or unknown:
            raise GateRefusal(f"unknown case(s) {unknown or cases}; choose from {', '.join(ALL_CASES)}")
        if args.test_timeout < 0 or args.wall_clock < 0:
            raise GateRefusal("--test-timeout and --wall-clock take seconds, 0 or more")
        if args.work_dir:
            work = Path(args.work_dir).expanduser().absolute()
            check_work_dir(work)
            if work.exists() or work.is_symlink():
                # The gate removes its work dir after a passing run, so it only ever uses one it created.
                raise GateRefusal(f"refusing work dir {work}: it already exists; name a new path")
        node_python = check_prerequisites(args.node_python)
    except GateRefusal as exc:
        print(f"gate: refused: {exc}", file=sys.stderr)
        return 2
    except GateMissing as exc:
        print(f"gate: missing prerequisite: {exc}", file=sys.stderr)
        return 3
    if args.check:
        print(f"gate: prerequisites present; cases {', '.join(cases) or 'none'}; db tests {' '.join(args.db_tests) or 'none'}")
        return 0

    if args.work_dir:
        work.mkdir(parents=True)
    else:
        work = Path(tempfile.mkdtemp(prefix="koi-gate-"))
    check_work_dir(work)
    junit_dir = Path(args.junit_dir).expanduser() if args.junit_dir else work / "junit"
    junit_dir.mkdir(parents=True, exist_ok=True)
    gate = Gate(work=work, node_python=node_python, test_timeout=args.test_timeout)
    atexit.register(gate.stop_all)

    def _on_signal(signum, _frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(sig, _on_signal)
    results: dict[str, int] = {}
    if args.wall_clock:
        signal.signal(signal.SIGALRM, _on_wall_clock)
        signal.alarm(args.wall_clock)
    try:
        gate.start_cluster()
        gate.build_template_db()
        if args.db_tests:
            try:
                # Existing suites passed with --db-tests may skip tests for their own reasons;
                # the gate's own DB tests may not.
                results["db-tests"] = gate.run_db_tests(args.db_tests, junit_dir,
                                                        allow_skips=args.db_tests != DEFAULT_DB_TESTS)
            except Exception as exc:
                print(f"gate: db-tests setup failed: {exc}", file=sys.stderr, flush=True)
                results["db-tests"] = 1
        for case in cases:
            try:
                results[case] = gate.run_case(case, junit_dir)
            except Exception as exc:  # a setup failure fails the case, never skips it
                print(f"gate: case {case} setup failed: {exc}", file=sys.stderr, flush=True)
                results[case] = 1
    except Exception as exc:
        print(f"gate: setup failed: {exc}", file=sys.stderr, flush=True)
        results["setup"] = 1
    except GateWallClock:
        signal.alarm(0)
        print(f"gate: wall clock of {args.wall_clock}s ran out; stopping every process it started", file=sys.stderr, flush=True)
        results["wall-clock"] = 1
    except SystemExit as exc:
        print(f"gate: interrupted ({exc.code}); stopping every process it started", file=sys.stderr, flush=True)
        gate.stop_all()
        print(f"gate: scratch directory kept: {work}", flush=True)
        raise
    finally:
        signal.alarm(0)
        gate.stop_all()

    failed = {c: r for c, r in results.items() if r != 0}
    print("gate: results " + ", ".join(f"{c}={r}" for c, r in results.items()), flush=True)
    if failed or args.keep:
        print(f"gate: scratch directory kept: {work}", flush=True)
    else:
        shutil.rmtree(work, ignore_errors=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
