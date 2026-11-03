"""The TerminusDB scripts act only on an explicit flag (D13).

- scripts/terminusdb/import_from_postgres.py imports only with --apply; --fresh (which can
  drop the TerminusDB database) also needs --confirm-destructive; POSTGRES_URL has no default.
- scripts/terminusdb/outbox_worker.py starts its loop only with --run.
- scripts/terminusdb/run_phase0.sh runs only with --apply; clean and --fresh also need
  --confirm-destructive.
- scripts/terminusdb/smoke_phase1.sh runs only with --apply, and then only on targets the
  test-target guard accepts (tests/live_write_shell_guard.sh).

Offline: database, TerminusDB and HTTP clients are replaced by recorders, and the shell
scripts run with fake docker, curl, psql, lsof and python executables that log their
arguments.
"""

from __future__ import annotations

import importlib
import pathlib
import shutil
import subprocess
import sys

import pytest

from tests import koi_test_targets as _targets

REPO = pathlib.Path(__file__).resolve().parents[1]


# --- import_from_postgres.py -------------------------------------------------------

@pytest.fixture
def importer(monkeypatch):
    mod = importlib.import_module("scripts.terminusdb.import_from_postgres")
    calls = []
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod.psycopg2, "connect", lambda *a, **k: calls.append(("pg", a)) or 1 / 0)
    monkeypatch.setattr(mod, "run_import", lambda fresh=False: calls.append(("import", fresh)) or {})
    return mod, calls


def test_importer_help(importer):
    mod, calls = importer
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0 and calls == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--fresh"], ["--fresh", "--confirm-destructive"],
                                  ["--dry-run", "--apply", "--fresh", "--confirm-destructive"]])
def test_importer_without_apply_is_a_dry_run(importer, capsys, argv):
    mod, calls = importer
    assert mod.main(argv) == 0
    assert calls == []
    assert "DRY RUN" in capsys.readouterr().out


def test_importer_fresh_needs_confirmation(importer):
    mod, calls = importer
    assert mod.main(["--apply", "--fresh"]) == 2
    assert calls == []


def test_importer_apply_runs_the_import(importer):
    mod, calls = importer
    assert mod.main(["--apply"]) == 0
    assert calls == [("import", False)]
    assert mod.main(["--apply", "--fresh", "--confirm-destructive"]) == 0
    assert calls[-1] == ("import", True)


def test_importer_has_no_default_database(importer, monkeypatch):
    mod, calls = importer
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    with pytest.raises(RuntimeError):
        mod.get_pg_connection()
    assert calls == []


# --- outbox_worker.py ---------------------------------------------------------------

@pytest.fixture
def worker(monkeypatch):
    mod = importlib.import_module("scripts.terminusdb.outbox_worker")
    started = []

    async def fake_main():
        started.append("loop")

    monkeypatch.setattr(mod, "_load_env", lambda: None)
    monkeypatch.setattr(mod, "main", fake_main)
    return mod, started


def test_worker_help(worker):
    mod, started = worker
    with pytest.raises(SystemExit) as exc:
        mod.cli(["--help"])
    assert exc.value.code == 0 and started == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--dry-run", "--run"]])
def test_worker_without_run_starts_nothing(worker, capsys, argv):
    mod, started = worker
    assert mod.cli(argv) == 0
    assert started == []
    assert "DRY RUN" in capsys.readouterr().out


def test_worker_run_starts_the_loop(worker):
    mod, started = worker
    assert mod.cli(["--run"]) == 0
    assert started == ["loop"]


def test_worker_has_no_default_database(monkeypatch):
    mod = importlib.import_module("scripts.terminusdb.outbox_worker")
    pools = []
    monkeypatch.setattr(mod, "_load_env", lambda: None)
    monkeypatch.setattr(mod, "_IMPORT_ERROR", None)
    monkeypatch.setattr(mod.asyncpg, "create_pool", lambda *a, **k: pools.append(a))
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    import asyncio
    with pytest.raises(SystemExit):
        asyncio.run(mod.main())
    assert pools == []


# --- the shell scripts -----------------------------------------------------------------

def _fakebin(tmp_path, docker_ps=""):
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    log = tmp_path / "calls.log"
    for name in ("docker", "curl", "psql", "lsof", "uvicorn"):
        body = f'#!/bin/sh\necho "{name} $*" >> "{log}"\n'
        if name == "docker" and docker_ps:
            body += f'case "$1" in ps) echo "{docker_ps}"; exit 0 ;; esac\n'
        body += "exit 97\n" if name in ("psql", "lsof") else "exit 0\n"
        (fakebin / name).write_text(body)
        (fakebin / name).chmod(0o755)
    py = fakebin / "python3"
    py.write_text(f'#!/bin/sh\necho "python3 $*" >> "{log}"\nexit 0\n')
    py.chmod(0o755)
    koi_python = fakebin / "koi-python"
    koi_python.write_text(
        f'#!/bin/sh\necho "koi_python $*" >> "{log}"\n'
        'case "$*" in *" cleanup "*|*" record "*) exit 0 ;; esac\n'
        f'exec "{sys.executable}" "$@"\n')
    koi_python.chmod(0o755)
    return fakebin, log


def _calls(log):
    return log.read_text().splitlines() if log.exists() else []


@pytest.fixture
def phase0(tmp_path):
    root = tmp_path / "tree"
    (root / "scripts" / "terminusdb").mkdir(parents=True)
    shutil.copy(REPO / "scripts" / "terminusdb" / "run_phase0.sh", root / "scripts" / "terminusdb")
    fakebin, log = _fakebin(tmp_path, docker_ps="terminusdb")
    env = {"PATH": f"{fakebin}:/usr/bin:/bin", "HOME": str(tmp_path)}

    def run(*args):
        return subprocess.run(["bash", str(root / "scripts" / "terminusdb" / "run_phase0.sh"), *args],
                              env=env, capture_output=True, text=True, timeout=60)
    return run, log, root


@pytest.mark.parametrize("args", [["--help"], [], ["0a"], ["0b"], ["all", "--fresh"], ["clean"],
                                  ["clean", "--confirm-destructive"], ["0a", "--dry-run", "--apply"]])
def test_phase0_without_apply_runs_nothing(phase0, args):
    run, log, root = phase0
    r = run(*args)
    assert r.returncode == 0, r.stdout + r.stderr
    assert _calls(log) == []
    assert not (root / "scripts" / "terminusdb" / "artifacts").exists()


@pytest.mark.parametrize("args", [["clean", "--apply"], ["0a", "--fresh", "--apply"]])
def test_phase0_destructive_needs_confirmation(phase0, args):
    run, log, root = phase0
    r = run(*args)
    assert r.returncode == 2
    assert _calls(log) == []


def test_phase0_apply_runs_the_import(phase0):
    run, log, _ = phase0
    r = run("0a", "--apply")
    assert r.returncode == 0, r.stdout + r.stderr
    calls = _calls(log)
    assert "python3 -m scripts.terminusdb.import_from_postgres --apply" in calls
    assert "python3 -m scripts.terminusdb.test_merge --import-first" in calls
    assert not any(c.startswith("docker rm") for c in calls)


def test_phase0_clean_with_confirmation_removes_containers(phase0):
    run, log, _ = phase0
    r = run("clean", "--apply", "--confirm-destructive")
    assert r.returncode == 0, r.stdout + r.stderr
    assert any(c.startswith("docker rm -f terminusdb") for c in _calls(log))


@pytest.fixture
def smoke(tmp_path):
    fakebin, log = _fakebin(tmp_path, docker_ps="terminusdb")
    base = {"PATH": f"{fakebin}:/usr/bin:/bin", "HOME": str(tmp_path),
            "KOI_PYTHON": str(fakebin / "koi-python")}

    def run(*args, **env):
        return subprocess.run(["bash", str(REPO / "scripts" / "terminusdb" / "smoke_phase1.sh"), *args],
                              env={**base, **env}, capture_output=True, text=True, timeout=60)
    return run, log


SAFE_TARGETS = {"KOI_TEST_API_URL": "http://127.0.0.1:59999",
                "KOI_TEST_DATABASE_URL": "postgresql://koi@127.0.0.1:1/koi_test_smoke"}


def _acting(calls):
    return [c for c in calls if c.split(" ", 1)[0] in ("docker", "curl", "psql", "uvicorn", "lsof")]


def test_smoke_help(smoke):
    run, log = smoke
    r = run("--help")
    assert r.returncode == 0 and "Usage" in r.stdout
    assert _calls(log) == []


@pytest.mark.parametrize("env", [{}, SAFE_TARGETS])
def test_smoke_without_apply_runs_nothing(smoke, env):
    run, log = smoke
    r = run(**env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "dry run" in r.stdout
    assert _acting(_calls(log)) == []


@pytest.mark.parametrize("env", [
    {},
    {"KOI_TEST_API_URL": f"http://127.0.0.1:{_targets.LIVE_API_PORT}",
     "KOI_TEST_DATABASE_URL": f"postgresql://koi@127.0.0.1:1/{_targets.LIVE_DATABASE}"},
    {**SAFE_TARGETS, "TERMINUSDB_DB": "koi_knowledge_graph"},
    {**SAFE_TARGETS, "KOI_TEST_API_URL": "http://192.0.2.1:59999"},
])
def test_smoke_apply_refuses_live_or_unnamed_targets(smoke, env):
    run, log = smoke
    r = run("--apply", **env)
    assert r.returncode != 0
    assert _acting(_calls(log)) == []


def test_smoke_fresh_needs_confirmation(smoke):
    run, log = smoke
    r = run("--apply", "--fresh", **SAFE_TARGETS)
    assert r.returncode == 2
    assert _acting(_calls(log)) == []


def test_smoke_apply_on_test_targets_reaches_the_preflight(smoke):
    run, log = smoke
    r = run("--apply", **SAFE_TARGETS)
    calls = _calls(log)
    # Past the guard: the preflight checks the port, the container and the test database
    # (the fake psql fails, so the run stops there), and cleanup runs on exit.
    assert r.returncode == 1, r.stdout + r.stderr
    assert any(c.startswith("docker ps") for c in calls)
    assert any(c.startswith("psql postgresql://koi@127.0.0.1:1/koi_test_smoke") for c in calls)
    assert any(" cleanup " in c for c in calls)
