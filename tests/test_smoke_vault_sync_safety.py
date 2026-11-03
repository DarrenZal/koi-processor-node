"""scripts/federation/smoke-vault-sync.sh runs only with --apply, only on targets the
test-target guard accepts, and has no default peer or vault (D13).

Offline: curl, psql, ssh and the guard's python run as fakes that log their arguments.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

from tests import koi_test_targets as _targets

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "federation" / "smoke-vault-sync.sh"


@pytest.fixture
def smoke(tmp_path):
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    log = tmp_path / "calls.log"
    for name, rc in (("curl", 0), ("psql", 97), ("ssh", 97)):
        exe = fakebin / name
        exe.write_text(f'#!/bin/sh\necho "{name} $*" >> "{log}"\nexit {rc}\n')
        exe.chmod(0o755)
    koi_python = fakebin / "koi-python"
    koi_python.write_text(
        f'#!/bin/sh\necho "koi_python $*" >> "{log}"\n'
        'case "$*" in *" cleanup "*|*" record "*) exit 0 ;; esac\n'
        f'exec "{sys.executable}" "$@"\n')
    koi_python.chmod(0o755)
    vault = tmp_path / "vault"
    vault.mkdir()
    base = {"PATH": f"{fakebin}:/usr/bin:/bin", "HOME": str(tmp_path),
            "KOI_PYTHON": str(koi_python)}

    def run(*args, **env):
        return subprocess.run(["bash", str(SCRIPT), *args], env={**base, **env},
                              capture_output=True, text=True, timeout=60)
    return run, log, vault


def _calls(log):
    return log.read_text().splitlines() if log.exists() else []


def _acting(calls):
    return [c for c in calls if c.split(" ", 1)[0] in ("curl", "psql", "ssh")]


SAFE = {"KOI_TEST_API_URL": "http://127.0.0.1:59998",
        "KOI_TEST_DATABASE_URL": "postgresql://koi@127.0.0.1:1/koi_test_vault",
        "PEER_NAME": "test-peer"}


def test_help(smoke):
    run, log, _ = smoke
    r = run("--help")
    assert r.returncode == 0 and "Usage" in r.stdout
    assert _calls(log) == []


@pytest.mark.parametrize("env", [{}, SAFE, {"MODE": "two-peer"}])
def test_without_apply_runs_nothing(smoke, env):
    run, log, vault = smoke
    r = run(**env)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "dry run" in r.stdout
    assert _acting(_calls(log)) == []
    assert list(vault.iterdir()) == []


def test_dry_run_wins_over_apply(smoke):
    run, log, vault = smoke
    r = run("--apply", "--dry-run", **SAFE, VAULT_PATH=str(vault))
    assert r.returncode == 0 and "dry run" in r.stdout
    assert _acting(_calls(log)) == []


@pytest.mark.parametrize("missing", ["PEER_NAME", "VAULT_PATH"])
def test_apply_needs_peer_and_vault(smoke, missing):
    run, log, vault = smoke
    env = {**SAFE, "VAULT_PATH": str(vault)}
    env.pop(missing)
    r = run("--apply", **env)
    assert r.returncode == 1 and "no default" in r.stdout
    assert _acting(_calls(log)) == []


def test_two_peer_needs_ssh_and_peer_vault(smoke):
    run, log, vault = smoke
    r = run("--apply", **SAFE, VAULT_PATH=str(vault), MODE="two-peer", PEER_SSH="u@h")
    assert r.returncode == 1
    assert _acting(_calls(log)) == []


@pytest.mark.parametrize("targets", [
    {},
    {"KOI_TEST_API_URL": f"http://127.0.0.1:{_targets.LIVE_API_PORT}",
     "KOI_TEST_DATABASE_URL": f"postgresql://koi@127.0.0.1:1/{_targets.LIVE_DATABASE}"},
])
def test_apply_refuses_live_or_unnamed_targets(smoke, targets):
    run, log, vault = smoke
    r = run("--apply", PEER_NAME="test-peer", VAULT_PATH=str(vault), **targets)
    assert r.returncode in (2, 4), r.stdout + r.stderr
    assert _acting(_calls(log)) == []
    assert list(vault.iterdir()) == []


def test_apply_on_test_targets_reaches_the_preflight(smoke):
    run, log, vault = smoke
    r = run("--apply", **SAFE, VAULT_PATH=str(vault))
    calls = _calls(log)
    # Past the guard: health check against the test API, then the test database (the fake
    # psql fails, so the run stops there), and the cleanup trap reaches the guard's purge.
    assert r.returncode == 1, r.stdout + r.stderr
    assert any(c.startswith("curl ") and "http://127.0.0.1:59998/health" in c for c in calls)
    assert any(c.startswith("psql ") and "postgresql://koi@127.0.0.1:1/koi_test_vault" in c for c in calls)
    assert any(" cleanup " in c for c in calls)
    assert "restored 0 of 0" in r.stdout
