"""scripts/eval_claims_pipeline.py and scripts/check_claims_regression.sh are dry runs
unless --apply is given, and anchor on chain only with --anchor (D13).

Offline: the harness's HTTP helper is replaced by a recorder, and the shell gate runs
with fake curl and python executables that only log their arguments.
"""

from __future__ import annotations

import importlib
import os
import pathlib
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
GATE = REPO / "scripts" / "check_claims_regression.sh"


@pytest.fixture
def harness(monkeypatch, tmp_path):
    mod = importlib.import_module("scripts.eval_claims_pipeline")
    calls: list[str] = []

    def fake_req(method, path, body=None):
        calls.append(path)
        if path == "/health":
            return 200, {"status": "healthy"}
        if path == "/claims/chain-info":
            return 200, {"chain_id": "test-chain", "is_testnet": True}
        if path == "/entity/resolve":
            return 200, {"candidates": [{"uri": f"orn:test:entity-{len(calls)}"}], "is_new": False}
        if path == "/claims/":
            return 201, {"claim_rid": "orn:test:claim-1"}
        if path.endswith("/prepare-anchor"):
            return 200, {"content_hash": "abc"}
        if path.endswith("/proof-pack"):
            return 200, {"claim_content_hash_verified": True}
        return 200, {}

    monkeypatch.setattr(mod, "_req", fake_req)
    monkeypatch.setattr(mod, "_SERVICE_TOKEN", "test-token")
    monkeypatch.chdir(tmp_path)
    return mod, calls, tmp_path


def _files(root: pathlib.Path) -> list[pathlib.Path]:
    return [p for p in root.rglob("*") if p.is_file()]


def test_help_exits_zero_and_sends_nothing(harness):
    mod, calls, root = harness
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0
    assert calls == []
    assert _files(root) == []


@pytest.mark.parametrize("argv", [[], ["--dry-run"], ["--anchor"], ["--dry-run", "--apply", "--anchor"]])
def test_without_apply_it_is_a_dry_run(harness, capsys, argv):
    mod, calls, root = harness
    mod.main(argv)
    assert calls == []
    assert _files(root) == []
    assert "DRY RUN" in capsys.readouterr().out


def test_apply_runs_the_pipeline_without_anchoring(harness):
    mod, calls, root = harness
    mod.main(["--apply", "--save", "out.json"])
    assert "/claims/" in calls
    assert any(p.endswith("/prepare-anchor") for p in calls)
    assert not any(p.endswith("/anchor") for p in calls)
    assert not any(p.endswith("/proof-pack") for p in calls)
    assert (root / "out.json").exists()


def test_apply_anchor_reaches_the_anchor_step(harness):
    mod, calls, root = harness
    mod.main(["--apply", "--anchor", "--save", "out.json"])
    assert any(p.endswith("/anchor") for p in calls)
    assert any(p.endswith("/proof-pack") for p in calls)


def test_old_skip_anchor_flag_still_parses(harness):
    mod, calls, _ = harness
    mod.main(["--apply", "--skip-anchor", "--save", "out.json"])
    assert not any(p.endswith("/anchor") for p in calls)


# --- the shell gate --------------------------------------------------------------

@pytest.fixture
def gate_env(tmp_path):
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    log = tmp_path / "calls.log"
    for name in ("curl", "python", "python3"):
        exe = fakebin / name
        exe.write_text(f'#!/bin/sh\necho "{name} $*" >> "{log}"\nexit 0\n')
        exe.chmod(0o755)
    work = tmp_path / "work"
    work.mkdir()
    env = {"PATH": f"{fakebin}:/usr/bin:/bin", "HOME": str(tmp_path)}
    return work, env, log


def _gate(args, work, env):
    return subprocess.run(["bash", str(GATE), *args], cwd=work, env=env,
                          capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize("args", [[], ["--dry-run"], ["5"], ["--runs", "2"], ["--dry-run", "--apply"]])
def test_gate_without_apply_calls_nothing(gate_env, args):
    work, env, log = gate_env
    r = _gate(args, work, env)
    assert r.returncode == 0, r.stderr
    assert "dry run" in r.stdout
    assert not log.exists()


def test_gate_help(gate_env):
    work, env, log = gate_env
    r = _gate(["--help"], work, env)
    assert r.returncode == 0 and "Usage" in r.stdout
    assert not log.exists()


def test_gate_rejects_unknown_argument(gate_env):
    work, env, log = gate_env
    r = _gate(["--bogus"], work, env)
    assert r.returncode == 2
    assert not log.exists()


def test_gate_apply_runs_the_harness_without_anchoring(gate_env):
    work, env, log = gate_env
    (work / "docs" / "eval").mkdir(parents=True)
    (work / "docs" / "eval" / "claims-baseline.json").write_text("{}")
    r = _gate(["--apply", "--runs", "2"], work, env)
    assert r.returncode == 0, r.stderr
    lines = log.read_text().splitlines()
    assert lines[0].startswith("curl ")
    harness_line = lines[-1]
    assert harness_line.startswith("python -m scripts.eval_claims_pipeline --apply --skip-anchor --runs 2 ")
    assert "--anchor " not in harness_line + " "
