"""--help and dry runs finish before optional packages are imported (D13 follow-up).

- scripts/terminusdb/test_merge.py: --help and --dry-run need no terminusdb-client. With
  no arguments it runs only its own tests; the Postgres to TerminusDB import, which writes
  the node's mirror database, runs only with --import-first (run_phase0.sh passes it).
- src/cat/cat_receipt_chain_v2.py: --help and the dry run need no rdflib.
- src/core/koi_event_bridge_semantic.py: --help needs no rdflib.

Offline: each script runs in a subprocess whose PYTHONPATH puts stub or blocked packages
first. Nothing here opens a socket or a database.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import textwrap

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]


def _blocked(tmp_path, name):
    """A package directory whose `name` raises ImportError on import."""
    root = tmp_path / f"blocked-{name}"
    (root / name).mkdir(parents=True)
    (root / name / "__init__.py").write_text(f"raise ImportError('{name} is blocked in this test')\n")
    return root


def _env(tmp_path, *paths):
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tmp_path),
        "PYTHONPATH": os.pathsep.join([str(p) for p in paths] + [str(REPO)]),
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def _run(args, env, cwd=REPO):
    return subprocess.run([sys.executable, *args], cwd=cwd, env=env, capture_output=True,
                          text=True, timeout=60)


# --- test_merge.py ---------------------------------------------------------------

@pytest.mark.parametrize("argv", [["--help"], ["--dry-run"], ["--dry-run", "--import-first"]])
def test_merge_help_and_dry_run_import_nothing(tmp_path, argv):
    env = _env(tmp_path, _blocked(tmp_path, "terminusdb_client"))
    res = _run(["-m", "scripts.terminusdb.test_merge", *argv], env)
    assert res.returncode == 0, res.stderr
    assert "blocked" not in res.stderr
    if argv == ["--help"]:
        assert "--import-first" in res.stdout
    else:
        assert "Dry run" in res.stdout


def test_merge_blocked_package_is_effective(tmp_path):
    """Control: without --help or --dry-run the blocked import is reached."""
    env = _env(tmp_path, _blocked(tmp_path, "terminusdb_client"))
    res = _run(["-m", "scripts.terminusdb.test_merge"], env)
    assert res.returncode != 0
    assert "terminusdb_client is blocked" in res.stderr


def _stub_terminusdb(tmp_path):
    """A terminusdb_client whose client logs and stops the run before any request."""
    root = tmp_path / "stub"
    pkg = root / "terminusdb_client"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(textwrap.dedent("""
        import os


        class WOQLClient:
            def __init__(self, *args, **kwargs):
                with open(os.environ["STUB_LOG"], "a") as fh:
                    fh.write("client\\n")
                raise SystemExit(97)
    """))
    (pkg / "woqlschema.py").write_text(textwrap.dedent("""
        class Schema:
            def __init__(self, *args, **kwargs):
                pass


        class LexicalKey:
            def __init__(self, *args, **kwargs):
                pass


        class DocumentTemplate:
            pass
    """))
    return root


_RUNNER = textwrap.dedent("""
    import os, runpy, sys, types

    fake = types.ModuleType("scripts.terminusdb.import_from_postgres")

    def run_import(fresh=False):
        with open(os.environ["STUB_LOG"], "a") as fh:
            fh.write("import\\n")
        return {}

    fake.run_import = run_import
    sys.modules["scripts.terminusdb.import_from_postgres"] = fake
    sys.argv = ["test_merge"] + sys.argv[1:]
    runpy.run_module("scripts.terminusdb.test_merge", run_name="__main__", alter_sys=True)
""")


@pytest.mark.parametrize("argv,expected", [([], ["client"]),
                                           (["--import-first"], ["import", "client"])])
def test_merge_imports_only_with_import_first(tmp_path, argv, expected):
    log = tmp_path / "stub.log"
    env = _env(tmp_path, _stub_terminusdb(tmp_path))
    env["STUB_LOG"] = str(log)
    results = REPO / "scripts" / "terminusdb" / "results.json"
    before = results.read_bytes() if results.exists() else None
    res = _run(["-c", _RUNNER, *argv], env)
    assert res.returncode == 97, res.stderr
    assert log.read_text().split() == expected
    after = results.read_bytes() if results.exists() else None
    assert after == before


def test_phase0_passes_import_first():
    text = (REPO / "scripts" / "terminusdb" / "run_phase0.sh").read_text()
    runs = [line for line in text.splitlines()
            if "scripts.terminusdb.test_merge" in line and not line.lstrip().startswith("#")]
    assert runs and all("--import-first" in line for line in runs)


# --- the two rdflib entry points ------------------------------------------------------

@pytest.mark.parametrize("argv,expect", [(["--help"], "--apply"), ([], "Dry run"),
                                         (["--dry-run", "--apply"], "Dry run")])
def test_receipt_chain_v2_cli_needs_no_rdflib(tmp_path, argv, expect):
    env = _env(tmp_path, _blocked(tmp_path, "rdflib"))
    res = _run([str(REPO / "src" / "cat" / "cat_receipt_chain_v2.py"), *argv], env)
    assert res.returncode == 0, res.stderr
    assert expect in res.stdout


def test_receipt_chain_v2_blocked_rdflib_is_effective(tmp_path):
    """Control: --apply goes on to the rdflib import (and stops there, contacting nothing)."""
    env = _env(tmp_path, _blocked(tmp_path, "rdflib"))
    res = _run([str(REPO / "src" / "cat" / "cat_receipt_chain_v2.py"), "--apply"], env)
    assert res.returncode != 0
    assert "rdflib is blocked" in res.stderr


def test_semantic_bridge_help_needs_no_rdflib(tmp_path):
    pytest.importorskip("fastapi")
    pytest.importorskip("asyncpg")
    env = _env(tmp_path, _blocked(tmp_path, "rdflib"))
    res = _run([str(REPO / "src" / "core" / "koi_event_bridge_semantic.py"), "--help"], env)
    assert res.returncode == 0, res.stderr
    assert "8004" in res.stdout


def test_semantic_bridge_blocked_rdflib_is_effective(tmp_path):
    """Control: imported as a module (no --help), the module reaches the rdflib import."""
    pytest.importorskip("fastapi")
    pytest.importorskip("asyncpg")
    env = _env(tmp_path, _blocked(tmp_path, "rdflib"))
    code = ("import runpy; runpy.run_path("
            f"{str(REPO / 'src' / 'core' / 'koi_event_bridge_semantic.py')!r}, run_name='bridge')")
    res = _run(["-c", code], env)
    assert res.returncode != 0
