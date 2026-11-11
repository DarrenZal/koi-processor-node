"""Entry-point scripts import/run under their real systemd invocation (offline).

The rest of the suite imports helper modules via an explicit ``sys.path.insert``,
which does NOT catch a module placed where the entry-point *script* cannot import
it at runtime. That gap caused a koi-forwarder crash-loop on 2026-11-05:
``forwarder_delivery`` was placed in ``src/core/`` but the forwarder script runs
from ``scripts/`` (its own directory is ``sys.path[0]``) with ``PYTHONPATH=src``,
so a bare ``from forwarder_delivery import ...`` raised ``ModuleNotFoundError`` at
startup. These tests run each touched entry point the way its systemd unit does -
repo-root cwd, ``PYTHONPATH=<repo>/src`` - and fail if an import does not resolve.

They shell out with ``sys.executable``, so they need the service dependencies
(fastapi/asyncpg/rdflib/httpx). Under the deps-complete gate they run; under a
deps-light interpreter they skip, like the other behavioural tests here.
"""
from __future__ import annotations

import os
import pathlib
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]


def _deps_present() -> bool:
    try:
        import asyncpg  # noqa: F401
        import fastapi  # noqa: F401
        import httpx  # noqa: F401
        import rdflib  # noqa: F401

        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _deps_present(),
    reason="entry-point subprocess tests need the deps-complete interpreter",
)


def _unit_env(**extra: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO / "src")  # the units' Environment=PYTHONPATH
    env.update(extra)
    return env


def test_forwarder_entrypoint_imports_like_systemd():
    """koi-forwarder.service runs ``python scripts/coordinator_to_eventbridge_forwarder.py``
    from the repo root with ``PYTHONPATH=src``. Running the script puts ``scripts/``
    on ``sys.path[0]``; import the module (top-level only, no ``__main__`` poll loop)
    and assert every import - including the scripts/-local helpers - resolves."""
    code = (
        "import sys; sys.path.insert(0, 'scripts'); "
        "import coordinator_to_eventbridge_forwarder as m; "
        "assert hasattr(m, 'poll_once'); print('FORWARDER_IMPORT_OK')"
    )
    r = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO),
        env=_unit_env(),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert r.returncode == 0, f"forwarder entry-point import failed:\n{r.stderr}"
    assert "FORWARDER_IMPORT_OK" in r.stdout


def test_bridge_preflight_runs_like_systemd():
    """koi-event-bridge.service runs ``python src/core/koi_event_bridge_v2.py`` from
    the repo root with ``PYTHONPATH=src``. ``--preflight`` loads config and exits
    before uvicorn and the DB pool, so it is offline. With a usable key it exits 0;
    this also proves the whole bridge import chain resolves under the real cwd."""
    r = subprocess.run(
        [sys.executable, "src/core/koi_event_bridge_v2.py", "--preflight"],
        cwd=str(REPO),
        env=_unit_env(KOI_SERVICE_KEYS="k" * 40, KOI_AUTH_MODE_BRIDGE="warn"),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert r.returncode == 0, (
        f"bridge --preflight failed (rc={r.returncode}):\n{r.stdout}\n{r.stderr}"
    )


def test_bge_server_preflight_runs_like_systemd():
    """koi-bge-server.service runs ``python src/core/bge_server.py``. ``--preflight``
    validates config and exits before uvicorn, so it is offline. With an API key
    present it exits 0; this proves the bge import chain (incl. the sibling
    ``bge_hardening`` module) resolves under the real cwd."""
    r = subprocess.run(
        [sys.executable, "src/core/bge_server.py", "--preflight"],
        cwd=str(REPO),
        env=_unit_env(OPENAI_API_KEY="offline-preflight-test-key-not-real"),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert r.returncode == 0, (
        f"bge --preflight failed (rc={r.returncode}):\n{r.stdout}\n{r.stderr}"
    )
