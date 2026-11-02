"""Test isolation for the koi-processor suite.

Tests reach a database or an API only through tests/koi_test_targets.py, which reads
KOI_TEST_DATABASE_URL, KOI_TEST_ADMIN_DSN and KOI_TEST_API_URL and nothing else. A test
whose variable is unset skips and names it.

This file does these things before any test module is imported:

1. Points the runtime variables that code under test reads at import (POSTGRES_URL,
   DATABASE_URL, KOI_API_URL, PGHOST, the local service URLs and others) at the
   KOI_TEST_* values, or at an address that cannot resolve when a value is unset. An
   inherited live value never reaches the code. This must happen at module level: pytest
   imports test modules during collection, before any fixture runs.
2. Points HOME, KOI_STATE_DIR (node keys), the vault paths, repair state and the auth log
   at a temporary directory for this run (removed at exit), so no test reads or creates
   keys or other state in the live default locations. PGPASSFILE and
   KOI_READ_SCOPES_FILE point at empty files there, and KOI_AUTH_MODE is set to its
   default, `log`, so neither a live password file nor the live access configuration
   reaches a test.
3. Turns python-dotenv's load_dotenv into a no-op, so imported code cannot fill unset
   variables from a .env or personal.env file.
4. In pytest_configure, ends the run before collection if a KOI_TEST_* value looks live
   (database personal_koi, without "test" as a word of its name, or named in the query
   string; API on port 8351; a control character anywhere), unless KOI_TEST_ALLOW_LIVE=1.
5. Skips every test marked `live` (it calls a live LLM API) unless KOI_TEST_LLM=1. A
   provider key in the environment is not enough.
"""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile

from tests import koi_test_targets as targets

targets.redirect_runtime_config(os.environ)

STATE_ROOT = tempfile.mkdtemp(prefix="koi-test-state-")
atexit.register(shutil.rmtree, STATE_ROOT, ignore_errors=True)
targets.redirect_state_dirs(STATE_ROOT, os.environ)

try:
    import dotenv as _dotenv
    import dotenv.main as _dotenv_main
except ImportError:  # pragma: no cover - python-dotenv is a runtime dependency
    _dotenv = None
if _dotenv is not None:
    def _no_dotenv(*_args, **_kwargs) -> bool:
        return False
    _dotenv.load_dotenv = _no_dotenv
    _dotenv_main.load_dotenv = _no_dotenv

import pytest  # noqa: E402


def pytest_configure(config):
    targets.check()
    # Again after plugins load, in case one changed the environment.
    targets.redirect_runtime_config(os.environ)
    targets.redirect_state_dirs(STATE_ROOT, os.environ)


def pytest_collection_modifyitems(config, items):
    reason = targets.live_llm_skip_reason()
    if reason is None:
        return
    skip = pytest.mark.skip(reason=reason)
    for item in items:
        if item.get_closest_marker("live") is not None:
            item.add_marker(skip)


def pytest_report_header(config):
    lines = []
    for var in (targets.DATABASE_VAR, targets.ADMIN_VAR, targets.API_VAR):
        value = os.environ.get(var, "").strip()
        if not value:
            lines.append(f"{var}: unset (tests that need it skip)")
            continue
        parts = targets.url_parts(value)
        where = f"{parts['host']}:{parts['port']}"
        if var != targets.API_VAR:
            where += f"/{parts['database']}"
        lines.append(f"{var}: {where}")
    if targets.allow_live():
        lines.append(f"{targets.ALLOW_LIVE_VAR}=1: the live-target refusal is OFF")
    return lines


@pytest.fixture(scope="session")
def test_dsn():
    """KOI_TEST_DATABASE_URL, or skip."""
    return targets.database_url()
