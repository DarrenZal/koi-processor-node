"""scripts/tier-3/run-migration.sh runs no SQL unless --apply is given (D13).

Its old --dry-run wrapped the file in BEGIN; ... ROLLBACK;, and a COMMIT inside the file
committed the work. The dry run now only reads the file. Offline: psql is a fake that
logs its arguments.
"""

from __future__ import annotations

import pathlib
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "tier-3" / "run-migration.sh"

DSN = "postgresql://koi:changeme@127.0.0.1:1/koi_test_probe"

SQL = """\\echo 'pre-state'
SELECT 1;
BEGIN;
CREATE TABLE d13_probe (id int);
UPDATE d13_probe SET id = 2;
COMMIT;
SELECT 2;
"""


@pytest.fixture
def env(tmp_path):
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    log = tmp_path / "psql.log"
    psql = fakebin / "psql"
    psql.write_text(f'#!/bin/sh\necho "$*" >> "{log}"\nexit 0\n')
    psql.chmod(0o755)
    sql = tmp_path / "m.sql"
    sql.write_text(SQL)
    e = {"PATH": f"{fakebin}:/usr/bin:/bin", "HOME": str(tmp_path),
         "POSTGRES_URL": DSN}
    return e, sql, log


def _run(args, e):
    return subprocess.run(["bash", str(SCRIPT), *args], env=e, capture_output=True,
                          text=True, timeout=30)


def test_help(env):
    e, _, log = env
    r = _run(["--help"], e)
    assert r.returncode == 0 and "Usage" in r.stdout
    assert not log.exists()


def test_no_arguments_runs_nothing(env):
    e, _, log = env
    r = _run([], e)
    assert r.returncode == 1
    assert not log.exists()


@pytest.mark.parametrize("flags", [[], ["--dry-run"], ["--dry-run", "--apply"], ["--apply", "--dry-run"]])
def test_dry_run_reads_the_file_and_runs_no_sql(env, flags):
    e, sql, log = env
    r = _run([str(sql), *flags], e)
    assert r.returncode == 0, r.stderr
    assert not log.exists()
    assert "no SQL is run" in r.stdout
    assert "6:COMMIT;" in r.stdout
    assert "4:CREATE TABLE d13_probe" in r.stdout
    assert "changeme" not in r.stdout + r.stderr


def test_unknown_flag_is_refused(env):
    e, sql, log = env
    r = _run([str(sql), "--live"], e)
    assert r.returncode == 2
    assert not log.exists()


def test_apply_runs_psql_on_the_file(env):
    e, sql, log = env
    r = _run([str(sql), "--apply"], e)
    assert r.returncode == 0, r.stderr
    assert log.read_text().strip() == f"{DSN} -f {sql}"
    assert "changeme" not in r.stdout + r.stderr
