"""scripts/run_migrations_with_backup.sh backs up, prunes and migrates only with --apply (D13).

Offline: the script runs from a copy in a temporary tree (so its backups/ directory is
there too), with fake psql and pg_dump that log their arguments and answer the probes.
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts" / "run_migrations_with_backup.sh"

FAKE_PSQL = r"""#!/bin/bash
printf '%s\n' "$*" >> "{log}"
case "$*" in
  *--version*) echo "psql (PostgreSQL) 14.15" ;;
  *"SELECT version();"*) echo "PostgreSQL 14.15" ;;
  *"SELECT current_database();"*) echo "koi_test_mwb" ;;
  *"public.koi_migrations"*) echo f ;;
  *"public.koi_schema_migrations"*) echo f ;;
  *"public.schema_migrations"*) echo t ;;
  *"SELECT version FROM schema_migrations;"*) echo "001_first" ;;
  *"SELECT COUNT(*) FROM schema_migrations WHERE version = '001_first'"*) echo " 1" ;;
  *"SELECT COUNT(*) FROM schema_migrations"*) echo " 0" ;;
  *"information_schema.tables"*) echo " 3" ;;
  *"CREATE EXTENSION"*) echo DO ;;
esac
exit 0
"""


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "scripts").mkdir()
    shutil.copy(SCRIPT, tmp_path / "scripts" / SCRIPT.name)
    mig = tmp_path / "migrations"
    mig.mkdir()
    (mig / "001_first.sql").write_text("SELECT 1;\n")
    (mig / "002_second.sql").write_text("SELECT 2;\n")
    (mig / "002_second_down.sql").write_text("SELECT 3;\n")
    backups = tmp_path / "backups"
    backups.mkdir()
    for i in range(6):
        (backups / f"backup_2026010{i}_000000.sql.gz").write_bytes(b"")
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    log = tmp_path / "calls.log"
    psql = fakebin / "psql"
    psql.write_text(FAKE_PSQL.replace("{log}", str(log)))
    pg_dump = fakebin / "pg_dump"
    pg_dump.write_text(f'#!/bin/bash\nprintf "pg_dump %s\\n" "$*" >> "{log}"\necho "-- dump"\n')
    for exe in (psql, pg_dump):
        exe.chmod(0o755)
    env = {"PATH": f"{fakebin}:/usr/bin:/bin", "HOME": str(tmp_path),
           "DATABASE_URL": "postgresql://koi:changeme@127.0.0.1:1/koi_test_mwb"}
    return tmp_path, env, log


def _run(tree, args):
    root, env, _ = tree
    return subprocess.run(["bash", str(root / "scripts" / SCRIPT.name), *args], env=env,
                          capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL)


def _backups(root):
    return sorted(p.name for p in (root / "backups").iterdir())


def test_help_touches_nothing(tree):
    root, _, log = tree
    r = _run(tree, ["--help"])
    assert r.returncode == 0 and "Usage" in r.stdout
    assert not log.exists()
    assert len(_backups(root)) == 6


def test_unknown_argument_is_refused(tree):
    root, _, log = tree
    r = _run(tree, ["--yes"])
    assert r.returncode == 2
    assert not log.exists()


@pytest.mark.parametrize("args", [[], ["--dry-run"], ["--dry-run", "--apply"]])
def test_dry_run_only_reads(tree, args):
    root, _, log = tree
    r = _run(tree, args)
    assert r.returncode == 0, r.stdout + r.stderr
    calls = log.read_text()
    assert "pg_dump" not in calls
    assert " -f " not in calls
    for word in ("INSERT", "CREATE", "DROP", "UPDATE", "DELETE"):
        assert word not in calls, word
    assert len(_backups(root)) == 6
    assert "Would apply 1 of 2 migration files" in r.stdout
    assert "002_second" in r.stdout
    assert "Would remove 2 older backup(s)" in r.stdout
    assert "changeme" not in r.stdout + r.stderr


def test_apply_backs_up_prunes_and_migrates(tree):
    root, _, log = tree
    r = _run(tree, ["--apply"])
    assert r.returncode == 0, r.stdout + r.stderr
    calls = log.read_text()
    assert "pg_dump " in calls
    assert f"-f {root}/scripts/../migrations/002_second.sql" in calls
    assert "001_first.sql" not in calls and "_down.sql" not in calls
    assert "INSERT INTO schema_migrations (version) VALUES ('002_second')" in calls
    names = _backups(root)
    assert len(names) == 5
    assert not any(n.startswith("backup_20260100") or n.startswith("backup_20260101") for n in names)
