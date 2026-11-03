#!/usr/bin/env bash
# Offline rehearsal of a read-auth deployment (the koi_sql_ro role behind POST /sql
# and the read-scope checks).
#
# Starts a THROWAWAY PostgreSQL cluster (initdb in a temp dir, 127.0.0.1 only,
# trust auth), so the cluster-wide koi_sql_ro role and the DB-wide REVOKEs are
# exercised without touching a live cluster. Then:
#   1. the per-module suites and the combined in-process HTTP integration test,
#      with the AST layer inactive (a venv without sqlglot) and again with the
#      pinned sqlglot wheel on PYTHONPATH (a venv that has it);
#   2. the koi_sql_ro role script end to end on a database holding every
#      allowlisted table: dry-run, apply, verify, a privilege probe as the role,
#      backout, and a check that the role is gone and cannot log in.
# The cluster is stopped and deleted on exit, pass or fail.
#
# Usage: scripts/read_auth_rehearsal.sh [--sqlglot-site DIR]
#   --sqlglot-site  a directory holding sqlglot==30.20.0 (pip install --target).
#                   Without it, the sqlglot-active pass is reported NOT RUN.
# Env: PY (the venv interpreter to test; set it, the default is a maintainer path),
#      PORT (55439).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-/Users/example/venvs/koi-server/bin/python}"
PORT="${PORT:-55439}"
SQLGLOT_SITE=""
if [[ "${1:-}" == "--sqlglot-site" ]]; then SQLGLOT_SITE="$2"; fi

[[ "$PORT" != "5432" ]] || { echo "refusing: port 5432 is the live cluster" >&2; exit 2; }
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "refusing: something already listens on $PORT" >&2; exit 2
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/koi-readauth-rehearsal.XXXXXX")"
DATA="$WORK/data"
cleanup() {
  pg_ctl -D "$DATA" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

initdb -D "$DATA" -U "$(id -un)" --auth=trust --no-instructions >/dev/null
pg_ctl -D "$DATA" -l "$WORK/pg.log" -w \
  -o "-p $PORT -c unix_socket_directories='' -c listen_addresses=127.0.0.1" start >/dev/null
ADMIN="postgresql://$(id -un)@127.0.0.1:$PORT/postgres"
echo "throwaway cluster: $DATA on 127.0.0.1:$PORT ($(psql -XAt "$ADMIN" -c 'show server_version'))"
# The test guard accepts an admin DSN only on a database whose name contains "test".
psql -XAtq "$ADMIN" -c "CREATE DATABASE koi_test_admin"
TEST_ADMIN="postgresql://$(id -un)@127.0.0.1:$PORT/koi_test_admin"

cd "$REPO"
SUITES=(tests/test_auth_observability.py tests/test_read_scope.py
        tests/test_read_scope_resolution.py tests/test_sql_hardening.py
        tests/test_discourse_search.py tests/test_discourse_search_scope.py
        tests/test_read_auth_integration.py)

echo
echo "== 1a. suites, AST layer inactive (live venv as today)"
KOI_TEST_ADMIN_DSN="$TEST_ADMIN" KOI_TEST_THROWAWAY_CLUSTER=1 \
  "$PY" -B -m pytest "${SUITES[@]}" -q -p no:cacheprovider -rs

echo
if [[ -n "$SQLGLOT_SITE" ]]; then
  echo "== 1b. suites, sqlglot $(PYTHONPATH="$SQLGLOT_SITE" "$PY" -c 'import sqlglot;print(sqlglot.__version__)') active"
  PYTHONPATH="$SQLGLOT_SITE" KOI_TEST_ADMIN_DSN="$TEST_ADMIN" KOI_TEST_THROWAWAY_CLUSTER=1 \
    "$PY" -B -m pytest "${SUITES[@]}" -q -p no:cacheprovider -rs
else
  echo "== 1b. NOT RUN: no --sqlglot-site given"
fi

echo
echo "== 2. koi_sql_ro role script on a database with every allowlisted table"
psql -XAtq "$ADMIN" -c "CREATE DATABASE personal_koi_rehearsal"
RDB="postgresql://$(id -un)@127.0.0.1:$PORT/personal_koi_rehearsal"
"$PY" -B - "$RDB" <<'PYEOF'
import sys, asyncio, asyncpg
sys.path.insert(0, ".")
from api.routers.query_router import ALLOWED_TABLES
async def main(dsn):
    c = await asyncpg.connect(dsn)
    for t in sorted(ALLOWED_TABLES):
        if t.startswith("v_"):
            continue
        await c.execute(f'CREATE TABLE "{t}" (id int, note text)')
    await c.execute("INSERT INTO entity_registry VALUES (1, 'visible')")
    await c.execute("CREATE VIEW v_mediawiki_page_resolved AS SELECT id FROM mediawiki_page_state")
    await c.execute("CREATE TABLE session_tokens (token_hash text)")
    await c.execute("INSERT INTO session_tokens VALUES ('not-for-the-role')")
    await c.close()
asyncio.run(main(sys.argv[1]))
PYEOF

echo "-- dry run (no change expected)"
"$PY" -B scripts/sql_ro_role.py --admin-dsn "$RDB" --apply --dry-run
test "$(psql -XAt "$RDB" -c "select count(*) from pg_roles where rolname='koi_sql_ro'")" = 0 \
  && echo "dry run created nothing: ok"
echo "-- apply + verify"
"$PY" -B scripts/sql_ro_role.py --admin-dsn "$RDB" --apply
echo "-- verify only"
"$PY" -B scripts/sql_ro_role.py --admin-dsn "$RDB" --verify | tail -1
echo "-- apply again (idempotent re-run)"
"$PY" -B scripts/sql_ro_role.py --admin-dsn "$RDB" --apply | tail -1
echo "-- privilege probe as koi_sql_ro"
RO="postgresql://koi_sql_ro@127.0.0.1:$PORT/personal_koi_rehearsal"
probe() {  # label, sql, expect(ok|denied)
  if out=$(psql -XAtq "$RO" -v ON_ERROR_STOP=1 -c "$2" 2>&1); then got=ok; else got=denied; fi
  [[ "$got" == "$3" ]] && r=PASS || r=FAIL
  printf '%s  %-34s expected %-6s got %-6s %s\n' "$r" "$1" "$3" "$got" "$(echo "$out" | head -1 | cut -c1-70)"
  [[ "$r" == PASS ]]
}
fails=0
probe "SELECT allowlisted table" "SELECT note FROM entity_registry" ok || fails=$((fails+1))
probe "SELECT allowlisted view" "SELECT count(*) FROM v_mediawiki_page_resolved" ok || fails=$((fails+1))
probe "SELECT session_tokens" "SELECT * FROM session_tokens" denied || fails=$((fails+1))
probe "query_to_xml (inner session_tokens)" "SELECT query_to_xml('SELECT * FROM session_tokens',true,false,'')" denied || fails=$((fails+1))
probe "query_to_xml (inner SELECT 1)" "SELECT query_to_xml('SELECT 1',true,false,'')" denied || fails=$((fails+1))
probe "pg_read_file" "SELECT pg_read_file('postgresql.conf')" denied || fails=$((fails+1))
probe "COPY TO PROGRAM" "COPY (SELECT 1) TO PROGRAM 'true'" denied || fails=$((fails+1))
probe "INSERT allowlisted table" "INSERT INTO entity_registry VALUES (2,'x')" denied || fails=$((fails+1))
probe "CREATE TABLE in public" "CREATE TABLE ro_probe (x int)" denied || fails=$((fails+1))
probe "SET ROLE to superuser" "SET ROLE $(id -un)" denied || fails=$((fails+1))
echo "-- backout (dry run, then real)"
"$PY" -B scripts/sql_ro_role.py --admin-dsn "$RDB" --backout --dry-run
"$PY" -B scripts/sql_ro_role.py --admin-dsn "$RDB" --backout
probe "login after backout" "SELECT 1" denied || fails=$((fails+1))
echo "-- after backout, PUBLIC defaults stay closed (not restored)"
psql -XAt "$RDB" -c "select 'query_to_xml PUBLIC execute: ' || has_function_privilege('public','query_to_xml(text,boolean,boolean,text)','EXECUTE')"
echo
echo "role probe failures: $fails"
exit "$fails"
