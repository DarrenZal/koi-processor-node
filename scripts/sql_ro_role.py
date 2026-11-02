#!/usr/bin/env python3
"""Create, verify or remove the ``koi_sql_ro`` role that ``POST /sql`` executes as.

The role gets SELECT on exactly ``api.routers.query_router.ALLOWED_TABLES`` (read
from the code, so the grant cannot drift from the route's allowlist) and nothing
else. The setup also closes two PUBLIC defaults in the target database:
``CREATE`` on schema ``public`` and ``EXECUTE`` on the three ``query_to_xml``
variants. It then verifies the role's effective privileges and exits non-zero
if any check fails.

No password is set (``PASSWORD NULL``). The role can log in only where
``pg_hba.conf`` does not ask it for one (``trust`` for ``koi_sql_ro`` and the
address it connects from). Where pg_hba requires a password for it (``scram-sha-256`` or
``md5``, the default for host connections on many installs), the role cannot log
in, the service's read pool fails to connect, and ``/sql`` returns 503: it fails
closed. In that case set a password yourself (``ALTER ROLE koi_sql_ro PASSWORD
...``) after every ``--apply``, which resets it to NULL, and put it in
``KOI_SQL_RO_DSN``.

Revoking ``CREATE`` on schema ``public`` from PUBLIC also removes it from every
role that held it only through PUBLIC. On PostgreSQL 14 the schema belongs to
the superuser that created the cluster, so a database owner that is not a
superuser loses it, and the API, which runs ``CREATE TABLE IF NOT EXISTS`` at
startup, fails with "permission denied for schema public". Before ``--apply``,
make that role the schema's owner as a superuser (``ALTER SCHEMA public OWNER
TO <owner>``); the same command repairs a node that already fails. PostgreSQL
15 and later give the schema to the database owner already.

Modes (dry-run wins over every other flag):
  (default) / --dry-run   print the SQL that would run; touch nothing
  --apply                 create + grant + revoke in one transaction, then verify
  --verify                verify only (read-only)
  --backout               revoke the role's privileges and drop it. Does NOT
                          re-grant the PUBLIC defaults: that would restore the
                          vulnerable state (see the printed note).

Usage:
  scripts/sql_ro_role.py --admin-dsn postgresql://koi@localhost:5432/personal_koi [--apply|--verify|--backout] [--dry-run]

Backout order for the live service: first take /sql out of service
(KOI_READ_ROUTES_DISABLED=sql, or unset KOI_SQL_RO_DSN) and restart, so no
process holds a connection as the role; then run --backout.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg  # noqa: E402

from api.routers.query_router import ALLOWED_TABLES  # noqa: E402

ROLE = "koi_sql_ro"
QUERY_TO_XML = (
    "query_to_xml(text,boolean,boolean,text)",
    "query_to_xmlschema(text,boolean,boolean,text)",
    "query_to_xml_and_xmlschema(text,boolean,boolean,text)",
)
DENIED_FUNCS = QUERY_TO_XML + ("pg_read_file(text)", "pg_ls_dir(text)", "lo_import(text)")


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def setup_sql(dbname: str) -> list[str]:
    tables = sorted(ALLOWED_TABLES)
    attrs = ("LOGIN PASSWORD NULL NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE "
             "NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 10")
    return [
        # Idempotent: create when absent, then (re)assert every attribute, so a
        # re-run after ALLOWED_TABLES grows refreshes grants instead of failing.
        f"DO $do$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') "
        f"THEN CREATE ROLE {ROLE}; END IF; END$do$",
        f"ALTER ROLE {ROLE} {attrs}",
        f"GRANT CONNECT ON DATABASE {_ident(dbname)} TO {ROLE}",
        f"GRANT USAGE ON SCHEMA public TO {ROLE}",
        f"GRANT SELECT ON {', '.join(_ident(t) for t in tables)} TO {ROLE}",
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC",
        "REVOKE EXECUTE ON FUNCTION " + ", ".join(QUERY_TO_XML) + " FROM PUBLIC",
    ]


def backout_sql() -> list[str]:
    # DROP OWNED revokes every privilege granted to the role in this database
    # and on shared objects (incl. CONNECT); the role owns nothing.
    return [f"DROP OWNED BY {ROLE}", f"DROP ROLE {ROLE}"]


VULNERABLE_RESTORE_NOTE = (
    "Not run, by design: 'GRANT EXECUTE ON FUNCTION query_to_xml(...) TO PUBLIC' and "
    "'GRANT CREATE ON SCHEMA public TO PUBLIC' restore the pre-S4 ACLs, which is the "
    "vulnerable state. Do it only for a confirmed regression, as a recorded operator "
    "decision against incident 8574."
)


async def verify(conn, dbname: str) -> list[tuple[str, bool, str]]:
    """Return (check, ok, detail). Read-only."""
    out: list[tuple[str, bool, str]] = []
    r = await conn.fetchrow(
        "SELECT rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolreplication,"
        " rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname = $1", ROLE)
    if r is None:
        return [("role exists", False, "missing")]
    out.append(("role exists and can log in", bool(r["rolcanlogin"]), ""))
    for attr in ("rolsuper", "rolinherit", "rolcreaterole", "rolcreatedb",
                 "rolreplication", "rolbypassrls"):
        out.append((f"{attr} is false", not r[attr], str(r[attr])))
    member_of = await conn.fetchval(
        "SELECT count(*) FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member"
        " WHERE r.rolname = $1", ROLE)
    out.append(("member of no other role", member_of == 0, str(member_of)))
    present = {x["relname"] for x in await conn.fetch(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'public' AND c.relname = ANY($1::text[])", sorted(ALLOWED_TABLES))}
    for t in sorted(ALLOWED_TABLES):
        if t not in present:
            out.append((f"allowlisted {t} exists", False, "missing in database"))
            continue
        sel = await conn.fetchval("SELECT has_table_privilege($1, $2, 'SELECT')", ROLE, f"public.{t}")
        wr = await conn.fetchval(
            "SELECT has_table_privilege($1, $2, 'INSERT,UPDATE,DELETE,TRUNCATE')", ROLE, f"public.{t}")
        out.append((f"SELECT on {t}", bool(sel), ""))
        out.append((f"no write on {t}", not wr, ""))
    extra = await conn.fetch(
        "SELECT n.nspname || '.' || c.relname AS rel FROM pg_class c"
        " JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE c.relkind IN ('r','v','m','p','f')"
        "   AND n.nspname NOT IN ('pg_catalog','information_schema')"
        "   AND has_table_privilege($1, c.oid, 'SELECT')"
        "   AND NOT (n.nspname = 'public' AND c.relname = ANY($2::text[]))",
        ROLE, sorted(ALLOWED_TABLES))
    out.append(("SELECT on no non-allowlisted relation", not extra,
                ", ".join(x["rel"] for x in extra[:10])))
    create = await conn.fetchval("SELECT has_schema_privilege($1, 'public', 'CREATE')", ROLE)
    out.append(("no CREATE on schema public", not create, ""))
    for fn in DENIED_FUNCS:
        ok = await conn.fetchval(
            "SELECT to_regprocedure($2) IS NULL OR NOT has_function_privilege($1, $2, 'EXECUTE')",
            ROLE, fn)
        out.append((f"no EXECUTE on {fn}", bool(ok), ""))
    db_ok = await conn.fetchval("SELECT has_database_privilege($1, $2, 'CONNECT')", ROLE, dbname)
    out.append((f"CONNECT on {dbname}", bool(db_ok), ""))
    return out


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--admin-dsn", required=True, help="superuser DSN for the target database")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--verify", action="store_true")
    mode.add_argument("--backout", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="print only; wins over --apply/--backout")
    args = ap.parse_args()

    conn = await asyncpg.connect(args.admin_dsn)
    try:
        dbname = await conn.fetchval("SELECT current_database()")
        print(f"target database: {dbname}")
        if args.dry_run or not (args.apply or args.verify or args.backout):
            stmts = backout_sql() if args.backout else setup_sql(dbname)
            print("-- dry run: nothing executed")
            for s in stmts:
                print(s + ";")
            if args.backout:
                print("-- " + VULNERABLE_RESTORE_NOTE)
            return 0
        if args.backout:
            async with conn.transaction():
                for s in backout_sql():
                    await conn.execute(s)
            gone = await conn.fetchval("SELECT count(*) FROM pg_roles WHERE rolname = $1", ROLE)
            print(f"role dropped: {gone == 0}")
            print(VULNERABLE_RESTORE_NOTE)
            return 0 if gone == 0 else 1
        if args.apply:
            async with conn.transaction():
                for s in setup_sql(dbname):
                    await conn.execute(s)
            print("applied")
        results = await verify(conn, dbname)
        width = max(len(c) for c, _, _ in results)
        for check, ok, detail in results:
            print(f"{'PASS' if ok else 'FAIL'}  {check.ljust(width)}  {detail}")
        failed = [c for c, ok, _ in results if not ok]
        print(f"{len(results) - len(failed)}/{len(results)} checks pass")
        return 1 if failed else 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
