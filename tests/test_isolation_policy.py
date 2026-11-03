"""Static isolation policy: tests take every target from tests/koi_test_targets.py.

Scans every .py and .sh file under tests/ (comments and docstrings excluded) and fails on

  env-read            a read of a runtime target variable to find a target: every variable
                      koi_test_targets redirects or removes (DATABASE_URL_VARS,
                      ADMIN_URL_VARS, API_URL_VARS, HOST_VARS, PORT_VARS, NAME_VARS,
                      PG_VARS, SERVICE_URL_VARS, REMOVED_VARS, LOGIN_VARS), read through
                      os.getenv, any .get("VAR"),
                      any ["VAR"] subscript that is not an assignment (os.environ["VAR"],
                      dict(os.environ)["VAR"], env["VAR"]), or os.environ.setdefault;
                      $VAR in shell. Setting one (monkeypatch.setenv, env[...] = ...) is
                      allowed.
  literal-5432        a local Postgres on the default port or socket: localhost, 127.0.0.1
                      or [::1] followed by :5432; any other host:5432; a postgresql:// URL
                      whose host is local and has no port, or that has no host; a /tmp
                      socket host (host=/tmp, %2Ftmp)
  literal-8351        :8351
  literal-personal_koi  personal_koi named as a database: a URL path, dbname=, -d, a
                      *_DB= assignment, createdb/dropdb, inside connect(...), or a mapping
                      entry ("dbname"/"database"/"db": "personal_koi")

outside the guard module and the ALLOWLIST below, where every row carries a reason. A
row that no longer matches anything fails too, so the list cannot outlive its cause.
test_detector_flags_every_rule is the positive control: a zero result above proves the
scan ran only because the same scanner flags a planted file.

The last two tests check the guard at run time: an inherited login (PGUSER, PGPASSWORD,
PGOPTIONS, PGSERVICE, PGSERVICEFILE, POSTGRES_USER, POSTGRES_PASSWORD) is gone before any
test runs, so a password reaches a test server only through its KOI_TEST_* URL.
"""
from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tokenize
from pathlib import Path

import pytest

from tests import koi_test_targets as _targets

TESTS = Path(__file__).resolve().parent
GUARD = {"tests/koi_test_targets.py"}

READ_VARS = tuple(dict.fromkeys(
    _targets.DATABASE_URL_VARS + _targets.ADMIN_URL_VARS + _targets.API_URL_VARS
    + _targets.HOST_VARS + _targets.PORT_VARS + _targets.NAME_VARS + _targets.PG_VARS
    + _targets.SERVICE_URL_VARS + _targets.REMOVED_VARS + _targets.LOGIN_VARS))
_VARS = "|".join(sorted(READ_VARS, key=len, reverse=True))
_Q = r"""['"]"""
PY_RULES = {
    "env-read": re.compile(
        rf"""(?:getenv|\.get|environ\.setdefault)\(\s*{_Q}({_VARS}){_Q}"""
        rf"""|\[\s*{_Q}({_VARS}){_Q}\s*\](?!\s*=[^=])"""),
}
SH_RULES = {
    "env-read": re.compile(rf"\$\{{?({_VARS})\b"),
}
_LOCAL = r"(?:localhost|127\.0\.0\.1|\[::1\])"
LITERAL_RULES = {
    "literal-5432": re.compile(
        rf"\b{_LOCAL}:5432\b|\[::1\]:5432\b|[\w\]]:5432\b"
        rf"|postgres(?:ql)?://(?:[^/@\s'\"]*@)?{_LOCAL}(?=[/?'\"\s]|$)"
        r"|postgres(?:ql)?:///"
        r"|\bhost=/tmp\b|%2[Ff]tmp\b"),
    "literal-8351": re.compile(r":8351\b"),
    "literal-personal_koi": re.compile(
        r"/personal_koi(?![\w.-])|dbname=personal_koi\b|-d\s+personal_koi\b"
        r"|['\"]-d['\"]\s*,\s*['\"]personal_koi['\"]|\b[A-Z_]*_DB=personal_koi\b"
        r"|\b(?:createdb|dropdb)\s+personal_koi\b|(?:connect|create_pool)\([^)\n]*\bpersonal_koi\b"
        r"|['\"](?:dbname|database|db)['\"]\s*:\s*['\"]personal_koi['\"]"),
}

# (path, rule) -> reason. Keep reasons specific enough to disagree with.
ALLOWLIST: dict[tuple[str, str], str] = {
    ("tests/test_isolation_redirect.py", "env-read"):
        "test_conftest_redirected_this_process reads POSTGRES_URL to check the redirect "
        "landed; it compares the value and never connects to it.",
    ("tests/test_isolation_redirect.py", "literal-8351"):
        "values the guard must refuse; they are passed to problems() or to a pytest run "
        "that must exit before collection, never to a client.",
    ("tests/test_isolation_redirect.py", "literal-personal_koi"):
        "a value the guard must refuse, in the same refusal tests.",
    ("tests/test_isolation_redirect.py", "literal-5432"):
        "test_redirect_overrides_inherited_live_values: inherited live-looking values that "
        "redirect_runtime_config must overwrite; they are never connected to.",
    ("tests/test_koi_test_targets.py", "literal-personal_koi"):
        "URLs the guard must refuse, passed only to problems(), database_name() and the "
        "drivers' offline DSN parsers; nothing connects.",
    ("tests/test_live_write_cleanup.py", "literal-personal_koi"):
        "test_cleanup_refuses_a_live_looking_database: validate_dsn must raise before connecting.",
    ("tests/router/router_suggestion_schema_checks.py", "literal-5432"):
        "fixture env-file text and DSNs that ensure_safe_dsn must refuse or report; not "
        "collected by pytest and never connected.",
    ("tests/router/router_suggestion_schema_checks.py", "literal-personal_koi"):
        "same fixtures: the DSN source-resolution check and the refusal case.",
    ("tests/test_isolation_policy.py", "env-read"):
        "PLANTED_PY, the positive-control fixture this check must flag; written to tmp_path only.",
    ("tests/test_isolation_policy.py", "literal-5432"):
        "PLANTED_PY positive-control fixture.",
    ("tests/test_isolation_policy.py", "literal-8351"):
        "PLANTED_PY positive-control fixture, and this check's own pattern text.",
    ("tests/test_isolation_policy.py", "literal-personal_koi"):
        "PLANTED_PY and PLANTED_SH positive-control fixtures.",
    ("tests/test_koi_backup_e2e.sh", "literal-personal_koi"):
        "E3 asserts the backup script refuses selftest mode for this database name before "
        "connecting; libpq is pointed at the KOI_TEST_ADMIN_DSN cluster in any case.",
}


def _python_code(text: str) -> str:
    """The source with comments and docstrings blanked, line numbers preserved."""
    lines = text.splitlines(keepends=True)
    blank: list[tuple[tuple[int, int], tuple[int, int]]] = []
    prev = tokenize.NEWLINE
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(text).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return text
    for i, tok in enumerate(toks):
        if tok.type == tokenize.COMMENT:
            blank.append((tok.start, tok.end))
        elif tok.type == tokenize.STRING and prev in (tokenize.NEWLINE, tokenize.NL, tokenize.INDENT,
                                                       tokenize.DEDENT, tokenize.ENCODING):
            nxt = next((t for t in toks[i + 1:] if t.type not in (tokenize.COMMENT,)), None)
            if nxt is not None and nxt.type in (tokenize.NEWLINE, tokenize.ENDMARKER):
                blank.append((tok.start, tok.end))      # a bare string statement: a docstring
        if tok.type not in (tokenize.COMMENT, tokenize.NL):
            prev = tok.type
    out = [list(line) for line in lines]
    for (sr, sc), (er, ec) in blank:
        for r in range(sr, er + 1):
            row = out[r - 1]
            a = sc if r == sr else 0
            b = ec if r == er else len(row)
            for c in range(a, min(b, len(row))):
                if row[c] != "\n":
                    row[c] = " "
    return "".join("".join(r) for r in out)


def _shell_code(text: str) -> str:
    out = []
    for line in text.splitlines():
        if line.lstrip().startswith("#"):
            out.append("")
        else:
            out.append(re.sub(r"(^|\s)#.*$", r"\1", line))
    return "\n".join(out)


def scan_text(path: str, text: str) -> list[tuple[str, int, str]]:
    """[(rule, line number, line)] for every policy hit in one file's text."""
    if path.endswith(".py"):
        code, rules = _python_code(text), {**PY_RULES, **LITERAL_RULES}
    else:
        code, rules = _shell_code(text), {**SH_RULES, **LITERAL_RULES}
    hits = []
    for n, line in enumerate(code.splitlines(), 1):
        for rule, rx in rules.items():
            if rx.search(line):
                hits.append((rule, n, line.strip()))
    return hits


def scan(root: Path = TESTS) -> dict[str, list[tuple[str, int, str]]]:
    found = {}
    for p in sorted(root.rglob("*")):
        if p.suffix not in (".py", ".sh") or "__pycache__" in p.parts or not p.is_file():
            continue
        rel = f"tests/{p.relative_to(root).as_posix()}"
        if rel in GUARD:
            continue
        hits = scan_text(rel, p.read_text(errors="ignore"))
        if hits:
            found[rel] = hits
    return found


def test_no_test_finds_a_target_outside_the_guard():
    offenders = [f"  {path}:{n}: {rule}: {line}"
                 for path, hits in scan().items() for rule, n, line in hits
                 if (path, rule) not in ALLOWLIST]
    assert not offenders, (
        "\n*** TEST TARGET OUTSIDE tests/koi_test_targets.py ***\n" + "\n".join(offenders) +
        "\n\nTake targets from koi_test_targets (database_url(), admin_dsn(), api_url()), set "
        "runtime variables with monkeypatch from those values, or add an ALLOWLIST row with "
        "a reason.")


def test_no_stale_allowlist_rows():
    found = {(path, rule) for path, hits in scan().items() for rule, _, _ in hits}
    # A row for a file this checkout does not carry (a host-specific suite left out of a
    # public export) cannot be checked here; it stays for the checkouts that have it.
    stale = sorted(k for k in ALLOWLIST if k not in found and (TESTS.parent / k[0]).exists())
    assert not stale, f"ALLOWLIST rows that no longer match anything: {stale}"


PLANTED_PY = '''"""docstring naming POSTGRES_URL and localhost:5432 is ignored"""
import os
# a comment naming os.getenv("DATABASE_URL") is ignored
A = os.getenv("POSTGRES_URL", "x")
B = os.environ["KOI_API_URL"]
C = os.environ.get('PGHOST')
D = "postgresql://u@localhost:5432/koi_test"
E = "http://127.0.0.1:8351"
F = "postgresql://u@127.0.0.1:55999/personal_koi"
os.environ["DATABASE_URL"] = "set, not read"
G = "postgresql://dev@localhost/koi_test"
H = "postgresql://dev@[::1]:5432/koi_test"
I = os.getenv("KOI_SQL_RO_DSN")
J = os.environ.get("KOI_DISCOURSE_DSN")
K = dict(os.environ)["DATABASE_URL"]
L = {"host": "localhost", "port": 5432, "dbname": "personal_koi"}
M = "postgresql:///koi_test?host=/tmp"
'''
PLANTED_SH = '''# comment: ${BASE_URL} and :8351 are ignored here
curl "$BASE_URL/health"
psql -d personal_koi -c 'select 1'
'''


@pytest.mark.parametrize("name, text, expected", [
    ("tests/planted.py", PLANTED_PY,
     {("env-read", 4), ("env-read", 5), ("env-read", 6), ("literal-5432", 7),
      ("literal-8351", 8), ("literal-personal_koi", 9), ("literal-5432", 11),
      ("literal-5432", 12), ("env-read", 13), ("env-read", 14), ("env-read", 15),
      ("literal-personal_koi", 16), ("literal-5432", 17)}),
    ("tests/planted.sh", PLANTED_SH, {("env-read", 2), ("literal-personal_koi", 3)}),
], ids=["python", "shell"])
def test_detector_flags_every_rule(tmp_path, name, text, expected):
    """Positive control, through the same scan() the real check uses."""
    target = tmp_path / Path(name).name
    target.write_text(text)
    hits = {(rule, n) for rule, n, _ in scan(tmp_path)[f"tests/{target.name}"]}
    assert hits == expected
    assert {rule for rule, _ in hits} == set(PY_RULES) | set(LITERAL_RULES) or name.endswith(".sh")


def test_the_guard_module_is_the_only_exemption():
    assert GUARD == {"tests/koi_test_targets.py"}
    assert (TESTS / "koi_test_targets.py").is_file()


# Logins an inherited shell can carry. libpq and asyncpg use them for any connection whose
# URL leaves the user, password or options out.
INHERITED_LOGIN = {"PGUSER": "inherited_user", "PGPASSWORD": "changeme",
                   "PGOPTIONS": "-c search_path=inherited", "PGSERVICE": "inherited",
                   "PGSERVICEFILE": "/nonexistent/pg_service.conf",
                   "POSTGRES_USER": "inherited_user", "POSTGRES_PASSWORD": "changeme"}


@pytest.mark.parametrize("db", ["", "postgresql://koi@127.0.0.1:55999/koi_test_example"],
                         ids=["no-target", "target"])
def test_redirect_removes_inherited_logins(db):
    env = dict(INHERITED_LOGIN)
    if db:
        env[_targets.DATABASE_VAR] = db
    _targets.redirect_runtime_config(env)
    assert not set(INHERITED_LOGIN) & set(env), sorted(set(INHERITED_LOGIN) & set(env))
    assert set(_targets.LOGIN_VARS) | {"PGSERVICE", "PGSERVICEFILE"} == set(INHERITED_LOGIN)


def test_conftest_removes_inherited_logins_before_tests_run():
    """A process that loads tests/conftest.py with an inherited login sees none of it, and
    its PGPASSFILE is the run's empty file."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("KOI_TEST_")}
    env.update(INHERITED_LOGIN)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    code = ("import json, os, sys, tests.conftest as c\n"
            "names = json.loads(sys.argv[1])\n"
            "print(json.dumps({'left': [v for v in names if v in os.environ],\n"
            "                  'pgpassfile': os.environ['PGPASSFILE'],\n"
            "                  'pgpass': open(os.environ['PGPASSFILE']).read(),\n"
            "                  'root': c.STATE_ROOT}))\n")
    r = subprocess.run([sys.executable, "-c", code, json.dumps(sorted(INHERITED_LOGIN))],
                       cwd=TESTS.parent, env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    assert out["left"] == []
    assert out["pgpassfile"].startswith(out["root"]) and out["pgpass"] == ""
