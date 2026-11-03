"""Where tests may connect. The only module in tests/ that reads test targets.

Tests take every database and API target from three variables, and from nowhere else:

    KOI_TEST_DATABASE_URL  a database the tests may write to
    KOI_TEST_ADMIN_DSN     a role that may CREATE DATABASE, for tests that build and drop
                           scratch databases named koi_test_<unique>
    KOI_TEST_API_URL       a running KOI API the tests may call

A test that needs a second node (a peer) reads KOI_TEST_PEER_API_URL and
KOI_TEST_PEER_DATABASE_URL, held to the same rules.

None has a default. A test that needs an unset variable skips and names it. Nothing
connects at import or collection time.

The guard refuses, for the whole run, a database URL or admin DSN whose database is
personal_koi, whose name does not have "test" as a word (test, test_x, x_test, x_test_y),
or whose name drivers could read differently (a dbname or database key in the query
string, or a key=value DSN instead of a URL), and an API URL on port 8351. Set
KOI_TEST_ALLOW_LIVE=1 to lift that refusal on purpose. A value with a control character
(NUL, a line break, a tab, DEL), written raw or percent-encoded, is refused even then:
asyncpg passes a decoded NUL into its startup packet, where it ends the database name
early. tests/conftest.py runs check() before collection, and every accessor below runs it
again before handing out a value.

Code under test reads runtime variables (POSTGRES_URL, KOI_API_URL and others) at import.
redirect_runtime_config() points them at the KOI_TEST_* value, or at an address that
cannot resolve when that value is unset, so an inherited live value never reaches it.
redirect_state_dirs() points HOME and every state, key, vault and log location the code
reads at a temporary directory for the run, so no test reads or creates node keys or
other state in the live default locations.

Tests that call a live LLM API run only with KOI_TEST_LLM=1 (requires_live_llm()); a
provider key alone is not enough.
"""

from __future__ import annotations

import os
import re
import sys
import uuid
from typing import Iterable, Mapping, MutableMapping, Optional
from urllib.parse import parse_qsl, unquote, urlsplit, urlunsplit

DATABASE_VAR = "KOI_TEST_DATABASE_URL"
ADMIN_VAR = "KOI_TEST_ADMIN_DSN"
API_VAR = "KOI_TEST_API_URL"
PEER_API_VAR = "KOI_TEST_PEER_API_URL"
PEER_DATABASE_VAR = "KOI_TEST_PEER_DATABASE_URL"
ALLOW_LIVE_VAR = "KOI_TEST_ALLOW_LIVE"
LLM_OPT_IN_VAR = "KOI_TEST_LLM"

# HOME as the test process inherited it, captured before redirect_state_dirs() replaces
# it. Only for read-only checks of installed deployment files (launchd plists, a sibling
# checkout's documentation). Never use it to reach state, keys, a vault or a log.
HOST_HOME = os.environ.get("HOME", "")

LIVE_DATABASE = "personal_koi"
LIVE_API_PORT = 8351
SCRATCH_PREFIX = "koi_test_"

# What runtime variables hold when no KOI_TEST_* value is set. ".invalid" never
# resolves (RFC 2606), and the PGHOST directory does not exist, so libpq cannot fall
# back to the default socket.
UNSET_HOST = "koi-test-target-unset.invalid"
UNSET_DATABASE_URL = f"postgresql://{UNSET_HOST}/koi_test_unset"
UNSET_API_URL = f"http://{UNSET_HOST}"
UNSET_PGHOST = "/nonexistent/koi-test-target-unset"

# Runtime variables that name a database, an admin connection or an API.
DATABASE_URL_VARS = ("POSTGRES_URL", "DATABASE_URL", "POSTGRES_TEST_URL", "KOI_SQL_RO_DSN",
                     "DRIFT_SWEEP_PG_URL", "KOI_DISCOURSE_DSN", "DISCOURSE_DISCOVERY_DSN",
                     "ROUTER_PG_DSN", "PERSONAL_KOI_DSN")
ADMIN_URL_VARS = ("KOI_SCRATCH_ADMIN_DSN",)
API_URL_VARS = ("KOI_API_URL", "BASE_URL", "KOI_BASE_URL", "PERSONAL_KOI_API_URL",
                "KOI_TASK_API_URL", "KOI_TASK_API", "DOC_INGEST_KOI_URL", "KOI_API_ENDPOINT",
                "KOI_API_BASE")
HOST_VARS = ("POSTGRES_HOST", "DB_HOST", "KOI_DB_HOST")
PORT_VARS = ("POSTGRES_PORT", "DB_PORT", "KOI_DB_PORT")
NAME_VARS = ("POSTGRES_DB", "DB_NAME", "PERSONAL_KOI_DB", "KOI_DB_NAME")
# Other local services the code calls (graph stores, embedding and model servers, the
# bridges). No KOI_TEST_* variable names one, so they always point at UNSET_API_URL.
SERVICE_URL_VARS = ("TERMINUSDB_URL", "OLLAMA_BASE_URL", "OLLAMA_URL", "BGE_SERVER_URL",
                    "BGE_API_URL", "EMBEDDING_REMOTE_URL", "HYBRID_RAG_API_URL", "FUSEKI_URL",
                    "JENA_ENDPOINT", "CODE_GRAPH_URL", "CODE_GRAPH_SERVICE_URL",
                    "SEMANTIC_BRIDGE_URL", "EVENT_BRIDGE_URL", "COORDINATOR_URL")
# State, key, vault and log locations (redirect_state_dirs). Each defaults to a path under
# the live home (for example ~/.config/personal-koi/koi-state for node keys).
STATE_DIR_VARS = ("KOI_STATE_DIR", "RSS_SENSOR_STATE_DIR", "KOI_VAULT_CONFLICT_BACKUP_ROOT",
                  "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME")
VAULT_PATH_VARS = ("OBSIDIAN_VAULT_PATH", "VAULT_PATH", "KOI_VAULT_PATH")
STATE_FILE_VARS = ("EMBED_REPAIR_STATE_FILE", "KOI_AUTH_LOG_PATH")
# Credentials and access configuration that an inherited environment can carry. libpq and
# asyncpg read PGPASSFILE on every connection; the read routes read the scopes file and
# the auth mode on every request. Each points at an empty per-run file or the default.
CREDENTIAL_FILE_VARS = ("PGPASSFILE", "KOI_READ_SCOPES_FILE")
DEFAULT_AUTH_MODE = "log"
# libpq's own variables. redirect_runtime_config() sets PGHOST, PGPORT and PGDATABASE and
# removes PGUSER (LOGIN_VARS) and the rest (REMOVED_VARS).
PG_VARS = ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER")
# Removed outright: live-target variables earlier versions of this suite published.
REMOVED_VARS = ("KOI_LIVE_POSTGRES_URL", "KOI_PEER_POSTGRES_URL", "PEER_URL", "PGHOSTADDR",
                "PGSERVICE", "PGSERVICEFILE", "KOI_SKIP_LIVE_TRIPWIRE")
# Removed outright: a login an inherited shell can carry. libpq and asyncpg apply PGUSER,
# PGPASSWORD and PGOPTIONS to every connection whose URL leaves them out, and code under
# test builds DSNs from POSTGRES_USER and POSTGRES_PASSWORD, so a live password in the
# shell would be sent to whatever test server is named. A test database that needs a user
# or a password must carry it in its KOI_TEST_* URL. (PGSERVICE and PGSERVICEFILE, which
# can carry the same, are in REMOVED_VARS; PGPASSFILE is emptied by redirect_state_dirs.)
LOGIN_VARS = ("PGUSER", "PGOPTIONS", "POSTGRES_USER", "PGPASSWORD", "POSTGRES_PASSWORD")


# --------------------------------------------------------------------------------------
# Checking values
# --------------------------------------------------------------------------------------

def allow_live(env: Mapping[str, str] = os.environ) -> bool:
    return env.get(ALLOW_LIVE_VAR) == "1"


# Query keys that name a database. libpq lets a query dbname override the path and keeps
# the last of duplicate keys; asyncpg keeps the path, and ignores the query whenever the
# URL has a "/" path, even an empty one. A URL that uses either key can therefore reach
# different databases through different drivers, so the guard refuses it outright.
_QUERY_DATABASE_KEYS = ("dbname", "database")
# "test" as a word of the name: test, test_x, x_test, x_test_y (not "latest").
_TEST_WORD = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


def database_name(dsn: str) -> Optional[str]:
    """The database a postgresql:// URL names in its path, or None.

    None also when the query string names a database (see _QUERY_DATABASE_KEYS): the
    name is then ambiguous, and database_problem() refuses the URL.
    """
    parts = urlsplit(dsn)
    if parts.scheme not in ("postgres", "postgresql"):
        return None
    if any(k in _QUERY_DATABASE_KEYS for k, _ in parse_qsl(parts.query, keep_blank_values=True)):
        return None
    name = unquote(parts.path[1:] if parts.path.startswith("/") else parts.path)
    return name or None


def _control_character(value: str) -> bool:
    """A control character (C0, including NUL, or DEL) in the value, raw or percent-encoded."""
    return any(ord(c) < 32 or ord(c) == 127 for c in value + unquote(value))


def control_problem(var: str, value: str) -> Optional[str]:
    if _control_character(value):
        return (f"{var} contains a control character (raw or percent-encoded, e.g. %00); "
                "drivers can read such a value as a different target")
    return None


def database_problem(var: str, value: str, env: Mapping[str, str] = os.environ) -> Optional[str]:
    problem = control_problem(var, value)  # refused even with KOI_TEST_ALLOW_LIVE=1
    if problem:
        return problem
    if allow_live(env):
        return None
    parts = urlsplit(value)
    if parts.scheme not in ("postgres", "postgresql"):
        return f"{var} must be a postgresql:// URL that names its database in the path"
    keys = [k for k, _ in parse_qsl(parts.query, keep_blank_values=True) if k in _QUERY_DATABASE_KEYS]
    if keys:
        return (f"{var} names a database in its query string ({', '.join(keys)}); drivers "
                "disagree on which name wins, so put the name only in the path")
    name = database_name(value)
    if name is None:
        return f"{var} must be a postgresql:// URL that names its database in the path"
    if name == LIVE_DATABASE:
        return f"{var} names the live database {LIVE_DATABASE!r}"
    if not _TEST_WORD.search(name):
        return f"{var} names database {name!r}, which does not have 'test' as a word of its name"
    return None


def api_problem(var: str, value: str, env: Mapping[str, str] = os.environ) -> Optional[str]:
    problem = control_problem(var, value)  # refused even with KOI_TEST_ALLOW_LIVE=1
    if problem:
        return problem
    if allow_live(env):
        return None
    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return f"{var} must be an http:// or https:// URL with a host"
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        return f"{var} has an invalid port"
    if port == LIVE_API_PORT:
        return f"{var} is on port {LIVE_API_PORT}, the live KOI API's"
    return None


_CHECKS = ((DATABASE_VAR, database_problem), (ADMIN_VAR, database_problem), (API_VAR, api_problem),
           (PEER_DATABASE_VAR, database_problem), (PEER_API_VAR, api_problem))


def problems(env: Mapping[str, str] = os.environ) -> list[str]:
    """Every refusal for the KOI_TEST_* values in env. Empty means all set values are safe."""
    out = []
    for var, check in _CHECKS:
        value = env.get(var, "").strip()
        if value:
            problem = check(var, value, env)
            if problem:
                out.append(problem)
    return out


def refusal_message(found: Iterable[str]) -> str:
    lines = "\n".join(f"  - {p}" for p in found)
    return (
        "koi test guard: refusing to run.\n"
        f"{lines}\n"
        f"Point {DATABASE_VAR} and {ADMIN_VAR} at databases named like koi_test, and "
        f"{API_VAR} at a throwaway API. Set {ALLOW_LIVE_VAR}=1 only to test a live system on "
        "purpose."
    )


def check(env: Mapping[str, str] = os.environ) -> None:
    """End the pytest run before any test if a KOI_TEST_* value looks live."""
    found = problems(env)
    if found:
        import pytest
        pytest.exit(refusal_message(found), returncode=4)


def check_for_script(env: Mapping[str, str] = os.environ) -> None:
    """check() for code that runs outside pytest (manual scripts, shell guards)."""
    found = problems(env)
    if found:
        print(refusal_message(found), file=sys.stderr)
        raise SystemExit(4)


# --------------------------------------------------------------------------------------
# Handing out targets
# --------------------------------------------------------------------------------------

def _get(var: str) -> str:
    import pytest
    value = os.environ.get(var, "").strip()
    if not value:
        pytest.skip(f"{var} not set")
    check()
    return value


def configured(var: str) -> Optional[str]:
    """The checked value of a KOI_TEST_* variable, or None when unset. Never skips."""
    value = os.environ.get(var, "").strip()
    if not value:
        return None
    check()
    return value


def database_url() -> str:
    """KOI_TEST_DATABASE_URL, or skip the calling test (or fixture) naming it."""
    return _get(DATABASE_VAR)


def admin_dsn() -> str:
    """KOI_TEST_ADMIN_DSN, or skip naming it."""
    return _get(ADMIN_VAR)


def api_url() -> str:
    """KOI_TEST_API_URL without a trailing slash, or skip naming it."""
    return _get(API_VAR).rstrip("/")


def peer_api_url() -> str:
    """KOI_TEST_PEER_API_URL without a trailing slash, or skip naming it."""
    return _get(PEER_API_VAR).rstrip("/")


def peer_database_url() -> str:
    """KOI_TEST_PEER_DATABASE_URL, or skip naming it."""
    return _get(PEER_DATABASE_VAR)


def require_for_script(*names: str) -> dict[str, str]:
    """For manual scripts: the named KOI_TEST_* values, or exit 2 naming what is missing."""
    missing = [n for n in names if not os.environ.get(n, "").strip()]
    if missing:
        print(f"set {', '.join(missing)} (see tests/koi_test_targets.py)", file=sys.stderr)
        raise SystemExit(2)
    check_for_script()
    return {n: os.environ[n].strip() for n in names}


def _requires(var: str):
    import pytest
    return pytest.mark.skipif(not os.environ.get(var, "").strip(), reason=f"{var} not set")


def __getattr__(name: str):
    # Markers are built on first use so this module imports without pytest.
    markers = {"requires_database": DATABASE_VAR, "requires_admin": ADMIN_VAR,
               "requires_api": API_VAR}
    if name in markers:
        return _requires(markers[name])
    raise AttributeError(name)


# --------------------------------------------------------------------------------------
# URL helpers and scratch databases
# --------------------------------------------------------------------------------------

def url_parts(dsn: str) -> dict:
    parts = urlsplit(dsn)
    return {"host": parts.hostname, "port": parts.port, "user": parts.username,
            "password": parts.password, "database": database_name(dsn)}


def with_database(dsn: str, name: str) -> str:
    """The same server and role as dsn, connected to database `name`."""
    parts = urlsplit(dsn)
    query = "&".join(q for q in parts.query.split("&") if q and not q.startswith("dbname="))
    return urlunsplit((parts.scheme, parts.netloc, "/" + name, query, ""))


def with_user(dsn: str, user: str, password: Optional[str] = None) -> str:
    """The same server and database as dsn, as role `user`."""
    parts = urlsplit(dsn)
    host = parts.hostname or ""
    if ":" in host:
        host = f"[{host}]"
    netloc = user + (f":{password}" if password is not None else "") + "@" + host
    if parts.port:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, ""))


_CREATED_SCRATCH: set[str] = set()
_SCRATCH_TAG = re.compile(r"[^a-z0-9_]")


def scratch_name(tag: str = "") -> str:
    """A fresh koi_test_<tag>_<random> name, recorded so only this process may drop it."""
    tag = _SCRATCH_TAG.sub("_", tag.lower()).strip("_")
    name = f"{SCRATCH_PREFIX}{tag + '_' if tag else ''}{uuid.uuid4().hex[:12]}"
    _CREATED_SCRATCH.add(name)
    return name


def droppable(name: str) -> str:
    """Return name if this process created it with scratch_name(); refuse anything else."""
    if not name.startswith(SCRATCH_PREFIX) or name not in _CREATED_SCRATCH:
        raise RuntimeError(f"refusing to drop {name!r}: not a scratch database this run created")
    return name


# --------------------------------------------------------------------------------------
# Runtime configuration seen by code under test
# --------------------------------------------------------------------------------------

def _accepted(env: Mapping[str, str], var: str, check) -> str:
    """The KOI_TEST_* value if the guard accepts it, else "" (check() then ends the run)."""
    value = env.get(var, "").strip()
    return value if value and check(var, value, env) is None else ""


def redirect_runtime_config(env: MutableMapping[str, str] = os.environ) -> None:
    """Point runtime target variables at the KOI_TEST_* values or at UNSET_* placeholders.

    A refused KOI_TEST_* value is never copied: its variables get the UNSET_* placeholders,
    and check() ends the run before collection with the refusal. Inherited logins
    (LOGIN_VARS) are removed, so credentials come only from the KOI_TEST_* URL.
    """
    for var in REMOVED_VARS + LOGIN_VARS:
        env.pop(var, None)

    db = _accepted(env, DATABASE_VAR, database_problem)
    admin = _accepted(env, ADMIN_VAR, database_problem)
    api = _accepted(env, API_VAR, api_problem).rstrip("/")

    for var in DATABASE_URL_VARS:
        env[var] = db or UNSET_DATABASE_URL
    for var in ADMIN_URL_VARS:
        env[var] = admin or UNSET_DATABASE_URL
    for var in API_URL_VARS:
        env[var] = api or UNSET_API_URL
    for var in SERVICE_URL_VARS:
        env[var] = UNSET_API_URL

    parts = url_parts(db) if db else {}
    host = parts.get("host") or UNSET_HOST
    for var in HOST_VARS:
        env[var] = host
    for var in NAME_VARS:
        env[var] = parts.get("database") or "koi_test_unset"
    for var in PORT_VARS + ("PGPORT",):
        if parts.get("port"):
            env[var] = str(parts["port"])
        else:
            env.pop(var, None)
    env["PGHOST"] = parts.get("host") or UNSET_PGHOST
    env["PGDATABASE"] = parts.get("database") or "koi_test_unset"


def redirect_state_dirs(root: str, env: MutableMapping[str, str] = os.environ) -> None:
    """Point HOME and every state, key, vault and log location at directories under root.

    HOME covers the paths the code builds from "~" with no variable of its own (the
    document-ingest status directory, ~/.koi/logs, the regen CLI's ~/.regen). The
    variables cover the rest: node keys (KOI_STATE_DIR), the vault, repair state and the
    auth log. PGPASSFILE becomes an empty file under root, so no test reads a live
    password file (a test database that needs a password must carry it in its
    KOI_TEST_* URL); KOI_READ_SCOPES_FILE becomes an empty JSON object under root, and
    KOI_AUTH_MODE the default, so the live access configuration does not change test
    outcomes. Run it before code under test is imported; it is idempotent for one root.
    """
    home = os.path.join(root, "home")
    os.makedirs(home, exist_ok=True)
    env["HOME"] = home
    for var in STATE_DIR_VARS:
        path = os.path.join(root, var.lower())
        os.makedirs(path, exist_ok=True)
        env[var] = path
    vault = os.path.join(root, "vault")
    os.makedirs(vault, exist_ok=True)
    for var in VAULT_PATH_VARS:
        env[var] = vault
    env["EMBED_REPAIR_STATE_FILE"] = os.path.join(root, "embedding-repair-state.json")
    env["KOI_AUTH_LOG_PATH"] = os.path.join(root, "auth-observations.jsonl")
    for var, content in (("PGPASSFILE", ""), ("KOI_READ_SCOPES_FILE", "{}\n")):
        path = os.path.join(root, var.lower())
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        os.chmod(path, 0o600)  # libpq ignores a password file that others can read
        env[var] = path
    env["KOI_AUTH_MODE"] = DEFAULT_AUTH_MODE


# --------------------------------------------------------------------------------------
# Live LLM tests
# --------------------------------------------------------------------------------------

def live_llm_skip_reason(*key_vars: str, env: Mapping[str, str] = os.environ) -> Optional[str]:
    """Why a test that calls a live LLM API must skip, or None when it may run.

    It runs only with KOI_TEST_LLM=1 and every named key set. A key in the environment
    alone (for example from `source config/personal.env`) never sends a paid request.
    """
    if env.get(LLM_OPT_IN_VAR) != "1":
        return f"{LLM_OPT_IN_VAR}=1 not set: this test calls a live LLM API"
    missing = [k for k in key_vars if not env.get(k, "").strip()]
    if missing:
        return f"{', '.join(missing)} not set"
    return None


def requires_live_llm(*key_vars: str):
    """skipif marker for a test that calls a live LLM API (see live_llm_skip_reason)."""
    import pytest
    reason = live_llm_skip_reason(*key_vars)
    return pytest.mark.skipif(reason is not None, reason=reason or "")


# --------------------------------------------------------------------------------------
# Tokenizer downloads
# --------------------------------------------------------------------------------------

ENCODING_DOWNLOAD_VAR = "KOI_TEST_TIKTOKEN_DOWNLOAD"


class _DownloadBlocked(Exception):
    pass


def tiktoken_skip_reason(encodings: Iterable[str] = (), models: Iterable[str] = (),
                         env: Mapping[str, str] = os.environ) -> Optional[str]:
    """Why a test that needs tiktoken encodings must skip, or None when it may run.

    tiktoken downloads an encoding from openaipublic.blob.core.windows.net the first time
    it is used, then caches it (TIKTOKEN_CACHE_DIR, else DATA_GYM_CACHE_DIR, else
    <tempdir>/data-gym-cache). The suite downloads nothing unless KOI_TEST_TIKTOKEN_DOWNLOAD=1:
    each encoding is loaded here with downloads blocked, so the test runs only when it is
    already cached. A model name resolves as the code under test resolves it
    (encoding_for_model, cl100k_base for an unknown model). Without tiktoken installed
    there is nothing to download, so the test runs.
    """
    if env.get(ENCODING_DOWNLOAD_VAR) == "1":
        return None
    try:
        import tiktoken
        import tiktoken.load as tiktoken_load
    except ImportError:
        return None
    names = list(encodings)
    for model in models:
        try:
            names.append(tiktoken.encoding_name_for_model(model))
        except KeyError:
            names.append("cl100k_base")
    real_read_file = tiktoken_load.read_file

    def read_file(blobpath: str) -> bytes:
        if blobpath.startswith(("http://", "https://")):
            raise _DownloadBlocked(blobpath)
        return real_read_file(blobpath)

    missing = []
    tiktoken_load.read_file = read_file
    try:
        for name in dict.fromkeys(names):
            try:
                tiktoken.get_encoding(name)
            except _DownloadBlocked:
                missing.append(name)
    finally:
        tiktoken_load.read_file = real_read_file
    if not missing:
        return None
    return (f"tiktoken encoding {', '.join(missing)} is not cached, and loading it would "
            f"download it from the internet. Set {ENCODING_DOWNLOAD_VAR}=1 to allow the "
            "download, or TIKTOKEN_CACHE_DIR to a directory that holds it")


def requires_tiktoken(encodings: Iterable[str] = (), models: Iterable[str] = ()):
    """skipif marker for a test that loads tiktoken encodings (see tiktoken_skip_reason)."""
    import pytest
    reason = tiktoken_skip_reason(encodings, models)
    return pytest.mark.skipif(reason is not None, reason=reason or "")


# --------------------------------------------------------------------------------------
# Shell use
# --------------------------------------------------------------------------------------

def _libpq_env(var: str) -> str:
    """`export PGHOST=... PGPORT=... PGUSER=...` for the cluster a checked KOI_TEST_* DSN names.

    For shell suites whose libpq tools (createdb, psql, pg_dump) take no DSN: eval the
    output so they reach that cluster instead of the default socket.
    """
    import shlex
    dsn = require_for_script(var)[var]
    parts = url_parts(dsn)
    if not parts["host"]:
        print(f"{var} must name a host", file=sys.stderr)
        raise SystemExit(2)
    env = {"PGHOST": parts["host"], "PGPORT": str(parts["port"] or 5432),
           "PGUSER": parts["user"] or ""}
    if parts["password"]:
        env["PGPASSWORD"] = parts["password"]
    lines = [f"export {k}={shlex.quote(v)}" for k, v in env.items() if v]
    return "\n".join(lines + ["unset PGDATABASE PGSERVICE PGHOSTADDR"])


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "libpq-env" and sys.argv[2] in (DATABASE_VAR, ADMIN_VAR):
        print(_libpq_env(sys.argv[2]))
    else:
        print(f"usage: {sys.argv[0]} libpq-env {ADMIN_VAR}|{DATABASE_VAR}", file=sys.stderr)
        raise SystemExit(2)
