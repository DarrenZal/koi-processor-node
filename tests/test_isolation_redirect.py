"""The test-target guard: tests/koi_test_targets.py and its use in tests/conftest.py.

Most checks here are pure or run a subprocess, with no database, API or network. The
last one connects, and only to KOI_TEST_DATABASE_URL (it skips when that is unset).
"""
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import asyncpg
import pytest

from tests import koi_test_targets as targets

REPO = Path(__file__).resolve().parent.parent
GOOD_DB = "postgresql://koi@127.0.0.1:55999/koi_test_example"
GOOD_API = "http://127.0.0.1:58999"


@pytest.mark.parametrize("value, refused", [
    (GOOD_DB, False),
    ("postgresql://koi@127.0.0.1:55999/personal_koi", True),       # the live database's name
    ("postgresql://koi@127.0.0.1:55999/koi_scratch", True),        # no "test" in the name
    ("postgresql://koi@127.0.0.1:55999/", True),                   # names no database
    ("host=127.0.0.1 dbname=koi_test", True),                      # not a URL: name unknowable
    ("postgresql://koi@h:1/koi_test_x?dbname=personal_koi", True), # dbname= overrides the path
])
def test_database_values(value, refused):
    for var in (targets.DATABASE_VAR, targets.ADMIN_VAR):
        assert bool(targets.problems({var: value})) is refused, (var, value)


@pytest.mark.parametrize("value, refused", [
    (GOOD_API, False),
    ("http://127.0.0.1:8351", True),
    ("http://localhost:8351/", True),
    ("https://koi.example.org", False),     # default port 443
    ("127.0.0.1:58999", True),              # no scheme
])
def test_api_values(value, refused):
    assert bool(targets.problems({targets.API_VAR: value})) is refused, value


def test_allow_live_lifts_every_refusal():
    env = {targets.DATABASE_VAR: "postgresql://koi@h:1/personal_koi",
           targets.API_VAR: "http://127.0.0.1:8351", targets.ALLOW_LIVE_VAR: "1"}
    assert targets.problems(env) == []
    env[targets.ALLOW_LIVE_VAR] = "yes"     # only the exact value 1 opts in
    assert len(targets.problems(env)) == 2


@pytest.mark.parametrize("accessor, var", [
    (targets.database_url, targets.DATABASE_VAR),
    (targets.admin_dsn, targets.ADMIN_VAR),
    (targets.api_url, targets.API_VAR),
])
def test_unset_variable_skips_and_names_it(monkeypatch, accessor, var):
    monkeypatch.delenv(var, raising=False)
    with pytest.raises(pytest.skip.Exception, match=var):
        accessor()


def test_redirect_overrides_inherited_live_values():
    env = {"POSTGRES_URL": "postgresql://u@localhost/personal_koi", "KOI_API_URL": "http://x:1",
           "PGHOST": "localhost", "PGPORT": "1", "PGDATABASE": "personal_koi",
           "KOI_LIVE_POSTGRES_URL": "postgresql://u@localhost/personal_koi"}
    targets.redirect_runtime_config(env)
    assert env["POSTGRES_URL"] == env["DATABASE_URL"] == targets.UNSET_DATABASE_URL
    assert env["KOI_API_URL"] == env["BASE_URL"] == targets.UNSET_API_URL
    assert env["PGHOST"] == targets.UNSET_PGHOST and "PGPORT" not in env
    assert env["POSTGRES_HOST"] == targets.UNSET_HOST
    assert "KOI_LIVE_POSTGRES_URL" not in env

    env = {targets.DATABASE_VAR: GOOD_DB, targets.API_VAR: GOOD_API + "/"}
    targets.redirect_runtime_config(env)
    assert env["POSTGRES_URL"] == GOOD_DB and env["KOI_API_URL"] == GOOD_API
    assert (env["PGHOST"], env["PGPORT"], env["PGDATABASE"]) == ("127.0.0.1", "55999", "koi_test_example")


def test_conftest_redirected_this_process():
    """conftest ran redirect_runtime_config before this module was imported."""
    expected = os.environ.get(targets.DATABASE_VAR, "").strip() or targets.UNSET_DATABASE_URL
    assert os.environ["POSTGRES_URL"] == expected
    assert "KOI_LIVE_POSTGRES_URL" not in os.environ


def test_scratch_names_and_drops():
    name = targets.scratch_name("Some Tag")
    assert name.startswith("koi_test_some_tag_") and name.replace("_", "").isalnum()
    assert targets.droppable(name) == name
    for other in ("koi_test_not_mine", "personal_koi", "postgres"):
        with pytest.raises(RuntimeError):
            targets.droppable(other)
    assert targets.with_database(GOOD_DB, name) == f"postgresql://koi@127.0.0.1:55999/{name}"
    assert targets.with_user(GOOD_DB, "ro") == "postgresql://ro@127.0.0.1:55999/koi_test_example"


@pytest.mark.parametrize("var, value", [
    (targets.DATABASE_VAR, "postgresql://koi@127.0.0.1:55999/personal_koi"),
    (targets.API_VAR, "http://127.0.0.1:8351"),
    # a NUL the guard used to accept; asyncpg then connected to personal_koi
    (targets.DATABASE_VAR, "postgresql://koi@127.0.0.1:55999/personal_koi%00application_name%00koi_test"),
    (targets.ADMIN_VAR, "postgresql://koi@127.0.0.1:55999/%00_test"),
])
def test_guard_ends_the_run_before_collection(tmp_path, var, value):
    env = {k: v for k, v in os.environ.items() if not k.startswith("KOI_TEST_")}
    env.update({var: value, "PYTHONDONTWRITEBYTECODE": "1"})
    r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--collect-only",
                        "-q", "tests/test_isolation_redirect.py"],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 4, (r.returncode, r.stdout[-2000:], r.stderr[-2000:])
    assert "koi test guard: refusing to run" in r.stderr
    assert "collected" not in r.stdout


def test_redirect_covers_every_target_variable_the_code_reads():
    live = "postgresql://u@localhost/personal_koi"
    env = {"KOI_TASK_API": "http://localhost:8351", "KOI_DB_NAME": "personal_koi",
           "ROUTER_PG_DSN": live, "PERSONAL_KOI_DSN": live,
           "TERMINUSDB_URL": "http://127.0.0.1:6363/", "OLLAMA_BASE_URL": "http://localhost:11434"}
    targets.redirect_runtime_config(env)
    assert env["KOI_TASK_API"] == targets.UNSET_API_URL
    assert env["KOI_DB_NAME"] == "koi_test_unset"
    assert env["ROUTER_PG_DSN"] == env["PERSONAL_KOI_DSN"] == targets.UNSET_DATABASE_URL
    assert env["TERMINUSDB_URL"] == env["OLLAMA_BASE_URL"] == targets.UNSET_API_URL
    for var in targets.SERVICE_URL_VARS:
        assert env[var] == targets.UNSET_API_URL, var

    env = {targets.DATABASE_VAR: GOOD_DB, targets.API_VAR: GOOD_API}
    targets.redirect_runtime_config(env)
    assert env["ROUTER_PG_DSN"] == env["PERSONAL_KOI_DSN"] == GOOD_DB
    assert env["KOI_DB_NAME"] == "koi_test_example" and env["KOI_TASK_API"] == GOOD_API


def test_refused_value_is_never_copied_into_runtime_variables():
    """A NUL in os.environ raises ValueError; a refused value must not get that far."""
    bad = "postgresql://koi@127.0.0.1:55999/personal_koi%00application_name%00koi_test"
    env = {targets.DATABASE_VAR: bad, targets.ADMIN_VAR: bad, "POSTGRES_URL": "x"}
    targets.redirect_runtime_config(env)
    assert env["POSTGRES_URL"] == env["KOI_SCRATCH_ADMIN_DSN"] == targets.UNSET_DATABASE_URL
    assert env["PGDATABASE"] == "koi_test_unset"


def test_state_dirs_point_at_this_runs_directory():
    """conftest pointed HOME and the state, key and vault locations at a temporary root."""
    from tests import conftest
    root = os.path.realpath(conftest.STATE_ROOT)

    def under_root(path):
        return os.path.realpath(path).startswith(root + os.sep)

    assert under_root(os.environ["HOME"]) and under_root(str(Path.home()))
    for var in (targets.STATE_DIR_VARS + targets.VAULT_PATH_VARS + targets.STATE_FILE_VARS
                + targets.CREDENTIAL_FILE_VARS):
        assert under_root(os.environ[var]), var
    for default in ("~/.config/personal-koi/koi-state", "~/.koi/logs", "~/.regen", "~/Documents/Notes"):
        assert under_root(os.path.expanduser(default)), default


def test_inherited_password_file_and_access_config_never_reach_a_test(tmp_path):
    """Round 7: tests read an inherited PGPASSFILE on every connection, and an inherited
    KOI_READ_SCOPES_FILE and KOI_AUTH_MODE changed read-route outcomes. A process that loads
    tests/conftest.py with hostile values sees an empty password file, an empty scopes map
    and the default auth mode, and never opens the inherited files."""
    live_pgpass = tmp_path / "live-pgpass"
    live_pgpass.write_text("*:*:*:*:inherited-from-the-shell\n")
    live_pgpass.chmod(0o600)
    live_scopes = tmp_path / "live-scopes.json"
    live_scopes.write_text('{"%s": {"view": "public"}}\n' % ("0" * 64))
    env = {k: v for k, v in os.environ.items() if not k.startswith("KOI_TEST_")}
    env.update({"PGPASSFILE": str(live_pgpass), "KOI_READ_SCOPES_FILE": str(live_scopes),
                "KOI_AUTH_MODE": "enforce", "PYTHONDONTWRITEBYTECODE": "1"})
    code = ("import json, os, sys, tests.conftest as c\n"
            "opened = []\n"
            "sys.addaudithook(lambda ev, a: opened.append(str(a[0])) if ev == 'open' else None)\n"
            "from api import read_auth, read_scope\n"
            "out = {v: os.environ[v] for v in ('PGPASSFILE', 'KOI_READ_SCOPES_FILE', 'KOI_AUTH_MODE')}\n"
            "out['pgpass'] = open(os.environ['PGPASSFILE']).read()\n"
            "out['scopes'] = len(read_scope.load_scope_map())\n"
            "out['mode'] = read_auth.auth_mode()\n"
            "out['root'] = c.STATE_ROOT\n"
            "out['opened'] = opened\n"
            "print(json.dumps(out))\n")
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    out = json.loads(r.stdout.strip().splitlines()[-1])
    for var in targets.CREDENTIAL_FILE_VARS:
        assert out[var].startswith(out["root"]), var
    assert out["pgpass"] == "" and out["scopes"] == 0
    assert out["KOI_AUTH_MODE"] == targets.DEFAULT_AUTH_MODE and out["mode"] == "log"
    assert not {str(live_pgpass), str(live_scopes)} & set(out["opened"])


def _hostile_state_env(tmp_path):
    """The test environment plus an inherited HOME, key dir and vault that must stay empty."""
    live = {name: tmp_path / name for name in ("live-home", "live-state", "live-vault")}
    for path in live.values():
        path.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith("KOI_TEST_")}
    env.update({"HOME": str(live["live-home"]), "KOI_STATE_DIR": str(live["live-state"]),
                "OBSIDIAN_VAULT_PATH": str(live["live-vault"]), "VAULT_PATH": str(live["live-vault"]),
                "KOI_NET_ENABLED": "true", "VAULT_SYNC_ENABLED": "true",
                "KOI_NODE_NAME": "hostile-live-node", "PYTHONDONTWRITEBYTECODE": "1"})
    return env, live


def test_node_keys_are_created_only_in_the_runs_directory(tmp_path):
    """A process that loads tests/conftest.py and then creates a node identity writes its keys
    under the run's temporary root, which is removed at exit, never in the inherited HOME or
    KOI_STATE_DIR."""
    env, live = _hostile_state_env(tmp_path)
    code = ("import os, tests.conftest as c\n"
            "from api import node_identity as n\n"
            "n.load_or_create_identity('hostile-live-node')\n"
            "print(c.STATE_ROOT)\n"
            "print(n.KEY_STATE_DIR)\n"
            "print(sorted(os.listdir(n.KEY_STATE_DIR)))\n")
    r = subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-2000:]
    root, key_dir, files = r.stdout.strip().splitlines()[-3:]
    assert key_dir.startswith(root) and "hostile-live-node_private_key.pem" in files
    assert not os.path.exists(root), "the run's state root is removed at exit"
    for name, path in live.items():
        assert not any(path.iterdir()), f"{name} was written to"


def test_full_api_startup_with_koi_net_writes_no_inherited_state(tmp_path):
    """Round-6 N2: a test that runs the API's startup with KOI-net enabled created node keys in
    the inherited KOI_STATE_DIR. Run it with a hostile HOME, key dir and vault."""
    target = "tests/test_knowledge_router_facts_gate.py::test_startup_sets_facts_surface_flag"
    if not (REPO / target.split("::")[0]).is_file():
        pytest.skip("the startup test is not in this checkout")
    env, live = _hostile_state_env(tmp_path)
    r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", target],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, (r.returncode, r.stdout[-2000:], r.stderr[-2000:])
    assert "1 passed" in r.stdout, r.stdout[-2000:]
    for name, path in live.items():
        written = sorted(p.name for p in path.rglob("*"))
        assert not written, f"{name} was written to: {written}"


def test_live_llm_tests_need_an_explicit_opt_in():
    key = {"OPENAI_API_KEY": "sk-test-not-used"}
    assert "KOI_TEST_LLM" in targets.live_llm_skip_reason("OPENAI_API_KEY", env=key)
    assert targets.live_llm_skip_reason("OPENAI_API_KEY", env={**key, "KOI_TEST_LLM": "yes"})
    assert targets.live_llm_skip_reason("OPENAI_API_KEY", env={**key, "KOI_TEST_LLM": "1"}) is None
    assert "OPENAI_API_KEY" in targets.live_llm_skip_reason("OPENAI_API_KEY", env={"KOI_TEST_LLM": "1"})


def test_live_marked_tests_skip_with_a_key_and_no_opt_in(tmp_path):
    """With a provider key set and KOI_TEST_LLM unset, every `live` test skips: none sends a
    request (the base URL points nowhere, so a request would fail rather than skip)."""
    files = [f for f in ("tests/test_chat_provider.py", "tests/test_classifier_regression.py",
                         "tests/test_crawl_page_analysis_prompt.py") if (REPO / f).is_file()]
    if not files:
        pytest.skip("no live-marked tests in this checkout")
    env = {k: v for k, v in os.environ.items() if not k.startswith("KOI_TEST_")}
    env.update({"OPENAI_API_KEY": "sk-test-not-used", "ANTHROPIC_API_KEY": "sk-ant-test-not-used",
                "OPENAI_BASE_URL": targets.UNSET_API_URL, "PYTHONDONTWRITEBYTECODE": "1"})
    r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", "-rs",
                        "-m", "live", *files], cwd=REPO, env=env, capture_output=True, text=True,
                       timeout=300)
    assert r.returncode == 0, (r.returncode, r.stdout[-3000:])
    assert "skipped" in r.stdout and " passed" not in r.stdout and "failed" not in r.stdout
    assert "KOI_TEST_LLM=1 not set" in r.stdout


def test_connection_lands_in_the_named_database():
    dsn = targets.database_url()

    async def run():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetchval("SELECT current_database()")
        finally:
            await conn.close()

    assert asyncio.run(run()) == targets.database_name(dsn)
