"""Which database URLs the test guard accepts and refuses (tests/koi_test_targets.py).

Offline: nothing here connects. Where psycopg2 and asyncpg are installed, every accepted
URL is also parsed by both drivers' own DSN parsers, and both must name the database the
guard checked.
"""
import inspect

import pytest

from tests import koi_test_targets as targets

B = "postgresql://koi@127.0.0.1:55999"

ACCEPT = [
    B + "/koi_test",
    B + "/koi_test_admin",
    B + "/koi_test_some_tag_0123456789ab",   # scratch_name() shape
    B + "/test",
    B + "/test_canon",
    B + "/personal_koi_test",
    B + "/conformance-test-node",
    B + "/koi%5Ftest",                         # percent-encoded path, decoded by both drivers
    "postgres://koi@127.0.0.1:55999/koi_test",
    B + "/koi_test?application_name=x&sslmode=disable",
]

REFUSE = [
    # the live database's name
    B + "/personal_koi",
    B + "/personal%5Fkoi",
    # "test" only as part of a longer word
    B + "/latest",
    B + "/regen_attestations",
    B + "/koitest",
    B + "/koi_testing",
    B + "/contest_koi",
    # no "test" at all
    B + "/koi_scratch",
    B + "/PERSONAL_KOI",
    # names no database
    B + "/",
    B,
    # a database in the query string: drivers disagree on which name wins (round-5 probes)
    B + "/personal_koi?dbname=koi_test",               # asyncpg keeps the path: personal_koi
    B + "/koi_test?dbname=personal_koi",               # libpq takes the query: personal_koi
    B + "/koi_test?dbname=koi_test&dbname=personal_koi",  # libpq keeps the last duplicate
    B + "/?dbname=koi_test&dbname=personal_koi",
    B + "/?dbname=koi_test",                           # asyncpg ignores the query after "/"
    B + "?dbname=koi_test",
    B + "/koi_test?database=personal_koi",             # asyncpg's other key
    B + "/koi_test?db%6Eame=personal_koi",             # percent-encoded key
    B + "/koi_test?dbname=",
    # key=value DSNs, including conflicting dbname keys (libpq keeps the last)
    "host=127.0.0.1 port=55999 dbname=koi_test",
    "host=127.0.0.1 port=55999 dbname=koi_test dbname=personal_koi",
    "dbname=koi_test host=127.0.0.1 dbname=personal_koi",
    # other schemes
    "mysql://koi@127.0.0.1:55999/koi_test",
    "koi_test",
    # control characters, raw or percent-encoded. asyncpg writes a decoded NUL into its
    # startup packet, where it ends the database name: the first one reaches personal_koi
    # with application_name=koi_test (round-6 probe).
    B + "/personal_koi%00application_name%00koi_test",
    B + "/%00_test",
    B + "/koi_test%00",
    B + "/koi_test\x00",
    B + "/koi%0Atest",
    B + "/koi_test%09",
    B + "/koi_test%7F",
    B + "/koi_test?application_name=a%00b",
    "postgresql://koi%00@127.0.0.1:55999/koi_test",
]

API_REFUSE = [
    "http://127.0.0.1:58999/%00",
    "http://127.0.0.1:58999/x\x00y",
    "http://127.0.0.1:58999/%0D%0AHost:%20other",
]


@pytest.mark.parametrize("value", ACCEPT)
def test_accepts(value):
    for var in (targets.DATABASE_VAR, targets.ADMIN_VAR, targets.PEER_DATABASE_VAR):
        assert targets.problems({var: value}) == [], (var, value)


@pytest.mark.parametrize("value", REFUSE)
def test_refuses(value):
    for var in (targets.DATABASE_VAR, targets.ADMIN_VAR, targets.PEER_DATABASE_VAR):
        found = targets.problems({var: value})
        assert len(found) == 1 and var in found[0], (var, value, found)


@pytest.mark.parametrize("value", API_REFUSE)
def test_api_url_with_a_control_character_is_refused(value):
    for var in (targets.API_VAR, targets.PEER_API_VAR):
        found = targets.problems({var: value})
        assert len(found) == 1 and "control character" in found[0], (var, value, found)


@pytest.mark.parametrize("value", [B + "/personal_koi%00application_name%00koi_test",
                                   "http://127.0.0.1:58999/%00"])
def test_allow_live_does_not_lift_the_control_character_refusal(value):
    var = targets.API_VAR if value.startswith("http") else targets.DATABASE_VAR
    assert targets.problems({var: value, targets.ALLOW_LIVE_VAR: "1"}), value


def test_query_database_refusal_names_the_reason():
    [msg] = targets.problems({targets.DATABASE_VAR: B + "/koi_test?dbname=personal_koi"})
    assert "query string" in msg and "dbname" in msg


def test_ambiguous_url_has_no_database_name():
    assert targets.database_name(B + "/koi_test?dbname=personal_koi") is None
    assert targets.database_name(B + "/koi%5Ftest") == "koi_test"


def test_allow_live_still_lifts_the_refusal():
    env = {targets.DATABASE_VAR: B + "/koi_test?dbname=personal_koi", targets.ALLOW_LIVE_VAR: "1"}
    assert targets.problems(env) == []


def _libpq_name(dsn):
    extensions = pytest.importorskip("psycopg2.extensions")
    return extensions.parse_dsn(dsn).get("dbname")


def _asyncpg_name(dsn):
    connect_utils = pytest.importorskip("asyncpg.connect_utils")
    parse = getattr(connect_utils, "_parse_connect_dsn_and_args", None)
    if parse is None:
        pytest.skip("asyncpg has no _parse_connect_dsn_and_args")
    params = inspect.signature(parse).parameters
    kwargs = {name: None for name in params if name != "dsn"}
    kwargs["dsn"] = dsn
    if "connect_timeout" in kwargs:
        kwargs["connect_timeout"] = 1
    try:
        _addrs, conn_params = parse(**kwargs)
    except TypeError as exc:  # a private signature this test does not know
        pytest.skip(f"asyncpg parser signature changed: {exc}")
    return conn_params.database


@pytest.mark.parametrize("value", ACCEPT)
def test_drivers_agree_with_the_guard(value):
    """Every accepted URL names the same database to the guard, libpq and asyncpg."""
    name = targets.database_name(value)
    assert name and _libpq_name(value) == name
    assert _asyncpg_name(value) == name


@pytest.mark.parametrize("value", [B + "/personal_koi?dbname=koi_test",
                                   B + "/koi_test?dbname=koi_test&dbname=personal_koi"])
def test_drivers_do_disagree_on_query_names(value):
    """Positive control: these are the shapes that motivated the refusal."""
    assert _libpq_name(value) != _asyncpg_name(value)
