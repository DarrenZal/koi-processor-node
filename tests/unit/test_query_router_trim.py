"""/sql response trimming, on the real router with a stub read pool. No database."""
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routers import query_router

TOKEN = "query-router-trim-test-token"


class _Attribute:
    def __init__(self, name):
        self.name = name


class _Statement:
    def __init__(self, values):
        self.values = values

    async def fetch(self, *_a):
        return [{"v": value} for value in self.values]

    def get_attributes(self):
        return [_Attribute("v")]


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class _Connection:
    def __init__(self, values):
        self.values = values

    def transaction(self, readonly=False):
        return _Transaction()

    async def execute(self, *_a):
        return "SET"

    async def prepare(self, _sql):
        return _Statement(self.values)

    async def fetchval(self, *_a):
        return False  # the least-privilege probe: not privileged


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_exc):
        return False


class _Pool:
    def __init__(self, values):
        self.conn = _Connection(values)

    def acquire(self):
        return _Acquire(self.conn)


@pytest.fixture
def run(monkeypatch):
    monkeypatch.setenv("KOI_ADMIN_TOKEN", TOKEN)

    def call(values):
        app = FastAPI()
        app.include_router(query_router.create_router(_Pool(values), None, log_pool=None), prefix="/sql")
        r = TestClient(app).post("/sql", headers={"Authorization": f"Bearer {TOKEN}"},
                                 json={"sql": "SELECT entity_text FROM entity_registry", "limit": 100})
        assert r.status_code == 200, r.text
        return r.json()

    return call


def test_result_under_the_limit_is_untouched(run):
    body = run(["one", "two"])
    assert body["rows"] == [["one"], ["two"]]
    assert (body["row_count"], body["truncated"]) == (2, False)


def test_oversized_result_keeps_the_rows_that_fit(run):
    body = run(["short", "x" * 150_000])
    assert body["rows"] == [["short"]]
    assert (body["row_count"], body["truncated"]) == (1, True)


def test_a_single_oversized_row_returns_no_rows_and_says_so(run):
    body = run(["x" * 150_000])
    assert body["rows"] == []
    assert (body["row_count"], body["truncated"]) == (0, True)
