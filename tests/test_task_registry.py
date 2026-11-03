"""
Regression tests for the task_registry API (task_router.py).

Runs against the API at KOI_TEST_API_URL, which must write to the database at
KOI_TEST_DATABASE_URL. The module skips unless both are set (see
tests/koi_test_targets.py, which refuses live-looking targets). Each test uses
unique task keys prefixed with "reg-test-".

The writes go over HTTP, so the API process decides where they land. Teardown
therefore deletes every key this run minted from KOI_TEST_DATABASE_URL and fails
the run if it cannot, or if it deletes nothing (the API is then writing somewhere
else). cleanup() below is a status transition kept for tests that assert on
cancelled semantics; it is not the purge.

Usage:
    KOI_TEST_API_URL=http://127.0.0.1:<port> KOI_TEST_DATABASE_URL=postgresql://.../<test db> \
        pytest tests/test_task_registry.py -v
"""

import uuid
import pytest
import httpx

from tests import koi_test_targets as targets

pytestmark = [targets.requires_api, targets.requires_database]

# Every key minted by make_key(), so teardown can purge exactly what this run
# created and nothing a concurrent session created. Populated at mint time
# rather than after a successful ingest: a test that dies mid-request may
# already have written the row.
CREATED_KEYS: set[str] = set()


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=targets.api_url(), timeout=10.0) as c:
        yield c


def make_key(suffix: str) -> str:
    """Generate a unique test task key, recorded for teardown."""
    key = f"reg-test-{suffix}-{uuid.uuid4().hex[:6]}"
    CREATED_KEYS.add(key)
    return key


def ingest(client, key: str, sourceType: str = "test", **kwargs) -> dict:
    payload = {"taskKey": key, "title": kwargs.pop("title", "Regression test task"), "sourceType": sourceType, **kwargs}
    r = client.post("/tasks/ingest", json=payload)
    assert r.status_code == 200, f"ingest failed ({r.status_code}): {r.text}"
    return r.json()


def cleanup(client, key: str):
    """Transition a test task to cancelled.

    NOT a purge -- the row survives. Kept because several tests assert on
    cancelled-status semantics. Removal is purge_test_tasks() below.
    """
    client.patch(f"/tasks/{key}", json={"status": "cancelled"})


@pytest.fixture(scope="module", autouse=True)
def purge_test_tasks():
    """Delete every task this module created from KOI_TEST_DATABASE_URL.

    Fails the run if it cannot purge, if it deletes nothing, or if rows survive.
    A teardown that cannot prove it acted is indistinguishable from one that did
    nothing. Nothing holds a foreign key onto task_registry.
    """
    dsn = targets.database_url()
    yield

    if not CREATED_KEYS:
        return

    keys = sorted(CREATED_KEYS)
    try:
        import psycopg2
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            landed = cur.fetchone()[0]
            cur.execute("DELETE FROM task_registry WHERE task_key = ANY(%s)", (keys,))
            purged = cur.rowcount
            # Confirm rather than trust: ask the same database what survived.
            cur.execute(
                "SELECT count(*) FROM task_registry WHERE task_key = ANY(%s)", (keys,)
            )
            remaining = cur.fetchone()[0]
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        pytest.fail(
            f"\n*** TASK FIXTURE LEAK — PURGE FAILED ***\n"
            f"{len(keys)} task(s) written via KOI_TEST_API_URL; cleanup against "
            f"KOI_TEST_DATABASE_URL raised {type(exc).__name__}: {exc}",
            pytrace=False,
        )

    if purged == 0:
        # CREATED_KEYS is non-empty and ingest() asserts 200, so at least one row
        # existed unless every ingest failed. Deleting nothing means the API writes
        # to a different database than KOI_TEST_DATABASE_URL.
        pytest.fail(
            f"\n*** TASK FIXTURE LEAK — PURGE DELETED NOTHING ***\n"
            f"{len(keys)} task(s) were ingested via KOI_TEST_API_URL but the DELETE in "
            f"'{landed}' removed 0 rows. The API is writing to another database.",
            pytrace=False,
        )

    if remaining:
        pytest.fail(
            f"\n*** TASK FIXTURE LEAK — ROW(S) SURVIVED PURGE ***\n"
            f"In {landed}: deleted {purged} of {len(keys)} expected rows; "
            f"{remaining} still match.",
            pytrace=False,
        )

    print(f"\n[cleanup] purged {purged} reg-test task row(s) from {landed}")


def get_task(client, key: str, status_filter: str = "open,inbox,in-progress,waiting,cancelled") -> dict | None:
    """Fetch one task by key via GET /tasks/{key} (exact, no list truncation).

    Was previously a filtered GET /tasks/?limit=1000 scan, which silently
    dropped freshly-created test tasks once the registry grew past 1000 rows of
    a status (they sort last by id). The by-key endpoint (P5 Fix 1) makes this
    deterministic; status_filter is retained for call-site compatibility.
    """
    r = client.get(f"/tasks/{key}")
    if r.status_code == 404:
        return None
    assert r.status_code == 200, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Precondition: backend is serving /tasks routes
# ---------------------------------------------------------------------------

def test_tasks_stats_reachable(client):
    """Readiness check — verifies router is mounted, not just the process alive."""
    r = client.get("/tasks/stats")
    assert r.status_code == 200, "/tasks/stats not reachable — is the backend running?"
    data = r.json()
    assert "total_open" in data
    assert "by_status" in data


# ---------------------------------------------------------------------------
# Regression 1: Partial ingest preserves existing status and priority
# ---------------------------------------------------------------------------

class TestIngestPreservesStatusPriority:
    """Fix 2: partial POST /tasks/ingest must not reset status/priority to defaults."""

    def test_partial_ingest_preserves_status_and_priority(self, client):
        key = make_key("partial-preserve")
        try:
            # Create with explicit status/priority
            r = ingest(client, key, status="open", priority="high")
            assert r["action"] == "created"

            # Re-ingest without status/priority (partial payload)
            r2 = client.post("/tasks/ingest", json={"taskKey": key, "title": "Updated title"})
            assert r2.status_code == 200
            assert r2.json()["action"] == "updated"

            task = get_task(client, key)
            assert task is not None, f"Task {key} not found after partial ingest"
            assert task["status"] == "open", f"status regressed to '{task['status']}' (expected 'open')"
            assert task["priority"] == "high", f"priority regressed to '{task['priority']}' (expected 'high')"
        finally:
            cleanup(client, key)

    def test_partial_ingest_null_status_preserves_existing(self, client):
        key = make_key("null-status-preserve")
        try:
            ingest(client, key, status="waiting", priority="critical")

            # Explicitly pass null status
            r = client.post("/tasks/ingest", json={"taskKey": key, "title": "T", "status": None})
            assert r.status_code == 200

            task = get_task(client, key)
            assert task is not None
            assert task["status"] == "waiting", f"status regressed: {task['status']}"
            assert task["priority"] == "critical", f"priority regressed: {task['priority']}"
        finally:
            cleanup(client, key)

    def test_new_task_gets_default_status_inbox(self, client):
        key = make_key("default-status")
        try:
            r = ingest(client, key)
            assert r["action"] == "created"

            task = get_task(client, key, status_filter="inbox")
            assert task is not None, f"New task {key} not found in inbox"
            assert task["status"] == "inbox"
            assert task["priority"] == "medium"
        finally:
            cleanup(client, key)


# ---------------------------------------------------------------------------
# Regression 2: PATCH date field behaviour
# ---------------------------------------------------------------------------

class TestPatchDateClearing:
    """Fix 1 & 3: PATCH must not 500; null date clears; absent date is no-op."""

    def test_patch_status_returns_200(self, client):
        key = make_key("patch-status")
        try:
            ingest(client, key, status="inbox")
            r = client.patch(f"/tasks/{key}", json={"status": "open"})
            assert r.status_code == 200, f"PATCH status returned {r.status_code}: {r.text}"
            task = get_task(client, key, status_filter="open")
            assert task is not None
            assert task["status"] == "open"
        finally:
            cleanup(client, key)

    def test_patch_set_due_date(self, client):
        key = make_key("patch-set-date")
        try:
            ingest(client, key)
            r = client.patch(f"/tasks/{key}", json={"dueDate": "2026-03-15"})
            assert r.status_code == 200, r.text

            task = get_task(client, key)
            assert task is not None
            assert task["due_date"] == "2026-03-15", f"due_date not set: {task['due_date']}"
        finally:
            cleanup(client, key)

    def test_patch_clear_due_date_with_null(self, client):
        key = make_key("patch-clear-date")
        try:
            ingest(client, key)
            client.patch(f"/tasks/{key}", json={"dueDate": "2026-03-15"})

            # Clear with explicit null
            r = client.patch(f"/tasks/{key}", json={"dueDate": None})
            assert r.status_code == 200, r.text

            task = get_task(client, key)
            assert task is not None
            assert task["due_date"] is None, f"due_date not cleared: {task['due_date']}"
        finally:
            cleanup(client, key)

    def test_patch_absent_date_does_not_clear(self, client):
        key = make_key("patch-absent-date")
        try:
            ingest(client, key)
            client.patch(f"/tasks/{key}", json={"dueDate": "2026-04-01"})

            # PATCH without dueDate — must leave it untouched
            r = client.patch(f"/tasks/{key}", json={"status": "open"})
            assert r.status_code == 200

            task = get_task(client, key, status_filter="open")
            assert task is not None
            assert task["due_date"] == "2026-04-01", f"due_date unexpectedly cleared: {task['due_date']}"
        finally:
            cleanup(client, key)

    def test_patch_nonexistent_task_returns_404(self, client):
        r = client.patch("/tasks/does-not-exist-xyz-000", json={"status": "open"})
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Regression 3: Owner name filter
# ---------------------------------------------------------------------------

class TestOwnerNameFilter:
    """Fix 4: ?owner=Name must resolve via entity_rid_mappings, not just URI substring."""

    def test_owner_uri_substring_filter_works(self, client):
        """Fallback: LIKE on owner_uri still returns tasks for opaque URIs."""
        key = make_key("owner-uri-filter")
        # Use a distinctive URI fragment that won't collide with real data
        owner_uri_fragment = f"orn:test:owner-filter-{uuid.uuid4().hex[:8]}"
        try:
            # Ingest without owner (no wikilink to resolve), then verify generic filter
            ingest(client, key, status="inbox")

            # Filter by an owner URI that matches nothing, and confirm no 500
            r = client.get("/tasks/", params={"owner": "orn:test:nonexistent"})
            assert r.status_code == 200
            # Should return empty list (no match), not an error
            tasks = r.json()
            assert isinstance(tasks, list)
        finally:
            cleanup(client, key)

    def test_owner_filter_no_500_on_name_query(self, client):
        """Name-based owner filter must return 200 (not 500) even with no matches."""
        r = client.get("/tasks/", params={"owner": "Pat Example"})
        assert r.status_code == 200, f"?owner=Name returned {r.status_code}: {r.text}"
        assert isinstance(r.json(), list)


# ---------------------------------------------------------------------------
# Regression 4: Stats excludes source_type='test' tasks
# ---------------------------------------------------------------------------

class TestStatsExcludesTestSourceType:
    def test_stats_does_not_count_test_source_type(self, client):
        """Tasks with source_type='test' must not appear in /tasks/stats counts."""
        key = make_key("stats-exclude")
        before_waiting = client.get("/tasks/stats").json()["by_status"].get("waiting", 0)

        ingest(client, key, status="waiting")
        try:
            # Sanity: task exists in direct query
            direct = client.get("/tasks/", params={"source_type": "test", "status": "waiting"}).json()
            assert any(t["task_key"] == key for t in direct), "Test task must exist via direct query"

            # Assertion: stats waiting count must not have increased
            after_waiting = client.get("/tasks/stats").json()["by_status"].get("waiting", 0)
            assert after_waiting == before_waiting, (
                f"stats waiting count grew {before_waiting} → {after_waiting}; "
                "test task with source_type='test' should be excluded"
            )
        finally:
            cleanup(client, key)


# ---------------------------------------------------------------------------
# P5 Fix 1: PATCH title, GET by key, /stats route ordering, limit + X-Total-Count
# ---------------------------------------------------------------------------

class TestPatchTitle:
    """PATCH title must persist (previously silently dropped)."""

    def test_patch_title_persists(self, client):
        key = make_key("patch-title")
        try:
            ingest(client, key, title="Original title")
            r = client.patch(f"/tasks/{key}", json={"title": "Renamed title"})
            assert r.status_code == 200, r.text

            task = get_task(client, key)
            assert task is not None
            assert task["title"] == "Renamed title", f"title not updated: {task['title']}"
        finally:
            cleanup(client, key)

    def test_patch_absent_title_preserves_existing(self, client):
        key = make_key("patch-title-absent")
        try:
            ingest(client, key, title="Keep me")
            r = client.patch(f"/tasks/{key}", json={"status": "open"})
            assert r.status_code == 200

            task = get_task(client, key, status_filter="open")
            assert task is not None
            assert task["title"] == "Keep me", f"title unexpectedly changed: {task['title']}"
        finally:
            cleanup(client, key)


class TestGetTaskByKey:
    """GET /tasks/{key} returns the record or 404 (previously 405 — only PATCH matched)."""

    def test_get_by_key_returns_200_and_record(self, client):
        key = make_key("get-by-key")
        try:
            ingest(client, key, title="Fetch me", status="open", priority="high")
            r = client.get(f"/tasks/{key}")
            assert r.status_code == 200, f"GET by key returned {r.status_code}: {r.text}"
            body = r.json()
            assert body["task_key"] == key
            assert body["title"] == "Fetch me"
            assert body["status"] == "open"
            assert body["priority"] == "high"
        finally:
            cleanup(client, key)

    def test_get_by_key_404_for_missing(self, client):
        r = client.get("/tasks/reg-test-does-not-exist-zzz-000")
        assert r.status_code == 404, f"expected 404, got {r.status_code}: {r.text}"

    def test_stats_route_still_wins_over_by_key(self, client):
        """Regression: /tasks/stats must resolve to the stats endpoint, not
        GET /{task_key} with task_key='stats'."""
        r = client.get("/tasks/stats")
        assert r.status_code == 200, r.text
        data = r.json()
        # Stats shape, not a TaskRecord — proves route ordering is intact.
        assert "by_status" in data and "total_open" in data
        assert "task_key" not in data


class TestListPaginationHeader:
    """limit accepts up to 5000; X-Total-Count header is present and consistent."""

    def test_limit_5000_accepted(self, client):
        r = client.get("/tasks/", params={"limit": 5000})
        assert r.status_code == 200, f"limit=5000 rejected: {r.status_code}: {r.text}"
        assert isinstance(r.json(), list)

    def test_limit_over_5000_rejected(self, client):
        r = client.get("/tasks/", params={"limit": 5001})
        assert r.status_code == 422, f"expected 422 for limit>5000, got {r.status_code}"

    def test_x_total_count_present_and_ge_results(self, client):
        # Use a small limit so total (all filtered rows) >= returned length.
        r = client.get("/tasks/", params={"limit": 5})
        assert r.status_code == 200, r.text
        assert "X-Total-Count" in r.headers, "X-Total-Count header missing"
        total = int(r.headers["X-Total-Count"])
        returned = len(r.json())
        assert total >= returned, f"X-Total-Count ({total}) < returned rows ({returned})"
        assert returned <= 5

    def test_x_total_count_matches_filtered_scope(self, client):
        """With a unique source_type filter the header equals the exact count."""
        keys = [make_key("total-count") for _ in range(3)]
        unique_source = f"reg-test-src-{uuid.uuid4().hex[:8]}"
        try:
            for k in keys:
                ingest(client, k, sourceType=unique_source, status="open")
            r = client.get("/tasks/", params={"source_type": unique_source, "limit": 1})
            assert r.status_code == 200, r.text
            assert int(r.headers["X-Total-Count"]) == 3, (
                f"X-Total-Count {r.headers.get('X-Total-Count')} != 3 for scoped filter"
            )
            assert len(r.json()) == 1  # limit honored independently of the count
        finally:
            for k in keys:
                cleanup(client, k)


class TestDateFilterValidation:
    """A malformed due_before/due_after/updated_before/updated_after must 422,
    not silently widen the query to unfiltered. _parse_date returned None for
    both "absent" and "unparseable", and the four call sites only added the
    WHERE clause `if d:` — so e.g. ?due_before=2026-08-24T00:00:00Z (a full
    ISO datetime; only YYYY-MM-DD parses) or ?due_before=garbage dropped the
    filter entirely and still returned HTTP 200 with every task. Live-verified
    before the fix: due_before=2026-08-24 -> 415 rows, both malformed values
    above -> 4,265 rows (identical to no filter at all)."""

    @pytest.mark.parametrize("param", [
        "due_before", "due_after", "updated_before", "updated_after",
    ])
    @pytest.mark.parametrize("bad_value", [
        "2026-08-24T00:00:00Z",  # full ISO datetime, not a bare date
        "garbage",
        "2026-13-40",  # not a real calendar date
    ])
    def test_malformed_date_filter_rejected(self, client, param, bad_value):
        r = client.get("/tasks/", params={param: bad_value, "limit": 1})
        assert r.status_code == 422, (
            f"{param}={bad_value!r} should 422, got {r.status_code}: {r.text}"
        )

    @pytest.mark.parametrize("param", [
        "due_before", "due_after", "updated_before", "updated_after",
    ])
    def test_valid_iso_date_filter_accepted(self, client, param):
        r = client.get("/tasks/", params={param: "2026-08-24", "limit": 1})
        assert r.status_code == 200, f"{param}=2026-08-24 rejected: {r.status_code}: {r.text}"
        assert isinstance(r.json(), list)

    def test_absent_date_filters_still_default_to_unfiltered(self, client):
        """Confirms the strict parser didn't change absent-filter behavior —
        only present-and-malformed values should 422."""
        r = client.get("/tasks/", params={"limit": 1})
        assert r.status_code == 200, r.text
