"""Contract tests for KOI runtime profiles.

Validates that any deployment profile satisfies the behavioral contract
defined by the KOI Runtime Convergence Plan. Tests run against the server at
KOI_TEST_API_URL, with no mocking, and skip when it is unset. Several POST an
empty body to trigger endpoints, so point it at a throwaway server.

Run against a specific server:
    KOI_TEST_API_URL=http://127.0.0.1:<throwaway-port> pytest tests/test_contract.py -v

Run only core tests:
    pytest tests/test_contract.py -v -m core

Run only federation tests:
    pytest tests/test_contract.py -v -m federation
"""

import httpx
import pytest

from tests import koi_test_targets as targets

# ---------------------------------------------------------------------------
# Endpoint catalogs
# ---------------------------------------------------------------------------

# Endpoints that exist in the current codebase and must not regress.
CORE_ENDPOINTS = [
    ("GET", "/health"),
    ("GET", "/stats"),
    ("POST", "/entity/resolve"),
    ("GET", "/entities"),
    ("POST", "/ingest"),
    ("POST", "/search"),
    ("GET", "/graph/neighborhood/test"),
    ("GET", "/graph/shortest-path"),
]

# Planned core endpoints from the convergence plan (Phase 1+).
# Separated so tests don't fail against the pre-convergence codebase.
# Move entries to CORE_ENDPOINTS as they are implemented.
PLANNED_CORE_ENDPOINTS = [
    ("GET", "/entities/types"),
    ("POST", "/batch-ingest"),
    ("GET", "/ingest/status"),
    ("POST", "/entity/merge"),
    ("GET", "/relationships"),
    ("GET", "/predicates"),
    ("GET", "/embedding-status"),
    ("POST", "/reindex"),
    ("GET", "/migrations"),
    ("POST", "/migrate"),
    ("POST", "/graph/query"),
    ("GET", "/graph/stats"),
]

FEDERATION_ENDPOINTS = [
    ("POST", "/koi-net/handshake"),
    ("POST", "/koi-net/events/poll"),
    ("POST", "/koi-net/events/broadcast"),
    ("POST", "/koi-net/events/confirm"),
    ("GET", "/koi-net/health"),
    ("GET", "/koi-net/edges"),
    ("GET", "/koi-net/nodes"),
    ("POST", "/koi-net/share"),
    ("GET", "/koi-net/shared-with-me"),
    ("POST", "/koi-net/cross-ref/resolve"),
    ("GET", "/koi-net/cross-refs"),
    ("GET", "/koi-net/event-queue"),
    ("POST", "/koi-net/connect"),
    ("GET", "/koi-net/rejected-events"),
]

PERSONAL_ONLY = [
    ("POST", "/koi-net/vault-sync/trigger"),
    ("GET", "/koi-net/vault-sync/status"),
    ("GET", "/graph/history/test"),
    ("GET", "/graph/timeline"),
]

BKC_ONLY = [
    ("POST", "/web/preview"),
    ("POST", "/web/evaluate"),
    ("POST", "/web/process"),
    ("POST", "/web/ingest"),
    ("GET", "/web/submissions"),
    ("POST", "/entity-search"),
    ("GET", "/network/nodes"),
    ("GET", "/network/entities"),
    ("GET", "/network/health"),
]

# BKC endpoints that exist as stubs (501) — tracked separately so we know
# when they become functional.  Move to BKC_ONLY when implemented.
BKC_PLANNED = [
    ("POST", "/github/scan"),
    ("GET", "/github/status"),
    ("GET", "/github/repos"),
]

ALL_ENDPOINTS = CORE_ENDPOINTS + PLANNED_CORE_ENDPOINTS + FEDERATION_ENDPOINTS + PERSONAL_ONLY + BKC_ONLY + BKC_PLANNED


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """Synchronous httpx client pointed at the server under test."""
    with httpx.Client(base_url=targets.api_url(), timeout=15.0) as c:
        yield c


@pytest.fixture(scope="module")
def server_profile(client):
    """Detect the deployment profile by probing marker endpoints.

    Returns a set of profile tags, e.g. {"core", "federation", "personal"}
    or {"core", "federation", "bkc"}.
    """
    profiles = {"core"}

    # Check federation
    try:
        r = client.get("/koi-net/health")
        if r.status_code != 404:
            profiles.add("federation")
    except httpx.ConnectError:
        pass

    # Check personal-only marker
    try:
        r = client.get("/koi-net/vault-sync/status")
        if r.status_code != 404:
            profiles.add("personal")
    except httpx.ConnectError:
        pass

    # Check BKC-only marker
    try:
        r = client.get("/web/submissions")
        if r.status_code != 404:
            profiles.add("bkc")
    except httpx.ConnectError:
        pass

    return profiles


# ---------------------------------------------------------------------------
# Core contract tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.core
class TestHealthContract:
    """The /health endpoint is the universal contract gate."""

    def test_health_response_shape(self, client):
        """GET /health must return {status: str, database: str}."""
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert "status" in body, "Missing 'status' key in /health response"
        assert isinstance(body["status"], str)
        assert "database" in body, "Missing 'database' key in /health response"

    def test_health_status_healthy(self, client):
        """A healthy server should report status 'healthy'."""
        r = client.get("/health")
        body = r.json()
        assert body["status"] == "healthy"


@pytest.mark.integration
@pytest.mark.core
class TestCoreEndpointsReachable:
    """Every core endpoint must be reachable (not 500/502/503)."""

    @pytest.mark.parametrize(
        "method,path",
        CORE_ENDPOINTS,
        ids=[f"{m} {p}" for m, p in CORE_ENDPOINTS],
    )
    def test_core_endpoint_registered(self, client, method, path):
        """Core endpoints must be registered (not 404) and not error (not 5xx).

        GET requests should return 2xx.
        POST with empty body should return 2xx or 422 (validation), proving
        the route exists and the handler runs.
        404 is a FAILURE — it means the route is missing from this deployment.

        Exception: path-parameter endpoints (e.g. /graph/neighborhood/{uri})
        may return 404 when the test entity doesn't exist — that's an
        application-level 404 (route IS registered), not a framework 404.
        """
        if method == "GET":
            r = client.get(path)
        else:
            # POST with empty body — expect 422 (validation) not 500
            r = client.request(method, path, json={})
        if r.status_code == 404:
            # Distinguish application 404 (entity not found) from framework
            # 404 (route not registered).  Application 404s include a JSON
            # body with "detail"; framework 404s return {"detail":"Not Found"}.
            try:
                body = r.json()
                detail = body.get("detail", "")
                app_404 = (
                    isinstance(detail, str)
                    and detail != "Not Found"
                    and detail != ""
                )
            except Exception:
                app_404 = False
            assert app_404, (
                f"{method} {path} returned 404 — route not registered"
            )
        assert r.status_code < 500, (
            f"{method} {path} returned {r.status_code}: {r.text[:200]}"
        )


@pytest.mark.integration
@pytest.mark.core
class TestPlannedCoreEndpoints:
    """Planned core endpoints — tracked so we know when they land."""

    @pytest.mark.parametrize(
        "method,path",
        PLANNED_CORE_ENDPOINTS,
        ids=[f"{m} {p}" for m, p in PLANNED_CORE_ENDPOINTS],
    )
    def test_planned_endpoint_status(self, client, method, path):
        """Planned endpoints: pass if registered, xfail if still 404.

        As endpoints are implemented and 404 disappears, move them
        from PLANNED_CORE_ENDPOINTS to CORE_ENDPOINTS.
        """
        if method == "GET":
            r = client.get(path)
        else:
            r = client.request(method, path, json={})
        if r.status_code == 404:
            pytest.xfail(f"{method} {path} not yet implemented (404)")
        assert r.status_code < 500, (
            f"{method} {path} returned {r.status_code}: {r.text[:200]}"
        )


@pytest.mark.integration
@pytest.mark.core
class TestEntityResolveContract:
    """POST /entity/resolve must return the canonical resolution shape."""

    def test_entity_resolve_response_shape(self, client):
        """Resolution response must include candidates array with expected fields."""
        payload = {
            "label": "Test Entity",
            "type_hint": "Concept",
        }
        r = client.post("/entity/resolve", json=payload)
        # 200 is expected
        assert r.status_code == 200, (
            f"Unexpected status {r.status_code}: {r.text[:200]}"
        )
        body = r.json()
        # Response shape: {candidates: [{name, uri, type, confidence, ...}], is_new: bool}
        assert "candidates" in body, "Missing 'candidates' key in resolve response"
        assert "is_new" in body, "Missing 'is_new' key in resolve response"
        if body["candidates"]:
            candidate = body["candidates"][0]
            required_keys = {"uri", "type", "confidence"}
            missing = required_keys - set(candidate.keys())
            assert not missing, f"Missing keys in candidate: {missing}"
            assert isinstance(candidate["confidence"], (int, float))

    def test_entity_resolve_without_label_returns_422(self, client):
        """Omitting required 'label' field should yield 422, not 500."""
        r = client.post("/entity/resolve", json={"type_hint": "Concept"})
        assert r.status_code == 422


@pytest.mark.integration
@pytest.mark.core
class TestErrorResponseShape:
    """Error responses must follow the {detail: str} convention."""

    def test_404_has_detail(self, client):
        """A route that doesn't exist should return {detail: ...}."""
        r = client.get("/nonexistent-route-for-contract-test")
        assert r.status_code == 404
        body = r.json()
        assert "detail" in body, "404 response missing 'detail' key"
        assert isinstance(body["detail"], str)

    def test_422_has_detail(self, client):
        """A validation error should surface a detail message."""
        # Send empty body to /entity/resolve — 'label' is required
        r = client.post("/entity/resolve", json={})
        assert r.status_code == 422
        body = r.json()
        assert "detail" in body, "422 response missing 'detail' key"


# ---------------------------------------------------------------------------
# Federation endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.federation
class TestFederationEndpointsReachable:
    """Federation endpoints must be reachable when the profile includes federation."""

    @pytest.mark.parametrize(
        "method,path",
        FEDERATION_ENDPOINTS,
        ids=[f"{m} {p}" for m, p in FEDERATION_ENDPOINTS],
    )
    def test_federation_endpoint_not_server_error(
        self, client, server_profile, method, path
    ):
        if "federation" not in server_profile:
            pytest.skip("Server does not include federation profile")
        if method == "GET":
            r = client.get(path)
        else:
            r = client.request(method, path, json={})
        assert r.status_code < 500, (
            f"{method} {path} returned {r.status_code}: {r.text[:200]}"
        )


# ---------------------------------------------------------------------------
# Personal-only endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.personal
class TestPersonalOnlyEndpoints:
    """Personal-only endpoints should be reachable on personal profile."""

    @pytest.mark.parametrize(
        "method,path",
        PERSONAL_ONLY,
        ids=[f"{m} {p}" for m, p in PERSONAL_ONLY],
    )
    def test_personal_endpoint_reachable(
        self, client, server_profile, method, path
    ):
        if "personal" not in server_profile:
            pytest.skip("Server does not include personal profile")
        if method == "GET":
            r = client.get(path)
        else:
            r = client.request(method, path, json={})
        assert r.status_code < 500, (
            f"{method} {path} returned {r.status_code}: {r.text[:200]}"
        )


# ---------------------------------------------------------------------------
# BKC-only endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.bkc
class TestBkcOnlyEndpoints:
    """BKC-only endpoints should be reachable on BKC profile."""

    @pytest.mark.parametrize(
        "method,path",
        BKC_ONLY,
        ids=[f"{m} {p}" for m, p in BKC_ONLY],
    )
    def test_bkc_endpoint_reachable(
        self, client, server_profile, method, path
    ):
        if "bkc" not in server_profile:
            pytest.skip("Server does not include bkc profile")
        if method == "GET":
            r = client.get(path)
        else:
            r = client.request(method, path, json={})
        assert r.status_code < 500, (
            f"{method} {path} returned {r.status_code}: {r.text[:200]}"
        )


@pytest.mark.integration
@pytest.mark.bkc
class TestBkcPlannedEndpoints:
    """BKC endpoints that exist as stubs (501) — tracked for future implementation."""

    @pytest.mark.parametrize(
        "method,path",
        BKC_PLANNED,
        ids=[f"{m} {p}" for m, p in BKC_PLANNED],
    )
    def test_bkc_planned_endpoint_status(
        self, client, server_profile, method, path
    ):
        if "bkc" not in server_profile:
            pytest.skip("Server does not include bkc profile")
        if method == "GET":
            r = client.get(path)
        else:
            r = client.request(method, path, json={})
        if r.status_code in (404, 501):
            pytest.xfail(f"{method} {path} not yet implemented ({r.status_code})")
        assert r.status_code < 500, (
            f"{method} {path} returned {r.status_code}: {r.text[:200]}"
        )


# ---------------------------------------------------------------------------
# Profile-specific exclusion tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.core
class TestProfileExclusion:
    """Endpoints outside the active profile should return 404."""

    def test_bkc_endpoints_disabled_on_personal(self, client, server_profile):
        """When running personal profile, BKC-only endpoints return 404."""
        if "bkc" in server_profile:
            pytest.skip("BKC endpoints are enabled on this server")
        for method, path in BKC_ONLY:
            if method == "GET":
                r = client.get(path)
            else:
                r = client.request(method, path, json={})
            assert r.status_code == 404, (
                f"Expected 404 for {method} {path} on non-BKC profile, "
                f"got {r.status_code}"
            )

    def test_personal_endpoints_disabled_on_bkc(self, client, server_profile):
        """When running BKC profile, personal-only endpoints return 404."""
        if "personal" in server_profile:
            pytest.skip("Personal endpoints are enabled on this server")
        for method, path in PERSONAL_ONLY:
            if method == "GET":
                r = client.get(path)
            else:
                r = client.request(method, path, json={})
            assert r.status_code in (404, 501), (
                f"Expected 404/501 for {method} {path} on non-personal profile, "
                f"got {r.status_code}"
            )
