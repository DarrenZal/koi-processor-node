"""Static census of the PR2 hardening (offline, no imports of the services).

Reads the source of the bridge, forwarder and provenance module and asserts the
A7 changes are present and the old insecure patterns are gone. Comments are
stripped first so an explanatory comment cannot satisfy or trip a check. These
fail on the base commit and pass on the branch.
"""
from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
BRIDGE = REPO / "src" / "core" / "koi_event_bridge_v2.py"
FORWARDER = REPO / "scripts" / "coordinator_to_eventbridge_forwarder.py"
PROVENANCE = REPO / "src" / "core" / "provenance_to_rdf.py"


def _code(path: pathlib.Path) -> str:
    """Return the source with ``#`` line comments removed.

    Triple-quoted blocks are kept: the provenance module builds its SPARQL in
    f-strings, so stripping them would hide real code. None of the tokens these
    tests look for appear in a docstring, so keeping docstrings is safe.
    """
    text = path.read_text(encoding="utf-8")
    out = []
    for line in text.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line)
    return "\n".join(out)


# ---- bridge ----
def test_bridge_imports_service_auth():
    assert "import service_auth" in _code(BRIDGE)


def test_bridge_removes_wildcard_cors():
    code = _code(BRIDGE)
    assert "CORSMiddleware" not in code
    assert "add_middleware" not in code


def test_bridge_disables_docs():
    assert "docs_url=None" in _code(BRIDGE)


def test_bridge_protects_write_and_stats_routes():
    code = _code(BRIDGE)
    # Both /process-koi-event and /stats carry the auth dependency.
    assert code.count("dependencies=[Depends(bridge_auth)]") >= 2


def test_bridge_refuses_to_start_without_key():
    assert 'assert_keys_configured("BRIDGE")' in _code(BRIDGE)


def test_bridge_masks_dsn_in_logs():
    code = _code(BRIDGE)
    assert "mask_dsn(DB_URL)" in code
    assert 'f"Database: {DB_URL}"' not in code


def test_bridge_binds_from_env_not_wildcard():
    code = _code(BRIDGE)
    assert "KOI_BIND_HOST" in code
    assert "host=BIND_HOST" in code
    assert '"0.0.0.0"' not in code and "'0.0.0.0'" not in code


def test_bridge_bge_timeout_configurable_and_no_retry_on_overload():
    code = _code(BRIDGE)
    assert "KOI_BGE_TIMEOUT" in code
    assert "timeout=BGE_TIMEOUT" in code
    assert "timeout=30.0" not in code
    assert "(503, 413)" in code


def test_bridge_validates_events_and_skips_private_provenance():
    code = _code(BRIDGE)
    assert "validate_koi_event" in code
    assert "bundle_is_private" in code


def test_bridge_privacy_is_sticky_and_three_location():
    code = _code(BRIDGE)
    # The row write marks private via the full 3-location predicate.
    assert "service_auth.bundle_is_private(event.bundle.dict())" in code
    # The Fuseki skip also consults existing DB privacy (sticky-OR), so a
    # flag-less update of a previously-private doc is not exposed.
    assert "SELECT is_private, access_source FROM koi_memories WHERE rid" in code


def test_forwarder_bounds_retries():
    code = _code(FORWARDER)
    # The hold-and-retry loop is bounded, then dead-letters, so a persistent
    # failure cannot wedge the pipeline forever.
    assert "max_retries" in code
    assert "retry attempts exhausted" in code


def test_bridge_has_preflight():
    assert '"--preflight"' in _code(BRIDGE)


# ---- forwarder ----
def test_forwarder_sends_service_key_header():
    code = _code(FORWARDER)
    assert "X-KOI-Service-Key" in code
    assert "SERVICE_KEY" in code


def test_forwarder_uses_delivery_helpers():
    code = _code(FORWARDER)
    for name in ("classify_delivery", "write_dead_letter", "backoff_delay"):
        assert name in code, name
    # The old fire-and-forget confirm-only flow is gone.
    assert "logger.critical" in code


# ---- provenance ----
def test_provenance_has_iri_escaper_and_wires_escaping():
    code = _code(PROVENANCE)
    assert "def _escape_iri" in code
    # The RID now goes through both the IRI encoder and the literal escaper
    # (on the base commit the RID was interpolated raw into the literal).
    assert "self._escape_literal(rid)" in code
    assert "self._escape_iri(rid)" in code
    assert "self._escape_literal(event_type)" in code
