"""Service-key auth and request validation (pure logic, offline).

These exercise src/core/service_auth.py, which is introduced on this branch, so
the whole module fails to import on the base commit and passes here. No network,
no database: only the standard library is needed.
"""
from __future__ import annotations

import os
import pathlib
import sys

import pytest

CORE = pathlib.Path(__file__).resolve().parents[1] / "src" / "core"
sys.path.insert(0, str(CORE))

import service_auth as sa  # noqa: E402

K = "a" * 40
K2 = "b" * 32


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in ("KOI_SERVICE_KEYS", "KOI_SERVICE_KEY", "KOI_AUTH_MODE_BRIDGE"):
        monkeypatch.delenv(var, raising=False)
    yield


# ----- key loading -----
def test_load_service_keys_drops_short_and_blank(monkeypatch):
    monkeypatch.setenv("KOI_SERVICE_KEYS", f"short, , {K} , {K2}")
    assert sa.load_service_keys() == [K, K2]


def test_load_service_keys_empty_when_unset():
    assert sa.load_service_keys() == []


def test_min_key_length_is_32():
    assert sa.MIN_KEY_LENGTH == 32
    assert sa.load_service_keys.__module__  # importable


# ----- header matching -----
def test_header_matches_exact_and_stripped():
    keys = [K, K2]
    assert sa.header_matches_a_key(K, keys) is True
    assert sa.header_matches_a_key(f"  {K}  ", keys) is True
    assert sa.header_matches_a_key(K2, keys) is True


def test_header_rejects_missing_short_wrong_and_no_keys():
    keys = [K]
    assert sa.header_matches_a_key(None, keys) is False
    assert sa.header_matches_a_key("", keys) is False
    assert sa.header_matches_a_key("   ", keys) is False
    assert sa.header_matches_a_key("a" * 31, keys) is False   # too short
    assert sa.header_matches_a_key("x" * 40, keys) is False   # wrong
    assert sa.header_matches_a_key(K, []) is False            # no keys configured


def test_header_non_ascii_does_not_raise():
    # A non-ASCII header must be rejected cleanly, never raise.
    assert sa.header_matches_a_key("ключ" * 10, [K]) is False


# ----- mode -----
def test_only_exact_warn_warns(monkeypatch):
    monkeypatch.setenv("KOI_AUTH_MODE_BRIDGE", "warn")
    assert sa.auth_mode("BRIDGE") == "warn"
    for val in ("Warn", "WARN", "warn ", "enforce", "", "true"):
        monkeypatch.setenv("KOI_AUTH_MODE_BRIDGE", val)
        assert sa.auth_mode("BRIDGE") == "enforce"


def test_mode_defaults_to_enforce_when_unset():
    assert sa.auth_mode("BRIDGE") == "enforce"


# ----- startup / preflight -----
def test_assert_keys_configured_raises_without_key():
    with pytest.raises(RuntimeError):
        sa.assert_keys_configured("BRIDGE")


def test_assert_keys_configured_ok_with_key(monkeypatch):
    monkeypatch.setenv("KOI_SERVICE_KEYS", K)
    sa.assert_keys_configured("BRIDGE")  # no raise


def test_preflight(monkeypatch):
    ok, _ = sa.preflight("BRIDGE")
    assert ok is False
    monkeypatch.setenv("KOI_SERVICE_KEYS", K)
    ok, _ = sa.preflight("BRIDGE")
    assert ok is True


# ----- identifier validation -----
def test_identifier_accepts_realistic_rids():
    assert sa.identifier_is_valid("koi:test-sensor/doc/1#chunk0") is True
    assert sa.identifier_is_valid("a" * 256) is True


@pytest.mark.parametrize("bad", [
    "", "a b", "a\tb", "a\nb", "a\x00b", "a\x7fb",
    'a"b', "a\\b", "a<b", "a>b", "a{b", "a}b", "a|b", "a^b", "a`b",
    "a" * 257, None, 123,
])
def test_identifier_rejects_forbidden(bad):
    assert sa.identifier_is_valid(bad) is False


# ----- timestamp / content hash -----
def test_timestamp_iso():
    assert sa.timestamp_is_valid("2026-11-05T00:00:00Z") is True
    assert sa.timestamp_is_valid("2026-11-05T00:00:00+00:00") is True
    assert sa.timestamp_is_valid("yesterday") is False
    assert sa.timestamp_is_valid("") is False
    assert sa.timestamp_is_valid(None) is False


def test_content_hash_token():
    # Format-lenient (bare hex, alg-prefixed, CID-like all pass) so a legitimate
    # producer hash is not rejected; escaping is the real injection guard.
    assert sa.content_hash_is_valid("a" * 64) is True
    assert sa.content_hash_is_valid("DEADBEEF" * 8) is True
    assert sa.content_hash_is_valid("sha256:" + "a" * 64) is True
    assert sa.content_hash_is_valid("bafybeigdyrabc123") is True
    # Rejected: injection / whitespace / control / empty / over-long.
    assert sa.content_hash_is_valid('a"b') is False
    assert sa.content_hash_is_valid("has space") is False
    assert sa.content_hash_is_valid("a\nb") is False
    assert sa.content_hash_is_valid("") is False
    assert sa.content_hash_is_valid("a" * 300) is False
    assert sa.content_hash_is_valid(None) is False


# ----- event validation -----
def _event(**over):
    ev = {
        "event_type": "NEW",
        "rid": "koi:s/1",
        "source_node": "koi:sensor",
        "timestamp": "2026-11-05T00:00:00Z",
        "bundle": {"rid": "koi:s/1", "manifest": {"rid": "koi:s/1", "content_hash": "a" * 64}},
    }
    ev.update(over)
    return ev


def test_valid_event_has_no_problems():
    assert sa.validate_koi_event(_event()) == []


def test_forget_without_bundle_is_valid():
    assert sa.validate_koi_event({
        "event_type": "FORGET", "rid": "koi:s/1",
        "source_node": "koi:sensor", "timestamp": "2026-11-05T00:00:00Z",
    }) == []


def test_disallowed_event_type_flagged():
    assert any("event_type" in p for p in sa.validate_koi_event(_event(event_type="DELETE")))
    assert any("event_type" in p for p in sa.validate_koi_event(_event(event_type="new")))  # case-exact


def test_mismatched_rids_flagged():
    ev = _event(bundle={"rid": "koi:other", "manifest": {"rid": "koi:s/1", "content_hash": "a" * 64}})
    assert any("not all equal" in p for p in sa.validate_koi_event(ev))


def test_injection_rid_flagged():
    ev = _event(rid='koi:s/1" }} INSERT', bundle={"rid": 'koi:s/1" }} INSERT',
                "manifest": {"rid": 'koi:s/1" }} INSERT', "content_hash": "a" * 64}})
    assert any("identifier" in p for p in sa.validate_koi_event(ev))


# ----- privacy predicate -----
def test_bundle_is_private():
    assert sa.bundle_is_private({"manifest": {"metadata": {"is_private": True}}}) is True
    assert sa.bundle_is_private({"manifest": {"metadata": {"access_source": "octo"}}}) is True
    assert sa.bundle_is_private({"contents": {"metadata": {"is_private": True}}}) is True
    assert sa.bundle_is_private({"contents": {"document": {"metadata": {"is_private": True}}}}) is True


def test_bundle_public_or_missing():
    assert sa.bundle_is_private({"manifest": {"metadata": {"is_private": False}}}) is False
    assert sa.bundle_is_private({"manifest": {"metadata": {}}}) is False
    assert sa.bundle_is_private({}) is False
    assert sa.bundle_is_private(None) is False


# ----- dsn mask -----
def test_mask_dsn_drops_credentials():
    assert sa.mask_dsn("postgresql://user:secret@host:5433/db") == "postgresql://host:5433"
    assert sa.mask_dsn("postgresql://user:secret@host/db") == "postgresql://host"
    assert sa.mask_dsn("") == "(unset)"
    assert sa.mask_dsn(None) == "(unset)"
    masked = sa.mask_dsn("postgresql://u:p@h:5/db")
    assert "secret" not in masked and "p@" not in masked
