"""Tests for api.crawl_canonicalize.canonicalize_start_url (AC23)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.crawl_canonicalize import StartUrlError, canonicalize_start_url


def test_lowercases_scheme_and_host():
    assert canonicalize_start_url("HTTPS://Peninsulastreams.CA/") == "https://peninsulastreams.ca/"


def test_strips_default_ports():
    assert canonicalize_start_url("https://example.org:443/") == "https://example.org/"
    assert canonicalize_start_url("http://example.org:80/") == "http://example.org/"


def test_keeps_non_default_port():
    assert canonicalize_start_url("https://example.org:8443/path") == "https://example.org:8443/path"


def test_strips_fragment():
    assert canonicalize_start_url("https://example.org/page#section") == "https://example.org/page"


def test_empty_path_becomes_slash():
    assert canonicalize_start_url("https://example.org") == "https://example.org/"


def test_preserves_trailing_slash_distinction():
    assert canonicalize_start_url("https://example.org/about") == "https://example.org/about"
    assert canonicalize_start_url("https://example.org/about/") == "https://example.org/about/"


def test_preserves_query_string():
    assert canonicalize_start_url("https://example.org/?a=1&b=2") == "https://example.org/?a=1&b=2"


def test_does_not_strip_www():
    assert canonicalize_start_url("https://www.example.org/") == "https://www.example.org/"
    # and differs from bare-host version
    assert canonicalize_start_url("https://www.example.org/") != canonicalize_start_url(
        "https://example.org/"
    )


def test_rejects_non_http_scheme():
    with pytest.raises(StartUrlError):
        canonicalize_start_url("ftp://example.org/")


def test_rejects_empty_input():
    with pytest.raises(StartUrlError):
        canonicalize_start_url("")
    with pytest.raises(StartUrlError):
        canonicalize_start_url("   ")


def test_rejects_missing_host():
    with pytest.raises(StartUrlError):
        canonicalize_start_url("https:///path-only")
