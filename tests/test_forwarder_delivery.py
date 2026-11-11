"""Forwarder delivery classification and dead-letter handling (offline).

Exercises scripts/forwarder_delivery.py, new on this branch (so the import
fails on base and passes here). It lives in scripts/ beside the forwarder that
uses it (like koi_envelope.py), so the forwarder's bare import resolves at
runtime. No network and no signing key needed.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import forwarder_delivery as fd  # noqa: E402

D = fd.Disposition


def test_success_confirms():
    assert fd.classify_delivery(200, True) is D.CONFIRM


def test_200_without_success_retries():
    # A 200 whose body says success=false is transient (e.g. a DB blip), so keep it.
    assert fd.classify_delivery(200, False) is D.RETRY
    assert fd.classify_delivery(200, None) is D.RETRY


@pytest.mark.parametrize("code", [400, 422])
def test_client_errors_dead_letter(code):
    assert fd.classify_delivery(code, None) is D.DEAD_LETTER


@pytest.mark.parametrize("code", [401, 403, 500, 502, 503, 599])
def test_auth_and_server_errors_retry(code):
    assert fd.classify_delivery(code, None) is D.RETRY


def test_connection_error_retries():
    assert fd.classify_delivery(None, None) is D.RETRY


def test_backoff_is_capped_exponential():
    assert fd.backoff_delay(0) == 1.0
    assert fd.backoff_delay(1) == 2.0
    assert fd.backoff_delay(3) == 8.0
    assert fd.backoff_delay(100) == 60.0
    assert fd.backoff_delay(-5) == 1.0


def test_dead_letter_writes_jsonl(tmp_path):
    path = tmp_path / "nested" / "dl.jsonl"
    returned = fd.write_dead_letter({"rid": "koi:x", "n": 1}, "eid-1", "HTTP 400", path=str(path))
    assert returned == str(path)
    rec = json.loads(path.read_text().strip())
    assert rec["event_id"] == "eid-1"
    assert rec["reason"] == "HTTP 400"
    assert rec["event"]["rid"] == "koi:x"
    assert "ts" in rec


def test_dead_letter_appends(tmp_path):
    path = tmp_path / "dl.jsonl"
    fd.write_dead_letter({"rid": "a"}, "1", "x", path=str(path))
    fd.write_dead_letter({"rid": "b"}, "2", "y", path=str(path))
    assert len(path.read_text().strip().splitlines()) == 2


def test_dead_letter_survives_unserialisable(tmp_path):
    class Weird:
        pass

    path = tmp_path / "dl.jsonl"
    fd.write_dead_letter(Weird(), None, "weird", path=str(path))
    # The line is still valid JSON even though the event was not serialisable.
    rec = json.loads(path.read_text().strip())
    assert rec["reason"] == "weird"


def test_max_retries_default_and_env(monkeypatch):
    monkeypatch.delenv("KOI_FORWARDER_MAX_RETRIES", raising=False)
    assert fd.max_retries() == 8
    monkeypatch.setenv("KOI_FORWARDER_MAX_RETRIES", "3")
    assert fd.max_retries() == 3
    monkeypatch.setenv("KOI_FORWARDER_MAX_RETRIES", "0")
    assert fd.max_retries() == 0
    monkeypatch.setenv("KOI_FORWARDER_MAX_RETRIES", "garbage")
    assert fd.max_retries() == 8
    monkeypatch.setenv("KOI_FORWARDER_MAX_RETRIES", "-4")
    assert fd.max_retries() == 0


def test_dead_letter_file_is_not_group_or_world_accessible(tmp_path):
    import stat

    path = tmp_path / "sub" / "dl.jsonl"
    fd.write_dead_letter({"rid": "koi:x"}, "1", "reason", path=str(path))
    file_mode = stat.S_IMODE(path.stat().st_mode)
    dir_mode = stat.S_IMODE((tmp_path / "sub").stat().st_mode)
    # Records may hold document contents: no group/other access to the file or dir.
    assert file_mode & 0o077 == 0, oct(file_mode)
    assert dir_mode & 0o077 == 0, oct(dir_mode)


def test_dead_letter_refuses_group_or_world_writable_dir(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    os.chmod(shared, 0o777)
    with pytest.raises(PermissionError):
        fd.write_dead_letter({"rid": "x"}, "1", "r", path=str(shared / "dl.jsonl"))


def test_dead_letter_refuses_symlinked_target(tmp_path):
    priv = tmp_path / "priv"
    priv.mkdir()
    os.chmod(priv, 0o700)
    outside = tmp_path / "outside.txt"
    outside.write_text("")
    link = priv / "dl.jsonl"
    os.symlink(str(outside), str(link))
    with pytest.raises(OSError):  # O_NOFOLLOW -> ELOOP
        fd.write_dead_letter({"rid": "x"}, "1", "r", path=str(link))


def test_default_dead_letter_path_env(monkeypatch):
    monkeypatch.setenv("KOI_FORWARDER_DEAD_LETTER", "/tmp/custom-dl.jsonl")
    assert fd.dead_letter_path() == "/tmp/custom-dl.jsonl"
    monkeypatch.delenv("KOI_FORWARDER_DEAD_LETTER", raising=False)
    assert fd.dead_letter_path().endswith(".jsonl")
