from __future__ import annotations

from pathlib import Path
import subprocess
import uuid

import psycopg2
import pytest

from tests import koi_test_targets as targets
from tests.live_write_cleanup import append_record, cleanup, validate_dsn


def test_cleanup_removes_exact_recorded_entity_and_edges(tmp_path):
    TEST_DSN = targets.database_url()
    run_id = uuid.uuid4().hex
    subject_uri = f"orn:test:cleanup-subject-{run_id}"
    object_uri = f"orn:test:cleanup-object-{run_id}"
    predicate = f"test_cleanup_{run_id}"
    manifest = tmp_path / "manifest.jsonl"
    with psycopg2.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO allowed_predicates (predicate, description) VALUES (%s, 'cleanup test')",
            (predicate,),
        )
        for uri, role in ((subject_uri, "subject"), (object_uri, "object")):
            cur.execute(
                """
                INSERT INTO entity_registry
                    (fuseki_uri, entity_text, entity_type, normalized_text, source)
                VALUES (%s, %s, 'Concept', %s, 'test')
                """,
                (uri, f"Cleanup {role} {run_id}", f"cleanup {role} {run_id}"),
            )
        cur.execute(
            """
            INSERT INTO entity_relationships
                (subject_uri, predicate, object_uri, confidence, source)
            VALUES (%s, %s, %s, 1.0, 'test')
            """,
            (subject_uri, predicate, object_uri),
        )
    append_record(manifest, "entity_uri", subject_uri)
    append_record(manifest, "entity_uri", object_uri)
    report = cleanup(TEST_DSN, manifest, run_id)
    assert report["deleted"]["entity_registry"] == 2
    with psycopg2.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM entity_registry WHERE fuseki_uri IN (%s, %s)",
            (subject_uri, object_uri),
        )
        assert cur.fetchone()[0] == 0
        cur.execute("DELETE FROM allowed_predicates WHERE predicate = %s", (predicate,))
        cur.execute(
            """
            SELECT count(*) FROM entity_relationships
            WHERE subject_uri IN (%s, %s) OR object_uri IN (%s, %s)
            """,
            (subject_uri, object_uri, subject_uri, object_uri),
        )
        assert cur.fetchone()[0] == 0


def test_recovery_scan_still_finds_unmanifested_recent_row(tmp_path):
    """The federation-table recovery scan must still catch a row whose
    identifier was never recorded to the manifest (the "HTTP response lost
    after commit" case it exists for) — proving the 2026-08-24 recency-bound
    fast path (FEDERATION_RECENCY_COLUMNS) didn't narrow that guarantee for
    the case it actually protects: a row created during THIS run."""
    TEST_DSN = targets.database_url()
    run_id = uuid.uuid4().hex
    marker_url = f"https://example.com/cleanup-recovery-test-{run_id}"
    with psycopg2.connect(TEST_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO web_submissions (url, rid, domain, status)
            VALUES (%s, %s, 'example.com', 'pending')
            """,
            (marker_url, f"orn:test:web-submission-{run_id}"),
        )
    manifest_path = tmp_path / f"cleanup-recovery-manifest-{run_id}.jsonl"
    manifest_path.touch()  # no append_record call — identifier was "lost"
    try:
        report = cleanup(TEST_DSN, manifest_path, run_id)
        assert report["deleted"].get("web_submissions", 0) == 1
        with psycopg2.connect(TEST_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM web_submissions WHERE url = %s", (marker_url,)
            )
            assert cur.fetchone()[0] == 0
    finally:
        manifest_path.unlink(missing_ok=True)


def test_shell_guard_refuses_writes_without_test_targets():
    guard = Path(__file__).with_name("live_write_shell_guard.sh")
    result = subprocess.run(
        ["bash", "-c", f'source "{guard}"; live_write_begin'],
        text=True,
        capture_output=True,
        env={},
    )
    assert result.returncode == 2
    assert "KOI_TEST_API_URL" in result.stderr and "KOI_TEST_DATABASE_URL" in result.stderr


@pytest.mark.parametrize("dsn", [
    "postgresql://koi@127.0.0.1:55999/personal_koi",
    "postgresql://koi@127.0.0.1:55999/koi_scratch",
    "",
])
def test_cleanup_refuses_a_live_looking_database(dsn, monkeypatch):
    monkeypatch.delenv(targets.ALLOW_LIVE_VAR, raising=False)
    with pytest.raises(RuntimeError):
        validate_dsn(dsn)
    assert validate_dsn("postgresql://koi@127.0.0.1:55999/koi_test_x") == "koi_test_x"
