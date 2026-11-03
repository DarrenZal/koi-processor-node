"""Drill tests for the canon-dependency watcher (PREREG-correction-drill-001).

Isolated-arm bars B8-B13 + idempotency (B4 shape) + traversal shape (B1).
Runs against KOI_TEST_DATABASE_URL, which must carry the drill schema. Every test
TRUNCATEs the canon tables, task_registry and claims there, so the module also needs
KOI_TEST_CANON_DRILL=1:

    KOI_TEST_DATABASE_URL=postgresql://.../<isolated drill db, e.g. koi_test_drill> \
        KOI_TEST_CANON_DRILL=1 pytest tests/test_canon_watch_drill.py

Skipped otherwise, so the repo suite runs anywhere.
"""
import os
import subprocess
from pathlib import Path

import pytest

from tests import koi_test_targets as targets

pytestmark = [
    targets.requires_database,
    pytest.mark.skipif(os.environ.get("KOI_TEST_CANON_DRILL") != "1",
                       reason="KOI_TEST_CANON_DRILL=1 not set (truncates canon, task and claim tables)"),
]

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts" / "membrane"


def sql(query: str) -> str:
    r = subprocess.run(
        ["psql", "-d", targets.database_url(), "-v", "ON_ERROR_STOP=1", "-tA", "-F", "\t"],
        input=query, text=True, capture_output=True,
    )
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


def reconcile(task_mode: str = "db"):
    r = subprocess.run(
        ["python3", str(SCRIPTS / "reconcile_canon.py"), "--db", targets.database_url(),
         "--task-mode", task_mode],
        capture_output=True, text=True,
    )
    assert r.returncode == 0, r.stderr
    return r.stdout


def add_event(old: str, new, kind: str = "unclassified", origin: str = "fixture"):
    new_sql = f"'{new}'" if new else "NULL"
    new_part = new if new else ""
    sql(
        "INSERT INTO claim_supersession_events (source_event_key, old_rid, new_rid, kind, actor) "
        f"VALUES (md5('{old}->{new_part}|{origin}'), '{old}', {new_sql}, '{kind}', 'test') "
        "ON CONFLICT (source_event_key) DO NOTHING;"
    )


def add_dep(slug: str, rid: str):
    sql(
        "INSERT INTO canon_dependencies (assertion_slug, claim_rid, repo, note_path) "
        f"VALUES ('{slug}', '{rid}', 'test-repo', 'test/note.md') "
        "ON CONFLICT (assertion_slug, claim_rid) DO NOTHING;"
    )


@pytest.fixture(autouse=True)
def clean_db():
    sql(
        "TRUNCATE canon_dependency_impacts, canon_review_cases, canon_dependencies, "
        "claim_supersession_events, canon_watch_status, task_registry, claims "
        "RESTART IDENTITY CASCADE;"
    )
    yield


def counts():
    return {
        "events": int(sql("SELECT COUNT(*) FROM claim_supersession_events;")),
        "impacts": int(sql("SELECT COUNT(*) FROM canon_dependency_impacts;")),
        "cases": int(sql("SELECT COUNT(*) FROM canon_review_cases;")),
        "open_cases": int(sql("SELECT COUNT(*) FROM canon_review_cases WHERE status='open';")),
        "tasks": int(sql("SELECT COUNT(*) FROM task_registry WHERE source_type='canon-review';")),
    }


def test_traversal_multi_hop_two_impacts():
    # B1 shape: dependency on chain ROOT sees both supersessions above it.
    add_event("rid:root", "rid:mid")
    add_event("rid:mid", "rid:head")
    add_dep("test:note:A1", "rid:root")
    reconcile()
    c = counts()
    assert c["impacts"] == 2 and c["open_cases"] == 1 and c["tasks"] == 1


def test_negative_control_head_and_decoy():
    # B8: dependency on the chain HEAD (0 supersessions above) fires nothing,
    # even though a decoy claim shares source_document/c_id with the family —
    # business-key traversal would fire here; RID-edge traversal must not.
    sql("INSERT INTO claims (claim_rid, claimant_uri, statement, source_document, metadata) VALUES "
        "('rid:head', 'urn:test', 'S', 'note.family', '{\"c_id\": \"C5\"}'),"
        "('rid:decoy', 'urn:test', 'S-decoy', 'note.family', '{\"c_id\": \"C5\"}');")
    add_event("rid:root", "rid:mid")
    add_event("rid:mid", "rid:head")
    add_dep("test:note:HEADDEP", "rid:head")
    add_dep("test:note:DECOYDEP", "rid:decoy")
    reconcile()
    c = counts()
    assert c["impacts"] == 0 and c["cases"] == 0 and c["tasks"] == 0


def test_per_assertion_case_counts():
    # B2/B12 shape in a shared DB: each assertion coalesces separately.
    add_event("rid:a", "rid:a2")
    add_event("rid:b", "rid:b2")
    add_dep("test:note:A", "rid:a")
    add_dep("test:note:B", "rid:b")
    reconcile()
    c = counts()
    assert c["open_cases"] == 2 and c["tasks"] == 2
    assert sql("SELECT COUNT(DISTINCT assertion_slug) FROM canon_review_cases;") == "2"


def test_idempotent_reruns():
    # B4: 10 re-runs add zero rows anywhere.
    add_event("rid:root", "rid:mid")
    add_event("rid:mid", "rid:head")
    add_dep("test:note:A1", "rid:root")
    reconcile()
    before = counts()
    for _ in range(10):
        reconcile()
    assert counts() == before


def test_replay_same_event_dedup():
    # B11: the same event replayed 10x -> 1 event row, 1 impact.
    for _ in range(10):
        add_event("rid:p", "rid:p2", origin="same-origin")
    add_dep("test:note:P", "rid:p")
    reconcile()
    c = counts()
    assert c["events"] == 1 and c["impacts"] == 1


def test_ten_distinct_changes():
    # B12: 10 distinct upstream changes -> 10 impacts, 1 case, 1 task.
    for i in range(10):
        add_event(f"rid:v{i}", f"rid:v{i+1}", origin=f"step{i}")
    add_dep("test:note:V", "rid:v0")
    reconcile()
    c = counts()
    assert c["impacts"] == 10 and c["open_cases"] == 1 and c["tasks"] == 1


def test_semantic_correction_case():
    # B9: P superseded by not-P (kind=correction) -> impact + case; the case can
    # record resolution update_canon.
    add_event("rid:P", "rid:notP", kind="correction")
    add_dep("test:note:SEM", "rid:P")
    reconcile()
    c = counts()
    assert c["impacts"] == 1 and c["open_cases"] == 1
    sql("UPDATE canon_review_cases SET status='resolved', resolution='update_canon', "
        "resolved_at=NOW() WHERE assertion_slug='test:note:SEM';")
    assert sql("SELECT resolution FROM canon_review_cases WHERE assertion_slug='test:note:SEM';") == "update_canon"


def test_rejection_immutable():
    # B10: a rejection event yields an impact; a later approval changes the
    # case's resolution while the rejection event row persists untouched.
    add_event("rid:R", None, kind="rejection")
    add_dep("test:note:REJ", "rid:R")
    reconcile()
    assert counts()["impacts"] == 1
    sql("UPDATE canon_review_cases SET status='resolved', resolution='retain_with_rationale', "
        "resolved_at=NOW() WHERE assertion_slug='test:note:REJ';")
    assert int(sql("SELECT COUNT(*) FROM claim_supersession_events WHERE kind='rejection';")) == 1
    assert sql("SELECT kind FROM claim_supersession_events WHERE old_rid='rid:R';") == "rejection"


def test_containment_new_case_new_task():
    # B13: resolve the case, inject one further distinct change -> NEW case and
    # NEW task; the completed task is untouched.
    add_event("rid:P", "rid:P2", kind="correction")
    add_dep("test:note:CONT", "rid:P")
    reconcile()
    first_case = sql("SELECT case_id FROM canon_review_cases WHERE status='open';")
    first_key = sql("SELECT task_key FROM task_registry;")
    sql(f"UPDATE canon_review_cases SET status='resolved', resolution='update_canon', "
        f"resolved_at=NOW() WHERE case_id={first_case};")
    sql(f"UPDATE task_registry SET status='done' WHERE task_key='{first_key}';")
    add_event("rid:P2", "rid:P3", kind="correction")
    reconcile()
    c = counts()
    assert c["cases"] == 2 and c["open_cases"] == 1
    new_case = sql("SELECT case_id FROM canon_review_cases WHERE status='open';")
    assert new_case != first_case
    assert c["tasks"] == 2
    assert sql(f"SELECT status FROM task_registry WHERE task_key='{first_key}';") == "done"
    new_key = sql(f"SELECT task_key FROM task_registry WHERE task_key != '{first_key}';")
    assert f"case-{new_case}" in new_key and f"case-{first_case}" in first_key
