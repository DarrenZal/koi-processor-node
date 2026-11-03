#!/usr/bin/env python3
"""Canon-dependency reconciliation — the correction watcher's core pass.

Idempotent full reconciliation (anti-join semantics via ON CONFLICT DO NOTHING),
never event-cursor-only. Traversal is strictly by RID edges in
claim_supersession_events — NEVER by source_document/c_id business keys (a
correction drill should include a negative control that catches exactly that
regression).

Stages: traverse -> impacts -> cases (one open case per assertion) -> attach ->
task projection -> watch status. Stdlib only; DB via psql subprocess.

Task modes:
  --task-mode api  POST /tasks/ingest (production; existing endpoint + federation)
  --task-mode db   direct upsert into task_registry (isolated drill DB only)
  --task-mode off  skip projection (dev)

Rehearse on an isolated drill database (--task-mode db) before running it against
a live personal_koi.
"""
import argparse, datetime, json, subprocess, sys, urllib.request

RECONCILE_SQL = """
BEGIN;
-- 1) multi-hop traversal by RID edges only
WITH RECURSIVE chain AS (
  SELECT d.dependency_id, e.event_id, e.new_rid
  FROM canon_dependencies d
  JOIN claim_supersession_events e ON e.old_rid = d.claim_rid
  UNION
  SELECT c.dependency_id, e.event_id, e.new_rid
  FROM chain c
  JOIN claim_supersession_events e ON e.old_rid = c.new_rid
)
INSERT INTO canon_dependency_impacts (dependency_id, causal_event_id)
SELECT dependency_id, event_id FROM chain
ON CONFLICT (dependency_id, causal_event_id) DO NOTHING;

-- 2) one open case per assertion that has uncased impacts
INSERT INTO canon_review_cases (assertion_slug)
SELECT DISTINCT d.assertion_slug
FROM canon_dependency_impacts i
JOIN canon_dependencies d USING (dependency_id)
WHERE i.case_id IS NULL
ON CONFLICT (assertion_slug) WHERE status = 'open' DO NOTHING;

-- 3) attach uncased impacts to the open case for their assertion
UPDATE canon_dependency_impacts i
SET case_id = c.case_id
FROM canon_dependencies d, canon_review_cases c
WHERE i.dependency_id = d.dependency_id
  AND c.assertion_slug = d.assertion_slug
  AND c.status = 'open'
  AND i.case_id IS NULL;
COMMIT;
"""

OPEN_CASES_SQL = (
    "SELECT c.case_id, c.assertion_slug, COUNT(i.impact_id) "
    "FROM canon_review_cases c "
    "LEFT JOIN canon_dependency_impacts i ON i.case_id = c.case_id "
    "WHERE c.status = 'open' GROUP BY 1, 2 ORDER BY 1;"
)


def psql(db, sql, capture=True):
    r = subprocess.run(["psql", "-d", db, "-v", "ON_ERROR_STOP=1", "-tA", "-F", "\t"],
                       input=sql, text=True, capture_output=True)
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        raise SystemExit(r.returncode)
    return r.stdout.strip()


def q(v):
    return "'" + str(v).replace("'", "''") + "'"


def project_task_db(db, key, title, context):
    psql(db,
         "INSERT INTO task_registry (task_key, title, status, source_type, context) VALUES ("
         + ", ".join([q(key), q(title), "'open'", "'canon-review'", q(context)])
         + ") ON CONFLICT (task_key) DO NOTHING;")


def project_task_api(endpoint, key, title, context):
    payload = json.dumps({
        "taskKey": key, "title": title, "sourceType": "canon-review",
        "context": context,
    }).encode()
    req = urllib.request.Request(endpoint.rstrip("/") + "/tasks/ingest",
                                 data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--task-mode", choices=["api", "db", "off"], default="off")
    ap.add_argument("--task-endpoint", default="http://localhost:8351")
    args = ap.parse_args()

    psql(args.db, RECONCILE_SQL)

    rows = psql(args.db, OPEN_CASES_SQL)
    now = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    open_cases = 0
    for row in (rows.splitlines() if rows else []):
        case_id, slug, n_impacts = row.split("\t")
        open_cases += 1
        key = f"canon-review::{slug}::case-{case_id}"
        title = f"Canon review: {slug} — {n_impacts} upstream change(s)"
        context = (f"canon-watch last_scan={now} case={case_id} "
                   f"impacts={n_impacts} watcher=reconcile_canon.py")
        if args.task_mode == "db":
            project_task_db(args.db, key, title, context)
        elif args.task_mode == "api":
            project_task_api(args.task_endpoint, key, title, context)

    last_event = psql(args.db, "SELECT COALESCE(MAX(event_id),0) FROM claim_supersession_events;")
    psql(args.db,
         "INSERT INTO canon_watch_status (id, last_scan_at, last_event_id_seen, open_cases, task_projection) "
         f"VALUES (1, NOW(), {last_event}, {open_cases}, {q(args.task_mode)}) "
         "ON CONFLICT (id) DO UPDATE SET last_scan_at = EXCLUDED.last_scan_at, "
         "last_event_id_seen = EXCLUDED.last_event_id_seen, "
         "open_cases = EXCLUDED.open_cases, task_projection = EXCLUDED.task_projection;")

    print(f"reconciled: open_cases={open_cases} last_event={last_event} task_mode={args.task_mode}")


if __name__ == "__main__":
    main()
