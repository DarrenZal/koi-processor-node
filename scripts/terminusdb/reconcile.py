"""
Reconciliation tool: detect and repair drift between PostgreSQL and TerminusDB.

Usage:
    python -m scripts.terminusdb.reconcile              # Report only
    python -m scripts.terminusdb.reconcile --repair      # Re-enqueue missing items
    python -m scripts.terminusdb.reconcile --json        # JSON output

Nightly job or on-demand.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

# Add project root for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.terminusdb.schema import compute_assertion_hash
from api.terminusdb_adapter import TerminusDBAdapter

logger = logging.getLogger(__name__)


def _load_env():
    env_path = PROJECT_ROOT / "config" / "personal.env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())


def get_pg_connection():
    url = os.environ.get("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
    return psycopg2.connect(url)


def get_adapter() -> TerminusDBAdapter:
    return TerminusDBAdapter(
        url=os.getenv("TERMINUSDB_URL", "http://127.0.0.1:6363/"),
        db_name=os.getenv("TERMINUSDB_DB", "koi_knowledge_graph"),
        team=os.getenv("TERMINUSDB_TEAM", "admin"),
        key=os.getenv("TERMINUSDB_KEY", "root"),
    )


# --------------------------------------------------------------------------
# Entity diff
# --------------------------------------------------------------------------

def get_pg_entity_rids(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT fuseki_uri FROM entity_registry ORDER BY fuseki_uri")
        return {row[0] for row in cur.fetchall()}


def get_tdb_entity_rids(adapter: TerminusDBAdapter) -> set[str]:
    try:
        docs = list(adapter.client.query_document({"@type": "Entity"}))
    except Exception:
        docs = [d for d in adapter.client.get_all_documents()
                if d.get("@type", "").endswith("Entity")]
    return {d.get("rid", d.get("fuseki_uri", "")) for d in docs}


# --------------------------------------------------------------------------
# Assertion diff
# --------------------------------------------------------------------------

def get_pg_assertion_hashes(conn) -> set[str]:
    """Recompute assertion hashes from entity_relationships."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT subject_uri, predicate, object_uri, confidence,
                   source, source_rid, source_field, raw_value
            FROM entity_relationships
            ORDER BY id
        """)
        rows = cur.fetchall()

    hashes = set()
    for rel in rows:
        h = compute_assertion_hash(
            subject_uri=rel["subject_uri"],
            predicate=rel["predicate"],
            object_kind="entity",
            object_uri=rel["object_uri"],
            literal_value="",
            literal_datatype="",
            literal_lang="",
            source=rel.get("source") or "personal-vault",
            source_rid=rel.get("source_rid") or "",
            source_field=rel.get("source_field") or "",
            asserted_by="darren-personal",
        )
        hashes.add(h)
    return hashes


def get_tdb_assertion_hashes(adapter: TerminusDBAdapter) -> set[str]:
    try:
        docs = list(adapter.client.query_document({"@type": "Assertion"}))
    except Exception:
        docs = [d for d in adapter.client.get_all_documents()
                if d.get("@type", "").endswith("Assertion")]
    return {d.get("assertion_hash", "") for d in docs if d.get("assertion_hash")}


# --------------------------------------------------------------------------
# Repair: re-enqueue pg_only items to outbox
# --------------------------------------------------------------------------

def repair_entities(conn_pg, pg_only: set[str]):
    """Re-enqueue missing entities to outbox."""
    if not pg_only:
        return 0

    with conn_pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        placeholders = ",".join(["%s"] * len(pg_only))
        cur.execute(f"""
            SELECT fuseki_uri, entity_text, entity_type, normalized_text,
                   phonetic_code, aliases, created_at, source, first_seen_rid
            FROM entity_registry
            WHERE fuseki_uri IN ({placeholders})
        """, list(pg_only))
        rows = cur.fetchall()

    enqueued = 0
    with conn_pg.cursor() as cur:
        for r in rows:
            rid = r["fuseki_uri"]
            payload = json.dumps({
                "fuseki_uri": rid,
                "entity_text": r["entity_text"] or "",
                "entity_type": r["entity_type"] or "",
                "normalized_text": r["normalized_text"] or "",
                "occurrence_count": 0,
                "phonetic_code": r["phonetic_code"] or "",
                "aliases": r["aliases"] or [],
                "created_at": r["created_at"].isoformat() if r.get("created_at") else "",
                "source": r.get("source") or "personal-vault",
                "first_seen_rid": r.get("first_seen_rid") or "",
            }, sort_keys=True)
            payload_hash = hashlib.sha256(
                f"entity_upsert:{rid}:{payload}".encode()
            ).hexdigest()

            cur.execute("""
                INSERT INTO terminusdb_outbox (operation, payload, payload_hash, rid, source_rid)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (payload_hash) WHERE status IN ('pending', 'processing') DO NOTHING
            """, ("entity_upsert", payload, payload_hash, rid, ""))
            enqueued += cur.rowcount
    conn_pg.commit()
    return enqueued


def repair_dead_letters(conn_pg) -> int:
    """Re-enqueue dead_letter rows to pending."""
    with conn_pg.cursor() as cur:
        cur.execute("""
            UPDATE terminusdb_outbox
            SET status = 'pending', attempts = 0, error = NULL,
                next_attempt_at = NOW(), claimed_at = NULL, claimed_by = NULL
            WHERE status = 'dead_letter'
        """)
        count = cur.rowcount
    conn_pg.commit()
    return count


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def reconcile(repair: bool = False) -> dict:
    _load_env()

    conn = get_pg_connection()
    adapter = get_adapter()

    # Entity diff
    pg_entities = get_pg_entity_rids(conn)
    tdb_entities = get_tdb_entity_rids(adapter)
    entity_pg_only = pg_entities - tdb_entities
    entity_tdb_only = tdb_entities - pg_entities

    # Assertion diff
    pg_assertions = get_pg_assertion_hashes(conn)
    tdb_assertions = get_tdb_assertion_hashes(adapter)
    assertion_pg_only = pg_assertions - tdb_assertions
    assertion_tdb_only = tdb_assertions - pg_assertions

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "entities": {
            "pg_count": len(pg_entities),
            "tdb_count": len(tdb_entities),
            "pg_only": sorted(list(entity_pg_only))[:100],  # cap output
            "tdb_only": sorted(list(entity_tdb_only))[:100],
            "pg_only_count": len(entity_pg_only),
            "tdb_only_count": len(entity_tdb_only),
        },
        "assertions": {
            "pg_count": len(pg_assertions),
            "tdb_count": len(tdb_assertions),
            "pg_only_count": len(assertion_pg_only),
            "tdb_only_count": len(assertion_tdb_only),
        },
        "repair": None,
    }

    if repair:
        entities_enqueued = repair_entities(conn, entity_pg_only)
        dead_letters_reset = repair_dead_letters(conn)
        report["repair"] = {
            "entities_enqueued": entities_enqueued,
            "dead_letters_reset": dead_letters_reset,
            "assertion_tdb_orphans": len(assertion_tdb_only),
        }

    conn.close()
    return report


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="TerminusDB reconciliation tool")
    parser.add_argument("--repair", action="store_true",
                        help="Re-enqueue pg_only items to outbox")
    parser.add_argument("--json", action="store_true",
                        help="Output JSON instead of text")
    args = parser.parse_args()

    report = reconcile(repair=args.repair)

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        e = report["entities"]
        a = report["assertions"]
        print(f"\n=== Reconciliation Report ===")
        print(f"Entities:   PG={e['pg_count']}  TDB={e['tdb_count']}  "
              f"PG-only={e['pg_only_count']}  TDB-only={e['tdb_only_count']}")
        print(f"Assertions: PG={a['pg_count']}  TDB={a['tdb_count']}  "
              f"PG-only={a['pg_only_count']}  TDB-only={a['tdb_only_count']}")

        if e['pg_only_count'] == 0 and a['pg_only_count'] == 0:
            print("\nNo drift detected.")
        else:
            print(f"\nDrift detected!")
            if e['pg_only_count'] > 0:
                print(f"  {e['pg_only_count']} entities in PG but not TDB")
            if a['pg_only_count'] > 0:
                print(f"  {a['pg_only_count']} assertions in PG but not TDB")
            if not args.repair:
                print("  Run with --repair to re-enqueue missing items")

        if report["repair"]:
            r = report["repair"]
            print(f"\nRepair results:")
            print(f"  Entities re-enqueued: {r['entities_enqueued']}")
            print(f"  Dead letters reset:   {r['dead_letters_reset']}")
            if r['assertion_tdb_orphans'] > 0:
                print(f"  TDB-only assertions (orphans): {r['assertion_tdb_orphans']} — requires manual review")


if __name__ == "__main__":
    main()
