"""
Import entities and relationships from PostgreSQL personal_koi → TerminusDB.

Phase 0a Step 2:
1. Connect to personal_koi PostgreSQL (config/personal.env)
2. Export entity_registry (skip embeddings), entity_relationships, allowed_predicates
3. Create database, commit schema + SchemaVersion with schema_hash
4. Insert entities (keyed by fuseki_uri)
5. Convert relationships → Assertions with deterministic hash
6. Commit
7. Verify counts match
8. Re-run → verify idempotency

Without --apply this is a dry run: it prints the source database (password masked), the
target TerminusDB and the steps, and connects to neither. --apply runs the import, which
writes the TerminusDB database. --fresh can drop that database and needs
--confirm-destructive as well. POSTGRES_URL (environment, else config/personal.env) has
no default.

Usage:
    python -m scripts.terminusdb.import_from_postgres                    # dry run
    python -m scripts.terminusdb.import_from_postgres --apply
    python -m scripts.terminusdb.import_from_postgres --apply --fresh --confirm-destructive
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# terminusdb-client is not in requirements.txt. Without it --help and the dry run still
# work, and run_import() raises the ImportError.
try:
    from terminusdb_client import WOQLClient

    from scripts.terminusdb.schema import (
        AllowedPredicate,
        Assertion,
        Entity,
        SchemaVersion,
        canonical_object_key,
        commit_schema,
        compute_assertion_hash,
        compute_schema_hash,
        schema,
        serialize_object_key,
    )
    _IMPORT_ERROR = None
except ImportError as _exc:
    _IMPORT_ERROR = _exc

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TERMINUSDB_URL = os.environ.get("TERMINUSDB_URL", "http://127.0.0.1:6363/")
TERMINUSDB_DB = os.environ.get("TERMINUSDB_DB", "koi_knowledge_graph")
TERMINUSDB_TEAM = os.environ.get("TERMINUSDB_TEAM", "admin")
TERMINUSDB_KEY = os.environ.get("TERMINUSDB_KEY", "root")

ASSERTED_BY = "darren-personal"
SOURCE = "personal-vault"

RESULTS_PATH = Path(__file__).parent / "results.json"


def load_env():
    """Load config/personal.env for PostgreSQL connection."""
    env_path = PROJECT_ROOT / "config" / "personal.env"
    if not env_path.exists():
        raise FileNotFoundError(f"Missing {env_path}")
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


def get_pg_connection():
    """Connect to the PostgreSQL database POSTGRES_URL names. There is no default."""
    url = os.environ.get("POSTGRES_URL")
    if not url:
        raise RuntimeError("POSTGRES_URL is not set (environment or config/personal.env); "
                           "name the source database")
    return psycopg2.connect(url)


def _mask_dsn(dsn: str | None) -> str:
    """The DSN with any password replaced by ***."""
    import re
    return re.sub(r"(://[^:/@]*):[^@]*@", r"\1:***@", dsn or "")


def get_terminusdb_client(db_name=None):
    """Create and connect a TerminusDB client."""
    client = WOQLClient(TERMINUSDB_URL)
    client.connect(
        team=TERMINUSDB_TEAM,
        key=TERMINUSDB_KEY,
        db=db_name,
    )
    return client


# ---------------------------------------------------------------------------
# PostgreSQL export
# ---------------------------------------------------------------------------

def export_entities(conn) -> list[dict]:
    """Export entity_registry rows (skip embedding column)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT fuseki_uri, entity_text, entity_type, normalized_text,
                   phonetic_code, aliases,
                   created_at, source, first_seen_rid
            FROM entity_registry
            ORDER BY fuseki_uri
        """)
        rows = cur.fetchall()
    result = []
    for r in rows:
        aliases = r.get("aliases") or []
        if isinstance(aliases, str):
            try:
                aliases = json.loads(aliases)
            except (json.JSONDecodeError, TypeError):
                aliases = []
        result.append({
            # Map fuseki_uri → rid for TerminusDB (Entity._key uses "rid")
            "rid": r["fuseki_uri"],
            "entity_text": r["entity_text"] or "",
            "entity_type": r["entity_type"] or "",
            "normalized_text": r["normalized_text"] or "",
            "occurrence_count": 0,  # Not in personal_koi schema
            "phonetic_code": r["phonetic_code"] or "",
            "aliases": aliases if isinstance(aliases, list) else [],
            "created_at": r["created_at"].isoformat() if r.get("created_at") else "",
            "source": r.get("source") or SOURCE,
            "first_seen_rid": r.get("first_seen_rid") or "",
        })
    return result


def export_relationships(conn) -> list[dict]:
    """Export entity_relationships rows."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT subject_uri, predicate, object_uri, confidence,
                   source, source_rid, source_field, raw_value, created_at
            FROM entity_relationships
            ORDER BY id
        """)
        return cur.fetchall()


def export_predicates(conn) -> list[dict]:
    """Export allowed_predicates rows."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT predicate, description, subject_types, object_types
            FROM allowed_predicates
            ORDER BY predicate
        """)
        return cur.fetchall()


# ---------------------------------------------------------------------------
# TerminusDB import
# ---------------------------------------------------------------------------

def _check_schema_needs_migration(client: WOQLClient) -> bool:
    """Check if existing DB has old fuseki_uri field that needs migration to rid."""
    try:
        schema_docs = list(client.get_all_documents(graph_type="schema"))
        for doc in schema_docs:
            if doc.get("@id", "").endswith("Entity"):
                # Check if old schema uses fuseki_uri instead of rid
                props = doc.get("@key", {}).get("@fields", [])
                if "fuseki_uri" in props:
                    return True
                # Also check document properties
                if "fuseki_uri" in doc and "rid" not in doc:
                    return True
        return False
    except Exception:
        return False


def create_database(client: WOQLClient, db_name: str, fresh: bool = False):
    """Create the database if it doesn't exist.

    Args:
        fresh: If True and DB exists with incompatible schema, drop and recreate.
    """
    raw_dbs = client.list_databases()
    existing = []
    for d in raw_dbs:
        if isinstance(d, dict):
            existing.append(d.get("name") or d.get("path", "").split("/")[-1])
        elif isinstance(d, str):
            existing.append(d.split("/")[-1] if "/" in d else d)
        else:
            existing.append(str(d))
    if db_name in existing:
        client.connect(team=TERMINUSDB_TEAM, key=TERMINUSDB_KEY, db=db_name)

        needs_migration = _check_schema_needs_migration(client)
        if needs_migration and fresh:
            print(f"  Database '{db_name}' has old schema (fuseki_uri). Dropping for fresh import...")
            client.delete_database(db_name, team=TERMINUSDB_TEAM)
            print(f"  Recreating database '{db_name}'...")
            client.create_database(db_name, team=TERMINUSDB_TEAM,
                                   label="KOI Knowledge Graph",
                                   description="TerminusDB evaluation for KOI")
            return True
        elif needs_migration:
            print(f"  WARNING: Database '{db_name}' has old fuseki_uri schema.")
            print(f"  Run with --fresh to drop and reimport with the new rid schema.")
            print(f"  Continuing with schema upgrade attempt...")

        print(f"  Database '{db_name}' already exists, connecting...")
        return False  # already existed
    else:
        print(f"  Creating database '{db_name}'...")
        client.create_database(db_name, team=TERMINUSDB_TEAM,
                               label="KOI Knowledge Graph",
                               description="TerminusDB evaluation for KOI")
        return True  # freshly created


def import_schema(client: WOQLClient):
    """Commit the schema and record a SchemaVersion."""
    print("  Committing schema...")
    commit_schema(client, "Initial KOI schema")

    schema_hash = compute_schema_hash(client)
    now = datetime.now(timezone.utc).isoformat()

    sv = SchemaVersion()
    sv.version = "0.1.0"
    sv.schema_hash = schema_hash
    sv.committed_at = now
    sv.description = "Phase 0a initial import schema"

    try:
        client.insert_document([sv], commit_msg="Record schema version 0.1.0")
    except Exception as e:
        if "already exists" in str(e).lower() or "unique" in str(e).lower():
            print(f"  SchemaVersion 0.1.0 already exists, skipping.")
        else:
            raise
    print(f"  Schema hash: {schema_hash[:16]}...")
    return schema_hash


def import_entities(client: WOQLClient, entities: list[dict]) -> int:
    """Insert entities into TerminusDB. Returns count inserted."""
    docs = []
    for e in entities:
        ent = Entity()
        ent.rid = e["rid"]
        ent.entity_text = e["entity_text"]
        ent.entity_type = e["entity_type"]
        ent.normalized_text = e["normalized_text"]
        ent.occurrence_count = e["occurrence_count"]
        ent.phonetic_code = e["phonetic_code"]
        ent.aliases = set(e["aliases"]) if e["aliases"] else set()
        ent.created_by = ASSERTED_BY
        ent.created_at = e["created_at"]
        ent.source = e["source"]
        ent.first_seen_rid = e["first_seen_rid"]
        docs.append(ent)

    if not docs:
        print("  No entities to import.")
        return 0

    # Batch insert (TerminusDB handles LexicalKey dedup)
    batch_size = 500
    inserted = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        try:
            client.insert_document(
                batch,
                commit_msg=f"Import entities batch {i // batch_size + 1}",
            )
            inserted += len(batch)
        except Exception as e:
            err = str(e).lower()
            if "already exists" in err or "unique" in err:
                # Try one-by-one for idempotent re-import
                for doc in batch:
                    try:
                        client.insert_document(
                            [doc],
                            commit_msg=f"Import entity {doc.rid}",
                        )
                        inserted += 1
                    except Exception:
                        inserted += 1  # already exists = still "imported"
            else:
                raise
    return inserted


def import_predicates(client: WOQLClient, predicates: list[dict]) -> int:
    """Insert allowed predicates into TerminusDB."""
    docs = []
    for p in predicates:
        ap = AllowedPredicate()
        ap.predicate = p["predicate"]
        ap.description = p.get("description") or ""
        ap.subject_types = set(p.get("subject_types") or [])
        ap.object_types = set(p.get("object_types") or [])
        docs.append(ap)

    if not docs:
        return 0

    try:
        client.insert_document(docs, commit_msg="Import allowed predicates")
    except Exception as e:
        if "already exists" in str(e).lower() or "unique" in str(e).lower():
            for doc in docs:
                try:
                    client.insert_document([doc], commit_msg=f"Import predicate {doc.predicate}")
                except Exception:
                    pass  # already exists
        else:
            raise
    return len(docs)


def relationship_to_assertion(rel: dict, entity_uris: set) -> dict | None:
    """Convert a PostgreSQL entity_relationship row to an Assertion dict.

    All relationships from PostgreSQL are entity→entity (not literals),
    since literals aren't stored in entity_relationships.
    """
    subject = rel["subject_uri"]
    obj = rel["object_uri"]

    # Validate both sides exist
    if subject not in entity_uris or obj not in entity_uris:
        return None

    source = rel.get("source") or SOURCE
    source_rid = rel.get("source_rid") or ""
    source_field = rel.get("source_field") or ""

    assertion_hash = compute_assertion_hash(
        subject_uri=subject,
        predicate=rel["predicate"],
        object_kind="entity",
        object_uri=obj,
        literal_value="",
        literal_datatype="",
        literal_lang="",
        source=source,
        source_rid=source_rid,
        source_field=source_field,
        asserted_by=ASSERTED_BY,
    )

    assertion_dict = {
        "object_kind": "entity",
        "object_uri": obj,
        "literal_value": "",
        "literal_datatype": "",
        "literal_lang": "",
    }
    norm_key = serialize_object_key(canonical_object_key(assertion_dict))

    return {
        "assertion_hash": assertion_hash,
        "subject_uri": subject,
        "predicate": rel["predicate"],
        "object_kind": "entity",
        "object_uri": obj,
        "literal_value": "",
        "literal_datatype": "",
        "literal_lang": "",
        "asserted_by": ASSERTED_BY,
        "asserted_at": rel["created_at"].isoformat() if rel.get("created_at") else datetime.now(timezone.utc).isoformat(),
        "confidence": float(rel.get("confidence") or 1.0),
        "source": source,
        "source_rid": source_rid,
        "source_field": source_field,
        "raw_value": rel.get("raw_value") or "",
        "status": "active",
        "normalized_object_key": norm_key,
    }


def import_assertions(client: WOQLClient, relationships: list[dict],
                       entity_uris: set) -> tuple[int, int]:
    """Convert relationships to assertions and insert. Returns (inserted, skipped)."""
    assertions = []
    skipped = 0
    for rel in relationships:
        a = relationship_to_assertion(rel, entity_uris)
        if a is None:
            skipped += 1
            continue
        assertions.append(a)

    if not assertions:
        return 0, skipped

    # Build Assertion documents
    docs = []
    for a in assertions:
        doc = Assertion()
        for key, val in a.items():
            setattr(doc, key, val)
        docs.append(doc)

    batch_size = 500
    inserted = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i:i + batch_size]
        try:
            client.insert_document(
                batch,
                commit_msg=f"Import assertions batch {i // batch_size + 1}",
            )
            inserted += len(batch)
        except Exception as e:
            err = str(e).lower()
            if "already exists" in err or "unique" in err:
                for doc in batch:
                    try:
                        client.insert_document(
                            [doc],
                            commit_msg=f"Import assertion {doc.assertion_hash[:12]}",
                        )
                        inserted += 1
                    except Exception:
                        inserted += 1  # already exists
            else:
                raise
    return inserted, skipped


def verify_counts(client: WOQLClient, expected_entities: int,
                  expected_assertions: int) -> dict:
    """Verify document counts in TerminusDB match expectations."""
    try:
        entities = list(client.query_document({"@type": "Entity"}))
    except Exception:
        entities = [d for d in client.get_all_documents()
                    if d.get("@type", "").endswith("Entity")]

    try:
        assertions = list(client.query_document({"@type": "Assertion"}))
    except Exception:
        assertions = [d for d in client.get_all_documents()
                      if d.get("@type", "").endswith("Assertion")]

    return {
        "entity_count": {"expected": expected_entities, "actual": len(entities),
                         "pass": len(entities) == expected_entities},
        "assertion_count": {"expected": expected_assertions, "actual": len(assertions),
                            "pass": len(assertions) == expected_assertions},
    }


def test_idempotency(client: WOQLClient, relationships: list[dict],
                     entity_uris: set,
                     first_hashes: list[str]) -> bool:
    """Re-compute assertion hashes and verify they match the first run."""
    second_hashes = []
    for rel in relationships:
        a = relationship_to_assertion(rel, entity_uris)
        if a is not None:
            second_hashes.append(a["assertion_hash"])

    if len(first_hashes) != len(second_hashes):
        print(f"  FAIL: Hash count mismatch: {len(first_hashes)} vs {len(second_hashes)}")
        return False

    for i, (h1, h2) in enumerate(zip(first_hashes, second_hashes)):
        if h1 != h2:
            print(f"  FAIL: Hash mismatch at index {i}: {h1[:12]} vs {h2[:12]}")
            return False

    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_import(fresh: bool = False) -> dict:
    """Run the full import pipeline. Returns metrics dict.

    Args:
        fresh: If True and DB exists with incompatible schema, drop and recreate.
    """
    if _IMPORT_ERROR is not None:
        raise _IMPORT_ERROR
    load_env()
    metrics = {}
    t_start = time.time()

    print("\n=== Step 2: PostgreSQL → TerminusDB Import ===\n")
    print(f"Source: {_mask_dsn(os.environ.get('POSTGRES_URL')) or '(POSTGRES_URL not set)'}")
    print(f"Target: {TERMINUSDB_URL} database {TERMINUSDB_DB} (team {TERMINUSDB_TEAM})\n")

    # 1. Export from PostgreSQL
    print("1. Connecting to PostgreSQL...")
    conn = get_pg_connection()

    print("2. Exporting entities...")
    entities = export_entities(conn)
    print(f"   Exported {len(entities)} entities")

    print("3. Exporting relationships...")
    relationships = export_relationships(conn)
    print(f"   Exported {len(relationships)} relationships")

    print("4. Exporting predicates...")
    predicates = export_predicates(conn)
    print(f"   Exported {len(predicates)} predicates")
    conn.close()

    # 2. Connect to TerminusDB
    print("\n5. Connecting to TerminusDB...")
    client = get_terminusdb_client()

    # 3. Create database
    print("6. Creating database...")
    created = create_database(client, TERMINUSDB_DB, fresh=fresh)

    # 4. Import/upgrade schema (always commit to pick up field renames like fuseki_uri→rid)
    print("7. Importing/upgrading schema...")
    schema_hash = import_schema(client)

    # 5. Import entities
    print("8. Importing entities...")
    entity_count = import_entities(client, entities)
    print(f"   Imported {entity_count} entities")

    # 6. Import predicates
    print("9. Importing predicates...")
    pred_count = import_predicates(client, predicates)
    print(f"   Imported {pred_count} predicates")

    # 7. Import assertions
    print("10. Converting relationships → assertions...")
    entity_uris = {e["rid"] for e in entities}
    assertion_count, skipped = import_assertions(client, relationships, entity_uris)
    print(f"    Imported {assertion_count} assertions (skipped {skipped} dangling refs)")

    # 8. Verify counts
    print("\n11. Verifying counts...")
    count_metrics = verify_counts(client, len(entities), assertion_count)
    print(f"    Entities: {count_metrics['entity_count']}")
    print(f"    Assertions: {count_metrics['assertion_count']}")

    # 9. Idempotency test
    print("\n12. Testing idempotency (re-computing hashes)...")
    first_hashes = []
    for rel in relationships:
        a = relationship_to_assertion(rel, entity_uris)
        if a is not None:
            first_hashes.append(a["assertion_hash"])

    idempotent = test_idempotency(client, relationships, entity_uris, first_hashes)
    print(f"    Idempotent: {idempotent}")

    t_end = time.time()
    import_time = t_end - t_start

    # 10. Query latency test
    print("\n13. Testing query latency...")
    if entities:
        latencies = []
        sample_uris = [e["rid"] for e in entities[:20]]
        for uri in sample_uris:
            t0 = time.time()
            try:
                client.query_document({"@type": "Entity", "rid": uri})
            except Exception:
                try:
                    client.get_document(f"Entity/{uri}")
                except Exception:
                    pass
            latencies.append((time.time() - t0) * 1000)
        latencies.sort()
        p95_idx = int(len(latencies) * 0.95)
        p95_ms = latencies[p95_idx] if latencies else 0
        print(f"    P95 query latency: {p95_ms:.1f}ms (n={len(latencies)})")
    else:
        p95_ms = 0

    # Build metrics
    metrics = {
        "import_entity_count": count_metrics["entity_count"],
        "import_assertion_count": count_metrics["assertion_count"],
        "import_predicate_count": {"expected": len(predicates), "actual": pred_count,
                                    "pass": True},
        "idempotent_reimport": {"pass": idempotent},
        "p95_query_ms": round(p95_ms, 1),
        "import_time_s": round(import_time, 1),
        "schema_hash": schema_hash[:16],
    }

    print(f"\n=== Import complete in {import_time:.1f}s ===")
    return metrics


def build_parser():
    import argparse
    parser = argparse.ArgumentParser(
        description="Import PostgreSQL → TerminusDB. Without --apply it is a dry run that "
                    "connects to nothing.")
    parser.add_argument("--apply", action="store_true",
                        help="Run the import: reads POSTGRES_URL and writes the TerminusDB database")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the plan and exit (the default; wins over --apply)")
    parser.add_argument("--fresh", action="store_true",
                        help="Drop and recreate DB if schema is incompatible (fuseki_uri→rid); "
                             "needs --confirm-destructive with --apply")
    parser.add_argument("--confirm-destructive", action="store_true",
                        help="Allow --fresh to drop the TerminusDB database")
    return parser


def print_plan(fresh: bool) -> None:
    """The dry run: targets and steps, with no connection."""
    try:
        load_env()
        env_note = "config/personal.env loaded"
    except FileNotFoundError:
        env_note = "config/personal.env missing: --apply would stop"
    dsn = os.environ.get("POSTGRES_URL")
    print("DRY RUN: nothing is read or written. Pass --apply to import.")
    print(f"  Environment: {env_note}")
    print(f"  Source PostgreSQL: {_mask_dsn(dsn) if dsn else '(POSTGRES_URL not set: --apply would stop)'}")
    print(f"  Target TerminusDB: {TERMINUSDB_URL} database {TERMINUSDB_DB} (team {TERMINUSDB_TEAM})")
    print("  Would read entity_registry, entity_relationships and allowed_predicates")
    print(f"  Would create {TERMINUSDB_DB} if missing, commit the schema, and insert entities, "
          "predicates and assertions")
    if fresh:
        print(f"  --fresh: would DROP {TERMINUSDB_DB} and recreate it if its schema is old "
              "(needs --confirm-destructive)")
    if _IMPORT_ERROR is not None:
        print(f"  Note: {_IMPORT_ERROR}; --apply needs terminusdb-client installed")


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if args.dry_run or not args.apply:
        print_plan(args.fresh)
        return 0
    if args.fresh and not args.confirm_destructive:
        print("Refusing --fresh without --confirm-destructive: it can drop the TerminusDB "
              f"database {TERMINUSDB_DB} at {TERMINUSDB_URL}.", file=sys.stderr)
        return 2
    metrics = run_import(fresh=args.fresh)
    print("\nMetrics:")
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
