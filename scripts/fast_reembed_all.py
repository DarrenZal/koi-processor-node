#!/usr/bin/env python3
"""
Fast concurrent re-embed for both entities and chunks.
Uses ThreadPoolExecutor for parallel Ollama requests + requests.Session for keep-alive.

Embeds every entity_registry and koi_memory_chunks row whose embedding IS NULL with
the Ollama model bge-large and writes the vectors back.

Usage:
    python3 scripts/fast_reembed_all.py            # dry run: count the rows, write nothing
    python3 scripts/fast_reembed_all.py --apply    # embed and UPDATE both tables

The dry run (the default; --dry-run also wins over --apply) counts the rows in a
read-only session and contacts no Ollama server.

Environment:
    POSTGRES_URL     required: postgresql://user:password@host:port/db
    OLLAMA_BASE_URL  Ollama server (default http://localhost:11434)
    EMBED_WORKERS    concurrent embedding requests (default 4)
"""
import argparse
import json, os, sys, time, re
import psycopg2
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import http.client

DEFAULT_OLLAMA_URL = "http://localhost:11434"
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", DEFAULT_OLLAMA_URL)
POSTGRES_URL = os.getenv("POSTGRES_URL")
WORKERS = int(os.getenv("EMBED_WORKERS", "4"))
DSN_PATTERN = r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)"


def make_connection():
    """Create a persistent HTTP connection to Ollama."""
    parsed = urllib.request.urlparse(OLLAMA_URL)
    return http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=300)


# Thread-local connections
import threading
_local = threading.local()


def get_ollama_conn():
    if not hasattr(_local, 'conn'):
        _local.conn = make_connection()
    return _local.conn


def embed_one(text):
    """Embed one text using thread-local keep-alive connection."""
    conn = get_ollama_conn()
    body = json.dumps({"model": "bge-large", "prompt": text}).encode()
    try:
        conn.request("POST", "/api/embeddings", body=body,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read())
        return data["embedding"]
    except Exception:
        # Reconnect on failure
        _local.conn = make_connection()
        conn = _local.conn
        conn.request("POST", "/api/embeddings", body=body,
                     headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read())
        return data["embedding"]


def get_db_conn():
    m = re.match(DSN_PATTERN, POSTGRES_URL)
    return psycopg2.connect(
        host=m.group(3), port=int(m.group(4)),
        dbname=m.group(5), user=m.group(1), password=m.group(2)
    )


def embed_entities():
    conn = get_db_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT fuseki_uri, entity_text, description, metadata->>'context' AS context
        FROM entity_registry WHERE embedding IS NULL
        ORDER BY entity_type, entity_text
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[entities] To embed: {total}", flush=True)
    if total == 0:
        cur.close()
        conn.close()
        return

    def prepare_text(row):
        name = row["entity_text"] or ""
        ctx = (row["context"] or "").strip()
        desc = (row["description"] or "").strip()
        combined = ". ".join(filter(None, [ctx, desc]))
        text = f"{name}: {combined}" if combined else name
        return text[:8000]

    embedded = 0
    failures = 0
    start = time.time()

    # Process in chunks of 50 for DB commit batches
    for batch_start in range(0, total, 50):
        batch = rows[batch_start:batch_start + 50]
        results = {}

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {}
            for row in batch:
                text = prepare_text(row)
                f = executor.submit(embed_one, text)
                futures[f] = row["fuseki_uri"]

            for future in as_completed(futures):
                uri = futures[future]
                try:
                    emb = future.result()
                    results[uri] = emb
                except Exception as e:
                    failures += 1
                    print(f"  [entities] FAIL {uri[:40]}: {e}", flush=True)

        # Write to DB
        for uri, emb in results.items():
            emb_str = "[" + ",".join(str(x) for x in emb) + "]"
            cur.execute(
                "UPDATE entity_registry SET embedding = %s::vector WHERE fuseki_uri = %s",
                (emb_str, uri)
            )
            embedded += 1

        conn.commit()
        elapsed = time.time() - start
        rate = embedded / elapsed * 60 if elapsed > 0 else 0
        remaining = total - embedded - failures
        eta = remaining / (rate / 60) if rate > 0 else 0
        print(f"  [entities] {embedded}/{total} ({rate:.0f}/min) failures={failures} ETA={eta/60:.0f}min", flush=True)

    conn.commit()
    elapsed = time.time() - start
    print(f"[entities] Done: {embedded}/{total} in {elapsed/60:.1f}min, failures={failures}", flush=True)
    cur.close()
    conn.close()


def embed_chunks():
    conn = get_db_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT chunk_rid, content->>'text' AS text, content->>'title' AS title
        FROM koi_memory_chunks WHERE embedding IS NULL
        ORDER BY chunk_rid
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"[chunks] To embed: {total}", flush=True)
    if total == 0:
        cur.close()
        conn.close()
        return

    def prepare_text(row):
        title = row["title"] or ""
        text = row["text"] or ""
        embed_text = f"Page: {title}\n\n{text}" if title else text
        return embed_text[:500]  # BGE-large has 512 token limit, ~500 chars matches entity speed

    embedded = 0
    failures = 0
    start = time.time()
    chunk_workers = min(WORKERS, 4)  # Cap at 4 for chunks (longer text)

    for batch_start in range(0, total, 50):
        batch = rows[batch_start:batch_start + 50]
        results = {}

        with ThreadPoolExecutor(max_workers=chunk_workers) as executor:
            futures = {}
            for row in batch:
                text = prepare_text(row)
                f = executor.submit(embed_one, text)
                futures[f] = row["chunk_rid"]

            for future in as_completed(futures):
                rid = futures[future]
                try:
                    emb = future.result()
                    results[rid] = emb
                except Exception as e:
                    failures += 1
                    print(f"  [chunks] FAIL {rid[:50]}: {e}", flush=True)

        for rid, emb in results.items():
            emb_str = "[" + ",".join(str(x) for x in emb) + "]"
            cur.execute(
                "UPDATE koi_memory_chunks SET embedding = %s::vector WHERE chunk_rid = %s",
                (emb_str, rid)
            )
            embedded += 1

        conn.commit()
        elapsed = time.time() - start
        rate = embedded / elapsed * 60 if elapsed > 0 else 0
        remaining = total - embedded - failures
        eta = remaining / (rate / 60) if rate > 0 else 0
        print(f"  [chunks] {embedded}/{total} ({rate:.0f}/min) failures={failures} ETA={eta/60:.0f}min", flush=True)

    conn.commit()
    elapsed = time.time() - start
    print(f"[chunks] Done: {embedded}/{total} in {elapsed/60:.1f}min, failures={failures}", flush=True)
    cur.close()
    conn.close()


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def count_null_rows():
    """Rows with a NULL embedding per table, read in a read-only session."""
    conn = get_db_conn()
    conn.set_session(readonly=True)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM entity_registry WHERE embedding IS NULL")
        entities = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM koi_memory_chunks WHERE embedding IS NULL")
        chunks = cur.fetchone()[0]
        cur.close()
    finally:
        conn.close()
    return entities, chunks


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Embed entity_registry and koi_memory_chunks rows whose embedding IS NULL "
                    "via Ollama (bge-large).",
        epilog="Without --apply this is a dry run: it counts the rows in a read-only session "
               "and contacts no Ollama server.")
    p.add_argument("--apply", action="store_true",
                   help="embed the rows and UPDATE entity_registry.embedding and "
                        "koi_memory_chunks.embedding")
    p.add_argument("--dry-run", action="store_true",
                   help="count only (the default); wins over --apply")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    dry_run = args.dry_run or not args.apply
    print(f"Database: {mask_url(POSTGRES_URL) or '(POSTGRES_URL not set)'}", flush=True)
    print(f"Ollama URL: {OLLAMA_URL}", flush=True)
    print(f"Workers: {WORKERS}", flush=True)
    if not re.match(DSN_PATTERN, POSTGRES_URL or ""):
        print("POSTGRES_URL must be postgresql://user:password@host:port/db", file=sys.stderr)
        return 1
    if dry_run:
        entities, chunks = count_null_rows()
        print(f"DRY RUN: {entities} entities and {chunks} chunks have a NULL embedding. "
              f"--apply would embed them and UPDATE both tables.", flush=True)
        return 0
    embed_entities()
    embed_chunks()
    print("All done!", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
