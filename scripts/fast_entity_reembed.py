#!/usr/bin/env python3
"""Fast entity re-embed: sequential calls via urllib, minimal overhead.

Embeds every entity_registry row whose embedding IS NULL with the Ollama model
bge-large and writes the vector back.

Usage:
    python3 scripts/fast_entity_reembed.py            # dry run: count the rows, write nothing
    python3 scripts/fast_entity_reembed.py --apply    # embed and UPDATE entity_registry

The dry run (the default; --dry-run also wins over --apply) counts the rows in a
read-only session and contacts no Ollama server.

Environment:
    POSTGRES_URL     required: postgresql://user:password@host:port/db
    OLLAMA_BASE_URL  Ollama server (default http://localhost:11434)
"""
import argparse
import json, os, sys, time, re, urllib.request
import psycopg2
from psycopg2.extras import RealDictCursor

DEFAULT_OLLAMA_URL = "http://localhost:11434"
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", DEFAULT_OLLAMA_URL)
POSTGRES_URL = os.getenv("POSTGRES_URL")
DSN_PATTERN = r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)"


def embed_one(text):
    data = json.dumps({"model": "bge-large", "prompt": text}).encode()
    req = urllib.request.Request(
        f"{OLLAMA_URL}/api/embeddings",
        data=data,
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["embedding"]


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Embed entity_registry rows whose embedding IS NULL via Ollama (bge-large).",
        epilog="Without --apply this is a dry run: it counts the rows in a read-only session "
               "and contacts no Ollama server.")
    p.add_argument("--apply", action="store_true",
                   help="embed the rows and UPDATE entity_registry.embedding")
    p.add_argument("--dry-run", action="store_true",
                   help="count only (the default); wins over --apply")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    dry_run = args.dry_run or not args.apply
    print(f"Database: {mask_url(POSTGRES_URL) or '(POSTGRES_URL not set)'}", flush=True)
    print(f"Ollama URL: {OLLAMA_URL}", flush=True)
    m = re.match(DSN_PATTERN, POSTGRES_URL or "")
    if not m:
        print("Cannot parse POSTGRES_URL")
        sys.exit(1)

    conn = psycopg2.connect(
        host=m.group(3), port=int(m.group(4)),
        dbname=m.group(5), user=m.group(1), password=m.group(2)
    )
    if dry_run:
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM entity_registry WHERE embedding IS NULL")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"DRY RUN: {total} entities have a NULL embedding. --apply would embed them "
              f"and UPDATE entity_registry.embedding.", flush=True)
        return 0
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT fuseki_uri, entity_text, description, metadata->>'context' AS context
        FROM entity_registry WHERE embedding IS NULL
        ORDER BY entity_type, entity_text
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Entities to embed: {total}", flush=True)

    embedded = 0
    failures = 0
    start = time.time()

    for i, row in enumerate(rows):
        name = row["entity_text"] or ""
        ctx = (row["context"] or "").strip()
        desc = (row["description"] or "").strip()
        combined = ". ".join(filter(None, [ctx, desc]))
        text = f"{name}: {combined}" if combined else name
        text = text[:8000]

        try:
            emb = embed_one(text)
            emb_str = "[" + ",".join(str(x) for x in emb) + "]"
            cur.execute(
                "UPDATE entity_registry SET embedding = %s::vector WHERE fuseki_uri = %s",
                (emb_str, row["fuseki_uri"])
            )
            embedded += 1
        except Exception as e:
            failures += 1
            print(f"  FAIL {row['fuseki_uri'][:40]}: {e}", flush=True)

        if (i + 1) % 50 == 0:
            conn.commit()
            elapsed = time.time() - start
            rate = embedded / elapsed * 60
            print(f"  {embedded}/{total} ({rate:.0f}/min) failures={failures}", flush=True)

    conn.commit()
    elapsed = time.time() - start
    print(f"\nDone: {embedded}/{total} in {elapsed:.0f}s ({elapsed/60:.1f}min), failures={failures}", flush=True)

    cur.close()
    conn.close()


if __name__ == "__main__":
    sys.exit(main())
