#!/usr/bin/env python3
"""Simplest possible chunk re-embed: serial urllib calls, no threading.

Embeds every koi_memory_chunks row whose embedding IS NULL with the Ollama model
bge-large and writes the vector back.

Usage:
    python3 scripts/simple_chunk_reembed.py            # dry run: count the rows, write nothing
    python3 scripts/simple_chunk_reembed.py --apply    # embed and UPDATE koi_memory_chunks

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
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())["embedding"]


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Embed koi_memory_chunks rows whose embedding IS NULL via Ollama (bge-large), one at a time.",
        epilog="Without --apply this is a dry run: it counts the rows in a read-only session "
               "and contacts no Ollama server.")
    p.add_argument("--apply", action="store_true",
                   help="embed the rows and UPDATE koi_memory_chunks.embedding")
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
        print("POSTGRES_URL must be postgresql://user:password@host:port/db", file=sys.stderr)
        return 1
    conn = psycopg2.connect(
        host=m.group(3), port=int(m.group(4)),
        dbname=m.group(5), user=m.group(1), password=m.group(2)
    )
    if dry_run:
        conn.set_session(readonly=True)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM koi_memory_chunks WHERE embedding IS NULL")
        total = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"DRY RUN: {total} chunks have a NULL embedding. --apply would embed them "
              f"one at a time and UPDATE koi_memory_chunks.embedding.", flush=True)
        return 0
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT chunk_rid, content->>'text' AS text, content->>'title' AS title
        FROM koi_memory_chunks WHERE embedding IS NULL
        ORDER BY chunk_rid
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f"Chunks to embed: {total}", flush=True)
    if total == 0:
        return

    embedded = 0
    failures = 0
    start = time.time()

    for i, row in enumerate(rows):
        title = row["title"] or ""
        text = row["text"] or ""
        embed_text = f"Page: {title}\n\n{text}" if title else text
        embed_text = embed_text[:4000]

        try:
            emb = embed_one(embed_text)
            emb_str = "[" + ",".join(str(x) for x in emb) + "]"
            cur.execute(
                "UPDATE koi_memory_chunks SET embedding = %s::vector WHERE chunk_rid = %s",
                (emb_str, row["chunk_rid"])
            )
            embedded += 1
        except Exception as e:
            failures += 1
            if failures <= 5:
                print(f"  FAIL {row['chunk_rid'][:50]}: {e}", flush=True)

        if (i + 1) % 50 == 0:
            conn.commit()
            elapsed = time.time() - start
            rate = embedded / elapsed * 60 if elapsed > 0 else 0
            remaining = total - embedded - failures
            eta = remaining / (rate / 60) if rate > 0 else 0
            print(f"  {embedded}/{total} ({rate:.0f}/min) fail={failures} ETA={eta/60:.0f}min", flush=True)

    conn.commit()
    elapsed = time.time() - start
    print(f"\nDone: {embedded}/{total} in {elapsed/60:.1f}min, failures={failures}", flush=True)
    cur.close()
    conn.close()


if __name__ == "__main__":
    sys.exit(main())
