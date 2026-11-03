"""Fix oversize chunks (>8K tokens) by split+mean embedding.

For each chunk where embedding_3072 IS NULL and text exceeds OpenAI's 8192
token input limit:
  1. Split text into sub-chunks of <=7500 tokens on paragraph/sentence
     boundaries (preserving word boundaries).
  2. Embed each sub-chunk via OpenAI text-embedding-3-large @ 3072-dim.
  3. Compute the mean vector across all sub-chunks.
  4. UPDATE koi_memory_chunks.embedding_3072 = mean for the parent chunk id.

Standard practice for handling oversize inputs in production retrieval
(OpenAI cookbook pattern). Preserves single-vector-per-chunk-rid invariant,
no schema change, no chunk_rid churn.

Usage:
    python scripts/reembed_oversize_chunks.py                     # dry run (the default)
    python scripts/reembed_oversize_chunks.py --apply             # first 100 oversize chunks
    python scripts/reembed_oversize_chunks.py --apply --limit 0   # every oversize chunk

Without --apply (or with --dry-run, which wins) it counts the candidates in a
read-only session, estimates the oversize ones from their length and makes no
OpenAI call and no tokenizer download. --apply makes paid OpenAI embedding calls
and UPDATEs koi_memory_chunks; --limit caps the oversize chunks per run at 100
unless given (0 = no limit). The database is POSTGRES_URL (default
postgresql://localhost:5432/personal_koi, connecting as the current OS user); --apply
needs OPENAI_API_KEY.
"""
import argparse
import asyncio
import os
import re
import sys
import time

import asyncpg
import tiktoken
from openai import OpenAI

MODEL = "text-embedding-3-large"
DIM = 3072
SUB_CHUNK_TOK = 7500
OVERLAP_TOK = 200
DEFAULT_LIMIT = 100
# No user in the default: the driver then uses the current OS user.
DB_URL = os.environ.get("POSTGRES_URL", "postgresql://localhost:5432/personal_koi")
READ_ONLY = {"default_transaction_read_only": "on"}
# A dry run does not load the tokenizer; text longer than this many characters may be
# over 8000 tokens (about 3 characters per token for prose and markup).
ESTIMATE_CHARS = 24000

CANDIDATES_SQL = """
        SELECT c.id, c.content->>'text' AS text
        FROM koi_memory_chunks c JOIN koi_memories m ON m.rid=c.document_rid
        WHERE m.source_sensor IN ('mediawiki-sensor','email-sensor','doc-scanner','ics-event')
          AND c.embedding_3072 IS NULL
          AND char_length(c.content->>'text') > 0
        """

# Created on first use (the tokenizer may download its vocabulary; the client needs
# OPENAI_API_KEY), never at import.
enc = None
client = None


def _encoder():
    global enc
    if enc is None:
        enc = tiktoken.get_encoding("cl100k_base")
    return enc


def _client():
    global client
    if client is None:
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return client


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def split_by_tokens(text: str, max_tok: int = SUB_CHUNK_TOK, overlap: int = OVERLAP_TOK):
    """Split on whitespace-preserving token boundaries with small overlap."""
    tokens = _encoder().encode(text, disallowed_special=())
    if len(tokens) <= max_tok:
        return [text]
    out = []
    i = 0
    while i < len(tokens):
        window = tokens[i : i + max_tok]
        sub = _encoder().decode(window)
        out.append(sub)
        if i + max_tok >= len(tokens):
            break
        i += max_tok - overlap
    return out


def mean_vectors(vecs):
    """Element-wise mean. vecs: list[list[float]]."""
    n = len(vecs)
    if n == 0:
        return None
    dim = len(vecs[0])
    acc = [0.0] * dim
    for v in vecs:
        for k in range(dim):
            acc[k] += v[k]
    return [x / n for x in acc]


def embed_batch(texts):
    resp = _client().embeddings.create(model=MODEL, input=texts, dimensions=DIM)
    return [d.embedding for d in resp.data]


async def dry_run_report(limit):
    """Count candidates in a read-only session; no tokenizer, no OpenAI call."""
    conn = await asyncpg.connect(DB_URL, server_settings=READ_ONLY)
    try:
        rows = await conn.fetch(CANDIDATES_SQL)
    finally:
        await conn.close()
    long_ = sum(1 for r in rows if len(r["text"] or "") > ESTIMATE_CHARS)
    print(f"Candidates: {len(rows)}")
    print(f"Longer than {ESTIMATE_CHARS} characters (possibly >8000 tokens): {long_}")
    print(f"DRY RUN: --apply would count tokens exactly, embed at most "
          f"{limit or 'all'} oversize chunks (--limit {limit}; 0 = no limit) with "
          f"{MODEL} (paid) and UPDATE koi_memory_chunks.embedding_3072.")
    return 0


async def main(limit=0):
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY is not set", file=sys.stderr)
        return 1
    conn = await asyncpg.connect(DB_URL)
    # Target: all chunks still NULL with text present.
    rows = await conn.fetch(CANDIDATES_SQL)
    print(f"Candidates: {len(rows)}")

    # Filter to only those truly > 8000 tokens (the only ones OpenAI rejects).
    oversize = []
    for r in rows:
        tok = len(_encoder().encode(r["text"] or "", disallowed_special=()))
        if tok > 8000:
            oversize.append((r["id"], r["text"], tok))
    print(f"Oversize (>8000 tokens): {len(oversize)}")
    if limit and len(oversize) > limit:
        oversize = oversize[:limit]
        print(f"Processing the first {limit} (--limit; 0 = no limit)")

    t0 = time.time()
    total_subs = 0
    total_tokens = 0
    total_cost = 0.0
    processed = 0
    failed = []
    for cid, text, tok in oversize:
        subs = split_by_tokens(text)
        # Embed sub-chunks (batched).
        try:
            vecs = embed_batch(subs)
        except Exception as e:
            failed.append((cid, str(e)))
            continue
        mean = mean_vectors(vecs)
        # Write back.
        await conn.execute(
            "UPDATE koi_memory_chunks SET embedding_3072 = $1::vector WHERE id = $2",
            str(mean), cid,
        )
        sub_tok = sum(len(_encoder().encode(s, disallowed_special=())) for s in subs)
        total_subs += len(subs)
        total_tokens += sub_tok
        total_cost += sub_tok / 1_000_000 * 0.13
        processed += 1
        if processed % 25 == 0:
            print(
                f"  {processed}/{len(oversize)}: id={cid} subs={len(subs)} "
                f"orig={tok}tok total_cost=${total_cost:.4f} "
                f"elapsed={time.time()-t0:.0f}s",
                flush=True,
            )

    print(
        f"\nDone: processed={processed} sub_chunks={total_subs} "
        f"tokens={total_tokens} cost=${total_cost:.4f} "
        f"elapsed={time.time()-t0:.0f}s"
    )
    if failed:
        print(f"Failed: {len(failed)}")
        for cid, err in failed[:5]:
            print(f"  id={cid}: {err[:120]}")

    # Verify coverage.
    final = await conn.fetch(
        """
        SELECT m.source_sensor, COUNT(c.id) total, COUNT(c.embedding_3072) n
        FROM koi_memory_chunks c JOIN koi_memories m ON m.rid=c.document_rid
        WHERE m.source_sensor IN ('mediawiki-sensor','email-sensor','doc-scanner','ics-event')
        GROUP BY m.source_sensor ORDER BY total DESC
        """
    )
    print("\nFinal coverage:")
    for r in final:
        pct = r["n"] / r["total"] * 100 if r["total"] else 0.0
        print(f"  {r['source_sensor']:>20}: {r['n']}/{r['total']} ({pct:.3f}%)")
    await conn.close()
    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Embed oversize (>8000 token) chunks with a NULL embedding_3072 by "
                    "split + mean embedding.",
        epilog="Without --apply this is a dry run: it counts candidates in a read-only "
               "session and makes no OpenAI call.")
    p.add_argument("--apply", action="store_true",
                   help="embed (paid OpenAI calls) and UPDATE koi_memory_chunks.embedding_3072")
    p.add_argument("--dry-run", action="store_true",
                   help="count only (the default); wins over --apply")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                   help=f"max oversize chunks to embed (default {DEFAULT_LIMIT}; 0 = no limit)")
    return p.parse_args(argv)


def cli(argv=None):
    args = parse_args(argv)
    print(f"Database: {mask_url(DB_URL)}")
    if args.dry_run or not args.apply:
        return asyncio.run(dry_run_report(args.limit))
    return asyncio.run(main(args.limit))


if __name__ == "__main__":
    sys.exit(cli())
