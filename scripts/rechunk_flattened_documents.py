#!/usr/bin/env python3
"""Re-chunk documents whose chunks were flattened by the pre-2026-08-14 chunker.

Background. `api/chunker.py` rebuilt every chunk as `' '.join(text.split())`,
which silently destroyed newlines, indentation and column alignment at ingest.
The chunker was fixed in August 2026. The fix only
affects NEW ingests: re-running an ingest is idempotent on the content hash of
the source bytes, which have not changed, so damaged documents never self-heal.

The migration is cheap and entirely local because the damage was only ever at the
chunk layer. `koi_memories.content->>'text'` still holds the full original
document with its whitespace intact (for a damaged document, the stored text keeps
its line breaks and column runs while none of its chunks has a newline at all).
Nothing has to be re-fetched; the source of truth is already in the database.

Damage signature, and why it is conservative:

    the stored full text contains a newline
    AND no chunk of that document contains one

A document with no newlines in its source is not damaged, it is simply flat, so
requiring the source to have them avoids rewriting documents that were always
fine. Documents are re-embedded, which costs money, so the selection is
deliberately narrow rather than "re-chunk everything".

Safety. Embedding happens for the whole document BEFORE anything is deleted, and
the swap runs inside a transaction, so a failure mid-document leaves the existing
chunks untouched rather than leaving it chunkless. The pre-existing
`upsert_document_chunks` is reused so chunk rids, metadata and the 3072-dim
column match exactly what a normal ingest writes.

SCOPE. `--rid-like` defaults to `document:%`. That covers documents ingested
through the document pipeline only: damaged chunks also occur under other RID
prefixes (`orn:` sensor and email ingests, `mediawiki:`, `doc-scanner:`,
`substack-corpus:`), so a run with the default can report complete while the
corpus is not. Pass `--rid-like '%'` to scan everything; the dry run prints a
cost estimate before anything is spent.

    python3 scripts/rechunk_flattened_documents.py --dry-run
    python3 scripts/rechunk_flattened_documents.py --dry-run --rid-like '%'
    python3 scripts/rechunk_flattened_documents.py --rid document:f4d6bdee...
    python3 scripts/rechunk_flattened_documents.py --limit 5
    python3 scripts/rechunk_flattened_documents.py --all --rid-like '%'
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncpg  # noqa: E402

from api.chunker import TextChunker  # noqa: E402
from scripts.ingest_document import (  # noqa: E402
    CHUNK_OVERLAP,
    CHUNK_SIZE,
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    OPENAI_API_KEY,
    OpenAIEmbeddingProvider,
    upsert_document_chunks,
)

DB = os.getenv("DATABASE_URL", "postgresql://localhost/personal_koi")

CANDIDATES = """
SELECT m.rid,
       coalesce(m.content->>'title', m.content->>'file_path', m.rid) AS title,
       length(m.content->>'text')                                    AS src_len,
       (SELECT count(*) FROM koi_memory_chunks c WHERE c.document_rid = m.rid) AS n_chunks
FROM koi_memories m
WHERE m.rid LIKE $1
  -- an INTERIOR newline, not merely a trailing one. A transcript that is one
  -- continuous line ending in a single newline is genuinely flat rather than
  -- damaged: re-chunking recovers nothing and still costs embedding spend.
  -- Caught on the 2026-08-14 run, where exactly one of 303 documents was this
  -- case and tripped the script's own "still no newlines after re-chunk" warning.
  AND position(chr(10) in rtrim(m.content->>'text', chr(10) || chr(13) || ' ')) > 0
  AND EXISTS (SELECT 1 FROM koi_memory_chunks c WHERE c.document_rid = m.rid)
  AND NOT EXISTS (
        SELECT 1 FROM koi_memory_chunks c
        WHERE c.document_rid = m.rid
          AND c.content->>'text' LIKE '%' || chr(10) || '%')
ORDER BY length(m.content->>'text') DESC
"""


class ProviderExhausted(RuntimeError):
    """The embedding account is out of credit or the key is rejected.

    Deliberately its own type, because it is the one failure that must NOT be treated
    as a per-document hiccup. An earlier version hit `credit_balance_exhausted`
    partway through and kept going: the batch call failed, the per-chunk fallback
    retried every chunk against the same dead account, each one appended None, and any
    document under the half-failed threshold was rewritten with NULL embeddings. The
    retries were pure waste — an exhausted balance does not recover on the next call.

    Retrying an EXTERNAL dependency that is down is not resilience, it is a busy loop
    with a log file.
    """


TERMINAL_PROVIDER_SIGNS = (
    "insufficient_quota",
    "credit_balance_exhausted",
    "no credits remaining",
    "invalid_api_key",
    "incorrect api key",
)


def _is_terminal(exc: Exception) -> bool:
    """True when retrying cannot possibly help — account state, not a transient fault.

    Matched on the message rather than the exception class because the provider SDK
    raises the same RateLimitError for a genuine per-minute rate limit (retry helps)
    and for an exhausted balance (retry never helps). The distinction is only in the
    body, so that is where it has to be read.
    """
    return any(s in str(exc).lower() for s in TERMINAL_PROVIDER_SIGNS)


async def structure_counts(conn, rid: str) -> dict:
    row = await conn.fetchrow("""
        SELECT count(*) AS n,
               count(*) FILTER (WHERE content->>'text' LIKE '%' || chr(10) || '%') AS nl,
               count(*) FILTER (WHERE content->>'text' ~ '[A-Za-z]{3,}[ ]{4,}[A-Za-z]{3,}') AS cols
        FROM koi_memory_chunks WHERE document_rid = $1""", rid)
    return dict(row) if row else {"n": 0, "nl": 0, "cols": 0}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="list candidates, change nothing")
    ap.add_argument("--rid", help="re-chunk exactly this document rid")
    ap.add_argument("--limit", type=int, help="process at most N documents")
    ap.add_argument("--all", action="store_true", help="process every candidate")
    ap.add_argument(
        "--rid-like", default="document:%",
        help=(
            "SQL LIKE pattern for which RID namespace to scan. Defaults to the "
            "'document:%%' scope the 2026-08-14 run used. That run reported COMPLETE and "
            "was complete FOR THAT NAMESPACE ONLY: re-measuring on 2026-08-17 with the "
            "prefix removed found 12,798 further damaged documents / 46,793 chunks under "
            "orn: (7,607), mediawiki: (4,201), doc-scanner: (604) and substack-corpus: "
            "(386). Verified by hand on the largest — its stored text has paragraph "
            "breaks and its first chunk has them collapsed to single spaces. Use '%%' to "
            "scan everything."
        ))
    a = ap.parse_args()

    if not (a.dry_run or a.rid or a.limit or a.all):
        ap.error("pick one of --dry-run / --rid / --limit / --all; "
                 "this rewrites chunks and costs embedding spend")

    conn = await asyncpg.connect(DB)
    try:
        rows = await conn.fetch(CANDIDATES, a.rid_like)
        if a.rid:
            rows = [r for r in rows if r["rid"] == a.rid]
            if not rows:
                print(f"{a.rid} is not a candidate: either it has no chunks, its stored "
                      f"text has no newlines, or its chunks already retain them.")
                return

        n_chunks = sum(r["n_chunks"] for r in rows)
        # ~$0.13 per 1M tokens for text-embedding-3-large.
        #
        # MEASURED, not derived. The first version of this line read CHUNK_SIZE (500) as
        # CHARACTERS and divided by 4 to get tokens, giving 125 tokens/chunk. CHUNK_SIZE
        # is TOKENS, and with overlap the chunks this script writes are several times
        # longer, so that estimate came out about 7x low; a run sized on it can exhaust
        # an account's credit partway, which takes embeddings down service-wide, not
        # just for this job.
        #
        # So: measure the source text, do not infer from a constant whose units you have
        # not checked. src_len is already in the candidate query.
        est_tokens = sum(r["src_len"] for r in rows) / 4.0
        est_usd = est_tokens / 1_000_000 * 0.13
        print(f"{len(rows)} damaged document(s) matching rid LIKE {a.rid_like!r}; "
              f"{n_chunks} chunks would be re-embedded (~${est_usd:.2f})\n")
        for r in rows[:20]:
            print(f"   {r['n_chunks']:>5} chunks  {r['src_len']:>9,} chars  {r['title'][:62]}")
        if len(rows) > 20:
            print(f"   ... and {len(rows) - 20} more")

        if a.dry_run:
            print("\ndry run; nothing changed")
            return

        todo = rows if a.all else rows[:(a.limit or len(rows))]
        embedder = OpenAIEmbeddingProvider(api_key=OPENAI_API_KEY, model=EMBEDDING_MODEL,
                                           dimension=EMBEDDING_DIMENSION)
        chunker = TextChunker(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)

        print(f"\nre-chunking {len(todo)} document(s)\n")
        for r in todo:
            rid = r["rid"]
            before = await structure_counts(conn, rid)
            text = await conn.fetchval(
                "SELECT content->>'text' FROM koi_memories WHERE rid = $1", rid)
            if not text:
                print(f"   SKIP {rid[:40]} — no stored text")
                continue

            chunks = chunker.chunk_text(text)
            # Embed everything BEFORE touching the existing rows, so a failure here
            # leaves the document's current chunks in place rather than deleting them.
            #
            # ONE request per document, not one per chunk. The original loop made a
            # round trip per chunk, which is fine for a few hundred documents and takes
            # most of a day for tens of thousands of chunks. `embed_batch` puts the
            # whole document in a single
            # embeddings call — a 40-chunk document is ~20k tokens, far under the
            # per-request limit — and returns them in order.
            #
            # Falls back to per-chunk on a batch failure rather than losing the whole
            # document to one bad chunk, which is what the per-chunk loop bought and
            # is worth keeping.
            embeddings: list = []
            try:
                embeddings = list(await embedder.embed_batch([c["text"] for c in chunks]))
            except Exception as e:                          # noqa: BLE001
                if _is_terminal(e):
                    raise ProviderExhausted(str(e)) from e
                print(f"   batch embed failed on {rid[:36]} ({e}); falling back per-chunk")
                embeddings = []
                for c in chunks:
                    try:
                        embeddings.append(await embedder.embed(c["text"]))
                    except Exception as e2:                 # noqa: BLE001
                        if _is_terminal(e2):
                            raise ProviderExhausted(str(e2)) from e2
                        print(f"   embed failed on {rid[:36]} chunk {c['index']}: {e2}")
                        embeddings.append(None)
            if sum(1 for e in embeddings if e is None) > len(embeddings) // 2:
                print(f"   ABORT {rid[:40]} — over half of embeddings failed, "
                      f"leaving existing chunks untouched")
                continue

            async with conn.transaction():
                written = await upsert_document_chunks(
                    conn, rid, chunks, embeddings,
                    {"name": r["title"], "group_id": "personal"})

            after = await structure_counts(conn, rid)
            print(f"   {r['title'][:52]:<52} {before['n']:>4} -> {written:>4} chunks  "
                  f"newlines {before['nl']}->{after['nl']}  columns {before['cols']}->{after['cols']}")
            if after["nl"] == 0:
                print(f"      ⚠ still no newlines after re-chunk — investigate {rid}")
    except ProviderExhausted as e:
        # Stop the whole run, loudly and immediately. Everything committed so far is
        # complete and correct — the transaction is per document — and the candidate
        # query only ever selects still-damaged rows, so re-running after a top-up
        # resumes exactly where this stopped with no bookkeeping.
        print(f"\nSTOPPED — the embedding account is exhausted or the key is rejected:\n"
              f"  {e}\n"
              f"Documents already rewritten are complete and stay repaired. Top up, then\n"
              f"re-run the same command; it resumes from what is still damaged.\n"
              f"Exiting 3 so a caller can tell this apart from a normal finish.",
              file=sys.stderr)
        return 3
    finally:
        await conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
