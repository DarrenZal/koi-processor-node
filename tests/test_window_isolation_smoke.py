#!/usr/bin/env python3
"""Live smoke test for PER-WINDOW ISOLATION.

Proves the real code path: force ONE window to fail, assert the document still
completes over the remaining windows instead of being discarded entirely.

Before isolation, any single window error propagated out of extract_deep_document
and aborted the whole document, throwing away every window already extracted. On
a large corpus (2026-07-31) that cost four separate 30-50 minute passes, each
losing 7-78 successfully extracted windows to one stray key.

Asserts:
  1. the failing window ends status='failed' with its last_error recorded
  2. the other windows end status='imported'
  3. facts are still written from the surviving windows
  4. deep_extracted_at stays NULL (an incomplete doc must not read as done)
  5. the result dict reports windows_failed

Needs a test database and a KOI API writing to it, and spends ~2 LLM calls.
Writes into group_id='ingest-isolation-test' and DELETES everything it created.

Run:  KOI_TEST_DATABASE_URL=... KOI_TEST_API_URL=http://127.0.0.1:<throwaway port> \
      <venv>/bin/python tests/test_window_isolation_smoke.py
Refuses live-looking targets (see tests/koi_test_targets.py). pytest collects no tests here.
"""
import asyncio
import importlib.util
import os
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
GROUP = "ingest-isolation-test"
SLUG = "window-isolation-test"
FAIL_WINDOW = 1  # 0-based; the middle window of three

sys.path.insert(0, str(REPO))
# Stage the fixture inside the repo's own allowlisted incoming dir, so the test needs
# no operator-specific sources folder.
STAGE = REPO / "tmp" / "incoming"


def _configure() -> str:
    """Targets from KOI_TEST_DATABASE_URL and KOI_TEST_API_URL only, set before code loads.

    Runs from main(), never at import, so pytest collection changes no environment."""
    from tests import koi_test_targets as targets
    t = targets.require_for_script(targets.DATABASE_VAR, targets.API_VAR)
    os.environ["POSTGRES_URL"] = t[targets.DATABASE_VAR]
    os.environ["DOC_INGEST_KOI_URL"] = t[targets.API_VAR].rstrip("/")
    STAGE.mkdir(parents=True, exist_ok=True)
    os.environ["INGEST_SOURCE_ROOT"] = str(STAGE)
    os.environ.setdefault("DOC_EXTRACTOR_TRANSPORT", "claude_p")
    os.environ.setdefault("DOC_EXTRACTOR_MODEL", "claude-haiku-4-5-20251001")
    os.environ.setdefault("DOC_EXTRACTOR_TIMEOUT", "900")
    os.environ.setdefault("DOC_EPISODE_TIMEOUT", "900")
    return t[targets.DATABASE_VAR]


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, str(REPO / rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


async def main() -> int:
    import asyncpg
    import httpx
    dsn = _configure()
    idoc = _load("ingest_document", "scripts/ingest_document.py")
    edd = _load("extract_deep_documents", "scripts/extract_deep_documents.py")

    # ~100k chars of REAL prose (the repo's own docs) -> 3 windows at the 45k default.
    # Real prose matters: the test asserts facts are still extracted from the surviving
    # windows, and synthetic filler would yield none.
    buf, total = [], 0
    for md in sorted((REPO / "docs").rglob("*.md")):
        try:
            text = md.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        buf.append(text)
        total += len(text)
        if total > 120000:
            break
    src_text = "\n\n".join(buf)[:100000]
    assert len(src_text) > 90000, f"need ~100k chars of prose, got {len(src_text)}"
    src = STAGE / f"{SLUG}.md"
    src.write_text(src_text)

    # Stage 1: RAG chunks only, so the deep-extract step has windows to walk.
    rag = await idoc.ingest_path(
        source_path=str(src), tier="rag", slug=SLUG, name="Window isolation test",
        source_url="https://example.invalid/isolation-test", retrieval_method="test",
        group_id=GROUP, fields=None, claims=False, force=False, dry_run=False)
    rid = rag["document_rid"]
    print(f"staged {rid} — {rag['rag']['chunks_written']} chunks")

    # Force exactly ONE window to fail, exactly as a schema violation would.
    real = edd.extract_window_validated
    seen = {"n": 0}

    async def flaky(prompt, http, schema, *, model):
        idx = seen["n"]
        seen["n"] += 1
        if idx == FAIL_WINDOW:
            raise edd.ExtractionError(
                "extract_parse_error",
                "schema: ['facts', 2, 'chunk_range']: [990] is too short (INJECTED BY TEST)")
        return await real(prompt, http, schema, model=model)

    edd.extract_window_validated = flaky
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    async with httpx.AsyncClient(timeout=900.0) as http:
        result = await edd.extract_deep_document(
            pool, http, document_rid=rid, tier="standard", group_id=GROUP,
            run_id="isolation-test", force=False)
    await pool.close()

    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            "SELECT window_index, status, coalesce(last_error,'') AS err "
            "FROM document_window_extractions WHERE document_rid=$1 ORDER BY window_index", rid)
        facts = await conn.fetchval(
            "SELECT count(*) FROM knowledge_facts WHERE group_id=$1", GROUP)
        dea = await conn.fetchval(
            "SELECT deep_extracted_at FROM document_ingestion_log WHERE document_rid=$1", rid)

        print("\n--- windows ---")
        for r in rows:
            print(f"  {r['window_index']}: {r['status']:9} {r['err'][:70]}")
        print(f"\nfacts written : {facts}")
        print(f"windows_failed: {result.get('windows_failed')} {result.get('windows_failed_idx')}")
        print(f"deep_extracted_at: {dea}")

        ok = True

        def check(cond, label):
            nonlocal ok
            print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
            ok = ok and cond

        print("\n--- assertions ---")
        st = {r["window_index"]: r["status"] for r in rows}
        check(st.get(FAIL_WINDOW) == "failed", f"window {FAIL_WINDOW} is 'failed'")
        check(any(r["window_index"] == FAIL_WINDOW and "INJECTED BY TEST" in (r["err"] or "")
                  for r in rows), "failed window recorded its last_error")
        others = [i for i in st if i != FAIL_WINDOW]
        check(bool(others) and all(st[i] == "imported" for i in others),
              f"surviving windows {others} are 'imported'")
        check(facts > 0, "facts were still written from surviving windows")
        check(result.get("windows_failed") == 1, "result reports windows_failed == 1")
        check(dea is None, "deep_extracted_at is NULL (incomplete doc not marked done)")
        return 0 if ok else 1
    finally:
        # Clean up everything this test created.
        await conn.execute("DELETE FROM knowledge_facts WHERE group_id=$1", GROUP)
        await conn.execute("DELETE FROM document_field_membership WHERE document_rid=$1", rid)
        await conn.execute("DELETE FROM document_window_extractions WHERE document_rid=$1", rid)
        await conn.execute("DELETE FROM document_ingestion_log WHERE document_rid=$1", rid)
        await conn.execute("DELETE FROM koi_memory_chunks WHERE document_rid=$1", rid)
        await conn.execute("DELETE FROM koi_memories WHERE rid=$1", rid)
        await conn.close()
        src.unlink(missing_ok=True)
        print("\ncleaned up test rows")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
