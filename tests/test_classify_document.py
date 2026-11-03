"""Unit tests for scripts/classify_document.py — the no-LLM ingestion governor.

Covers AC1 (no DB writes, ≤1 embedding), AC2 (GATE flag / dataset / paper),
AC7 (failure-safety paths), and the relevance=null fallback. Everything is
deterministic + injectable: no real DB, no OpenAI call (doc_embedding injected).
"""
import asyncio
from pathlib import Path

import pytest

import scripts.classify_document as cd


# ── Fakes ────────────────────────────────────────────────────────────────────

class WriteGuardConn:
    """A fake connection whose only allowed op is SELECT (conn.fetch). Any write
    (execute/executemany/fetchval/copy) raises — mechanically proves AC1 no-writes."""

    def __init__(self, fetch_rows):
        self._rows = fetch_rows

    async def fetch(self, sql, *args):
        # Defensive: classify must only ever issue read-only SELECTs.
        assert sql.lstrip().upper().startswith(("WITH", "SELECT")), sql
        return list(self._rows)

    async def execute(self, *a, **k):
        raise AssertionError("classify_document attempted a DB write (execute)")

    async def executemany(self, *a, **k):
        raise AssertionError("classify_document attempted a DB write (executemany)")

    async def fetchval(self, *a, **k):
        raise AssertionError("classify_document attempted fetchval")

    async def close(self):
        pass


def _centroids(*pairs):
    """Build fake centroid rows: (field_id, cosine, n_chunks)."""
    return [{"field_id": f, "cos": c, "n": n} for f, c, n in pairs]


EMB = [0.01] * cd.EMBEDDING_DIMENSION


def _run(coro):
    return asyncio.run(coro)


# ── Profiler determinism ──────────────────────────────────────────────────────

def test_est_windows_formula(tmp_path):
    p = tmp_path / "x.md"
    p.write_text("a" * (45000 * 3 + 1))
    sig = cd.profile_document(p.read_text(), p)
    # ceil((135001)/45000) = 4
    assert sig["est_windows"] == 4


def test_density_proxy_counts_cue_families():
    text = ("We propose a theorem. However, this lemma is an open question. "
            "We prove the proposition; future work remains unclear.")
    density, cue_count, words = cd.density_proxy(text)
    assert cue_count >= 6
    assert density > 0


def test_doc_type_dataset_vs_paper():
    assert cd.detect_doc_type("We release the WidgetBench dataset with a leaderboard.",
                              Path("widgetbench.md"))[0] == "dataset"
    assert cd.detect_doc_type("Abstract. We propose. Theorem 1. References.",
                              Path("paper.md"))[0] == "paper"


# ── AC2: rubric — dataset → rag, benchmark-paper → standard, paper → thorough ──

def test_dataset_recommends_rag(tmp_path):
    p = tmp_path / "data.md"
    p.write_text("This dataset / benchmark provides a leaderboard and a data card. " * 50)
    conn = WriteGuardConn(_centroids(("personal", 0.4, 352), ("spore", 0.3, 63)))
    res = _run(cd.classify(p, conn=conn, doc_embedding=EMB))
    assert res["doc_type"] == "dataset"
    assert res["recommended_tier"] == "rag"


def test_benchmark_paper_lifts_to_standard(tmp_path):
    p = tmp_path / "bench.md"
    # dataset cues AND paper structure (abstract/references/theorem) → benchmark paper.
    p.write_text(
        "Abstract. We introduce the FooBench dataset with a leaderboard. "
        "Theorem 1 holds. References. " * 50)
    conn = WriteGuardConn(_centroids(("personal", 0.5, 352), ("spore", 0.3, 63)))
    res = _run(cd.classify(p, conn=conn, doc_embedding=EMB))
    assert res["doc_type"] == "dataset"
    assert res["signals"]["is_benchmark_paper"] is True
    assert res["recommended_tier"] == "standard"


def test_short_dense_relevant_paper_is_thorough(tmp_path):
    p = tmp_path / "paper.md"
    body = ("Abstract. We propose. We argue and we prove this theorem. "
            "However, this lemma and proposition remain an open question. "
            "Definition follows. References. ")
    p.write_text(body * 30)  # small → est_windows == 1
    conn = WriteGuardConn(_centroids(("sheaf-explorer", 0.81, 2300),
                                     ("spore", 0.40, 63)))
    res = _run(cd.classify(p, conn=conn, doc_embedding=EMB))
    assert res["doc_type"] == "paper"
    assert res["est_windows"] <= 8
    assert res["relevance"] >= cd.CLASSIFY_REL_THOROUGH
    assert res["recommended_tier"] == "thorough"
    assert res["fields"][0] == "sheaf-explorer"


# ── AC2: GATE — a long, dense, relevant paper is capped at standard + flagged ──

def test_gate_caps_long_paper_to_standard_with_flag(tmp_path):
    p = tmp_path / "big.md"
    unit = ("Abstract. We propose and we prove this theorem. However the lemma "
            "and proposition remain an open question. Definition. References. ")
    # est_windows > GATE_MAX_WINDOWS (12): need > 12*45000 = 540000 chars.
    reps = (cd.GATE_MAX_WINDOWS * cd.DOC_WINDOW_CHARS // len(unit)) + 200
    p.write_text(unit * reps)
    conn = WriteGuardConn(_centroids(("sheaf-explorer", 0.81, 2300),
                                     ("spore", 0.40, 63)))
    res = _run(cd.classify(p, conn=conn, doc_embedding=EMB))
    assert res["est_windows"] > cd.GATE_MAX_WINDOWS
    # A long doc can never be thorough (R3 caps at est<=8) → standard, and the
    # GATE flag warns thorough would truncate.
    assert res["recommended_tier"] == "standard"
    assert any("GATE" in f for f in res["flags"])


def test_gate_downgrade_branch_direct():
    """Defensive: recommend_tier downgrades an explicit thorough candidate when
    est_windows exceeds the cap (the rubric never produces this combo, but the
    safety net must hold if R3's threshold ever moves)."""
    signals = {"est_windows": 20, "doc_type": "paper", "density_proxy": 99.0,
               "is_benchmark_paper": False}
    # Force the thorough path by satisfying R3 except est: temporarily it would be
    # standard via R4 then GATE-flagged. Assert the flag + standard outcome.
    rec = cd.recommend_tier(signals, relevance=0.9,
                            field_cosines=[("sheaf-explorer", 0.9, 2300),
                                           ("spore", 0.4, 63)])
    assert rec["tier"] == "standard"
    assert any("GATE" in f for f in rec["flags"])


# ── relevance=null fallback (AC7: <2 eligible centroids) ──────────────────────

def test_relevance_null_when_fewer_than_two_centroids(tmp_path):
    p = tmp_path / "paper.md"
    p.write_text("Abstract. We propose. Theorem. References. " * 40)
    conn = WriteGuardConn(_centroids(("sheaf-explorer", 0.81, 2300)))  # only 1
    res = _run(cd.classify(p, conn=conn, doc_embedding=EMB))
    assert res["relevance"] is None
    assert res["fields"] == ["personal"]
    assert res["breadth_note"]
    # The command carries the explicit manual-field note.
    assert "relevance unavailable" in res["exact_command"]
    # Still a clean recommendation (exit-0 semantics), defaults to standard.
    assert res["recommended_tier"] in ("rag", "standard")


def test_relevance_null_emits_recommendation_via_main(tmp_path, monkeypatch, capsys):
    p = tmp_path / "paper.md"
    p.write_text("Abstract. We propose. Theorem. References. " * 40)

    # Drive the REAL classify path end-to-end via main(): fake the one embedding
    # call and the DB connect (returns a single-centroid read-only conn → null).
    import asyncpg

    async def fake_embed(text, **kw):
        return EMB

    async def fake_connect(*a, **k):
        return WriteGuardConn(_centroids(("spore", 0.5, 63)))  # only 1 → relevance null

    monkeypatch.setattr(cd, "embed_doc_head", fake_embed)
    monkeypatch.setattr(cd, "OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(asyncpg, "connect", fake_connect)
    rc = cd.main([str(p), "--json"])
    assert rc == 0  # <2 centroids is exit-0, not a failure
    out = capsys.readouterr().out
    assert '"relevance": null' in out


# ── AC1: no DB writes proven by the WriteGuardConn (above tests would raise) ───

def test_no_db_write_enforced(tmp_path):
    p = tmp_path / "paper.md"
    p.write_text("Abstract. We propose. Theorem. References. " * 40)
    conn = WriteGuardConn(_centroids(("sheaf-explorer", 0.81, 2300), ("spore", 0.4, 63)))
    # If classify issued any write, WriteGuardConn.execute would raise.
    res = _run(cd.classify(p, conn=conn, doc_embedding=EMB))
    assert res["recommended_tier"] in ("rag", "standard", "thorough")


# ── AC7: failure-safety paths ─────────────────────────────────────────────────

def test_missing_openai_key_is_clean_error(tmp_path, monkeypatch):
    p = tmp_path / "paper.md"
    p.write_text("Abstract. We propose. " * 40)
    monkeypatch.setattr(cd, "OPENAI_API_KEY", "")
    with pytest.raises(cd.ClassifyError):
        _run(cd.classify(p))  # doc_embedding=None → needs the key


def test_missing_openai_key_main_returns_nonzero(tmp_path, monkeypatch):
    p = tmp_path / "paper.md"
    p.write_text("Abstract. We propose. " * 40)
    monkeypatch.setattr(cd, "OPENAI_API_KEY", "")
    assert cd.main([str(p)]) == 1


def test_no_db_connection_is_clean_error(tmp_path, monkeypatch):
    p = tmp_path / "paper.md"
    p.write_text("Abstract. We propose. Theorem. References. " * 40)
    import asyncpg

    async def boom(*a, **k):
        raise OSError("no route to host")
    monkeypatch.setattr(asyncpg, "connect", boom)
    # doc_embedding provided → skips the key + embed, goes straight to connect.
    with pytest.raises(cd.ClassifyError):
        _run(cd.classify(p, doc_embedding=EMB))


def test_empty_document_is_clean_error(tmp_path):
    p = tmp_path / "empty.md"
    p.write_text("   \n\t  ")
    with pytest.raises(cd.ClassifyError):
        cd.load_markdown(p)


def test_unsupported_file_type_is_clean_error(tmp_path):
    p = tmp_path / "thing.xyz"
    p.write_text("hello")
    with pytest.raises(cd.ClassifyError):
        cd.load_markdown(p)


def test_corrupt_pdf_is_clean_error(tmp_path):
    p = tmp_path / "broken.pdf"
    p.write_bytes(b"%PDF-1.4 not actually a pdf \x00\x01\x02")
    with pytest.raises(cd.ClassifyError):
        cd.load_markdown(p)


def test_missing_file_main_returns_nonzero(tmp_path):
    assert cd.main([str(tmp_path / "nope.md")]) == 1


# ── command / cost builders ───────────────────────────────────────────────────

def test_build_command_threads_fields_and_group():
    cmd = cd.build_command(Path("/x/foo.md"), "thorough",
                           ["sheaf-explorer", "spore"],
                           slug="foo", source_url="https://arxiv.org/abs/1",
                           name="Foo", relevance=0.8)
    assert "--tier thorough" in cmd
    assert "--group-id sheaf-explorer" in cmd
    assert "--fields spore" in cmd
    assert "--source-url https://arxiv.org/abs/1" in cmd


def test_cost_line_rag_has_zero_llm_calls():
    assert "0 claude -p" in cd.predicted_cost_line("rag", 3)


def test_cost_line_caps_at_window_budget():
    line = cd.predicted_cost_line("standard", cd.GATE_MAX_WINDOWS + 8)
    assert "capped" in line
