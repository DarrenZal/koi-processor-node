#!/usr/bin/env python3
"""KOI ingestion governor v1 — deterministic, no-LLM document profiler (Piece A / G1).

Profiles a document (markdown or PDF) and PRINTS a recommended `{tier, fields}`
plus an up-front cost estimate and the exact, copy-paste `ingest_document.py`
command. SUGGESTION-MODE ONLY: it never ingests, never writes the DB, and makes
no `claude -p` / LLM call. The single permitted spend is ONE OpenAI embedding of
the document head (to score relevance against existing learning-field centroids).

It mechanically prevents the silent-truncation footgun: at `thorough` a long doc
truncates at the extractor's MAX_WINDOWS cap (extract_deep_documents.py). The GATE
rule downgrades a thorough candidate to standard (and always flags) when the
estimated window count exceeds that cap.

Usage:
    cd /path/to/koi-processor
    source config/personal.env
    python scripts/classify_document.py /path/to/foo/foo.md
    python scripts/classify_document.py foo.pdf --json
    python scripts/classify_document.py foo.md --slug foo --source-url https://... --name "Foo"

Determinism: every signal except `relevance` (one embedding cosine vs field
centroids) is a pure function of the file bytes. All thresholds are env vars with
the in-code defaults below; no config file, no magic numbers.
"""

import argparse
import asyncio
import json
import math
import os
import re
import shlex
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add repo root to path (mirror ingest_document.py) so `api.*` imports resolve.
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Config (env-with-default; no config file, no magic numbers) ──────────────────

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "3072"))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Reuse the extractor's window size so est_windows estimates the real truncation
# surface; default 45000 mirrors extract_deep_documents.WINDOW_CHARS.
DOC_WINDOW_CHARS = int(os.getenv("DOC_WINDOW_CHARS", "45000"))
# The deep-extract per-invocation window budget (extract_deep_documents.MAX_WINDOWS).
GATE_MAX_WINDOWS = int(os.getenv("DOC_MAX_WINDOWS", "12"))

# Relevance gating thresholds (cosine of doc-head embedding to field centroids).
CLASSIFY_MIN_FIELD_CHUNKS = int(os.getenv("CLASSIFY_MIN_FIELD_CHUNKS", "3"))
CLASSIFY_REL_PRIMARY = float(os.getenv("CLASSIFY_REL_PRIMARY", "0.62"))
CLASSIFY_REL_SECONDARY = float(os.getenv("CLASSIFY_REL_SECONDARY", "0.70"))
CLASSIFY_REL_THOROUGH = float(os.getenv("CLASSIFY_REL_THOROUGH", "0.70"))
CLASSIFY_DENSITY_MIN = float(os.getenv("CLASSIFY_DENSITY_MIN", "6.0"))
# How much of the doc head to embed for relevance (chars).
CLASSIFY_HEAD_CHARS = int(os.getenv("CLASSIFY_HEAD_CHARS", "8000"))
# The safe default field when relevance is unavailable (never guesses a topic).
DEFAULT_FIELD = os.getenv("CLASSIFY_DEFAULT_FIELD", "personal")
# Sentinel global field never used as a centroid.
GLOBAL_FIELD = "__global__"

SUPPORTED_TEXT_EXT = {".md", ".markdown", ".txt"}
PDF_EXT = {".pdf"}


class ClassifyError(Exception):
    """Clean, operator-facing failure (single-line diagnostic; non-zero exit)."""


# ── Profiler (pure: signals are a function of the file bytes only) ───────────────

# density_proxy cue families (no LLM). cue_count per 1000 words.
_CUE_PATTERNS = [
    # argumentative
    r"\bwe\s+(?:argue|claim|propose|contend|show|prove|demonstrate)\b",
    r"\bhowever\b", r"\bin\s+contrast\b", r"\bwe\s+disagree\b",
    r"\bcounterexample\b", r"\bobjection\b",
    # scientific-structure
    r"\btheorem\b", r"\blemma\b", r"\bdefinition\b", r"\bproposition\b",
    r"\bhypothesis\b", r"\bwe\s+conjecture\b",
    # inquiry
    r"\bopen\s+question\b", r"\bfuture\s+work\b", r"\bremains\s+unclear\b",
    r"\bit\s+is\s+unknown\b",
]
_CUE_RE = re.compile("|".join(_CUE_PATTERNS), re.IGNORECASE)

# doc_type cue regexes.
_PAPER_CUES = re.compile(
    r"\babstract\b|\barxiv\b|\bwe\s+propose\b|\breferences\b|\bcitation\b|"
    r"\btheorem\b|\bproposition\b|\b§\s*\d|\bequation\b|\bpreprint\b",
    re.IGNORECASE)
_SURVEY_CUES = re.compile(r"\bsurvey\b|\ba\s+review\s+of\b|\btaxonomy\b|\bsystematic\s+review\b", re.IGNORECASE)
_DATASET_CUES = re.compile(
    r"\bdataset\b|\bbenchmark\b|\bleaderboard\b|\bwe\s+(?:introduce|release|present)\s+(?:the\s+)?\w+\s+dataset\b|"
    r"\btrain(?:ing)?\s*/\s*(?:val|test)\b|\bcsv\b|\bdata\s+card\b",
    re.IGNORECASE)


def detect_doc_type(text: str, path: Path) -> Tuple[str, bool]:
    """Return (doc_type, is_benchmark_paper). doc_type ∈ {readme, dataset, survey, paper, essay}.

    Deterministic: filename + lightweight content regex. is_benchmark_paper is True
    when a dataset-flavored doc also carries paper structure (abstract/references/
    theorems) — those lift from rag to standard per rubric R1.
    """
    name = path.name.lower()
    head = text[:20000]
    paper_like = bool(_PAPER_CUES.search(head))

    if name.startswith("readme") or name == "readme.md":
        return "readme", False

    dataset_like = bool(_DATASET_CUES.search(head))
    if dataset_like:
        # A benchmark *paper* carries paper structure → not a bare dataset.
        return "dataset", paper_like

    if _SURVEY_CUES.search(head):
        return "survey", False
    if paper_like:
        return "paper", False
    return "essay", False


def density_proxy(text: str) -> Tuple[float, int, int]:
    """Argument-structure cue density = 1000 * cue_count / word_count (deterministic).

    Returns (density_proxy, cue_count, word_count). A dataset/benchmark scores low;
    a theory/argument paper scores high. Explicitly a proxy — the LLM-based
    argument_density classifier is v2 (Parking Lot).
    """
    words = re.findall(r"\b\w+\b", text)
    word_count = len(words)
    cue_count = len(_CUE_RE.findall(text))
    if word_count == 0:
        return 0.0, cue_count, 0
    return 1000.0 * cue_count / word_count, cue_count, word_count


def profile_document(text: str, path: Path) -> Dict[str, Any]:
    """Pure profiler — every signal is a function of the file bytes (no DB/network)."""
    char_count = len(text)
    est_windows = max(1, math.ceil(char_count / DOC_WINDOW_CHARS))
    doc_type, is_benchmark_paper = detect_doc_type(text, path)
    density, cue_count, word_count = density_proxy(text)
    return {
        "char_count": char_count,
        "est_windows": est_windows,
        "doc_type": doc_type,
        "is_benchmark_paper": is_benchmark_paper,
        "density_proxy": round(density, 2),
        "cue_count": cue_count,
        "word_count": word_count,
    }


# ── File loading (markdown direct; PDF via pymupdf4llm) ───────────────────────────

def load_markdown(path: Path) -> str:
    """Load a document as markdown. Markdown/txt read directly; PDF converted via
    pymupdf4llm. Raises ClassifyError (clean, AC7) on unsupported / unreadable /
    empty inputs."""
    if not path.exists() or not path.is_file():
        raise ClassifyError(f"not a readable file: {path}")
    ext = path.suffix.lower()
    if ext in SUPPORTED_TEXT_EXT:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            raise ClassifyError(f"could not read {path}: {e}") from e
    elif ext in PDF_EXT:
        try:
            import pymupdf4llm  # noqa: PLC0415 — optional dep, only for the PDF path
        except ImportError as e:
            raise ClassifyError(
                f"PDF input needs pymupdf4llm (pip install pymupdf4llm), or pass the "
                f"converted markdown instead: {path}") from e
        try:
            text = pymupdf4llm.to_markdown(str(path))
        except Exception as e:  # noqa: BLE001 — corrupt/unreadable PDF → clean error
            raise ClassifyError(f"could not convert PDF {path}: {e}") from e
    else:
        raise ClassifyError(
            f"unsupported file type {ext!r} (expected .md/.markdown/.txt/.pdf): {path}")
    if not text or not text.strip():
        raise ClassifyError(f"document is empty (0 usable chars): {path}")
    return text


# ── Relevance via field centroids (read-only DB; one embedding call) ─────────────

async def compute_field_relevance(
    conn, doc_embedding: List[float], *, min_chunks: int = CLASSIFY_MIN_FIELD_CHUNKS,
) -> List[Tuple[str, float, int]]:
    """Return [(field_id, cosine, n_chunks)] for each ELIGIBLE field centroid,
    sorted by cosine desc. READ-ONLY: a single SELECT (centroid = element-wise mean
    of member chunk embeddings, computed server-side via avg()). Fields with
    < min_chunks member chunks, and the __global__ sentinel, are excluded."""
    emb_str = "[" + ",".join(str(x) for x in doc_embedding) + "]"
    rows = await conn.fetch(
        """
        WITH centroids AS (
            SELECT m.field_id AS field_id,
                   avg(c.embedding_3072) AS centroid,
                   count(*) AS n
            FROM document_field_membership m
            JOIN koi_memory_chunks c ON c.document_rid = m.document_rid
            WHERE c.embedding_3072 IS NOT NULL
              AND m.field_id <> $3
            GROUP BY m.field_id
            HAVING count(*) >= $2
        )
        SELECT field_id, n,
               1 - (centroid::halfvec(3072) <=> $1::halfvec(3072)) AS cos
        FROM centroids
        ORDER BY cos DESC
        """,
        emb_str, min_chunks, GLOBAL_FIELD,
    )
    return [(r["field_id"], float(r["cos"]), int(r["n"])) for r in rows]


async def embed_doc_head(text: str, *, api_key: str = "") -> List[float]:
    """One OpenAI embedding of the doc head (the only spend). Raises on failure."""
    from api.embedding_provider import OpenAIEmbeddingProvider  # noqa: PLC0415
    key = api_key or OPENAI_API_KEY
    provider = OpenAIEmbeddingProvider(
        api_key=key, model=EMBEDDING_MODEL, dimension=EMBEDDING_DIMENSION)
    return await provider.embed(text[:CLASSIFY_HEAD_CHARS])


# ── Rubric (pure, deterministic) ─────────────────────────────────────────────────

def recommend_tier(
    signals: Dict[str, Any],
    relevance: Optional[float],
    field_cosines: List[Tuple[str, float, int]],
) -> Dict[str, Any]:
    """Deterministic tier+fields rubric, evaluated GATE-first then R1, R3, R4, R6.

    Returns {tier, fields, reasons[], flags[], relevance, breadth_note}.
    GATE has top precedence: a thorough candidate whose est_windows exceeds the
    deep-extract cap is downgraded to standard (truncation prevented); the flag
    fires whenever est_windows > cap.
    """
    est = signals["est_windows"]
    dt = signals["doc_type"]
    density = signals["density_proxy"]
    reasons: List[str] = []
    flags: List[str] = []
    breadth_note: Optional[str] = None

    # ── Breadth (fields) ──
    if relevance is None:
        fields = [DEFAULT_FIELD]
        breadth_note = (
            "relevance unavailable (sparse fields) — set --group-id/--fields "
            "manually if this belongs to a topic field")
    else:
        top_field, top_cos, _ = field_cosines[0]
        primary = top_field if top_cos >= CLASSIFY_REL_PRIMARY else DEFAULT_FIELD
        secondary = [
            fid for fid, cos, _n in field_cosines
            if cos >= CLASSIFY_REL_SECONDARY and fid != primary
        ]
        fields = [primary] + secondary

    # ── Tier candidate via R1, R3, R4, R6 (first match wins, top-down) ──
    if dt == "dataset":
        if signals["is_benchmark_paper"]:
            tier = "standard"
            reasons.append("R1: dataset reads as a benchmark *paper* (paper structure present) → standard")
        else:
            tier = "rag"
            reasons.append("R1: dataset → rag (chunk + embed only)")
    elif (dt in ("paper", "survey") and density >= CLASSIFY_DENSITY_MIN
          and relevance is not None and relevance >= CLASSIFY_REL_THOROUGH and est <= 8):
        tier = "thorough"
        reasons.append(
            f"R3: {dt}, density {density:.1f}≥{CLASSIFY_DENSITY_MIN}, relevance "
            f"{relevance:.2f}≥{CLASSIFY_REL_THOROUGH}, est_windows {est}≤8 → thorough")
    elif (dt in ("paper", "essay", "survey")
          and relevance is not None and relevance >= CLASSIFY_REL_PRIMARY):
        tier = "standard"
        reasons.append(f"R4: {dt}, relevance {relevance:.2f}≥{CLASSIFY_REL_PRIMARY} → standard")
    else:
        tier = "standard"
        reasons.append("R6: default → standard")

    # ── GATE (top precedence): prevent silent truncation at the window cap ──
    if est > GATE_MAX_WINDOWS:
        if tier == "thorough":
            tier = "standard"
            flags.append(
                f"GATE: thorough → standard — est_windows {est} > {GATE_MAX_WINDOWS}-window "
                f"deep-extract cap (thorough would silently truncate ~{est - GATE_MAX_WINDOWS} "
                f"windows of extraction)")
        else:
            flags.append(
                f"GATE: est_windows {est} > {GATE_MAX_WINDOWS}-window cap — a thorough run "
                f"would truncate at {GATE_MAX_WINDOWS} windows; {tier} is safe")

    return {
        "tier": tier,
        "fields": fields,
        "reasons": reasons,
        "flags": flags,
        "relevance": relevance,
        "breadth_note": breadth_note,
    }


def predicted_cost_line(tier: str, est_windows: int) -> str:
    """Up-front cost estimate. rag = embeddings only (0 LLM calls); standard/thorough
    = ~1 claude -p extraction call per window, capped at the window budget."""
    capped = min(est_windows, GATE_MAX_WINDOWS)
    if tier == "rag":
        return (f"rag: 0 claude -p calls (chunk + embed only); ~{est_windows} chunk-window(s) "
                f"embedded ≈ <1 min, subscription")
    per_min = 0.9 if tier == "thorough" else 0.6
    mins = round(capped * per_min, 1)
    trunc = ""
    if est_windows > capped:
        trunc = f" (capped from {est_windows}; {est_windows - capped} window(s) skipped)"
    return (f"{tier}: ~{capped} claude -p extraction call(s) over {capped} window(s){trunc} "
            f"≈ ~{mins} min, subscription")


def build_command(
    path: Path, tier: str, fields: List[str], *,
    slug: Optional[str], source_url: Optional[str], name: Optional[str],
    relevance: Optional[float],
) -> str:
    """The exact, copy-paste ingest_document.py command. --group-id = primary field;
    --fields = the additional (secondary) fields."""
    primary = fields[0]
    extra = fields[1:]
    parts = [
        "python scripts/ingest_document.py",
        f"--source-path {shlex.quote(str(path))}",
        f"--tier {tier}",
        f"--group-id {shlex.quote(primary)}",
    ]
    if extra:
        parts.append(f"--fields {shlex.quote(','.join(extra))}")
    if slug:
        parts.append(f"--slug {shlex.quote(slug)}")
    if source_url:
        parts.append(f"--source-url {shlex.quote(source_url)}")
    if name:
        parts.append(f"--name {shlex.quote(name)}")
    cmd = " ".join(parts)
    if relevance is None:
        cmd += ("  # relevance unavailable (sparse fields) — set --group-id/--fields "
                "manually if this belongs to a topic field")
    return cmd


# ── Orchestrator (read-only; ≤1 embedding; never writes / never ingests) ─────────

async def classify(
    path: Path, *,
    slug: Optional[str] = None,
    source_url: Optional[str] = None,
    name: Optional[str] = None,
    conn=None,
    doc_embedding: Optional[List[float]] = None,
) -> Dict[str, Any]:
    """Profile `path` and return the full recommendation dict. SUGGESTION-MODE:
    no DB writes, no ingestion, no LLM call (≤1 embedding for relevance).

    `conn` / `doc_embedding` are injectable for tests (no-DB-write guard, fake
    embedder). When omitted, a connection is opened (read-only use) and one OpenAI
    embedding is taken on the doc head.
    """
    text = load_markdown(path)
    signals = profile_document(text, path)

    # Relevance: one embedding + read-only centroid cosines. Degrade to null on a
    # missing key (fatal, raised) handled by caller; <2 eligible centroids → null.
    if doc_embedding is None:
        if not (OPENAI_API_KEY):
            raise ClassifyError(
                "OPENAI_API_KEY not set — required for relevance scoring "
                "(source config/personal.env or export the key)")
        try:
            doc_embedding = await embed_doc_head(text)
        except Exception as e:  # noqa: BLE001 — embed network blip → degrade to null
            doc_embedding = None
            signals["relevance_error"] = f"embedding failed: {e}"

    own_conn = False
    field_cosines: List[Tuple[str, float, int]] = []
    if doc_embedding is not None:
        if conn is None:
            import asyncpg  # noqa: PLC0415
            try:
                conn = await asyncpg.connect(POSTGRES_URL)
            except Exception as e:  # noqa: BLE001 — no DB → clean failure (AC7)
                raise ClassifyError(f"could not connect to the database: {e}") from e
            own_conn = True
        try:
            field_cosines = await compute_field_relevance(conn, doc_embedding)
        finally:
            if own_conn:
                await conn.close()

    # <2 eligible centroids → relevance untrustworthy → null (rubric falls back).
    relevance = field_cosines[0][1] if len(field_cosines) >= 2 else None

    rec = recommend_tier(signals, relevance, field_cosines)

    command = build_command(
        path, rec["tier"], rec["fields"],
        slug=slug or path.stem, source_url=source_url, name=name,
        relevance=rec["relevance"])
    cost = predicted_cost_line(rec["tier"], signals["est_windows"])

    return {
        "path": str(path),
        "recommended_tier": rec["tier"],
        "fields": rec["fields"],
        "est_windows": signals["est_windows"],
        "predicted_cost_line": cost,
        "exact_command": command,
        "doc_type": signals["doc_type"],
        "char_count": signals["char_count"],
        "density_proxy": signals["density_proxy"],
        "relevance": rec["relevance"],
        "field_cosines": [
            {"field_id": fid, "cosine": round(cos, 4), "n_chunks": n}
            for fid, cos, n in field_cosines
        ],
        "reasons": rec["reasons"],
        "flags": rec["flags"],
        "breadth_note": rec["breadth_note"],
        "signals": signals,
    }


def render_block(result: Dict[str, Any]) -> str:
    """Human-readable recommendation block."""
    lines = []
    lines.append("KOI ingestion governor — suggestion (no DB write, no ingest):")
    lines.append(f"  file:            {result['path']}")
    lines.append(f"  doc_type:        {result['doc_type']}  (char_count {result['char_count']:,})")
    lines.append(f"  est_windows:     {result['est_windows']}")
    lines.append(f"  density_proxy:   {result['density_proxy']}")
    rel = result["relevance"]
    lines.append(f"  relevance:       {rel:.3f}" if rel is not None else "  relevance:       null (sparse fields)")
    if result["field_cosines"]:
        top = ", ".join(f"{c['field_id']}={c['cosine']:.3f}" for c in result["field_cosines"][:5])
        lines.append(f"  field_cosines:   {top}")
    lines.append("")
    lines.append(f"  RECOMMENDED TIER: {result['recommended_tier']}")
    lines.append(f"  FIELDS:           {', '.join(result['fields'])}")
    lines.append(f"  COST:             {result['predicted_cost_line']}")
    for r in result["reasons"]:
        lines.append(f"  reason:          {r}")
    for f in result["flags"]:
        lines.append(f"  ⚠ FLAG:          {f}")
    if result["breadth_note"]:
        lines.append(f"  note:            {result['breadth_note']}")
    lines.append("")
    lines.append("  RUN:")
    lines.append(f"    {result['exact_command']}")
    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path", help="Path to the document (.md/.markdown/.txt or .pdf)")
    parser.add_argument("--json", action="store_true", help="Emit the recommendation as JSON")
    parser.add_argument("--slug", help="Stable slug for the ingest command (default: file stem)")
    parser.add_argument("--source-url", help="Canonical source URL to thread into the command")
    parser.add_argument("--name", help="Human-readable document title for the command")
    args = parser.parse_args(argv)

    try:
        result = asyncio.run(classify(
            Path(args.path).expanduser(),
            slug=args.slug, source_url=args.source_url, name=args.name))
    except ClassifyError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(render_block(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
