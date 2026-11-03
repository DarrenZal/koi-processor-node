#!/usr/bin/env python3
"""Batch-ingest research-paper folders into personal KOI.

This is a paper-corpus wrapper around scripts/ingest_document.py. It selects
paper folders with converted `extracted.md`, runs the thorough document ingest
when KOI lacks facts/discourse moves, stamps stable paper metadata onto the KOI
document row, and exports local artifacts back into the paper folder:

- discourse-elements.json
- triples.jsonl
- ingest-result.json

Default mode is conservative: process at most three `download_now` papers.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts import extract_deep_documents  # noqa: E402
from scripts import ingest_document  # noqa: E402


POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://koi:@localhost:5432/personal_koi",
)
DEFAULT_CORPUS_ROOT = Path("/Users/example/Documents/Research/Papers")
DEFAULT_AUTHOR = "ghrist-robert"
DEFAULT_GROUP_ID = "sheaf-explorer"
ENTITY_OBJECT_PREDICATES = frozenset({"AUTHORED_BY"})
PAPER_LEDGER_SCHEMA = "personal-koi-paper-ledger-v1"
SOURCE_LEDGER_SCHEMA = "personal-koi-paper-source-ledger-v1"
ISSUE_LOG_SCHEMA = "personal-koi-paper-ingestion-issue-v1"
BACKTEST_REPORT_SCHEMA = "personal-koi-paper-backtest-v1"
ISSUE_LOG_NAME = "ingestion-issue-log.jsonl"
PROMOTION_CANDIDATE_LIMIT = 12
MIN_EXTRACTED_WORDS_FOR_TEXT_READY = 500
MAX_SUSPICIOUS_CHAR_RATIO_FOR_TEXT_READY = 0.02
MIN_COMMON_WORD_RATIO_FOR_TEXT_READY = 0.03
COMMON_ENGLISH_WORDS = frozenset({
    "a",
    "an",
    "and",
    "are",
    "as",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "this",
    "to",
    "we",
    "which",
    "with",
})
PROMOTION_TOPIC_WEIGHTS = {
    "participatory mapping": 10,
    "participatory sensing": 10,
    "public participation gis": 10,
    "ppgis": 10,
    "heterogeneous lens": 10,
    "stakeholder conflict": 9,
    "deep disagreement": 9,
    "irreducible disagreement": 9,
    "heterophily": 9,
    "values mapping": 9,
    "sheaf": 8,
    "sheaves": 8,
    "sheaf neural network": 8,
    "sheaf neural networks": 8,
    "neural sheaf": 8,
    "discourse": 8,
    "argumentation map": 8,
    "deliberative mapping": 8,
    "partial views": 8,
    "obstruction": 8,
    "seam": 8,
    "seams": 8,
    "sovereignty boundary": 8,
    "social information": 8,
    "belief": 7,
    "communication": 7,
    "coherence": 7,
    "frustration": 7,
    "multi criteria mapping": 7,
    "lattice": 7,
    "lattices": 7,
    "tarski": 7,
    "cohomology": 7,
    "network coding": 7,
    "argumentation": 6,
    "stakeholder": 6,
    "conflict": 6,
    "quiver": 6,
    "liability": 6,
    "directed hypergraph": 6,
    "hypergraph": 6,
    "neural network": 6,
    "oversmoothing": 5,
    "preference": 5,
    "gossip": 5,
    "local-to-global": 5,
    "distributed": 4,
    "multi-robot": 3,
    "coordination": 4,
    "sensor": 3,
    "network": 3,
    "graph": 3,
    "homology": 3,
    "homological": 3,
    "persistence": 3,
    "persistent": 3,
    "topological data": 3,
    "euler": 2,
    "robot": 1,
    "configuration space": 2,
    "braid": 2,
    "geometry": 2,
}
PROMOTION_DECISION_WEIGHTS = {
    "download_now": 12,
    "review_then_download": 6,
    "maybe": 1,
    "skip_for_now": -8,
    "duplicate": -20,
}
PROMOTION_SOURCE_WEIGHTS = {
    "scholarly_preprint": 4,
    "author_homepage": 3,
    "scholarly_publication": 2,
    "paywalled_metadata": -8,
}
DEEP_PROMOTION_THRESHOLD = 28
LIGHT_PROMOTION_THRESHOLD = 16
PROVES_SUBJECT_HINTS = frozenset(
    {"article", "corollary", "lemma", "paper", "proof", "proposition", "result", "theorem"}
)
NUMBERED_STATEMENT_RE = re.compile(
    r"\b(Theorem|Proposition|Lemma|Definition|Corollary)\s+([0-9]+(?:\.[0-9]+)*)\b",
    re.IGNORECASE,
)
ALGORITHM_TEXT_RE = re.compile(r"\b(Algorithm\s+\d+|distributed algorithm|distributed computation)\b", re.IGNORECASE)
ALGORITHM_OWNER_PREDICATES = frozenset({"COMPUTES", "HAS_BOUND", "HAS_PARAMETER"})
KG_EMBEDDING_MODEL_LABELS = (
    "Structured Embedding",
    "ExtensionTransE",
    "ExtensionSE",
    "NaiveTransE",
    "TransE",
    "TransR",
    "RotatE",
    "TorusE",
)
KG_EMBEDDING_MODEL_RE = re.compile(
    r"\b(" + "|".join(re.escape(label) for label in KG_EMBEDDING_MODEL_LABELS) + r")\b",
    re.IGNORECASE,
)
KG_MODEL_LEADING_RE = re.compile(
    r"^\s*(?:" + "|".join(re.escape(label) for label in KG_EMBEDDING_MODEL_LABELS) + r")\b",
    re.IGNORECASE,
)
SCIENTIFIC_LABEL_OVERMERGES = {
    "knowledgetab": ("knowledge sheaf",),
    "knowledge graph ui": ("knowledge graph completion",),
}
NUMBERED_REFERENCE_RE = re.compile(
    r"\b(Figure|Example|Theorem|Proposition|Lemma|Corollary)\s+([0-9]+(?:\.[0-9]+)*)\b",
    re.IGNORECASE,
)
NEGATIVE_EXISTENCE_RE = re.compile(r"\b(non[- ]existence|empty|no|not|without|absence)\b", re.IGNORECASE)
POSITIVE_EXISTENCE_RE = re.compile(r"\b(existence|exists|non[- ]empty|presence)\b", re.IGNORECASE)
DISCOURSE_TITLE_STOPWORDS = frozenset(
    {
        "and",
        "are",
        "but",
        "can",
        "for",
        "from",
        "has",
        "how",
        "into",
        "its",
        "may",
        "not",
        "that",
        "the",
        "their",
        "this",
        "through",
        "via",
        "with",
    }
)


@dataclass
class PaperCandidate:
    path: Path
    metadata_path: Path
    metadata: dict[str, Any]
    paper_id: str
    title: str
    year: int | None
    decision: str
    relevance_score: int
    source_url: str
    extracted_path: Path
    document_rid: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_yaml(path: Path) -> dict[str, Any]:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def relpath(path: Path, root: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def author_dir(corpus_root: Path, author_id: str) -> Path:
    return corpus_root / "authors" / author_id


def issue_log_path(corpus_root: Path, author_id: str) -> Path:
    return author_dir(corpus_root, author_id) / ISSUE_LOG_NAME


def stable_issue_id(issue: dict[str, Any]) -> str:
    fact_id = issue.get("fact_id")
    if issue.get("category") == "fact_validation" and issue.get("code") == "retired_invalid_fact":
        fact_id = None
    identity = {
        "paper_id": issue.get("paper_id"),
        "document_rid": issue.get("document_rid"),
        "category": issue.get("category"),
        "code": issue.get("code"),
        "detail": issue.get("detail"),
        "fact_id": fact_id,
        "fact_text": issue.get("fact_text"),
        "predicate": issue.get("predicate"),
        "warning": issue.get("warning"),
    }
    digest = hashlib.sha1(json.dumps(identity, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()
    return digest[:16]


def read_issue_log(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    issues: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            issues.append(item)
    return issues


def summarize_issue_log(path: Path) -> dict[str, Any]:
    issues = read_issue_log(path)
    summary: dict[str, Any] = {
        "path": path.name,
        "total": len(issues),
        "by_status": {},
        "by_category": {},
        "by_severity": {},
    }
    for issue in issues:
        for field, bucket in (
            ("status", "by_status"),
            ("category", "by_category"),
            ("severity", "by_severity"),
        ):
            value = str(issue.get(field) or "unspecified")
            summary[bucket][value] = summary[bucket].get(value, 0) + 1
    for key in ("by_status", "by_category", "by_severity"):
        summary[key] = dict(sorted(summary[key].items()))
    return summary


def append_issue_log(corpus_root: Path, author_id: str, issues: list[dict[str, Any]]) -> dict[str, Any]:
    path = issue_log_path(corpus_root, author_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_issue_log(path)
    existing_ids: set[str] = set()
    for item in existing:
        if item.get("issue_id"):
            existing_ids.add(str(item["issue_id"]))
        existing_ids.add(stable_issue_id(item))
    appended: list[dict[str, Any]] = []
    for raw in issues:
        issue = dict(raw)
        issue.setdefault("schema", ISSUE_LOG_SCHEMA)
        issue.setdefault("logged_at", now_iso())
        issue.setdefault("source", "ingest-research-papers")
        issue.setdefault("status", "open")
        issue.setdefault("severity", "review")
        issue["issue_id"] = str(issue.get("issue_id") or stable_issue_id(issue))
        if issue["issue_id"] in existing_ids:
            continue
        existing_ids.add(issue["issue_id"])
        appended.append(issue)

    if appended:
        with path.open("a", encoding="utf-8") as f:
            for issue in appended:
                f.write(json.dumps(issue, ensure_ascii=True, sort_keys=True) + "\n")
    return {"path": str(path), "appended": len(appended), "total": len(existing) + len(appended)}


def warning_code(warning: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", warning.lower()).strip("_")
    return normalized[:80] or "quality_warning"


def issues_from_processing(
    paper: PaperCandidate,
    quality_review: dict[str, Any],
    invalid_facts_retired: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for warning in as_list(quality_review.get("warnings")):
        warning_text = str(warning)
        issues.append(
            {
                "paper_id": paper.paper_id,
                "document_rid": paper.document_rid,
                "title": paper.title,
                "category": "quality_warning",
                "code": warning_code(warning_text),
                "severity": "review",
                "status": "open",
                "detail": warning_text,
                "warning": warning_text,
                "quality_verdict": quality_review.get("verdict"),
            }
        )
    for mismatch in as_list(quality_review.get("type_mismatches")):
        issues.append(
            {
                "paper_id": paper.paper_id,
                "document_rid": paper.document_rid,
                "title": paper.title,
                "category": "entity_resolution",
                "code": "unresolved_type_mismatch",
                "severity": "review",
                "status": "open",
                "detail": "unresolved entity type mismatch",
                "payload": mismatch,
            }
        )
    for fact in invalid_facts_retired:
        issues.append(
            {
                "paper_id": paper.paper_id,
                "document_rid": paper.document_rid,
                "title": paper.title,
                "category": "fact_validation",
                "code": "retired_invalid_fact",
                "severity": "low",
                "status": "fixed",
                "detail": str(fact.get("reason") or "invalid fact retired"),
                "fact_id": fact.get("id"),
                "predicate": fact.get("predicate"),
                "fact_text": fact.get("fact_text"),
                "object_literal": fact.get("object_literal"),
            }
        )
    return issues


def first_existing_pdf(path: Path) -> Path | None:
    pdfs = sorted(path.glob("*.pdf"))
    return pdfs[0] if pdfs else None


def extracted_text_metrics(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "word_count": 0,
            "suspicious_char_count": 0,
            "suspicious_char_ratio": 0.0,
            "common_word_ratio": 0.0,
            "quality": "missing",
        }
    text = path.read_text(encoding="utf-8", errors="replace")
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_'-]*", text)
    word_count = len(words)
    suspicious_char_count = sum(
        1
        for ch in text
        if ch == "\ufffd" or (unicodedata.category(ch)[0] == "C" and ch not in "\n\r\t\f")
    )
    suspicious_char_ratio = suspicious_char_count / max(1, len(text))
    common_word_count = sum(1 for word in words if word.lower() in COMMON_ENGLISH_WORDS)
    common_word_ratio = common_word_count / max(1, word_count)
    quality = "ok"
    if suspicious_char_ratio > MAX_SUSPICIOUS_CHAR_RATIO_FOR_TEXT_READY:
        quality = "mojibake_suspected"
    elif word_count >= MIN_EXTRACTED_WORDS_FOR_TEXT_READY and common_word_ratio < MIN_COMMON_WORD_RATIO_FOR_TEXT_READY:
        quality = "low_prose_signal"
    return {
        "word_count": word_count,
        "suspicious_char_count": suspicious_char_count,
        "suspicious_char_ratio": round(suspicious_char_ratio, 6),
        "common_word_ratio": round(common_word_ratio, 6),
        "quality": quality,
    }


def extracted_word_count(path: Path) -> int:
    return int(extracted_text_metrics(path)["word_count"])


def infer_source_tier(meta: dict[str, Any]) -> str:
    if meta.get("paywalled") or str(meta.get("pdf_status") or "").startswith("paywalled"):
        return "paywalled_metadata"
    if meta.get("arxiv_id") or "arxiv.org" in str(meta.get("source_url") or meta.get("pdf_url") or ""):
        return "scholarly_preprint"
    if "doi.org" in str(meta.get("source_url") or ""):
        return "scholarly_publication"
    if meta.get("official_url"):
        return "author_homepage"
    return "unknown"


def canonical_url(meta: dict[str, Any]) -> str:
    return str(meta.get("source_url") or meta.get("official_url") or meta.get("pdf_url") or "")


def build_paper_ledger_entry(meta_path: Path, corpus_root: Path, author_id: str) -> dict[str, Any]:
    meta = read_yaml(meta_path)
    paper_dir = meta_path.parent
    paper_id = str(meta.get("paper_id") or f"{author_id}/{paper_dir.name}")
    extracted = paper_dir / "extracted.md"
    document_rid = str(meta.get("document_rid") or "")
    if extracted.exists() and not document_rid:
        markdown = extracted.read_text(encoding="utf-8", errors="replace")
        _, document_rid = ingest_document.compute_document_rid(markdown)

    quality_path = paper_dir / "quality-review.json"
    quality = read_json(quality_path)
    ingest_result = read_json(paper_dir / "ingest-result.json")
    deep = meta.get("deep_ingestion") if isinstance(meta.get("deep_ingestion"), dict) else {}
    rag = meta.get("rag_ingestion") if isinstance(meta.get("rag_ingestion"), dict) else {}
    local_pdf = first_existing_pdf(paper_dir)
    extracted_metrics = extracted_text_metrics(extracted)
    extracted_words = int(extracted_metrics["word_count"])

    return {
        "schema": PAPER_LEDGER_SCHEMA,
        "paper_id": paper_id,
        "author_slug": author_id,
        "title": str(meta.get("title") or paper_dir.name),
        "year": int(meta["year"]) if str(meta.get("year") or "").isdigit() else None,
        "authors": [str(v) for v in as_list(meta.get("authors"))],
        "source": {
            "canonical_url": canonical_url(meta),
            "source_url": str(meta.get("source_url") or ""),
            "pdf_url": str(meta.get("pdf_url") or ""),
            "official_url": str(meta.get("official_url") or ""),
            "arxiv_id": str(meta.get("arxiv_id") or ""),
            "source_tier": infer_source_tier(meta),
            "checked_as_of": str(meta.get("created") or ""),
            "paywalled": bool(meta.get("paywalled")),
            "access_note": str(meta.get("access_note") or ""),
        },
        "routing": {
            "decision": str(meta.get("decision") or ""),
            "relevance_score": int(meta.get("relevance_score") or 0),
            "matched_topics": [str(v) for v in as_list(meta.get("matched_topics"))],
            "project_tags": [str(v) for v in as_list(meta.get("project_tags"))],
        },
        "local": {
            "folder": relpath(paper_dir, corpus_root),
            "metadata_path": relpath(meta_path, corpus_root),
            "pdf_path": relpath(local_pdf, corpus_root) if local_pdf else None,
            "extracted_path": relpath(extracted, corpus_root),
            "extracted_word_count": extracted_words,
            "extracted_text_quality": extracted_metrics["quality"],
            "extracted_suspicious_char_ratio": extracted_metrics["suspicious_char_ratio"],
            "extracted_common_word_ratio": extracted_metrics["common_word_ratio"],
            "discourse_path": relpath(paper_dir / "discourse-elements.json", corpus_root),
            "triples_path": relpath(paper_dir / "triples.jsonl", corpus_root),
            "quality_review_path": relpath(quality_path, corpus_root),
            "ingest_result_path": relpath(paper_dir / "ingest-result.json", corpus_root),
        },
        "koi": {
            "document_rid": document_rid or None,
            "ingest_status": str(meta.get("ingest_status") or ""),
            "pdf_status": str(meta.get("pdf_status") or ""),
            "extraction_profile": str(meta.get("extraction_profile") or ""),
            "facts_count": int(deep.get("facts_count") or quality.get("facts", {}).get("count") or 0),
            "discourse_moves_count": int(
                deep.get("discourse_moves_count") or quality.get("discourse", {}).get("count") or 0
            ),
            "chunks_count": int(deep.get("chunks_count") or rag.get("chunks_count") or 0),
            "quality_verdict": str(quality.get("verdict") or ""),
            "quality_warnings": [str(v) for v in as_list(quality.get("warnings"))],
            "last_processed_at": str(
                deep.get("updated_at")
                or rag.get("updated_at")
                or ingest_result.get("after", {}).get("processed_at")
                or ""
            ),
        },
    }


def load_paper_ledger_entries(corpus_root: Path, author_id: str) -> list[dict[str, Any]]:
    author_dir = corpus_root / "authors" / author_id
    entries = [
        build_paper_ledger_entry(meta_path, corpus_root, author_id)
        for meta_path in sorted(author_dir.glob("*/metadata.yaml"))
    ]
    return sorted(entries, key=lambda e: (-(e.get("year") or 0), str(e.get("title") or "").lower()))


def write_author_ledgers(corpus_root: Path, author_id: str) -> dict[str, Any]:
    author_dir = corpus_root / "authors" / author_id
    entries = load_paper_ledger_entries(corpus_root, author_id)
    paper_ledger = author_dir / "paper-ledger.jsonl"
    source_ledger = author_dir / "source-ledger.yaml"
    issue_summary = summarize_issue_log(issue_log_path(corpus_root, author_id))

    paper_ledger.write_text(
        "".join(json.dumps(entry, ensure_ascii=True, sort_keys=True) + "\n" for entry in entries),
        encoding="utf-8",
    )

    quality_counts: dict[str, int] = {}
    decision_counts: dict[str, int] = {}
    for entry in entries:
        verdict = entry["koi"]["quality_verdict"] or "unreviewed"
        quality_counts[verdict] = quality_counts.get(verdict, 0) + 1
        decision = entry["routing"]["decision"] or "none"
        decision_counts[decision] = decision_counts.get(decision, 0) + 1

    source_payload = {
        "schema": SOURCE_LEDGER_SCHEMA,
        "author_slug": author_id,
        "generated_at": now_iso(),
        "paper_ledger_path": "paper-ledger.jsonl",
        "issue_log_path": ISSUE_LOG_NAME,
        "counts": {
            "papers": len(entries),
            "decisions": dict(sorted(decision_counts.items())),
            "quality_verdicts": dict(sorted(quality_counts.items())),
            "issues": issue_summary,
        },
        "sources": [
            {
                "paper_id": entry["paper_id"],
                "title": entry["title"],
                "year": entry["year"],
                "canonical_url": entry["source"]["canonical_url"],
                "source_tier": entry["source"]["source_tier"],
                "checked_as_of": entry["source"]["checked_as_of"],
                "decision": entry["routing"]["decision"],
                "relevance_score": entry["routing"]["relevance_score"],
                "project_tags": entry["routing"]["project_tags"],
                "local_folder": entry["local"]["folder"],
                "document_rid": entry["koi"]["document_rid"],
                "ingest_status": entry["koi"]["ingest_status"],
                "quality_verdict": entry["koi"]["quality_verdict"],
                "quality_warnings": entry["koi"]["quality_warnings"],
            }
            for entry in entries
        ],
    }
    write_yaml(source_ledger, source_payload)
    return {
        "paper_ledger_path": str(paper_ledger),
        "source_ledger_path": str(source_ledger),
        "issue_log_path": str(issue_log_path(corpus_root, author_id)),
        "papers": len(entries),
        "quality_verdicts": dict(sorted(quality_counts.items())),
        "decisions": dict(sorted(decision_counts.items())),
        "issues": issue_summary,
    }


def promotion_candidate(entry: dict[str, Any]) -> dict[str, Any] | None:
    """Rank unreviewed papers for the next ingestion pass.

    The author sensor already records a relevance score. This layer adds
    learning-membrane readiness signals: whether the PDF/text is already local,
    whether the source is accessible, and whether matched topics align with the
    current discourse/sheaf/coordination work.
    """
    koi = entry.get("koi") if isinstance(entry.get("koi"), dict) else {}
    if koi.get("quality_verdict"):
        return None
    if str(koi.get("ingest_status") or "").lower() in {"ocr_blocked", "extraction_blocked"}:
        return None

    routing = entry.get("routing") if isinstance(entry.get("routing"), dict) else {}
    source = entry.get("source") if isinstance(entry.get("source"), dict) else {}
    local = entry.get("local") if isinstance(entry.get("local"), dict) else {}
    matched_topics = [str(v) for v in as_list(routing.get("matched_topics"))]
    decision = str(routing.get("decision") or "")
    source_tier = str(source.get("source_tier") or "")
    pdf_ready = bool(local.get("pdf_path")) or str(koi.get("pdf_status") or "").lower() == "downloaded"
    extracted_words = int(local.get("extracted_word_count") or 0)
    extracted_text_quality = str(local.get("extracted_text_quality") or "ok")
    text_quality_ok = extracted_text_quality == "ok"
    text_ready = (
        bool(local.get("extracted_path"))
        and extracted_words >= MIN_EXTRACTED_WORDS_FOR_TEXT_READY
        and text_quality_ok
    )
    rag_ready = int(koi.get("chunks_count") or 0) > 0
    paywalled = bool(source.get("paywalled")) or source_tier == "paywalled_metadata"

    score = int(routing.get("relevance_score") or 0)
    reasons: list[str] = []
    decision_weight = PROMOTION_DECISION_WEIGHTS.get(decision, 0)
    if decision_weight:
        score += decision_weight
        reasons.append(f"decision:{decision} ({decision_weight:+d})")

    source_weight = PROMOTION_SOURCE_WEIGHTS.get(source_tier, 0)
    if source_weight:
        score += source_weight
        reasons.append(f"source:{source_tier} ({source_weight:+d})")

    topic_bonus = 0
    weighted_topics: list[str] = []
    for topic in matched_topics:
        weight = PROMOTION_TOPIC_WEIGHTS.get(topic.lower(), 0)
        if not weight:
            continue
        topic_bonus += weight
        weighted_topics.append(topic)
    if topic_bonus:
        score += topic_bonus
        reasons.append(f"topics:{', '.join(weighted_topics[:6])} ({topic_bonus:+d})")

    year = entry.get("year")
    if isinstance(year, int):
        if year >= 2024:
            score += 4
            reasons.append("recent:2024+ (+4)")
        elif year >= 2020:
            score += 2
            reasons.append("recent:2020+ (+2)")

    if text_ready:
        score += 6
        reasons.append("text_ready (+6)")
    elif pdf_ready:
        score += 3
        reasons.append("pdf_ready (+3)")
        if local.get("extracted_path"):
            if not text_quality_ok:
                reasons.append(f"extracted_text_quality:{extracted_text_quality}")
            else:
                reasons.append(f"extracted_text_short:{extracted_words} (<{MIN_EXTRACTED_WORDS_FOR_TEXT_READY})")
    if paywalled and not pdf_ready:
        score -= 8
        reasons.append("paywalled_without_pdf (-8)")

    if decision == "duplicate":
        recommended_level = "metadata_index"
    elif score >= DEEP_PROMOTION_THRESHOLD and rag_ready:
        recommended_level = "deep_ingest_reviewed"
    elif score >= DEEP_PROMOTION_THRESHOLD and text_ready:
        recommended_level = "rag_then_deep_ingest_reviewed"
    elif score >= DEEP_PROMOTION_THRESHOLD:
        recommended_level = "download_extract_rag_then_deep_ingest_reviewed"
    elif score >= LIGHT_PROMOTION_THRESHOLD:
        recommended_level = "light_ingest"
    else:
        recommended_level = "metadata_index"

    return {
        "paper_id": entry["paper_id"],
        "title": entry["title"],
        "year": entry["year"],
        "promotion_score": score,
        "recommended_level": recommended_level,
        "decision": decision,
        "relevance_score": int(routing.get("relevance_score") or 0),
        "matched_topics": matched_topics,
        "project_tags": [str(v) for v in as_list(routing.get("project_tags"))],
        "source_tier": source_tier,
        "pdf_ready": pdf_ready,
        "text_ready": text_ready,
        "text_quality": extracted_text_quality,
        "rag_ready": rag_ready,
        "reasons": reasons,
    }


def build_promotion_candidates(entries: list[dict[str, Any]], limit: int = PROMOTION_CANDIDATE_LIMIT) -> list[dict[str, Any]]:
    candidates = [candidate for entry in entries if (candidate := promotion_candidate(entry))]
    candidates.sort(
        key=lambda item: (
            -int(item["promotion_score"]),
            -int(item["year"] or 0),
            str(item["title"]).lower(),
        )
    )
    return candidates[:limit]


def build_backtest_report(corpus_root: Path, author_id: str) -> dict[str, Any]:
    entries = load_paper_ledger_entries(corpus_root, author_id)
    issues = read_issue_log(issue_log_path(corpus_root, author_id))
    processed = [entry for entry in entries if entry["koi"]["quality_verdict"]]
    promotion_candidates = build_promotion_candidates(entries)

    warning_counts: dict[str, int] = {}
    verdict_counts: dict[str, int] = {}
    review_candidates: list[dict[str, Any]] = []
    totals = {"facts": 0, "discourse_moves": 0, "chunks": 0}

    for entry in processed:
        koi = entry["koi"]
        verdict = koi["quality_verdict"] or "unreviewed"
        verdict_counts[verdict] = verdict_counts.get(verdict, 0) + 1
        totals["facts"] += int(koi["facts_count"] or 0)
        totals["discourse_moves"] += int(koi["discourse_moves_count"] or 0)
        totals["chunks"] += int(koi["chunks_count"] or 0)
        for warning in koi["quality_warnings"]:
            warning_counts[warning] = warning_counts.get(warning, 0) + 1

        quality_path = corpus_root / str(entry["local"]["quality_review_path"] or "")
        quality = read_json(quality_path) if quality_path.exists() else {}
        facts = quality.get("facts", {}) if isinstance(quality.get("facts"), dict) else {}
        discourse = quality.get("discourse", {}) if isinstance(quality.get("discourse"), dict) else {}
        needs_review = (
            verdict != "ok"
            or facts.get("invalid_active_facts")
            or facts.get("missing_chunk_ranges")
            or discourse.get("missing_chunk_ranges")
        )
        if needs_review:
            review_candidates.append(
                {
                    "paper_id": entry["paper_id"],
                    "title": entry["title"],
                    "year": entry["year"],
                    "quality_verdict": verdict,
                    "warnings": koi["quality_warnings"],
                    "facts_count": koi["facts_count"],
                    "discourse_moves_count": koi["discourse_moves_count"],
                    "generic_proves": int(facts.get("proves_generic_subject") or 0),
                    "numbered_statement_mismatches": int(facts.get("numbered_statement_mismatches") or 0),
                    "near_duplicate_move_pairs": len(discourse.get("near_duplicate_move_pairs") or []),
                }
            )

    open_issues = [
        {
            "issue_id": str(issue.get("issue_id") or ""),
            "paper_id": str(issue.get("paper_id") or ""),
            "title": str(issue.get("title") or ""),
            "category": str(issue.get("category") or ""),
            "code": str(issue.get("code") or ""),
            "severity": str(issue.get("severity") or ""),
            "detail": str(issue.get("detail") or ""),
        }
        for issue in issues
        if issue.get("status") != "fixed"
    ]

    recommendations: list[str] = []
    if any("near-duplicate discourse" in warning for warning in warning_counts):
        recommendations.append("Review and merge near-duplicate discourse moves before synthesizing across papers.")
    if any("HAS_DESCRIPTION dominates" in warning for warning in warning_counts):
        recommendations.append("Tune extraction prompts/gates toward more specific scientific predicates on description-heavy papers.")
    if any("PROVES fact" in warning for warning in warning_counts):
        recommendations.append("Prioritize theorem/proposition ownership checks for PROVES facts.")
    if open_issues:
        recommendations.append("Work open issue-log rows before promoting the corpus as a stable research substrate.")
    if promotion_candidates:
        top = promotion_candidates[0]
        recommendations.append(
            "Next promotion candidate: "
            f"{top['title']} ({top['paper_id']}) -> {top['recommended_level']}."
        )

    return {
        "schema": BACKTEST_REPORT_SCHEMA,
        "author_slug": author_id,
        "generated_at": now_iso(),
        "papers_total": len(entries),
        "papers_with_quality_reviews": len(processed),
        "quality_verdicts": dict(sorted(verdict_counts.items())),
        "totals": totals,
        "warning_counts": dict(sorted(warning_counts.items(), key=lambda item: (-item[1], item[0]))),
        "review_candidates": review_candidates,
        "promotion_candidates": promotion_candidates,
        "issue_log": summarize_issue_log(issue_log_path(corpus_root, author_id)),
        "open_issues": open_issues,
        "recommendations": recommendations,
    }


def write_backtest_report(corpus_root: Path, author_id: str, output_path: Path | None = None) -> dict[str, Any]:
    report = build_backtest_report(corpus_root, author_id)
    out = output_path or (author_dir(corpus_root, author_id) / "paper-backtest-report.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"path": str(out), **report}


def load_candidates(
    corpus_root: Path,
    author_id: str,
    decisions: set[str],
    paper_ids: set[str],
    min_score: int | None,
    require_extracted: bool,
) -> list[PaperCandidate]:
    author_dir = corpus_root / "authors" / author_id
    candidates: list[PaperCandidate] = []
    for meta_path in sorted(author_dir.glob("*/metadata.yaml")):
        meta = read_yaml(meta_path)
        paper_id = str(meta.get("paper_id") or f"{author_id}/{meta_path.parent.name}")
        if paper_ids and paper_id not in paper_ids and meta_path.parent.name not in paper_ids:
            continue
        decision = str(meta.get("decision") or "")
        if decisions and not paper_ids and decision not in decisions:
            continue
        score = int(meta.get("relevance_score") or 0)
        if min_score is not None and score < min_score:
            continue
        extracted = meta_path.parent / "extracted.md"
        if require_extracted and not extracted.exists():
            continue
        if not extracted.exists():
            continue
        markdown = extracted.read_text(encoding="utf-8", errors="replace")
        _, document_rid = ingest_document.compute_document_rid(markdown)
        candidates.append(
            PaperCandidate(
                path=meta_path.parent,
                metadata_path=meta_path,
                metadata=meta,
                paper_id=paper_id,
                title=str(meta.get("title") or meta_path.parent.name),
                year=int(meta["year"]) if str(meta.get("year") or "").isdigit() else None,
                decision=decision,
                relevance_score=score,
                source_url=str(meta.get("source_url") or meta.get("pdf_url") or ""),
                extracted_path=extracted,
                document_rid=document_rid,
            )
        )
    return sorted(candidates, key=lambda p: (-p.relevance_score, -(p.year or 0), p.title.lower()))


async def fetch_counts(conn: asyncpg.Connection, document_rid: str) -> dict[str, int]:
    facts = await conn.fetchval(
        "SELECT count(*) FROM knowledge_facts WHERE source_node_rid=$1 AND valid_to IS NULL",
        document_rid,
    )
    moves = await conn.fetchval(
        "SELECT count(*) FROM session_discourse_moves "
        "WHERE source_type='document' AND source_rid=$1",
        document_rid,
    )
    chunks = await conn.fetchval(
        "SELECT count(*) FROM koi_memory_chunks WHERE document_rid=$1",
        document_rid,
    )
    return {"facts": int(facts or 0), "discourse_moves": int(moves or 0), "chunks": int(chunks or 0)}


async def stamp_document_metadata(
    conn: asyncpg.Connection,
    paper: PaperCandidate,
    group_id: str,
    status: dict[str, Any],
) -> None:
    payload = {
        "paper_id": paper.paper_id,
        "paper_title": paper.title,
        "author_id": paper.paper_id.split("/", 1)[0],
        "year": paper.year,
        "source_url": paper.source_url or None,
        "corpus_path": str(paper.path),
        "decision": paper.decision,
        "relevance_score": paper.relevance_score,
        "group_id": group_id,
        "source_type": "research_paper",
        "extraction_profile": "scientific-discourse-v1",
        "paper_ingest_status": status,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    await conn.execute(
        """
        UPDATE koi_memories
           SET metadata = COALESCE(metadata, '{}'::jsonb) || $1::jsonb,
               content = jsonb_set(content, '{title}', to_jsonb($2::text), true),
               updated_at = NOW()
         WHERE rid = $3 AND source_sensor = 'document-ingest'
        """,
        json.dumps(payload),
        paper.title,
        paper.document_rid,
    )


async def export_discourse(conn: asyncpg.Connection, paper: PaperCandidate, path: Path) -> int:
    rows = await conn.fetch(
        """
        SELECT m.id::text AS id, m.move_type, m.title, m.detail, m.status,
               m.turn_range_start, m.turn_range_end,
               m.resolves_move_id::text AS supports_move_id,
               s.title AS supports_title,
               m.created_at::text AS created_at
          FROM session_discourse_moves m
          LEFT JOIN session_discourse_moves s ON s.id = m.resolves_move_id
         WHERE m.source_type='document' AND m.source_rid=$1
         ORDER BY m.turn_range_start NULLS LAST, m.move_type, m.title
        """,
        paper.document_rid,
    )
    moves = [
        {
            "id": r["id"],
            "move_type": r["move_type"],
            "title": r["title"],
            "detail": r["detail"],
            "status": r["status"],
            "supports_move_id": r["supports_move_id"],
            "supports_title": r["supports_title"],
            "chunk_range": [r["turn_range_start"], r["turn_range_end"]],
            "created_at": r["created_at"],
        }
        for r in rows
    ]
    payload = {
        "paper_id": paper.paper_id,
        "document_rid": paper.document_rid,
        "title": paper.title,
        "source_url": paper.source_url,
        "generated_at": now_iso(),
        "count": len(moves),
        "moves": moves,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return len(moves)


async def export_triples(conn: asyncpg.Connection, paper: PaperCandidate, path: Path) -> int:
    rows = await conn.fetch(
        """
        SELECT f.id::text AS id,
               f.subject_uri, es.entity_text AS subject_label, es.entity_type AS subject_type,
               f.predicate,
               f.object_uri, eo.entity_text AS object_label, eo.entity_type AS object_type,
               f.object_literal, f.fact_text, f.group_id,
               f.turn_range_start, f.turn_range_end,
               f.created_at::text AS created_at
          FROM knowledge_facts f
          LEFT JOIN entity_registry es ON es.fuseki_uri = f.subject_uri
          LEFT JOIN entity_registry eo ON eo.fuseki_uri = f.object_uri
         WHERE f.source_node_rid=$1 AND f.valid_to IS NULL
         ORDER BY f.created_at, f.id
        """,
        paper.document_rid,
    )
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            item = {
                "id": r["id"],
                "paper_id": paper.paper_id,
                "document_rid": paper.document_rid,
                "subject_uri": r["subject_uri"],
                "subject_label": r["subject_label"],
                "subject_type": r["subject_type"],
                "predicate": r["predicate"],
                "object_uri": r["object_uri"],
                "object_label": r["object_label"],
                "object_type": r["object_type"],
                "object_literal": r["object_literal"],
                "fact_text": r["fact_text"],
                "group_id": r["group_id"],
                "chunk_range": [r["turn_range_start"], r["turn_range_end"]],
                "created_at": r["created_at"],
            }
            f.write(json.dumps(item, ensure_ascii=True) + "\n")
    return len(rows)


async def export_quality_review(conn: asyncpg.Connection, paper: PaperCandidate, path: Path) -> dict[str, Any]:
    fact_rows = await conn.fetch(
        """
        SELECT f.subject_uri, s.entity_text AS subject_label, s.entity_type AS subject_type,
               f.predicate, f.object_uri, o.entity_text AS object_label, o.entity_type AS object_type,
               f.object_literal, f.fact_text,
               turn_range_start, turn_range_end
          FROM knowledge_facts f
          LEFT JOIN entity_registry s ON s.fuseki_uri=f.subject_uri
          LEFT JOIN entity_registry o ON o.fuseki_uri=f.object_uri
         WHERE f.source_node_rid=$1 AND f.valid_to IS NULL
        """,
        paper.document_rid,
    )
    move_rows = await conn.fetch(
        """
        SELECT id::text AS id, move_type, title, detail, status,
               resolves_move_id IS NOT NULL AS has_support,
               turn_range_start, turn_range_end
          FROM session_discourse_moves
         WHERE source_type='document' AND source_rid=$1
        """,
        paper.document_rid,
    )
    mismatch_rows = await conn.fetch(
        """
        SELECT payload, error
          FROM document_extraction_item_errors
         WHERE document_rid=$1 AND item_type='type_mismatch'
         ORDER BY created_at DESC
        """,
        paper.document_rid,
    )

    predicate_counts: dict[str, int] = {}
    move_type_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    literal_non_has = 0
    authored_by_literal = 0
    invalid_active_facts = 0
    proves_generic_subject = 0
    numbered_statement_mismatches = 0
    algorithm_owner_mismatches = 0
    kg_model_label_mismatches = 0
    scientific_entity_overmerges = 0
    facts_missing_ranges = 0
    for row in fact_rows:
        predicate = row["predicate"] or ""
        predicate_counts[predicate] = predicate_counts.get(predicate, 0) + 1
        if proves_predicate_needs_review(predicate, row["subject_label"]):
            proves_generic_subject += 1
        if numbered_statement_needs_review(
            row["subject_label"],
            row["fact_text"],
        ) or numbered_statement_needs_review(row["object_label"], row["fact_text"]):
            numbered_statement_mismatches += 1
        if algorithm_fact_owner_needs_review(row["subject_label"], predicate, row["fact_text"]):
            algorithm_owner_mismatches += 1
        if kg_embedding_model_fact_needs_review(
            row["subject_label"],
            row["object_label"],
            row["fact_text"],
        ):
            kg_model_label_mismatches += 1
        if scientific_label_overmerge_needs_review(
            row["subject_label"],
            row["object_label"],
            row["fact_text"],
        ):
            scientific_entity_overmerges += 1
        if invalid_fact_reason(
            predicate,
            row["object_uri"],
            row["object_literal"],
            subject_uri=row["subject_uri"],
            subject_type=row["subject_type"],
            object_type=row["object_type"],
        ):
            invalid_active_facts += 1
        if row["turn_range_start"] is None or row["turn_range_end"] is None:
            facts_missing_ranges += 1
        if row["object_uri"] is None and row["object_literal"] and not predicate.startswith("HAS_"):
            literal_non_has += 1
        if predicate == "AUTHORED_BY" and row["object_uri"] is None:
            authored_by_literal += 1

    moves_missing_ranges = 0
    support_edges = 0
    for row in move_rows:
        move_type = row["move_type"] or ""
        status = row["status"] or "null"
        move_type_counts[move_type] = move_type_counts.get(move_type, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
        if row["has_support"]:
            support_edges += 1
        if row["turn_range_start"] is None or row["turn_range_end"] is None:
            moves_missing_ranges += 1
    near_duplicate_moves = find_near_duplicate_discourse_moves(move_rows)

    unresolved_mismatches: list[dict[str, Any]] = []
    for row in mismatch_rows:
        payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
        requested_type = payload.get("requested_type") if isinstance(payload, dict) else None
        resolved_uri = payload.get("resolved_uri") if isinstance(payload, dict) else None
        current_type = None
        if requested_type and resolved_uri:
            current_type = await conn.fetchval(
                "SELECT entity_type FROM entity_registry WHERE fuseki_uri=$1",
                resolved_uri,
            )
        if current_type != requested_type:
            unresolved_mismatches.append(
                {"payload": payload, "error": row["error"], "current_type": current_type}
            )

    warnings: list[str] = []
    if facts_missing_ranges:
        warnings.append(f"{facts_missing_ranges} fact(s) lack chunk-range provenance")
    if invalid_active_facts:
        warnings.append(f"{invalid_active_facts} structurally invalid fact(s) remain active")
    if literal_non_has:
        warnings.append(f"{literal_non_has} literal-object fact(s) use non-HAS predicates")
    if authored_by_literal:
        warnings.append(f"{authored_by_literal} AUTHORED_BY fact(s) point to literals instead of entity authors")
    if proves_generic_subject:
        warnings.append(f"{proves_generic_subject} PROVES fact(s) have generic concept subjects; review predicate ownership")
    if numbered_statement_mismatches:
        warnings.append(
            f"{numbered_statement_mismatches} numbered theorem/proposition/lemma fact(s) have label/text mismatches"
        )
    if algorithm_owner_mismatches:
        warnings.append(f"{algorithm_owner_mismatches} algorithm fact(s) appear attached to non-algorithm subjects")
    if kg_model_label_mismatches:
        warnings.append(
            f"{kg_model_label_mismatches} KG embedding model fact(s) have label/text mismatches"
        )
    if scientific_entity_overmerges:
        warnings.append(
            f"{scientific_entity_overmerges} scientific entity label over-merge(s) need review"
        )
    if unresolved_mismatches:
        warnings.append(f"{len(unresolved_mismatches)} unresolved entity type mismatch item(s)")
    if predicate_counts.get("HAS_DESCRIPTION", 0) > max(4, len(fact_rows) // 2):
        warnings.append("HAS_DESCRIPTION dominates the fact layer; consider more specific scientific predicates")
    if near_duplicate_moves:
        warnings.append(f"{len(near_duplicate_moves)} near-duplicate discourse move pair(s) need merge review")

    review = {
        "paper_id": paper.paper_id,
        "document_rid": paper.document_rid,
        "title": paper.title,
        "generated_at": now_iso(),
        "facts": {
            "count": len(fact_rows),
            "missing_chunk_ranges": facts_missing_ranges,
            "literal_non_has_predicates": literal_non_has,
            "authored_by_literal": authored_by_literal,
            "invalid_active_facts": invalid_active_facts,
            "proves_generic_subject": proves_generic_subject,
            "numbered_statement_mismatches": numbered_statement_mismatches,
            "algorithm_owner_mismatches": algorithm_owner_mismatches,
            "kg_model_label_mismatches": kg_model_label_mismatches,
            "scientific_entity_overmerges": scientific_entity_overmerges,
            "predicate_counts": dict(sorted(predicate_counts.items())),
        },
        "discourse": {
            "count": len(move_rows),
            "missing_chunk_ranges": moves_missing_ranges,
            "support_edges": support_edges,
            "near_duplicate_move_pairs": near_duplicate_moves[:20],
            "move_type_counts": dict(sorted(move_type_counts.items())),
            "status_counts": dict(sorted(status_counts.items())),
        },
        "type_mismatches": unresolved_mismatches,
        "warnings": warnings,
        "verdict": "needs_review" if warnings else "ok",
    }
    path.write_text(json.dumps(review, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return review


def proves_predicate_needs_review(predicate: str | None, subject_label: str | None) -> bool:
    if (predicate or "").upper() != "PROVES":
        return False
    label = (subject_label or "").lower()
    return not any(hint in label for hint in PROVES_SUBJECT_HINTS)


def numbered_statement_needs_review(subject_label: str | None, fact_text: str | None) -> bool:
    """Return True when a numbered theorem/proposition subject conflicts with text."""
    subject_match = NUMBERED_STATEMENT_RE.search((subject_label or "").strip())
    if not subject_match:
        return False
    subject_ref = (subject_match.group(1).lower(), subject_match.group(2))
    text_refs = [
        (match.group(1).lower(), match.group(2))
        for match in NUMBERED_STATEMENT_RE.finditer(fact_text or "")
        if match.group(1).lower() == subject_ref[0]
    ]
    if not text_refs:
        return False
    return subject_ref not in text_refs


def algorithm_fact_owner_needs_review(
    subject_label: str | None,
    predicate: str | None,
    fact_text: str | None,
) -> bool:
    """Flag computational facts whose text names an algorithm but whose subject is not algorithm-like."""
    if (predicate or "").upper() not in ALGORITHM_OWNER_PREDICATES:
        return False
    if not ALGORITHM_TEXT_RE.search(fact_text or ""):
        return False
    label = (subject_label or "").lower()
    return "algorithm" not in label and "distributed computation" not in label


def normalize_kg_model_label(text: str | None) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()
    normalized = re.sub(r"\bknowledge graph embedding model\b", "", normalized)
    normalized = re.sub(r"\bembedding model\b", "", normalized)
    normalized = re.sub(r"\bmodel\b", "", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def kg_models_in_text(text: str | None) -> set[str]:
    text = re.sub(r"\bnaive\s+TransE\b", "NaiveTransE", text or "", flags=re.IGNORECASE)
    return {
        normalize_kg_model_label(match.group(1))
        for match in KG_EMBEDDING_MODEL_RE.finditer(text)
    }


def kg_model_label_for_entity(label: str | None) -> str | None:
    models = kg_models_in_text(label)
    if len(models) != 1:
        return None
    return next(iter(models))


def leading_kg_model_in_fact(fact_text: str | None) -> str | None:
    match = KG_MODEL_LEADING_RE.search(fact_text or "")
    if not match:
        return None
    return normalize_kg_model_label(match.group(0))


def normalize_entity_label_for_review(text: str | None) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()
    return re.sub(r"\s+", " ", normalized)


def scientific_label_overmerge_needs_review(
    subject_label: str | None,
    object_label: str | None,
    fact_text: str | None,
) -> bool:
    """Flag known scientific-term over-merges into unrelated local labels."""
    text = normalize_entity_label_for_review(fact_text)
    if not text:
        return False
    for label in (subject_label, object_label):
        key = normalize_entity_label_for_review(label)
        for expected_term in SCIENTIFIC_LABEL_OVERMERGES.get(key, ()):
            if expected_term in text:
                return True
    return False


def kg_embedding_model_fact_needs_review(
    subject_label: str | None,
    object_label: str | None,
    fact_text: str | None,
) -> bool:
    """Flag KG embedding model facts whose resolved entity label contradicts the text."""
    text_models = kg_models_in_text(fact_text)
    if not text_models:
        return False

    subject_model = kg_model_label_for_entity(subject_label)
    object_model = kg_model_label_for_entity(object_label)
    leading_model = leading_kg_model_in_fact(fact_text)

    if leading_model and subject_model != leading_model and object_model != leading_model:
        return True
    if subject_model and subject_model not in text_models:
        return True
    if object_model and object_model not in text_models:
        return True
    return False


def discourse_title_tokens(title: str | None) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", (title or "").lower())
        if len(token) > 2 and token not in DISCOURSE_TITLE_STOPWORDS
    }


def discourse_title_similarity(title_a: str | None, title_b: str | None) -> float:
    tokens_a = discourse_title_tokens(title_a)
    tokens_b = discourse_title_tokens(title_b)
    if len(tokens_a) < 5 or len(tokens_b) < 5:
        return 0.0
    return len(tokens_a & tokens_b) / min(len(tokens_a), len(tokens_b))


def find_near_duplicate_discourse_moves(rows: list[Any], threshold: float = 0.70) -> list[dict[str, Any]]:
    pairs: list[dict[str, Any]] = []
    for i, left in enumerate(rows):
        left_type = left["move_type"] or ""
        left_title = left["title"] or ""
        for right in rows[i + 1 :]:
            if (right["move_type"] or "") != left_type:
                continue
            if discourse_titles_are_contrastive(left_title, right["title"] or ""):
                continue
            score = discourse_title_similarity(left_title, right["title"] or "")
            if score < threshold:
                continue
            pairs.append(
                {
                    "move_type": left_type,
                    "similarity": round(score, 3),
                    "left_id": left["id"],
                    "left_title": left_title,
                    "right_id": right["id"],
                    "right_title": right["title"] or "",
                }
            )
    return pairs


def numbered_references(title: str | None) -> set[tuple[str, str]]:
    return {
        (match.group(1).lower(), match.group(2))
        for match in NUMBERED_REFERENCE_RE.finditer(title or "")
    }


def existence_polarity(title: str | None) -> str | None:
    text = title or ""
    if NEGATIVE_EXISTENCE_RE.search(text):
        return "negative"
    if POSITIVE_EXISTENCE_RE.search(text):
        return "positive"
    return None


def discourse_titles_are_contrastive(title_a: str | None, title_b: str | None) -> bool:
    refs_a = numbered_references(title_a)
    refs_b = numbered_references(title_b)
    if refs_a and refs_b and refs_a.isdisjoint(refs_b):
        return True
    polarity_a = existence_polarity(title_a)
    polarity_b = existence_polarity(title_b)
    return bool(polarity_a and polarity_b and polarity_a != polarity_b)


def invalid_fact_reason(
    predicate: str | None,
    object_uri: str | None,
    object_literal: str | None,
    *,
    subject_uri: str | None = None,
    subject_type: str | None = None,
    object_type: str | None = None,
) -> str | None:
    """Return why a scientific fact should not remain active."""
    predicate_upper = (predicate or "").upper()
    if subject_uri and object_uri and subject_uri == object_uri:
        return "entity fact links an entity to itself"
    if predicate_upper in ENTITY_OBJECT_PREDICATES and not object_uri and not object_literal:
        return f"{predicate_upper} requires an entity object"
    if predicate_upper in ENTITY_OBJECT_PREDICATES and not object_uri and object_literal:
        return f"{predicate_upper} requires an entity object, not a literal"
    if predicate_upper == "SUPPORTS" and subject_type == "Person" and object_type == "Organization":
        return "person-to-organization SUPPORTS is likely a reversed funding acknowledgement"
    if predicate_upper == "HAS_DESCRIPTION" and object_literal and object_literal.lstrip().startswith("arXiv:"):
        return "bibliographic header should be document metadata, not a description fact"
    return None


async def retire_invalid_scientific_facts(conn: asyncpg.Connection, paper: PaperCandidate) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT f.id::text AS id, f.subject_uri, s.entity_type AS subject_type,
               f.predicate, f.object_uri, o.entity_type AS object_type,
               f.object_literal, f.fact_text
          FROM knowledge_facts f
          LEFT JOIN entity_registry s ON s.fuseki_uri=f.subject_uri
          LEFT JOIN entity_registry o ON o.fuseki_uri=f.object_uri
         WHERE f.source_node_rid=$1
           AND f.valid_to IS NULL
        """,
        paper.document_rid,
    )
    invalids: list[dict[str, Any]] = []
    for row in rows:
        reason = invalid_fact_reason(
            row["predicate"],
            row["object_uri"],
            row["object_literal"],
            subject_uri=row["subject_uri"],
            subject_type=row["subject_type"],
            object_type=row["object_type"],
        )
        if not reason:
            continue
        invalids.append(
            {
                "id": row["id"],
                "predicate": row["predicate"],
                "object_literal": row["object_literal"],
                "fact_text": row["fact_text"],
                "reason": reason,
            }
        )
    if invalids:
        await conn.execute(
            """
            UPDATE knowledge_facts
               SET valid_to=NOW()
             WHERE id=ANY($1::uuid[])
               AND valid_to IS NULL
            """,
            [item["id"] for item in invalids],
        )
    return invalids


async def backfill_fact_chunk_ranges(conn: asyncpg.Connection, paper: PaperCandidate) -> int:
    rows = await conn.fetch(
        """
        SELECT raw_json
          FROM document_window_extractions
         WHERE document_rid=$1 AND raw_json IS NOT NULL
         ORDER BY window_index
        """,
        paper.document_rid,
    )
    if not rows:
        return 0

    per_window: list[dict[str, Any]] = []
    for row in rows:
        raw = row["raw_json"]
        per_window.append(json.loads(raw) if isinstance(raw, str) else raw)
    merged = extract_deep_documents.merge_extractions(per_window, [])

    ranges: dict[str, tuple[int, int]] = {}
    for fact in merged.get("facts", []):
        fact_text = (fact.get("fact_text") or "").strip()
        chunk_range = fact.get("chunk_range") or []
        if len(chunk_range) != 2 or not all(isinstance(v, int) for v in chunk_range):
            continue
        if fact_text not in ranges:
            ranges[fact_text] = (chunk_range[0], chunk_range[1])
            continue
        prev = ranges[fact_text]
        ranges[fact_text] = (min(prev[0], chunk_range[0]), max(prev[1], chunk_range[1]))

    updated = 0
    for fact_text, (start, end) in ranges.items():
        tag = await conn.execute(
            """
            UPDATE knowledge_facts
               SET turn_range_start=$3, turn_range_end=$4
             WHERE source_node_rid=$1
               AND fact_text=$2
               AND valid_to IS NULL
               AND (turn_range_start IS NULL OR turn_range_end IS NULL)
            """,
            paper.document_rid,
            fact_text,
            start,
            end,
        )
        if tag.startswith("UPDATE"):
            updated += int(tag.split()[-1])
    return updated


def update_paper_metadata(paper: PaperCandidate, status: dict[str, Any]) -> None:
    meta = dict(paper.metadata)
    meta["document_rid"] = paper.document_rid
    meta["ingest_status"] = "deep_ingested" if status.get("facts", 0) or status.get("discourse_moves", 0) else "rag_ingested"
    meta["deep_ingestion"] = {
        "updated_at": now_iso(),
        "group_id": status.get("group_id"),
        "facts_count": status.get("facts", 0),
        "discourse_moves_count": status.get("discourse_moves", 0),
        "chunks_count": status.get("chunks", 0),
        "discourse_path": "discourse-elements.json",
        "triples_path": "triples.jsonl",
    }
    write_yaml(paper.metadata_path, meta)


async def process_one(args: argparse.Namespace, paper: PaperCandidate) -> dict[str, Any]:
    corpus_root = Path(args.corpus_root).expanduser().resolve()
    async with asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=2) as pool:
        async with pool.acquire() as conn:
            before = await fetch_counts(conn, paper.document_rid)

        should_ingest = (
            not args.export_only
            and not args.dry_run
            and (args.force_ingest or before["chunks"] == 0 or before["facts"] == 0 or before["discourse_moves"] == 0)
        )
        ingest_result: dict[str, Any] | None = None
        if should_ingest:
            ingest_result = await ingest_document.ingest_path(
                source_path=str(paper.extracted_path),
                tier="thorough",
                slug=paper.path.name,
                name=paper.title,
                source_url=paper.source_url or None,
                retrieval_method="research-paper-corpus",
                group_id=args.group_id,
                claims=not args.no_claims,
                force=args.force_ingest,
                dry_run=False,
            )

        async with pool.acquire() as conn:
            invalid_facts_retired: list[dict[str, Any]] = []
            range_backfilled = 0
            if not args.dry_run:
                invalid_facts_retired = await retire_invalid_scientific_facts(conn, paper)
                range_backfilled = await backfill_fact_chunk_ranges(conn, paper)
            after = await fetch_counts(conn, paper.document_rid)
            status = {**after, "group_id": args.group_id, "processed_at": now_iso()}
            if not args.dry_run:
                status["invalid_facts_retired"] = len(invalid_facts_retired)
                if invalid_facts_retired:
                    status["retired_invalid_facts"] = invalid_facts_retired
                status["fact_ranges_backfilled"] = range_backfilled
                await stamp_document_metadata(conn, paper, args.group_id, status)
                discourse_count = await export_discourse(conn, paper, paper.path / "discourse-elements.json")
                triples_count = await export_triples(conn, paper, paper.path / "triples.jsonl")
                quality_review = await export_quality_review(conn, paper, paper.path / "quality-review.json")
                status["discourse_moves"] = discourse_count
                status["facts"] = triples_count
                status["quality_verdict"] = quality_review["verdict"]
                status["quality_warnings"] = quality_review["warnings"]
                status["issue_log"] = append_issue_log(
                    corpus_root,
                    args.author,
                    issues_from_processing(paper, quality_review, invalid_facts_retired),
                )
                update_paper_metadata(paper, status)
                (paper.path / "ingest-result.json").write_text(
                    json.dumps(
                        {
                            "paper_id": paper.paper_id,
                            "document_rid": paper.document_rid,
                            "before": before,
                            "after": status,
                            "ingest_result": ingest_result,
                            "quality_review": quality_review,
                        },
                        indent=2,
                        ensure_ascii=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
        return {
            "paper_id": paper.paper_id,
            "title": paper.title,
            "document_rid": paper.document_rid,
            "before": before,
            "after": after if args.dry_run else status,
            "ingested": should_ingest,
            "path": str(paper.path),
        }


async def amain(args: argparse.Namespace) -> int:
    corpus_root = Path(args.corpus_root).expanduser().resolve()
    os.environ["INGEST_SOURCE_ROOT"] = str(corpus_root)
    decisions = {d.strip() for d in args.decisions.split(",") if d.strip()}
    paper_ids = {p.strip() for p in args.paper_id if p.strip()}
    if args.ledger_only:
        ledger_summary = write_author_ledgers(corpus_root, args.author)
        print(json.dumps({"mode": "ledger-only", "author": args.author, "ledger": ledger_summary}, indent=2))
        return 0
    if args.backtest_only:
        report = write_backtest_report(
            corpus_root,
            args.author,
            Path(args.backtest_out).expanduser().resolve() if args.backtest_out else None,
        )
        print(json.dumps({"mode": "backtest-only", "author": args.author, "backtest": report}, indent=2))
        return 0

    candidates = load_candidates(
        corpus_root,
        args.author,
        decisions,
        paper_ids,
        args.min_score,
        require_extracted=True,
    )
    if args.skip_exported and not args.force_ingest:
        candidates = [
            p for p in candidates
            if not ((p.path / "discourse-elements.json").exists() and (p.path / "triples.jsonl").exists())
        ]
    if args.limit is not None:
        candidates = candidates[: args.limit]

    summary: dict[str, Any] = {
        "mode": "dry-run" if args.dry_run else "apply",
        "corpus_root": str(corpus_root),
        "author": args.author,
        "group_id": args.group_id,
        "selected": len(candidates),
        "results": [],
    }
    if args.dry_run:
        for p in candidates:
            summary["results"].append(
                {
                    "paper_id": p.paper_id,
                    "title": p.title,
                    "score": p.relevance_score,
                    "year": p.year,
                    "path": str(p.path),
                    "document_rid": p.document_rid,
                }
            )
        print(json.dumps(summary, indent=2, ensure_ascii=True))
        return 0

    for paper in candidates:
        print(f"processing {paper.paper_id}: {paper.title}", file=sys.stderr)
        summary["results"].append(await process_one(args, paper))

    if not args.dry_run:
        summary["ledger"] = write_author_ledgers(corpus_root, args.author)

    print(json.dumps(summary, indent=2, ensure_ascii=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-root", default=str(DEFAULT_CORPUS_ROOT))
    parser.add_argument("--author", default=DEFAULT_AUTHOR)
    parser.add_argument("--decisions", default="download_now")
    parser.add_argument("--paper-id", action="append", default=[], help="Exact paper_id or folder name to process")
    parser.add_argument("--min-score", type=int)
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--no-claims", action="store_true", help="Skip /claims/extract stage")
    parser.add_argument("--force-ingest", action="store_true", help="Re-run deep extraction even if DB artifacts exist")
    parser.add_argument("--export-only", action="store_true", help="Only export existing DB facts/discourse to local files")
    parser.add_argument("--skip-exported", action="store_true", help="Skip folders that already have discourse/triples artifacts")
    parser.add_argument("--ledger-only", action="store_true", help="Refresh author-level source/paper ledgers only")
    parser.add_argument("--backtest-only", action="store_true", help="Write a corpus quality backtest report and exit")
    parser.add_argument("--backtest-out", help="Optional output path for --backtest-only")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        return asyncio.run(amain(args))
    except Exception as exc:  # noqa: BLE001 - operator CLI should surface the exact failure
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
