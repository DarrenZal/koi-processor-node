#!/usr/bin/env python3
"""Research author publication sensor for personal-koi.

Polls configured author sources, detects papers not already in the shared
research-paper corpus, optionally creates paper folders/PDFs, and emits
personal-KOI memory events for newly discovered papers.

Default mode is dry-run. Use --apply to write corpus files and KOI rows.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import logging
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Optional

import asyncpg
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from api.chunker import TextChunker  # noqa: E402
from api.embedding_provider import create_embedding_provider  # noqa: E402


POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://koi:@localhost:5432/personal_koi",
)
SOURCE_SENSOR = "research-paper-sensor"
ACCESS_SOURCE = "research-public"
USER_AGENT = "personal-koi research author sensor/1.0"
DEFAULT_CONFIG = Path(__file__).parent.parent / "config" / "research_author_sensors.yaml"
MAX_CHUNKS_PER_PAPER_EVENT = 4

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("research-author-sensor")


@dataclass
class AuthorConfig:
    author_id: str
    canonical_name: str
    aliases: list[str]
    corpus_root: Path
    author_dir: Path
    official_preprints: Optional[str]
    arxiv_queries: list[str]
    project_tags: list[str]
    corpus_tags: list[str]
    direct_patterns: list[str]
    project_patterns: list[str]


@dataclass
class PaperRecord:
    author_id: str
    canonical_author: str
    title: str
    year: Optional[int]
    authors: list[str] = field(default_factory=list)
    abstract: str = ""
    source_url: str = ""
    pdf_url: str = ""
    official_url: str = ""
    official_pdf_or_page: str = ""
    arxiv_id: str = ""
    arxiv_version: str = ""
    published: str = ""
    updated: str = ""
    citation: str = ""
    source_kinds: list[str] = field(default_factory=list)
    paper_id: str = ""
    decision: str = ""
    relevance_score: int = 0
    matched_topics: list[str] = field(default_factory=list)
    existing: bool = False
    existing_reason: str = ""
    corpus_path: str = ""
    pdf_status: str = "not_downloaded"


def clean_text(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def slugify(value: str, max_len: int = 90) -> str:
    value = html.unescape(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return re.sub(r"-+", "-", value).strip("-")[:max_len] or "untitled"


def norm_title(value: str) -> str:
    value = re.sub(r"^\s*\[\d{4}\]\s+", " ", html.unescape(value))
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def fetch_text(url: str, timeout: int = 60) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def fetch_bytes(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_config(path: Path) -> list[AuthorConfig]:
    # Which authors to watch is personal config (research_author_sensors.yaml,
    # gitignored). Fall back to the committed template so a fresh fork still runs.
    if not path.exists():
        example = path.with_name(path.stem + ".example" + path.suffix)
        if example.exists():
            logger.warning("No personal %s — using %s. Copy it to %s and add your authors.",
                           path.name, example.name, path.name)
            path = example
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    authors: list[AuthorConfig] = []
    for item in payload.get("authors", []):
        root = Path(item["corpus_root"]).expanduser()
        rel_author_dir = Path(item.get("author_dir", f"authors/{item['author_id']}"))
        relevance = item.get("relevance") or {}
        authors.append(
            AuthorConfig(
                author_id=item["author_id"],
                canonical_name=item["canonical_name"],
                aliases=list(item.get("aliases") or []),
                corpus_root=root,
                author_dir=root / rel_author_dir,
                official_preprints=item.get("official_preprints"),
                arxiv_queries=list(item.get("arxiv_queries") or []),
                project_tags=list(item.get("project_tags") or []),
                corpus_tags=list(item.get("corpus_tags") or []),
                direct_patterns=[p.lower() for p in relevance.get("direct", [])],
                project_patterns=[p.lower() for p in relevance.get("project", [])],
            )
        )
    return authors


def parse_official_preprints(html_text: str, author: AuthorConfig) -> list[PaperRecord]:
    if not author.official_preprints:
        return []
    base_url = author.official_preprints.rsplit("/", 1)[0] + "/"
    records: list[PaperRecord] = []
    for block in re.findall(r"<li>(.*?)</li>", html_text, flags=re.I | re.S):
        year_match = re.search(r"\[(\d{4})\]", block)
        if not year_match:
            continue
        link_match = re.search(r'<a\s+href="([^"]+)">(.*?)</a>', block, flags=re.I | re.S)
        if link_match:
            href = html.unescape(link_match.group(1).strip())
            title = clean_text(link_match.group(2)).strip('" ')
            source_url = urllib.parse.urljoin(base_url, href)
        else:
            title = clean_text(re.sub(r"\[\d{4}\]", " ", block, count=1)).strip('" ')
            source_url = ""
        if not title:
            continue
        records.append(
            PaperRecord(
                author_id=author.author_id,
                canonical_author=author.canonical_name,
                title=title,
                year=int(year_match.group(1)),
                authors=[author.canonical_name],
                source_url=source_url,
                official_url=author.official_preprints,
                citation=clean_text(block),
                source_kinds=["official_preprints"],
            )
        )
    return records


def arxiv_url(query: str, max_results: int) -> str:
    encoded = urllib.parse.quote(query)
    return (
        "https://export.arxiv.org/api/query?"
        f"search_query={encoded}&start=0&max_results={max_results}"
        "&sortBy=submittedDate&sortOrder=descending"
    )


def _arxiv_id_parts(abs_url: str) -> tuple[str, str]:
    arxiv_ref = re.sub(r"^https?://arxiv.org/abs/", "", abs_url).strip()
    match = re.match(r"(.+?)(v\d+)?$", arxiv_ref)
    if not match:
        return arxiv_ref, ""
    return match.group(1), match.group(2) or ""


def parse_arxiv_xml(xml_text: str, author: AuthorConfig) -> list[PaperRecord]:
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "arxiv": "http://arxiv.org/schemas/atom",
    }
    root = ET.fromstring(xml_text)
    records: list[PaperRecord] = []
    for entry in root.findall("atom:entry", ns):
        title = clean_text(entry.findtext("atom:title", default="", namespaces=ns))
        if not title:
            continue
        summary = clean_text(entry.findtext("atom:summary", default="", namespaces=ns))
        published = entry.findtext("atom:published", default="", namespaces=ns) or ""
        updated = entry.findtext("atom:updated", default="", namespaces=ns) or ""
        year = int(published[:4]) if published[:4].isdigit() else None
        abs_url = (entry.findtext("atom:id", default="", namespaces=ns) or "").replace("http://", "https://")
        arxiv_id, arxiv_version = _arxiv_id_parts(abs_url)
        authors = [
            clean_text(a.findtext("atom:name", default="", namespaces=ns))
            for a in entry.findall("atom:author", ns)
        ]
        records.append(
            PaperRecord(
                author_id=author.author_id,
                canonical_author=author.canonical_name,
                title=title,
                year=year,
                authors=[a for a in authors if a],
                abstract=summary,
                source_url=abs_url,
                pdf_url=f"https://arxiv.org/pdf/{arxiv_id}" if arxiv_id else "",
                official_url=author.official_preprints or "",
                arxiv_id=arxiv_id,
                arxiv_version=arxiv_version,
                published=published,
                updated=updated,
                source_kinds=["arxiv"],
            )
        )
    return records


def merge_records(records: list[PaperRecord]) -> list[PaperRecord]:
    merged: dict[str, PaperRecord] = {}
    for record in records:
        key = norm_title(record.title)
        existing = merged.get(key)
        if not existing:
            merged[key] = record
            continue

        if record.abstract and not existing.abstract:
            existing.abstract = record.abstract
        if record.pdf_url:
            existing.pdf_url = record.pdf_url
        if record.arxiv_id:
            existing.arxiv_id = record.arxiv_id
            existing.arxiv_version = record.arxiv_version
        if record.published:
            existing.published = record.published
        if record.updated:
            existing.updated = record.updated
        if record.authors and (
            not existing.authors or existing.authors == [existing.canonical_author]
        ):
            existing.authors = record.authors
        if record.source_url and record.source_url != existing.source_url:
            if "official_preprints" in existing.source_kinds:
                existing.official_pdf_or_page = existing.source_url
            elif "official_preprints" in record.source_kinds:
                existing.official_pdf_or_page = record.source_url
            existing.source_url = record.source_url
        if record.citation and not existing.citation:
            existing.citation = record.citation
        for kind in record.source_kinds:
            if kind not in existing.source_kinds:
                existing.source_kinds.append(kind)
        if not existing.year and record.year:
            existing.year = record.year
    return sorted(merged.values(), key=lambda r: (r.year or 0, r.title), reverse=True)


def score_record(record: PaperRecord, author: AuthorConfig) -> tuple[int, list[str], str]:
    haystack = f"{record.title} {record.abstract} {record.citation}".lower()
    matches: list[str] = []
    score = 0
    for pattern in author.direct_patterns:
        if pattern in haystack and pattern not in matches:
            matches.append(pattern)
            score += 3
    for pattern in author.project_patterns:
        if pattern in haystack and pattern not in matches:
            matches.append(pattern)
            score += 1

    if any(pattern in matches for pattern in author.direct_patterns):
        decision = "download_now"
    elif score >= 3:
        decision = "review_then_download"
    elif score >= 1:
        decision = "maybe"
    else:
        decision = "skip_for_now"
    return score, matches, decision


def apply_record_ids(records: list[PaperRecord]) -> None:
    for record in records:
        year = str(record.year or "undated")
        record.paper_id = f"{record.author_id}/{year}-{slugify(record.title)}"


def load_existing_index(author: AuthorConfig) -> tuple[set[str], set[str]]:
    ids: set[str] = set()
    titles: set[str] = set()
    manifest = author.corpus_root / "manifest.jsonl"
    if manifest.exists():
        for line in manifest.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get("paper_id"):
                ids.add(item["paper_id"])
            if item.get("title"):
                titles.add(norm_title(item["title"]))

    if author.author_dir.exists():
        for child in author.author_dir.iterdir():
            if child.is_dir():
                ids.add(f"{author.author_id}/{child.name}")
                meta_path = child / "metadata.yaml"
                if meta_path.exists():
                    try:
                        meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
                    except Exception:
                        meta = {}
                    if meta.get("title"):
                        titles.add(norm_title(str(meta["title"])))
    return ids, titles


def mark_existing(records: list[PaperRecord], existing_ids: set[str], existing_titles: set[str]) -> None:
    for record in records:
        title_key = norm_title(record.title)
        if record.paper_id in existing_ids:
            record.existing = True
            record.existing_reason = "paper_id"
        elif title_key in existing_titles:
            record.existing = True
            record.existing_reason = "title"


def triage_note(record: PaperRecord) -> str:
    matches = ", ".join(record.matched_topics[:8]) if record.matched_topics else "no configured topic match"
    if record.decision == "download_now":
        return f"High-priority for indexing: {matches}."
    if record.decision == "review_then_download":
        return "Likely useful applied-topology or coordination background. Review before deep extraction."
    if record.decision == "maybe":
        return "Possible background value. Keep queued until a project asks for this lineage."
    return "Lower immediate relevance to current sheaf, Spore, discourse, or coordination work."


def metadata_payload(author: AuthorConfig, record: PaperRecord) -> dict[str, Any]:
    return {
        "paper_id": record.paper_id,
        "title": record.title,
        "year": record.year,
        "authors": record.authors or [author.canonical_name],
        "source_url": record.source_url,
        "pdf_url": record.pdf_url,
        "official_url": author.official_preprints or "",
        "official_pdf_or_page": record.official_pdf_or_page,
        "arxiv_id": record.arxiv_id,
        "arxiv_version": record.arxiv_version,
        "published": record.published,
        "updated": record.updated,
        "decision": record.decision,
        "relevance_score": record.relevance_score,
        "matched_topics": record.matched_topics,
        "pdf_status": record.pdf_status,
        "ingest_status": "downloaded" if record.pdf_status in {"downloaded", "already_downloaded"} else "queued",
        "project_tags": author.project_tags,
        "corpus_tags": author.corpus_tags,
        "extraction_profile": "scientific-discourse-v1",
        "source_sensor": SOURCE_SENSOR,
        "created": date.today().isoformat(),
    }


def download_pdf(record: PaperRecord, dest: Path) -> str:
    url = record.pdf_url
    if not url and record.source_url.lower().endswith(".pdf"):
        url = record.source_url
    if not url:
        return "not_downloaded"
    if dest.exists():
        return "already_downloaded"
    try:
        data = fetch_bytes(url)
    except Exception as exc:
        logger.warning("PDF download failed for %s: %s", record.paper_id, exc)
        return "download_failed"
    if len(data) < 1000 or data[:5] != b"%PDF-":
        logger.warning("PDF URL did not return a PDF for %s: %s", record.paper_id, url)
        return "download_failed"
    dest.write_bytes(data)
    time.sleep(0.2)
    return "downloaded"


def write_paper_files(author: AuthorConfig, record: PaperRecord, download_pdfs: bool) -> Path:
    paper_dir = author.corpus_root / "authors" / record.paper_id
    paper_dir.mkdir(parents=True, exist_ok=True)
    if download_pdfs:
        record.pdf_status = download_pdf(record, paper_dir / "source.pdf")
    elif (paper_dir / "source.pdf").exists():
        record.pdf_status = "already_downloaded"

    (paper_dir / "metadata.yaml").write_text(
        yaml.safe_dump(metadata_payload(author, record), sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    abstract_md = [
        f"# {record.title}",
        "",
        f"- Paper ID: `{record.paper_id}`",
        f"- Year: {record.year or 'unknown'}",
        f"- Source: {record.source_url}",
        f"- PDF: {record.pdf_url}",
        f"- Decision: `{record.decision}`",
        f"- Relevance score: {record.relevance_score}",
        f"- Matched topics: {', '.join(record.matched_topics) if record.matched_topics else 'none'}",
        "",
        "## Abstract",
        "",
        record.abstract or "_Abstract not yet available; use PDF conversion pass._",
        "",
        "## Triage Note",
        "",
        triage_note(record),
    ]
    (paper_dir / "abstract.md").write_text("\n".join(abstract_md) + "\n", encoding="utf-8")
    notes_path = paper_dir / "notes.md"
    if not notes_path.exists():
        notes_path.write_text(
            "\n".join(
                [
                    f"# Notes: {record.title}",
                    "",
                    "## Reading Questions",
                    "",
                    "- What claims does this paper make that can be represented as graph facts?",
                    "- What questions, hypotheses, or open problems does it introduce?",
                    "- Which definitions or constructions should become reusable ontology terms?",
                    "- What evidence, proof, experiment, or example supports each central claim?",
                    "- How does this affect Sheaf Explorer, Spore, discourse graphs, or coordination protocols?",
                    "",
                    "## Extraction Todo",
                    "",
                    "- [ ] Convert PDF to Markdown as `extracted.md`.",
                    "- [ ] Extract scientific discourse elements as `discourse-elements.json`.",
                    "- [ ] Extract candidate triples as `triples.jsonl`.",
                    "- [ ] Promote project-specific insights into relevant bridge notes.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
    record.corpus_path = str(paper_dir)
    return paper_dir


def append_manifest(author: AuthorConfig, records: list[PaperRecord]) -> None:
    manifest = author.corpus_root / "manifest.jsonl"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    existing = set()
    if manifest.exists():
        for line in manifest.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get("paper_id"):
                existing.add(item["paper_id"])
    with manifest.open("a", encoding="utf-8") as f:
        for record in records:
            if record.paper_id in existing:
                continue
            f.write(
                json.dumps(
                    {
                        "paper_id": record.paper_id,
                        "title": record.title,
                        "year": record.year,
                        "decision": record.decision,
                        "source_url": record.source_url,
                        "source_sensor": SOURCE_SENSOR,
                    },
                    ensure_ascii=True,
                )
                + "\n"
            )


def event_text(record: PaperRecord) -> str:
    parts = [
        f"New research paper detected: {record.title}",
        f"Author monitor: {record.canonical_author}",
        f"Paper ID: {record.paper_id}",
        f"Decision: {record.decision}",
        f"Matched topics: {', '.join(record.matched_topics) if record.matched_topics else 'none'}",
        f"Source URL: {record.source_url}",
    ]
    if record.abstract:
        parts.extend(["", "Abstract:", record.abstract])
    parts.extend(["", "Triage:", triage_note(record)])
    return "\n".join(parts).strip()


def koi_event_rid(record: PaperRecord) -> str:
    digest = hashlib.sha256(record.paper_id.encode("utf-8")).hexdigest()[:16]
    return f"{SOURCE_SENSOR}:{record.author_id}:{digest}"


async def emit_koi_event(
    conn: asyncpg.Connection,
    provider: Any,
    chunker: TextChunker,
    author: AuthorConfig,
    record: PaperRecord,
    no_embed: bool,
) -> str:
    rid = koi_event_rid(record)
    exists = await conn.fetchval("SELECT 1 FROM koi_memories WHERE rid = $1", rid)
    if exists:
        await conn.execute(
            "UPDATE koi_memories SET last_seen_at = NOW(), metadata = metadata || $1::jsonb WHERE rid = $2",
            json.dumps({"last_seen_by": SOURCE_SENSOR, "paper_id": record.paper_id}),
            rid,
        )
        return "skipped_idempotent"

    text = event_text(record)
    chunks = chunker.chunk_text(text)[:MAX_CHUNKS_PER_PAPER_EVENT]
    embeddings: list[Optional[list[float]]] = []
    if not no_embed:
        if provider is None:
            raise RuntimeError("Embedding provider not configured; source config/personal.env or pass --no-embed")
        for chunk in chunks:
            emb = await provider.embed(chunk["text"])
            if len(emb) != 3072:
                raise RuntimeError(f"expected 3072-dim embedding, got {len(emb)}")
            embeddings.append(emb)
    else:
        embeddings = [None for _ in chunks]

    published_at = parse_datetime(record.published)
    content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    content = {
        "text": text,
        "title": record.title,
        "paper_id": record.paper_id,
        "source_url": record.source_url,
    }
    metadata = {
        "repo": "research-papers",
        "source_type": "research_author_publication",
        "author_id": author.author_id,
        "author": author.canonical_name,
        "paper_id": record.paper_id,
        "paper_title": record.title,
        "year": record.year,
        "source_url": record.source_url,
        "pdf_url": record.pdf_url,
        "arxiv_id": record.arxiv_id,
        "decision": record.decision,
        "relevance_score": record.relevance_score,
        "matched_topics": record.matched_topics,
        "project_tags": author.project_tags,
        "corpus_tags": author.corpus_tags,
        "corpus_path": record.corpus_path,
        "pdf_status": record.pdf_status,
    }
    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO koi_memories
                (rid, event_type, source_sensor, content, metadata, access_source,
                 published_at, content_hash)
            VALUES ($1, 'NEW', $2, $3::jsonb, $4::jsonb, $5, $6, $7)
            ON CONFLICT (rid) DO NOTHING
            """,
            rid,
            SOURCE_SENSOR,
            json.dumps(content),
            json.dumps(metadata),
            ACCESS_SOURCE,
            published_at,
            content_hash,
        )
        for chunk, emb in zip(chunks, embeddings):
            chunk_index = int(chunk["index"])
            await conn.execute(
                """
                INSERT INTO koi_memory_chunks
                    (chunk_rid, document_rid, chunk_index, total_chunks, content,
                     embedding_3072, metadata)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector, $7::jsonb)
                ON CONFLICT (chunk_rid) DO NOTHING
                """,
                f"{rid}:{chunk_index}",
                rid,
                chunk_index,
                len(chunks),
                json.dumps({"text": chunk["text"], "context": f"research-paper:{record.paper_id}"}),
                json.dumps(emb) if emb is not None else None,
                json.dumps(metadata),
            )
    return "inserted"


def source_cache_paths(author: AuthorConfig, source_name: str) -> Path:
    safe_author = slugify(author.author_id)
    return author.corpus_root / "_sources" / f"{safe_author}-{source_name}"


def fetch_author_records(author: AuthorConfig, max_results: int, cache_sources: bool) -> tuple[list[PaperRecord], dict[str, Any]]:
    records: list[PaperRecord] = []
    stats: dict[str, Any] = {
        "sources_succeeded": 0,
        "sources_failed": 0,
        "source_errors": [],
        "official_items": 0,
        "arxiv_items": 0,
    }

    if author.official_preprints:
        try:
            official_html = fetch_text(author.official_preprints)
            if cache_sources:
                path = source_cache_paths(author, "preprints.html")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(official_html, encoding="utf-8")
            official_records = parse_official_preprints(official_html, author)
            stats["official_items"] = len(official_records)
            records.extend(official_records)
            stats["sources_succeeded"] += 1
        except Exception as exc:
            stats["sources_failed"] += 1
            stats["source_errors"].append({"source": author.official_preprints, "error": str(exc)})
            logger.warning("Official source failed for %s: %s", author.author_id, exc)

    for index, query in enumerate(author.arxiv_queries):
        url = arxiv_url(query, max_results)
        try:
            xml_text = fetch_text(url)
            if cache_sources:
                path = source_cache_paths(author, f"arxiv-{index}.xml")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(xml_text, encoding="utf-8")
            arxiv_records = parse_arxiv_xml(xml_text, author)
            stats["arxiv_items"] += len(arxiv_records)
            records.extend(arxiv_records)
            stats["sources_succeeded"] += 1
        except Exception as exc:
            stats["sources_failed"] += 1
            stats["source_errors"].append({"source": url, "error": str(exc)})
            logger.warning("arXiv source failed for %s: %s", author.author_id, exc)
    return records, stats


async def run(args: argparse.Namespace) -> dict[str, Any]:
    authors = load_config(Path(args.config))
    if args.author:
        authors = [a for a in authors if a.author_id == args.author]
    if not authors:
        raise RuntimeError("No configured authors matched")

    provider = None
    if args.apply and not args.no_koi and not args.no_embed:
        provider = create_embedding_provider()
        if provider is None:
            raise RuntimeError("Embedding provider not configured; source config/personal.env or pass --no-embed")

    conn = None
    if args.apply and not args.no_koi:
        conn = await asyncpg.connect(POSTGRES_URL)

    chunker = TextChunker(chunk_size=500, chunk_overlap=50, min_chunk_size=20)
    summary: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry-run",
        "authors_total": len(authors),
        "sources_succeeded": 0,
        "sources_failed": 0,
        "items_seen": 0,
        "items_merged": 0,
        "new_items": 0,
        "corpus_written": 0,
        "pdfs_downloaded": 0,
        "koi_events_inserted": 0,
        "koi_events_skipped": 0,
        "authors": {},
        "partial": False,
    }

    try:
        for author in authors:
            raw_records, stats = fetch_author_records(
                author,
                max_results=args.max_results,
                cache_sources=args.apply and args.cache_sources,
            )
            records = merge_records(raw_records)
            apply_record_ids(records)
            for record in records:
                score, matches, decision = score_record(record, author)
                record.relevance_score = score
                record.matched_topics = matches
                record.decision = decision
            existing_ids, existing_titles = load_existing_index(author)
            mark_existing(records, existing_ids, existing_titles)
            new_records = [r for r in records if not r.existing]
            new_records.sort(key=lambda r: (r.year or 0, r.published or "", r.title), reverse=True)
            if args.max_new is not None:
                new_records = new_records[: args.max_new]

            author_summary = {
                **stats,
                "items_seen": len(raw_records),
                "items_merged": len(records),
                "new_items": len(new_records),
                "new_papers": [
                    {
                        "paper_id": r.paper_id,
                        "title": r.title,
                        "year": r.year,
                        "decision": r.decision,
                        "score": r.relevance_score,
                        "source_url": r.source_url,
                    }
                    for r in new_records
                ],
            }
            summary["sources_succeeded"] += stats["sources_succeeded"]
            summary["sources_failed"] += stats["sources_failed"]
            summary["items_seen"] += len(raw_records)
            summary["items_merged"] += len(records)
            summary["new_items"] += len(new_records)

            if args.apply:
                written_records: list[PaperRecord] = []
                for record in new_records:
                    paper_dir = write_paper_files(author, record, args.download_pdfs)
                    written_records.append(record)
                    summary["corpus_written"] += 1
                    if record.pdf_status == "downloaded":
                        summary["pdfs_downloaded"] += 1
                    logger.info("queued %s in %s", record.paper_id, paper_dir)
                    if conn is not None:
                        status = await emit_koi_event(conn, provider, chunker, author, record, args.no_embed)
                        if status == "inserted":
                            summary["koi_events_inserted"] += 1
                        elif status == "skipped_idempotent":
                            summary["koi_events_skipped"] += 1
                append_manifest(author, written_records)

            summary["authors"][author.author_id] = author_summary
    finally:
        if conn is not None:
            await conn.close()

    summary["partial"] = summary["sources_failed"] > 0
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--author", help="Only run one configured author_id")
    parser.add_argument("--apply", action="store_true", help="Write corpus files and KOI events")
    parser.add_argument("--download-pdfs", action="store_true", help="Download public PDFs for newly queued papers")
    parser.add_argument("--no-koi", action="store_true", help="Do not write KOI memory events in --apply mode")
    parser.add_argument("--no-embed", action="store_true", help="Write KOI parent events without embedding chunks")
    parser.set_defaults(cache_sources=True)
    parser.add_argument("--cache-sources", dest="cache_sources", action="store_true", help="Cache fetched source pages/XML in the paper corpus")
    parser.add_argument("--no-cache-sources", dest="cache_sources", action="store_false", help="Do not update cached source pages/XML")
    parser.add_argument("--max-results", type=int, default=200, help="Max arXiv results per configured query")
    parser.add_argument("--max-new", type=int, default=None, help="Cap newly queued papers per author")
    args = parser.parse_args()

    try:
        summary = asyncio.run(run(args))
    except Exception as exc:
        logger.error("research author sensor failed: %s", exc)
        return 4

    print(json.dumps(summary, indent=2, sort_keys=True))
    if summary["sources_succeeded"] == 0:
        return 2
    return 2 if summary["partial"] else 0


if __name__ == "__main__":
    sys.exit(main())
