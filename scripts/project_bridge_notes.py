#!/usr/bin/env python3
"""
Learning Field Graph Projection

Projects bridge notes from the canon-bearing repos registered in your projects
config (config/projects.json — see config/projects.example.json) into the KOI
knowledge graph as structured Claim, Concept, and Question entities with
argumentative edges (supports/opposes). Project keys, learning-field claimant
URIs, and bridge-note directories are operator-specific and are NOT baked into
this tool — see _load_projects() below.

This script covers one surface of a graph projection, bridge-note intake; it does
not encode any wider taxonomy of views over the canon repos.

Two claim layers:
  - Source claims: extracted from bridge note Claim Registers
  - Review claims: about proposed canon changes (derived from relates_to × concept)

Usage:
  python scripts/project_bridge_notes.py --dry-run          # preview what would be created
  python scripts/project_bridge_notes.py --apply            # create entities and edges
  python scripts/project_bridge_notes.py --apply --note <path>  # single note
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import asyncpg
import httpx
import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("project_bridge_notes")

KOI_BASE = "http://localhost:8351"

def _load_projects() -> dict:
    """Load the operator-local project registry.

    koi-processor ships with NO baked-in projects: project keys, learning-field
    claimant URIs, and bridge-note directories are operator-specific. Register
    your own by copying config/projects.example.json to config/projects.json
    (gitignored), or point KOI_PROJECTS_CONFIG at a JSON file. Per-key schema:

        {"project_id": str, "claimant_uri": str, "bridge_dir": str}

    where bridge_dir is a filesystem path with ~ and $VARS expanded.
    """
    candidates = []
    if os.getenv("KOI_PROJECTS_CONFIG"):
        candidates.append(Path(os.environ["KOI_PROJECTS_CONFIG"]).expanduser())
    candidates.append(Path(__file__).resolve().parent.parent / "config" / "projects.json")
    for cfg_path in candidates:
        if cfg_path.is_file():
            raw = json.loads(cfg_path.read_text())
            return {
                key: {
                    "project_id": entry["project_id"],
                    "claimant_uri": entry["claimant_uri"],
                    "bridge_dir": Path(os.path.expandvars(entry["bridge_dir"])).expanduser(),
                }
                for key, entry in raw.items()
                if not key.startswith("_")  # skip _comment-style keys
            }
    log.warning(
        "No projects config found (looked for $KOI_PROJECTS_CONFIG and "
        "config/projects.json). Copy config/projects.example.json to "
        "config/projects.json to register your canon repos."
    )
    return {}


PROJECTS = _load_projects()

DISPOSITION_SLUG = {
    "clarify existing term": "clarify",
    "candidate primitive": "propose-primitive",
    "candidate pattern": "propose-pattern",
    "implementation hypothesis": "hypothesize",
    "unresolved tension": "resolve-tension",
    "no change": "no-change",
    # New-dialect disposition controlled vocab. These
    # are already slug-shaped; map to themselves. "n/a" -> "n-a" (slug-safe).
    "canon-pressure-decision-brief": "canon-pressure-decision-brief",
    "framing-note-only": "framing-note-only",
    "decline-with-trigger": "decline-with-trigger",
    "not-present": "not-present",
    "n/a": "n-a",
}

# New-dialect R-claim disposition controlled vocab. Every R-row becomes a
# queryable governance cluster; only canon-pressure-decision-brief proposes change
# (carries the supports stance). The rest are descriptive "no change proposed"
# but still get a cluster so the convergence board can answer concept × target →
# disposition (note). Ordered most-specific-first for prefix normalization.
NEW_DISPOSITIONS = (
    "canon-pressure-decision-brief",
    "framing-note-only",
    "decline-with-trigger",
    "not-present",
    "n/a",
)
PROPOSE_DISPOSITIONS = {"canon-pressure-decision-brief"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BridgeClaim:
    c_id: str           # e.g. "C1"
    confidence: str     # high | medium | low
    anchor: str         # e.g. "§What the canon does not already have — closure risk"
    statement: str      # claim text

@dataclass
class OpenQuestion:
    number: int
    question: str       # question text (bold part)
    context: str        # full text including context

@dataclass
class ReviewDirective:
    """Explicit review claim parsed from ## Review Claims (old dialect) or from
    a ## R-claim disposition table row (new dialect)."""
    r_id: str                  # e.g. "R1" / "R-LifeValue" / synthesized "Rrow3"
    target_doc: str            # primary target doc (first if "or")
    concept: str               # explicit concept slug
    statement: str             # claim text
    supported_by: list         # list of C-IDs (e.g. ["C1", "C2"])
    disposition: Optional[str] = None  # new-dialect per-row disposition; None for old dialect

@dataclass
class BridgeNote:
    path: Path
    doc_id: str
    doc_kind: str
    status: str
    disposition: str
    research_subkind: str
    concepts: list
    depends_on: list
    relates_to: list
    claims: list        # list of BridgeClaim
    questions: list     # list of OpenQuestion
    review_directives: list  # list of ReviewDirective (may be empty)
    project_key: str    # a key of the operator's projects config


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_frontmatter(text: str) -> dict:
    """Extract YAML frontmatter from markdown."""
    m = re.match(r'^---\s*\n(.*?)\n---', text, re.DOTALL)
    if not m:
        raise ValueError("No YAML frontmatter found")
    return yaml.safe_load(m.group(1))


def parse_claims(text: str) -> list[BridgeClaim]:
    """Parse Claim Register section for C-ID entries."""
    # Find the Claim Register section (may or may not have number prefix)
    m = re.search(r'## (?:\d+\.\s+)?Claim Register\s*\n(.*?)(?=\n## |\Z)', text, re.DOTALL)
    if not m:
        return []

    section = m.group(1)
    claims = []

    # Pattern: **C1** [confidence: high] [anchor: §section] Statement text
    pattern = re.compile(
        r'\*\*(C\d+)\*\*\s+'
        r'\[confidence:\s*(high|medium|low)\]\s+'
        r'\[anchor:\s*(.+?)\]\s*\n'
        r'(.*?)(?=\n\*\*C\d+\*\*|\Z)',
        re.DOTALL
    )

    for match in pattern.finditer(section):
        statement = match.group(4).strip()
        # Clean up any trailing whitespace/newlines
        statement = re.sub(r'\s+', ' ', statement)
        claims.append(BridgeClaim(
            c_id=match.group(1),
            confidence=match.group(2),
            anchor=match.group(3).strip(),
            statement=statement,
        ))

    return claims


def parse_questions(text: str) -> list[OpenQuestion]:
    """Parse Open Questions section (may have numbered header like '## 7. Open Questions')."""
    m = re.search(r'## (?:\d+\.\s+)?Open Questions\s*\n(.*?)(?=\n## |\Z)', text, re.DOTALL)
    if not m:
        return []

    section = m.group(1)
    questions = []

    # Two formats:
    #   "1. **Question text?** Context..."
    #   "1. **Topic:** Context with question?..."
    # Capture the full numbered entry, then extract bold part + rest
    pattern = re.compile(
        r'(\d+)\.\s+\*\*(.+?)\*\*\s*(.*?)(?=\n\d+\.\s+\*\*|\Z)',
        re.DOTALL
    )

    for match in pattern.finditer(section):
        bold_part = match.group(2).strip()
        rest = match.group(3).strip()
        rest = re.sub(r'\s+', ' ', rest)
        # The "question" is the bold part; full text is bold + rest
        full_text = f"{bold_part} {rest}".strip() if rest else bold_part
        questions.append(OpenQuestion(
            number=int(match.group(1)),
            question=bold_part,
            context=full_text,
        ))

    return questions


def parse_review_directives(text: str) -> list[ReviewDirective]:
    """Parse R-claim directives with explicit target + concept.

    Searches both '## Review Claims' and '## Claim Register' sections,
    since R-claims may appear alongside C-claims in the Claim Register.
    """
    # Try dedicated section first, then Claim Register, then full text
    section = None
    for header in [r'Review Claims', r'Claim Register']:
        m = re.search(rf'## (?:\d+\.\s+)?{header}\s*\n(.*?)(?=\n## |\Z)', text, re.DOTALL)
        if m:
            section = m.group(1)
            # Check if this section actually contains R-claims
            if re.search(r'\*\*R\d+\*\*\s+\[review claim\]', section):
                break
            section = None

    if not section:
        return []
    directives = []

    # Pattern: **R1** [review claim] [target: doc.id] [concept: slug]
    # Statement text
    # *R1 is supported by C1, C2, C3.*
    pattern = re.compile(
        r'\*\*(R\d+)\*\*\s+'
        r'\[review claim\]\s+'
        r'\[target:\s*([^\]]+)\]\s+'
        r'\[concept:\s*([^\]]+)\]\s*\n'
        r'(.*?)(?=\n\*\*R\d+\*\*|\Z)',
        re.DOTALL
    )

    for match in pattern.finditer(section):
        r_id = match.group(1)

        # Parse target: "a or b" → primary is first, alternatives ignored
        target_str = match.group(2).strip()
        targets = [t.strip() for t in re.split(r'\s+or\s+', target_str)]
        primary_target = targets[0]
        if len(targets) > 1:
            log.info(f"  {r_id}: target '{target_str}' → primary: {primary_target}")

        concept = match.group(3).strip()

        body = match.group(4).strip()

        # Extract supported_by from italic line: *R1 is supported by C1, C2, C3.*
        supported_by = []
        sup_match = re.search(r'\*R\d+\s+is supported by\s+([^*]+)\*', body)
        if sup_match:
            sup_text = sup_match.group(1)
            # Extract C-IDs, ignoring "Relates to..." suffix
            sup_text = re.split(r'\.\s*Relates to', sup_text)[0]
            supported_by = [c.strip().rstrip('.') for c in re.split(r',\s*', sup_text) if c.strip()]
            # Remove the support line from statement
            body = re.sub(r'\n?\*R\d+\s+is supported by[^*]*\*', '', body).strip()

        statement = re.sub(r'\s+', ' ', body)

        directives.append(ReviewDirective(
            r_id=r_id,
            target_doc=primary_target,
            concept=concept,
            statement=statement,
            supported_by=supported_by,
        ))

    return directives


# ---------------------------------------------------------------------------
# NEW-DIALECT parsers: "## C-claims" list-bullet blockquotes
# + "## R-claim disposition table". The old-dialect parsers above are preserved
# unchanged so claims already projected from old-dialect notes project identically.
# ---------------------------------------------------------------------------

_CCLAIM_SECTION_NEW = re.compile(
    # Any H2 header containing "C-claims" (tolerates "## 2. C-claims", "## §2 Verbatim
    # C-claims", "## C-claims"); body runs to the next H2 (### sub-headers don't terminate).
    r'^## [^\n]*?\bC-?claims?\b[^\n]*\n(.*?)(?=\n## |\Z)',
    re.DOTALL | re.IGNORECASE | re.MULTILINE,
)
# C-claim head: optional "- " bullet, then **C<n> at line start. Matches BOTH the
# inline list-bullet form (- **C-1** [anchor] "...") AND the header form
# (**C-1 [pdf-p1]** — label, statement in a following blockquote). Table rows (lines
# starting with "|") never match. Anchor + statement are parsed from the remainder.
_CLAIM_HEAD = re.compile(r'^(?:-\s+)?\*\*(C-?\d+[a-z]?)\b(.*)$')      # bold form (any **C-N ...); spurious `**C5 Label:**` headers are dropped by the no-anchor post-filter in parse_claims_new, NOT by tightening this regex (which would clip real citation-/punctuation-bearing forms)
_CLAIM_HEAD_H = re.compile(r'^#{2,4}\s+(C-?\d+[a-z]?)\b(.*)$')        # markdown-header form (### C-1 ...)
_ANCHOR_MARKER = re.compile(r'(?:anchor:|html-section:|pdf-p|§)', re.IGNORECASE)
_BRACKET = re.compile(r'\[([^\]]*)\]')
# Leading run of **/LOAD-BEARING/[anchor] tokens that PRECEDE the statement. Stripping
# only this leading run (not all brackets) removes the anchor bracket — and any stray
# quote inside it — without mangling editorial brackets ([Sen's], [is]) inside the statement.
_LEAD = re.compile(r'^(?:\s|\*\*|LOAD-BEARING|\[[^\]]*\])*')


def _claim_head(line: str):
    """Match a C-claim head in either form (bold-bullet or markdown-header)."""
    return _CLAIM_HEAD.match(line) or _CLAIM_HEAD_H.match(line)


def _pick_anchor(*sources: str) -> str:
    """Anchor = first anchor-marker bracket; else a page-like parenthetical; else the
    first non-LOAD-BEARING bracket; else ''. (Anchor is evidence metadata, not load-bearing.)"""
    brackets = [b for s in sources for b in _BRACKET.findall(s)]
    am = next((b for b in brackets if _ANCHOR_MARKER.search(b)), None)
    if am:
        return am.strip()
    for s in sources:
        pm = re.search(r'\(([^)]*?(?:pdf-p\d|p\.?\s*\d|§|Abstract|Summary|TOC|Cover)[^)]*)\)', s)
        if pm:
            return pm.group(1).strip()
    nb = next((b for b in brackets if "LOAD-BEARING" not in b.upper()), None)
    return nb.strip() if nb else ""


def _merge_claims(*lists) -> list[BridgeClaim]:
    """Concatenate claim lists, deduping by c_id (first wins)."""
    out, seen = [], set()
    for lst in lists:
        for c in lst:
            if c.c_id not in seen:
                seen.add(c.c_id)
                out.append(c)
    return out


def parse_claims_new(text: str) -> list[BridgeClaim]:
    """Parse the new-dialect ``## C-claims`` section.

    Two layouts occur; both handled:
      * inline list-bullet — ``- **C-1** [anchor] "statement"`` (4 catalogued variants:
        hyphen-optional id, citation-parenthetical-in-bold, [anchor:]/[html-section:]/
        [pdf-pN] labels, separate **LOAD-BEARING** token).
      * header + blockquote — ``**C-1 [pdf-p1]** — label`` (no bullet, anchor inside the
        bold) optionally followed by ``> "verbatim statement"`` blockquote line(s) (the
        one-label-per-claim layout).
    Three head layouts occur and all are handled:
      * inline list-bullet — ``- **C-1** [anchor] "statement"`` (4 catalogued variants).
      * header-in-bold + blockquote — ``**C-1 [pdf-p1]** — label`` then ``> "..."``.
      * markdown-header + blockquote — ``### C-1 (p3, Abstract) — label [LOAD-BEARING]``
        then ``> "..." [pdf-pN]`` (markdown-header layout).
    Anchor via _pick_anchor (head brackets + parenthetical + blockquote); load_bearing if
    'LOAD-BEARING' appears on the head line or its blockquote. Table rows (lines starting
    with '|') never match — heads require '**C', '- **C', or '## C'/'### C'.
    """
    m = _CCLAIM_SECTION_NEW.search(text)
    if not m:
        return []
    lines = m.group(1).splitlines()
    claims: list[BridgeClaim] = []
    seen: set = set()
    i = 0
    while i < len(lines):
        hm = _claim_head(lines[i].strip())
        if not hm:
            i += 1
            continue
        c_id = hm.group(1).replace("-", "")        # "C-1" -> "C1"
        rest = hm.group(2)
        load_bearing = "LOAD-BEARING" in rest
        blob = ""
        qm = re.search(r'"(.+)"', _LEAD.sub("", rest))  # strip ONLY the leading anchor/bold run (kills the in-anchor quote) then GREEDY first..last — preserves editorial brackets + inner quotes inside the statement
        if qm:
            statement = qm.group(1).strip()
        else:
            # No inline quote: scan forward for a blockquote statement before the next
            # C-head (the header + ``> "..."`` layout); accumulate consecutive > lines.
            statement = ""
            j = i + 1
            while j < len(lines) and not _claim_head(lines[j].strip()):
                if lines[j].strip().startswith(">"):
                    qlines = []
                    while j < len(lines) and lines[j].strip().startswith(">"):
                        if "LOAD-BEARING" in lines[j]:
                            load_bearing = True
                        qlines.append(lines[j].strip().lstrip(">").strip())
                        j += 1
                    blob = " ".join(qlines)
                    qq = re.search(r'"(.+)"', _LEAD.sub("", blob))  # strip leading anchor/bold run, then greedy (keep inner quotes + editorial brackets)
                    statement = qq.group(1).strip() if qq else blob.strip().strip('"').strip()
                    break
                j += 1
            if not statement:
                # one-liner label: drop bold + the anchor bracket(s), keep the prose label
                tail = _BRACKET.sub('', rest.replace("**", ""))
                statement = re.sub(r'\s+', ' ', tail).strip(" —-:*").strip()
        anchor = _pick_anchor(rest, blob)
        if not anchor.strip():
            # HARDENING (a): no evidence anchor -> a bold/header **C<n> ...** with no
            # [anchor]/(page)/§ is a subsection label (e.g. **C5 Label:**), not a real
            # evidence-anchored claim. Drop it. Form-agnostic (doesn't clip citation/
            # punctuation head forms the way a tightened head regex did). 0 occurrences in
            # the corpora seen so far -> no-op now; forward-proofing for other corpora.
            i += 1
            continue
        if c_id in seen:
            i += 1
            continue
        seen.add(c_id)
        claims.append(BridgeClaim(
            c_id=c_id,
            confidence="high" if load_bearing else "medium",
            anchor=anchor,
            statement=re.sub(r'\s+', ' ', statement),
        ))
        i += 1
    return claims


_RTABLE_SECTION = re.compile(
    # Any H2 header containing "R-claim disposition table" (tolerates "## 3. ...",
    # "## §3 ... (...)"); body runs to the next H2.
    r'^## [^\n]*?R-claim disposition table[^\n]*\n(.*?)(?=\n## |\Z)',
    re.DOTALL | re.IGNORECASE | re.MULTILINE,
)
_TABLE_ROW_SEP = re.compile(r'^\s*\|?[\s:|\-]+\|?\s*$')
_SPORE_DOCID = re.compile(r'(spore\.[a-z0-9][a-z0-9.\-]+)')
_BACKTICK_SPAN = re.compile(r'`([^`]+)`')
_CID_REF = re.compile(r'\bC-?\d+[a-z]?\b')


def _clean_cell(cell: str) -> str:
    # Remove ALL backticks + bold markers (not just edges): a `**`token`**`` cell would
    # otherwise leave stray backticks after **-removal and break disposition normalization.
    return cell.replace("**", "").replace("`", "").strip()


def _extract_target(cell: str) -> str:
    """Canonical target: spore.* doc_id > first backtick span > cleaned prose (parens stripped)."""
    md = _SPORE_DOCID.search(cell)
    if md:
        return md.group(1)
    bm = _BACKTICK_SPAN.search(cell)
    if bm:
        return bm.group(1).strip()
    return re.sub(r'\([^)]*\)', '', _clean_cell(cell)).strip()


def _normalize_disposition(cell: str) -> str:
    d = _clean_cell(cell).lower().lstrip("*").strip()
    for v in NEW_DISPOSITIONS:
        if d.startswith(v):
            return v
    return d


def parse_disposition_table(text: str) -> list[ReviewDirective]:
    """Parse the new-dialect ``## R-claim disposition table`` into ReviewDirectives.

    Columns vary (optional leading #/R-id, Target, Concept, Disposition, One-line);
    located by header name. Emits one ReviewDirective per (row x concept); a row's
    one-line cell C-IDs populate ``supported_by`` (precise C->R edges), with a
    concept-match fallback applied in the apply path when a row names no C-IDs.
    Rows with no resolvable target (e.g. routing-completeness "(none)") are skipped
    with a warning.
    """
    m = _RTABLE_SECTION.search(text)
    if not m:
        return []
    rows = [ln for ln in m.group(1).splitlines() if ln.strip().startswith("|")]
    if len(rows) < 2:
        return []
    header = [h.strip().lower() for h in rows[0].strip().strip("|").split("|")]

    def col(*names):
        for i, h in enumerate(header):
            if any(n in h for n in names):
                return i
        return None

    ti, ci, di = col("target"), col("concept"), col("disposition")
    idi = col("#", "r-id", "r-claim")
    if ci is not None and ci == ti:
        # merged "Target + concept" column (one note schema): keep it as target,
        # generalize concept to 'unspecified' rather than duplicating the merged text.
        ci = None
    if ti is None or di is None:
        # No "Target"/"Concept" column (e.g. a note with Resonance /
        # Resolved-state-citation prose schema): leave unparsed + surfaced, rather than
        # mapping prose into messy cluster keys. Operator can normalize that note's header.
        return []
    out: list[ReviewDirective] = []
    for ri, ln in enumerate(rows[1:]):
        if _TABLE_ROW_SEP.match(ln):
            continue
        cells = ln.strip().strip("|").split("|")
        if len(cells) <= max(x for x in (ti, ci, di) if x is not None):
            continue
        target = _extract_target(cells[ti])
        tlow = _clean_cell(cells[ti]).lower()
        if not target or tlow.startswith("none") or tlow in ("—", "-", "n/a", ""):
            log.warning(f"  disposition-table row {ri + 1}: no resolvable target "
                        f"({cells[ti].strip()!r}) — skipped")
            continue
        disposition = _normalize_disposition(cells[di])
        r_id = (_clean_cell(cells[idi]) if idi is not None and idi < len(cells) else "") or f"Rrow{ri + 1}"
        oneline = cells[-1].strip() if (len(cells) - 1) > di else ""
        supported_by = sorted({c.replace("-", "") for c in _CID_REF.findall(oneline)})
        concepts: list = []
        if ci is not None and ci < len(cells):
            craw = re.sub(r'^\((.*)\)$', r'\1', _clean_cell(cells[ci])).strip()
            if craw and craw not in ("—", "-"):
                concepts = [c.strip() for c in craw.split(",") if c.strip()]
        if not concepts:
            # Blank concept -> a target-level cluster (no concept node). concept=None
            # is handled in the apply path (about_uri=spec:target, gck=target:disposition).
            concepts = [None]
        for concept in concepts:
            out.append(ReviewDirective(
                r_id=r_id,
                target_doc=target,
                concept=concept,
                statement=re.sub(r'\s+', ' ', _clean_cell(oneline)),
                supported_by=supported_by,
                disposition=disposition,
            ))
    return out


def parse_bridge_note(path: Path, project_key: str) -> BridgeNote:
    """Parse a bridge note file into structured data."""
    text = path.read_text()
    fm = parse_frontmatter(text)

    return BridgeNote(
        path=path,
        doc_id=fm.get("doc_id", ""),
        doc_kind=fm.get("doc_kind", "research"),
        status=fm.get("status", "draft"),
        disposition=fm.get("disposition", ""),
        research_subkind=fm.get("research_subkind", ""),
        concepts=fm.get("concepts") or [],
        depends_on=fm.get("depends_on") or [],
        relates_to=fm.get("relates_to") or [],
        claims=_merge_claims(parse_claims(text), parse_claims_new(text)),
        questions=parse_questions(text),
        review_directives=parse_review_directives(text) + parse_disposition_table(text),
        project_key=project_key,
    )


# ---------------------------------------------------------------------------
# Entity URI generation (mirrors KOI's generate_entity_uri)
# ---------------------------------------------------------------------------

def normalize_text(text: str) -> str:
    """Normalize entity text for matching."""
    return text.lower().strip()

def generate_entity_uri(name: str, entity_type: str) -> str:
    """Generate a deterministic URI matching KOI's convention."""
    normalized = normalize_text(name)
    hash_input = f"{entity_type}:{normalized}"
    hash_id = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
    type_prefix = entity_type.lower()
    safe_name = normalized.replace(' ', '-').replace("'", '')[:50]
    return f"orn:personal-koi.entity:{type_prefix}-{safe_name}-{hash_id}"


# ---------------------------------------------------------------------------
# KOI interactions
# ---------------------------------------------------------------------------

async def resolve_project_uri(conn: asyncpg.Connection, project_id: str) -> str:
    """Resolve project URI from KOI. Fail closed if not exactly 1 row."""
    rows = await conn.fetch(
        "SELECT fuseki_uri FROM entity_registry "
        "WHERE entity_type = 'Project' AND metadata->>'project_id' = $1",
        project_id,
    )
    if len(rows) == 0:
        raise RuntimeError(f"Project entity not found for project_id='{project_id}'. "
                           "Run ingest_spec_dag.py first.")
    if len(rows) > 1:
        uris = [r["fuseki_uri"] for r in rows]
        raise RuntimeError(f"Ambiguous project for project_id='{project_id}': {uris}")
    return rows[0]["fuseki_uri"]


async def resolve_or_create_concept(
    conn: asyncpg.Connection,
    concept_name: str,
) -> str:
    """Resolve a concept name to an entity URI, creating if needed."""
    # Try exact match first
    normalized = normalize_text(concept_name)
    row = await conn.fetchrow(
        "SELECT fuseki_uri FROM entity_registry "
        "WHERE normalized_text = $1 AND entity_type = 'Concept' LIMIT 1",
        normalized,
    )
    if row:
        return row["fuseki_uri"]

    # Also try with hyphens replaced by spaces (concept tags use hyphens)
    normalized_spaced = normalized.replace("-", " ")
    if normalized_spaced != normalized:
        row = await conn.fetchrow(
            "SELECT fuseki_uri FROM entity_registry "
            "WHERE normalized_text = $1 AND entity_type = 'Concept' LIMIT 1",
            normalized_spaced,
        )
        if row:
            return row["fuseki_uri"]

    # Create new concept entity
    uri = generate_entity_uri(concept_name.replace("-", " "), "Concept")
    entity_text = concept_name.replace("-", " ").title()

    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, metadata) "
        "VALUES ($1, $2, 'Concept', $3, $4::jsonb) "
        "ON CONFLICT (fuseki_uri) DO NOTHING",
        uri, entity_text, normalized_spaced or normalized,
        json.dumps({"source": "learning_field"}),
    )
    log.info(f"  Created Concept: {entity_text} → {uri}")
    return uri


async def find_previous_source_claim(
    conn: asyncpg.Connection,
    source_document: str,
    c_id: str,
) -> Optional[dict]:
    """Find the most recent source claim for a (doc, C-ID) pair."""
    row = await conn.fetchrow(
        "SELECT claim_rid, entity_uri, statement, "
        "       metadata->>'evidence_anchor' AS evidence_anchor, "
        "       metadata->>'confidence' AS confidence, "
        "       metadata->>'project_uri' AS project_uri "
        "FROM claims "
        "WHERE source_document = $1 "
        "  AND metadata->>'c_id' = $2 "
        "  AND metadata->>'source' = 'learning_field' "
        "  AND metadata->>'claim_layer' = 'source' "
        "ORDER BY created_at DESC LIMIT 1",
        source_document, c_id,
    )
    if row:
        return {
            "claim_rid": row["claim_rid"],
            "entity_uri": row["entity_uri"],
            "statement": row["statement"],
            "evidence_anchor": row["evidence_anchor"],
            "confidence": row["confidence"],
            "project_uri": row["project_uri"],
        }
    return None


async def create_source_claim(
    client: httpx.AsyncClient,
    conn: asyncpg.Connection,
    *,
    claimant_uri: str,
    statement: str,
    about_uri: str,
    source_document: str,
    c_id: str,
    confidence: str,
    anchor: str,
    project_uri: str,
    projection_batch: str,
) -> dict:
    """Create a source claim via POST /claims/, with versioning."""
    # Check for previous version of this claim
    previous = await find_previous_source_claim(conn, source_document, c_id)
    supersedes_rid = None
    if previous:
        business_key_unchanged = (
            previous["statement"] == statement
            and previous["evidence_anchor"] == anchor
            and previous["confidence"] == confidence
            and previous["project_uri"] == project_uri
        )
        if business_key_unchanged:
            # True no-op: server-side claim_rid would be identical, but skip the
            # round-trip so the projection summary reflects zero new claims.
            log.info(f"  Source claim {c_id}: unchanged, skipping (dedup early-return)")
            # Refresh projection_batch so the audit trail records this run.
            await conn.execute(
                "UPDATE claims SET metadata = metadata || $1::jsonb WHERE claim_rid = $2",
                json.dumps({"projection_batch": projection_batch}),
                previous["claim_rid"],
            )
            return {
                "claim_rid": previous["claim_rid"],
                "entity_uri": previous["entity_uri"],
                "_dedup_skipped": True,
            }
        if previous["statement"] != statement:
            supersedes_rid = previous["claim_rid"]
            log.info(f"  {c_id} supersedes {supersedes_rid} (statement changed)")

    payload = {
        "claimant_uri": claimant_uri,
        "statement": statement,
        "claim_type": "governance",
        "about_uri": about_uri,
        "source_document": source_document,
        "metadata": {
            "c_id": c_id,
            "confidence": confidence,
            "evidence_anchor": anchor,
            "claim_layer": "source",
            "extraction_status": "extracted",
            "project_uri": project_uri,
            "source": "learning_field",
        },
        "created_by": "darren",
    }
    if supersedes_rid:
        payload["supersedes_rid"] = supersedes_rid

    resp = await client.post(f"{KOI_BASE}/claims/", json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        log.error(f"  Failed to create source claim {c_id}: {resp.status_code} {resp.text}")
        return {}
    data = resp.json()
    claim_rid = data.get("claim_rid", "?")
    log.info(f"  Source claim {c_id}: {claim_rid}")

    # Add projection_batch to metadata (post-creation, doesn't affect RID)
    await conn.execute(
        "UPDATE claims SET metadata = metadata || $1::jsonb WHERE claim_rid = $2",
        json.dumps({"projection_batch": projection_batch}),
        claim_rid,
    )
    return data


async def create_review_claim(
    client: httpx.AsyncClient,
    conn: asyncpg.Connection,
    *,
    claimant_uri: str,
    concept_name: str,
    about_uri: str,
    target_spec_doc: str,
    disposition_slug: str,
    project_uri: str,
    projection_batch: str,
    disposition: Optional[str] = None,
) -> dict:
    """Create a review claim via POST /claims/. Deterministic from target+concept.

    Note: review claims intentionally leave the top-level ``source_document``
    column empty. They are governance-layer recommendations keyed by
    (target_spec_doc, concept, disposition) so that multiple bridge notes
    converge on the same claim RID rather than each spawning a duplicate.
    Provenance back to the authoring bridge note is preserved via the
    supporting source (C-) claims, which do carry source_document and are
    linked to the review claim by ``supports`` edges in entity_relationships.

    Consumers filtering by ``source_document = '<bridge-note-doc-id>'`` will
    therefore only see source claims; to enumerate the review claims induced
    by a bridge note, traverse ``supports`` edges from its source claims
    (or filter on ``metadata->>'projection_batch'`` for batch-level audits).
    """
    target_spec_uri = f"spec:{target_spec_doc}"
    if concept_name is None:
        # Target-level cluster (blank concept): keyed by target:disposition, no concept node.
        change_slug = disposition_slug
        governance_cluster_key = f"{target_spec_doc}:{disposition_slug}"
        statement = (
            f"Canon review: {disposition_slug.replace('-', ' ')} — "
            f"target-level (no concept) in {target_spec_doc}"
        )
    else:
        change_slug = f"{disposition_slug}-{concept_name}"
        # New-dialect clusters are disposition-keyed so an OLD-dialect (target:concept)
        # find_review_claim lookup can never collide with them. Old-dialect path unchanged.
        if disposition is not None:
            governance_cluster_key = f"{target_spec_doc}:{concept_name}:{disposition_slug}"
        else:
            governance_cluster_key = f"{target_spec_doc}:{concept_name}"
        statement = (
            f"Canon review: {disposition_slug.replace('-', ' ')} — "
            f"{concept_name.replace('-', ' ')} in {target_spec_doc}"
        )

    metadata = {
        "claim_layer": "review",
        "target_spec_doc": target_spec_doc,
        "target_section": concept_name,
        "change_slug": change_slug,
        "target_spec_uri": target_spec_uri,
        "governance_cluster_key": governance_cluster_key,
        "project_uri": project_uri,
        "source": "learning_field",
    }
    # Emit disposition only for new-dialect clusters; old-dialect metadata stays byte-identical.
    if disposition is not None:
        metadata["disposition"] = disposition

    payload = {
        "claimant_uri": claimant_uri,
        "statement": statement,
        "claim_type": "governance",
        "about_uri": about_uri,
        "metadata": metadata,
        "created_by": "darren",
    }

    resp = await client.post(f"{KOI_BASE}/claims/", json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        log.error(f"  Failed to create review claim: {resp.status_code} {resp.text}")
        return {}
    data = resp.json()
    claim_rid = data.get("claim_rid", "?")
    log.info(f"  Review claim ({target_spec_doc} × {concept_name}): {claim_rid}")

    # Add projection_batch to metadata (post-creation, doesn't affect RID)
    await conn.execute(
        "UPDATE claims SET metadata = metadata || $1::jsonb WHERE claim_rid = $2",
        json.dumps({"projection_batch": projection_batch}),
        claim_rid,
    )
    return data


async def find_review_claim(
    conn: asyncpg.Connection,
    target_spec_doc: str,
    concept_name: str,
    change_slug: Optional[str] = None,
) -> Optional[str]:
    """Find an existing review claim.

    Old dialect: dedup by (target_spec_doc, governance_cluster_key=target:concept).
    New dialect (change_slug given): dedup by (target_spec_doc, change_slug=
    disposition-concept) so a framing-note-only cluster never shadows a
    canon-pressure-decision-brief cluster for the same (target, concept).
    """
    if change_slug is not None:
        row = await conn.fetchrow(
            "SELECT entity_uri FROM claims "
            "WHERE metadata->>'claim_layer' = 'review' "
            "  AND metadata->>'source' = 'learning_field' "
            "  AND metadata->>'target_spec_doc' = $1 "
            "  AND metadata->>'change_slug' = $2 "
            "LIMIT 1",
            target_spec_doc,
            change_slug,
        )
    else:
        row = await conn.fetchrow(
            "SELECT entity_uri FROM claims "
            "WHERE metadata->>'claim_layer' = 'review' "
            "  AND metadata->>'source' = 'learning_field' "
            "  AND metadata->>'target_spec_doc' = $1 "
            "  AND metadata->>'governance_cluster_key' = $2 "
            "LIMIT 1",
            target_spec_doc,
            f"{target_spec_doc}:{concept_name}",
        )
    return row["entity_uri"] if row else None


async def insert_edge(
    conn: asyncpg.Connection,
    subject_uri: str,
    predicate: str,
    object_uri: str,
    source: str = "learning_field",
    confidence: float = 1.0,
    source_rid: Optional[str] = None,
) -> bool:
    """Insert an edge into entity_relationships. Returns True if inserted."""
    try:
        result = await conn.execute(
            "INSERT INTO entity_relationships "
            "(subject_uri, predicate, object_uri, confidence, source, source_rid) "
            "VALUES ($1, $2, $3, $4, $5, $6) "
            "ON CONFLICT DO NOTHING",
            subject_uri, predicate, object_uri, confidence, source, source_rid,
        )
        return result == "INSERT 0 1"
    except Exception as e:
        log.error(f"  Edge insert failed ({subject_uri} -{predicate}-> {object_uri}): {e}")
        return False


async def create_question_entity(
    conn: asyncpg.Connection,
    question: OpenQuestion,
    doc_id: str,
    source_rid: str,
) -> str:
    """Create a Question entity in entity_registry."""
    # Stable URI from doc_id + question number
    uri = generate_entity_uri(f"{doc_id}-Q{question.number}", "Question")
    entity_text = question.question[:200]

    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, metadata) "
        "VALUES ($1, $2, 'Question', $3, $4::jsonb) "
        "ON CONFLICT (fuseki_uri) DO NOTHING",
        uri, entity_text, normalize_text(entity_text),
        json.dumps({
            "source": "learning_field",
            "source_document": doc_id,
            "question_number": question.number,
        }),
    )
    return uri


# ---------------------------------------------------------------------------
# Projection logic
# ---------------------------------------------------------------------------

async def project_bridge_note(
    note: BridgeNote,
    conn: asyncpg.Connection,
    client: httpx.AsyncClient,
    dry_run: bool = False,
    projection_batch: str = "",
) -> dict:
    """Project a single bridge note into the KOI graph."""
    stats = {
        "source_claims": 0,
        "source_claims_skipped": 0,
        "review_claims": 0,
        "concepts": 0,
        "questions": 0,
        "supports_edges": 0,
        "opposes_edges": 0,
        "about_edges": 0,
        "related_to_edges": 0,
    }

    project_cfg = PROJECTS[note.project_key]
    claimant_uri = project_cfg["claimant_uri"]

    # Disposition determines stance: "no change" → opposes, everything else → supports
    proposes_change = note.disposition != "no change"
    default_stance = "supports" if proposes_change else "opposes"

    has_review_directives = len(note.review_directives) > 0

    log.info(f"\n{'='*60}")
    log.info(f"Projecting: {note.doc_id} ({len(note.claims)} claims, {len(note.questions)} questions)")
    log.info(f"  Disposition: {note.disposition} → stance: {default_stance}")
    log.info(f"  Concepts: {note.concepts}")
    if has_review_directives:
        log.info(f"  Review directives: {len(note.review_directives)} (authoritative)")
    else:
        log.info(f"  Review directives: none (using depends_on fallback)")

    if dry_run:
        log.info("  [DRY RUN] Would create entities and edges")
        stats["source_claims"] = len(note.claims)
        stats["review_claims"] = len(note.review_directives) if has_review_directives else 0
        stats["questions"] = len(note.questions)
        stats["concepts"] = len(note.concepts)
        return stats

    # 1. Resolve project URI (fail-closed)
    project_uri = await resolve_project_uri(conn, project_cfg["project_id"])
    log.info(f"  Project URI: {project_uri}")

    # 2. Resolve/create all concept entities (frontmatter + R-claim concepts)
    concept_uris = {}
    all_concept_names = set(note.concepts)
    for rd in note.review_directives:
        if rd.concept is not None:        # (ii) blank-concept rows are target-level — no concept node
            all_concept_names.add(rd.concept)
    for concept_name in all_concept_names:
        uri = await resolve_or_create_concept(conn, concept_name)
        concept_uris[concept_name] = uri
        stats["concepts"] += 1

    # 3. Determine disposition slug
    disp_slug = DISPOSITION_SLUG.get(note.disposition, "unclassified")

    # 4. Determine canon targets (depends_on only as a fallback)
    # Only used when note has no explicit review directives.
    fallback_targets = [
        r for r in note.depends_on
        if not r.startswith(note.doc_id)
        and ".connection." not in r
        and ".term." not in r
    ]
    seen = set()
    fallback_targets = [x for x in fallback_targets if not (x in seen or seen.add(x))]

    # 5a. If note has review directives: create review claims from
    # explicit R-claims only. Each R-claim has a deterministic (target, concept).
    # Global dedup: always check for existing review claim first.
    review_claim_cache = {}  # (target, concept) → entity_uri
    source_claim_uris = {}   # c_id → entity_uri (for supported_by linkage)

    if has_review_directives:
        for rd in note.review_directives:
            # Per-directive disposition (new-dialect table rows) or note-level (old dialect).
            if rd.disposition is not None:
                rd_slug = DISPOSITION_SLUG.get(rd.disposition, "unclassified")
                rd_proposes = rd.disposition in PROPOSE_DISPOSITIONS
                # (ii) blank-concept rows dedup by (target, disposition); concept'd by (target, disposition, concept)
                rd_change_slug = rd_slug if rd.concept is None else f"{rd_slug}-{rd.concept}"
            else:
                rd_slug = disp_slug
                rd_proposes = proposes_change
                rd_change_slug = None
            cache_key = (rd.target_doc, rd.concept, rd.disposition)
            if cache_key not in review_claim_cache:
                if rd.concept is None:
                    # (ii) target-level cluster, no concept node. about_uri must be a valid
                    # 'about' type (concept/pattern/project/...); a spec:<target> SpecDoc is
                    # rejected by /claims/, so anchor on the project. Target traversability is
                    # preserved via metadata.target_spec_doc + the supports/opposes edges.
                    about_uri = project_uri
                else:
                    about_uri = concept_uris.get(rd.concept)
                    if not about_uri:
                        log.warning(f"  Skipping {rd.r_id}: concept '{rd.concept}' not resolved")
                        continue
                # Always check for an existing cluster first (global dedup). For
                # new-dialect rows dedup by change_slug (disposition+concept) so a
                # framing-note-only cluster never shadows a canon-pressure one.
                existing = await find_review_claim(
                    conn, rd.target_doc, rd.concept, change_slug=rd_change_slug)
                if existing:
                    review_claim_cache[cache_key] = existing
                    log.info(f"  {rd.r_id}: reusing existing review claim "
                             f"({rd.target_doc} × {rd.concept} / {rd.disposition or note.disposition})")
                elif (rd.disposition is not None) or rd_proposes:
                    # Every new-dialect R-row gets a governance cluster
                    # (incl. framing-note-only / decline-with-trigger). Old dialect
                    # keeps the proposes_change gate.
                    review_data = await create_review_claim(
                        client, conn,
                        claimant_uri=claimant_uri,
                        concept_name=rd.concept,
                        about_uri=about_uri,
                        target_spec_doc=rd.target_doc,
                        disposition_slug=rd_slug,
                        project_uri=project_uri,
                        projection_batch=projection_batch,
                        disposition=rd.disposition,
                    )
                    if review_data and review_data.get("entity_uri"):
                        review_claim_cache[cache_key] = review_data["entity_uri"]
                        stats["review_claims"] += 1

    # 5b. Create source claims + link to review claims
    for claim in note.claims:
        # Determine the primary concept for this claim
        primary_concept = note.concepts[0] if note.concepts else None
        stmt_normalized = claim.statement.lower().replace("-", " ")
        for c in note.concepts:
            c_readable = c.replace("-", " ")
            if c_readable.lower() in stmt_normalized:
                primary_concept = c
                break

        about_uri = concept_uris.get(primary_concept) if primary_concept else None
        if not about_uri:
            log.warning(f"  Skipping claim {claim.c_id}: no concept to link")
            continue

        source_data = await create_source_claim(
            client, conn,
            claimant_uri=claimant_uri,
            statement=claim.statement,
            about_uri=about_uri,
            source_document=note.doc_id,
            c_id=claim.c_id,
            confidence=claim.confidence,
            anchor=claim.anchor,
            project_uri=project_uri,
            projection_batch=projection_batch,
        )

        if not source_data or not source_data.get("entity_uri"):
            continue

        if source_data.get("_dedup_skipped"):
            stats["source_claims_skipped"] += 1
        else:
            stats["source_claims"] += 1
        source_entity_uri = source_data["entity_uri"]
        source_claim_uris[claim.c_id] = source_entity_uri

        if has_review_directives:
            # Link source claims to review claims. New dialect: each R-row names its
            # supporting C-IDs in the one-line cell (supported_by); fall back to
            # concept-match only when the row named none. Old dialect: explicit
            # supported_by only (unchanged). Stance is per-directive.
            for rd in note.review_directives:
                rd_stance = "supports" if (
                    (rd.disposition in PROPOSE_DISPOSITIONS)
                    if rd.disposition is not None else proposes_change
                ) else "opposes"
                linked = claim.c_id in rd.supported_by
                if (not linked and rd.disposition is not None
                        and not rd.supported_by and primary_concept == rd.concept):
                    linked = True
                if not linked:
                    continue
                review_uri = review_claim_cache.get((rd.target_doc, rd.concept, rd.disposition))
                if review_uri:
                    inserted = await insert_edge(
                        conn, source_entity_uri, rd_stance, review_uri,
                        source_rid=f"projection:{note.doc_id}",
                    )
                    if inserted:
                        if rd_stance == "supports":
                            stats["supports_edges"] += 1
                        else:
                            stats["opposes_edges"] += 1
        else:
            # Fallback: use depends_on targets only
            if not fallback_targets or not primary_concept:
                continue

            for target in fallback_targets:
                cache_key = (target, primary_concept)

                if cache_key not in review_claim_cache:
                    # Always check for existing review claim first
                    existing = await find_review_claim(conn, target, primary_concept)
                    if existing:
                        review_claim_cache[cache_key] = existing
                    elif proposes_change:
                        review_data = await create_review_claim(
                            client, conn,
                            claimant_uri=claimant_uri,
                            concept_name=primary_concept,
                            about_uri=about_uri,
                            target_spec_doc=target,
                            disposition_slug=disp_slug,
                            project_uri=project_uri,
                            projection_batch=projection_batch,
                        )
                        if review_data and review_data.get("entity_uri"):
                            review_claim_cache[cache_key] = review_data["entity_uri"]
                            stats["review_claims"] += 1

                review_entity_uri = review_claim_cache.get(cache_key)
                if review_entity_uri:
                    inserted = await insert_edge(
                        conn, source_entity_uri, default_stance, review_entity_uri,
                        source_rid=f"projection:{note.doc_id}",
                    )
                    if inserted:
                        if default_stance == "supports":
                            stats["supports_edges"] += 1
                        else:
                            stats["opposes_edges"] += 1

    # 7. Create Question entities + about edges
    for q in note.questions:
        q_uri = await create_question_entity(conn, q, note.doc_id,
                                              source_rid=f"projection:{note.doc_id}")
        stats["questions"] += 1

        # Link question to the most relevant concept
        # Normalize both sides for matching
        linked_concept = note.concepts[0] if note.concepts else None
        q_normalized = q.question.lower().replace("-", " ")
        for c in note.concepts:
            c_readable = c.replace("-", " ")
            if c_readable.lower() in q_normalized:
                linked_concept = c
                break

        if linked_concept and linked_concept in concept_uris:
            inserted = await insert_edge(
                conn, q_uri, "about", concept_uris[linked_concept],
                source_rid=f"projection:{note.doc_id}",
            )
            if inserted:
                stats["about_edges"] += 1

    # 8. Create related_to edges between bridge note SpecDocs (lateral links)
    # The bridge note itself should be a SpecDoc; link to other bridge notes in relates_to
    note_spec_uri = f"spec:{note.doc_id}"
    # Check if this SpecDoc exists
    note_spec_exists = await conn.fetchval(
        "SELECT 1 FROM entity_registry WHERE fuseki_uri = $1", note_spec_uri,
    )

    if note_spec_exists:
        for related_doc_id in note.relates_to:
            related_uri = f"spec:{related_doc_id}"
            related_exists = await conn.fetchval(
                "SELECT 1 FROM entity_registry WHERE fuseki_uri = $1", related_uri,
            )
            if related_exists:
                inserted = await insert_edge(
                    conn, note_spec_uri, "related_to", related_uri,
                    source_rid=f"projection:{note.doc_id}",
                )
                if inserted:
                    stats["related_to_edges"] += 1

    return stats


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_bridge_notes() -> list[tuple[Path, str]]:
    """Find all bridge notes across configured project repos."""
    notes = []

    for project_key, cfg in PROJECTS.items():
        bridge_dir = cfg["bridge_dir"]
        if not bridge_dir.exists():
            log.warning(f"Bridge dir not found: {bridge_dir}")
            continue

        for md_path in sorted(bridge_dir.rglob("*.md")):
            if md_path.name == "CLAUDE.md":
                continue
            # Parse frontmatter rather than substring-match the body —
            # schema docs in YAML code fences can otherwise trigger false positives.
            try:
                text = md_path.read_text()
            except Exception:
                continue
            try:
                fm = parse_frontmatter(text)
            except ValueError:
                continue  # no frontmatter — not a bridge note
            except Exception:
                continue
            # Tolerant predicate: old-dialect bridge_note OR new-dialect
            # connection note. Synthesis docs (zero claims) are caught by the
            # per-note zero-claim warning, not excluded here.
            if isinstance(fm, dict) and (
                fm.get("research_subkind") == "bridge_note"
                or fm.get("doc_kind") == "connection"
            ):
                notes.append((md_path, project_key))

    return notes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Project bridge notes into KOI graph")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--apply", action="store_true", help="Write to KOI graph")
    parser.add_argument("--note", type=str, help="Project a single note by path")
    parser.add_argument("--match", type=str, default=None,
                        help="Only project notes whose filename contains this substring (e.g. 'sahely-')")
    parser.add_argument("--project", type=str, default=None,
                        help="Only project notes from this PROJECTS key (e.g. 'spore')")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not args.apply:
        parser.error("Specify --dry-run or --apply")

    if args.verbose:
        log.setLevel(logging.DEBUG)

    # Discover notes
    if args.note:
        note_path = Path(args.note).expanduser().resolve()
        # Determine project key by matching the note path against each configured
        # bridge_dir (config-driven; no hardcoded per-project paths).
        project_key = None
        for key, cfg in PROJECTS.items():
            bridge_dir = cfg["bridge_dir"].expanduser().resolve()
            if note_path == bridge_dir or bridge_dir in note_path.parents:
                project_key = key
                break
        if project_key is None:
            parser.error(
                f"could not match note path to any configured project bridge_dir: {note_path}\n"
                "Register the project in config/projects.json (or $KOI_PROJECTS_CONFIG)."
            )
        note_paths = [(note_path, project_key)]
    else:
        note_paths = discover_bridge_notes()
        # Scoping: --project <key> and/or --match <substr>.
        if args.project:
            note_paths = [(p, k) for (p, k) in note_paths if k == args.project]
        if args.match:
            note_paths = [(p, k) for (p, k) in note_paths if args.match in p.name]

    log.info(f"Found {len(note_paths)} bridge notes")

    if not note_paths:
        log.error("No bridge notes found")
        sys.exit(1)

    # Connect to KOI
    conn = await asyncpg.connect("postgresql://localhost:5432/personal_koi")

    active_project_keys = {project_key for _, project_key in note_paths}

    # Verify claimant orgs + project URIs resolve for participating projects.
    # Unregistered-project guard: a project that is configured but not yet
    # ingested into the KB is SKIPPED with a warning rather than
    # halting the whole run; its notes are dropped from this run.
    skip_projects: set = set()
    for project_key in sorted(active_project_keys):
        cfg = PROJECTS[project_key]
        exists = await conn.fetchval(
            "SELECT 1 FROM entity_registry WHERE fuseki_uri = $1",
            cfg["claimant_uri"],
        )
        if not exists:
            log.warning(f"Skipping project '{project_key}': claimant entity "
                        f"missing ({cfg['claimant_uri']})")
            skip_projects.add(project_key)
            continue
        try:
            uri = await resolve_project_uri(conn, cfg["project_id"])
            log.info(f"Project {cfg['project_id']} → {uri}")
        except RuntimeError as e:
            log.warning(f"Skipping project '{project_key}': {e}")
            skip_projects.add(project_key)

    if skip_projects:
        note_paths = [(p, k) for (p, k) in note_paths if k not in skip_projects]
        if not note_paths:
            log.error("No projectable notes remain after unregistered-project skip")
            await conn.close()
            sys.exit(1)

    totals = {
        "notes": 0,
        "source_claims": 0,
        "source_claims_skipped": 0,
        "review_claims": 0,
        "concepts": 0,
        "questions": 0,
        "supports_edges": 0,
        "opposes_edges": 0,
        "about_edges": 0,
        "related_to_edges": 0,
    }

    # Parse all notes first, then process in two passes:
    # Pass 1: change-proposing notes (creates review claims + supports edges)
    # Pass 2: "no change" notes (links to existing review claims with opposes)
    parsed_notes = []
    zero_claim_notes = []
    for note_path, project_key in note_paths:
        try:
            n = parse_bridge_note(note_path, project_key)
        except Exception as e:
            log.error(f"Failed to parse {note_path}: {e}")
            continue
        parsed_notes.append(n)
        # Silent-data-loss guard: a discovered note that yields no claims AND no
        # review directives is logged so partial coverage is never mistaken for
        # complete (synthesis docs / capstones land here correctly).
        if not n.claims and not n.review_directives:
            zero_claim_notes.append(n.doc_id or str(note_path))
            log.warning(f"  ZERO-CLAIM: {n.doc_id or note_path} discovered but "
                        f"yielded 0 claims + 0 review directives")

    change_notes = [n for n in parsed_notes if n.disposition != "no change"]
    nochange_notes = [n for n in parsed_notes if n.disposition == "no change"]
    log.info(f"  Pass 1: {len(change_notes)} change-proposing notes")
    log.info(f"  Pass 2: {len(nochange_notes)} no-change notes (opposes)")

    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log.info(f"  Projection batch: {batch_ts}")

    # Write path on POST /claims/ requires auth (make_service_token_auth).
    # Send the service token from the env (sourced from config/personal.env);
    # never hardcode it. Falls back to no header if unset (read-only/dry-run).
    _svc_token = os.getenv("KOI_CLAIMS_SERVICE_TOKEN", "")
    _auth_headers = {"Authorization": f"Bearer {_svc_token}"} if _svc_token else {}
    async with httpx.AsyncClient(headers=_auth_headers) as client:
        for note in change_notes + nochange_notes:
            stats = await project_bridge_note(
                note, conn, client,
                dry_run=args.dry_run,
                projection_batch=batch_ts,
            )
            totals["notes"] += 1
            for k, v in stats.items():
                totals[k] = totals.get(k, 0) + v

    await conn.close()

    log.info(f"\n{'='*60}")
    log.info("PROJECTION SUMMARY")
    log.info(f"  Notes processed: {totals['notes']}")
    log.info(f"  Source claims:   {totals['source_claims']} new"
             f"{' (+' + str(totals.get('source_claims_skipped', 0)) + ' unchanged, dedup-skipped)' if totals.get('source_claims_skipped', 0) else ''}")
    log.info(f"  Review claims:   {totals['review_claims']}")
    log.info(f"  Concepts:        {totals['concepts']}")
    log.info(f"  Questions:       {totals['questions']}")
    log.info(f"  Supports edges:  {totals['supports_edges']}")
    log.info(f"  Opposes edges:   {totals.get('opposes_edges', 0)}")
    log.info(f"  About edges:     {totals['about_edges']}")
    log.info(f"  Related_to edges:{totals['related_to_edges']}")

    # New-dialect convergence visibility: R-row count, disposition histogram,
    # zero-claim notes (plus the silent-data-loss guard).
    disp_hist: dict = {}
    rrow_total = 0
    for n in parsed_notes:
        for rd in n.review_directives:
            rrow_total += 1
            key = rd.disposition or "(old-dialect/no-disposition)"
            disp_hist[key] = disp_hist.get(key, 0) + 1
    log.info(f"  R-rows / review directives parsed: {rrow_total}")
    log.info(f"  Disposition histogram: {dict(sorted(disp_hist.items(), key=lambda kv: -kv[1]))}")
    log.info(f"  Zero-claim discovered notes: {len(zero_claim_notes)}"
             + (f" → {zero_claim_notes}" if zero_claim_notes and len(zero_claim_notes) <= 25 else ""))


if __name__ == "__main__":
    asyncio.run(main())
