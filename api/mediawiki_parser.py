"""
Pure-Python MediaWiki wikitext parser for the Salish Sea Wiki import pipeline.

Parses MediaWiki XML dumps and individual pages into structured representations
suitable for BKC entity resolution and knowledge graph ingest.

Dependencies: mwparserfromhell (only external dependency)
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from typing import Dict, Iterator, List, Optional, Set, Tuple

import mwparserfromhell

PARSER_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class WikiLink:
    target: str
    display_text: str
    section: str
    is_category: bool


@dataclass
class WikiSection:
    id: str
    title: str
    level: int
    text: str


@dataclass
class StructuralEdge:
    target_title: str
    predicate: str
    target_type_hint: str
    field_name: str
    confidence: float
    source_section: str


@dataclass
class EditorialEdge:
    target_title: str
    source_section: str
    confidence: float


@dataclass
class WikiPageParse:
    title: str
    normalized_title: str
    source_rid: str
    page_id: int
    revision_id: int
    namespace: int
    template_type: Optional[str]
    bkc_entity_type: Optional[str]
    page_class: str
    is_redirect: bool
    redirect_target: Optional[str]
    aliases: List[str]
    template_fields: Dict[str, List[str]]
    wikilinks: List[WikiLink]
    sections: List[WikiSection]
    categories: List[str]
    plain_text: str
    section_texts: Dict[str, str]
    word_count: int
    content_hash: str
    entity_density_score: float
    ingest_confidence: float
    promotion_priority: float
    structural_edges: List[StructuralEdge]
    editorial_edges: List[EditorialEdge]
    parse_warnings: List[str]
    parse_version: str


# ---------------------------------------------------------------------------
# Template -> BKC type mapping
# ---------------------------------------------------------------------------

TEMPLATE_BKC_MAP: Dict[str, Optional[str]] = {
    "Topic": "Concept",
    "Effort": "Project",
    "Workgroup": "Organization",
    "Place": "Location",
    "Product": None,
}

# ---------------------------------------------------------------------------
# Three-tier predicate mapping
# ---------------------------------------------------------------------------

# Tier 1 (confidence 0.95): Strong deterministic
STRONG_FIELD_MAP: Dict[str, Tuple[str, str]] = {
    "Places": ("located_in", "Location"),
}

# Tier 2 (confidence 0.85): Typed candidate
CANDIDATE_FIELD_MAP: Dict[str, Tuple[str, str]] = {
    "Jurisdictions": ("located_in", "Location"),
    "Workgroups": ("involves_organization", "Organization"),
    "AnthroTopics": ("broader", "Concept"),
    "EcoTopics": ("broader", "Concept"),
}

# Tier 3 (confidence 0.7): Generic relation
GENERIC_FIELD_MAP: Dict[str, Tuple[str, Optional[str]]] = {
    "RelatedEfforts": ("related_to", "Project"),
    "RelatedTopics": ("related_to", "Concept"),
    "Products": ("related_to", None),
}

# Namespace prefixes to strip from wikilink targets
_SKIP_NS_PREFIXES = frozenset({
    "file", "image", "media", "special", "template",
    "help", "user", "user talk", "talk", "wikipedia",
    "mediawiki", "module",
})

_REDIRECT_RE = re.compile(
    r"^\s*#REDIRECT\s*\[\[(.+?)\]\]", re.IGNORECASE | re.MULTILINE
)

_SLUG_UNSAFE_RE = re.compile(r"[^a-z0-9\s-]")
_SLUG_SPACE_RE = re.compile(r"[\s]+")


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize_title(title: str) -> str:
    """Normalize a wiki page title for matching.

    - Trim whitespace, underscore -> space, collapse spaces
    - NFC unicode normalization, lowercase
    - Strip namespace prefix (e.g. Category:X -> X)
    - Preserve parentheticals
    """
    t = title.strip()
    t = t.replace("_", " ")
    t = re.sub(r"\s+", " ", t)
    t = unicodedata.normalize("NFC", t)
    t = t.lower()
    # Strip namespace prefix
    if ":" in t:
        prefix = t.split(":", 1)[0]
        if prefix in _SKIP_NS_PREFIXES or prefix == "category":
            t = t.split(":", 1)[1].strip()
    return t


def derive_aliases(title: str, normalized: str) -> List[str]:
    """Derive candidate aliases from a title.

    If the title contains a parenthetical, produce the version without it.
    """
    aliases: List[str] = []
    m = re.match(r"^(.+?)\s*\(.*\)\s*$", normalized)
    if m:
        base = m.group(1).strip()
        if base and base != normalized:
            aliases.append(base)
    return aliases


def normalize_section_id(heading_text: str) -> str:
    """Slugify a section heading into an anchor-safe id.

    Lowercase, strip markup, replace spaces with hyphens, ASCII-safe.
    """
    text = heading_text.strip()
    # Strip any remaining wiki markup (bold, italic, links)
    text = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", text)
    text = re.sub(r"'{2,}", "", text)
    text = text.lower()
    text = unicodedata.normalize("NFC", text)
    text = _SLUG_UNSAFE_RE.sub("", text)
    text = _SLUG_SPACE_RE.sub("-", text.strip())
    text = text.strip("-")
    return text or "section"


# ---------------------------------------------------------------------------
# Template detection and field extraction
# ---------------------------------------------------------------------------

def detect_template_type(wikitext: str) -> Optional[str]:
    """Detect the first template matching TEMPLATE_BKC_MAP keys."""
    try:
        parsed = mwparserfromhell.parse(wikitext)
    except Exception:
        return None
    template_names_lower = {k.lower(): k for k in TEMPLATE_BKC_MAP}
    for tpl in parsed.filter_templates(recursive=True):
        name = str(tpl.name).strip()
        name_lower = name.lower()
        if name_lower in template_names_lower:
            return template_names_lower[name_lower]
    return None


def extract_template_fields(
    parsed: mwparserfromhell.wikicode.Wikicode,
    template_name: str,
) -> Dict[str, List[str]]:
    """Extract named parameters from the matching template.

    Values are lists because some fields have pipe-separated or
    comma-separated values.
    """
    fields: Dict[str, List[str]] = {}
    template_name_lower = template_name.lower()
    for tpl in parsed.filter_templates(recursive=True):
        if str(tpl.name).strip().lower() == template_name_lower:
            for param in tpl.params:
                pname = str(param.name).strip()
                if not pname:
                    continue
                raw_value = str(param.value).strip()
                if not raw_value:
                    continue
                # Split on pipe or comma for multi-value fields
                values = re.split(r"[|,]", raw_value)
                values = [_strip_wikilinks(v.strip()) for v in values if v.strip()]
                if values:
                    fields[pname] = values
            break
    return fields


def _strip_wikilinks(text: str) -> str:
    """Remove wikilink brackets, keeping display text."""
    text = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]", r"\1", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Wikilink and section extraction
# ---------------------------------------------------------------------------

def extract_sections(wikitext: str) -> List[WikiSection]:
    """Parse heading hierarchy into WikiSection list.

    The intro text (before any heading) gets id 'lead'.
    Duplicate headings get ordinals appended (e.g. 'history', 'history-2').
    """
    try:
        parsed = mwparserfromhell.parse(wikitext)
    except Exception:
        return [WikiSection(
            id="lead", title="", level=0,
            text=wikitext.strip(),
        )]

    headings = list(parsed.filter_headings())
    sections: List[WikiSection] = []
    seen_ids: Dict[str, int] = {}

    # Helper to deduplicate section ids
    def _dedup_id(sid: str) -> str:
        if sid not in seen_ids:
            seen_ids[sid] = 1
            return sid
        seen_ids[sid] += 1
        return f"{sid}-{seen_ids[sid]}"

    if not headings:
        plain = parsed.strip_code().strip()
        return [WikiSection(id="lead", title="", level=0, text=plain)]

    # Lead section: text before first heading
    first_heading_idx = wikitext.find(str(headings[0]))
    if first_heading_idx > 0:
        lead_text = wikitext[:first_heading_idx]
        try:
            lead_plain = mwparserfromhell.parse(lead_text).strip_code().strip()
        except Exception:
            lead_plain = lead_text.strip()
        if lead_plain:
            sections.append(WikiSection(id="lead", title="", level=0, text=lead_plain))
            seen_ids["lead"] = 1

    # Process each heading
    for i, heading in enumerate(headings):
        title_text = str(heading.title).strip()
        level = heading.level

        # Find section content between this heading and the next
        heading_str = str(heading)
        start = wikitext.find(heading_str)
        if start == -1:
            continue
        content_start = start + len(heading_str)

        if i + 1 < len(headings):
            next_heading_str = str(headings[i + 1])
            end = wikitext.find(next_heading_str, content_start)
            if end == -1:
                end = len(wikitext)
        else:
            end = len(wikitext)

        section_wikitext = wikitext[content_start:end]
        try:
            section_plain = mwparserfromhell.parse(section_wikitext).strip_code().strip()
        except Exception:
            section_plain = section_wikitext.strip()

        sid = normalize_section_id(title_text)
        sid = _dedup_id(sid)

        sections.append(WikiSection(
            id=sid,
            title=title_text,
            level=level,
            text=section_plain,
        ))

    return sections


def extract_wikilinks(
    parsed: mwparserfromhell.wikicode.Wikicode,
    sections: List[WikiSection],
) -> List[WikiLink]:
    """Extract all [[wikilinks]] from parsed wikitext.

    Maps each link to the section it appears in.
    Skips external links, file/image links, interwiki, and template namespace.
    """
    links: List[WikiLink] = []

    for wikilink in parsed.filter_wikilinks():
        target = str(wikilink.title).strip()
        if not target:
            continue

        # Skip file/image links and other namespace links
        if ":" in target:
            prefix = target.split(":", 1)[0].lower().strip()
            if prefix in _SKIP_NS_PREFIXES:
                continue

        is_category = False
        if ":" in target:
            prefix = target.split(":", 1)[0].lower().strip()
            if prefix == "category":
                is_category = True
                target = target.split(":", 1)[1].strip()

        display_text = str(wikilink.text).strip() if wikilink.text else target

        # Determine section: find where this link text appears in sections
        section_id = _find_section_for_text(str(wikilink), sections)

        links.append(WikiLink(
            target=target,
            display_text=display_text,
            section=section_id,
            is_category=is_category,
        ))

    return links


def _find_section_for_text(link_str: str, sections: List[WikiSection]) -> str:
    """Best-effort: find which section contains a link by checking plain-text overlap."""
    # Since we can't easily get character positions from mwparserfromhell,
    # we check which section's text contains the display text.
    # Default to 'lead' if no match.
    display = _strip_wikilinks(link_str)
    for section in reversed(sections):
        if display and display in section.text:
            return section.id
    return sections[0].id if sections else "lead"


# ---------------------------------------------------------------------------
# Plain text and markup stripping
# ---------------------------------------------------------------------------

def strip_markup(wikitext: str) -> str:
    """Remove all wiki markup, return plain text."""
    try:
        return mwparserfromhell.parse(wikitext).strip_code().strip()
    except Exception:
        return wikitext.strip()


# ---------------------------------------------------------------------------
# Page classification
# ---------------------------------------------------------------------------

def classify_page(
    template_type: Optional[str],
    is_redirect: bool,
    bkc_entity_type: Optional[str],
    word_count: int,
) -> str:
    """Classify a page as entity_bearing, source_only, or alias_only."""
    if is_redirect:
        return "alias_only"
    if template_type == "Product":
        return "source_only"
    if bkc_entity_type is not None:
        return "entity_bearing"
    if template_type is None and word_count < 50:
        return "source_only"
    return "source_only"


# ---------------------------------------------------------------------------
# Scoring functions
# ---------------------------------------------------------------------------

def compute_ingest_confidence(
    template_type: Optional[str],
    field_count: int,
    word_count: int,
    wikilink_count: int,
    warning_count: int,
) -> float:
    """Compute an ingest confidence score in [0, 1]."""
    score = 0.0
    if template_type is not None:
        score += 0.3
    if field_count >= 3:
        score += 0.2
    if word_count > 100:
        score += 0.15
    if word_count > 300:
        score += 0.05
    if wikilink_count >= 3:
        score += 0.1
    if warning_count == 0:
        score += 0.1
    else:
        score -= 0.1
    return max(0.0, min(1.0, score))


def compute_promotion_priority(
    template_type: Optional[str],
    word_count: int,
    wikilink_count: int,
    structural_edge_count: int,
) -> float:
    """Compute a promotion priority score in [0, 1]."""
    score = 0.0
    # Entity density
    if word_count > 0:
        score += min(wikilink_count / max(word_count, 1) * 100, 0.3)
    # Relationship richness
    score += structural_edge_count * 0.05
    # Word count contribution
    score += min(word_count / 1000, 0.2)
    # Template bonus
    if template_type is not None:
        score += 0.2
    return max(0.0, min(1.0, score))


def compute_entity_density(wikilink_count: int, word_count: int) -> float:
    """Entity density: wikilinks per 100 words."""
    if word_count == 0:
        return 0.0
    return wikilink_count / word_count * 100


# ---------------------------------------------------------------------------
# Edge builders
# ---------------------------------------------------------------------------

def build_structural_edges(
    template_fields: Dict[str, List[str]],
) -> List[StructuralEdge]:
    """Build structural edges from template fields using three-tier maps."""
    edges: List[StructuralEdge] = []

    for field_name, values in template_fields.items():
        predicate: Optional[str] = None
        target_type: Optional[str] = None
        confidence: float = 0.0

        if field_name in STRONG_FIELD_MAP:
            predicate, target_type = STRONG_FIELD_MAP[field_name]
            confidence = 0.95
        elif field_name in CANDIDATE_FIELD_MAP:
            predicate, target_type = CANDIDATE_FIELD_MAP[field_name]
            confidence = 0.85
        elif field_name in GENERIC_FIELD_MAP:
            pred, ttype = GENERIC_FIELD_MAP[field_name]
            predicate = pred
            target_type = ttype
            confidence = 0.7

        if predicate is None:
            continue

        for val in values:
            normalized = normalize_title(val)
            if not normalized:
                continue
            edges.append(StructuralEdge(
                target_title=normalized,
                predicate=predicate,
                target_type_hint=target_type or "",
                field_name=field_name,
                confidence=confidence,
                source_section="lead",
            ))

    return edges


def build_editorial_edges(
    wikilinks: List[WikiLink],
    structural_targets: Set[str],
) -> List[EditorialEdge]:
    """Build editorial edges from wikilinks not covered by structural edges.

    Skips category links and targets already covered by structural edges.
    """
    edges: List[EditorialEdge] = []
    seen: Set[str] = set()

    for link in wikilinks:
        if link.is_category:
            continue
        normalized = normalize_title(link.target)
        if not normalized:
            continue
        if normalized in structural_targets:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        edges.append(EditorialEdge(
            target_title=normalized,
            source_section=link.section,
            confidence=0.6,
        ))

    return edges


# ---------------------------------------------------------------------------
# Main parse entry point
# ---------------------------------------------------------------------------

def parse_page(
    title: str,
    wikitext: str,
    page_id: int,
    revision_id: int,
    wiki_domain: str,
    namespace: int = 0,
) -> WikiPageParse:
    """Parse a single MediaWiki page into a WikiPageParse.

    This is the main entry point for single-page parsing.
    """
    warnings: List[str] = []
    normalized = normalize_title(title)
    source_rid = f"mediawiki:{wiki_domain}:{page_id}"
    content_hash = hashlib.sha256(wikitext.encode("utf-8")).hexdigest()

    # Redirect detection
    redirect_match = _REDIRECT_RE.match(wikitext)
    is_redirect = redirect_match is not None
    redirect_target = redirect_match.group(1).strip() if redirect_match else None

    if is_redirect:
        aliases = derive_aliases(title, normalized)
        return WikiPageParse(
            title=title,
            normalized_title=normalized,
            source_rid=source_rid,
            page_id=page_id,
            revision_id=revision_id,
            namespace=namespace,
            template_type=None,
            bkc_entity_type=None,
            page_class="alias_only",
            is_redirect=True,
            redirect_target=redirect_target,
            aliases=aliases,
            template_fields={},
            wikilinks=[],
            sections=[],
            categories=[],
            plain_text="",
            section_texts={},
            word_count=0,
            content_hash=content_hash,
            entity_density_score=0.0,
            ingest_confidence=0.0,
            promotion_priority=0.0,
            structural_edges=[],
            editorial_edges=[],
            parse_warnings=warnings,
            parse_version=PARSER_VERSION,
        )

    # Parse wikitext
    try:
        parsed = mwparserfromhell.parse(wikitext)
    except Exception as e:
        warnings.append(f"mwparserfromhell parse error: {e}")
        return _empty_parse(
            title, normalized, source_rid, page_id, revision_id,
            namespace, content_hash, warnings,
        )

    # Template detection
    template_type = detect_template_type(wikitext)
    bkc_entity_type = TEMPLATE_BKC_MAP.get(template_type) if template_type else None

    # Template fields
    template_fields: Dict[str, List[str]] = {}
    if template_type:
        template_fields = extract_template_fields(parsed, template_type)

    # Sections
    sections = extract_sections(wikitext)

    # Wikilinks
    wikilinks = extract_wikilinks(parsed, sections)

    # Categories
    categories = [
        link.target for link in wikilinks if link.is_category
    ]
    # Filter categories out of the wikilinks list for edge building
    non_category_links = [link for link in wikilinks if not link.is_category]

    # Plain text
    plain_text = strip_markup(wikitext)
    word_count = len(plain_text.split()) if plain_text else 0

    # Section texts
    section_texts = {s.id: s.text for s in sections}

    # Aliases
    aliases = derive_aliases(title, normalized)

    # Page classification
    page_class = classify_page(template_type, False, bkc_entity_type, word_count)

    # Structural edges
    structural_edges = build_structural_edges(template_fields)
    structural_targets: Set[str] = {e.target_title for e in structural_edges}

    # Editorial edges
    editorial_edges = build_editorial_edges(non_category_links, structural_targets)

    # Scores
    entity_density = compute_entity_density(len(non_category_links), word_count)
    ingest_confidence = compute_ingest_confidence(
        template_type,
        len(template_fields),
        word_count,
        len(non_category_links),
        len(warnings),
    )
    promotion_priority = compute_promotion_priority(
        template_type,
        word_count,
        len(non_category_links),
        len(structural_edges),
    )

    return WikiPageParse(
        title=title,
        normalized_title=normalized,
        source_rid=source_rid,
        page_id=page_id,
        revision_id=revision_id,
        namespace=namespace,
        template_type=template_type,
        bkc_entity_type=bkc_entity_type,
        page_class=page_class,
        is_redirect=False,
        redirect_target=None,
        aliases=aliases,
        template_fields=template_fields,
        wikilinks=wikilinks,
        sections=sections,
        categories=categories,
        plain_text=plain_text,
        section_texts=section_texts,
        word_count=word_count,
        content_hash=content_hash,
        entity_density_score=entity_density,
        ingest_confidence=ingest_confidence,
        promotion_priority=promotion_priority,
        structural_edges=structural_edges,
        editorial_edges=editorial_edges,
        parse_warnings=warnings,
        parse_version=PARSER_VERSION,
    )


def _empty_parse(
    title: str,
    normalized: str,
    source_rid: str,
    page_id: int,
    revision_id: int,
    namespace: int,
    content_hash: str,
    warnings: List[str],
) -> WikiPageParse:
    """Return a minimal WikiPageParse when parsing fails."""
    return WikiPageParse(
        title=title,
        normalized_title=normalized,
        source_rid=source_rid,
        page_id=page_id,
        revision_id=revision_id,
        namespace=namespace,
        template_type=None,
        bkc_entity_type=None,
        page_class="source_only",
        is_redirect=False,
        redirect_target=None,
        aliases=[],
        template_fields={},
        wikilinks=[],
        sections=[],
        categories=[],
        plain_text="",
        section_texts={},
        word_count=0,
        content_hash=content_hash,
        entity_density_score=0.0,
        ingest_confidence=0.0,
        promotion_priority=0.0,
        structural_edges=[],
        editorial_edges=[],
        parse_warnings=warnings,
        parse_version=PARSER_VERSION,
    )


# ---------------------------------------------------------------------------
# XML dump streaming
# ---------------------------------------------------------------------------

_MW_NS = "http://www.mediawiki.org/xml/export-0.10/"


def parse_dump(
    xml_path: str,
    wiki_domain: str,
) -> Iterator[WikiPageParse]:
    """Stream a MediaWiki XML dump, yielding WikiPageParse for each NS-0 page.

    Uses iterparse for memory-efficient processing of large dumps.
    The MediaWiki XML namespace varies by version; we detect it from the root element.
    """
    mw_ns = _detect_xml_namespace(xml_path)
    ns_prefix = f"{{{mw_ns}}}" if mw_ns else ""

    context = ET.iterparse(xml_path, events=("end",))
    page_tag = f"{ns_prefix}page"

    for event, elem in context:
        if elem.tag != page_tag:
            continue

        try:
            result = _parse_page_element(elem, ns_prefix, wiki_domain)
        except Exception:
            elem.clear()
            continue

        elem.clear()

        if result is not None:
            yield result


def parse_json_export(
    json_path: str,
    wiki_domain: str,
) -> Iterator[WikiPageParse]:
    """Parse a JSON export of the form {title: {content, timestamp, pageid}, ...}.

    This format is produced by the Scrapling-based MediaWiki API exporter
    that fetches pages via api.php?action=query&prop=revisions.
    """
    import json as _json

    with open(json_path, "r", encoding="utf-8") as f:
        data = _json.load(f)

    for title, entry in data.items():
        wikitext = entry.get("content", "")
        page_id = int(entry.get("pageid", 0))
        # No revision_id in this format; use 0
        revision_id = 0

        if not title:
            continue

        try:
            result = parse_page(
                title=title,
                wikitext=wikitext,
                page_id=page_id,
                revision_id=revision_id,
                wiki_domain=wiki_domain,
                namespace=0,
            )
            yield result
        except Exception:
            continue


def _detect_xml_namespace(xml_path: str) -> str:
    """Detect the MediaWiki XML namespace from the root element."""
    for event, elem in ET.iterparse(xml_path, events=("start",)):
        tag = elem.tag
        if tag.startswith("{"):
            return tag[1:tag.index("}")]
        return ""
    return ""


def _parse_page_element(
    elem: ET.Element,
    ns_prefix: str,
    wiki_domain: str,
) -> Optional[WikiPageParse]:
    """Parse a single <page> element from the XML dump."""
    ns_elem = elem.find(f"{ns_prefix}ns")
    namespace = int(ns_elem.text) if ns_elem is not None and ns_elem.text else 0

    # Only process namespace 0 (main articles)
    if namespace != 0:
        return None

    title_elem = elem.find(f"{ns_prefix}title")
    id_elem = elem.find(f"{ns_prefix}id")
    revision_elem = elem.find(f"{ns_prefix}revision")

    if title_elem is None or id_elem is None or revision_elem is None:
        return None

    title = title_elem.text or ""
    page_id = int(id_elem.text) if id_elem.text else 0

    rev_id_elem = revision_elem.find(f"{ns_prefix}id")
    text_elem = revision_elem.find(f"{ns_prefix}text")

    revision_id = int(rev_id_elem.text) if rev_id_elem is not None and rev_id_elem.text else 0
    wikitext = text_elem.text or "" if text_elem is not None else ""

    if not title:
        return None

    return parse_page(
        title=title,
        wikitext=wikitext,
        page_id=page_id,
        revision_id=revision_id,
        wiki_domain=wiki_domain,
        namespace=namespace,
    )
