"""
Brief payload: stable ref assignment and parser for answer_mode=explainer briefs.

Phase: Brief -> Claims Bridge v1 (2026-04-05)

Responsibilities:
- assign stable S# / R# reference IDs to sources in prompt-visible order
- render entity / doc / web / relationship blocks with those IDs for the LLM
- parse the LLM's structured brief back into a machine-usable brief_payload dict
- map S# refs to real source URIs from the sources list
- emit parse_warnings rather than raising on partial failures
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── Reference ID pattern ──────────────────────────────────────────────────────

_REF_PATTERN = re.compile(r'\b([SRG]\d+)\b', re.IGNORECASE)


def _extract_refs(text: str) -> list[str]:
    """Extract all reference IDs (S1, R2, G3, …) from text, uppercased."""
    return [m.upper() for m in _REF_PATTERN.findall(text)]


# ── Ref assignment ────────────────────────────────────────────────────────────

def assign_source_refs(
    sources: list[dict],
    doc_chunks: list[dict],
    web_sources: list[dict],
    relationships_ctx: list[str],
) -> dict[str, dict]:
    """Assign stable S# / R# refs in prompt-visible order.

    Mutates sources, doc_chunks, and web_sources in-place by adding a 'ref' key.
    Relationship refs (R#) are recorded in ref_to_source only — relationships_ctx
    items are plain strings and cannot be mutated the same way.

    Ref ordering matches exactly what the model sees in the rendered prompt:
      1. entity sources (non-Document, non-WebSource entries in sources[])
      2. doc_chunks (each entry in order)
      3. web_sources (each entry in order)
      4. relationships → R1, R2, ...

    Returns:
        ref_to_source: maps ref string ("S1", "R1", …) to
            {uri, label, entity_type} info dict
    """
    ref_to_source: dict[str, dict] = {}
    s_counter = 1

    # 1. Entity sources (rendered in entity_block)
    for s in sources:
        if s.get("entity_type") not in ("Document", "WebSource"):
            ref = f"S{s_counter}"
            s_counter += 1
            s["ref"] = ref
            ref_to_source[ref] = {
                "uri": s.get("uri", ""),
                "label": s.get("label", ""),
                "entity_type": s.get("entity_type", ""),
            }

    # 2. Doc chunk sources (rendered in doc_block)
    for d in doc_chunks:
        ref = f"S{s_counter}"
        s_counter += 1
        d["ref"] = ref
        uri = d.get("rid") or d.get("uri") or d.get("title", "")
        ref_to_source[ref] = {
            "uri": uri,
            "label": d.get("title", ""),
            "entity_type": "Document",
            "url": d.get("wiki_url"),
        }
        # Mirror the ref onto the corresponding Document entry in sources[]
        for s in sources:
            if s.get("entity_type") == "Document" and s.get("uri") == uri and "ref" not in s:
                s["ref"] = ref
                break

    # 3. Web sources (rendered in web_block)
    for w in web_sources:
        ref = f"S{s_counter}"
        s_counter += 1
        w["ref"] = ref
        web_uri = w.get("url") or w.get("uri", "")
        ref_to_source[ref] = {
            "uri": web_uri,
            "label": w.get("title", ""),
            "entity_type": "WebSource",
        }
        # Mirror the ref onto the corresponding WebSource entry in sources[]
        for s in sources:
            if s.get("entity_type") == "WebSource" and s.get("uri") == web_uri and "ref" not in s:
                s["ref"] = ref
                break

    # 4. Relationship refs (R#) — no mutation of strings
    for i, rel_text in enumerate(relationships_ctx):
        ref = f"R{i + 1}"
        ref_to_source[ref] = {
            "uri": None,
            "label": rel_text[:200],
            "entity_type": "relationship",
        }

    return ref_to_source


# ── Ref-aware prompt renderers ────────────────────────────────────────────────

def render_entity_block(sources: list[dict]) -> str:
    """Render entity_block with [S#] prefixes (explainer mode only)."""
    lines = []
    for s in sources:
        if s.get("entity_type") in ("Document", "WebSource"):
            continue
        ref = s.get("ref", "")
        prefix = f"[{ref}] " if ref else ""
        line = f"- {prefix}{s['label']} ({s['entity_type']})"
        if s.get("description"):
            line += f": {s['description']}"
        lines.append(line)
    return "\n".join(lines) or "(no matching entities found)"


def render_doc_block(doc_chunks: list[dict]) -> str:
    """Render doc_block with [S#] prefixes (explainer mode only)."""
    lines = []
    for d in doc_chunks:
        ref = d.get("ref", "")
        prefix = f"[{ref}] " if ref else ""
        line = f"- {prefix}**{d['title']}**"
        if d.get("section_title"):
            line += f" (Section: {d['section_title']})"
        if d.get("wiki_url"):
            line += f" [source]({d['wiki_url']})"
        if d.get("context"):
            line += f" Context: {d['context']}."
        line += f": {d['text'][:1500]}"
        lines.append(line)
    return "\n".join(lines)


def render_web_block(web_sources: list[dict]) -> str:
    """Render web_block with [S#] prefixes (explainer mode only)."""
    lines = []
    for w in web_sources:
        ref = w.get("ref", "")
        prefix = f"[{ref}] " if ref else ""
        lines.append(f"- {prefix}[{w['title']}]({w['url']}): {w['summary'][:500]}")
    return "\n".join(lines)


def render_rel_block(relationships_ctx: list[str], ref_to_source: dict) -> str:
    """Render relationships with [R#] prefixes (explainer mode only)."""
    lines = []
    for i, rel_text in enumerate(relationships_ctx):
        lines.append(f"- [R{i + 1}] {rel_text}")
    return "\n".join(lines) or "(none)"


def assign_and_render_graph_query_refs(
    graph_query_block: str,
    ref_to_source: dict[str, dict],
) -> str:
    """Assign stable G# refs to graph-query result lines and render with prefixes.

    The first line (summary header) keeps no ref — it is context, not a citable
    item. Each subsequent `- ...` item line gets G1, G2, … assigned in order.
    G# entries are added to ref_to_source with uri=None and
    entity_type="graph_query".

    Returns the rendered block (or the original string unchanged if empty).
    """
    if not graph_query_block:
        return graph_query_block

    raw_lines = graph_query_block.split("\n")
    rendered: list[str] = []
    g_counter = 1

    for i, line in enumerate(raw_lines):
        stripped = line.strip()
        if i == 0 or not stripped.startswith("-"):
            # Header line or blank separator: no ref
            rendered.append(line)
        else:
            ref = f"G{g_counter}"
            g_counter += 1
            item_text = stripped.lstrip("- ").strip()
            ref_to_source[ref] = {
                "uri": None,
                "label": item_text[:200],
                "entity_type": "graph_query",
            }
            rendered.append(f"- [{ref}] {item_text}")

    return "\n".join(rendered)


# ── Brief parser ──────────────────────────────────────────────────────────────

def _parse_claim_block(
    block: str,
    support_class: str,
    ref_to_source: dict,
) -> Optional[dict]:
    """Parse one claim block (Claim / Evidence / Support / optional Missing)."""
    claim_text: Optional[str] = None
    evidence_text: Optional[str] = None
    support_text: Optional[str] = None
    missing_text: Optional[str] = None

    for raw_line in block.split("\n"):
        line = raw_line.strip().lstrip("- ").strip()
        m = re.match(r'\*\*[Cc]laim:?\*\*:?\s*(.*)', line)
        if m:
            claim_text = m.group(1).strip().strip("*").strip()
            continue
        m = re.match(r'\*\*[Ee]vidence:?\*\*:?\s*(.*)', line)
        if m:
            evidence_text = m.group(1).strip()
            continue
        m = re.match(r'\*\*[Ss]upport:?\*\*:?\s*(.*)', line)
        if m:
            support_text = m.group(1).strip().strip("`").strip()
            continue
        m = re.match(r'\*\*[Mm]issing:?\*\*:?\s*(.*)', line)
        if m:
            missing_text = m.group(1).strip()

    if not claim_text:
        return None

    # Resolve evidence refs → evidence item list
    evidence: list[dict] = []
    if evidence_text:
        refs = _extract_refs(evidence_text)
        if refs:
            for ref in refs:
                src = ref_to_source.get(ref, {})
                if ref.startswith("S"):
                    kind = "source"
                elif ref.startswith("R"):
                    kind = "relationship"
                else:
                    kind = "graph_query"
                evidence.append({
                    "kind": kind,
                    "ref": ref,
                    "label": src.get("label", ""),
                    "uri": src.get("uri") if kind == "source" else None,
                    "entity_type": src.get("entity_type", kind),
                })
        else:
            # Free-text evidence with no recognisable ref IDs
            evidence.append({
                "kind": "unresolved",
                "ref": None,
                "label": evidence_text[:200],
                "uri": None,
                "entity_type": "unknown",
            })

    # Normalise support label
    support = support_class  # fallback from section context
    if support_text:
        s_lower = support_text.lower()
        if "well" in s_lower:
            support = "well-supported"
        elif "partial" in s_lower or "tentative" in s_lower:
            support = "partially supported"
        else:
            support = s_lower

    return {
        "statement": claim_text,
        "support": support,
        "evidence": evidence,
        "missing": missing_text,
    }


def _parse_claims_section(
    section_text: str,
    support_class: str,
    ref_to_source: dict,
) -> tuple[list[dict], list[str]]:
    """Parse a claims section into claim dicts and parse warnings.

    Uses **Claim:** as the authoritative block separator — works even when
    the model omits blank lines between claim entries.
    """
    claims: list[dict] = []
    warnings: list[str] = []

    claim_start_re = re.compile(
        r'(?:^|\n)\s*-?\s*\*\*[Cc]laim:?\*\*:?',
        re.MULTILINE,
    )
    positions = [m.start() for m in claim_start_re.finditer(section_text)]
    if not positions:
        return claims, warnings

    for i, pos in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else len(section_text)
        block = section_text[pos:end].strip()
        parsed = _parse_claim_block(block, support_class, ref_to_source)
        if parsed:
            claims.append(parsed)
        else:
            warnings.append(f"Could not parse claim block: {block[:80]!r}")

    return claims, warnings


def _parse_key_sources(section_text: str, ref_to_source: dict) -> list[dict]:
    """Parse Key Sources section into source objects."""
    key_sources: list[dict] = []
    for raw_line in section_text.split("\n"):
        line = raw_line.strip().lstrip("- ").strip()
        if not line:
            continue
        # Match "S1: ...", "[S1]: ...", "S1 - ..."
        ref_match = re.match(r'^\[?([SRG]\d+)\]?[:\s-]+\s*(.*)', line, re.IGNORECASE)
        if ref_match:
            ref = ref_match.group(1).upper()
            label_part = ref_match.group(2).strip()
            src = ref_to_source.get(ref, {})
            uri = src.get("uri") or src.get("url")
            label = src.get("label", ref)
            # Pull label/URL from markdown link syntax if present
            url_match = re.search(r'\[([^\]]+)\]\(([^)]+)\)', label_part)
            if url_match:
                label = url_match.group(1) or label
                uri = uri or url_match.group(2)
            elif label_part:
                label = label_part
            key_sources.append({
                "ref": ref,
                "label": label,
                "uri": uri,
                "entity_type": src.get("entity_type", ""),
            })
        else:
            key_sources.append({
                "ref": None,
                "label": line[:200],
                "uri": None,
                "entity_type": "unknown",
            })
    return key_sources


def _parse_open_questions(section_text: str) -> list[str]:
    """Parse Open Questions section into a list of strings."""
    questions: list[str] = []
    for raw_line in section_text.split("\n"):
        line = raw_line.strip().lstrip("- ").strip()
        if line:
            questions.append(line)
    return questions


def parse_brief(answer: str, ref_to_source: dict[str, dict]) -> dict:
    """Parse a structured explainer brief into brief_payload.

    Args:
        answer: The markdown brief produced by the LLM.
        ref_to_source: Maps ref strings ("S1", "R1", …) to source info dicts
            with keys: uri, label, entity_type. Built by assign_source_refs().

    Returns:
        brief_payload dict with keys:
            bottom_line, claims, open_questions, key_sources, parse_warnings.
        Never raises — degraded output is returned with parse_warnings set.
    """
    parse_warnings: list[str] = []

    # ── Section splitting ─────────────────────────────────────────────────────
    section_re = re.compile(r'^## (.+)$', re.MULTILINE)
    section_matches = list(section_re.finditer(answer))

    sections: dict[str, str] = {}
    for idx, match in enumerate(section_matches):
        heading = match.group(1).strip()
        start = match.end()
        end = section_matches[idx + 1].start() if idx + 1 < len(section_matches) else len(answer)
        sections[heading.lower()] = answer[start:end].strip()

    if not sections:
        parse_warnings.append("No ## sections found — cannot parse brief structure")
        return {
            "bottom_line": answer[:500] if answer else "",
            "claims": [],
            "open_questions": [],
            "key_sources": [],
            "parse_warnings": parse_warnings,
        }

    # ── Bottom Line ───────────────────────────────────────────────────────────
    bottom_line = ""
    for heading, text in sections.items():
        if "bottom" in heading or "line" in heading:
            bottom_line = text
            break
    if not bottom_line:
        parse_warnings.append("No 'Bottom Line' section found")

    # ── Claims (Well-Supported + Partial/Tentative) ───────────────────────────
    all_claims: list[dict] = []
    claim_counter = 1

    for heading, text in sections.items():
        if "well" in heading and ("support" in heading or "claim" in heading):
            new_claims, warns = _parse_claims_section(text, "well-supported", ref_to_source)
            parse_warnings.extend(warns)
        elif "partial" in heading or "tentative" in heading:
            new_claims, warns = _parse_claims_section(text, "partially supported", ref_to_source)
            parse_warnings.extend(warns)
        else:
            continue
        for c in new_claims:
            c["id"] = f"C{claim_counter}"
            claim_counter += 1
        all_claims.extend(new_claims)

    # ── Open Questions ────────────────────────────────────────────────────────
    open_questions: list[str] = []
    for heading, text in sections.items():
        if "open" in heading or "question" in heading:
            open_questions = _parse_open_questions(text)
            break

    # ── Key Sources ───────────────────────────────────────────────────────────
    key_sources: list[dict] = []
    for heading, text in sections.items():
        if "source" in heading or "key" in heading:
            key_sources = _parse_key_sources(text, ref_to_source)
            break

    # ── Support value guard ───────────────────────────────────────────────────
    _valid_support = {"well-supported", "partially supported"}
    for c in all_claims:
        if c.get("support") not in _valid_support:
            parse_warnings.append(
                f"Claim {c.get('id', '?')} has unexpected support value: {c.get('support')!r}"
            )

    return {
        "bottom_line": bottom_line,
        "claims": all_claims,
        "open_questions": open_questions,
        "key_sources": key_sources,
        "parse_warnings": parse_warnings,
    }
