"""
Prompt + schema for per-page LLM analysis during agentic web crawl.

The LLM receives a CompactedPage (crawl_llm.compact_page output), the current
world-model summary, and the ontology allow-lists, and returns a structured
analysis: entities found, relationships, next-link suggestions, judgment, and
optional image-region hints for Phase 2 vision.

Pydantic models are the single source of truth for the expected JSON shape —
used both for the `response_format` instruction and for strict parsing of the
LLM reply.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ProposedEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    description: str | None = None
    metadata: dict = Field(default_factory=dict)
    confidence: float = 1.0


class ProposedRelationship(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subject_name: str
    predicate: str
    object_name: str


class ImageRegion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image_url: str
    role: Literal[
        "partner_grid",
        "sponsor_list",
        "funder_list",
        "team_photo",
        "infographic",
        "generic_decoration",
        "unknown",
    ] = "unknown"
    context: str | None = None


class NextLink(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    priority: float = 0.5
    reason: str = ""


class PageAnalysis(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entities: list[ProposedEntity] = Field(default_factory=list)
    relationships: list[ProposedRelationship] = Field(default_factory=list)
    worth_ocr_images: list[ImageRegion] = Field(default_factory=list)
    next_links: list[NextLink] = Field(default_factory=list)
    judgment: Literal["continue", "sufficient"] = "continue"
    reasoning: str = ""


_SYSTEM_TEMPLATE = """You are a careful knowledge-graph curator mapping a website to the BKC (Bioregional Knowledge Commons) ontology.

You will be shown:
1. The user's goal for this crawl (may be empty).
2. A compacted representation of a single web page (title, headings, main text, link list with anchors, image list).
3. A short summary of the world-model accumulated so far from prior pages.

Your job: extract entities and relationships that this page contributes, flag image regions that likely contain more entity signal, propose next links to fetch on the same site, and judge whether the overall picture is already sufficient.

Rules:
- Only use entity types from this allow-list: {allowed_types}
- Only use predicates from this allow-list: {allowed_predicates}
- Common mapping: recurring named programs on an org site → type "Project" with metadata.kind="program" linked from the Organization via has_project. Specific project instances also use type "Project" with metadata.kind="instance". Reusable methods → type "Practice".
- Do not re-propose entities already in the world-model summary unless you have new metadata or a new relationship for them.
- For next_links: only suggest URLs that appear in the page's link list; prefer same-domain URLs that look like they reveal new structure (programs, partners, team, projects). Give each a priority 0.0–1.0.
- When nav or internal links expose likely org subpages such as /about/*, /partners/*, /team/*, /staff/*, /sponsors/*, /funders/*, /people/*, or /contact/*, include those exact visible URLs in next_links with short reason strings. Treat these patterns as illustrative rather than exhaustive; use page semantics and anchor text, not a hardcoded rule list, and never infer URLs that are not present.
- For worth_ocr_images: only flag images whose surrounding heading or alt-text suggests they carry entity information. Skip decorative imagery and icons. At most 8 images per page (hard cap — flag only the highest-value ones). Tag each with one of these roles; vision cost is meaningful so the role dictates downstream handling:
  - "partner_grid" — a grid or strip of partner organization logos (e.g., under headings like "Partners", "Our Partners", "Collaborators"). Downstream adds Organization entities with collaborates_with.
  - "sponsor_list" / "funder_list" — logos under headings like "Sponsors", "Funders", "Supported by". Downstream adds Organization entities with collaborates_with + affiliated_with.
  - "team_photo" — a photo with captioned Person names (e.g., a Team page grid). Downstream adds Person entities with affiliated_with to the root Org.
  - "infographic" — an image with named nodes/labels carrying Concept meaning. Downstream adds Concept entities with about.
  - "generic_decoration" / "unknown" — DO NOT flag these. If you're unsure and the image is probably decorative, omit it entirely.
  The LLM call on each image costs real dollars. Err on the side of flagging fewer, higher-confidence images.
- judgment="sufficient" means: no more pages on this site will materially improve the graph. Use sparingly.

Respond with JSON matching this schema exactly, no prose:
{json_schema}
"""


def build_messages(
    *,
    goal: str,
    compacted_page_json: str,
    world_model_summary: str,
    allowed_types: list[str],
    allowed_predicates: list[str],
) -> list[dict]:
    """Return the OpenAI-style messages list for a single page analysis call."""
    system = _SYSTEM_TEMPLATE.format(
        allowed_types=", ".join(sorted(allowed_types)),
        allowed_predicates=", ".join(sorted(allowed_predicates)),
        json_schema=PageAnalysis.model_json_schema(),
    )
    user = (
        f"USER GOAL:\n{goal or '(none specified)'}\n\n"
        f"WORLD-MODEL SUMMARY:\n{world_model_summary or '(empty)'}\n\n"
        f"COMPACTED PAGE:\n{compacted_page_json}\n"
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def build_retry_messages(
    *,
    goal: str,
    compacted_page_json: str,
    world_model_summary: str,
    allowed_types: list[str],
    allowed_predicates: list[str],
    prior_error: str,
    prior_output: str,
) -> list[dict]:
    """Messages for the single retry after a malformed/ontology-invalid first reply."""
    base = build_messages(
        goal=goal,
        compacted_page_json=compacted_page_json,
        world_model_summary=world_model_summary,
        allowed_types=allowed_types,
        allowed_predicates=allowed_predicates,
    )
    base.append({"role": "assistant", "content": prior_output[:4000]})
    base.append(
        {
            "role": "user",
            "content": (
                "Your previous response was rejected:\n"
                f"{prior_error}\n\n"
                "Re-emit the full JSON document. Keep valid entries; drop or correct "
                "invalid ones. Do not invent new entity types or predicates outside "
                "the allow-lists above."
            ),
        }
    )
    return base
