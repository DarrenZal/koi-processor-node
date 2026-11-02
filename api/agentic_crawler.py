"""
Agentic web crawler for BKC org ingest.

Given a start URL and a user goal, walk the site page-by-page, let an LLM
decide each hop, and return a single structured proposal (one Org + programs
+ partners + people, wired with BKC ontology predicates) for curator approval.

Phase 1 surface is synchronous and vision-free: no background worker, no
image OCR. Those land in Phases 2 and 3. The loop structure here anticipates
both: it already accepts a progress_callback and a CostTracker, and its
`WorldModel.merge()` is the single place both text and (future) vision
extractions converge.

Contract:
- All page fetches go through ``fetch_and_preview(url, _internal_call=True)``
  in ``api.web_fetcher``. The crawler never imports aiohttp/requests/playwright
  directly — enforced by AC4 grep.
- All LLM calls go through ``api.crawl_llm`` (enforced by AC49; the main
  chat abstraction is intentionally not used in this path).
- The ontology allow-list comes from ``api.ontology_registry`` (loaded once at
  service startup).
"""

from __future__ import annotations

import asyncio
import heapq
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, Tuple
from urllib.parse import urlparse

from pydantic import BaseModel, Field

from api import crawl_llm, ontology_registry
from api.prompts.crawl_page_analysis import PageAnalysis

logger = logging.getLogger(__name__)

PROPOSAL_VERSION = "v1"
DEFAULT_MAX_PAGES = 40
DEFAULT_MAX_SECONDS = 180
DEFAULT_MAX_USD = 0.50
DEFAULT_MAX_VISION = 20
SYSTEM_MAX_PAGES = 60
SYSTEM_MAX_SECONDS = 300
SYSTEM_MAX_USD = 1.00
SYSTEM_MAX_VISION = 30
WORLD_MODEL_MAX_TOKENS = 2_000
PHASE1_SYNC_PAGE_CAP = 20

CONSECUTIVE_SKIP_TERMINATE = 3
MIN_PAGES_BEFORE_SUFFICIENT = 3  # don't honor an early LLM "sufficient" until we've expanded past a thin landing page


class CrawlBudgetExceeded(Exception):
    """Raised when the crawl hits a budget guardrail."""


@dataclass
class CrawlBudget:
    max_pages: int = DEFAULT_MAX_PAGES
    max_vision_calls: int = DEFAULT_MAX_VISION
    max_seconds: int = DEFAULT_MAX_SECONDS
    max_usd: float = DEFAULT_MAX_USD

    def clamp_to_system_ceilings(self) -> None:
        if self.max_pages > SYSTEM_MAX_PAGES:
            raise ValueError(
                f"max_pages {self.max_pages} > system ceiling {SYSTEM_MAX_PAGES}"
            )
        if self.max_vision_calls > SYSTEM_MAX_VISION:
            raise ValueError(
                f"max_vision_calls {self.max_vision_calls} > system ceiling {SYSTEM_MAX_VISION}"
            )
        if self.max_seconds > SYSTEM_MAX_SECONDS:
            raise ValueError(
                f"max_seconds {self.max_seconds} > system ceiling {SYSTEM_MAX_SECONDS}"
            )
        if self.max_usd > SYSTEM_MAX_USD:
            raise ValueError(
                f"max_usd {self.max_usd} > system ceiling {SYSTEM_MAX_USD}"
            )

    def as_snapshot(self) -> dict[str, Any]:
        return {
            "max_pages": self.max_pages,
            "max_vision_calls": self.max_vision_calls,
            "max_seconds": self.max_seconds,
            "max_usd": self.max_usd,
        }


@dataclass
class CostTracker:
    usd: float = 0.0
    calls: int = 0

    def record(self, usage: dict[str, int], model: str) -> None:
        self.usd += crawl_llm.estimate_usd(usage, model)
        self.calls += 1


@dataclass
class ProposedEntity:
    name: str
    type: str
    description: str | None = None
    source_url: str | None = None
    source_image: str | None = None
    confidence: float = 1.0
    requires_review: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
    existing_rid: str | None = None


@dataclass
class ProposedRelationship:
    subject_index: int
    predicate: str
    object_index: int


class WorldModel:
    """In-memory accumulator for entities + relationships across pages."""

    def __init__(self) -> None:
        self._entities: list[ProposedEntity] = []
        self._entity_index: dict[tuple[str, str], int] = {}
        self._relationships: list[ProposedRelationship] = []
        self._relationship_keys: set[tuple[int, str, int]] = set()
        self._vision_calls: int = 0
        self._summary_compression_events: list[dict[str, Any]] = []

    @property
    def vision_calls(self) -> int:
        return self._vision_calls

    def register_vision_call(self) -> None:
        self._vision_calls += 1

    @property
    def entities(self) -> list[ProposedEntity]:
        return list(self._entities)

    @property
    def relationships(self) -> list[ProposedRelationship]:
        return list(self._relationships)

    @property
    def summary_compression_events(self) -> list[dict[str, Any]]:
        return list(self._summary_compression_events)

    def _key(self, name: str, entity_type: str) -> tuple[str, str]:
        return (name.strip().lower(), entity_type)

    def upsert_entity(self, entity: ProposedEntity) -> int:
        key = self._key(entity.name, entity.type)
        existing_idx = self._entity_index.get(key)
        if existing_idx is not None:
            current = self._entities[existing_idx]
            if entity.confidence > current.confidence:
                current.confidence = entity.confidence
                current.requires_review = entity.requires_review
            if entity.description and not current.description:
                current.description = entity.description
            if entity.source_image and not current.source_image:
                current.source_image = entity.source_image
            current.metadata = {**current.metadata, **entity.metadata}
            return existing_idx
        self._entities.append(entity)
        idx = len(self._entities) - 1
        self._entity_index[key] = idx
        return idx

    def add_relationship(
        self, subject_index: int, predicate: str, object_index: int
    ) -> None:
        key = (subject_index, predicate, object_index)
        if key in self._relationship_keys:
            return
        self._relationship_keys.add(key)
        self._relationships.append(
            ProposedRelationship(subject_index, predicate, object_index)
        )

    def merge_page_analysis(
        self,
        analysis: PageAnalysis,
        *,
        source_url: str,
    ) -> tuple[int, int]:
        """Fold one page's extracted entities + relationships into the world.

        Returns (entities_added, relationships_added). Skips relationships whose
        subject or object name is unknown to the model after this page.
        """
        entities_added_before = len(self._entities)
        relationships_added_before = len(self._relationships)

        name_to_index: dict[tuple[str, str], int] = {}
        for ent in analysis.entities:
            entity = ProposedEntity(
                name=ent.name,
                type=ent.type,
                description=ent.description,
                source_url=source_url,
                confidence=ent.confidence,
                metadata=dict(ent.metadata),
            )
            idx = self.upsert_entity(entity)
            name_to_index[(ent.name.strip().lower(), ent.type)] = idx

        for rel in analysis.relationships:
            subj_idx = self._find_index_by_name(rel.subject_name, name_to_index)
            obj_idx = self._find_index_by_name(rel.object_name, name_to_index)
            if subj_idx is None or obj_idx is None:
                continue
            if rel.predicate not in ontology_registry.ALLOWED_PREDICATES:
                continue
            self.add_relationship(subj_idx, rel.predicate, obj_idx)

        return (
            len(self._entities) - entities_added_before,
            len(self._relationships) - relationships_added_before,
        )

    def _find_index_by_name(
        self, name: str, locals_: dict[tuple[str, str], int]
    ) -> Optional[int]:
        needle = name.strip().lower()
        for (lname, _), idx in locals_.items():
            if lname == needle:
                return idx
        for (lname, _), idx in self._entity_index.items():
            if lname == needle:
                return idx
        return None

    def summary(self, max_tokens: int = WORLD_MODEL_MAX_TOKENS) -> str:
        lines = []
        for i, ent in enumerate(self._entities):
            desc = (ent.description or "").replace("\n", " ")[:120]
            lines.append(f"- [{i}] {ent.name} ({ent.type}): {desc}")
        rel_lines = []
        for rel in self._relationships:
            if rel.subject_index >= len(self._entities) or rel.object_index >= len(self._entities):
                continue
            subj = self._entities[rel.subject_index].name
            obj = self._entities[rel.object_index].name
            rel_lines.append(f"- {subj} --{rel.predicate}--> {obj}")

        combined = "ENTITIES:\n" + "\n".join(lines) + "\nRELATIONSHIPS:\n" + "\n".join(rel_lines)
        tokens = crawl_llm._count_tokens(combined)
        if tokens <= max_tokens:
            return combined

        # Compression: drop relationships first, then low-confidence leaf entities.
        dropped = {"relationships": 0, "entities": 0}
        while tokens > max_tokens and rel_lines:
            rel_lines.pop()
            dropped["relationships"] += 1
            combined = "ENTITIES:\n" + "\n".join(lines) + "\nRELATIONSHIPS:\n" + "\n".join(rel_lines)
            tokens = crawl_llm._count_tokens(combined)

        # Sort by confidence ascending; keep root (index 0) always.
        entity_order = sorted(
            range(len(self._entities)),
            key=lambda i: (i == 0, self._entities[i].confidence),
        )
        drop_queue = [i for i in entity_order if i != 0]
        lines_by_index = {i: lines[i] for i in range(len(lines))}
        while tokens > max_tokens and drop_queue:
            drop_idx = drop_queue.pop(0)
            if drop_idx in lines_by_index:
                del lines_by_index[drop_idx]
                dropped["entities"] += 1
            lines = [lines_by_index[k] for k in sorted(lines_by_index.keys())]
            combined = "ENTITIES:\n" + "\n".join(lines) + "\nRELATIONSHIPS:\n" + "\n".join(rel_lines)
            tokens = crawl_llm._count_tokens(combined)

        self._summary_compression_events.append(
            {"dropped": dropped, "final_tokens": tokens}
        )
        return combined


class CrawlProposal(BaseModel):
    proposal_version: str = PROPOSAL_VERSION
    ontology_version: str
    start_url: str
    root_entity_index: int = 0
    entities: list[dict] = Field(default_factory=list)
    relationships: list[dict] = Field(default_factory=list)
    recommended_next_crawls: list[str] = Field(default_factory=list)
    stats: dict = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _registrable_domain(host: str) -> str:
    host = host.split(":", 1)[0].lower()
    try:
        import tldextract

        result = tldextract.extract(host)
        if result.registered_domain:
            return result.registered_domain
    except ImportError:
        logger.warning("tldextract not installed; falling back to naive domain match")
    # Fallback: last two labels.
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def same_domain(candidate_url: str, start_url: str) -> bool:
    """True if both URLs share the same registrable domain (eTLD+1).

    www/subdomain tolerant: `www.example.org`, `blog.example.org`, and
    `example.org` all match each other.
    """
    try:
        cand_host = urlparse(candidate_url).hostname or ""
        start_host = urlparse(start_url).hostname or ""
    except Exception:
        return False
    if not cand_host or not start_host:
        return False
    return _registrable_domain(cand_host) == _registrable_domain(start_host)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

ProgressCallback = Callable[[dict], Awaitable[None]]
LookupFn = Callable[[str, str], Awaitable[Optional[str]]]
# VisionFn(image_url, role, context) -> (orgs_list, usage, fetched_url)
VisionFn = Callable[[str, str, str], Awaitable[Tuple[list[dict], dict[str, int], Optional[str]]]]


# Vision confidence policy — single source of truth (plan Assumptions).
VISION_INCLUDE_THRESHOLD = 0.8
VISION_REVIEW_FLOOR = 0.7

# Role -> (entity_type, [(predicate, direction)]). Direction "out" = root_subject→extracted_object;
# "in" = extracted_subject→root_object. Roles not in this map are skipped.
_ROLE_POLICY: dict[str, tuple[str, list[tuple[str, str]]]] = {
    "partner_grid": ("Organization", [("collaborates_with", "out")]),
    "sponsor_list": ("Organization", [("collaborates_with", "out"), ("affiliated_with", "in")]),
    "funder_list": ("Organization", [("collaborates_with", "out"), ("affiliated_with", "in")]),
    "team_photo": ("Person", [("affiliated_with", "in")]),
    "infographic": ("Concept", [("about", "in")]),
}


async def agentic_crawl(
    *,
    start_url: str,
    goal: str = "",
    budget: Optional[CrawlBudget] = None,
    fetch_fn: Callable[[str], Awaitable[Any]],
    lookup_fn: Optional[LookupFn] = None,
    vision_fn: Optional[VisionFn] = None,
    progress_callback: Optional[ProgressCallback] = None,
    cost_tracker: Optional[CostTracker] = None,
    cancel_check: Optional[Callable[[], Awaitable[bool]]] = None,
) -> CrawlProposal:
    """Drive the page-by-page loop. Returns a structured CrawlProposal.

    Parameters intentionally take in callables so this module can be unit-tested
    without HTTP, OpenAI, or DB dependencies.

    Args:
        fetch_fn: ``url -> WebPreview-like``. Expected to expose ``.content_text``
            attribute (the raw text — analysis uses the compacted HTML derived
            here when available; the test harness passes raw HTML via a
            CompactedPage-compatible surrogate).
        lookup_fn: ``(name, type) -> existing_rid_or_None``. Optional; used to
            populate each proposal entity's ``existing_rid``. None means skip.
        progress_callback: awaited once per page iteration with a progress dict.
        cost_tracker: optional shared CostTracker; created if not passed.
        cancel_check: awaited each iteration; if it returns True, the loop
            halts with whatever it has.
    """
    budget = budget or CrawlBudget()
    budget.clamp_to_system_ceilings()
    cost_tracker = cost_tracker or CostTracker()

    world = WorldModel()
    visited: list[str] = []
    visited_set: set[str] = set()
    consecutive_skips = 0
    pending_vision: list[tuple[list[dict], str, str, str]] = []

    # Priority queue of (-priority, seq, url, reason). Negate for max-heap via heapq.
    seq_counter = 0
    candidates: list[tuple[float, int, str, str]] = []
    heapq.heappush(candidates, (-1.0, seq_counter, start_url, "start"))
    seq_counter += 1

    allowed_types = sorted(ontology_registry.ALLOWED_ENTITY_TYPES)
    allowed_predicates = sorted(ontology_registry.ALLOWED_PREDICATES)
    started_at = time.monotonic()

    async def _emit_progress(stage: str, current_url: Optional[str], judgment: str | None):
        if not progress_callback:
            return
        payload = {
            "progress_version": "v1",
            "stage": stage,
            "pages_visited": len(visited),
            "visited": list(visited),
            "current_url": current_url,
            "entities_so_far": len(world.entities),
            "relationships_so_far": len(world.relationships),
            "vision_calls": world.vision_calls,
            "world_model_summary_tokens": crawl_llm._count_tokens(world.summary()),
            "summary_compression_events": world.summary_compression_events,
            "cost_usd": round(cost_tracker.usd, 6),
            "elapsed_s": int(time.monotonic() - started_at),
            "last_judgment": judgment,
            "candidates_remaining": len(candidates),
        }
        await progress_callback(payload)

    def _flush_pending_vision() -> None:
        nonlocal pending_vision
        if not pending_vision or not world.entities:
            return
        for orgs, role, source_url, source_image in pending_vision:
            _apply_vision_orgs(
                world,
                orgs=orgs,
                role=role,
                source_url=source_url,
                source_image=source_image,
            )
        pending_vision = []

    while candidates and len(visited) < budget.max_pages:
        if cancel_check and await cancel_check():
            logger.info("agentic_crawl cancelled by caller")
            break

        elapsed = time.monotonic() - started_at
        if elapsed > budget.max_seconds:
            logger.info(
                "wall-clock budget hit at %d pages/%d entities; finalizing partial proposal",
                len(visited), len(world.entities),
            )
            break
        if cost_tracker.usd > budget.max_usd:
            logger.info(
                "cost budget hit at %d pages/%d entities; finalizing partial proposal",
                len(visited), len(world.entities),
            )
            break

        neg_priority, _, url, reason = heapq.heappop(candidates)
        if url in visited_set:
            continue
        visited_set.add(url)
        visited.append(url)

        is_start_page = url == start_url

        await _emit_progress("fetching", url, None)

        try:
            preview = await fetch_fn(url)
        except Exception as exc:
            logger.warning("fetch_fn error on %s: %s", url, exc)
            if is_start_page:
                raise CrawlBudgetExceeded(f"start page unreachable: {exc}") from exc
            consecutive_skips += 1
            if consecutive_skips >= CONSECUTIVE_SKIP_TERMINATE:
                raise CrawlBudgetExceeded("LLM analysis failing repeatedly") from exc
            continue

        html = _extract_html(preview)
        if not html:
            logger.info("no html for %s; skipping", url)
            if is_start_page:
                raise CrawlBudgetExceeded("start page unreachable: empty content")
            consecutive_skips += 1
            if consecutive_skips >= CONSECUTIVE_SKIP_TERMINATE:
                raise CrawlBudgetExceeded("LLM analysis failing repeatedly")
            continue

        page = crawl_llm.compact_page(html, url)
        await _emit_progress("analyzing", url, None)

        try:
            analysis, usage = await crawl_llm.analyze_page(
                page=page,
                goal=goal,
                world_model_summary=world.summary(),
                allowed_types=allowed_types,
                allowed_predicates=allowed_predicates,
                model=crawl_llm.DEFAULT_MODEL,
            )
        except Exception as exc:  # malformed JSON + retry exhausted, or SDK error
            logger.warning("analyze_page failed on %s: %s", url, exc)
            if is_start_page:
                raise CrawlBudgetExceeded(f"start page analysis failed: {exc}") from exc
            consecutive_skips += 1
            if consecutive_skips >= CONSECUTIVE_SKIP_TERMINATE:
                raise CrawlBudgetExceeded("LLM analysis failing repeatedly") from exc
            continue

        consecutive_skips = 0
        cost_tracker.record(usage, crawl_llm.DEFAULT_MODEL)
        world.merge_page_analysis(analysis, source_url=url)
        _flush_pending_vision()

        # Vision step: run OCR on LLM-flagged images up to budget.
        if vision_fn is not None and analysis.worth_ocr_images:
            await _emit_progress("vision", url, analysis.judgment)
            for img_region in analysis.worth_ocr_images[:8]:  # prompt cap 8
                if world.vision_calls >= budget.max_vision_calls:
                    break
                if cost_tracker.usd > budget.max_usd:
                    break
                if img_region.role not in _ROLE_POLICY:
                    continue
                try:
                    orgs, v_usage, fetched_url = await vision_fn(
                        img_region.image_url,
                        img_region.role,
                        img_region.context or "",
                    )
                except Exception as exc:
                    logger.warning(
                        "vision_fn raised on %s: %s", img_region.image_url, exc
                    )
                    continue
                world.register_vision_call()
                if v_usage:
                    cost_tracker.record(v_usage, crawl_llm.DEFAULT_MODEL)
                target_image = fetched_url or img_region.image_url
                if not world.entities:
                    pending_vision.append((orgs, img_region.role, url, target_image))
                    continue
                _apply_vision_orgs(
                    world,
                    orgs=orgs,
                    role=img_region.role,
                    source_url=url,
                    source_image=target_image,
                )

        for link in analysis.next_links:
            if not link.url or link.url in visited_set:
                continue
            if not same_domain(link.url, start_url):
                continue
            heapq.heappush(
                candidates,
                (-max(0.0, min(1.0, link.priority)), seq_counter, link.url, link.reason),
            )
            seq_counter += 1

        # Thin/SPA landing pages: the LLM often returns few/no next_links (it is
        # told to only suggest URLs present in the page), which starves the frontier
        # and stops the crawl at one page. If we're about to run dry on the start
        # page, seed from the same-domain links compact_page already extracted from
        # the rendered HTML — this is what surfaces /our-work, /guardians, /grants.
        if is_start_page and not candidates:
            seeded = 0
            for _l in page.links:
                _lu = (_l.get("url") or "").strip()
                if not _lu or _lu in visited_set or not same_domain(_lu, start_url):
                    continue
                heapq.heappush(candidates, (-0.4, seq_counter, _lu, "seed:page-link"))
                seq_counter += 1
                seeded += 1
                if seeded >= 12:
                    break
            if seeded:
                logger.info("seeded %d same-domain links on thin start page %s", seeded, url)

        await _emit_progress("fetching", url, analysis.judgment)

        if analysis.judgment == "sufficient":
            # Don't honor an early "sufficient" (a soft LLM call with no mechanical
            # floor) on a thin landing page: keep expanding until we've visited a few
            # pages AND gathered real entity signal, as long as the frontier isn't
            # empty. Once there's genuinely nothing left to crawl, honor it.
            enough_pages = len(visited) >= MIN_PAGES_BEFORE_SUFFICIENT
            have_signal = len(world.entities) > 1  # more than just the root org
            if candidates and not (enough_pages and have_signal):
                logger.info(
                    "crawl judged sufficient at %s but only %d page(s)/%d entities; "
                    "overriding to keep expanding",
                    url, len(visited), len(world.entities),
                )
            else:
                logger.info("crawl judged sufficient at %s", url)
                break

    if lookup_fn is not None:
        for ent in world.entities:
            try:
                ent.existing_rid = await lookup_fn(ent.name, ent.type)
            except Exception as exc:
                logger.warning("lookup_fn failed for %s (%s): %s", ent.name, ent.type, exc)

    proposal = CrawlProposal(
        proposal_version=PROPOSAL_VERSION,
        ontology_version=ontology_registry.ONTOLOGY_VERSION,
        start_url=start_url,
        root_entity_index=0,
        entities=[_entity_to_dict(i, e) for i, e in enumerate(world.entities)],
        relationships=[
            {
                "subject_index": r.subject_index,
                "predicate": r.predicate,
                "object_index": r.object_index,
            }
            for r in world.relationships
        ],
        stats={
            "pages_visited": len(visited),
            "vision_calls": world.vision_calls,
            "cost_usd": round(cost_tracker.usd, 6),
            "duration_s": int(time.monotonic() - started_at),
        },
    )
    await _emit_progress("done", None, None)
    return proposal


def _apply_vision_orgs(
    world: "WorldModel",
    *,
    orgs: list[dict],
    role: str,
    source_url: str,
    source_image: str,
) -> None:
    """Merge vision-extracted entities + role-driven predicates into the world.

    Confidence routing (single source of truth, see plan Assumptions):
      ≥0.8       → included, requires_review=False
      0.7..0.79  → included, requires_review=True
      <0.7       → dropped entirely

    Role routing: only roles in ``_ROLE_POLICY`` yield entities; others are
    dropped silently (prompt already excludes them, this is a defensive gate).
    """
    policy = _ROLE_POLICY.get(role)
    if policy is None:
        return
    entity_type, predicate_specs = policy

    if not world.entities:
        logger.info(
            "_apply_vision_orgs: no root entity yet, dropping vision orgs for %s",
            source_image,
        )
        return
    root_index = 0

    for org in orgs:
        name = (org.get("name") or "").strip()
        if not name:
            continue
        try:
            confidence = float(org.get("confidence", 0.0))
        except (TypeError, ValueError):
            continue
        if confidence < VISION_REVIEW_FLOOR:
            continue
        requires_review = confidence < VISION_INCLUDE_THRESHOLD

        entity = ProposedEntity(
            name=name,
            type=entity_type,
            description=None,
            source_url=source_url,
            source_image=source_image,
            confidence=confidence,
            requires_review=requires_review,
            metadata={"vision_role": role},
        )
        new_index = world.upsert_entity(entity)
        # Record that this entity picked up a source_image (upsert_entity only
        # sets it if previously empty; we want it even if LLM already added
        # the entity from text context earlier).
        if not world._entities[new_index].source_image:
            world._entities[new_index].source_image = source_image
        if requires_review:
            world._entities[new_index].requires_review = True

        for predicate, direction in predicate_specs:
            if predicate not in ontology_registry.ALLOWED_PREDICATES:
                continue
            if direction == "out":
                world.add_relationship(root_index, predicate, new_index)
            else:  # "in"
                world.add_relationship(new_index, predicate, root_index)


def _extract_html(preview: Any) -> str:
    """Pick the HTML-ish payload from whatever fetch_fn returned.

    WebPreview already carries ``content_text`` (extracted text, not HTML). The
    crawler wants raw HTML so the LLM sees links + images. If the preview
    exposes ``raw_html``/``html`` we use that; otherwise fall back to content_text
    which still gives the LLM headings and inline links via BeautifulSoup.
    """
    for attr in ("raw_html", "html", "content_html"):
        val = getattr(preview, attr, None)
        if val:
            return val
    return getattr(preview, "content_text", "") or ""


def _entity_to_dict(index: int, ent: ProposedEntity) -> dict:
    return {
        "index": index,
        "name": ent.name,
        "type": ent.type,
        "description": ent.description,
        "source_url": ent.source_url,
        "source_image": ent.source_image,
        "confidence": ent.confidence,
        "requires_review": ent.requires_review,
        "metadata": ent.metadata,
        "existing_rid": ent.existing_rid,
    }
