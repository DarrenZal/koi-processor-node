#!/usr/bin/env python3
"""The authoritative entity-type contract for deep DOCUMENT extraction (issue #68).

WHY THIS FILE EXISTS
--------------------
The same type vocabulary was written out by hand in six places — two JSON schemas,
two prompts (three spots each), and `TYPE_PRIORITY` in the extractor — with nothing
comparing them. On 2026-09-14 that cost real graph damage:

  - `DWeb Berlin`, a dated five-day gathering, could not be emitted as `Event`
    because no schema admitted the word. It came out `Organization`, then bound to
    an unrelated `Project`.
  - The named essays `The End of Hoop Jumping` and `How To Think About Science`
    could not be emitted as `Document`. They came out `Project` and were fuzzily
    collapsed into unrelated `Project` entities.

Neither was a model-quality failure. The correct answer was *structurally
forbidden*: the live registry admits `Document` and `Event` and marks both
`extractable = true`, and the extractor's contract had never been told.

  personal_koi=# SELECT count(*) FROM allowed_entity_types WHERE extractable;
   9          -- Person Organization Project Concept Location Protocol CaseStudy
              -- Document Event                     (verified read-only 2026-09-14)

So this module is the ONE place the vocabulary is written. Every other surface is
either derived from it (`scripts/render_extraction_contract.py`) or asserted equal
to it (`tests/test_document_extraction_type_contract.py`). Editing a schema or a
prompt by hand now fails a test instead of drifting quietly for weeks.

WHY IT MATTERS *NOW* SPECIFICALLY
---------------------------------
PR #66 pins endpoint identity: the entity_type is hashed into the canonical URI and
cannot be corrected in place afterwards. Today's unpinned path writes the wrong type
*correctably*; pinning writes it *irreversibly*. Pinning a wrong contract is worse
than not pinning — which is why #68 blocks #66's deployment rather than following it.

THIS MODULE IS IMPORT-ONLY
--------------------------
No I/O, no database, no environment. Tests that assert the contract must run with no
DSN and no network, because a check that skips is a check that does not exist.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

# Bumped whenever the admitted type SET or the priority ORDER changes. Recorded in
# extraction run receipts so a stored payload can be read back against the contract
# that produced it. Shape follows the sibling identifiers in api/ingest_identity.py
# (IDENTITY_CONTRACT_VERSION, NORMALIZER_VERSION).
DOCUMENT_TYPE_CONTRACT_VERSION = "doc-entity-types-v2-2026-09-14"

# The version this one succeeds, kept so a historical payload's provenance is
# readable rather than merely "older". v1 = the seven types shipped with the
# deep-document extractor; v2 adds Document and Event.
DOCUMENT_TYPE_CONTRACT_PREVIOUS = "doc-entity-types-v1-2026-06"


#: Both prompts wrap at 88 columns. The generated regions match, so a contract
#: change produces a readable diff rather than one 600-character line.
_WRAP_WIDTH = 88


def _wrap(text: str, *, subsequent_indent: str = "") -> str:
    return textwrap.fill(text, width=_WRAP_WIDTH, subsequent_indent=subsequent_indent,
                         break_long_words=False, break_on_hyphens=False)


@dataclass(frozen=True)
class DocumentEntityType:
    """One admitted type, with everything every surface needs to render it."""

    name: str
    priority: int
    registry_description: str
    rule: str
    #: How this type is named in the prompt's opening "extract ... entities" line.
    #: Derived rather than hand-written, because that sentence is a second copy of
    #: the vocabulary and went stale the moment Document and Event were added.
    noun_phrase: str = ""
    examples: Tuple[str, ...] = ()

    def bullet(self) -> str:
        """The canonicalization bullet as it appears in both prompts."""
        text = self.rule
        if self.examples:
            text += " (e.g. " + ", ".join(self.examples) + ")"
        return _wrap(f"- {text}", subsequent_indent="  ")


# ─────────────────────────────────────────────────────────────────────────────
# The vocabulary.
#
# DECLARATION ORDER IS THE ENUM ORDER. The original seven keep their original
# positions and `Document`/`Event` are appended, so the schema diff is purely
# additive and a historical payload validating against v1's enum still validates.
#
# PRIORITY governs cross-window reconciliation only: when two windows type one
# normalized label differently, the higher priority wins (extract_deep_documents.
# merge_extractions). Two properties are load-bearing and are asserted by tests:
#
#   1. The RELATIVE order of the original seven is unchanged — Person >
#      Organization > Project > Location > Protocol > CaseStudy > Concept. Adding
#      types must not silently re-resolve conflicts that already resolve correctly.
#
#   2. Document and Event rank strictly ABOVE Project. `Project` is the generic
#      "named thing" sink that both 2026-09-14 mis-types fell into, so a window
#      that recognises an essay as a Document or a gathering as an Event must not
#      be overruled by a window that fell back to Project.
#
#   Event outranks Organization on measured risk, not taste: the one observed
#   cross-type collapse in this corpus is a dated gathering captured as an
#   Organization (`DWeb Berlin`). The reverse — an organization captured as an
#   event — has not been observed. If it ever is, this ordering is the thing to
#   revisit, and the test that pins it names this paragraph.
#
# registry_description is copied VERBATIM from allowed_entity_types so the prompt
# teaches the same definition the registry stores. Read 2026-09-14 read-only.
# ─────────────────────────────────────────────────────────────────────────────

DOCUMENT_ENTITY_TYPES: Tuple[DocumentEntityType, ...] = (
    DocumentEntityType(
        name="Person",
        noun_phrase="people",
        priority=9,
        registry_description="A human individual.",
        rule="**A human full name is `Person`** — never `Concept` or `Project` — even "
             "when cited as the author, originator, reviewer, or interviewee of an idea",
        examples=('"Ernesto van Peborgh" is a `Person`, not the name of a framework',),
    ),
    DocumentEntityType(
        name="Organization",
        noun_phrase="organizations",
        priority=7,
        registry_description="A named collective agent: company, nonprofit, DAO, team.",
        rule="**An institution, school, initiative, programme, fund, publisher, or lab "
             "is `Organization`** — never type a multi-word initiative as a `Person`. "
             "Prefer the **most complete surface form**",
        examples=("`Design School for Regenerating Earth`, not `Design School`",),
    ),
    DocumentEntityType(
        name="Project",
        noun_phrase="projects, ventures and named software products",
        priority=5,
        registry_description="A bounded named endeavour with scope and participants.",
        rule="**A named built initiative, site, venture, product, or piece of named "
             "consumer software is `Project`.** Software gets `Project` deliberately: "
             "there is no software/application type in this vocabulary yet, so an app "
             "or tool is a `Project` until that ontology decision is made. `Project` is "
             "NOT the bucket for written works or for gatherings — those have their own "
             "types below, and using `Project` for them is the specific defect this "
             "contract exists to stop",
        examples=("the website-blocking app `Freedom` is a `Project`",),
    ),
    DocumentEntityType(
        name="Concept",
        noun_phrase="abstract concepts and frameworks",
        priority=1,
        registry_description="An abstract idea, topic, method or framework.",
        rule="**An abstract idea, framework, method, practice, or named outcome is "
             "`Concept`.** When a named product shares its name with an abstract idea "
             "the document also discusses, they are TWO DIFFERENT ENTITIES: emit both, "
             "and give the product a disambiguating surface form so the two never "
             "collide",
        examples=("`Freedom (internet-blocking software)` as a `Project` alongside "
                  "`freedom` as a `Concept`",),
    ),
    DocumentEntityType(
        name="Location",
        noun_phrase="places",
        priority=4,
        registry_description="A geographic place: settlement, region, bioregion, venue.",
        rule="**A geographic place — region, city, country, bioregion, watershed, venue "
             "— is `Location`**",
    ),
    DocumentEntityType(
        name="Protocol",
        noun_phrase="protocols and standards",
        priority=3,
        registry_description="A specified, executable procedure or standard.",
        rule="**A named methodology, standard, or interoperability contract is "
             "`Protocol`**",
        examples=("`HTTPS`, `DNS`",),
    ),
    DocumentEntityType(
        name="CaseStudy",
        noun_phrase="prior-example case studies",
        priority=2,
        registry_description="A narrative account of a practice in context.",
        rule="**A prior real-world example or precedent site the document cites as a "
             "model is `CaseStudy`**",
        examples=("Auroville, Findhorn, SEKEM, Crystal Waters",),
    ),
    DocumentEntityType(
        name="Document",
        noun_phrase="named written works",
        priority=6,
        registry_description=(
            "A discrete written work ingested from a corpus: paper, guide, report, "
            "or article."
        ),
        rule="**A NAMED WRITTEN WORK is `Document`** — an essay, book, paper, report, "
             "article, newsletter post, catalogue, manifesto, or published talk that "
             "the text refers to by title. This is NOT `Project`: a titled piece of "
             "writing is a `Document` even when the author also treats it as a body of "
             "work. Use the title as it appears",
        examples=("`The End of Hoop Jumping`, `How To Think About Science`, "
                  "`Standing by Words`, `Whole Earth Catalog`",),
    ),
    DocumentEntityType(
        name="Event",
        noun_phrase="dated happenings",
        priority=8,
        registry_description=(
            "A dated happening that is not a Meeting: a conference, launch, incident, "
            "or milestone occurrence."
        ),
        rule="**A DATED HAPPENING is `Event`** — a conference, camp, summit, festival, "
             "workshop, gathering, retreat, launch, or other occurrence the text places "
             "in time. This is NOT `Organization` (the body that convenes it) and NOT "
             "`Project`. A private convened conversation with an attendee roster is a "
             "`Meeting`, which this vocabulary does not admit — type it `Event` only "
             "when it is a public, named, dated happening",
        examples=("`DWeb Berlin`, `DWeb Camp 2026: Root Systems`, `Burning Man` "
                  "when referred to as the gathering",),
    ),
)


# ── Derived views. Nothing below is written by hand. ─────────────────────────

DOCUMENT_ENTITY_TYPE_NAMES: Tuple[str, ...] = tuple(t.name for t in DOCUMENT_ENTITY_TYPES)

TYPE_PRIORITY: Dict[str, int] = {t.name: t.priority for t in DOCUMENT_ENTITY_TYPES}

BY_NAME: Dict[str, DocumentEntityType] = {t.name: t for t in DOCUMENT_ENTITY_TYPES}

#: Types the extractor may emit, as a set, for membership tests.
EXTRACTABLE_TYPES = frozenset(DOCUMENT_ENTITY_TYPE_NAMES)

#: Priority assigned to a type the contract does not admit. It is deliberately
#: BELOW every admitted type, so an off-contract label never wins a coercion — but
#: `priority_for()` reports the unknown-ness rather than letting it vanish into a
#: `.get(x, 0)`, which is what issue #68 requirement 4 means by "rather than
#: silently assigning priority zero".
UNKNOWN_TYPE_PRIORITY = 0


def priority_for(entity_type: Optional[str]) -> Tuple[int, bool]:
    """Return `(priority, is_known)` for a type name.

    The second element is the whole point. Callers that drop it reproduce the
    silent-zero behaviour this function replaces.
    """
    if entity_type in TYPE_PRIORITY:
        return TYPE_PRIORITY[entity_type], True
    return UNKNOWN_TYPE_PRIORITY, False


def is_extractable(entity_type: Optional[str]) -> bool:
    return entity_type in EXTRACTABLE_TYPES


# ── Renderers used by scripts/render_extraction_contract.py ──────────────────
#
# These produce the exact bytes that land in the prompts and schemas, so the
# generator has no formatting opinions of its own and `--check` can compare
# byte-for-byte.

def render_schema_enum(*, spaced: bool) -> str:
    """The JSON array literal for `entities[].type.enum`.

    `spaced` matches the .schema.json files' style; the prompts' inline appendix
    is compact. Both spellings are generated, never typed.
    """
    sep = ", " if spaced else ","
    return "[" + sep.join(f'"{n}"' for n in DOCUMENT_ENTITY_TYPE_NAMES) + "]"


def render_prompt_type_list() -> str:
    """The inline back-ticked list used in the prompt body and hard rules."""
    return ", ".join(f"`{n}`" for n in DOCUMENT_ENTITY_TYPE_NAMES)


def render_plain_type_list() -> str:
    """Unadorned comma list, for the HARD OUTPUT RULES block."""
    return ", ".join(DOCUMENT_ENTITY_TYPE_NAMES)


def render_canonicalization_block() -> str:
    """The whole `### Entities` rules region: the lead-in sentence naming what to
    extract, the admitted type list, and one canonicalization bullet per type.

    The lead-in is generated too. It used to be prose ("people, organizations,
    projects, concepts/frameworks, places, protocols, and prior-example case
    studies") — a seventh hand-maintained copy of the vocabulary, and one that
    would have stayed seven-typed while the enum below it said nine.
    """
    nouns = [t.noun_phrase for t in DOCUMENT_ENTITY_TYPES if t.noun_phrase]
    lead = _wrap(
        "Extract named, knowledge-worthy entities: "
        + ", ".join(nouns[:-1]) + ", and " + nouns[-1] + ".")
    head = _wrap(
        f"**Type each entity as exactly one of:** {render_prompt_type_list()}. "
        f"Canonicalization rules — follow these exactly; they prevent the most common "
        f"mis-types:")
    bullets = "\n".join(t.bullet() for t in DOCUMENT_ENTITY_TYPES)
    return lead + "\n\n" + head + "\n\n" + bullets


def render_hard_rule_block() -> str:
    """HARD OUTPUT RULES item 2 — the last line of defence in the prompt."""
    n = len(DOCUMENT_ENTITY_TYPE_NAMES)
    return (
        f"2. `entities[].type` MUST be exactly one of:\n"
        f"   {render_plain_type_list()}.\n"
        f"   There is no Practice/Pattern/Method/Evidence/Claim/Meeting option here — map\n"
        f"   to the closest of the {n} (a named method or practice is a `Concept`; a\n"
        f"   convened dated happening is an `Event`; a titled written work is a\n"
        f"   `Document`)."
    )
