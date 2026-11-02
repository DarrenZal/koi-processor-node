# Deep Document Extraction — v2.0 Prompt (Phase 3: entities + facts + discourse)

You are analyzing a long-form document (whitepaper, paper, report, essay, or spec)
to extract structured knowledge for a knowledge graph. This is non-interactive:
return exactly one JSON object, nothing else.

You are seeing **window {WINDOW_INDEX} of {WINDOW_COUNT}** of the document. Extract
only what THIS window's text supports. The text is labeled with **global chunk
indices** `[N]` — use them verbatim (do not renumber from zero).

## OUTPUT

Return ONE JSON object matching the schema in the APPENDIX. NO prose. NO markdown
fences. NO explanation. The entire response must be valid JSON starting with `{`
and ending with `}`. If unsure about a field, emit `null` rather than omitting it.
Every required field in the schema must appear.

## EXTRACTION RULES (per layer)

### Entities (identity layer)

<!-- generated:entity-types:begin — from api/document_extraction_contract.py; run scripts/render_extraction_contract.py -->
Extract named, knowledge-worthy entities: people, organizations, projects, ventures and
named software products, abstract concepts and frameworks, places, protocols and
standards, prior-example case studies, named written works, and dated happenings.

**Type each entity as exactly one of:** `Person`, `Organization`, `Project`, `Concept`,
`Location`, `Protocol`, `CaseStudy`, `Document`, `Event`. Canonicalization rules —
follow these exactly; they prevent the most common mis-types:

- **A human full name is `Person`** — never `Concept` or `Project` — even when cited as
  the author, originator, reviewer, or interviewee of an idea (e.g. "Ernesto van
  Peborgh" is a `Person`, not the name of a framework)
- **An institution, school, initiative, programme, fund, publisher, or lab is
  `Organization`** — never type a multi-word initiative as a `Person`. Prefer the **most
  complete surface form** (e.g. `Design School for Regenerating Earth`, not `Design
  School`)
- **A named built initiative, site, venture, product, or piece of named consumer
  software is `Project`.** Software gets `Project` deliberately: there is no
  software/application type in this vocabulary yet, so an app or tool is a `Project`
  until that ontology decision is made. `Project` is NOT the bucket for written works or
  for gatherings — those have their own types below, and using `Project` for them is the
  specific defect this contract exists to stop (e.g. the website-blocking app `Freedom`
  is a `Project`)
- **An abstract idea, framework, method, practice, or named outcome is `Concept`.** When
  a named product shares its name with an abstract idea the document also discusses,
  they are TWO DIFFERENT ENTITIES: emit both, and give the product a disambiguating
  surface form so the two never collide (e.g. `Freedom (internet-blocking software)` as
  a `Project` alongside `freedom` as a `Concept`)
- **A geographic place — region, city, country, bioregion, watershed, venue — is
  `Location`**
- **A named methodology, standard, or interoperability contract is `Protocol`** (e.g.
  `HTTPS`, `DNS`)
- **A prior real-world example or precedent site the document cites as a model is
  `CaseStudy`** (e.g. Auroville, Findhorn, SEKEM, Crystal Waters)
- **A NAMED WRITTEN WORK is `Document`** — an essay, book, paper, report, article,
  newsletter post, catalogue, manifesto, or published talk that the text refers to by
  title. This is NOT `Project`: a titled piece of writing is a `Document` even when the
  author also treats it as a body of work. Use the title as it appears (e.g. `The End of
  Hoop Jumping`, `How To Think About Science`, `Standing by Words`, `Whole Earth
  Catalog`)
- **A DATED HAPPENING is `Event`** — a conference, camp, summit, festival, workshop,
  gathering, retreat, launch, or other occurrence the text places in time. This is NOT
  `Organization` (the body that convenes it) and NOT `Project`. A private convened
  conversation with an attendee roster is a `Meeting`, which this vocabulary does not
  admit — type it `Event` only when it is a public, named, dated happening (e.g. `DWeb
  Berlin`, `DWeb Camp 2026: Root Systems`, `Burning Man` when referred to as the
  gathering)
<!-- generated:entity-types:end — from api/document_extraction_contract.py; run scripts/render_extraction_contract.py -->

`first_seen_chunk` is the lowest global `[N]` where the entity appears in this
window. `mention_count` is the count within this window (≥1).

**Do NOT extract:** generic placeholders ("the community", "the region", "the
authors"), section headers, page numbers, or figure labels.

### Facts (semantic layer)

Extract factual triples in `(subject, predicate, object)` form grounded in this
window's text.

- `subject` and `object` MUST be strings that appear in this window's `entities`
  list — OR `object` is null and the value goes in `object_literal` (for
  literal-valued predicates).
- `predicate` is UPPER_SNAKE_CASE.
  - **Entity predicates** (object is an entity): `AUTHORED_BY`, `PUBLISHED_BY`,
    `FOUNDED_BY`, `PARTNERS_WITH`, `MEMBER_OF`, `LOCATED_IN`, `PART_OF`,
    `INSTANCE_OF`, `DEFINES`, `PROPOSES`, `EXEMPLIFIED_BY`, `DERIVES_FROM`,
    `SUPPORTS`, `CONTRASTS_WITH`, `COLLABORATES_WITH`, `RELATES_TO`.
  - **Literal predicates** (use `object_literal`, set `object` = null):
    `HAS_QUANTITY`, `HAS_TARGET`, `HAS_METRIC`, `HAS_TIMEFRAME`, `HAS_LOCATION`,
    `HAS_DATE`, `HAS_COUNT`, `HAS_STATUS`, `HAS_DESCRIPTION`, `HAS_URL`.
- For entity predicates: set `object` (string), `object_literal` = null.
  For literal predicates: set `object` = null, `object_literal` = the value.
- **Emit quantified claims as facts** — numbers, percentages, dollar amounts,
  counts, hectares, dates — using a literal predicate with the number/string in
  `object_literal` (e.g. subject="BioHubs research base", predicate="HAS_COUNT",
  object_literal="152 initiatives across 44 countries").
- `fact_text` is a single-sentence natural-language restatement.
- `chunk_range` = `[first_chunk_with_evidence, last_chunk_with_evidence]` (global
  indices, both integers). It MUST ALWAYS contain EXACTLY TWO integers. When the
  evidence sits in a single chunk, repeat the index: `[26, 26]` — NEVER `[26]`.
- `confidence`: `"high"` if explicitly stated, `"medium"` if strongly implied.
  Do not emit low-confidence facts. Skip vague/obvious facts.

### Discourse (argument layer) — NEW in v2

Extract the document's **argument structure** — the load-bearing moves that make up
its reasoning, NOT a summary. Capture its theses and claims, the evidence and premises
that support them, the counterpoints it acknowledges, the questions it leaves open, the
terms it defines, and the implications it draws. A substantial whitepaper window
typically yields **2–6 moves**; do not pad, do not invent moves the window's text does
not support.

**Type each move as exactly one of these 8 `move_type`s** — the document's ARGUMENT
structure:

- `thesis` — the document's central position or overarching argument.
- `claim` — a specific assertion or proposition the document advances.
- `evidence` — data, an example, a case study, or a finding that backs a claim (e.g.
  "152 initiatives across 44 countries"). Set `supports` to the claim/thesis it backs.
- `premise` — a foundational assumption the argument rests on. Set `supports` to the
  claim/thesis it underpins.
- `counterpoint` — a tension, objection, limitation, or competing consideration the
  document acknowledges. Set `supports` to the claim/thesis it qualifies or contests.
- `open_question` — an unresolved question the document raises but does not answer.
- `definition` — a key term, framework, or concept the document defines (e.g. "defines
  a BioHub as a physically-anchored project…").
- `implication` — a consequence, recommendation, or call to action the document draws.

Fields per move:
- `title`: a single declarative sentence stating the move (for a `claim`/`thesis`, the
  claim itself); keep it to one sentence and put any elaboration in `detail`.
- `detail`: 1–3 sentences of substance, or `null`.
- `status`: one of `asserted` (stated as fact), `supported` (a claim backed by
  evidence), `contested` (marked as debated), `speculative` (a tentative implication),
  `open` (an unresolved open_question/counterpoint), `deferred` (pushed to future work),
  or `null`. Per-type guidance: thesis→asserted; premise→asserted|contested;
  evidence→asserted; claim→asserted|supported|contested; counterpoint→open|contested;
  open_question→open|deferred; definition→asserted; implication→speculative|asserted.
- `supports`: for `premise`/`evidence`/`counterpoint` (any move that backs or contests
  another), the EXACT `title` of the `claim`/`thesis` it supports — this builds the
  argument edge. `null` for standalone moves.
- `chunk_range`: `[first, last]` global chunk index supporting the move. ALWAYS
  exactly two integers; for a single chunk repeat it, e.g. `[26, 26]`, never `[26]`.

Do NOT duplicate a fact as a discourse move — facts are atomic triples; moves are
argument steps. A move may summarize several facts.

### Document object

- `name`: the document's title (or best inference for this window).
- `summary`: 1–3 sentences, present tense, no "this window".
- `doc_kind`: one of `whitepaper`, `paper`, `report`, `essay`, `spec`, `book`, `manual`, `textbook`, `handbook`, `other`. This value MUST be copied exactly from that list — never invent a different word.
- `chunk_span`: `[first, last]` global chunk index this window covers.

## APPENDIX: Schema

```json
{
  "type": "object",
  "required": ["document", "entities", "facts", "discourse"],
  "additionalProperties": false,
  "properties": {
    "document": {
      "type": "object",
      "required": ["name", "summary", "doc_kind", "chunk_span"],
      "properties": {
        "name": {"type": "string", "minLength": 1, "maxLength": 200},
        "summary": {"type": "string"},
        "doc_kind": {"enum": ["whitepaper","paper","report","essay","spec","book","manual","textbook","handbook","other"]},
        "chunk_span": {"type": "array", "items": {"type": "integer", "minimum": 0}, "minItems": 2, "maxItems": 2}
      }
    },
    "entities": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name","type","first_seen_chunk","mention_count"],
        "properties": {
          "name": {"type": "string"},
          "type": {"enum": ["Person","Organization","Project","Concept","Location","Protocol","CaseStudy","Document","Event"]},
          "first_seen_chunk": {"type": "integer", "minimum": 0},
          "mention_count": {"type": "integer", "minimum": 1}
        }
      }
    },
    "facts": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["subject","predicate","object","object_literal","fact_text","chunk_range","confidence"],
        "properties": {
          "subject": {"type": "string"},
          "predicate": {"type": "string", "pattern": "^[A-Z][A-Z0-9_]*$"},
          "object": {"type": ["string","null"]},
          "object_literal": {"type": ["string","null"]},
          "fact_text": {"type": "string"},
          "chunk_range": {"type": "array", "items": {"type": "integer", "minimum": 0}, "minItems": 2, "maxItems": 2},
          "confidence": {"enum": ["high","medium"]}
        }
      }
    },
    "discourse": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["move_type","title","detail","status","supports","chunk_range"],
        "properties": {
          "move_type": {"type": "string", "minLength": 1},
          "title": {"type": "string", "minLength": 1},
          "detail": {"type": ["string","null"]},
          "status": {"type": ["string","null"]},
          "supports": {"type": ["string","null"]},
          "chunk_range": {"type": "array", "items": {"type": "integer", "minimum": 0}, "minItems": 2, "maxItems": 2}
        }
      }
    }
  }
}
```

## DOCUMENT WINDOW (chunks labeled [N] with global indices)

<!-- The pipeline appends the concatenated window chunks here at call time -->

## HARD OUTPUT RULES (a violation aborts the ENTIRE document, not just this window)

1. Emit ONLY the keys named in the schema. Do NOT add any extra key — not
   `confidence`, not `definition`, not `notes` — to a `discourse` or `entities` item.
   If you want to record something there is no key for, fold it into an existing
   free-text field instead.
<!-- generated:entity-type-hard-rule:begin — from api/document_extraction_contract.py; run scripts/render_extraction_contract.py -->
2. `entities[].type` MUST be exactly one of:
   Person, Organization, Project, Concept, Location, Protocol, CaseStudy, Document, Event.
   There is no Practice/Pattern/Method/Evidence/Claim/Meeting option here — map
   to the closest of the 9 (a named method or practice is a `Concept`; a
   convened dated happening is an `Event`; a titled written work is a
   `Document`).
<!-- generated:entity-type-hard-rule:end — from api/document_extraction_contract.py; run scripts/render_extraction_contract.py -->
3. `facts[].confidence` MUST be one of: high, medium, low.
4. `document.doc_kind` MUST be copied exactly from its enum — never invent a word.
5. `chunk_range` is ALWAYS exactly two integers; for a single chunk repeat it: [26, 26].
