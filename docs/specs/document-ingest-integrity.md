# Document-ingest integrity: payload identity and extraction provenance

Issue numbers below refer to the original issue tracker: #62 (payload identity), #61
(normalizer drift), #64 (provenance and quality), coordinated with #53 (resolver tuning).

Status: implemented, 2026-09-13. Two independent gates, deliberately not merged into one.

---

## 1. What was actually wrong

### 1.1 Identity depended on fact order (#62)

`POST /knowledge/episodes` resolved every fact endpoint by NAME at write time, and
created missing entities as it went. So an entity minted while writing fact 45
became a fuzzy candidate for a *different* endpoint at fact 47.

On the audited Buehler (2024) import: 115 distinct typed endpoints, 213 facts, 108
exact + 7 fuzzy resolutions — **6 of the 7 fuzzy resolutions were wrong
self-collapses**, leaving 11 persisted fact endpoints bound to the wrong entity. The
write returned 201 and the structural gate passed.

**Measured 2026-09-13: all four reported collapses still reproduce, unchanged**, with
Jaro-Winkler scores matching the issue's table to four decimals:

| requested | collapsed onto | JW | Concept fuzzy threshold (`similarity_threshold`) |
|---|---|---:|---:|
| `Anthropic Claude 3 Opus` | `Anthropic Claude 3 Sonnet` | 0.9411 | 0.75 |
| `Buehler 2024 fitted exponential degree model` | `…fitted power-law degree model` | 0.9168 | 0.75 |
| `Buehler 2024 adversarial-X-LoRA generated graph` | `…adversarial-X-LoRA augmented graph` | 0.9608 | 0.75 |
| `Buehler 2024 global-graph modularity score` | `…global-graph community structure` | 0.9393 | 0.75 |

The four scores are compared against Concept's Jaro-Winkler `similarity_threshold`
of **0.75** (`api/entity_schema.py`; applied in `api/personal_ingest_api.py` as
`threshold = schema.similarity_threshold` → `if score >= threshold`); 0.88 is
`semantic_threshold`, the separate embedding-cosine threshold used at Tier 2, which an
earlier version of this table mislabelled as the fuzzy cut.

They survive the current strict guards because `passes_distinctive_token_check`
rejects **disjoint** distinctive-token sets, and these pairs are not disjoint: they
share most tokens and differ in exactly the one token that distinguishes them.

**A correction to the obvious hypothesis.** The 100-fact batch boundary is *not* the
mechanism. Measured against the real payload, 3 of the 4 pairs fall in the *same*
batch. The collapse happens inside a single request, through sequential fact
processing. Batching amplifies it across requests but is not required — which is why
the fix is per-endpoint pinning and not a batching change.

### 1.2 Normalizer drift split identities (#61)

Entity identity keys on the persisted `entity_registry.normalized_text`, but the
normalizer changed without a version, a migration, or a compatibility read.
Re-measured 2026-09-13 on 36,484 rows:

- **1,268 rows** (1,261 live) disagree with the current normalizer;
- **44 pre-existing duplicate sets** (93 live rows) already share a stored value;
- a recompute would create **125 further collision groups** (254 rows).

`GPT-4` is the instance the issue reports, and it was repaired by merge log 324.
**`GPT-4o`, `GPT-4.1` and `gpt-5.4` are three more live instances today** — each has
an active row that an `exact_only: true` registration would miss and duplicate. The
class was repaired instance-by-instance, never fixed.

### 1.3 Provenance was wrong, not merely thin (#64)

`document_window_extractions.route_used` is written from a module-level constant
derived from the *configured* transport, while `_extract_window()` degrades through
`DOC_EXTRACTOR_TRANSPORT_FALLBACK` and returned only the completion text. With a
fallback configured, a window served by the fallback was **recorded as having used
the primary**. Separately, `_call_openai()` ignores its `model` argument and uses
`OPENAI_MODEL`, while the caller logged `ANTHROPIC_MODEL`.

### 1.4 A green gate said nothing about quality (#64)

The gate's own catalog states it: *"green = every stage ran and met its structural
floor, NOT the extracted knowledge is good."* The original Buehler run passed strict
with 25 facts; a curated re-run of the same document produced 213. Both "complete".

### 1.5 Found while fixing the above: extraction confidence was discarded

The extraction schema **requires** `confidence` on every fact, `FactInput` accepts
it, and migration 115 added `knowledge_facts.confidence` — but
`facts_to_episode_payload()` never forwarded it. Measured: of **30,474** live
document-source facts, **213 carry a confidence value**, and those 213 are the
Buehler curated repair, which POSTed it explicitly. Every fact the normal pipeline
has written has `confidence = NULL`. Fixed here; all 32,753 cached facts were
verified to carry a valid enum value first, so the change cannot 422 a resume.

---

## 2. The identity contract (#62)

### The seam

`merge_extractions()` produces the complete payload — every window extracted,
nothing written — and only then does the first fact POST happen. So the importer
*does* know the complete typed endpoint set before it writes. Everything below sits
in that gap.

```
collect_endpoints(payload)        1. distinct (current-normalized label, type) set
preflight_endpoints(conn, eps)    2. classify each against the live graph   [BLOCKS]
preregister_missing(http, report) 3. exact-only + force_type create the missing
freeze_endpoint_map(conn, ...)    4. pin endpoint -> URI, assert bijection  [BLOCKS]
   ... facts are written carrying their pinned URIs ...
verify_persisted_graph(conn, ...) 5. every persisted endpoint is in the map [BLOCKS]
```

Step 5 runs **before** relational/discourse finalization, so a failed verification
never leaves a document that reads as finished.

### Why pinning rather than a higher threshold

Raising a threshold trades one arbitrary cut for another and leaves the import
contract implicit. Pinning makes the property **structural**: once an endpoint is
bound to a URI, no resolver tier runs for it, so no threshold can be wrong. #53's
tuning remains worth doing for every other caller; it is not what makes this path
safe.

`FactInput` gains `subject_uri` / `object_uri`. When set, `_bind_pinned_uri()` binds
verbatim after a liveness + type check, and:

- it **never** falls back to name resolution — an unhonourable pin is a 422 for the
  whole request. Falling back would mean the caller asked for a determinate binding,
  silently got a probabilistic one, and got a 201 saying it worked;
- it does **not** follow `merged_into`. Tombstone-following is right for a *name*; a
  caller that pinned a URI is asserting an identity. If it was merged away since the
  freeze, the map is stale and the caller must re-freeze.

The payload also sets `create_entities: false`. Every endpoint was pre-registered, so
nothing legitimate remains to create; if the server wants to create one anyway,
something is referenced that the freeze did not cover.

### The escape hatch, and the only one

`DOC_IDENTITY_ALIAS_DECISIONS` points at a flat `{label: uri}` JSON file. An audited
alias decision is the sole thing that clears an `alias_only` block and the sole way
two distinct payload labels may share one URI (#62 requirement 6). It is recorded in
the run's evidence.

### A hazard the dual read creates

Widening the exact lookup to match **either** the stored `normalized_text` **or** the
current normalization means a drifted row answers to two keys at once — so two
distinct payload endpoints can reach one row. Without the bijection assertion,
fixing #61's false *split* would have introduced a false *merge*. That case is
covered by `test_two_endpoints_reaching_one_row_via_drift_raise`.

### Modes

`DOC_IDENTITY_MODE=strict` (**default**) | `warn` | `off`. An unrecognised value
refuses to start rather than silently meaning `off`. `warn` proceeds **unpinned** and
stamps `warn_proceeded_unpinned` in the evidence — it is honest about being the
pre-#62 behaviour, and exists for a bounded backlog where a pre-existing graph-wide
duplicate set would otherwise block every document over a defect unrelated to it.

---

## 3. Normalizer compatibility (#61) — migration 124

Additive only: an `IMMUTABLE` SQL mirror `koi_normalize_entity_text()`, two
expression indexes, and a read-only view `entity_current_norm_duplicates`
(`drift_created` distinguishes pre-existing duplicate sets from drift-created ones).

**It deliberately does not convert `normalized_text` to a GENERATED column or
backfill it.** `migrations/DRAFT_normalized_text_generated.sql` analysed that, and
re-measurement confirms it is a live-behaviour change: 125 currently-distinct live
groups would collapse onto shared keys that `… LIMIT 1` with no `ORDER BY` would
arbitrate nondeterministically. Those need an operator merge pass. This closes the
**read** side only.

Known divergence, inherited and bounded: this cluster has `lc_ctype='C'`, so SQL
`lower()` folds ASCII only while Python's is full Unicode. The index is a fast
**superset filter**; the preflight confirms in Python.

---

## 4. Provenance and quality (#64) — migration 125

**Provenance.** `_extract_window()` now returns a producer receipt captured at the
call site *after* any fallback. `document_window_extractions` gains `provider`,
`model`, `transport`, `producer`; `route_used` is written from the actual transport.
A failed window records its producer as `intended_only: true` — there is no accepted
output to attribute.

Two things about the existing column, both measured rather than assumed:

- **`route_used` is not a three-value column.** It holds **9 distinct live values**
  (`claude_p_cli` 844, `openai_compat` 706, `anthropic_api` 498, `claude_p_primary`
  227, `agent_window_json` 77, `openai_fallback` 22, `claude_p_fallback` 17,
  `session_parallel` 11, NULL 11). The current code's vocabulary decodes ~64% of
  rows. That is why the fix adds `provider`/`model`/`transport` as their own columns
  rather than enriching a string whose history is already mixed.
- **This problem was solved once and lost.** An earlier, unmerged branch returned the
  executed route as a second tuple element and is where `claude_p_primary` /
  `claude_p_fallback` / `openai_fallback` came from. So "records the configured rather
  than the executed transport" is a **regression**, not an oversight. Also:
  `session_parallel` (11 rows) is written by something outside this file — no string in
  this repository matches it.

**Also fixed here, because it is in the same call path and defeats the per-window
isolation the file promises:** `raise_for_status()` raises `httpx.HTTPStatusError`,
which is absent from both transports' `except` tuples and is not an
`ExtractionError` — so a non-retryable provider status (400/401/403/413/422) escaped
the transport fallback, the repair loop **and** the per-window dead-letter, aborting
the whole document and discarding every window already extracted. It is now
re-raised as `extract_http_error` (which is in `TRANSPORT_FALLBACK_REASONS`, so a
transport-specific rejection can still try the next transport). Covered by
`test_a_provider_4xx_becomes_a_dead_letterable_extraction_error`, which was
mutation-checked: deleting the new arm makes it fail with a raw `HTTPStatusError`.

**Quality.** `document_extraction_runs` holds one row per (document, run) with
**two independent statuses**: `structural_status` and `semantic_status`. Collapsing
them into one is how the distinction gets lost again. `semantic_status` defaults to
`not_evaluated`, which is **not** a pass. A waiver records a decision alongside the
verdict and never overwrites it; a partial waiver is rejected by a CHECK constraint
(demonstrated failing at apply time).

`api/extraction_quality.py` is deterministic and calls no model — a model grading
another model's output would make the measurement depend on the thing measured.

**No dimension is a raw count** (#64 requirement 5). Every one is a ratio whose
denominator is a property of the source document, and the two load-bearing ones get
*worse* when output is padded. Demonstrated: padding an honest 3-fact extraction with
20 invented facts drives `citation_support` 1.0 → 0.13 and `predicate_concentration`
0.33 → 0.91 (it *rises*, and higher is worse). Every citation-reading dimension reads
the per-window `chunk_ranges` a fact actually cited, not the span widened across
windows (`extraction-quality-v2`, 2026-09-14).

| dimension | what it divides by | catches |
|---|---|---|
| `window_coverage` | the document's own windows | no fact at all from a whole section |
| **`chunk_coverage`** | the document's own chunks | a shallow read — **the load-bearing one** |
| `citation_in_range` | facts | fabricated chunk citations |
| `citation_support` | facts | endpoint language absent from the cited span |
| `endpoint_integrity` | — (carried from #62) | wrong-entity bindings |
| `predicate_concentration` | facts (**higher is worse**) | one relation repeated for volume |
| `discourse_concentration` | moves (thorough; **higher is worse**; unmeasured → review under 5 moves) | one move type repeated for volume |

### A design error this layer had, found by running it on the real fixture

The first version of this module **scored the two real Buehler runs backwards**: the
thin 25-fact original **passed** and the curated 213-fact re-run went to **review** —
the exact inversion of the criterion it exists to enforce. Its unit tests were green,
because I had written fixtures that matched my assumptions.

Two causes, both design rather than threshold:

1. **`fact_diversity` was "distinct predicates / facts".** Any distinct-over-total
   ratio *falls* as a thorough extraction legitimately adds facts, so it penalised
   precisely the behaviour it was meant to reward: 0.48 for the thin run, 0.108 for
   the curated one. It is replaced by **`predicate_concentration`** — the share held
   by the single most common predicate, where high is the bad news. Scale-free: thin
   0.40, curated 0.49, both far from a degenerate dump.
2. **`window_coverage` could not see depth.** Both runs scored 1.0, because the thin
   run touched all six windows — just shallowly. "Read every section" and "read every
   section properly" are different claims. **`chunk_coverage`** is the second one, and
   it separates cleanly: **0.2131 (13/61 chunks) vs 0.9344 (57/61)**.

Padding still cannot buy `chunk_coverage`: twenty invented facts all citing chunk 0
add one chunk to the numerator, exactly as one fact would, and the denominator is the
document.

Thresholds are calibrated on these two runs and stated as such (`chunk_coverage`
fails below 0.40, reviews below 0.75). `test_a_thorough_extraction_is_not_penalised_
for_being_thorough` is the regression that encodes the inversion.

**Verified end-to-end against the live repaired graph:**

| | facts | `chunk_coverage` | verdict |
|---|---:|---:|---|
| original stored windows | 25 | 0.2131 | **FAIL** |
| curated repaired payload | 213 | 0.9344 | **PASS** |

Thresholds are **starting values**, chosen against the two Buehler runs and stated as
such. #64 requirement 6 asks for a labelled audit set and shadow evaluation before
automatic promotion; until that exists this layer reports and routes to review rather
than arbitrating promotion.

---

## 5. Gate evidence

New keys on `--gate-evidence-out`. Booleans are `1 = ok` because the gate evaluator
supports only `>=`:

`identity_verified`, `identity_mode`, `identity_endpoints`, `identity_distinct_uris`,
`identity_bijective`, `endpoints_pinned`, `producers_recorded`, `providers`,
`models`, `transports`, `semantic_status`, `semantic_ok`.

`identity_verified` is 0 for warn mode, off mode and `rag` tier — **"did not verify"
must not read the same as "verified clean"**. Both operands of the cardinality check
are emitted so a reader can see *which* side shrank.

### Follow-up: an external completion gate

A completion gate that reads `--gate-evidence-out` from outside this repository can add
the floors below, but only after every ingestion path it checks runs this code: a gate
that fails a floor whose key is absent would otherwise fail every run made by an older
checkout. The primary enforcement meanwhile is **in-repo** — the ingest itself refuses
to finalize — which is where it belongs anyway.

The catalog floors, for that later step:

```yaml
  standard:
    floors:
      identity_verified: 1       # payload endpoints pinned AND verified post-write
      identity_bijective: 1      # N distinct endpoints -> N distinct identities
      producers_recorded: 1      # every window names its actual provider/model
  thorough:
    floors:
      identity_verified: 1
      identity_bijective: 1
      producers_recorded: 1
```

`semantic_ok` is intentionally **not** proposed as a floor yet — see the thresholds
note in §4.

---

## 6. Tests

`tests/test_ingest_identity.py`. Isolation is one asyncpg connection in a
rolled-back transaction plus in-process ASGI; nothing talks to `localhost:8351`,
because a test that posts over HTTP writes through a separate process holding its own
pool against the **live** database and no environment variable can redirect that.

Load-bearing tests:

- `TestTheDefectIsReal` — the positive control for the whole change. Without it,
  every test below could pass against a system that never had the bug.
- `test_unpinned_siblings_collapse_control` — drives the **real endpoint** and
  asserts the siblings *do* collapse unpinned. If this stops collapsing, the pinning
  tests prove nothing and it fails loudly rather than staying green.
- `test_pinned_siblings_stay_distinct` — parametrized over all four reported pairs.
- `test_reversed_fact_order_produces_the_same_graph`, `test_more_than_one_batch…`
  (120 endpoints, >1 batch), `test_rerun_creates_no_additional_entities_or_facts`.
- `test_finds_a_row_whose_stored_normalization_is_legacy` **paired with**
  `test_control_the_stock_exact_lookup_misses_that_row`.
- `test_a_thin_extraction_passes_structurally_but_fails_semantically`,
  `test_padding_with_invented_facts_makes_the_verdict_worse`.

Two defects in this change were found by these tests, not by re-reading the diff: a
cross-type advisory that could **never fire** (the candidate query was type-filtered,
so the check's silence read as a clean result), and a type-conflict check defeated by
dict last-write-wins.

Both migrations were dry-run inside a rolled-back transaction **and** given negative
controls that break them by deletion — a deleted normalizer step (exit 3, naming
every case) and a deleted CHECK constraint (exit 3, "the waiver-attribution
constraint did NOT reject a partial waiver"). The first two attempts at those
controls failed on a syntax error rather than the assertion, which proved nothing,
and were redone.

---

## Review fixes (2026-09-14, independent review)

Behaviour changes made in response to the independent review, one per finding.
Tests for each live beside the code they cover (`tests/test_ingest_identity.py`,
`tests/test_extraction_quality.py`, `tests/test_check_document_integrity.py`,
`tests/test_migration_126.py`).

- **B1** — `preregister_missing` resolved an ABSOLUTE registration URL (the `base_url`
  argument, an absolute `post_path`, or the client's own base URL) BEFORE any request,
  and refused with `IdentityError` when none was available; the extractor passes
  `base_url=KOI_BASE_URL`. Previously a relative path on a hostless client raised
  `httpx.UnsupportedProtocol` on every first ingest.
- **B2** — cross-type conflict detection became per ENDPOINT: the same-label any-type
  read is no longer filtered by the payload-wide type set, and an alias decision is
  accepted at preflight only for a live row of the endpoint's own type.
- **B3** — `FrozenMap.uri_for` / `type_for` were changed to resolve by the canonical
  normalized key, so every raw spelling of a bound endpoint resolves;
  `identity_map_incomplete` can no longer fire for spelling variants after
  preregistration.
- **M3** — an untyped fact endpoint (absent from `entities[]`, `type_map` and
  `type_decisions`) was given `entity_type=None` and preflights to the new BLOCKING
  state `type_undeclared`, with the graph's same-label rows (any type) as evidence;
  only an audited type decision types it (`--type-decisions` /
  `DOC_IDENTITY_TYPE_DECISIONS`). Nothing defaults to `Concept` any more.
- **M4** — the freeze validates an alias decision's target: it must exist, be live (not
  merged or revoked), be of the endpoint's type, and agree with `expected_uris`; new
  blocker states `alias_decision_not_live` and `alias_decision_type_mismatch`.
- **M5** — `EpisodeCreateResponse` gained `fact_ids`, listing the rows the request
  inserted; `verify_persisted_graph(..., fact_ids=)` now verifies exactly those, and
  the extractor refuses (`identity_verification_unscoped`) if a server returns none.
  Episodes are shared by (`source_document`, `group_id`).
- **M6** — the cross-window merge moved to `api/extraction_merge.py`, keyed on
  `normalize_entity_text` (the identity key); the extractor re-exports it; facts carry
  `chunk_ranges` (per-window citations) beside the widened `chunk_range`.
  **Scope of the key change (re-review follow-up):** the entity key is NOT the
  discourse-move id key. `write_discourse_moves` hashes the title into a uuid5 id and
  upserts `ON CONFLICT (id)`, and every stored row was derived with lower + strip +
  collapse-whitespace only. Rebinding `_norm` had silently changed that hash input
  (5,578 of 13,767 stored document moves on 1,169 documents would have stopped
  matching their own id; a replay would insert a twin beside each). The extractor
  now derives move ids through a dedicated `discourse_move_id_key` /
  `discourse_move_id` with the historical whitespace-only behaviour, pinned by
  literal UUIDs in `tests/test_discourse_move_ids.py`; merging and identity keep
  `normalize_entity_text`.
- **M7** — `scripts/check_document_integrity.py` runs the production merge over the
  stored windows; evaluates the bijection on the resolvable endpoints even when some
  are missing; reports would-be-minted endpoints WITH the type they would be minted
  as; counts blockers before truncating; and refuses a `--payload` whose
  `curation.document_rid` differs from `--document-rid`.
  `scripts/curate_cached_payload.py` emits the production merge.
- **M1/M2** (quality) — `chunk_ranges` are used for coverage and citation; the
  window-band fallback was fixed; `discourse_variety` was replaced by
  `discourse_concentration` (higher is worse; `None` → review under 5 moves);
  `QUALITY_CONTRACT_VERSION` = `extraction-quality-v2`; `ingest_document.py` reads
  `ext["quality"]` so `semantic_status` reaches gate evidence.
- **M8** — migration 126 became transactional (in-file `BEGIN;`/`COMMIT;`), records a
  `koi_migrations` ledger row (`personal:126_document_extraction_type_registry`), and
  asserts the extractable set EXACTLY (the count check accepted 'Meeting' extractable
  plus 'CaseStudy' missing as 9). Its down file mirrors this for the legacy seven.
  Dry-run and both negative controls: `tests/test_migration_126.py`.
- **B4** (runbook, no code) — deploy the change to EVERY checkout that serves the API or
  runs ingestion, restart the API and check `/openapi.json` BEFORE any pinned replay.
  Curated payloads are validation artifacts for the read-only gate only, not replay
  inputs — no replay path consumes them until sanctioned reconciliation is defined.
