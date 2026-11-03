---
ticket_id: mw-redirect-resolution
status: proposed
priority: medium
owner: unassigned
opened: 2026-04-15
---

# MediaWiki redirect resolution — sensor should store canonical content under redirect pageids

## Symptom

Bridge-note authors reading the local P2P Foundation wiki mirror (or querying
KOI via `unified_search` / `knowledge_search` for known-good page titles) hit
"empty" pages for redirect source titles. Examples surfaced during Tier 2
Wave 2:

- `Boundary spanner` → redirects to `Boundary Spanner` (case variant)
- `WIR`, `WIR Bank` → redirect to `WIR Economic Circle Cooperative`
- `Federation` → redirect to `Cross-Platform Federation of Internet Infrastructures`
- `Reputation-based Employment Marketplaces` → redirect to canonical capitalization

Agents typing the redirect-source title get a page with only `#REDIRECT [[Target]]`
in the chunks (and on disk), instead of the canonical target content. On the
live wiki a browser resolves this client-side via HTTP 302; locally we don't.

## Root cause

`api/mediawiki_api.py::MediaWikiClient.fetch_page_batch` fetches pages by
`pageid` via `action=query&prop=revisions&rvprop=content|ids|timestamp`. It
does **not** pass `redirects=1`, so the MediaWiki API returns the raw wikitext
of redirect pages (the `#REDIRECT [[Target]]` stub), not the resolved target
content.

`api/mediawiki_sensor.py:303–306` then detects `parsed.is_redirect`, calls
`register_redirect_alias(…)` (which adds the alias to the target's
`entity_registry.aliases` array **if the target entity already exists**), and
line 327 skips re-chunking/re-embedding the redirect page.

Net effect:

1. `mediawiki_page_state` has a row for the redirect page with no chunks.
2. Alias registration silently no-ops when the target is ingested AFTER the
   redirect (ordering hazard on first-time bulk import).
3. `knowledge_search` / `unified_search` queries for the redirect title match
   nothing, even though the target content is fully embedded.

## Proposed fix (option A — sensor follows redirects)

In `MediaWikiClient.fetch_page_batch`, pass `redirects=1`. The API will return
both the original page metadata AND a `redirects` block mapping redirect
titles → canonical titles, plus the canonical page's revision content under
the canonical pageid.

On the sensor side:

- If `parsed.is_redirect`, fetch the canonical target's parsed content
  (one extra API call with `redirects=1` on the target title, or reuse the
  canonical payload returned in the same batch).
- Store the canonical wikitext + chunks **under the redirect source pageid**
  (or: store a chunk-level pointer to the canonical pageid so chunks are
  shared, not duplicated).
- Continue to register the alias so entity lookups by either title resolve
  to the same `entity_registry` row.

Option A is the clean long-run fix.

## Proposed fix (option B — nightshift normalizer)

A nightly script walks `mediawiki_page_state WHERE is_redirect = true AND
chunk_count = 0`, fetches each target's content (one API call per redirect),
and re-links chunks. Simpler to ship, but creates a two-phase data model
(redirects are temporarily "empty" until the normalizer runs).

Prefer option A.

## Re-embedding cost estimate

Redirect pages are typically 5–15% of a MediaWiki corpus. For the
P2P Foundation wiki (40,239 pages, 96,775 chunks, embedded on one H200 GPU):

- Assume 10% = ~4,000 redirect pages.
- Canonical targets average 2–3 chunks each → ~10,000 new chunks.
- H200 embedding throughput ≈ 948 chunks/s (measured 2026-04-04).
- Incremental re-embed: **≈ 11 seconds of H200 time**.
- Full corpus re-embed (if the fix requires it): ≈ 100 seconds.

Re-embedding cost is NOT a blocker. The real cost is:

1. Sensor code change + test coverage (small).
2. Careful migration for the existing 40K-page corpus (one-time bulk pass
   to resolve all existing redirect pages; must be idempotent).
3. Verification that alias lookups work end-to-end after the change (a
   fuzz test over a sample of known redirect pairs).

## Out of scope (separate concern)

A local filesystem mirror of the wiki
is a pre-existing XML-dump archive (origin: `Jeff-Emmett/p2pfoundation-wiki`,
initial commit 7e6a512f). Its `#REDIRECT`-only `.mediawiki` files are
dump-level artifacts, not sensor output. Fixing redirect chains on the
filesystem mirror is a separate upstream task and doesn't depend on KOI.

## Acceptance criteria (sketch)

- `MediaWikiClient.fetch_page_batch` passes `redirects=1` and returns
  resolved canonical content for redirect-source pageids.
- Sensor pipeline: fetching a redirect page produces `chunk_count > 0` with
  the canonical target's content.
- `register_redirect_alias` handles the "target not yet ingested" ordering
  case (defer registration + retry on next poll, or resolve on lookup).
- Targeted query: `SELECT COUNT(*) FROM mediawiki_page_state WHERE
  is_redirect = true AND chunk_count = 0` returns 0 after the bulk migration.
- Fuzz test: 20 known redirect pairs from the P2P Foundation corpus all
  resolve to target content via `knowledge_search` on the redirect title.
