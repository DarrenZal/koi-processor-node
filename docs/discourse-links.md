# Cross-source discourse links (Stage 1)

## What it does

A thorough-tier ingestion can queue a job that looks for claims in *other* sources resembling each of the new source's discourse moves. It uses keyword and local-embedding search fused by rank (`hybrid-rrf/v0`). It records the matches as **candidates**.

- A candidate is a proposal to inspect. Similarity establishes no identity, support, contradiction or independent corroboration.
- A candidate becomes a relation only through an explicit, authorized judgment. Judgments are disabled outside scratch databases unless `KOI_DISCOURSE_JUDGMENTS=1`, and the review router is not mounted.
- Every candidate keeps both ends' sources (rid, title, URL), both claims' exact text (by representation hash), and how each claim is bound to its source version.
- A candidate is admissible only when both ends carry an extraction binding recorded with the extraction input. Claims extracted before bindings existed never become admissible retrospectively.

## Ingest a source with discovery

```bash
cd <koi-processor checkout> && set -a && source config/personal.env && set +a
venv/bin/python scripts/ingest_document.py --source-path <markdown> --tier thorough --discourse-discovery \
  --source-url <canonical url> --name "<title>" --gate-evidence-out <gate.json>
```

- **The option:** `--discourse-discovery` requires `--tier thorough`. It sets `KOI_DISCOURSE_BINDINGS` and `KOI_DISCOURSE_INGEST_DISCOVERY` for that process only.
- **What it adds:** the extraction records each move's binding to the source version it read, and, once the extraction is complete, inserts one discovery job. The summary prints `discovery job: {...}`. Retrieval never runs inside the ingestion.
- **Without the option:** ingestion is exactly as before. No binding is recorded and no job is queued.
- **Never queued:** sources of excluded corpora (`EXCLUDED_SOURCE_PATTERNS`, the held wiki material) and moves without a binding captured with their input.
- **Held material:** do not ingest it with this option.

### Per-claim limit (optional)

`--discourse-move-cap 10` (with `--discourse-discovery`) keeps up to 10 candidates per new claim instead of 5. The only values are 5 and 10 (`PER_MOVE_CAPS`). The limit of 2 per other source does not change.

- **Without the option:** the job is exactly what it was before. Same profile, same `job_key`, 5 per claim. An inherited `KOI_DISCOURSE_DISCOVERY_PER_MOVE_CAP` is cleared, so only the option chooses.
- **Where it is recorded:** in the job's own `profile.per_move_cap` (so in its `job_key`), the plan's `method_params`, and the run's `approval_scope`. The approved write budget is claims × limit.
- **What never changes:** the module `PROFILE`, and any job already queued. A job keeps its own copy. Nothing is ever replaced, cancelled or modified.
- **Repeats and different limits:** the job's identity includes its profile.
  - The same extraction asked for again with the same limit is the same job. The result is `exists`, with a note that nothing changed.
  - Asking with a different limit creates a separate job (`enqueued`). The result lists the other jobs for that extraction, which are unchanged. `koi-discourse jobs` shows each job's limit.
- **Refusals:** any other value is refused at the command line. If it reaches the hook, scheduling is refused and the extraction still completes. A stored limit the worker does not allow fails the job.
- **If nothing is queued** (no moves extracted), the summary says the limit was not applied.

## Tiers in review

Every sighting of a pair records which claim's search found it and its pick, meaning its position among that search's picks (`after_state.occurrence` in the run's effect rows).

- **The tiers:** picks 1–5 are `first_five`, which is what the default keeps. Picks 6 and later are `additional`.
- **What a tier is:** it belongs to one occurrence (one run, one search), not to the candidate. The same pair can be `first_five` in one run and `additional` in another.
- **Rediscovery:** a later sighting never changes the row's first sighting (run, query, ranks, parameters). It adds an occurrence.
- **Sightings recorded before picks were:** these are `first_five` when their run kept at most 5 per claim, which covers every run so far. Otherwise they are `unknown`, never guessed.
- **Rolled-back runs:** their sightings stay in the run history and are not listed.

```bash
koi-discourse queue --seen-in-run ingest:<job>:<attempt> --tier additional --show-scores   # picks 6-10 of one run
koi-discourse batch --seen-in-run ingest:<job>:<attempt> --from-queue 40 \
  --out reviewer/batch.json --binding-out operator/batch.binding.json                         # both tiers, blind
```

The two run filters, with the same meaning in `queue`, `batch` and `candidates`:

- **`--run-id RUN`:** candidates first discovered by `RUN`, the run stored on the candidate.
  - Each is reviewed once, as stored: A is the query of its first sighting.
  - `--tier` is that first sighting's tier.
- **`--seen-in-run RUN`** (`queue` and `batch`): every sighting by `RUN`, the first or a rediscovery.
  - Each sighting is reviewed from its own query. A is the claim whose search found B there.
  - A pair met from both ends gives two items, and their reviews stay separate.
  - `--tier` is each sighting's tier.
  - A sighting whose query was not recorded is counted and not listed.

What a sighting review records:

- **The anchor:** the sighting's query, `anchor.move_id`, and which stored end it is, `anchor.stored_end`.
- **The sighting itself:** run and query, in the binding file and the annotations, never in the reviewer file. The annotation's orientation reads `A=query of the selected sighting, B=the claim it picked`.
- **What changes nothing:** the candidate's stored first sighting. A batch made without filters and its annotations are exactly as before (`A=query, B=target`).
- **Supersession:** a later answer can supersede an earlier one only for the same reviewer, candidate, question and claim A.
- **Blind views and the reviewer file** show no pick, tier, run or anchor.
- **The batch's selection:** the filters are recorded in the binding file.

## The worker

- **Scheduled:** `scripts/run_discourse_discovery.sh` makes one pass that claims at most one job. Run it from a scheduler (launchd, cron or a systemd timer) every 15 minutes, with no keep-alive and no run at load.
- **By hand:** `scripts/run_discourse_discovery.sh`.
- **Steps for each job:**
  1. **Check the binding:** source revision, extraction run and input digest, claims, caller context. If any changed, the job becomes `stale`.
  2. **Plan:** in one READ ONLY transaction, with local Ollama embeddings only.
  3. **Apply:** one short transaction, with the writer lock, a 2 s lock timeout and a write-scope check. The candidates, the run row (`ingest:<job>:<attempt>`) and the job's completion commit together.
- **Limits** (`api/discourse_discovery.py` `LIMITS`, stored with each job):
  - 200 moves;
  - 3 attempts, 1 replan;
  - planning 600 s, apply 60 s;
  - lease 900 s;
  - backoff 60, 300 and 1,800 s.

### Seeing pending, failed and stale jobs

- `koi-discourse jobs` (read-only) lists every job with status, attempts, last error, run, run status, outputs (`present` or `rolled_back`) and candidate count. It also lists extractions that recorded bindings but have no job.
- `logs/discourse-discovery-state.json` is rewritten after every scheduled pass. It holds what the pass did, the queue by status, and every failed or stale job with its error under `needs_attention`.
- `logs/discourse-discovery.log` and `.error.log` hold the pass output and scheduler errors.

Failed and stale jobs are not retried past their limits. A newer extraction of the same source queues its own job.

## Query (Claude Code, Codex, people)

Install once:

```bash
ln -sfn "$PWD/scripts/koi-discourse" ~/.local/bin/koi-discourse   # run from the checkout
```

```bash
koi-discourse candidates --query "cognitive glue" --limit 5          # moves matching the text, with their candidates
koi-discourse candidates --move-id <uuid>                            # one move
koi-discourse candidates --run-id ingest:1:1 --limit 20              # candidates first discovered by one run
koi-discourse jobs --source-rid document:<sha256>
```

- **Read-only:** both commands run in one READ ONLY transaction and print JSON. The top-level `label` says what candidates are.
- **Results:** each result has `move` (the claim and its source), `admitted_relations` (reviewed judgments; empty while judgments are disabled) and `candidates`.
- **Each candidate:** `label: candidate (unreviewed)`, the legs that found it, scores, `other` (the other claim and its source), `admissible_now` and its blockers.
- **Reporting:** quote both sources and the label when reporting a candidate. Do not report it as support or agreement.

## Local review (I1)

`koi-discourse queue | batch | show | annotate | review | annotations` (`scripts/discourse_review.py`, `api/discourse_review.py`) puts stored candidates in front of a person. Database reads run in one READ ONLY transaction. Nothing changes a candidate, a judgment, a retrieval limit or the database.

- **Lanes:** `cross_source`, `same_work_mapped` (from a mapping file) and `same_work_suggested` (from `same_work_signals`, labelled unverified). Every stored candidate stays listed.
- **Order:** `source_diverse` (round-robin over source pairs, the default) or `fused`.
- **Cues:** shared listed author, same publication feed, or the other source's listed surname in the claim text. Each is shown as unverified and is never a dependence or restatement verdict. Unmapped authors show as unknown.
- **Batches:** blind by default, with no scores, ranks or run ids. A batch records the exact claim texts, versions and the mapping file's sha256.
  - **Without filters:** one file (`koi-discourse-review-batch/v1`), exactly as before. Existing batches keep their ids.
  - **With `--run-id`, `--seen-in-run` or `--tier`:** two files. See [Reviewer and binding files](#reviewer-and-binding-files).
  - **One-file batches made by I2 with a selection or sightings** still load, show and annotate. `show` warns that they are for the operator only.
- **Annotations:** one question, "Is B useful to someone studying A?" (`useful | marginal | not_useful | needs_context`, optional reason).
  - Appended to a JSONL file you name, never edited. A correction supersedes an earlier line.
  - Each line is bound to the candidate id and key, both texts' representation hashes, the source and bound versions, and the declared reviewer.
  - Refused when the text or version changed after the batch was made.

```bash
koi-discourse queue --mappings mapping.json --limit 10
koi-discourse batch --from-queue 12 --mappings mapping.json --out batch.json
koi-discourse review --batch batch.json --reviewer <name> --annotations annotations.jsonl
```

### Reviewer and binding files

A filtered batch is written as `--out` (the reviewer file, `koi-discourse-review-batch/v2-reviewer`) and `--binding-out` (the binding file, `koi-discourse-review-binding/v1`). `--binding-out` is required, must be outside the reviewer file's directory, and is refused for an unfiltered batch. Neither file is ever overwritten. If the binding file cannot be written, the reviewer file is removed.

- **The reviewer file is all a reviewer receives.** Hand it over in a directory that holds nothing else.
  - **It holds:** the question and answers, the orientation ("A is the claim being studied; B is the claim to judge"), a salted commitment to the binding file's contents (`binding_commitment`), and per item a position, an opaque item id, the lane, listed authors, cues, and A and B: move id, type, title, detail, status, chunk range, representation hash, source (rid, title, URL, version and its basis) and binding provenance (kind, association, bound version, current, limitations).
  - **It holds none of:** candidate ids or keys, scores, ranks, runs, picks, tiers, anchors, stored ends, the selection, the note, the database or a time.
  - **Positions** follow the salted item ids, not the queue's score order, so position carries no rank.
  - **Direction:** a sighting item is shown from its own query (A is the claim whose search found B), as in the queue.
- **A reviewer must not be able to look the claims up.** The reviewer file is blind only to a reader who cannot query KOI: with a shell, database or KOI MCP access on this host, `koi-discourse queue --seen-in-run RUN --show-scores` shows every run, pick and tier, and a claim's text or move id finds its candidate. Give an agent reviewer the file's contents in a session with no shell, database, KOI MCP or other file access.
- **One batch per run.** A batch whose items would show the same A and B twice (one pair sighted from the same claim by two `--seen-in-run` runs) is refused, because the repeat tells the reviewer the pair was retrieved twice.
- **The binding file is the operator's.** Reviewers must not see it. Keep it, and the annotation file, outside the directory handed to reviewers: `annotate` and `review` refuse an annotation file inside the reviewer file's directory.
  - **It holds:** per item the position, item id, queue position, candidate id and key, anchor, sighting and tier (the tier key), and for the batch the salt, selection, note, mapping file, database and creation time.
- **Integrity checks.** `annotate`, `review` and `show --verify` need `--binding` for a reviewer file and refuse, recording nothing, unless all of these hold:
  1. the reviewer file matches its own `reviewer_batch_id` (the sha256 of its canonical body), so an edited reviewer file is refused;
  2. a binding file is given, has exactly the binding file's fields, and matches its own `binding_id` (the sha256 of its canonical body);
  3. the binding file names this reviewer file, by `reviewer_batch_id` and by `reviewer_file_sha256`, the sha256 of the reviewer file's exact bytes, so another batch's binding file, or a re-serialised copy of the reviewer file, is refused;
  4. the binding file's contents (items with their tiers, queue positions, candidates, anchors and sightings, and the selection, note, mappings, database and time) hash, with its salt, to the reviewer file's `binding_commitment`. An edited binding file is therefore refused even when its own id was recomputed;
  5. both files list the same positions with the same item ids, and each item id equals `sha256(salt, candidate id and key, the A and B move ids the reviewer saw, anchor, sighting)`.
- **Recording an answer.** `annotate` on a reviewer file also needs `--item-id`, at least 8 characters of the item id the reviewer was shown (`item <id>` in `show`). An answer recorded against another item, or against the same position in another reviewer file, is refused. Reviewers' answers should therefore name the item id. `review` records what the person at the terminal answers for the item it shows; record an agent's answers with `annotate --item-id`, not by retyping them into `review`.
- **What the checks cannot show.** The files are not signed. Someone holding both can rewrite them consistently (for example a tier, with every hash recomputed). Record the `reviewer_batch_id` and `binding_id` that `batch` prints in the trial's own record when the batch is made, and have the analysis run the checks again and compare both ids.
- **Paths.** Folders are compared by file identity, so a case-insensitive volume or a symlink cannot place the binding or annotation file inside the reviewer file's directory under another spelling. `annotate`, `review` and `show --verify` refuse a binding file that sits there. `batch` never overwrites a file, and if it cannot write both files it removes whatever it wrote.

  The candidate checks then apply as for every batch: both ends end for end, text and version unchanged, and the selected sighting still current, so a rolled-back run refuses.
- **Annotations of a reviewer file** use `koi-discourse-annotation/v2`. That is v1 plus `binding_id` and `item_id`, with `batch_id` set to the reviewer file's `reviewer_batch_id`. Annotations of one-file batches are unchanged v1.

```bash
mkdir -p trial/reviewer trial/operator
koi-discourse batch --seen-in-run ingest:<job>:<attempt> --from-queue 40 \
  --out trial/reviewer/batch.json --binding-out trial/operator/batch.binding.json
koi-discourse show --batch trial/reviewer/batch.json            # exactly what reviewers see; copy only this file to each reviewer
koi-discourse annotate --batch trial/reviewer/batch.json --binding trial/operator/batch.binding.json \
  --item 3 --item-id 1a2b3c4d --answer useful --reviewer agent:a --annotations trial/operator/annotations.jsonl
```

## Backout

- **Supported backout:** operational rollback of one run. It undoes the run's owned outputs (candidates, vectors, unreferenced claim texts), keeps the job, run and effect rows as history (run status `rolled_back`), and never touches the ingested document or its bindings.
  ```bash
  venv/bin/python -B scripts/discourse_links_canary.py rollback --dsn <dsn> --run-id ingest:<job>:<attempt> --apply
  ```
- **Schema removal:** `migrations/134_discourse_discovery_jobs_down.sql` is destructive and separate. It refuses while ingestion run history exists.
- **The per-claim limit:** a run at 10 rolls back like any other.
  - Reverting the code needs no data change. Jobs keep their stored limit, and the extra `occurrence` key in effect rows is ignored by earlier code.
  - A job still queued at 10 when the code is reverted would run at 10 under the earlier worker. Check `koi-discourse jobs` first.
- **Delivering code to the checkout the scheduled jobs run from:** if its branch carries local commits or uncommitted files,
  - Move it only by `git merge --ff-only` to a prepared integration commit, and back by a fast-forward to a prepared revert commit. Never a bare `git pull`.
  - Pause the discourse-discovery job first, and start no ingestion until the move is done.

## Files

- **Library:**
  - `api/discourse_links.py`: visibility, representations, bindings, retrieval, plan and apply, review;
  - `api/discourse_discovery.py`: jobs and status.
- **Scripts:**
  - `scripts/discourse_discovery_worker.py`: run, status;
  - `scripts/discourse_links_canary.py`: manifest canary, rollback, `candidates`;
  - `scripts/extract_deep_documents.py`: binding capture and the job hook;
  - `scripts/ingest_document.py`: `--discourse-discovery`, `--discourse-move-cap`;
  - `scripts/discourse_move_cap_demo.py`: offline demonstration of the per-claim limit and tiers.
- **Schema and tests:**
  - `migrations/133_*`, `migrations/134_*`;
  - `tests/test_discourse_links.py`.
