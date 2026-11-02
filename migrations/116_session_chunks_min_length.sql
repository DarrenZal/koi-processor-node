-- =============================================================================
-- Migration 116: minimum chunk length on session_chunks
-- =============================================================================
-- Date:     2026-09-02
-- Plan:     an internal planning document, Phase 1 #13
--
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/116_session_chunks_min_length.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/116_session_chunks_min_length_down.sql
--
-- =============================================================================
-- *** DO NOT APPLY YET. APPLYING THIS TODAY BREAKS SESSION INGESTION. ***
-- =============================================================================
-- The producer is still emitting the rows this constraint rejects. Measured
-- 2026-09-02:
--
--     day          chunks written    under 100 chars
--     2026-09-02        5,227          4,118  (79%)
--     2026-09-01       11,504         10,429  (91%)
--     2026-08-31        2,746          2,368  (86%)
--
-- So this is not a guard against a historical mistake -- it rejects what the
-- live chunker writes on most inserts. Applied now, session-ingest batches
-- start failing in production.
--
-- ROOT CAUSE, in a different repo:
--   RegenAI/koi-sensors/sensors/claude_sessions/claude_session_sensor.py:1162
--       chunk_text = f"User: {user_text}\n\nAssistant: {assistant_text}"
--   Nothing checks that either half is non-empty. When _extract_text() returns
--   "" for both (tool-result and meta messages), the chunk is exactly
--   "User: \n\nAssistant: " -- 18 chars trimmed. Same pattern at :1332.
--
-- REQUIRED ORDER (starting anywhere else is an outage or a re-fill):
--   1. Fix the sensor to skip empty turn-pairs.        <-- BLOCKS THIS FILE
--   2. Apply this migration (NOT VALID; new writes only).
--   3. Delete the historical stubs, then REINDEX the 3.6GB HNSW index.
--      MEASURED 2026-09-02, the exact repair population at this threshold:
--          length(trim(chunk_text)) <= 18   ->  294,937 rows
--      (At the audit's >=100 it would have been 322,662 -- so the corrected
--      threshold spares 27,725 rows of real short content that the audit's
--      number would have deleted as junk. Worth re-counting immediately
--      before the delete; the producer fix stops it growing, but it has not
--      reached koi-sensors-runtime yet.)
--   4. ALTER TABLE session_chunks VALIDATE CONSTRAINT chk_session_chunks_min_length;
--
-- Step 2 before step 1 breaks ingestion. Step 3 before step 2 lets the
-- pipeline refill what was just deleted.
--
-- =============================================================================
-- WHY "NOT VALID", AND WHY THAT IS THE POINT
-- =============================================================================
-- Measured on personal_koi, 2026-09-02:
--
--     session_chunks total                       481,846
--     with length(trim(chunk_text)) < 100        322,662   (67.0%)
--
-- Two thirds of the table violates this constraint, so it CANNOT be added
-- validated without either failing outright or forcing a 322k-row deletion
-- into this transaction. NOT VALID adds the constraint for all FUTURE writes
-- while leaving existing rows untouched and unscanned -- no table rewrite, no
-- ACCESS EXCLUSIVE beyond a brief catalog lock.
--
-- This is deliberate sequencing, not a compromise: the guard must land BEFORE
-- the repair, or the generator keeps re-creating the condition while the
-- cleanup runs. Deleting the 322k rows first and adding the constraint after
-- leaves a window in which the pipeline refills what was just deleted.
--
-- The dominant shape is an 18-character stub -- literally "User: \n\nAssistant:"
-- (note the space after "User:", which is easy to get wrong when constructing
-- a comparison by hand rather than sampling the column). These are turn-pair
-- chunks where both turns were empty. They carry no content, yet each one is
-- embedded at 3072 dimensions and indexed into the HNSW index that the
-- sessions retrieval surface queries -- forming a large near-duplicate cluster
-- that wins ANN on unspecific queries and occupies a guaranteed answer slot.
--
-- (The audit proposed 100 here, reasoning that TextChunker.min_chunk_size=100
-- already exists in the Python chunker and is simply not applied on this path.
-- That reasoning does not survive measurement -- see the THRESHOLD section
-- below, which is why this constraint is > 18 and not >= 100.)
--
-- TO VALIDATE LATER (after the 322k rows are dealt with, which is a separate
-- operator decision -- it is a deletion plus a REINDEX of a 3.6GB index):
--     ALTER TABLE session_chunks VALIDATE CONSTRAINT chk_session_chunks_min_length;
-- VALIDATE takes only a SHARE UPDATE EXCLUSIVE lock, so it does not block
-- reads or writes; it will simply fail while any violating row remains.
-- =============================================================================

-- =============================================================================
-- THRESHOLD: 19, NOT 100. Derived from the format, then verified against data.
-- =============================================================================
-- The audit specified >= 100, borrowed from TextChunker.min_chunk_size. That
-- value governs DOCUMENT prose chunking. These are conversational turn-pairs,
-- which are legitimately short, and 100 would keep rejecting real content long
-- after the producer is fixed.
--
-- Measured on a 17,281-row sample of the last 2 days (2026-09-02):
--
--     the exact empty stub          14,480   83.8%   <- what we want to reject
--     real content, under 100 chars    544    3.1%   <- would ALSO be rejected at 100
--     over 100 chars                 2,257   13.1%
--
-- So at >= 100, roughly 1 in 5 of the remaining REAL chunks (544 of 2,801)
-- would still fail after the producer fix. That is the same outage one level
-- down, just quieter.
--
-- The correct boundary is structural. The chunk is:
--     "User: " + user_text + "\n\nAssistant: " + assistant_text
--      = 19 characters of scaffold + content
-- With both halves empty, trimming the trailing space leaves exactly 18.
-- One single character of content anywhere makes it 19 or more. So:
--
--     length(trim(chunk_text)) > 18   <=>   "carries at least one character
--                                            that is not scaffold"
--
-- Confirmed against the same sample: shortest real chunk = 20, count under
-- 20 = 0, and the stub = 18. The threshold separates them with no overlap and
-- does not depend on the distribution holding -- it depends on the format.
-- =============================================================================

ALTER TABLE session_chunks
    DROP CONSTRAINT IF EXISTS chk_session_chunks_min_length;

ALTER TABLE session_chunks
    ADD CONSTRAINT chk_session_chunks_min_length
    CHECK (length(trim(chunk_text)) > 18)
    NOT VALID;

COMMENT ON CONSTRAINT chk_session_chunks_min_length ON session_chunks IS
  'Rejects scaffold-only session chunks. The turn-pair format is "User: " + user_text + E''\n\nAssistant: " + assistant_text = 19 chars of scaffold; with both halves empty, trimming leaves exactly 18, and one character of real content anywhere makes it 19+. So >18 means "carries at least one non-scaffold character". Deliberately NOT the >=100 the audit proposed (that is TextChunker.min_chunk_size, for document prose): measured 2026-09-02, 544 of 2,801 real chunks in a 2-day sample were under 100 chars, so >=100 would keep rejecting legitimate short turns. Added NOT VALID -- the producer (koi-sensors claude_session_sensor.py, fixed 2026-09-02) had been emitting the 18-char stub at 79-91% of daily writes, so a large historical population violates. Count the violators before ALTER TABLE ... VALIDATE CONSTRAINT.';


-- -----------------------------------------------------------------------------
-- Migration bookkeeping
-- -----------------------------------------------------------------------------
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('116_session_chunks_min_length', 'v1_not_valid_min_length')
ON CONFLICT (migration_id) DO NOTHING;
