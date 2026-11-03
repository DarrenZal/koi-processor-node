-- =============================================================================
-- 125_extraction_provenance_and_quality_down — reverse 125.
-- =============================================================================
-- ⚠ THIS DESTROYS DATA THAT ONLY LIVES HERE.
--
-- 125 added columns and a table; nothing else writes the provenance receipts or
-- the semantic verdicts, so dropping them is the only copy gone. That is why the
-- export below runs FIRST and why it is placed BELOW `\set ON_ERROR_STOP on`:
-- above it, a failed export exits 0 and psql continues straight into the DROPs.
-- (Migration 123's down file once shipped with its export lines commented out, and
-- running it destroyed rows at exit 0. Same shape, so: same fix.)
--
-- Paths are LITERAL. `\copy` does not interpolate :'var' inside a filename — a
-- repair that tried to parameterise this was silently broken by exactly that.
-- =============================================================================

\set ON_ERROR_STOP on

-- Export first. If either of these fails, ON_ERROR_STOP aborts before any DROP.
\copy (SELECT * FROM document_extraction_runs) TO '/tmp/koi_125_down_extraction_runs.csv' WITH CSV HEADER
\copy (SELECT document_rid, window_index, route_used, provider, model, transport, producer FROM document_window_extractions WHERE provider IS NOT NULL OR model IS NOT NULL OR transport IS NOT NULL OR producer IS NOT NULL) TO '/tmp/koi_125_down_window_producers.csv' WITH CSV HEADER

BEGIN;

DROP TABLE IF EXISTS document_extraction_runs;

DROP INDEX IF EXISTS idx_docwin_provider_model;

ALTER TABLE document_window_extractions
    DROP COLUMN IF EXISTS provider,
    DROP COLUMN IF EXISTS model,
    DROP COLUMN IF EXISTS transport,
    DROP COLUMN IF EXISTS producer;

-- Restore the pre-125 meaning of route_used's comment, so a reader after a
-- rollback is not told to prefer columns that no longer exist.
COMMENT ON COLUMN document_window_extractions.route_used IS
  'Coarse route label derived from the CONFIGURED transport. Unreliable for any run with DOC_EXTRACTOR_TRANSPORT_FALLBACK set — see issue #64.';

DELETE FROM koi_migrations
 WHERE migration_id = 'personal:125_extraction_provenance_and_quality';

COMMIT;
