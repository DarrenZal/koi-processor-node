-- Migration 092d: session_metadata table for Pass-1 extraction outputs
--
-- One row per session holding the parsed JSON output from Pass-1 LLM
-- extraction (gpt-oss-120b via TELUS). The parsed JSON includes session
-- title, summary, key decisions, entities mentioned, and facts. Used
-- as input to Pass-2 contextual retrieval (chunk_context generation).
--
-- Plan: an internal planning document §P_sample_A2 step 1
-- F2 patch authored 2026-04-28.

CREATE TABLE IF NOT EXISTS session_metadata (
  session_id TEXT PRIMARY KEY,
  parsed JSONB NOT NULL,
  llm_model TEXT,
  latency_s REAL,
  created_at TIMESTAMP DEFAULT now(),
  error TEXT
);

COMMENT ON TABLE session_metadata IS
  'Pass-1 extraction outputs per session. Populated by scripts/pass1_session_extract.py (or tmp/p_sample_pass1.py for narrow sample). Read by Pass-2 contextual-retrieval enrichment.';
