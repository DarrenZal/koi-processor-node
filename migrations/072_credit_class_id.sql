-- Issue #11: Schema Integration (ADR-004) — add credit_class_id to claims
-- Multiple claims can reference the same credit class (e.g., C04, C05).

ALTER TABLE claims ADD COLUMN IF NOT EXISTS credit_class_id TEXT;
CREATE INDEX IF NOT EXISTS idx_claims_credit_class ON claims(credit_class_id) WHERE credit_class_id IS NOT NULL;
