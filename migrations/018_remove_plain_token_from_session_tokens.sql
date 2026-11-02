-- Migration: Remove plain token from session_tokens table
-- Security improvement: Plain tokens should only exist temporarily in auth_requests
-- session_tokens (long-lived) should only store hashes

-- Step 1: Add plain token column to auth_requests (temporary storage for one-time retrieval)
ALTER TABLE auth_requests
    ADD COLUMN IF NOT EXISTS session_token VARCHAR(64);

COMMENT ON COLUMN auth_requests.session_token IS 'Plain session token - temporary, returned once then NULLed';

-- Step 2: For any existing session_tokens, migrate the hash if not already set
UPDATE session_tokens
SET token_hash = encode(sha256(session_token::bytea), 'hex')
WHERE token_hash IS NULL AND session_token IS NOT NULL;

-- Step 3: Drop the plain token column from session_tokens
-- This is the key security fix - long-lived storage should only have hashes
ALTER TABLE session_tokens DROP COLUMN IF EXISTS session_token;

-- Verify the schema
COMMENT ON TABLE session_tokens IS 'Long-lived session tokens - stores ONLY hashes, never plain tokens';
