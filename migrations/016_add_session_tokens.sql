-- Migration: Add session_tokens table for secure MCP authentication
-- This replaces passing Google OAuth tokens to MCP clients
-- Session tokens are short-lived, revocable, and only work with our API

CREATE TABLE IF NOT EXISTS session_tokens (
    id SERIAL PRIMARY KEY,
    session_token VARCHAR(64) NOT NULL UNIQUE,  -- Random UUID, not the Google token
    user_email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revoked_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    client_info TEXT  -- Optional: store MCP client info for audit
);

-- Index for fast token lookups
CREATE INDEX IF NOT EXISTS idx_session_tokens_token ON session_tokens(session_token);
CREATE INDEX IF NOT EXISTS idx_session_tokens_email ON session_tokens(user_email);
CREATE INDEX IF NOT EXISTS idx_session_tokens_expires ON session_tokens(expires_at);

-- Clean up expired tokens periodically (can be run as cron job)
-- DELETE FROM session_tokens WHERE expires_at < NOW() OR revoked_at IS NOT NULL;

COMMENT ON TABLE session_tokens IS 'Short-lived session tokens for MCP authentication. These replace exposing Google OAuth tokens to clients.';
COMMENT ON COLUMN session_tokens.session_token IS 'Random UUID - NOT the Google OAuth token. Safe to expose to MCP clients.';
