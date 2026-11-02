-- Migration: RFC 8628 Device Authorization Grant (User Code Flow)
--
-- Security improvements:
-- 1. user_code: Short code user manually types (prevents phishing)
-- 2. state_id: Opaque ID for OAuth state (device_code never sent to Google)
-- 3. POST endpoints: Secrets in body, not URL (prevents logging exposure)

-- Add user_code column (the short code users type, e.g., "WDJV-QK4Z")
ALTER TABLE auth_requests
    ADD COLUMN IF NOT EXISTS user_code VARCHAR(12);

-- Add state_id column (opaque ID sent to Google instead of device_code)
ALTER TABLE auth_requests
    ADD COLUMN IF NOT EXISTS state_id VARCHAR(64);

-- Index for user_code lookups (when user submits on /activate page)
CREATE INDEX IF NOT EXISTS idx_auth_requests_user_code ON auth_requests(user_code);

-- Index for state_id lookups (when Google callback returns)
CREATE INDEX IF NOT EXISTS idx_auth_requests_state_id ON auth_requests(state_id);

-- Comments for documentation
COMMENT ON COLUMN auth_requests.user_code IS 'Short user-visible code (e.g., WDJV-QK4Z) - user types this manually';
COMMENT ON COLUMN auth_requests.state_id IS 'Opaque ID sent to Google OAuth - device_code never exposed to Google';

-- Update the table comment
COMMENT ON TABLE auth_requests IS 'RFC 8628 Device Authorization - user_code prevents phishing, state_id keeps device_code secret';
