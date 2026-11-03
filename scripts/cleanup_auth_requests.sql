-- Cleanup script for auth_requests table
-- Run this every 15 minutes via cron or pg_cron
--
-- SECURITY: auth_requests stores plain tokens temporarily.
-- This cleanup ensures they don't linger in the database.

-- Delete expired, used, or old auth requests
DELETE FROM auth_requests
WHERE expires_at < NOW()
   OR status IN ('used', 'rejected', 'expired')
   OR created_at < NOW() - INTERVAL '30 minutes';

-- Also clean up any orphaned session_tokens (belt and suspenders)
DELETE FROM session_tokens
WHERE expires_at < NOW()
   OR revoked_at IS NOT NULL;
