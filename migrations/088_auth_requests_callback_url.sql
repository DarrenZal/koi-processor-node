-- Migration 088: add callback_url to auth_requests
--
-- Supports browser-based OAuth flow. When a request originates from a
-- web client (e.g. the /claims portal) rather than a device (MCP), the
-- callback_url is stored here and the OAuth callback handler uses it to
-- 1) set an HttpOnly session cookie and 2) 303 back to the portal URL.
--
-- NULL = device flow (RFC 8628), unchanged behavior.
-- Non-NULL = web flow; callback redirects to this path after setting cookie.
--
-- Safe change — column is nullable with NULL default; existing code paths
-- (device flow) ignore it entirely.

ALTER TABLE auth_requests
  ADD COLUMN IF NOT EXISTS callback_url TEXT NULL;
