-- Rollback for migration 139. See that file for rationale.
--
-- This is deliberately empty. Restoring GRANT SELECT ON v_daily_content TO PUBLIC would let every role
-- read all private koi_memories content again, which is the exposure 139 closed. If a named role needs
-- the view, grant it to that role.

SELECT 1;
