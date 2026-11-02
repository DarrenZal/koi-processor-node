-- Migration 031: User Profiles for Teaching Mode
-- Stores user experience level and preferences for personalized responses
-- Relationships (manages, works_on) are stored in Apache Jena for semantic queries

-- Create user_profiles table
CREATE TABLE IF NOT EXISTS user_profiles (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  experience_level VARCHAR(20) DEFAULT 'mid',  -- junior|mid|senior|staff|principal
  role VARCHAR(50),                             -- frontend|backend|full-stack|devops|data|etc
  preferences JSONB DEFAULT '{}',               -- explain_before_code, verbosity, etc.
  managed_by VARCHAR(255),                      -- also stored in Jena for semantic queries
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Index on email for fast auth-time lookups
CREATE INDEX IF NOT EXISTS idx_user_profiles_email ON user_profiles(email);

-- Index on managed_by for team lookups
CREATE INDEX IF NOT EXISTS idx_user_profiles_managed_by ON user_profiles(managed_by);

-- Trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_user_profiles_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_user_profiles_updated_at ON user_profiles;
CREATE TRIGGER trigger_user_profiles_updated_at
  BEFORE UPDATE ON user_profiles
  FOR EACH ROW
  EXECUTE FUNCTION update_user_profiles_updated_at();

-- Add constraint to validate experience_level values
ALTER TABLE user_profiles
  DROP CONSTRAINT IF EXISTS valid_experience_level,
  ADD CONSTRAINT valid_experience_level
  CHECK (experience_level IN ('junior', 'mid', 'senior', 'staff', 'principal'));

-- Add comment for documentation
COMMENT ON TABLE user_profiles IS 'User profiles for teaching mode personalization. Core data stored here; relationships in Apache Jena.';
COMMENT ON COLUMN user_profiles.experience_level IS 'User experience level: junior, mid, senior, staff, principal';
COMMENT ON COLUMN user_profiles.role IS 'Technical role: frontend, backend, full-stack, devops, data, etc';
COMMENT ON COLUMN user_profiles.preferences IS 'JSON preferences including explain_before_code, verbosity, etc';
COMMENT ON COLUMN user_profiles.managed_by IS 'Email of manager (also stored in Jena as :manages relationship)';
