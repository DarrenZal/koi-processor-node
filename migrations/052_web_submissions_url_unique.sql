-- 052: Enforce unique URL on web_submissions
-- Required because web_router uses INSERT ... ON CONFLICT (url)

-- Keep the newest row for any duplicate URL before enforcing uniqueness.
DELETE FROM web_submissions older
USING web_submissions newer
WHERE older.url = newer.url
  AND older.id < newer.id;

-- Replace the non-unique lookup index with a unique index usable by ON CONFLICT (url).
DROP INDEX IF EXISTS idx_web_submissions_url;
CREATE UNIQUE INDEX IF NOT EXISTS idx_web_submissions_url_unique
    ON web_submissions(url);
