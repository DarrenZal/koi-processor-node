-- Migration: Fix CAT receipts table structure
-- Date: 2025-09-14
-- Purpose: Add missing PRIMARY KEY constraint to koi_transformation_receipts table

-- Add PRIMARY KEY constraint to koi_transformation_receipts
-- This is required for the ON CONFLICT clause to work in CAT receipt creation
DO $$
BEGIN
    -- Check if the table exists and doesn't have a primary key
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'koi_transformation_receipts'
    ) AND NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'koi_transformation_receipts'
        AND constraint_type = 'PRIMARY KEY'
    ) THEN
        ALTER TABLE koi_transformation_receipts ADD PRIMARY KEY (receipt_id);
        RAISE NOTICE 'Added PRIMARY KEY constraint to koi_transformation_receipts';
    END IF;
END $$;

-- Ensure all required columns have defaults where appropriate
ALTER TABLE koi_transformation_receipts
    ALTER COLUMN chunks_created SET DEFAULT 0,
    ALTER COLUMN embeddings_created SET DEFAULT 0,
    ALTER COLUMN entities_extracted SET DEFAULT 0,
    ALTER COLUMN metadata SET DEFAULT '{}',
    ALTER COLUMN created_at SET DEFAULT NOW();

-- Add indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_koi_receipts_input_rid ON koi_transformation_receipts(input_rid);
CREATE INDEX IF NOT EXISTS idx_koi_receipts_output_rid ON koi_transformation_receipts(output_rid);
CREATE INDEX IF NOT EXISTS idx_koi_receipts_created_at ON koi_transformation_receipts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_koi_receipts_transformation_type ON koi_transformation_receipts(transformation_type);

-- Add comment for documentation
COMMENT ON TABLE koi_transformation_receipts IS 'CAT (Content Addressable Transformation) receipts for complete KOI provenance tracking';