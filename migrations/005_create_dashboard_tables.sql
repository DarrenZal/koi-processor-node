-- Migration: Create Dashboard Tables for Milestone B Content Operations
-- Purpose: Support the monitoring dashboard for Daily Bot and Weekly Digest
-- Date: 2025-09-13

-- Create content_reviews table for tracking drafts and approvals
CREATE TABLE IF NOT EXISTS content_reviews (
    id SERIAL PRIMARY KEY,
    content_type VARCHAR(50) NOT NULL, -- 'daily_thread', 'weekly_digest', 'podcast_brief'
    status VARCHAR(50) NOT NULL DEFAULT 'draft', -- 'draft', 'pending_review', 'approved', 'rejected', 'published', 'auto_published', 'rolled_back'
    content JSONB NOT NULL DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    reviewer VARCHAR(100),
    review_notes TEXT,
    approved_at TIMESTAMP,
    auto_published BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes for common queries
    INDEX idx_content_reviews_status (status),
    INDEX idx_content_reviews_type (content_type),
    INDEX idx_content_reviews_created (created_at DESC)
);

-- Create dashboard_alerts table for system notifications and errors
CREATE TABLE IF NOT EXISTS dashboard_alerts (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL, -- 'sensor_failure', 'content_validation', 'style_score_low', 'api_error', etc.
    severity VARCHAR(20) NOT NULL DEFAULT 'info', -- 'critical', 'warning', 'info'
    message TEXT NOT NULL,
    details JSONB DEFAULT '{}',
    resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes for alert management
    INDEX idx_dashboard_alerts_severity (severity),
    INDEX idx_dashboard_alerts_resolved (resolved),
    INDEX idx_dashboard_alerts_created (created_at DESC),
    
    -- Validate severity values
    CHECK (severity IN ('critical', 'warning', 'info'))
);

-- Create dashboard_metrics table for tracking performance metrics
CREATE TABLE IF NOT EXISTS dashboard_metrics (
    id SERIAL PRIMARY KEY,
    metric_type VARCHAR(50) NOT NULL, -- 'daily_generation', 'weekly_progress', 'quality_score', etc.
    metric_value JSONB NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Index for time-series queries
    INDEX idx_dashboard_metrics_type (metric_type),
    INDEX idx_dashboard_metrics_timestamp (timestamp DESC)
);

-- Create scheduled_runs table for tracking automation
CREATE TABLE IF NOT EXISTS scheduled_runs (
    id SERIAL PRIMARY KEY,
    run_type VARCHAR(50) NOT NULL, -- 'daily_bot', 'weekly_digest'
    scheduled_time TIMESTAMP WITH TIME ZONE NOT NULL,
    actual_start_time TIMESTAMP WITH TIME ZONE,
    actual_end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) DEFAULT 'scheduled', -- 'scheduled', 'running', 'completed', 'failed', 'skipped'
    result JSONB DEFAULT '{}',
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes for schedule management
    INDEX idx_scheduled_runs_type (run_type),
    INDEX idx_scheduled_runs_scheduled (scheduled_time),
    INDEX idx_scheduled_runs_status (status)
);

-- Create approval_history table for audit trail
CREATE TABLE IF NOT EXISTS approval_history (
    id SERIAL PRIMARY KEY,
    content_review_id INTEGER REFERENCES content_reviews(id),
    action VARCHAR(50) NOT NULL, -- 'approve', 'reject', 'request_revision', 'auto_approve'
    performed_by VARCHAR(100) NOT NULL,
    notes TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Index for audit queries
    INDEX idx_approval_history_review (content_review_id),
    INDEX idx_approval_history_action (action),
    INDEX idx_approval_history_created (created_at DESC)
);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for content_reviews
DROP TRIGGER IF EXISTS update_content_reviews_updated_at ON content_reviews;
CREATE TRIGGER update_content_reviews_updated_at 
    BEFORE UPDATE ON content_reviews 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Insert default alert for system initialization
INSERT INTO dashboard_alerts (alert_type, severity, message, resolved)
VALUES ('system', 'info', 'Dashboard tables initialized successfully', true)
ON CONFLICT DO NOTHING;

-- Add comments for documentation
COMMENT ON TABLE content_reviews IS 'Stores draft content for Daily Bot and Weekly Digest with review workflow';
COMMENT ON TABLE dashboard_alerts IS 'System alerts and notifications for the content operations dashboard';
COMMENT ON TABLE dashboard_metrics IS 'Time-series metrics for monitoring content generation performance';
COMMENT ON TABLE scheduled_runs IS 'Tracks scheduled and executed automation runs';
COMMENT ON TABLE approval_history IS 'Audit trail for content approval workflow';

COMMENT ON COLUMN content_reviews.content_type IS 'Type of content: daily_thread, weekly_digest, or podcast_brief';
COMMENT ON COLUMN content_reviews.status IS 'Current status in the review workflow';
COMMENT ON COLUMN content_reviews.auto_published IS 'Whether content was auto-published after approval period';

COMMENT ON COLUMN dashboard_alerts.severity IS 'Alert severity: critical (immediate action), warning (attention needed), info (informational)';
COMMENT ON COLUMN dashboard_alerts.resolved IS 'Whether the alert has been addressed';

-- Grant permissions if needed (adjust user as necessary)
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO your_app_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_app_user;