-- ============================================
-- Migration 001: Initial Schema Setup
-- ============================================
-- Purpose: Baseline schema for JobInsight
-- Date: 2025-01-01
-- Author: JobInsight Team
-- ============================================
-- This migration creates all base tables
-- Run ONLY on fresh database
-- ============================================

\c jobinsight;

\echo '⚙️  Running Migration 001: Initial Schema Setup'

-- ============================================
-- SCHEMA: public (Raw data)
-- ============================================

CREATE TABLE IF NOT EXISTS public.raw_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    job_url TEXT,
    company_name VARCHAR(200),
    company_url TEXT,
    salary VARCHAR(100),
    skills JSONB,
    location VARCHAR(100),
    location_detail TEXT,
    deadline VARCHAR(50),
    verified_employer BOOLEAN DEFAULT FALSE,
    last_update VARCHAR(100),
    logo_url TEXT,
    posted_time TIMESTAMP WITH TIME ZONE,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for raw_jobs
CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON public.raw_jobs(company_name);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_location ON public.raw_jobs(location);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_posted_time ON public.raw_jobs(posted_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_crawled_at ON public.raw_jobs(crawled_at DESC);

-- ============================================
-- SCHEMA: jobinsight_staging (Cleaned data)
-- ============================================

CREATE SCHEMA IF NOT EXISTS jobinsight_staging;

CREATE TABLE IF NOT EXISTS jobinsight_staging.staging_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    title_clean VARCHAR(255) NOT NULL,
    job_url TEXT,
    company_name VARCHAR(200),
    company_name_standardized VARCHAR(200),
    company_url TEXT,
    salary VARCHAR(100),
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_type VARCHAR(20),
    skills JSONB,
    location VARCHAR(200),
    location_detail TEXT,
    location_pairs JSONB,
    deadline VARCHAR(50),
    verified_employer BOOLEAN DEFAULT FALSE,
    last_update VARCHAR(100),
    logo_url TEXT,
    posted_time TIMESTAMP WITH TIME ZONE,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    due_date TIMESTAMP WITH TIME ZONE,
    time_remaining TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_to_dwh BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

-- Indexes for staging_jobs
CREATE INDEX IF NOT EXISTS idx_staging_jobs_company ON jobinsight_staging.staging_jobs(company_name_standardized);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_location ON jobinsight_staging.staging_jobs(location);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_salary ON jobinsight_staging.staging_jobs(salary_min, salary_max);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_posted_time ON jobinsight_staging.staging_jobs(posted_time DESC);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_crawled_at ON jobinsight_staging.staging_jobs(crawled_at DESC);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_due_date ON jobinsight_staging.staging_jobs(due_date ASC);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_processed ON jobinsight_staging.staging_jobs(processed_to_dwh, crawled_at);

-- ============================================
-- SCHEMA: monitoring (Metrics & tracking)
-- ============================================

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(50) NOT NULL,
    task_id VARCHAR(50) NOT NULL,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(20) NOT NULL,
    duration_seconds FLOAT,
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    error_type VARCHAR(100),
    data_quality_score FLOAT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL,
    metric_category VARCHAR(30),
    metric_value FLOAT NOT NULL,
    metric_unit VARCHAR(20),
    metadata JSONB,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_table VARCHAR(50) NOT NULL,
    source_key VARCHAR(100) NOT NULL,
    target_table VARCHAR(50) NOT NULL,
    target_key VARCHAR(100) NOT NULL,
    transformation VARCHAR(100),
    metadata JSONB,
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS monitoring.data_quality (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(30) NOT NULL,
    table_name VARCHAR(50),
    status VARCHAR(20) NOT NULL,
    expected_value FLOAT,
    actual_value FLOAT,
    threshold FLOAT,
    variance FLOAT,
    details TEXT,
    metadata JSONB,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Monitoring indexes
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_date ON monitoring.pipeline_runs(dag_id, run_date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON monitoring.pipeline_runs(status, execution_date DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON monitoring.metrics(metric_name, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_category ON monitoring.metrics(metric_category, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON monitoring.data_lineage(target_table, target_key);
CREATE INDEX IF NOT EXISTS idx_lineage_source ON monitoring.data_lineage(source_table, source_key);
CREATE INDEX IF NOT EXISTS idx_dq_status ON monitoring.data_quality(status, checked_at DESC);

-- ============================================
-- COMPLETION
-- ============================================

\echo '✅ Migration 001 completed: Initial schema created'
\echo '   - public.raw_jobs'
\echo '   - jobinsight_staging.staging_jobs'
\echo '   - monitoring.* (4 tables)'
