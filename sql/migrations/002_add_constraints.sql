-- ============================================
-- Migration 002: Add Data Integrity Constraints
-- ============================================
-- Purpose: Add FK, NOT NULL, CHECK constraints
-- Date: 2025-01-06
-- Author: JobInsight Team
-- ============================================
-- Run AFTER migration 001 and AFTER initial data load
-- ============================================

\c jobinsight;

\echo '⚙️  Running Migration 002: Adding constraints'

-- ============================================
-- STAGING: Add constraints
-- ============================================

-- NOT NULL constraints for critical fields
ALTER TABLE jobinsight_staging.staging_jobs
    ALTER COLUMN title_clean SET NOT NULL,
    ALTER COLUMN company_name_standardized SET NOT NULL;

-- CHECK constraints for data validation
ALTER TABLE jobinsight_staging.staging_jobs
    ADD CONSTRAINT chk_staging_salary_range 
        CHECK (salary_max IS NULL OR salary_min IS NULL OR salary_max >= salary_min),
    
    ADD CONSTRAINT chk_staging_salary_positive 
        CHECK (salary_min IS NULL OR salary_min >= 0),
    
    ADD CONSTRAINT chk_staging_salary_type 
        CHECK (salary_type IN ('range', 'negotiable', 'from', 'upto', 'fixed'));

-- ============================================
-- MONITORING: Add constraints
-- ============================================

-- Pipeline runs constraints
ALTER TABLE monitoring.pipeline_runs
    ADD CONSTRAINT chk_pipeline_status 
        CHECK (status IN ('success', 'failed', 'running', 'skipped')),
    
    ADD CONSTRAINT chk_pipeline_dq_score 
        CHECK (data_quality_score IS NULL OR (data_quality_score >= 0 AND data_quality_score <= 1));

-- Metrics constraints
ALTER TABLE monitoring.metrics
    ADD CONSTRAINT chk_metrics_category 
        CHECK (metric_category IN ('crawler', 'etl', 'warehouse', 'business', 'system'));

-- Data quality constraints
ALTER TABLE monitoring.data_quality
    ADD CONSTRAINT chk_dq_status 
        CHECK (status IN ('passed', 'warning', 'failed')),
    
    ADD CONSTRAINT chk_dq_type 
        CHECK (check_type IN ('reconciliation', 'integrity', 'variance', 'freshness', 'completeness'));

-- Data lineage constraints
ALTER TABLE monitoring.data_lineage
    ADD CONSTRAINT chk_lineage_source_system 
        CHECK (source_system IN ('TopCV', 'VietnamWorks', 'Manual', 'API'));

-- ============================================
-- COMPLETION
-- ============================================

\echo '✅ Migration 002 completed: Constraints added'
\echo '   - CHECK constraints for data validation'
\echo '   - NOT NULL constraints for critical fields'
\echo '   - Enum constraints for status/type columns'

\echo ''
\echo '⚠️  NOTE: DuckDB DWH constraints will be added separately'
\echo '   Run: migrations/002b_dwh_constraints.sql (for DuckDB)'
