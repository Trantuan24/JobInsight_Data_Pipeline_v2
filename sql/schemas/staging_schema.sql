-- ============================================
-- JobInsight - Staging Schema
-- ============================================

\c jobinsight;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS jobinsight_staging;

CREATE TABLE IF NOT EXISTS jobinsight_staging.staging_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    title_clean VARCHAR(255),
    job_url TEXT,
    company_name VARCHAR(200),
    company_name_standardized VARCHAR(200),
    company_url TEXT,
    verified_employer BOOLEAN DEFAULT FALSE,
    logo_url TEXT,
    salary VARCHAR(100),
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_type VARCHAR(20),
    location VARCHAR(200),
    deadline VARCHAR(50),
    due_date TIMESTAMP WITH TIME ZONE,
    time_remaining TEXT,
    skills JSONB,
    last_update VARCHAR(100),
    posted_time TIMESTAMP WITH TIME ZONE,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_to_dwh BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_staging_jobs_company ON jobinsight_staging.staging_jobs(company_name_standardized);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_location ON jobinsight_staging.staging_jobs(location);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_salary ON jobinsight_staging.staging_jobs(salary_min, salary_max) WHERE salary_min IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_staging_jobs_crawled_at ON jobinsight_staging.staging_jobs(crawled_at DESC);
CREATE INDEX IF NOT EXISTS idx_staging_jobs_processed ON jobinsight_staging.staging_jobs(processed_to_dwh) WHERE processed_to_dwh = FALSE;

-- Grant to jobinsight
GRANT ALL ON SCHEMA jobinsight_staging TO jobinsight;
GRANT ALL ON jobinsight_staging.staging_jobs TO jobinsight;

\echo '✅ Staging schema created'
