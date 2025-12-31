-- ============================================
-- JobInsight - Raw Jobs Schema
-- ============================================

\c jobinsight;

CREATE TABLE IF NOT EXISTS public.raw_jobs (
    job_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    job_url TEXT,
    company_name VARCHAR(200),
    company_url TEXT,
    verified_employer BOOLEAN DEFAULT FALSE,
    logo_url TEXT,
    salary VARCHAR(100),
    location VARCHAR(100),
    skills JSONB,
    deadline VARCHAR(50),
    last_update VARCHAR(100),
    posted_time TIMESTAMP WITH TIME ZONE,
    crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON public.raw_jobs(company_name);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_location ON public.raw_jobs(location);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_posted_time ON public.raw_jobs(posted_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_crawled_at ON public.raw_jobs(crawled_at DESC);

-- Grant to jobinsight
GRANT ALL ON public.raw_jobs TO jobinsight;

\echo '✅ Raw jobs schema created'
