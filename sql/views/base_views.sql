-- ============================================
-- JobInsight - Base Dimensional Views
-- ============================================
-- Purpose: Basic views for DuckDB warehouse
-- Database: DuckDB
-- ============================================
-- These views provide simple access to dimensional data
-- For business logic views, see business_views.sql
-- ============================================

-- ============================================
-- VIEW: vw_current_jobs
-- ============================================
-- Current jobs with company information (SCD2 filtered)

CREATE OR REPLACE VIEW vw_current_jobs AS
SELECT 
    j.job_sk,
    j.job_id,
    j.title_clean,
    j.job_url,
    j.skills,
    c.company_sk,
    c.company_name_standardized,
    c.company_url,
    c.verified_employer,
    f.date_id,
    f.salary_min,
    f.salary_max,
    f.salary_type,
    f.due_date,
    f.time_remaining,
    f.posted_time,
    f.crawled_at
FROM DimJob j
JOIN FactJobPostingDaily f ON j.job_sk = f.job_sk
JOIN DimCompany c ON f.company_sk = c.company_sk
WHERE j.is_current = TRUE
  AND c.is_current = TRUE;

COMMENT ON VIEW vw_current_jobs IS 
    'Current jobs with company info. SCD Type 2 filtered (is_current=TRUE).';


-- ============================================
-- VIEW: vw_job_locations
-- ============================================
-- Jobs with locations denormalized (many-to-many expanded)

CREATE OR REPLACE VIEW vw_job_locations AS
SELECT 
    f.fact_id,
    f.job_sk,
    j.job_id,
    j.title_clean,
    f.date_id,
    l.location_sk,
    l.province,
    l.city,
    l.district
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE j.is_current = TRUE
  AND l.is_current = TRUE;

COMMENT ON VIEW vw_job_locations IS 
    'Jobs with all locations denormalized. One row per job-date-location combination.';


-- ============================================
-- VIEW: vw_monthly_jobs
-- ============================================
-- Monthly job statistics (partitioned aggregation)

CREATE OR REPLACE VIEW vw_monthly_jobs AS 
SELECT 
    f.load_month,
    DATE_TRUNC('month', f.date_id) AS month,
    COUNT(DISTINCT f.job_sk) AS job_count,
    COUNT(DISTINCT f.company_sk) AS company_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max,
    COUNT(*) FILTER (WHERE f.verified_employer = TRUE) AS verified_jobs
FROM FactJobPostingDaily f
GROUP BY f.load_month, DATE_TRUNC('month', f.date_id)
ORDER BY f.load_month DESC, month DESC;

COMMENT ON VIEW vw_monthly_jobs IS 
    'Monthly aggregated job statistics by load_month partition.';


-- ============================================
-- VIEW: vw_top_companies
-- ============================================
-- Companies ranked by job posting count

CREATE OR REPLACE VIEW vw_top_companies AS
SELECT 
    c.company_sk,
    c.company_name_standardized,
    c.company_url,
    c.verified_employer,
    COUNT(DISTINCT f.job_sk) AS job_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max,
    MAX(f.date_id) AS last_posting_date
FROM DimCompany c
JOIN FactJobPostingDaily f ON c.company_sk = f.company_sk
WHERE c.is_current = TRUE
GROUP BY c.company_sk, c.company_name_standardized, c.company_url, c.verified_employer
ORDER BY job_count DESC;

COMMENT ON VIEW vw_top_companies IS 
    'Companies ranked by total job postings. Includes salary averages.';


-- ============================================
-- VIEW: vw_top_locations
-- ============================================
-- Locations ranked by job count

CREATE OR REPLACE VIEW vw_top_locations AS
SELECT 
    l.location_sk,
    COALESCE(l.province, 'Unknown') AS province,
    l.city,
    l.district,
    COUNT(DISTINCT f.job_sk) AS job_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max
FROM DimLocation l
JOIN FactJobLocationBridge b ON l.location_sk = b.location_sk
JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
WHERE l.is_current = TRUE
GROUP BY l.location_sk, l.province, l.city, l.district
ORDER BY job_count DESC;

COMMENT ON VIEW vw_top_locations IS 
    'Locations ranked by job count. Includes salary averages.';


-- ============================================
-- VIEW: vw_job_full_details
-- ============================================
-- Complete job information (all dimensions joined)

CREATE OR REPLACE VIEW vw_job_full_details AS
SELECT 
    j.job_id,
    j.title_clean,
    c.company_name_standardized,
    c.verified_employer,
    f.date_id,
    f.salary_min,
    f.salary_max,
    f.salary_type,
    f.due_date,
    f.time_remaining,
    f.posted_time,
    STRING_AGG(DISTINCT l.city, ', ') AS locations,
    j.skills,
    j.job_url,
    c.company_url,
    f.load_month
FROM DimJob j
JOIN FactJobPostingDaily f ON j.job_sk = f.job_sk
JOIN DimCompany c ON f.company_sk = c.company_sk
LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk AND l.is_current = TRUE
WHERE j.is_current = TRUE
  AND c.is_current = TRUE
GROUP BY j.job_id, j.title_clean, c.company_name_standardized, c.verified_employer,
         f.date_id, f.salary_min, f.salary_max, f.salary_type, f.due_date, 
         f.time_remaining, f.posted_time, j.skills, j.job_url, c.company_url, f.load_month;

COMMENT ON VIEW vw_job_full_details IS 
    'Complete job information with all dimensions denormalized.';


-- ============================================
-- COMPLETION
-- ============================================

SELECT '✅ Base views created' AS message;
SELECT '   - vw_current_jobs' AS message;
SELECT '   - vw_job_locations' AS message;
SELECT '   - vw_monthly_jobs' AS message;
SELECT '   - vw_top_companies' AS message;
SELECT '   - vw_top_locations' AS message;
SELECT '   - vw_job_full_details' AS message;
