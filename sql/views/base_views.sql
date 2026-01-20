-- ============================================
-- JobInsight - Base Dimensional Views
-- ============================================

-- ============================================
-- VIEW: vw_current_jobs
-- ============================================
-- Current jobs with company information (SCD2 filtered)

CREATE OR REPLACE VIEW vw_current_jobs AS
SELECT 
    j.job_sk,
    j.job_id,
    j.title,
    j.job_url,
    j.skills,
    c.company_sk,
    c.company_name,
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


-- ============================================
-- VIEW: vw_job_locations
-- ============================================
-- Jobs with locations denormalized

CREATE OR REPLACE VIEW vw_job_locations AS
SELECT 
    f.fact_id,
    f.job_sk,
    j.job_id,
    j.title,
    f.date_id,
    l.location_sk,
    l.city,
    l.country
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE j.is_current = TRUE;


-- ============================================
-- VIEW: vw_monthly_stats
-- ============================================
-- Monthly job statistics

CREATE OR REPLACE VIEW vw_monthly_stats AS 
SELECT 
    f.load_month,
    COUNT(DISTINCT f.job_sk) AS job_count,
    COUNT(DISTINCT f.company_sk) AS company_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max,
    COUNT(DISTINCT CASE WHEN c.verified_employer THEN f.job_sk END) AS verified_jobs
FROM FactJobPostingDaily f
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
GROUP BY f.load_month
ORDER BY f.load_month DESC;


-- ============================================
-- VIEW: vw_top_companies
-- ============================================
-- Companies ranked by job posting count

CREATE OR REPLACE VIEW vw_top_companies AS
SELECT 
    c.company_sk,
    c.company_name,
    c.company_url,
    c.verified_employer,
    COUNT(DISTINCT f.job_sk) AS job_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max,
    MAX(f.date_id) AS last_posting_date
FROM DimCompany c
JOIN FactJobPostingDaily f ON c.company_sk = f.company_sk
WHERE c.is_current = TRUE
GROUP BY c.company_sk, c.company_name, c.company_url, c.verified_employer
ORDER BY job_count DESC;


-- ============================================
-- VIEW: vw_top_locations
-- ============================================
-- Locations ranked by job count

CREATE OR REPLACE VIEW vw_top_locations AS
SELECT 
    l.location_sk,
    l.city,
    l.country,
    COUNT(DISTINCT f.job_sk) AS job_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max
FROM DimLocation l
JOIN FactJobLocationBridge b ON l.location_sk = b.location_sk
JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
GROUP BY l.location_sk, l.city, l.country
ORDER BY job_count DESC;


-- ============================================
-- VIEW: vw_job_full_details
-- ============================================
-- Complete job information

CREATE OR REPLACE VIEW vw_job_full_details AS
SELECT 
    j.job_id,
    j.title,
    c.company_name,
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
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE j.is_current = TRUE
  AND c.is_current = TRUE
GROUP BY j.job_id, j.title, c.company_name, c.verified_employer,
         f.date_id, f.salary_min, f.salary_max, f.salary_type, f.due_date, 
         f.time_remaining, f.posted_time, j.skills, j.job_url, c.company_url, f.load_month;
