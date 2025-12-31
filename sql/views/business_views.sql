-- ============================================
-- JobInsight - Business Logic Views
-- ============================================
-- Purpose: Views with business logic built-in
-- Database: DuckDB
-- ============================================
-- These views encapsulate business rules
-- Source of truth for analytics queries
-- ============================================

-- ============================================
-- VIEW: vw_top_10_jobs_hn
-- ============================================
-- Top 10 jobs in Hà Nội, salary 10-15M, sorted by deadline
-- Business requirement: Daily notification to Discord

CREATE OR REPLACE VIEW vw_top_10_jobs_hn AS
SELECT 
    j.job_id,
    j.title_clean,
    c.company_name_standardized,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.salary_type,
    f.due_date,
    f.time_remaining,
    f.posted_time,
    j.job_url,
    c.company_url,
    j.skills,
    STRING_AGG(DISTINCT l.city || COALESCE(' - ' || l.district, ''), ', ') AS locations,
    DATEDIFF('day', CURRENT_DATE, f.due_date) AS days_until_deadline
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk
JOIN DimCompany c ON f.company_sk = c.company_sk
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE 
    -- Current records only
    j.is_current = TRUE
    AND c.is_current = TRUE
    AND l.is_current = TRUE
    
    -- Today's snapshot
    AND f.date_id = CURRENT_DATE
    
    -- Salary range: 10-15M VND
    AND f.salary_min >= 10.0
    AND f.salary_max <= 15.0
    AND f.salary_min IS NOT NULL
    AND f.salary_max IS NOT NULL
    
    -- Location: Hà Nội
    AND (l.city ILIKE '%Hà Nội%' OR l.city ILIKE '%Hanoi%' OR l.province ILIKE '%Hà Nội%')
    
    -- Not expired
    AND f.due_date >= CURRENT_DATE
    
GROUP BY j.job_id, j.title_clean, c.company_name_standardized, c.verified_employer,
         f.salary_min, f.salary_max, f.salary_type, f.due_date, f.time_remaining,
         f.posted_time, j.job_url, c.company_url, j.skills
         
ORDER BY f.due_date ASC  -- Closest deadline first
LIMIT 10;

COMMENT ON VIEW vw_top_10_jobs_hn IS 
    'Top 10 jobs in Hà Nội (salary 10-15M VND) sorted by deadline. Used for Discord bot daily notification.';


-- ============================================
-- VIEW: vw_jobs_expiring_soon
-- ============================================
-- Jobs expiring within 7 days

CREATE OR REPLACE VIEW vw_jobs_expiring_soon AS
SELECT 
    j.job_id,
    j.title_clean,
    c.company_name_standardized,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.due_date,
    f.time_remaining,
    DATEDIFF('day', CURRENT_DATE, f.due_date) AS days_until_deadline,
    STRING_AGG(DISTINCT l.city, ', ') AS locations,
    j.job_url
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk AND l.is_current = TRUE
WHERE 
    f.date_id = CURRENT_DATE
    AND f.due_date >= CURRENT_DATE
    AND f.due_date <= CURRENT_DATE + INTERVAL '7 days'
GROUP BY j.job_id, j.title_clean, c.company_name_standardized, c.verified_employer,
         f.salary_min, f.salary_max, f.due_date, f.time_remaining, j.job_url
ORDER BY f.due_date ASC;

COMMENT ON VIEW vw_jobs_expiring_soon IS 
    'Jobs with deadline within next 7 days. Urgent applications.';


-- ============================================
-- VIEW: vw_jobs_by_salary_range
-- ============================================
-- Jobs grouped by salary ranges

CREATE OR REPLACE VIEW vw_jobs_by_salary_range AS
SELECT 
    CASE 
        WHEN f.salary_max <= 10.0 THEN 'Dưới 10M'
        WHEN f.salary_max <= 15.0 THEN '10M - 15M'
        WHEN f.salary_max <= 20.0 THEN '15M - 20M'
        WHEN f.salary_max <= 30.0 THEN '20M - 30M'
        WHEN f.salary_max <= 50.0 THEN '30M - 50M'
        ELSE 'Trên 50M'
    END AS salary_range,
    COUNT(DISTINCT f.job_sk) AS job_count,
    ROUND(AVG(f.salary_min), 2) AS avg_min,
    ROUND(AVG(f.salary_max), 2) AS avg_max,
    COUNT(DISTINCT f.company_sk) AS company_count
FROM FactJobPostingDaily f
WHERE f.date_id = CURRENT_DATE
  AND f.salary_min IS NOT NULL
  AND f.salary_max IS NOT NULL
GROUP BY 1
ORDER BY 
    CASE 
        WHEN salary_range = 'Dưới 10M' THEN 1
        WHEN salary_range = '10M - 15M' THEN 2
        WHEN salary_range = '15M - 20M' THEN 3
        WHEN salary_range = '20M - 30M' THEN 4
        WHEN salary_range = '30M - 50M' THEN 5
        ELSE 6
    END;

COMMENT ON VIEW vw_jobs_by_salary_range IS 
    'Job distribution across salary ranges. Updated daily.';


-- ============================================
-- VIEW: vw_verified_jobs
-- ============================================
-- Jobs from verified employers only

CREATE OR REPLACE VIEW vw_verified_jobs AS
SELECT 
    j.job_id,
    j.title_clean,
    c.company_name_standardized,
    f.salary_min,
    f.salary_max,
    f.due_date,
    f.time_remaining,
    STRING_AGG(DISTINCT l.city, ', ') AS locations,
    j.skills,
    j.job_url,
    f.posted_time
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk AND l.is_current = TRUE
WHERE 
    f.date_id = CURRENT_DATE
    AND c.verified_employer = TRUE
    AND f.due_date >= CURRENT_DATE
GROUP BY j.job_id, j.title_clean, c.company_name_standardized, f.salary_min, 
         f.salary_max, f.due_date, f.time_remaining, j.skills, j.job_url, f.posted_time
ORDER BY f.due_date ASC;

COMMENT ON VIEW vw_verified_jobs IS 
    'Jobs from verified employers only. Higher trust level.';


-- ============================================
-- VIEW: vw_jobs_by_location
-- ============================================
-- Jobs aggregated by location (city level)

CREATE OR REPLACE VIEW vw_jobs_by_location AS
SELECT 
    l.city,
    COALESCE(l.province, 'Unknown') AS province,
    COUNT(DISTINCT f.job_sk) AS job_count,
    COUNT(DISTINCT f.company_sk) AS company_count,
    ROUND(AVG(f.salary_min), 2) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 2) AS avg_salary_max,
    COUNT(*) FILTER (WHERE c.verified_employer = TRUE) AS verified_jobs
FROM DimLocation l
JOIN FactJobLocationBridge b ON l.location_sk = b.location_sk
JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
WHERE l.is_current = TRUE
  AND f.date_id = CURRENT_DATE
GROUP BY l.city, l.province
ORDER BY job_count DESC;

COMMENT ON VIEW vw_jobs_by_location IS 
    'Job distribution by location (city level). Daily snapshot.';


-- ============================================
-- COMPLETION
-- ============================================

SELECT '✅ Business views created' AS message;
SELECT '   - vw_top_10_jobs_hn (Discord bot)' AS message;
SELECT '   - vw_jobs_expiring_soon' AS message;
SELECT '   - vw_jobs_by_salary_range' AS message;
SELECT '   - vw_verified_jobs' AS message;
SELECT '   - vw_jobs_by_location' AS message;
