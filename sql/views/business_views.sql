-- ============================================
-- JobInsight - Business Logic Views
-- ============================================

-- ============================================
-- VIEW: vw_jobs_today
-- ============================================
-- All jobs from today's snapshot

CREATE OR REPLACE VIEW vw_jobs_today AS
SELECT 
    j.job_id,
    j.title,
    c.company_name,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.salary_type,
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
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE f.date_id = CURRENT_DATE
GROUP BY j.job_id, j.title, c.company_name, c.verified_employer,
         f.salary_min, f.salary_max, f.salary_type, f.due_date, 
         f.time_remaining, j.skills, j.job_url, f.posted_time
ORDER BY f.due_date ASC;


-- ============================================
-- VIEW: vw_jobs_hanoi
-- ============================================
-- Jobs in Hà Nội

CREATE OR REPLACE VIEW vw_jobs_hanoi AS
SELECT 
    j.job_id,
    j.title,
    c.company_name,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.due_date,
    f.time_remaining,
    j.skills,
    j.job_url
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE f.date_id = CURRENT_DATE
  AND (l.city ILIKE '%Hà Nội%' OR l.city ILIKE '%Hanoi%')
ORDER BY f.due_date ASC;


-- ============================================
-- VIEW: vw_jobs_hcm
-- ============================================
-- Jobs in Hồ Chí Minh

CREATE OR REPLACE VIEW vw_jobs_hcm AS
SELECT 
    j.job_id,
    j.title,
    c.company_name,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.due_date,
    f.time_remaining,
    j.skills,
    j.job_url
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE f.date_id = CURRENT_DATE
  AND (l.city ILIKE '%Hồ Chí Minh%' OR l.city ILIKE '%HCM%' OR l.city ILIKE '%Sài Gòn%')
ORDER BY f.due_date ASC;


-- ============================================
-- VIEW: vw_jobs_expiring_soon
-- ============================================
-- Jobs expiring within 7 days

CREATE OR REPLACE VIEW vw_jobs_expiring_soon AS
SELECT 
    j.job_id,
    j.title,
    c.company_name,
    c.verified_employer,
    f.salary_min,
    f.salary_max,
    f.due_date,
    f.time_remaining,
    DATEDIFF('day', CURRENT_DATE, f.due_date::DATE) AS days_left,
    STRING_AGG(DISTINCT l.city, ', ') AS locations,
    j.job_url
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE f.date_id = CURRENT_DATE
  AND f.due_date >= CURRENT_DATE
  AND f.due_date <= CURRENT_DATE + INTERVAL '7 days'
GROUP BY j.job_id, j.title, c.company_name, c.verified_employer,
         f.salary_min, f.salary_max, f.due_date, f.time_remaining, j.job_url
ORDER BY f.due_date ASC;


-- ============================================
-- VIEW: vw_salary_distribution
-- ============================================
-- Jobs grouped by salary ranges (triệu VND)

CREATE OR REPLACE VIEW vw_salary_distribution AS
SELECT 
    CASE 
        WHEN f.salary_max IS NULL THEN 'Thỏa thuận'
        WHEN f.salary_max <= 10 THEN 'Dưới 10M'
        WHEN f.salary_max <= 15 THEN '10M - 15M'
        WHEN f.salary_max <= 20 THEN '15M - 20M'
        WHEN f.salary_max <= 30 THEN '20M - 30M'
        WHEN f.salary_max <= 50 THEN '30M - 50M'
        ELSE 'Trên 50M'
    END AS salary_range,
    COUNT(DISTINCT f.job_sk) AS job_count,
    COUNT(DISTINCT f.company_sk) AS company_count,
    ROUND(AVG(f.salary_min), 1) AS avg_min,
    ROUND(AVG(f.salary_max), 1) AS avg_max
FROM FactJobPostingDaily f
WHERE f.date_id = CURRENT_DATE
GROUP BY 1
ORDER BY 
    CASE salary_range
        WHEN 'Thỏa thuận' THEN 0
        WHEN 'Dưới 10M' THEN 1
        WHEN '10M - 15M' THEN 2
        WHEN '15M - 20M' THEN 3
        WHEN '20M - 30M' THEN 4
        WHEN '30M - 50M' THEN 5
        ELSE 6
    END;


-- ============================================
-- VIEW: vw_verified_employers
-- ============================================
-- Jobs from verified employers only

CREATE OR REPLACE VIEW vw_verified_employers AS
SELECT 
    j.job_id,
    j.title,
    c.company_name,
    f.salary_min,
    f.salary_max,
    f.due_date,
    STRING_AGG(DISTINCT l.city, ', ') AS locations,
    j.job_url
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
LEFT JOIN FactJobLocationBridge b ON f.fact_id = b.fact_id
LEFT JOIN DimLocation l ON b.location_sk = l.location_sk
WHERE f.date_id = CURRENT_DATE
  AND c.verified_employer = TRUE
GROUP BY j.job_id, j.title, c.company_name, f.salary_min, f.salary_max, f.due_date, j.job_url
ORDER BY f.due_date ASC;


-- ============================================
-- VIEW: vw_location_stats
-- ============================================
-- Job distribution by location

CREATE OR REPLACE VIEW vw_location_stats AS
SELECT 
    l.city,
    COUNT(DISTINCT f.job_sk) AS job_count,
    COUNT(DISTINCT f.company_sk) AS company_count,
    ROUND(AVG(f.salary_min), 1) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 1) AS avg_salary_max,
    COUNT(DISTINCT CASE WHEN c.verified_employer THEN f.job_sk END) AS verified_jobs
FROM DimLocation l
JOIN FactJobLocationBridge b ON l.location_sk = b.location_sk
JOIN FactJobPostingDaily f ON b.fact_id = f.fact_id
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
WHERE f.date_id = CURRENT_DATE
  AND l.city != 'Unknown'
GROUP BY l.city
ORDER BY job_count DESC;


-- ============================================
-- VIEW: vw_company_stats
-- ============================================
-- Company hiring statistics

CREATE OR REPLACE VIEW vw_company_stats AS
SELECT 
    c.company_name,
    c.verified_employer,
    COUNT(DISTINCT f.job_sk) AS active_jobs,
    ROUND(AVG(f.salary_min), 1) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 1) AS avg_salary_max,
    MIN(f.due_date) AS earliest_deadline
FROM DimCompany c
JOIN FactJobPostingDaily f ON c.company_sk = f.company_sk
WHERE c.is_current = TRUE
  AND f.date_id = CURRENT_DATE
GROUP BY c.company_name, c.verified_employer
ORDER BY active_jobs DESC;


-- ============================================
-- VIEW: vw_daily_summary
-- ============================================
-- Daily summary metrics

CREATE OR REPLACE VIEW vw_daily_summary AS
SELECT 
    f.date_id,
    COUNT(DISTINCT f.job_sk) AS total_jobs,
    COUNT(DISTINCT f.company_sk) AS total_companies,
    COUNT(DISTINCT CASE WHEN c.verified_employer THEN f.company_sk END) AS verified_companies,
    ROUND(AVG(f.salary_min), 1) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 1) AS avg_salary_max,
    COUNT(DISTINCT CASE WHEN f.salary_min IS NOT NULL THEN f.job_sk END) AS jobs_with_salary
FROM FactJobPostingDaily f
JOIN DimCompany c ON f.company_sk = c.company_sk AND c.is_current = TRUE
GROUP BY f.date_id
ORDER BY f.date_id DESC;


-- ============================================
-- VIEW: vw_skills_demand
-- ============================================
-- Most demanded skills (from JSON array)

CREATE OR REPLACE VIEW vw_skills_demand AS
SELECT 
    skill,
    COUNT(*) AS job_count,
    ROUND(AVG(f.salary_min), 1) AS avg_salary_min,
    ROUND(AVG(f.salary_max), 1) AS avg_salary_max
FROM FactJobPostingDaily f
JOIN DimJob j ON f.job_sk = j.job_sk AND j.is_current = TRUE,
UNNEST(CAST(j.skills AS VARCHAR[])) AS t(skill)
WHERE f.date_id = CURRENT_DATE
  AND skill IS NOT NULL
  AND skill != ''
GROUP BY skill
ORDER BY job_count DESC
LIMIT 50;
