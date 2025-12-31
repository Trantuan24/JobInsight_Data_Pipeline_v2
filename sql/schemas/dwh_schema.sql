-- ============================================
-- JobInsight - Data Warehouse Schema (DuckDB)
-- ============================================
-- Purpose: Star Schema for analytics
-- Database: DuckDB
-- Run order: Executed by Python (not PostgreSQL)
-- ============================================
-- NOTE: This file is for DuckDB, not PostgreSQL
-- Executed by: src/etl/warehouse/pipeline.py
-- ============================================

-- ============================================
-- SEQUENCES (Auto-increment keys)
-- ============================================

CREATE SEQUENCE IF NOT EXISTS seq_dim_job_sk START 10000;
CREATE SEQUENCE IF NOT EXISTS seq_dim_company_sk START 10000;
CREATE SEQUENCE IF NOT EXISTS seq_dim_location_sk START 10000;
CREATE SEQUENCE IF NOT EXISTS seq_fact_id START 10000;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- DimJob: Job postings with SCD Type 2
CREATE TABLE IF NOT EXISTS DimJob (
    job_sk INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_dim_job_sk'),
    job_id VARCHAR(20) NOT NULL UNIQUE,
    title_clean VARCHAR(255) NOT NULL,
    job_url TEXT,
    skills JSON,
    last_update VARCHAR(100),
    logo_url TEXT,
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- DimCompany: Companies with SCD Type 2
CREATE TABLE IF NOT EXISTS DimCompany (
    company_sk INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_dim_company_sk'),
    company_name_standardized VARCHAR(200) NOT NULL,
    company_url TEXT,
    verified_employer BOOLEAN DEFAULT FALSE,
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- DimLocation: Locations (composite natural key)
CREATE TABLE IF NOT EXISTS DimLocation (
    location_sk INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_dim_location_sk'),
    province VARCHAR(100),
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100),
    
    -- SCD Type 2 columns (usually static for locations)
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    
    -- Composite natural key
    UNIQUE (province, city, district)
);

-- DimDate: Date dimension
CREATE TABLE IF NOT EXISTS DimDate (
    date_id DATE PRIMARY KEY,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday VARCHAR(10) NOT NULL
);

-- ============================================
-- FACT TABLES
-- ============================================

-- FactJobPostingDaily: Main fact table (daily grain)
-- Grain: One job posting for one calendar day
CREATE TABLE IF NOT EXISTS FactJobPostingDaily (
    fact_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_fact_id'),
    
    -- Dimension foreign keys
    job_sk INTEGER NOT NULL,
    company_sk INTEGER NOT NULL,
    date_id DATE NOT NULL,
    
    -- Measures
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_type VARCHAR(20),
    due_date TIMESTAMP,
    time_remaining TEXT,
    verified_employer BOOLEAN DEFAULT FALSE,
    posted_time TIMESTAMP,
    crawled_at TIMESTAMP NOT NULL,
    
    -- Partition key
    load_month VARCHAR(7) NOT NULL,  -- Format: YYYY-MM
    
    -- Constraints (FK constraints added in migration 002)
    UNIQUE (job_sk, date_id)  -- Prevent duplicate facts
);

-- FactJobLocationBridge: Many-to-many (Job × Location)
CREATE TABLE IF NOT EXISTS FactJobLocationBridge (
    fact_id INTEGER NOT NULL,
    location_sk INTEGER NOT NULL,
    
    -- Composite primary key
    PRIMARY KEY (fact_id, location_sk)
    
    -- FK constraints added in migration 002
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Dimension indexes
CREATE INDEX IF NOT EXISTS idx_dimjob_current 
    ON DimJob(is_current);

CREATE INDEX IF NOT EXISTS idx_dimjob_job_id 
    ON DimJob(job_id) WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dimcompany_current 
    ON DimCompany(is_current);

CREATE INDEX IF NOT EXISTS idx_dimcompany_name 
    ON DimCompany(company_name_standardized) WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dimlocation_current 
    ON DimLocation(is_current);

CREATE INDEX IF NOT EXISTS idx_dimlocation_city 
    ON DimLocation(city) WHERE is_current = TRUE;

-- Fact table indexes
CREATE INDEX IF NOT EXISTS idx_fact_date 
    ON FactJobPostingDaily(date_id DESC);

CREATE INDEX IF NOT EXISTS idx_fact_load_month 
    ON FactJobPostingDaily(load_month);

CREATE INDEX IF NOT EXISTS idx_fact_job_date 
    ON FactJobPostingDaily(job_sk, date_id);

CREATE INDEX IF NOT EXISTS idx_fact_company_date 
    ON FactJobPostingDaily(company_sk, date_id);

CREATE INDEX IF NOT EXISTS idx_fact_salary 
    ON FactJobPostingDaily(salary_min, salary_max) 
    WHERE salary_min IS NOT NULL;

-- Bridge table index
CREATE INDEX IF NOT EXISTS idx_bridge_fact 
    ON FactJobLocationBridge(fact_id);

CREATE INDEX IF NOT EXISTS idx_bridge_location 
    ON FactJobLocationBridge(location_sk);

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON TABLE DimJob IS 
    'Job dimension with SCD Type 2. Tracks job title, skills, URL changes over time.';

COMMENT ON TABLE DimCompany IS 
    'Company dimension with SCD Type 2. Tracks company info changes.';

COMMENT ON TABLE DimLocation IS 
    'Location dimension with composite key (province, city, district).';

COMMENT ON TABLE FactJobPostingDaily IS 
    'Main fact table. Grain: One job posting for one calendar day. Partitioned by load_month.';

COMMENT ON TABLE FactJobLocationBridge IS 
    'Bridge table for many-to-many relationship between jobs and locations.';
