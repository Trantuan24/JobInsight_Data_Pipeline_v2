-- ============================================
-- JobInsight - Data Warehouse Schema (DuckDB)
-- ============================================

-- ============================================
-- DROP EXISTING OBJECTS (for clean re-run)
-- ============================================

DROP INDEX IF EXISTS idx_bridge_location;
DROP INDEX IF EXISTS idx_bridge_fact;
DROP INDEX IF EXISTS idx_fact_load_month;
DROP INDEX IF EXISTS idx_fact_date;
DROP INDEX IF EXISTS idx_dimlocation_city;
DROP INDEX IF EXISTS idx_dimcompany_hash;
DROP INDEX IF EXISTS uq_dimcompany_current;
DROP INDEX IF EXISTS idx_dimjob_jobid;
DROP INDEX IF EXISTS uq_dimjob_current;

DROP TABLE IF EXISTS FactJobLocationBridge;
DROP TABLE IF EXISTS FactJobPostingDaily;
DROP TABLE IF EXISTS DimJob;
DROP TABLE IF EXISTS DimCompany;
DROP TABLE IF EXISTS DimLocation;
DROP TABLE IF EXISTS DimDate;

DROP SEQUENCE IF EXISTS seq_bridge_id;
DROP SEQUENCE IF EXISTS seq_fact_id;
DROP SEQUENCE IF EXISTS seq_dim_location_sk;
DROP SEQUENCE IF EXISTS seq_dim_company_sk;
DROP SEQUENCE IF EXISTS seq_dim_job_sk;

-- ============================================
-- SEQUENCES (Auto-increment keys)
-- ============================================

CREATE SEQUENCE seq_dim_job_sk START 1;
CREATE SEQUENCE seq_dim_company_sk START 1;
CREATE SEQUENCE seq_dim_location_sk START 1;
CREATE SEQUENCE seq_fact_id START 1;
CREATE SEQUENCE seq_bridge_id START 1;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- DimJob: Job postings with SCD Type 2
CREATE TABLE DimJob (
    job_sk INTEGER PRIMARY KEY,
    job_id VARCHAR(20) NOT NULL,           -- Business key từ TopCV
    title VARCHAR(255),
    job_url TEXT,
    skills JSON,
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,          -- Ngày bắt đầu hiệu lực
    expiry_date DATE,                      -- NULL = current record
    is_current BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX uq_dimjob_current 
    ON DimJob(job_id) WHERE is_current = TRUE;

CREATE INDEX idx_dimjob_jobid 
    ON DimJob(job_id);


-- DimCompany: Companies with SCD Type 2
CREATE TABLE DimCompany (
    company_sk INTEGER PRIMARY KEY,
    company_bk_hash VARCHAR(64) NOT NULL,  -- MD5(LOWER(company_name)) - Business key
    company_name VARCHAR(200) NOT NULL,
    company_url TEXT,
    logo_url TEXT,
    verified_employer BOOLEAN DEFAULT FALSE,
    
    -- SCD Type 2 columns
    effective_date DATE NOT NULL,
    expiry_date DATE,                      -- NULL = current record
    is_current BOOLEAN DEFAULT TRUE
);

CREATE UNIQUE INDEX uq_dimcompany_current 
    ON DimCompany(company_bk_hash) WHERE is_current = TRUE;

CREATE INDEX idx_dimcompany_hash 
    ON DimCompany(company_bk_hash);


-- DimLocation: Locations (city level)
CREATE TABLE DimLocation (
    location_sk INTEGER PRIMARY KEY,       -- -1 = Unknown
    city VARCHAR(100) NOT NULL,
    country VARCHAR(50) DEFAULT 'Vietnam',
    UNIQUE (city, country)
);

CREATE INDEX idx_dimlocation_city 
    ON DimLocation(city);


-- DimDate: Date dimension (data-driven range)
CREATE TABLE DimDate (
    date_id DATE PRIMARY KEY,
    day INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,          -- 1=Monday, 7=Sunday
    weekday_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    year_month VARCHAR(7) NOT NULL,        -- 'YYYY-MM'
    quarter_name VARCHAR(2) NOT NULL       -- 'Q1', 'Q2', 'Q3', 'Q4'
);

-- ============================================
-- FACT TABLES
-- ============================================

-- FactJobPostingDaily: Main fact table
-- Grain: 1 job × 1 day (Pure Periodic Snapshot)
CREATE TABLE FactJobPostingDaily (
    fact_id INTEGER PRIMARY KEY,
    job_sk INTEGER NOT NULL,               -- FK → DimJob
    company_sk INTEGER NOT NULL,           -- FK → DimCompany
    date_id DATE NOT NULL,                 -- FK → DimDate
    
    -- Date FKs
    posted_date_id DATE,                   -- FK → DimDate
    due_date_id DATE,                      -- FK → DimDate
    
    -- Measures
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_type VARCHAR(20),               -- 'range', 'upto', 'from', 'negotiable'
    time_remaining TEXT,
    
    -- Timestamps
    posted_time TIMESTAMP,
    due_date TIMESTAMP,
    crawled_at TIMESTAMP NOT NULL,
    
    -- Partition key
    load_month VARCHAR(7) NOT NULL,        -- 'YYYY-MM'
    
    CONSTRAINT uq_fact_job_date UNIQUE (job_sk, date_id)
);

CREATE INDEX idx_fact_date 
    ON FactJobPostingDaily(date_id);

CREATE INDEX idx_fact_load_month 
    ON FactJobPostingDaily(load_month);


-- FactJobLocationBridge: Many-to-many (Fact × Location)
CREATE TABLE FactJobLocationBridge (
    bridge_id INTEGER PRIMARY KEY,
    fact_id INTEGER NOT NULL,              -- FK → FactJobPostingDaily
    location_sk INTEGER NOT NULL,          -- FK → DimLocation
    
    CONSTRAINT uq_bridge UNIQUE (fact_id, location_sk)
);

CREATE INDEX idx_bridge_fact 
    ON FactJobLocationBridge(fact_id);

CREATE INDEX idx_bridge_location 
    ON FactJobLocationBridge(location_sk);

-- ============================================
-- DEFAULT RECORDS
-- ============================================

INSERT INTO DimLocation (location_sk, city, country) 
VALUES (-1, 'Unknown', 'Unknown');
