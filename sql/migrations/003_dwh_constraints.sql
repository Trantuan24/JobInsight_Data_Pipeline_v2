-- ============================================
-- Migration 002b: DuckDB Warehouse Constraints
-- ============================================
-- Purpose: Add FK and CHECK constraints to DWH
-- Database: DuckDB (NOT PostgreSQL)
-- Executed by: Python script after schema creation
-- ============================================
-- Run AFTER dwh_schema.sql and initial dimension load
-- ============================================

-- ============================================
-- FACT TABLE: Enable Foreign Keys
-- ============================================

-- Enable FK constraints for FactJobPostingDaily
ALTER TABLE FactJobPostingDaily
    ADD CONSTRAINT fk_fact_job 
        FOREIGN KEY (job_sk) REFERENCES DimJob(job_sk),
    
    ADD CONSTRAINT fk_fact_company 
        FOREIGN KEY (company_sk) REFERENCES DimCompany(company_sk),
    
    ADD CONSTRAINT fk_fact_date 
        FOREIGN KEY (date_id) REFERENCES DimDate(date_id);

-- Enable FK constraints for FactJobLocationBridge
ALTER TABLE FactJobLocationBridge
    ADD CONSTRAINT fk_bridge_fact 
        FOREIGN KEY (fact_id) REFERENCES FactJobPostingDaily(fact_id) ON DELETE CASCADE,
    
    ADD CONSTRAINT fk_bridge_location 
        FOREIGN KEY (location_sk) REFERENCES DimLocation(location_sk);

-- ============================================
-- FACT TABLE: Data Validation Constraints
-- ============================================

-- Salary validation
ALTER TABLE FactJobPostingDaily
    ADD CONSTRAINT chk_fact_salary_range 
        CHECK (salary_max IS NULL OR salary_min IS NULL OR salary_max >= salary_min),
    
    ADD CONSTRAINT chk_fact_salary_positive 
        CHECK (salary_min IS NULL OR salary_min >= 0);

-- Load month format validation
ALTER TABLE FactJobPostingDaily
    ADD CONSTRAINT chk_fact_load_month_format 
        CHECK (load_month ~ '^\d{4}-\d{2}$');

-- Date validation
ALTER TABLE FactJobPostingDaily
    ADD CONSTRAINT chk_fact_date_reasonable 
        CHECK (date_id >= DATE '2020-01-01' AND date_id <= DATE '2030-12-31');

-- ============================================
-- DIMENSION: SCD Type 2 Validation
-- ============================================

-- DimJob: Ensure SCD2 consistency
ALTER TABLE DimJob
    ADD CONSTRAINT chk_dimjob_scd2_dates 
        CHECK (expiry_date IS NULL OR expiry_date > effective_date),
    
    ADD CONSTRAINT chk_dimjob_current_no_expiry 
        CHECK (NOT is_current OR expiry_date IS NULL);

-- DimCompany: Ensure SCD2 consistency
ALTER TABLE DimCompany
    ADD CONSTRAINT chk_dimcompany_scd2_dates 
        CHECK (expiry_date IS NULL OR expiry_date > effective_date),
    
    ADD CONSTRAINT chk_dimcompany_current_no_expiry 
        CHECK (NOT is_current OR expiry_date IS NULL);

-- DimLocation: Ensure SCD2 consistency
ALTER TABLE DimLocation
    ADD CONSTRAINT chk_dimlocation_scd2_dates 
        CHECK (expiry_date IS NULL OR expiry_date > effective_date),
    
    ADD CONSTRAINT chk_dimlocation_current_no_expiry 
        CHECK (NOT is_current OR expiry_date IS NULL);

-- ============================================
-- COMPLETION
-- ============================================

SELECT '✅ Migration 002b completed: DWH constraints added' AS message;
SELECT '   - Foreign key constraints enabled' AS message;
SELECT '   - CHECK constraints for data validation' AS message;
SELECT '   - SCD Type 2 consistency checks' AS message;
