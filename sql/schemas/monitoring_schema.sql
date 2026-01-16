-- ============================================
-- JobInsight - Monitoring Schema
-- ============================================

\c jobinsight;

-- ETL Metrics - Track pipeline performance
CREATE TABLE IF NOT EXISTS monitoring.etl_metrics (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(50) NOT NULL,
    task_id VARCHAR(50) NOT NULL,
    dag_run_id VARCHAR(100),
    status VARCHAR(20) NOT NULL,  -- success, failed
    duration_seconds FLOAT,
    rows_in INTEGER,
    rows_out INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_failed INTEGER,
    throughput FLOAT,  -- rows/sec
    error_message TEXT,
    metadata JSONB,  -- extra stats (dim counts, etc.)
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etl_metrics_dag ON monitoring.etl_metrics(dag_id, task_id);
CREATE INDEX IF NOT EXISTS idx_etl_metrics_time ON monitoring.etl_metrics(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_etl_metrics_status ON monitoring.etl_metrics(status);

-- Quality Metrics - Track data quality validation
CREATE TABLE IF NOT EXISTS monitoring.quality_metrics (
    id SERIAL PRIMARY KEY,
    validation_type VARCHAR(20) NOT NULL,  -- 'crawl', 'staging', 'business_rules'
    dag_run_id VARCHAR(100),
    total_jobs INT NOT NULL,
    unique_jobs INT NOT NULL,
    duplicate_count INT NOT NULL,
    duplicate_rate DECIMAL(5,4) NOT NULL,
    valid_jobs INT NOT NULL,
    invalid_jobs INT NOT NULL,
    valid_rate DECIMAL(5,4) NOT NULL,
    field_missing_rates JSONB,  -- or violations for business_rules
    raw_count INT,
    data_loss_rate DECIMAL(5,4),
    gate_status VARCHAR(20) NOT NULL,  -- success, warning, failed, healthy, degraded, unhealthy
    gate_message TEXT,
    run_timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_qm_type ON monitoring.quality_metrics(validation_type);
CREATE INDEX IF NOT EXISTS idx_qm_timestamp ON monitoring.quality_metrics(run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_qm_status ON monitoring.quality_metrics(gate_status);

-- Views
CREATE OR REPLACE VIEW monitoring.vw_etl_health AS
SELECT 
    dag_id,
    task_id,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs,
    ROUND(AVG(duration_seconds)::NUMERIC, 2) as avg_duration_sec,
    ROUND(AVG(rows_out)::NUMERIC, 0) as avg_rows_out,
    MAX(started_at) as last_run
FROM monitoring.etl_metrics
WHERE started_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dag_id, task_id;

CREATE OR REPLACE VIEW monitoring.vw_quality_health AS
SELECT 
    validation_type,
    COUNT(*) as total_checks,
    SUM(CASE WHEN gate_status IN ('success', 'healthy') THEN 1 ELSE 0 END) as passed,
    ROUND(AVG(valid_rate)::NUMERIC, 4) as avg_valid_rate,
    MAX(run_timestamp) as last_check
FROM monitoring.quality_metrics
WHERE run_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY validation_type;

-- Grant permissions
GRANT ALL ON ALL TABLES IN SCHEMA monitoring TO jobinsight;
GRANT ALL ON ALL SEQUENCES IN SCHEMA monitoring TO jobinsight;

\echo '✅ Monitoring schema created (etl_metrics + quality_metrics)'
