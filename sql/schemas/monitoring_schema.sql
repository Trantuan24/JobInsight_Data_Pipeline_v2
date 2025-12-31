-- ============================================
-- JobInsight - Monitoring Schema
-- ============================================

\c jobinsight;

-- Pipeline runs
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(50) NOT NULL,
    task_id VARCHAR(50) NOT NULL,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    execution_date TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(20) NOT NULL,
    duration_seconds FLOAT,
    records_processed INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_failed INTEGER,
    error_message TEXT,
    error_type VARCHAR(100),
    data_quality_score FLOAT,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_dag_date ON monitoring.pipeline_runs(dag_id, run_date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON monitoring.pipeline_runs(status, execution_date DESC);

-- Metrics
CREATE TABLE IF NOT EXISTS monitoring.metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) NOT NULL,
    metric_category VARCHAR(30),
    metric_value FLOAT NOT NULL,
    metric_unit VARCHAR(20),
    metadata JSONB,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON monitoring.metrics(metric_name, recorded_at DESC);

-- Data lineage
CREATE TABLE IF NOT EXISTS monitoring.data_lineage (
    lineage_id SERIAL PRIMARY KEY,
    source_system VARCHAR(50) NOT NULL,
    source_table VARCHAR(50) NOT NULL,
    source_key VARCHAR(100) NOT NULL,
    target_table VARCHAR(50) NOT NULL,
    target_key VARCHAR(100) NOT NULL,
    transformation VARCHAR(100),
    metadata JSONB,
    load_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_lineage_target ON monitoring.data_lineage(target_table, target_key);

-- Data quality
CREATE TABLE IF NOT EXISTS monitoring.data_quality (
    check_id SERIAL PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(30) NOT NULL,
    table_name VARCHAR(50),
    status VARCHAR(20) NOT NULL,
    expected_value FLOAT,
    actual_value FLOAT,
    threshold FLOAT,
    variance FLOAT,
    details TEXT,
    metadata JSONB,
    checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_status_time ON monitoring.data_quality(status, checked_at DESC);

-- Views
CREATE OR REPLACE VIEW monitoring.vw_pipeline_health AS
SELECT 
    dag_id,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_runs,
    ROUND(AVG(duration_seconds)::NUMERIC, 2) as avg_duration_sec,
    MAX(execution_date) as last_run
FROM monitoring.pipeline_runs
WHERE execution_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY dag_id;

-- Grant to jobinsight
GRANT ALL ON ALL TABLES IN SCHEMA monitoring TO jobinsight;
GRANT ALL ON ALL SEQUENCES IN SCHEMA monitoring TO jobinsight;

\echo '✅ Monitoring schema created'
