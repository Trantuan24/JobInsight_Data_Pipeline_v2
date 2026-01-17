# Data Quality Rules

> Version: 2.0 | Updated: 2026-01-15

## Overview

Tài liệu này mô tả các quy tắc kiểm tra chất lượng dữ liệu trong JobInsight pipeline.

## Quality Gates

### 1. Crawl Validation

Chạy sau khi parse HTML, trước khi lưu vào `raw_jobs`.

| Rule | Threshold | Action |
|------|-----------|--------|
| Job count | >= 50 | Hard fail nếu < 50 |
| Duplicate rate | < 20% | Hard fail nếu >= 20% |
| Valid rate | >= 90% | Success |
| Valid rate | 70% - 90% | Warning (tiếp tục) |
| Valid rate | < 70% | Hard fail |

**Required Fields:**
- `job_id` - phải là số
- `title` - không rỗng
- `company_name` - không null

### 2. Business Rule Validation

Chạy cùng với Crawl Validation, kiểm tra business logic.

| Rule | Threshold | Action |
|------|-----------|--------|
| Salary invalid | min < 0 hoặc max < min | Violation |
| Salary too high | > 200 triệu | Violation |
| Salary suspicious | > 500 triệu | Warning |
| Deadline past | < today | Violation |
| Deadline too far | > 180 ngày | Violation |
| Deadline suspicious | > 90 ngày | Warning |
| Title too short | < 5 ký tự | Violation |
| Company too short | < 3 ký tự | Violation |
| Location invalid | rỗng hoặc N/A | Violation |

**Status:**
- `healthy`: < 5% violations, < 10% warnings
- `degraded`: 5-10% violations hoặc > 10% warnings
- `unhealthy`: > 10% violations → Hard fail

### 3. Staging Validation

Chạy sau ETL transform, trước khi kết thúc pipeline.

| Rule | Threshold | Action |
|------|-----------|--------|
| Job count | >= 50 | Hard fail nếu < 50 |
| Duplicate rate | < 20% | Hard fail nếu >= 20% |
| Valid rate | >= 95% | Success |
| Valid rate | 90% - 95% | Warning (tiếp tục) |
| Valid rate | < 90% | Hard fail |

**Validated Fields:**
- `title_clean` - không null/rỗng
- `company_name_standardized` - không null

## Fail Behavior (Option C - Conditional)

- **SUCCESS**: valid_rate >= success_threshold → tiếp tục pipeline
- **WARNING**: valid_rate >= warning_threshold → log cảnh báo, tiếp tục
- **HARD FAIL**: valid_rate < warning_threshold → dừng pipeline

## Metrics Logging

Validation results được log vào PostgreSQL table `monitoring.quality_metrics`:

| Column | Description |
|--------|-------------|
| validation_type | 'crawl', 'staging', hoặc 'business_rules' |
| total_jobs | Tổng số jobs |
| unique_jobs | Số jobs unique |
| duplicate_rate | Tỷ lệ trùng lặp |
| valid_jobs | Số jobs hợp lệ |
| valid_rate | Tỷ lệ hợp lệ |
| field_missing_rates | JSON chứa chi tiết (hoặc violations cho business_rules) |
| gate_status | 'success', 'warning', 'failed', 'healthy', 'degraded', 'unhealthy' |
| dag_run_id | Airflow run ID |
| created_at | Timestamp |

## Pipeline Flow

crawl → parse → validate_crawl (+ business rules) → upsert_raw → transform_staging → validate_staging → end

## Related Files

- `src/quality/validators.py` - CrawlValidator, StagingValidator, BusinessRuleValidator
- `src/quality/gates.py` - QualityGate logic
- `src/quality/metrics_logger.py` - Log to PostgreSQL
- `sql/schemas/monitoring_schema.sql` - Table schema
