# JobInsight - Data Contracts

## Tổng quan

Data Contracts định nghĩa "hợp đồng" giữa data producer và consumer, bao gồm:
- **Schema**: Cấu trúc dữ liệu, kiểu dữ liệu, constraints
- **Quality**: Quy tắc chất lượng dữ liệu
- **SLA**: Service Level Agreement (freshness, availability)

## Chuẩn sử dụng

Các contracts trong thư mục này tuân theo [Open Data Contract Standard (ODCS) v3.1](https://bitol-io.github.io/open-data-contract-standard/latest/).

## Danh sách Contracts

| Contract | Table | Mô tả |
|----------|-------|-------|
| [fact_job_posting.yaml](fact_job_posting.yaml) | FactJobPostingDaily | Fact table chính |
| [bridge_job_location.yaml](bridge_job_location.yaml) | FactJobLocationBridge | Bridge table (M:N) |
| [dim_job.yaml](dim_job.yaml) | DimJob | Dimension tin tuyển dụng |
| [dim_company.yaml](dim_company.yaml) | DimCompany | Dimension công ty |
| [dim_location.yaml](dim_location.yaml) | DimLocation | Dimension địa điểm |
| [dim_date.yaml](dim_date.yaml) | DimDate | Dimension thời gian |
| [staging_jobs.yaml](staging_jobs.yaml) | staging_jobs | Staging layer |

## Cách sử dụng

1. **Validate schema**: Kiểm tra data types, constraints
2. **Quality checks**: Chạy quality rules trong ETL pipeline
3. **Documentation**: Tham khảo khi phát triển/debug

## Tài liệu tham khảo

- [Open Data Contract Standard (ODCS)](https://bitol-io.github.io/open-data-contract-standard/latest/)
- [PayPal Data Contract Template](https://github.com/paypal/data-contract-template)
- [Data Mesh Manager - What is a Data Contract](https://datamesh-manager.com/learn/what-is-a-data-contract)
