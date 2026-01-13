# JobInsight - Enterprise Bus Matrix

## Tổng quan

Bus Matrix là công cụ lập kế hoạch Data Warehouse theo phương pháp Kimball. Matrix thể hiện mối quan hệ giữa Business Process (Fact) và Dimensions.

**Nguồn dữ liệu**: TopCV (trang tuyển dụng IT Việt Nam)

---

## Bus Matrix

| Business Process | DimDate | DimJob | DimCompany | DimLocation |
|------------------|:-------:|:------:|:----------:|:-----------:|
| **Job Posting Tracking** | ✓ | ✓ | ✓ | ✓* |

> ✓ = FK trực tiếp | ✓* = Qua Bridge Table (many-to-many)

**Note:** Phase 1 chỉ có 1 fact table. Hướng mở rộng: Application Tracking, Salary Analysis (khi có thêm data sources).

---

## Fact Table

### FactJobPostingDaily

| Thuộc tính | Giá trị |
|------------|---------|
| **Grain** | 1 job × 1 ngày |
| **Fact Type** | Periodic Snapshot |
| **Mô tả** | Chụp trạng thái tin tuyển dụng theo ngày |
| **Load** | Daily, 5 ngày/job (1 observed + 4 projected) |

**Foreign Keys:**

| FK | Dimension | Role |
|----|-----------|------|
| date_id | DimDate | Ngày tracking |
| posted_date_id | DimDate | Ngày đăng tin |
| due_date_id | DimDate | Ngày hết hạn |
| job_sk | DimJob | Tin tuyển dụng |
| company_sk | DimCompany | Công ty |
| → Bridge | DimLocation | Địa điểm (M:N) |

**Measures:**

| Measure | Type | Mô tả |
|---------|------|-------|
| salary_min | Semi-additive | Lương tối thiểu (VND) |
| salary_max | Semi-additive | Lương tối đa (VND) |
| is_observed | Non-additive | TRUE=crawled, FALSE=projected |

**Degenerate Dimensions:**

| Column | Mô tả |
|--------|-------|
| salary_type | 'range', 'upto', 'from', 'negotiable' |
| time_remaining | Text: "Còn X ngày để ứng tuyển" |
| load_month | Partition key: 'YYYY-MM' |

---

## Dimensions

### DimDate (Role-Playing)

| Thuộc tính | Giá trị |
|------------|---------|
| **Type** | Conformed |
| **SCD** | Type 0 (Fixed) |
| **Key** | date_id (Natural Key) |

**Attributes:** day, month, quarter, year, week_of_year, day_of_week, weekday_name, is_weekend, year_month, quarter_name

**Roles:** date_id, posted_date_id, due_date_id

---

### DimJob

| Thuộc tính | Giá trị |
|------------|---------|
| **Type** | Conformed (Domain) |
| **SCD** | Type 2 |
| **Business Key** | job_id (từ TopCV) |
| **Surrogate Key** | job_sk |

**Attributes:**

| Attribute | Mô tả |
|-----------|-------|
| job_id | ID từ TopCV (VD: 2008076) |
| title | Tiêu đề công việc |
| job_url | URL tin tuyển dụng |
| skills | JSON array kỹ năng |

---

### DimCompany

| Thuộc tính | Giá trị |
|------------|---------|
| **Type** | Conformed (Domain) |
| **SCD** | Type 2 |
| **Business Key** | company_bk_hash = MD5(LOWER(company_name)) |
| **Surrogate Key** | company_sk |

**Lý do dùng Hash:** TopCV tạo nhiều URL khác nhau cho cùng công ty, tên có thể viết hoa/thường khác nhau.

**Attributes:**

| Attribute | Mô tả |
|-----------|-------|
| company_name | Tên công ty (đã chuẩn hóa) |
| company_url | URL trang công ty |
| logo_url | URL logo |
| verified_employer | Nhà tuyển dụng xác thực |

---

### DimLocation

| Thuộc tính | Giá trị |
|------------|---------|
| **Type** | Conformed (Domain) |
| **SCD** | Type 1 |
| **Key** | location_sk |
| **Default** | -1 = Unknown |

**Attributes:** city, country (default: Vietnam)

**Xử lý đặc biệt:**
- "Nước Ngoài" → country="Nước Ngoài", city="Unknown"
- Multi-location → Tách thành nhiều records qua Bridge

---

## Bridge Table

### FactJobLocationBridge

Xử lý quan hệ many-to-many: 1 job có thể tuyển ở nhiều địa điểm.

| Column | FK to |
|--------|-------|
| fact_id | FactJobPostingDaily |
| location_sk | DimLocation |

---

## Business Questions Supported

**Trend Analysis:**
- Số lượng job postings theo thời gian?
- Top companies đăng nhiều jobs nhất?
- Xu hướng tuyển dụng theo tháng/quý?

**Geographic Analysis:**
- Phân bố jobs theo thành phố?
- Salary range theo location?

**Temporal Patterns:**
- Jobs posted weekends vs weekdays?
- Thời gian trung bình job còn active?

**Salary Insights:**
- Salary range theo job title?
- Companies trả lương cao nhất?

---

## Data Flow

```
TopCV Website
     ↓ (Crawl)
PostgreSQL: raw_jobs
     ↓ (Transform)
PostgreSQL: staging_jobs
     ↓ (ETL)
DuckDB/MinIO: Star Schema
```

---

## Conformed Dimensions

| Dimension | Scope | Tái sử dụng |
|-----------|-------|-------------|
| DimDate | Enterprise | Mọi time-series fact |
| DimCompany | Domain | Facts liên quan công ty |
| DimLocation | Domain | Facts có yếu tố địa lý |
| DimJob | Domain | Facts liên quan job posting |

---

## Tài liệu tham khảo

- [The Matrix - Kimball Group](https://www.kimballgroup.com/1999/12/the-matrix/) - Bài viết gốc của Ralph Kimball về Bus Matrix
- [Data Warehouse Bus Matrix Template - Dimodelo](https://www.dimodelo.com/data-warehouse-bus-matrix-template/) - Template và hướng dẫn sử dụng
- [The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/) - Sách của Ralph Kimball

---
