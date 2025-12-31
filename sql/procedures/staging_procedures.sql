-- ============================================
-- JobInsight - Staging Procedures
-- Salary lưu theo VND (không chia triệu)
-- Python xử lý title_clean và company_name_standardized
-- SQL chỉ xử lý salary và deadline
-- ============================================

-- 1. Hàm chuẩn hoá lương
DROP FUNCTION IF EXISTS jobinsight_staging.normalize_salary(text);

CREATE FUNCTION jobinsight_staging.normalize_salary(salary_text text)
RETURNS TABLE (salary_min numeric, salary_max numeric, salary_type varchar)
LANGUAGE plpgsql AS $func$
DECLARE
    matches text[];
    usd_exchange_rate numeric := 25000;
BEGIN
    -- 1. Thoả thuận / trống
    IF salary_text IS NULL OR salary_text = '' 
       OR lower(salary_text) ~ 'thoả thuận|thỏa thuận|thương lượng' THEN
        salary_min := NULL; salary_max := NULL; salary_type := 'negotiable';

    -- 2. Cạnh tranh
    ELSIF lower(salary_text) ~ 'cạnh tranh' THEN
        salary_min := NULL; salary_max := NULL; salary_type := 'competitive';

    -- 3. Dữ liệu đặc biệt "0.0 - 0.0 triệu"
    ELSIF salary_text = '0.0 - 0.0 triệu' THEN
        salary_min := NULL; salary_max := NULL; salary_type := 'negotiable';

    -- 4. Range: x - y USD
    ELSIF salary_text ~* '([0-9,.]+)\s*-\s*([0-9,.]+)\s*usd' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*-\s*([0-9,.]+)\s*usd', 'i');
        salary_min := replace(matches[1], ',', '')::numeric * usd_exchange_rate;
        salary_max := replace(matches[2], ',', '')::numeric * usd_exchange_rate;
        salary_type := 'range';

    -- 5. Range: x - y triệu
    ELSIF salary_text ~* '([0-9,.]+)\s*-\s*([0-9,.]+)\s*triệu' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*-\s*([0-9,.]+)\s*triệu', 'i');
        salary_min := replace(matches[1], ',', '.')::numeric * 1000000;
        salary_max := replace(matches[2], ',', '.')::numeric * 1000000;
        salary_type := 'range';

    -- 6. Upto: tới x USD
    ELSIF salary_text ~* 'tới\s+([0-9,.]+)\s*usd' THEN
        matches := regexp_matches(salary_text, 'tới\s+([0-9,.]+)\s*usd', 'i');
        salary_min := NULL;
        salary_max := replace(matches[1], ',', '')::numeric * usd_exchange_rate;
        salary_type := 'upto';

    -- 7. Upto: tới x triệu
    ELSIF salary_text ~* 'tới\s+([0-9,.]+)\s*triệu' THEN
        matches := regexp_matches(salary_text, 'tới\s+([0-9,.]+)\s*triệu', 'i');
        salary_min := NULL;
        salary_max := replace(matches[1], ',', '.')::numeric * 1000000;
        salary_type := 'upto';

    -- 8. From: từ x triệu
    ELSIF salary_text ~* 'từ\s+([0-9,.]+)\s*triệu' THEN
        matches := regexp_matches(salary_text, 'từ\s+([0-9,.]+)\s*triệu', 'i');
        salary_min := replace(matches[1], ',', '.')::numeric * 1000000;
        salary_max := NULL;
        salary_type := 'from';

    -- 9. Single value USD
    ELSIF salary_text ~* '([0-9,.]+)\s*usd' AND salary_text !~* '-' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*usd', 'i');
        salary_min := replace(matches[1], ',', '')::numeric * usd_exchange_rate;
        salary_max := salary_min;
        salary_type := 'range';

    -- 10. Single value triệu
    ELSIF salary_text ~* '([0-9,.]+)\s*triệu' AND salary_text !~* '-' THEN
        matches := regexp_matches(salary_text, '([0-9,.]+)\s*triệu', 'i');
        salary_min := replace(matches[1], ',', '.')::numeric * 1000000;
        salary_max := salary_min;
        salary_type := 'range';

    -- 11. Default
    ELSE
        salary_min := NULL; salary_max := NULL; salary_type := 'unknown';
    END IF;

    RETURN QUERY SELECT salary_min, salary_max, salary_type;
END;
$func$;

-- 2. Thủ tục cập nhật time_remaining
DROP PROCEDURE IF EXISTS jobinsight_staging.update_deadline();

CREATE PROCEDURE jobinsight_staging.update_deadline()
LANGUAGE plpgsql AS $proc$
BEGIN
    -- > 1 ngày
    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(day FROM (due_date - CURRENT_TIMESTAMP))::int || ' ngày để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP 
      AND due_date - CURRENT_TIMESTAMP >= INTERVAL '1 day';

    -- 1 giờ - < 1 ngày
    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(hour FROM (due_date - CURRENT_TIMESTAMP))::int || ' giờ để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP 
      AND due_date - CURRENT_TIMESTAMP >= INTERVAL '1 hour' 
      AND due_date - CURRENT_TIMESTAMP < INTERVAL '1 day';

    -- 1 phút - < 1 giờ
    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(minute FROM (due_date - CURRENT_TIMESTAMP))::int || ' phút để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP 
      AND due_date - CURRENT_TIMESTAMP >= INTERVAL '1 minute' 
      AND due_date - CURRENT_TIMESTAMP < INTERVAL '1 hour';

    -- < 1 phút
    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Còn ' || EXTRACT(second FROM (due_date - CURRENT_TIMESTAMP))::int || ' giây để ứng tuyển'
    WHERE due_date > CURRENT_TIMESTAMP 
      AND due_date - CURRENT_TIMESTAMP < INTERVAL '1 minute';

    -- Hết hạn
    UPDATE jobinsight_staging.staging_jobs
    SET time_remaining = 'Đã hết thời gian ứng tuyển'
    WHERE due_date <= CURRENT_TIMESTAMP;
END;
$proc$;

-- 3. Main transform procedure (chỉ xử lý salary và deadline)
DROP PROCEDURE IF EXISTS jobinsight_staging.transform_raw_to_staging();

CREATE PROCEDURE jobinsight_staging.transform_raw_to_staging()
LANGUAGE plpgsql AS $proc$
BEGIN
    -- Cập nhật salary cho các record đã có
    UPDATE jobinsight_staging.staging_jobs AS s
    SET salary_min = n.salary_min, salary_max = n.salary_max, salary_type = n.salary_type
    FROM (
        SELECT job_id, (jobinsight_staging.normalize_salary(salary)).*
        FROM jobinsight_staging.staging_jobs
    ) AS n
    WHERE s.job_id = n.job_id;

    -- Cập nhật due_date từ deadline (số ngày)
    UPDATE jobinsight_staging.staging_jobs
    SET due_date = crawled_at + (deadline || ' days')::interval
    WHERE due_date IS NULL 
      AND deadline IS NOT NULL 
      AND deadline ~ '^\d+$';

    -- Cập nhật time_remaining
    CALL jobinsight_staging.update_deadline();
END;
$proc$;
