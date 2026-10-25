{{ config(materialized = 'table') }}

WITH date_spine AS (
    SELECT 
        DATEADD(day, SEQ4(), '2000-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 13000))   -- đến khoảng năm 2035
)

SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    EXTRACT(day FROM full_date) AS day_of_month,
    EXTRACT(dayofweek FROM full_date) AS day_of_week_number,
    DAYNAME(full_date) AS day_of_week_name,
    CASE WHEN EXTRACT(dayofweek FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    EXTRACT(week FROM full_date) AS week_of_year,
    EXTRACT(month FROM full_date) AS month_number,
    MONTHNAME(full_date) AS month_name,
    EXTRACT(quarter FROM full_date) AS quarter,
    EXTRACT(year FROM full_date) AS year
FROM date_spine
WHERE full_date <= '2032-12-31'::DATE