{{ config(materialized = 'table') }}

WITH raw_conditions AS (
    SELECT DISTINCT
        COALESCE(condition_id, -1) AS condition_id,
        COALESCE(NULLIF(TRIM(condition_text), ''), 'Unknown') AS condition_name
    FROM {{ ref('ebay_data_json_expanded') }}
),

deduplicated_conditions AS (
    SELECT
        condition_id,
        condition_name,
        -- Dùng ROW_NUMBER để lọc bỏ hoàn toàn các dòng trùng lặp phát sinh nếu có
        ROW_NUMBER() OVER (PARTITION BY condition_id ORDER BY condition_name) as rn
    FROM raw_conditions
)

SELECT
    HASH(condition_id) AS condition_key,
    condition_id,
    condition_name
FROM deduplicated_conditions
WHERE rn = 1