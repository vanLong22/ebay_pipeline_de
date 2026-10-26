{{ config(
    materialized = 'table'
) }}

WITH source_data AS (
    SELECT 
        item_id,
        scraped_category_id,
        condition_id,
        seller_username,
        item_creation_date,
        load_timestamp,
        price_value,
        marketing_price_original_price_value,
        marketing_price_discount_percentage
    FROM {{ ref('ebay_data_json_expanded') }}
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY item_id, TO_DATE(load_timestamp)
        ORDER BY load_timestamp DESC, silver_cleansed_at DESC
    ) = 1
)

SELECT
    -- Khóa định danh dòng duy nhất
    HASH(s.item_id, TO_DATE(s.load_timestamp)) AS listing_key,
    s.item_id,
    
    -- Khóa ngoại trỏ tới các bảng Dim
    HASH(s.scraped_category_id)             AS category_key,
    HASH(COALESCE(s.condition_id, -1))      AS condition_key,
    HASH(s.seller_username)                 AS seller_key,
    
    -- Khóa thời gian (Date Keys dạng YYYYMMDD)
    COALESCE(TO_NUMBER(TO_CHAR(TO_DATE(s.item_creation_date), 'YYYYMMDD')), 19700101) AS creation_date_key,
    TO_NUMBER(TO_CHAR(TO_DATE(s.load_timestamp), 'YYYYMMDD')) AS snapshot_date_key,

    -- Các chỉ số đo lường (Measures)
    s.price_value AS price,
    s.marketing_price_original_price_value AS original_price,
    s.marketing_price_discount_percentage AS discount_pct,
    
    -- Cờ tính toán giảm giá
    CASE 
        WHEN s.marketing_price_discount_percentage > 0 THEN TRUE 
        ELSE FALSE 
    END AS is_discounted

FROM source_data s
WHERE s.item_id IS NOT NULL