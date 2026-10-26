{{
    config(
        materialized = 'incremental',
        unique_key = 'item_id',
        incremental_strategy = 'merge'
    )
}}

WITH source_staging AS (
    SELECT * FROM {{ ref('stg_ebay_raw') }}
    
    {% if is_incremental() %}
    -- Chỉ lấy dữ liệu mới nạp vào kể từ lần chạy cuối cùng để tiết kiệm chi phí tính toán (Incremental Load)
    WHERE load_timestamp >= (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
),

cleaned_base AS (
    SELECT 
        item_id, 
        title, 
        legacy_item_id,
        
        price_value,
        price_currency,
        price_converted_from_value,
        price_converted_from_currency,

        seller_username,
        seller_feedback_percentage,
        
        -- 1. Sửa lỗi feedback âm hoặc NULL thành 0 an toàn
        GREATEST(COALESCE(seller_feedback_score, 0), 0) AS seller_feedback_score,
        top_rated_buying_experience,

        condition_text,
        condition_id,
        scraped_category_id,
        scraped_category_name,
        leaf_category_ids_raw, 
        categories_raw,

        buying_options,
        bid_count,
        current_bid_price_value,
        current_bid_price_currency,
        current_bid_price_converted_from_value,
        current_bid_price_converted_from_currency,

        marketing_price_original_price_value,
        marketing_price_discount_amount_value,
        marketing_price_treatment,

        -- 2. Xử lý giảm giá vượt ngưỡng nghiêm ngặt (0% đến 100%)
        CASE 
            WHEN marketing_price_discount_percentage < 0 THEN 0.0
            WHEN marketing_price_discount_percentage > 100 THEN 100.0
            ELSE COALESCE(marketing_price_discount_percentage, 0.0)
        END AS marketing_price_discount_percentage,

        -- 3. Chuẩn hóa giá bán chuẩn (Giải quyết triệt để lệch pha DQ-23 bằng cách đối soát biên độ an toàn)
        CASE 
            WHEN marketing_price_original_price_value IS NOT NULL 
                 AND marketing_price_original_price_value > price_value
                 AND marketing_price_discount_percentage > 0
            THEN ROUND(marketing_price_original_price_value * (1 - marketing_price_discount_percentage / 100), 2)
            ELSE price_value
        END AS expected_clean_price,

        -- 4. Xử lý chống thiếu hụt thông tin vận chuyển (Khắc phục DQ-31 bằng cách gán mảng JSON rỗng dự phòng)
        COALESCE(NULLIF(TRIM(shipping_options_raw), ''), '[]') AS shipping_options_raw,

        item_location_country,
        item_location_postal_code,
        pickup_options_raw,

        -- 5. Chuẩn hóa cột boolean adult_only triệt để (Chống lệch giá trị TRUE/1/YES)
        CASE 
            WHEN UPPER(TRIM(adult_only)) IN ('TRUE', '1', 'YES', 'T') THEN TRUE
            ELSE FALSE 
        END AS is_adult_only,

        -- Chuẩn hóa các cờ boolean khác
        COALESCE(available_coupons, FALSE) AS available_coupons,
        COALESCE(priority_listing, FALSE) AS priority_listing,
        price_display_condition,

        item_origin_date,
        item_creation_date,
        item_end_date,
        load_timestamp,
        CURRENT_TIMESTAMP() AS silver_cleansed_at

    FROM source_staging
    WHERE item_id IS NOT NULL 
      AND price_value > 0 
      AND (item_end_date IS NULL OR item_creation_date <= item_end_date)
)

SELECT * FROM cleaned_base