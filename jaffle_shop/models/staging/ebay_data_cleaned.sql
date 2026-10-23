{{config(materialized = 'table', unique_key = ' item_id')}}
with source_staging as (
    select * from {{ref('stg_ebay_raw')}}
)

select item_id, title, legacy_item_id,
    -- 
    price_value,
    price_currency,
    price_converted_from_value,
    price_converted_from_currency,

    seller_username,
    seller_feedback_percentage,
    -- sửa lỗi feedback âm
    greatest(coalesce(seller_feedback_score, 0), 0) as seller_feedback_score,
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

    -- sửa lỗi logic giảm giá: tính toán thêm 1 cột giá bán chuẩn
    case 
        when marketing_price_original_price_value is not null and marketing_price_discount_percentage > 0
        then round(marketing_price_original_price_value * (1 - marketing_price_discount_percentage / 100), 2)
        else price_value
    end as expected_clean_price,

    -- sửa lỗi giảm giá vượt ngưỡng
    case 
        when marketing_price_discount_percentage < 0 then 0.0
        when marketing_price_discount_percentage > 100 then 100.0
        else coalesce(marketing_price_discount_percentage, 0.0)
    end as marketing_price_discount_percentage,

    shipping_options_raw,
    item_location_country,
    item_location_postal_code,
    pickup_options_raw,

    -- item_href,
    -- item_web_url,
    -- epid,
    -- image_url,
    -- thumbnail_images_raw,
    -- additional_images_raw,
    -- listing_marketplace_id,

    -- sửa lỗi ko đồng nhất giá trị: chuẩn hóa cột về dạng boolean
    case 
        when upper(trim(adult_only)) in ('TRUE', '1', 'YES') then true
        else false 
    end as is_adult_only,

    coalesce(available_coupons, FALSE) as availabel_coupons,
    coalesce(priority_listing, FALSE) AS priority_listing,
    price_display_condition,

    item_origin_date,
    item_creation_date,
    item_end_date,
    load_timestamp,
    CURRENT_TIMESTAMP() AS silver_cleansed_at

from source_staging
where item_id is not null 
and price_value > 0 
and (item_end_date is null or item_creation_date <= item_end_date)

