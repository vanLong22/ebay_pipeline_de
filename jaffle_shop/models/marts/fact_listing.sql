{{ config(
    materialized = 'incremental',
    unique_key = ['item_id', 'snapshot_date']
) }}

with source_data as (
    select 
        item_id,
        legacy_item_id,
        seller_username,
        condition_id,
        item_location_country,
        item_location_postal_code,
        scraped_category_id,
        price_value,
        price_currency,
        price_converted_from_value,
        price_converted_from_currency,
        bid_count,
        current_bid_price_value,
        current_bid_price_currency,
        current_bid_price_converted_from_value,
        current_bid_price_converted_from_currency,
        marketing_price_original_price_value,
        marketing_price_discount_amount_value,
        marketing_price_discount_percentage,
        expected_clean_price,
        marketing_price_treatment,
        seller_feedback_percentage,
        seller_feedback_score,
        top_rated_buying_experience,
        shipping_cost_1,
        shipping_currency_1,
        shipping_cost_type_1,
        min_estimated_delivery_1,
        max_estimated_delivery_1,
        buying_option_1,
        buying_option_2,
        buying_option_3,
        item_origin_date,
        item_creation_date,
        item_end_date,
        is_adult_only,
        priority_listing,
        price_display_condition,
        load_timestamp,
        silver_cleansed_at,
        -- Khai báo cột snapshot_date tại đây để dùng cho partition và các bước sau
        to_date(load_timestamp) as snapshot_date
    from {{ ref('ebay_data_json_expanded') }}
    qualify row_number() over(
        partition by item_id, to_date(load_timestamp)
        order by load_timestamp desc,
                 silver_cleansed_at desc
    ) = 1
)

select
    -- Grain: one ITEM_ID per snapshot date
    HASH(item_id) AS listing_key,
    TO_NUMBER(TO_CHAR(snapshot_date, 'YYYYMMDD')) AS date_key,

    HASH(seller_username) AS seller_key,
    HASH(condition_id) AS condition_key,
    HASH(item_location_country, item_location_postal_code) AS location_key,
    HASH(scraped_category_id) AS category_key,
    HASH(buying_option_1) AS buying_option_key,

    -- Business identifiers
    item_id,
    legacy_item_id,
    snapshot_date,

    -- Listing price
    price_value,
    price_currency,
    price_converted_from_value,
    price_converted_from_currency,

    -- Bidding
    bid_count,
    current_bid_price_value,
    current_bid_price_currency,
    current_bid_price_converted_from_value,
    current_bid_price_converted_from_currency,

    -- Discount
    marketing_price_original_price_value,
    marketing_price_discount_amount_value,
    marketing_price_discount_percentage,
    expected_clean_price,
    marketing_price_treatment,

    -- Seller metrics at snapshot time
    seller_feedback_percentage,
    seller_feedback_score,
    top_rated_buying_experience,

    -- Shipping 
    shipping_cost_1,
    shipping_currency_1,
    shipping_cost_type_1,
    min_estimated_delivery_1,
    max_estimated_delivery_1,

    -- Additional buying options
    buying_option_1,
    buying_option_2,
    buying_option_3,

    -- Listing attributes and dates
    item_origin_date,
    item_creation_date,
    item_end_date,
    is_adult_only,
    priority_listing,
    price_display_condition,

    -- Data lineage
    load_timestamp,
    silver_cleansed_at

from source_data