{{config(materialized = 'table')}}

with source_data as (
    select * 
    from {{ref('ebay_data_json_expanded')}}
    qualify row_number() over(
        partition by item_id, to_date(load_timestamp)
        order by load_timestamp desc,
        silver_cleansed_at desc
    ) = 1
),

fact_source as(
    select 
        s.*,
        to_date(s.load_timestamp) as snapshot_date
    from source_data s 
)


SELECT
    -- Grain: one ITEM_ID per snapshot date
    HASH(s.ITEM_ID) AS LISTING_KEY,
    TO_NUMBER(TO_CHAR(s.SNAPSHOT_DATE, 'YYYYMMDD')) AS DATE_KEY,

    HASH(s.SELLER_USERNAME) AS SELLER_KEY,
    HASH(s.CONDITION_ID) AS CONDITION_KEY,
    HASH(s.ITEM_LOCATION_COUNTRY, s.ITEM_LOCATION_POSTAL_CODE)
        AS LOCATION_KEY,
    HASH(s.SCRAPED_CATEGORY_ID) AS CATEGORY_KEY,
    HASH(s.BUYING_OPTION_1) AS BUYING_OPTION_KEY,

    -- Business identifiers
    s.ITEM_ID,
    s.LEGACY_ITEM_ID,
    s.SNAPSHOT_DATE,

    -- Listing price
    s.PRICE_VALUE,
    s.PRICE_CURRENCY,
    s.PRICE_CONVERTED_FROM_VALUE,
    s.PRICE_CONVERTED_FROM_CURRENCY,

    -- Bidding
    s.BID_COUNT,
    s.CURRENT_BID_PRICE_VALUE,
    s.CURRENT_BID_PRICE_CURRENCY,
    s.CURRENT_BID_PRICE_CONVERTED_FROM_VALUE,
    s.CURRENT_BID_PRICE_CONVERTED_FROM_CURRENCY,

    -- Discount
    s.MARKETING_PRICE_ORIGINAL_PRICE_VALUE,
    s.MARKETING_PRICE_DISCOUNT_AMOUNT_VALUE,
    s.MARKETING_PRICE_DISCOUNT_PERCENTAGE,
    s.EXPECTED_CLEAN_PRICE,
    s.MARKETING_PRICE_TREATMENT,

    -- Seller metrics at snapshot time
    s.SELLER_FEEDBACK_PERCENTAGE,
    s.SELLER_FEEDBACK_SCORE,
    s.TOP_RATED_BUYING_EXPERIENCE,

    -- Shipping stays in the main fact
    s.SHIPPING_COST_1,
    s.SHIPPING_CURRENCY_1,
    s.SHIPPING_COST_TYPE_1,
    s.MIN_ESTIMATED_DELIVERY_1,
    s.MAX_ESTIMATED_DELIVERY_1,

    -- Additional buying options
    s.BUYING_OPTION_1,
    s.BUYING_OPTION_2,
    s.BUYING_OPTION_3,

    -- Listing attributes and dates
    s.ITEM_ORIGIN_DATE,
    s.ITEM_CREATION_DATE,
    s.ITEM_END_DATE,
    s.IS_ADULT_ONLY,
    s.PRIORITY_LISTING,
    s.PRICE_DISPLAY_CONDITION,

    -- Data lineage
    s.LOAD_TIMESTAMP,
    s.SILVER_CLEANSED_AT

FROM fact_source s