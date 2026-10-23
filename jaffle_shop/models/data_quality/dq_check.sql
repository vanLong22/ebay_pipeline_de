
{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_ebay_raw') }}
),

row_level_flags AS (
    SELECT
        item_id,
        legacy_item_id,
        seller_username,
        price_value,
        price_currency,
        seller_feedback_percentage,
        seller_feedback_score,
        condition_id,
        buying_options,
        bid_count,
        current_bid_price_value,
        marketing_price_original_price_value,
        marketing_price_discount_percentage,
        item_creation_date,
        item_end_date,
        shipping_options_raw,
        categories_raw,
        scraped_category_id,
        scraped_category_name,
        item_location_country,
        item_location_postal_code,
        listing_marketplace_id,
        load_timestamp,

        -- Existing checks

        CASE
            WHEN item_id IS NULL OR TRIM(item_id) = '' THEN 1
            ELSE 0
        END AS flag_missing_item_id,

        CASE
            WHEN legacy_item_id ILIKE '%E+%'
              OR legacy_item_id ILIKE '%E-%'
            THEN 1 ELSE 0
        END AS flag_scientific_notation_error,

        CASE
            WHEN item_id IS NOT NULL
             AND COUNT(*) OVER (PARTITION BY item_id) > 1
            THEN 1 ELSE 0
        END AS flag_duplicate_item_id,

        CASE
            WHEN item_id IS NOT NULL
             AND COUNT(*) OVER (
                 PARTITION BY
                     item_id,
                     title,
                     seller_username,
                     price_value,
                     price_currency,
                     condition_id,
                     scraped_category_id
             ) > 1
            THEN 1 ELSE 0
        END AS flag_duplicate_item_details,

        CASE
            WHEN price_value <= 0 OR price_value IS NULL THEN 1
            ELSE 0
        END AS flag_invalid_price,

        CASE
            WHEN seller_feedback_percentage < 0
              OR seller_feedback_percentage > 100
            THEN 1 ELSE 0
        END AS flag_invalid_feedback_rate,

        CASE
            WHEN buying_options NOT LIKE '%AUCTION%'
             AND (
                 bid_count > 0
                 OR current_bid_price_value IS NOT NULL
             )
            THEN 1 ELSE 0
        END AS flag_buying_option_conflict,

        CASE
            WHEN marketing_price_original_price_value IS NOT NULL
             AND marketing_price_discount_percentage > 0
             AND price_value IS NOT NULL
             AND ABS(
                 price_value -
                 (
                     marketing_price_original_price_value
                     * (1 - marketing_price_discount_percentage / 100)
                 )
             ) > 0.05
            THEN 1 ELSE 0
        END AS flag_discount_math_anomaly,

        CASE
            WHEN item_creation_date > item_end_date THEN 1
            ELSE 0
        END AS flag_timeline_anomaly,

        -- DQ-30: Missing important fields

        CASE
            WHEN price_currency IS NULL OR TRIM(price_currency) = ''
            THEN 1 ELSE 0
        END AS flag_missing_price_currency,

        CASE
            WHEN seller_username IS NULL OR TRIM(seller_username) = ''
            THEN 1 ELSE 0
        END AS flag_missing_seller,

        CASE
            WHEN seller_feedback_percentage IS NULL
            THEN 1 ELSE 0
        END AS flag_missing_feedback_percentage,

        CASE
            WHEN condition_id IS NULL THEN 1
            ELSE 0
        END AS flag_missing_condition,

        CASE
            WHEN item_creation_date IS NULL THEN 1
            ELSE 0
        END AS flag_missing_creation_date,

        CASE
            WHEN item_location_country IS NULL
              OR TRIM(item_location_country) = ''
            THEN 1 ELSE 0
        END AS flag_missing_location_country,

        CASE
            WHEN scraped_category_id IS NULL
              OR TRIM(scraped_category_id) = ''
            THEN 1 ELSE 0
        END AS flag_missing_category,

        CASE
            WHEN load_timestamp IS NULL THEN 1
            ELSE 0
        END AS flag_missing_load_timestamp,

        -- DQ-31: Empty strings and missing JSON-like fields

        CASE
            WHEN shipping_options_raw IS NULL
              OR TRIM(shipping_options_raw) IN ('', '[]', 'null', 'None')
            THEN 1 ELSE 0
        END AS flag_missing_shipping_options,

        CASE
            WHEN categories_raw IS NULL
              OR TRIM(categories_raw) IN ('', '[]', 'null', 'None')
            THEN 1 ELSE 0
        END AS flag_missing_categories,

        CASE
            WHEN buying_options IS NULL
              OR TRIM(buying_options) IN ('', '[]', 'null', 'None')
            THEN 1 ELSE 0
        END AS flag_missing_buying_options,

        -- DQ-32: Additional numeric validation

        CASE
            WHEN marketing_price_discount_percentage < 0
              OR marketing_price_discount_percentage > 100
            THEN 1 ELSE 0
        END AS flag_invalid_discount_percentage,

        CASE
            WHEN marketing_price_original_price_value IS NOT NULL
             AND price_value IS NOT NULL
             AND marketing_price_original_price_value < price_value
            THEN 1 ELSE 0
        END AS flag_original_price_below_current_price,

        CASE
            WHEN seller_feedback_score < 0 THEN 1
            ELSE 0
        END AS flag_negative_feedback_score,

        -- DQ-33: Date validation

        CASE
            WHEN item_creation_date > CURRENT_TIMESTAMP()
            THEN 1 ELSE 0
        END AS flag_creation_date_in_future,

        CASE
            WHEN item_end_date IS NOT NULL
             AND item_creation_date IS NOT NULL
             AND item_end_date < item_creation_date
            THEN 1 ELSE 0
        END AS flag_end_date_before_creation,

        -- DQ-34: Unrecognized condition IDs
        -- Review these IDs against actual values in your dataset.

        CASE
            WHEN condition_id IS NOT NULL
             AND condition_id NOT IN (
                 1000, 1500, 1750, 2000, 2010, 2020,
                 2500, 2750, 3000, 3010, 4000, 5000,
                 6000, 7000
             )
            THEN 1 ELSE 0
        END AS flag_unrecognized_condition_id

    FROM source_data
),

total_metrics AS (
    SELECT
        COUNT(*) AS total_rows,
        GREATEST(COUNT(*), 1) AS denominator
    FROM row_level_flags
),

check_metrics AS (

    SELECT
        'DQ-01' AS check_id,
        'Total Rows' AS check_name,
        total_rows::FLOAT AS metric_value,
        'PASS' AS status,
        'Total records in the source model.' AS message
    FROM total_metrics

    UNION ALL

    SELECT
        'DQ-06',
        'Scientific Notation ID Rate',
        100.0 * COALESCE(SUM(flag_scientific_notation_error), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_scientific_notation_error), 0) > 0,
            'FAIL', 'PASS'),
        'IDs containing scientific notation patterns.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-07',
        'Missing Item ID Rate',
        100.0 * COALESCE(SUM(flag_missing_item_id), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_item_id), 0) > 0,
            'FAIL', 'PASS'),
        'Rows with missing or empty item IDs.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-10',
        'Duplicate Item ID Row Rate',
        100.0 * COALESCE(SUM(flag_duplicate_item_id), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_duplicate_item_id), 0) > 0,
            'WARN', 'PASS'),
        'Rows belonging to a repeated item ID group; investigate snapshot grain.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-18',
        'Invalid Price Rate',
        100.0 * COALESCE(SUM(flag_invalid_price), 0)
            / MAX(denominator),
        CASE
            WHEN 100.0 * COALESCE(SUM(flag_invalid_price), 0)
                 / MAX(denominator) > 2 THEN 'WARN'
            WHEN COALESCE(SUM(flag_invalid_price), 0) > 0 THEN 'FAIL'
            ELSE 'PASS'
        END,
        'Rows with missing, zero or negative prices; verify business rules.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-19',
        'Invalid Feedback Percentage Rate',
        100.0 * COALESCE(SUM(flag_invalid_feedback_rate), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_invalid_feedback_rate), 0) > 0,
            'FAIL', 'PASS'),
        'Seller feedback percentages outside 0-100.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-23',
        'Discount Calculation Anomaly Rate',
        100.0 * COALESCE(SUM(flag_discount_math_anomaly), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_discount_math_anomaly), 0) > 0,
            'WARN', 'PASS'),
        'Potential mismatch between current price, original price and discount percentage.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-24',
        'Buying Option Conflict Rate',
        100.0 * COALESCE(SUM(flag_buying_option_conflict), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_buying_option_conflict), 0) > 0,
            'WARN', 'PASS'),
        'Potential conflict between buying options and auction fields.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-29',
        'Timeline Anomaly Rate',
        100.0 * COALESCE(SUM(flag_timeline_anomaly), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_timeline_anomaly), 0) > 0,
            'FAIL', 'PASS'),
        'Creation date occurs after end date.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-30',
        'Missing Price Currency Rate',
        100.0 * COALESCE(SUM(flag_missing_price_currency), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_price_currency), 0) > 0,
            'WARN', 'PASS'),
        'Rows with missing price currency.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-31',
        'Missing Shipping Options Rate',
        100.0 * COALESCE(SUM(flag_missing_shipping_options), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_shipping_options), 0) > 0,
            'WARN', 'PASS'),
        'Rows without usable shipping options; some listings may legitimately omit them.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-32',
        'Invalid Discount Percentage Rate',
        100.0 * COALESCE(SUM(flag_invalid_discount_percentage), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_invalid_discount_percentage), 0) > 0,
            'FAIL', 'PASS'),
        'Discount percentages outside 0-100.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-33',
        'Missing Load Timestamp Rate',
        100.0 * COALESCE(SUM(flag_missing_load_timestamp), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_load_timestamp), 0) > 0,
            'FAIL', 'PASS'),
        'Rows without load timestamp.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-34',
        'Unrecognized Condition ID Rate',
        100.0 * COALESCE(SUM(flag_unrecognized_condition_id), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_unrecognized_condition_id), 0) > 0,
            'WARN', 'PASS'),
        'Condition IDs outside the configured allowlist; validate against source data.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-35',
        'Missing Seller Rate',
        100.0 * COALESCE(SUM(flag_missing_seller), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_seller), 0) > 0,
            'WARN', 'PASS'),
        'Rows with missing seller username.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-36',
        'Missing Feedback Score Rate',
        100.0 * COALESCE(SUM(IFF(seller_feedback_score IS NULL, 1, 0)), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(IFF(seller_feedback_score IS NULL, 1, 0)), 0) > 0,
            'WARN', 'PASS'),
        'Rows without seller feedback score.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-37',
        'Missing Location Country Rate',
        100.0 * COALESCE(SUM(flag_missing_location_country), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_location_country), 0) > 0,
            'WARN', 'PASS'),
        'Rows without item location country.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-38',
        'Missing Category Rate',
        100.0 * COALESCE(SUM(flag_missing_category), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_category), 0) > 0,
            'WARN', 'PASS'),
        'Rows without scraped category ID.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-39',
        'Original Price Below Current Price Rate',
        100.0 * COALESCE(SUM(flag_original_price_below_current_price), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_original_price_below_current_price), 0) > 0,
            'WARN', 'PASS'),
        'Potential inconsistency between original and current price.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-40',
        'Negative Seller Feedback Score Rate',
        100.0 * COALESCE(SUM(flag_negative_feedback_score), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_negative_feedback_score), 0) > 0,
            'FAIL', 'PASS'),
        'Rows with negative seller feedback score.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-41',
        'Creation Date In Future Rate',
        100.0 * COALESCE(SUM(flag_creation_date_in_future), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_creation_date_in_future), 0) > 0,
            'WARN', 'PASS'),
        'Rows with creation dates later than the current timestamp.'
    FROM row_level_flags
    CROSS JOIN total_metrics

    UNION ALL

    SELECT
        'DQ-42',
        'Missing Buying Options Rate',
        100.0 * COALESCE(SUM(flag_missing_buying_options), 0)
            / MAX(denominator),
        IFF(COALESCE(SUM(flag_missing_buying_options), 0) > 0,
            'WARN', 'PASS'),
        'Rows without buying option information.'
    FROM row_level_flags
    CROSS JOIN total_metrics
)

SELECT
    check_id,
    check_name,
    metric_value,
    status,
    message,
    CURRENT_TIMESTAMP() AS checked_at
FROM check_metrics
