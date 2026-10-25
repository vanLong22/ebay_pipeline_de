{{ config(materialized = 'table') }}

WITH ranked_sellers AS (
    SELECT 
        seller_username,
        seller_feedback_percentage,
        seller_feedback_score,
        ROW_NUMBER() OVER (
            PARTITION BY seller_username 
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM {{ ref('ebay_data_json_expanded') }}
    WHERE seller_username IS NOT NULL AND TRIM(seller_username) != ''
)

SELECT
    HASH(seller_username) AS seller_key,
    seller_username,
    seller_feedback_percentage,
    seller_feedback_score
FROM ranked_sellers
WHERE rn = 1