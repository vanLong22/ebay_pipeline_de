{{
    config(materialized = 'table')
}}

SELECT DISTINCT
    HASH(SELLER_USERNAME) AS SELLER_KEY,
    SELLER_USERNAME
FROM {{ ref('ebay_data_json_expanded') }}
WHERE SELLER_USERNAME IS NOT NULL