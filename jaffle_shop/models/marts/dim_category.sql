{{ config(materialized = 'table') }}

SELECT
    HASH(scraped_category_id)      AS category_key,
    scraped_category_id            AS category_l1_id,
    MAX(scraped_category_name)     AS category_l1_name
FROM {{ ref('ebay_data_json_expanded') }}
WHERE scraped_category_id IS NOT NULL
GROUP BY scraped_category_id