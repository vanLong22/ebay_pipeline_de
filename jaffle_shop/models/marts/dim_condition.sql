{{
    config(materialized = 'table')
}}

SELECT DISTINCT
    HASH(CONDITION_ID) AS CONDITION_KEY,
    CONDITION_ID,
    CONDITION_TEXT
FROM {{ ref('ebay_data_json_expanded') }}
WHERE CONDITION_ID IS NOT NULL