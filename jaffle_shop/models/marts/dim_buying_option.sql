{{
    config(materialized = 'table')
}}

WITH options AS (
    SELECT BUYING_OPTION_1 AS BUYING_OPTION_CODE
    FROM {{ ref('ebay_data_json_expanded') }}

    UNION

    SELECT BUYING_OPTION_2
    FROM {{ ref('ebay_data_json_expanded') }}

    UNION

    SELECT BUYING_OPTION_3
    FROM {{ ref('ebay_data_json_expanded') }}
)

SELECT DISTINCT
    HASH(BUYING_OPTION_CODE) AS BUYING_OPTION_KEY,
    BUYING_OPTION_CODE,
    BUYING_OPTION_CODE AS BUYING_OPTION_NAME
FROM options
WHERE BUYING_OPTION_CODE IS NOT NULL