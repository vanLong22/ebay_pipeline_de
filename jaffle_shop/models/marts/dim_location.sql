{{
    config(materialized = 'table')
}}

SELECT DISTINCT
    HASH(
        ITEM_LOCATION_COUNTRY,
        ITEM_LOCATION_POSTAL_CODE
    ) AS LOCATION_KEY,
    ITEM_LOCATION_COUNTRY,
    ITEM_LOCATION_POSTAL_CODE
FROM {{ ref('ebay_data_json_expanded') }}