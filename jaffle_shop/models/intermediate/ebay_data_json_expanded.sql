{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('ebay_data_cleaned') }}
),

parsed AS (
    SELECT
        *,

        TRY_PARSE_JSON(
            TO_VARCHAR(leaf_category_ids_raw)
        ) AS LEAF_CATEGORY_IDS_JSON,

        TRY_PARSE_JSON(
            TO_VARCHAR(categories_raw)
        ) AS CATEGORIES_JSON,

        TRY_PARSE_JSON(
            TO_VARCHAR(buying_options)
        ) AS BUYING_OPTIONS_JSON,

        TRY_PARSE_JSON(
            TO_VARCHAR(shipping_options_raw)
        ) AS SHIPPING_OPTIONS_JSON

    FROM source_data
)

SELECT
    -- Giữ các cột khác, loại bỏ các cột JSON gốc và cột trung gian sau khi đã bóc tách
    p.* EXCLUDE (
        leaf_category_ids_raw,
        categories_raw,
        buying_options,
        shipping_options_raw,
        LEAF_CATEGORY_IDS_JSON,
        CATEGORIES_JSON,
        BUYING_OPTIONS_JSON,
        SHIPPING_OPTIONS_JSON
    ),

    -- Leaf category IDs
    p.LEAF_CATEGORY_IDS_JSON[0]::VARCHAR AS LEAF_CATEGORY_ID_1,
    p.LEAF_CATEGORY_IDS_JSON[1]::VARCHAR AS LEAF_CATEGORY_ID_2,
    p.LEAF_CATEGORY_IDS_JSON[2]::VARCHAR AS LEAF_CATEGORY_ID_3,

    -- Categories (Bóc tách các cấp danh mục từ mảng JSON)
    p.CATEGORIES_JSON[0]:categoryId::VARCHAR
        AS CATEGORY_1_ID,
    p.CATEGORIES_JSON[0]:categoryName::VARCHAR
        AS CATEGORY_1_NAME,

    p.CATEGORIES_JSON[1]:categoryId::VARCHAR
        AS CATEGORY_2_ID,
    p.CATEGORIES_JSON[1]:categoryName::VARCHAR
        AS CATEGORY_2_NAME,

    p.CATEGORIES_JSON[2]:categoryId::VARCHAR
        AS CATEGORY_3_ID,
    p.CATEGORIES_JSON[2]:categoryName::VARCHAR
        AS CATEGORY_3_NAME,

    p.CATEGORIES_JSON[3]:categoryId::VARCHAR
        AS CATEGORY_4_ID,
    p.CATEGORIES_JSON[3]:categoryName::VARCHAR
        AS CATEGORY_4_NAME,

    -- Buying options
    p.BUYING_OPTIONS_JSON[0]::VARCHAR AS BUYING_OPTION_1,
    p.BUYING_OPTIONS_JSON[1]::VARCHAR AS BUYING_OPTION_2,
    p.BUYING_OPTIONS_JSON[2]::VARCHAR AS BUYING_OPTION_3,

    -- Shipping option đầu tiên (Đã được bảo vệ an toàn từ tầng Cleaned)
    p.SHIPPING_OPTIONS_JSON[0]:shippingCost:value::NUMBER(12,2)
        AS SHIPPING_COST_1,

    p.SHIPPING_OPTIONS_JSON[0]:shippingCost:currency::VARCHAR
        AS SHIPPING_CURRENCY_1,

    p.SHIPPING_OPTIONS_JSON[0]:shippingCostType::VARCHAR
        AS SHIPPING_COST_TYPE_1,

    TRY_TO_TIMESTAMP_TZ(
        p.SHIPPING_OPTIONS_JSON[0]:minEstimatedDeliveryDate::VARCHAR
    ) AS MIN_ESTIMATED_DELIVERY_1,

    TRY_TO_TIMESTAMP_TZ(
        p.SHIPPING_OPTIONS_JSON[0]:maxEstimatedDeliveryDate::VARCHAR
    ) AS MAX_ESTIMATED_DELIVERY_1,

    -- Cờ phát hiện JSON không parse được (Chính xác tuyệt đối, không bị lỗi vặt do chuỗi rỗng)
    (
        p.leaf_category_ids_raw IS NOT NULL
        AND p.LEAF_CATEGORY_IDS_JSON IS NULL
    ) AS LEAF_CATEGORY_IDS_JSON_PARSE_FAILED,

    (
        p.categories_raw IS NOT NULL
        AND p.CATEGORIES_JSON IS NULL
    ) AS CATEGORIES_JSON_PARSE_FAILED,

    (
        p.buying_options IS NOT NULL
        AND p.BUYING_OPTIONS_JSON IS NULL
    ) AS BUYING_OPTIONS_JSON_PARSE_FAILED,

    (
        p.shipping_options_raw IS NOT NULL
        AND p.SHIPPING_OPTIONS_JSON IS NULL
    ) AS SHIPPING_OPTIONS_JSON_PARSE_FAILED

FROM parsed p