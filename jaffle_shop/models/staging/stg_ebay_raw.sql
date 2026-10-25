{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('raw_ebay_bronze', 'raw_ebay_listings') }}
)

select 
    raw_data:itemId::string as item_id,
    raw_data:title::string as title,
    raw_data:price.value::float as price_value,
    raw_data:price.currency::string as price_currency,
    raw_data:seller.username::string as seller_username,
    raw_data:seller.feedbackPercentage::float as seller_feedback_percentage,
    raw_data:condition::string as condition_text,
    raw_data:conditionId::int as condition_id,
    raw_data:scraped_category_id::string as scraped_category_id,
    raw_data:scraped_category_name::string as scraped_category_name,
    raw_data:buyingOptions::string as buying_options,
    raw_data:bidCount::int as bid_count,
    raw_data:currentBidPrice.value::float as current_bid_price_value,
    raw_data:marketingPrice.originalPrice.value::float as marketing_price_original_price_value,
    raw_data:marketingPrice.discountPercentage::float as marketing_price_discount_percentage,
    raw_data:itemCreationDate::timestamp as item_creation_date,
    raw_data:itemEndDate::timestamp as item_end_date,
    raw_data:shippingOptions::string as shipping_options_raw,
    raw_data:categories::string as categories_raw,
    raw_data:legacyItemId::string as legacy_item_id,
    raw_data:adultOnly::string as adult_only,

    -- Additional discount fields
    raw_data:marketingPrice.discountAmount.value::float as marketing_price_discount_amount_value,
    raw_data:marketingPrice.priceTreatment::string as marketing_price_treatment,

    -- Additional seller fields
    raw_data:seller.feedbackScore::int as seller_feedback_score,
    raw_data:topRatedBuyingExperience::boolean as top_rated_buying_experience,

    -- Additional shipping fields
    raw_data:itemLocation.country::string as item_location_country,
    raw_data:itemLocation.postalCode::string as item_location_postal_code,

    -- Marketplace
    raw_data:listingMarketplaceId::string as listing_marketplace_id,

    -- Additional price fields
    raw_data:price.convertedFromValue::float as price_converted_from_value,
    raw_data:price.convertedFromCurrency::string as price_converted_from_currency,

    raw_data:currentBidPrice.currency::string as current_bid_price_currency,
    raw_data:currentBidPrice.convertedFromValue::float
        as current_bid_price_converted_from_value,
    raw_data:currentBidPrice.convertedFromCurrency::string
        as current_bid_price_converted_from_currency,

    -- Product URL and identification
    raw_data:itemHref::string as item_href,
    raw_data:itemWebUrl::string as item_web_url,
    raw_data:epid::string as epid,

    -- Product images
    raw_data:image.imageUrl::string as image_url,
    raw_data:thumbnailImages::string as thumbnail_images_raw,
    raw_data:additionalImages::string as additional_images_raw,

    -- Category details
    raw_data:leafCategoryIds::string as leaf_category_ids_raw,

    -- Product variations
    raw_data:itemGroupHref::string as item_group_href,
    raw_data:itemGroupType::string as item_group_type,

    -- Additional listing attributes
    raw_data:itemOriginDate::timestamp as item_origin_date,
    raw_data:availableCoupons::boolean as available_coupons,
    raw_data:priorityListing::boolean as priority_listing,
    raw_data:priceDisplayCondition::string as price_display_condition,
    raw_data:pickupOptions::string as pickup_options_raw,

    load_timestamp

from raw_data
-- where cast(load_timestamp as date) = current_date()
qualify row_number() over (
    partition by raw_data:itemId::string 
    order by load_timestamp desc
) = 1