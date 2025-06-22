{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        l."listing_id" AS listing_id,
        l.listing_neighbourhood,
        l.property_type,
        l.room_type,
        l."accommodates",
        l.price,
        l."scraped_date"
    FROM {{ ref('silver_airbnb') }} l
)

SELECT
    listing_id,
    listing_neighbourhood,
    property_type,
    room_type,
    "accommodates",
    price,
    "scraped_date",
    CURRENT_TIMESTAMP AS effective_date
FROM source_data
