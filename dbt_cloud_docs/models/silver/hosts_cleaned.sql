{{ config(
    schema="silver",
    materialized="table"
) }}

SELECT 
    "host_id" AS host_id,
    host_neighbourhood,
    "host_is_superhost",
    COUNT(DISTINCT "listing_id") AS num_listings
FROM {{ ref('silver_airbnb') }}
GROUP BY host_id, host_neighbourhood, "host_is_superhost"