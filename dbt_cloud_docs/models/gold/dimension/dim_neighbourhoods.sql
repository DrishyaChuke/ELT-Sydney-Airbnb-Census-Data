{{ config(
    materialized='table'
) }}

SELECT 
    listing_neighbourhood
FROM {{ ref('silver_lga') }}