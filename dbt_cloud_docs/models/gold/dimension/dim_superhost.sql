{{ config(
    materialized='table'
) }}

SELECT 
    host_id,
    host_neighbourhood,
    "host_is_superhost",
    num_listings
FROM {{ ref('hosts_cleaned') }}
WHERE "host_is_superhost" = 'true'