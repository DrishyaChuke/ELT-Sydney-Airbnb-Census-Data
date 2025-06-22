{% snapshot silver_airbnb_snapshot %}

{{ config(
    target_schema='dbt_dchuke_silver',
    unique_key="price",
    strategy='timestamp',
    updated_at='price'
) }}

SELECT 
    "listing_id",
    "scraped_date",
    "host_id",
    "host_is_superhost",
    host_neighbourhood,
    property_type,
    room_type,
    "accommodates",
    price,
    has_availability,
    availability_30,
    review_scores_value
FROM {{ ref('silver_airbnb') }}

{% endsnapshot %}