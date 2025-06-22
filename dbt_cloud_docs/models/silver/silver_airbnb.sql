{{ config(
    schema="silver",
    materialized="table"
) }}

WITH cleaned_airbnb_listings AS (
    SELECT
        "listing_id",
        "scrape_id",
        CASE
            WHEN CAST("scraped_date" AS TEXT) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
                TO_DATE(CAST("scraped_date" AS TEXT), 'DD/MM/YYYY')
            WHEN CAST("scraped_date" AS TEXT) ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' THEN
                CAST("scraped_date" AS DATE)
            ELSE
                NULL 
        END AS "scraped_date",
        
        EXTRACT(YEAR FROM TO_DATE(CAST("scraped_date" AS TEXT), 'YYYY-MM-DD'))::INTEGER AS "scraped_year",
        EXTRACT(MONTH FROM TO_DATE(CAST("scraped_date" AS TEXT), 'YYYY-MM-DD'))::INTEGER AS "scraped_month",
        EXTRACT(DAY FROM TO_DATE(CAST("scraped_date" AS TEXT), 'YYYY-MM-DD'))::INTEGER AS "scraped_day",

        "host_id",
        INITCAP(TRIM("host_name")) AS "host_name",
        CASE
            WHEN CAST("host_since" AS TEXT) ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
                TO_DATE(CAST("host_since" AS TEXT), 'DD/MM/YYYY')
            WHEN CAST("host_since" AS TEXT) ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' THEN
                CAST("host_since" AS DATE)
            ELSE
                NULL 
        END AS "host_since",
        CASE
            WHEN TRIM("host_is_superhost") = 't' THEN TRUE
            ELSE FALSE
        END AS "host_is_superhost",
        
        COALESCE(INITCAP(TRIM("host_neighbourhood")), INITCAP(TRIM("listing_neighbourhood"))) AS "host_neighbourhood",
        
        INITCAP(TRIM("listing_neighbourhood")) AS "listing_neighbourhood",
        INITCAP(TRIM("property_type")) AS "property_type",
        INITCAP(TRIM("room_type")) AS "room_type",
        "accommodates",
        
        COALESCE("price", 0) AS "price",
        
        CASE
            WHEN TRIM("has_availability") = 't' THEN TRUE
            ELSE FALSE
        END AS "has_availability",
        
        COALESCE("availability_30", 0) AS "availability_30",
        COALESCE("number_of_reviews", 0) AS "number_of_reviews",
        COALESCE("review_scores_rating", 0.0) AS "review_scores_rating",
        COALESCE("review_scores_accuracy", 0.0) AS "review_scores_accuracy",
        COALESCE("review_scores_cleanliness", 0.0) AS "review_scores_cleanliness",
        COALESCE("review_scores_checkin", 0.0) AS "review_scores_checkin",
        COALESCE("review_scores_communication", 0.0) AS "review_scores_communication",
        COALESCE("review_scores_value", 0.0) AS "review_scores_value",
        
        CURRENT_TIMESTAMP AS "snapshot_timestamp"
    FROM {{ ref('airbnb_bronze') }}
    
    WHERE 
        "listing_id" IS NOT NULL
        AND "host_id" IS NOT NULL 
        AND "scraped_date" IS NOT NULL
)

SELECT DISTINCT * 
FROM cleaned_airbnb_listings


