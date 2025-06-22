WITH listings AS (
    SELECT
        "listing_id",
        COUNT("listing_id") AS total_active_listings,
        AVG(PRICE) AS avg_price,
        MIN(PRICE) AS min_price,
        MAX(PRICE) AS max_price,
        AVG(review_scores_rating) AS avg_review_score,
        COUNT(CASE WHEN has_availability = 't' THEN 1 END) AS active_listings
    FROM {{ ref('silver_airbnb') }}
    GROUP BY "listing_id"
)
SELECT *
FROM listings