SELECT 
    "listing_id",
    "host_id",
    listing_neighbourhood,
    COUNT(*) AS total_stays,
    SUM(price * (30 - availability_30)) AS estimated_revenue
FROM {{ ref('silver_airbnb') }}
GROUP BY "listing_id", "host_id", listing_neighbourhood