WITH property_metrics AS (
    SELECT
        l.property_type,
        l.room_type,
        l."accommodates",
        date_trunc('month', CAST(l."scraped_date" AS DATE)) AS month_year,  
        "scraped_month" as month,
        "scraped_year" as year,
        COUNT(l."listing_id") AS active_listings,  
        MIN(l.price) AS min_price,  
        MAX(l.price) AS max_price,  
        AVG(l.price) AS avg_price, 
        COUNT(DISTINCT l."host_id") AS distinct_hosts,  
        AVG(l.review_scores_rating) AS avg_review_score,  
        SUM(30 - l.availability_30) AS total_stays,  
        SUM(l.price * (30 - l.availability_30)) / NULLIF(COUNT(l."listing_id"), 0) AS estimated_revenue_per_listing, 
        COUNT(CASE WHEN l."host_is_superhost" = 't' THEN 1 END) * 1.0 / NULLIF(COUNT(DISTINCT l."host_id"), 0) AS superhost_rate, 
        COUNT(CASE WHEN l.has_availability = 't' THEN 1 END) AS total_active_listings, 
        COUNT(CASE WHEN l.has_availability = 'f' THEN 1 END) AS total_inactive_listings  
    FROM 
        {{ ref('silver_airbnb') }} l
    WHERE 
        l.has_availability = 't'  
    GROUP BY 
        l.property_type, l.room_type, l."accommodates", month_year, month, year  
),

property_changes AS (
    SELECT 
        *,
        100.0 * (active_listings - LAG(active_listings) OVER (PARTITION BY property_type, room_type, "accommodates" ORDER BY month_year)) / NULLIF(LAG(active_listings) OVER (PARTITION BY property_type, room_type, "accommodates" ORDER BY month_year), 0) AS pct_change_active_listings,  
        100.0 * (total_inactive_listings - LAG(total_inactive_listings) OVER (PARTITION BY property_type, room_type, "accommodates" ORDER BY month_year)) / NULLIF(LAG(total_inactive_listings) OVER (PARTITION BY property_type, room_type, "accommodates" ORDER BY month_year), 0) AS pct_change_inactive_listings  
    FROM 
        property_metrics
)

SELECT 
    property_type,
    room_type,
    "accommodates",
    month_year,
    month,
    year,
    active_listings,
    min_price,
    max_price,
    avg_price,
    distinct_hosts,
    avg_review_score,
    total_stays,
    estimated_revenue_per_listing,
    superhost_rate,
    total_active_listings,
    total_inactive_listings,
    pct_change_active_listings,
    pct_change_inactive_listings
FROM 
    property_changes
ORDER BY 
    property_type, room_type, "accommodates", month_year