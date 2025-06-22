WITH lga_mapping AS (
    SELECT 
        TRIM(LOWER(ln.listing_neighbourhood)) AS listing_neighbourhood,  
        ln.lga_name AS host_neighbourhood_lga 
    FROM {{ ref('silver_lga') }} ln
),
host_metrics AS (
    SELECT
        lm.host_neighbourhood_lga, 
        date_trunc('month', CAST(l."scraped_date" AS DATE)) AS month_year,  
        COUNT(DISTINCT l."host_id") AS distinct_hosts, 
        SUM(l.price * (30 - l.availability_30)) AS estimated_revenue,  
        (SUM(l.price * (30 - l.availability_30)) / NULLIF(COUNT(DISTINCT l."host_id"), 0)) AS estimated_revenue_per_host  
    FROM {{ ref('silver_airbnb') }} l
    LEFT JOIN lga_mapping lm
        ON LOWER(l.listing_neighbourhood) = lm.listing_neighbourhood  
    WHERE l.has_availability = 't'  
    GROUP BY lm.host_neighbourhood_lga, month_year  
)
SELECT *
FROM host_metrics
WHERE host_neighbourhood_lga IS NOT NULL 
ORDER BY host_neighbourhood_lga, month_year 