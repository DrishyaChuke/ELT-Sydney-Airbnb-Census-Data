WITH revenue_data AS (
    SELECT
        a."listing_id",
        a."host_id",
        a.listing_neighbourhood,
        a.price,
        a."scraped_date",
        a."scraped_month",
        l.lga_name,
        l.lga_code,
        SUM(a.price * (30 - a.availability_30)) AS estimated_revenue,
        COUNT(DISTINCT a."listing_id") AS active_listings         
    FROM
        {{ ref('silver_airbnb') }} AS a
    JOIN
        {{ ref('silver_lga') }} AS l
    ON
        LOWER(a.listing_neighbourhood) = LOWER(l.listing_neighbourhood)
    WHERE
        a.has_availability = 'true'
    GROUP BY
        a."listing_id",
        a."host_id",
        a.listing_neighbourhood,
        l.lga_name,
        a.price,
        l.lga_code,
        a."scraped_month",
        a."scraped_date"
)

SELECT
    "listing_id",
    "host_id",
    listing_neighbourhood,
    lga_code,
    estimated_revenue,
    active_listings,
    "scraped_month",
    "scraped_date"
FROM
    revenue_data
