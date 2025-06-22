-- PART 4

-- Question No:1

SELECT
    sc.lga_code,
    sc.total_population,
    sc.median_age_persons,
    sc.average_household_size,
    sc.age_0_4_total,
    sc.age_5_14_total,
    sc.age_15_19_total,
    sc.age_20_24_total,
    sc.age_25_34_total,
    sc.age_35_44_total,
    sc.age_45_54_total,
    sc.age_55_64_total,
    sc.age_65_74_total,
    sc.age_75_84_total,
    sc.age_85_plus_total
FROM silver.silver_census sc
JOIN (
    SELECT 
    	lga_code
    FROM (
        (SELECT lga_code
         FROM (
             SELECT lga_code,
                    SUM(estimated_revenue) / NULLIF(SUM(active_listings), 0) AS revenue_per_active_listing
             FROM gold.facts_list_rev
             GROUP BY lga_code
         ) AS revenue_per_listing
         ORDER BY revenue_per_active_listing DESC
         LIMIT 3)
        UNION ALL
        (SELECT 
        	lga_code
         FROM (
             SELECT lga_code,
                    SUM(estimated_revenue) / NULLIF(SUM(active_listings), 0) AS revenue_per_active_listing
             FROM gold.facts_list_rev
             GROUP BY lga_code
         ) AS revenue_per_listing
         ORDER BY revenue_per_active_listing ASC
         LIMIT 3)
    ) AS top_bottom_lgas
) tbl ON sc.lga_code::TEXT = tbl.lga_code::TEXT
ORDER BY sc.lga_code;

-- Question No:2

SELECT 
    AVG(c.median_age_persons) AS avg_median_age,
    AVG(r.avg_revenue_per_listing) AS avg_revenue_per_listing
FROM 
    silver.silver_census c
JOIN 
    (SELECT 
         lga_code::text AS lga_code,
         AVG(estimated_revenue) AS avg_revenue_per_listing
     FROM 
         gold.facts_list_rev
     GROUP BY 
         lga_code::text) r 
ON 
    c.lga_code = r.lga_code
GROUP BY 
    c.lga_code
    
-- Question No:3

WITH top_neighbourhoods AS (
    SELECT 
        listing_neighbourhood,
        AVG(estimated_revenue) AS avg_revenue_per_listing
    FROM 
        gold.facts_list_rev
    GROUP BY 
        listing_neighbourhood
    ORDER BY 
        avg_revenue_per_listing DESC
    LIMIT 5
),
neighbourhood_listing_revenue AS (
    SELECT 
        l.listing_neighbourhood,
        l.property_type,
        l.room_type,
        l."accommodates",
        SUM(fr.estimated_revenue) AS total_revenue  
    FROM 
        silver.silver_airbnb l
    JOIN 
        gold.facts_list_rev fr ON l."listing_id" = fr."listing_id"
    JOIN 
        top_neighbourhoods t ON l.listing_neighbourhood = t.listing_neighbourhood
    GROUP BY 
        l.listing_neighbourhood, l.property_type, l.room_type, l."accommodates"
),
best_listing_per_neighbourhood AS (
    SELECT 
        listing_neighbourhood,
        property_type,
        room_type,
        "accommodates",
        total_revenue,
        ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY total_revenue DESC) AS revenue_rank
    FROM 
        neighbourhood_listing_revenue
)
SELECT 
    listing_neighbourhood,
    property_type,
    room_type,
    "accommodates",
    total_revenue
FROM 
    best_listing_per_neighbourhood
WHERE 
    revenue_rank = 1
ORDER BY 
    total_revenue desc
    
-- Question No:4

SELECT
    CASE
        WHEN unique_lgas = 1 THEN 'Concentrated in Single LGA'
        ELSE 'Distributed across Multiple LGAs'
    END AS distribution,
    COUNT(*) AS num_hosts 
FROM (
    SELECT
        dim_host.host_id,
        COUNT(DISTINCT fact_listing_revenue.lga_code) AS unique_lgas,
        COUNT(fact_listing_revenue."listing_id") AS total_listings
    FROM
        gold.dim_host AS dim_host
    JOIN
        gold.facts_list_rev AS fact_listing_revenue
        ON dim_host.host_id = fact_listing_revenue."host_id"
    GROUP BY
        dim_host.host_id
) AS host_lga_distribution
WHERE
    total_listings > 1
GROUP BY
    distribution
ORDER BY
    distribution; 
   
-- Question No:5

WITH single_listing_hosts AS (
    SELECT 
        "host_id",
        lga_code::text,
        AVG(estimated_revenue) AS avg_revenue
    FROM 
        gold.facts_list_rev
    WHERE 
        "host_id" IN (
            SELECT "host_id" 
            FROM silver.silver_airbnb
            GROUP BY "host_id" 
            HAVING COUNT("listing_id") = 1
        )
    GROUP BY 
        "host_id", lga_code
),
lga_mortgage AS (
    SELECT 
        lga_code,
        median_mortgage_repay_monthly
    FROM 
        silver.silver_census
)
SELECT 
    s.lga_code,
    COUNT(s."host_id") AS single_listing_hosts,
    SUM(CASE WHEN avg_revenue >= m.median_mortgage_repay_monthly THEN 1 ELSE 0 END) AS can_cover_mortgage,
    (SUM(CASE WHEN avg_revenue >= m.median_mortgage_repay_monthly THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(s."host_id"), 0)) AS percentage_cover
FROM 
    single_listing_hosts s
JOIN 
    lga_mortgage m ON s.lga_code = m.lga_code
GROUP BY 
    s.lga_code
ORDER BY 
    percentage_cover desc;
   
 
