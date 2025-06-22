{{ config(
    schema="silver",
    materialized="table"
) }}

WITH lga_neighbourhood AS (
    SELECT 
        ls."lga_code" AS "lga_code",                         
        INITCAP(ls."lga_name") AS "lga_name",                          
        INITCAP(s."suburb_name") AS "listing_neighbourhood"          
    FROM {{ ref('lga_codes_bronze') }} ls
    LEFT JOIN {{ ref('lga_suburb_bronze') }} s
        ON LOWER(ls."lga_name") = LOWER(s."lga_name")               
)
SELECT *
FROM lga_neighbourhood
