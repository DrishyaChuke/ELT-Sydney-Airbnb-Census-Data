{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('raw', 'census_g02') }}