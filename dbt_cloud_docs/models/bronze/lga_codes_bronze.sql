{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ source('raw', 'lga_codes') }}