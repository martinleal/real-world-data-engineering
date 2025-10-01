{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(
        origin_region
    ) AS region_id,
    origin_region AS region,
    origin_country AS country

FROM {{ ref('silver_flights') }}

UNION

SELECT DISTINCT
    HASH(
        destination_region
    ) AS region_id,
    destination_region AS region,
    destination_country AS country

FROM {{ ref('silver_flights') }}
