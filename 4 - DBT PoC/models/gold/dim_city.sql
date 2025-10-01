{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(
        origin_city
    ) AS city_id,
    origin_city AS city,
    origin_region AS region,
    origin_country AS country

FROM {{ ref('silver_flights') }}

UNION

SELECT DISTINCT
    HASH(
        destination_city
    ) AS city_id,
    destination_city AS city,
    destination_region AS region,
    destination_country AS country

FROM {{ ref('silver_flights') }}
