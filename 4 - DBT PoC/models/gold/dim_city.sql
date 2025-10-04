{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(
        origin_city
    ) AS city_id,
    origin_city AS city,
    origin_country AS country

FROM {{ ref('silver_flights') }}

UNION

SELECT DISTINCT
    HASH(
        destination_city
    ) AS city_id,
    destination_city AS city,
    destination_country AS country

FROM {{ ref('silver_flights') }}
