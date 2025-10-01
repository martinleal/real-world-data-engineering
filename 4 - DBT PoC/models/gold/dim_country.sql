{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(
        origin_country
    ) AS country_id,
    origin_country AS country

FROM {{ ref('silver_flights') }}

UNION

SELECT DISTINCT
    HASH(
        destination_country
    ) AS country_id,
    destination_country AS country

FROM {{ ref('silver_flights') }}
