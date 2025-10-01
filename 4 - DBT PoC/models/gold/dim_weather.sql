{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(
        origin_weather
    ) AS weather_id,
    origin_weather AS weather_description

FROM {{ ref('silver_flights') }}

UNION

SELECT DISTINCT
    HASH(
        destination_weather
    ) AS weather_id,
    destination_weather AS weather_description

FROM {{ ref('silver_flights') }}
