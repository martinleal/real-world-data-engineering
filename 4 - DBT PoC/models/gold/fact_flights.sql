{{ config(
    materialized="incremental",
    unique_key="flight_id"
) }}

SELECT
    silver_flights.flight_id,
    silver_flights.flight_number,
    silver_flights.departure_timestamp,
    silver_flights.avg_ticket_price,
    silver_flights.distance_km,
    silver_flights.distance_miles,
    silver_flights.is_cancelled,
    silver_flights.is_flight_delayed,
    silver_flights.flight_delay_min,
    silver_flights.flight_time_hours,
    silver_flights.flight_time_minutes,
    origin_airport.airport_id                      AS origin_airport_id,
    destination_airport.airport_id                 AS destination_airport_id,
    origin_city.city_id                            AS origin_city_id,
    destination_city.city_id                       AS destination_city_id,
    origin_country.country_id                      AS origin_country_id,
    destination_country.country_id                 AS destination_country_id,
    flight_delay_type.flight_delay_type_id,
    currency.currency_id

FROM {{ ref('silver_flights') }} silver_flights

LEFT JOIN {{ ref('dim_airport') }} origin_airport
    ON silver_flights.origin_airport_id = origin_airport.airport_code
    AND silver_flights.departure_timestamp >= origin_airport.valid_from
    AND silver_flights.departure_timestamp < origin_airport.valid_to

LEFT JOIN {{ ref('dim_airport') }} destination_airport
    ON silver_flights.destination_airport_id = destination_airport.airport_code
    AND silver_flights.departure_timestamp >= destination_airport.valid_from
    AND silver_flights.departure_timestamp < destination_airport.valid_to

LEFT JOIN {{ ref('dim_city') }} origin_city
    ON silver_flights.origin_city = origin_city.city

LEFT JOIN {{ ref('dim_city') }} destination_city
    ON silver_flights.destination_city = destination_city.city

LEFT JOIN {{ ref('dim_country') }} origin_country
    ON silver_flights.origin_country = origin_country.country

LEFT JOIN {{ ref('dim_country') }} destination_country
    ON silver_flights.destination_country = destination_country.country

LEFT JOIN {{ ref('dim_flight_delay_type') }} flight_delay_type
    ON silver_flights.flight_delay_type = flight_delay_type.flight_delay_type_description

LEFT JOIN {{ ref('dim_currency') }} currency
    ON silver_flights.currency = currency.currency

{% if is_incremental() %}
    WHERE silver_flights.departure_timestamp > (
        SELECT MAX(departure_timestamp)
        FROM {{ this }}
    )
{% endif %}
