{{ config(
    materialized="incremental",
    unique_key="flight_id"
) }}

SELECT
    HASH(
        FLIGHTNUM,
        DEPARTURE_TIMESTAMP
    ) AS flight_id,
    FLIGHTNUM AS flight_number,
    TO_TIMESTAMP(DEPARTURE_TIMESTAMP) AS departure_timestamp,
    ORIGIN AS origin_airport_name,
    ORIGINAIRPORTID AS origin_airport_id,
    ORIGINCITYNAME AS origin_city,
    ORIGINCOUNTRY AS origin_country,
    PARSE_JSON(ORIGINLOCATION):lat::FLOAT AS origin_latitude,
    PARSE_JSON(ORIGINLOCATION):lon::FLOAT AS origin_longitude,
    ORIGINWEATHER AS origin_weather,
    DEST AS destination_airport_name,
    DESTAIRPORTID AS destination_airport_id,
    DESTCITYNAME AS destination_city,
    DESTCOUNTRY AS destination_country,
    PARSE_JSON(DESTLOCATION):lat::FLOAT AS destination_latitude,
    PARSE_JSON(DESTLOCATION):lon::FLOAT AS destination_longitude,
    DESTWEATHER AS destination_weather,
    REGEXP_REPLACE(AVGTICKETPRICE, '[$,]', '')::FLOAT AS avg_ticket_price,
    NVL(REGEXP_REPLACE(AVGTICKETPRICE, '[0-9., +\-]', ''), '-') AS currency,
    CANCELLED::BOOLEAN AS is_cancelled,
    REPLACE(DISTANCEKILOMETERS, ',', '')::FLOAT AS distance_km,
    REPLACE(DISTANCEMILES, ',', '')::FLOAT AS distance_miles,
    FLIGHTDELAY::BOOLEAN AS is_flight_delayed,
    FLIGHTDELAYMIN::INTEGER AS flight_delay_min,
    FLIGHTDELAYTYPE AS flight_delay_type,
    FLIGHTTIMEHOUR::FLOAT AS flight_time_hours,
    REPLACE(FLIGHTTIMEMIN, ',', '')::FLOAT AS flight_time_minutes
FROM {{ source('bronze_layer', 'raw_flights') }}

{% if is_incremental() %}
    WHERE departure_timestamp > (
        SELECT MAX(departure_timestamp)
        FROM {{ this }}
    )
{% endif %}

