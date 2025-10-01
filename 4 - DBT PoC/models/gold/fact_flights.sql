{{ config(
    materialized="table"
) }}

SELECT
    sf.flight_id,
    sf.flight_number,
    sf.departure_timestamp,
    sf.avg_ticket_price,
    sf.distance_km,
    sf.distance_miles,
    sf.is_cancelled,
    sf.is_flight_delayed,
    sf.flight_delay_min,
    sf.flight_time_hours,
    sf.flight_time_minutes,

    -- Dimension keys
    sf.origin_airport_id,
    sf.destination_airport_id,
    dr.region_id AS origin_region_id,
    dr2.region_id AS destination_region_id,
    dc.country AS origin_country,
    dc2.country AS destination_country,
    dfd.flight_delay_type_id

FROM {{ ref('silver_flights') }} sf

LEFT JOIN {{ ref('dim_region') }} dr
    ON sf.origin_region = dr.region

LEFT JOIN {{ ref('dim_region') }} dr2
    ON sf.destination_region = dr2.region

LEFT JOIN {{ ref('dim_country') }} dc
    ON sf.origin_country = dc.country

LEFT JOIN {{ ref('dim_country') }} dc2
    ON sf.destination_country = dc2.country

LEFT JOIN {{ ref('dim_flight_delay_type') }} dfd
    ON sf.flight_delay_type = dfd.flight_delay_type_description
