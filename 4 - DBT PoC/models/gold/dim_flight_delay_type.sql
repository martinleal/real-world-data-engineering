{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(
        flight_delay_type
    ) AS flight_delay_type_id,
    flight_delay_type AS flight_delay_type_description

FROM {{ ref('silver_flights') }}
