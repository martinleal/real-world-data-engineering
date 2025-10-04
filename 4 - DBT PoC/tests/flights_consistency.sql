-- Custom test to check data consistency between source and fact_flights
-- Ensures all flight_number and departure_timestamp from source are in fact_flights, and row counts match

WITH source_data AS (
    SELECT
        FLIGHTNUM AS flight_number,
        TO_TIMESTAMP(DEPARTURE_TIMESTAMP) AS departure_timestamp
    FROM {{ source('bronze_layer', 'raw_flights') }}
),
fact_data AS (
    SELECT
        flight_number,
        departure_timestamp
    FROM {{ ref('fact_flights') }}
),
consistency_check AS (
    SELECT
        CASE
            WHEN f.flight_number IS NULL THEN 'missing_in_fact'
            WHEN s.flight_number IS NULL THEN 'extra_in_fact'
        END AS issue,
        COALESCE(s.flight_number, f.flight_number) AS flight_number,
        COALESCE(s.departure_timestamp, f.departure_timestamp) AS departure_timestamp
    FROM source_data s
    FULL OUTER JOIN fact_data f
        ON s.flight_number = f.flight_number
        AND s.departure_timestamp = f.departure_timestamp
    WHERE s.flight_number IS NULL OR f.flight_number IS NULL
)
SELECT issue, flight_number, departure_timestamp
FROM consistency_check