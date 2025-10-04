-- Freshness test: Ensure fact_flights has data from the last 24 hours
-- Fails if the latest departure_timestamp is older than 1 day

SELECT 1 AS stale_data
WHERE (SELECT MAX(departure_timestamp) FROM {{ ref('fact_flights') }}) < DATEADD(HOUR,-24, CURRENT_TIMESTAMP())