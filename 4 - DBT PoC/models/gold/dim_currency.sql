{{ config(
    materialized="table"
) }}

SELECT DISTINCT
    HASH(currency) AS currency_id,
    currency
FROM {{ ref('silver_flights') }}