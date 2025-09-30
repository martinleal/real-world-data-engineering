-- models/gold/airport_dim.sql
select
    dbt_scd_id as airport_id,
    airport_code,
    airport_name,
    city_name,
    country,
    latitude,
    longitude,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to
from {{ ref('airport_snapshot') }}
