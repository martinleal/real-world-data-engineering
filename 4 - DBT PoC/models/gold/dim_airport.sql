select
    dbt_scd_id as airport_id,
    airport_code,
    airport_name,
    city_name,
    country,
    latitude,
    longitude,
    dbt_valid_from as valid_from,
    nvl(dbt_valid_to, '9999-12-31') as valid_to,
    iff(dbt_valid_to is null, true, false) as is_current
from {{ ref('airport_snapshot') }}
