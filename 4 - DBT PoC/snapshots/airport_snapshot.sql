{% snapshot airport_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='airport_code',
    strategy='timestamp',
    updated_at='first_seen'
  )
}}

with all_airports as (

  -- combine origin + destination rows, keep the flight timestamp
  select
    hash(origin_airport_id) as airport_id,
    origin_airport_id      as airport_code,
    origin_airport_name    as airport_name,
    origin_city            as city_name,
    origin_country         as country,
    origin_latitude        as latitude,
    origin_longitude       as longitude,
    departure_timestamp    as seen_ts
  from {{ ref('silver_flights') }}

  union all

  select
    hash(destination_airport_id) as airport_id,
    destination_airport_id    as airport_code,
    destination_airport_name  as airport_name,
    destination_city          as city_name,
    destination_country       as country,
    destination_latitude      as latitude,
    destination_longitude     as longitude,
    departure_timestamp       as seen_ts
  from {{ ref('silver_flights') }}

),

agg as (

  -- compute first_seen per airport-version (and keep last_seen for diagnostics)
  select
    airport_id,
    airport_code,
    airport_name,
    city_name,
    country,
    latitude,
    longitude,
    min(seen_ts) as first_seen,
    max(seen_ts) as last_seen
  from all_airports
  group by
    airport_id,
    airport_code,
    airport_name,
    city_name,
    country,
    latitude,
    longitude

)

select
  airport_id,
  airport_code,
  airport_name,
  city_name,
  country,
  latitude,
  longitude,
  first_seen,
  last_seen
from agg

{% endsnapshot %}
