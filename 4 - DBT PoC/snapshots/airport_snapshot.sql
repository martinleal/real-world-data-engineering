{% snapshot airport_snapshot %}

{{
  config(
    target_schema='snapshots',       
    unique_key='airport_code',         
    strategy='check',                
    check_cols=['airport_name','city_name','country','latitude','longitude']
  )
}}

select distinct
    origin_airport_id      as airport_code,
    origin_airport_name    as airport_name,
    origin_city            as city_name,
    origin_country         as country,
    origin_latitude        as latitude,
    origin_longitude       as longitude
from {{ ref('silver_flights') }}

union

select distinct
    destination_airport_id    as airport_code,
    destination_airport_name  as airport_name,
    destination_city          as city_name,
    destination_country       as country,
    destination_latitude      as latitude,
    destination_longitude     as longitude
from {{ ref('silver_flights') }}

{% endsnapshot %}
