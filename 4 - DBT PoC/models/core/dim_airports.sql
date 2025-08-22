with airports as (
    select distinct
        origin_airport_id as airport_id,
        origin_city as city,
        origin_country as country
    from {{ ref('stg_flights') }}

    union

    select distinct
        dest_airport_id as airport_id,
        dest_city as city,
        dest_country as country
    from {{ ref('stg_flights') }}
)

select * from airports
