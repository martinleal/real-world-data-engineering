with raw as (
    select *
    from {{ ref('flights') }}
)

select
    cast(replace(replace(AvgTicketPrice, '$', ''), ',', '') as numeric) as avg_ticket_price,
    case when Cancelled = 'TRUE' then true else false end as cancelled,
    DestAirportID as dest_airport_id,
    DestCityName as dest_city,
    DestCountry as dest_country,
    DistanceKilometers::float as distance_km,
    DistanceMiles::float as distance_miles,
    case when FlightDelay = 'TRUE' then true else false end as flight_delayed,
    FlightDelayMin::int as delay_minutes,
    FlightDelayType as delay_type,
    FlightNum as flight_num,
    OriginAirportID as origin_airport_id,
    OriginCityName as origin_city,
    OriginCountry as origin_country,
    dayOfWeek::int as day_of_week,
    hour_of_day::int as hour_of_day
from raw
