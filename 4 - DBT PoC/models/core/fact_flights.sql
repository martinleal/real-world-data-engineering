select
    flight_num,
    origin_airport_id,
    dest_airport_id,
    avg_ticket_price,
    delay_minutes,
    distance_km,
    day_of_week,
    hour_of_day
from {{ ref('stg_flights') }}
