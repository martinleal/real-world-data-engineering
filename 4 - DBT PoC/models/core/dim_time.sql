with time_dim as (
    select distinct
        day_of_week,
        hour_of_day,
        case when day_of_week in (6,7) then true else false end as is_weekend
    from {{ ref('stg_flights') }}
)

select * from time_dim
