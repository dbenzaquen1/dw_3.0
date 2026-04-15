with daily_cru as (
    select * from {{ ref('stg_daily_cru') }}
)

select * from daily_cru 