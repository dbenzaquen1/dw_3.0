with weekly_cru as (
    select * from {{ ref('stg_weekly_cru') }}
)

select * from weekly_cru 