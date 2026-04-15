with monthly_cru as (
    select * from {{ ref('stg_monthly_cru') }}
)

select * from monthly_cru 