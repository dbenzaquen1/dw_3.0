with daily_sales_targets as (
    select * from {{ref("seed_daily_targets")}}
)

select * from daily_sales_targets