with weekly_cru as (
    select * from {{ ref('stg_weekly_cru') }}
),
date_dim as (
    select * from {{ ref('date_dim') }}
)

select
    d.full_date as reporting_date,
    w.commodity_group,
    w.pricing_type,
    w.metal_sub_type,
    w.hot_dipped_pricing_type,
    w.price_type,
    w.market,
    w.unit_of_measurement,
    w.price_id,
    w.price_value_per_ton,
    w.price_value_cwt,
    w.price_value_per_pound,
    w.date_frequency,
    w.week_end_date
from weekly_cru w
join date_dim d
  on d.full_date between date_sub(w.week_end_date, interval 6 day) and w.week_end_date 