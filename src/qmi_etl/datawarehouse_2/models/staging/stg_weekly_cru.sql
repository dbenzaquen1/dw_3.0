with base_cru as (
    select * from {{ ref('base_cru_data') }} where date_frequency = 'Weekly'
)

select
    commodity_group,
    pricing_type,
    metal_sub_type,
    hot_dipped_pricing_type,
    price_type,
    market,
    unit_of_measurement,
    price_id,
    report_date,
    {{ week_end_date('report_date') }} as week_end_date,
    price_value as price_value_per_ton,
    {{ ton_to_cwt('price_value') }} as price_value_cwt,
    {{ton_to_pound('price_value')}} as price_value_per_pound,
    date_frequency
from base_cru

