-- Staging model for metal prices combining CRU and aluminum ingot data
-- This model unions daily CRU pricing data with aluminum ingot pricing
-- and includes all dates from the date dimension, populating blanks for missing data

with daily_cru as (
    select 
        reporting_date,
        price_value_per_pound as cost_amount,
        metal_sub_type,
        'CRU' as data_source
    from {{ ref('stg_daily_cru') }}
    where hot_dipped_pricing_type in ('Base', 'Standard')
        and not (
            metal_sub_type = 'Hot Dipped Galvanised' 
            and hot_dipped_pricing_type = 'Standard'
        )
),

alum_ingot as (
    select 
        reporting_date,
        midwest_price as cost_amount,
        type_description as metal_sub_type,
        'Aluminum Ingot' as data_source
    from {{ ref('base_alum_ingot') }}
    where midwest_price is not null
),

-- Union all pricing data from both sources
all_prices as (
    select 
        reporting_date,
        cost_amount,
        metal_sub_type,
        data_source
    from daily_cru
    
    union all
    
    select 
        reporting_date,
        cost_amount,
        metal_sub_type,
        data_source
    from alum_ingot
),

date_dim as (
    select full_date as reporting_date
    from {{ ref('date_dim') }}
)

-- Join with date dimension to ensure all dates are represented
select 
    date_dim.reporting_date,
    all_prices.cost_amount,
    all_prices.metal_sub_type,
    all_prices.data_source
from date_dim 
left join all_prices 
    on date_dim.reporting_date = all_prices.reporting_date