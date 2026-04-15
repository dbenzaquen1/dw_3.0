with seed_ingot_prices as (
    select * from {{ ref('seed_ingot_prices') }}
),

-- Get the date range we need to cover
date_range as (
    select 
        min(date) as start_date,
        max(date) as end_date
    from seed_ingot_prices
),

-- Generate complete date series from min to max date in seed data
complete_date_series as (
    select date
    from unnest(generate_date_array(
        (select start_date from date_range),
        (select end_date from date_range)
    )) as date
),

-- Historical data from seed with forward-filling
historical_data_with_fill as (
    select 
        cds.date as reporting_date,
        last_value(sp.cash_price ignore nulls) over (
            order by cds.date 
            rows between unbounded preceding and current row
        ) as cash_price,
        last_value(sp.`3_month` ignore nulls) over (
            order by cds.date 
            rows between unbounded preceding and current row
        ) as three_month_price,
        last_value(sp.midwest_price ignore nulls) over (
            order by cds.date 
            rows between unbounded preceding and current row
        ) as midwest_price
    from complete_date_series cds
    left join seed_ingot_prices sp on cds.date = sp.date
),

-- Future dates from last seed date to current date with forward-filling
future_dates_with_fill as (
    select 
        date as reporting_date,
        last_value(cash_price ignore nulls) over (
            order by date 
            rows between unbounded preceding and current row
        ) as cash_price,
        last_value(three_month_price ignore nulls) over (
            order by date 
            rows between unbounded preceding and current row
        ) as three_month_price,
        last_value(midwest_price ignore nulls) over (
            order by date 
            rows between unbounded preceding and current row
        ) as midwest_price
    from (
        select 
            date,
            null as cash_price,
            null as three_month_price,
            null as midwest_price
        from unnest(generate_date_array(
            (select max(date) + 1 from seed_ingot_prices),
            current_date()
        )) as date
    )
),

-- Combine historical and future data
all_dates_combined as (
    select 
        reporting_date,
        cash_price,
        three_month_price,
        midwest_price
    from historical_data_with_fill
    
    union all
    
    select 
        reporting_date,
        cash_price,
        three_month_price,
        midwest_price
    from future_dates_with_fill
),

-- Final forward-fill to ensure no gaps
final_prices as (
    select 
        reporting_date,
        last_value(cash_price ignore nulls) over (
            order by reporting_date 
            rows between unbounded preceding and current row
        ) as cash_price,
        last_value(three_month_price ignore nulls) over (
            order by reporting_date 
            rows between unbounded preceding and current row
        ) as three_month_price,
        last_value(midwest_price ignore nulls) over (
            order by reporting_date 
            rows between unbounded preceding and current row
        ) as midwest_price
    from all_dates_combined
),

-- Add type description and clean up
final_result as (
    select 
        reporting_date,
        cash_price,
        three_month_price,
        midwest_price,
        'ALUMINUM' as type_description
    from final_prices
    where reporting_date is not null
)

select * from final_result