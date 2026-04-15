

with daily_alum_ingot as (
    select * from {{ ref('base_alum_ingot') }}
),

-- Add week information to daily data
daily_with_week_info as (
    select 
        *,
        -- Extract ISO week information
        extract(isoweek from reporting_date) as week_number,
        extract(isoyear from reporting_date) as year_number,
        -- Get Monday of the week as week_start_date
        date_trunc(reporting_date, week(monday)) as week_start_date,
        -- Get Sunday of the week as week_end_date
        date_add(date_trunc(reporting_date, week(monday)), interval 6 day) as week_end_date
    from daily_alum_ingot
),

-- Calculate weekly averages
weekly_averages as (
    select 
        year_number,
        week_number,
        week_start_date,
        week_end_date,
        -- Calculate averages for each pricing field
        avg(cash_price) as avg_cash_price,
        avg(three_month_price) as avg_three_month_price,
        avg(midwest_price) as avg_midwest_price,
        -- Count of days in the week for data quality
        count(*) as days_in_week,
        -- Standard deviation for data quality monitoring
        stddev(cash_price) as cash_price_stddev,
        stddev(three_month_price) as three_month_price_stddev,
        stddev(midwest_price) as midwest_price_stddev,
        -- Min and max values for the week
        min(cash_price) as min_cash_price,
        max(cash_price) as max_cash_price,
        min(three_month_price) as min_three_month_price,
        max(three_month_price) as max_three_month_price,
        min(midwest_price) as min_midwest_price,
        max(midwest_price) as max_midwest_price
    from daily_with_week_info
    group by 
        year_number,
        week_number,
        week_start_date,
        week_end_date
    order by 
        year_number,
        week_number
)

-- Final result with clean column names and type description
select 
    year_number,
    week_number,
    week_start_date,
    week_end_date,
    round(avg_cash_price*100, 2) as avg_cash_price,
    round(avg_three_month_price*100, 2) as avg_three_month_price,
    round(avg_midwest_price*100, 2) as avg_midwest_price,
    round(min_cash_price*100, 2) as min_cash_price,
    round(max_cash_price*100, 2) as max_cash_price,
    round(min_three_month_price*100, 2) as min_three_month_price,
    round(max_three_month_price*100, 2) as max_three_month_price,
    round(min_midwest_price*100, 2) as min_midwest_price,
    round(max_midwest_price*100, 2) as max_midwest_price,
    'ALUMINUM' as type_description
from weekly_averages
order by week_start_date
