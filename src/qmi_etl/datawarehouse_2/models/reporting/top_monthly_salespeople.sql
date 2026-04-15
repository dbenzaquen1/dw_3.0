

with orders as (
    select * from {{ ref('stg_orders')}}
),

-- Calculate total sales by month and primary salesperson
primary_sales_by_month as (
    select
        date_trunc( order_date, month) as sales_month,
        primary_sales_person_id,
        sum(sale_amount) as total_sales
    from orders
    where order_date is not null
    and primary_sales_person_id is not null
    group by 1, 2
),

-- Calculate total sales by month and secondary salesperson
secondary_sales_by_month as (
    select
        date_trunc( order_date, month) as sales_month,
        secondary_sales_person_id,
        sum(sale_amount) as total_sales
    from orders
    where order_date is not null
    and secondary_sales_person_id is not null
    group by 1, 2
),

-- Rank primary salespeople by sales amount for each month
ranked_primary_salespeople as (
    select
        sales_month,
        primary_sales_person_id,
        total_sales,
        row_number() over(partition by sales_month order by total_sales desc) as sales_rank
    from primary_sales_by_month
),

-- Rank secondary salespeople by sales amount for each month
ranked_secondary_salespeople as (
    select
        sales_month,
        secondary_sales_person_id,
        total_sales,
        row_number() over(partition by sales_month order by total_sales desc) as sales_rank
    from secondary_sales_by_month
),

-- Get the top primary salesperson for each month
top_primary_salespeople as (
    select
        sales_month,
        primary_sales_person_id,
        total_sales as primary_sales_amount
    from ranked_primary_salespeople
    where sales_rank = 1
),

-- Get the top secondary salesperson for each month
top_secondary_salespeople as (
    select
        sales_month,
        secondary_sales_person_id,
        total_sales as secondary_sales_amount
    from ranked_secondary_salespeople
    where sales_rank = 1
)

-- Join the results to get the final report
select
    format_date('%Y-%m', p.sales_month) as month_year,
    p.sales_month,
    p.primary_sales_person_id as top_inside_salesperson_id,
    ps.username as top_inside_salesperson_name,
    p.primary_sales_amount as inside_sales_amount,
    s.secondary_sales_person_id as top_outside_salesperson_id,
    ss.username as top_secondary_outside_name,
    s.secondary_sales_amount as outside_sales_amount
from top_primary_salespeople p
left join top_secondary_salespeople s on p.sales_month = s.sales_month
left join {{ ref('stg_salesperson') }} ps on p.primary_sales_person_id = ps.salesperson_id
left join {{ ref('stg_salesperson') }} ss on s.secondary_sales_person_id = ss.salesperson_id
order by p.sales_month desc
