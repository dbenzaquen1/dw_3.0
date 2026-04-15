

with orders as (
    select * from {{ref("orders")}}
),
stg_customer as (
    select * from {{ref("stg_customer")}}
),

-- Base customer order data with revenue information
customer_order_data as (
    select 
        orders.customer_id,
        stg_customer.customer_name,
        orders.order_date,
        extract(year from orders.order_date) as order_year,
        orders.invoice_amount,
        orders.order_weight,
        orders.invoice_weight
    from orders 
    left join stg_customer on orders.customer_id = stg_customer.customer_id
    where orders.order_date is not null
),

-- Customer first and last order analysis
customer_order_summary as (
    select 
        customer_id,
        customer_name,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        extract(year from min(order_date)) as first_order_year,
        extract(year from max(order_date)) as last_order_year,
        count(*) as total_orders,
        sum(coalesce(invoice_amount, 0)) as total_revenue,
        sum(coalesce(order_weight, 0)) as total_ordered_weight,
        sum(coalesce(invoice_weight, 0)) as total_invoice_weight
    from customer_order_data
    group by customer_id, customer_name
),

-- Yearly active customers (customers who placed orders each year)
yearly_active_customers as (
    select 
        order_year,
        count(distinct customer_id) as active_customers,
        count(*) as total_orders,
        sum(coalesce(invoice_amount, 0)) as total_revenue,
        sum(coalesce(order_weight, 0)) as total_ordered_weight,
        sum(coalesce(invoice_weight, 0)) as total_invoice_weight
    from customer_order_data
    group by order_year
),

-- Yearly new customers (first order in that year)
yearly_new_customers as (
    select 
        first_order_year as order_year,
        count(distinct customer_id) as new_customers
    from customer_order_summary
    group by first_order_year
),

-- Yearly churned customers (last order was in previous year and no orders this year)
yearly_churned_customers as (
    select 
        extract(year from date_add(last_order_date, interval 1 year)) as churn_year,
        count(distinct customer_id) as churned_customers
    from customer_order_summary
    where last_order_date < date_trunc(current_date(), year)  -- Only count churned customers up to current year
    group by churn_year
),

-- Customers with repeat purchases per year
yearly_repeat_customers as (
    select 
        order_year,
        count(distinct customer_id) as repeat_customers
    from (
        select 
            customer_id,
            order_year,
            count(*) as orders_in_year
        from customer_order_data
        group by customer_id, order_year
        having count(*) > 1
    )
    group by order_year
),

-- Generate year dimension
year_dimension as (
    select 
        extract(year from date) as year_number
    from unnest(generate_date_array(
        (select min(first_order_date) from customer_order_summary),
        current_date(),
        interval 1 year
    )) as date
),

-- Join all metrics together
yearly_metrics_joined as (
    select 
        yd.year_number,
        date(yd.year_number, 12, 31) as year_end_date,
        
        -- Active customers this year
        coalesce(yac.active_customers, 0) as active_customers,
        
        -- New customers this year
        coalesce(ync.new_customers, 0) as new_customers,
        
        -- Churned customers this year
        coalesce(ycc.churned_customers, 0) as churned_customers,
        
        -- Repeat customers this year
        coalesce(yrc.repeat_customers, 0) as repeat_customers,
        
        -- Total orders and revenue this year
        coalesce(yac.total_orders, 0) as total_orders,
        coalesce(yac.total_revenue, 0) as total_revenue,
        coalesce(yac.total_ordered_weight, 0) as total_ordered_weight,
        coalesce(yac.total_invoice_weight, 0) as total_invoice_weight
        
    from year_dimension yd
    left join yearly_active_customers yac 
        on yd.year_number = yac.order_year
    left join yearly_new_customers ync 
        on yd.year_number = ync.order_year
    left join yearly_churned_customers ycc 
        on yd.year_number = ycc.churn_year
    left join yearly_repeat_customers yrc 
        on yd.year_number = yrc.order_year
),

-- Calculate final metrics with derived calculations
final_yearly_metrics as (
    select 
        year_number,
        year_end_date,
        active_customers,
        new_customers,
        churned_customers,
        repeat_customers,
        total_orders,
        total_revenue,
        total_ordered_weight,
        total_invoice_weight,
        
        -- Yearly churn rate
        case 
            when lag(active_customers) over (order by year_number) > 0 
            then round(
                (churned_customers * 100.0) / 
                lag(active_customers) over (order by year_number), 
                2
            )
            else null 
        end as yearly_churn_rate_percent,
        
        -- Repeat purchase rate
        case 
            when active_customers > 0 
            then round(
                (repeat_customers * 100.0) / active_customers, 
                2
            )
            else 0 
        end as repeat_purchase_rate_percent,
        
        -- Average order frequency
        case 
            when active_customers > 0 
            then round(
                total_orders * 1.0 / active_customers, 
                2
            )
            else 0 
        end as average_order_frequency,
        
        -- Average revenue per customer
        case 
            when active_customers > 0 
            then round(
                total_revenue / active_customers, 
                2
            )
            else 0 
        end as average_revenue_per_customer,
        
        -- Average weight per customer
        case 
            when active_customers > 0 
            then round(
                total_ordered_weight / active_customers, 
                2
            )
            else 0 
        end as average_weight_per_customer,
        -- Average invoice weight per customer
        case 
            when active_customers > 0 
            then round(
                total_invoice_weight / active_customers, 
                2
            )
            else 0 
        end as average_invoice_weight_per_customer,
        
        -- Net customer growth
        new_customers - churned_customers as net_customer_growth,
        
        -- Customer retention rate
        case 
            when lag(active_customers) over (order by year_number) > 0 
            then round(
                ((active_customers - new_customers) * 100.0) / 
                lag(active_customers) over (order by year_number), 
                2
            )
            else null 
        end as customer_retention_rate_percent
        
    from yearly_metrics_joined
)

select * from final_yearly_metrics
order by year_number
