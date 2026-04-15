{#
    Monthly Customer Metrics Model
    
    This model provides comprehensive monthly customer metrics including active customers,
    new customers, churned customers, retention rates, and growth metrics.
    
    Key Features:
    - Tracks active customers per month (customers who placed orders)
    - Identifies new customers (first order in that month)
    - Identifies churned customers (last order was 12+ months ago)
    - Calculates retention and churn rates
    - Provides cumulative metrics for trend analysis
    - Tracks repeat customers and purchase rates
    - Calculates average order frequency and revenue per customer
    - Uses the orders table as the primary data source
    
    Data Sources:
    - orders: Main orders table with order details and customer information
    - stg_customer: Customer master data for customer names and details
    
    Business Logic:
    - New customers: First order date falls within the month
    - Churned customers: Last order was 12+ months ago (churn date = last_order_date + 12 months)
    - Active customers: Customers who placed orders in the month
    - Repeat customers: Customers who placed multiple orders in the month
    - Retention rate: (Active customers this month / Active customers previous month) * 100
    - Churn rate: (Churned customers this month / Active customers previous month) * 100
    - Net growth: New customers - Churned customers
    - Repeat purchase rate: (Repeat customers / Active customers) * 100
    - Average order frequency: Total orders / Active customers
    - Average revenue per customer: Total revenue / Active customers
    - Average weight per customer: Total weight / Active customers
#}
with orders as (select * from {{ref("orders")}}),
 stg_customer as (select * from {{ref("stg_customer")}}),
 customer_order_dates as (
    -- Get all customer order dates with customer info from the orders table
    select 
        orders.customer_id,
        stg_customer.customer_name,
        orders.order_date,
        orders.invoice_amount,
        orders.order_weight,
        orders.invoice_weight,
        extract(year from orders.order_date) as order_year,
        extract(month from orders.order_date) as order_month,
        -- Create year-month for grouping
        date_trunc(orders.order_date, month) as order_year_month
    from orders 
    left join stg_customer  on orders.customer_id = stg_customer.customer_id
    where orders.order_date is not null
),

customer_first_orders as (
    -- Get the first order date for each customer
    select 
        customer_id,
        min(order_date) as first_order_date,
        extract(year from min(order_date)) as first_order_year,
        extract(month from min(order_date)) as first_order_month,
        date_trunc(min(order_date), month) as first_order_year_month
    from customer_order_dates
    group by customer_id
),

customer_last_orders as (
    -- Get the last order date for each customer
    select 
        customer_id,
        max(order_date) as last_order_date,
        extract(year from max(order_date)) as last_order_year,
        extract(month from max(order_date)) as last_order_month,
        date_trunc(max(order_date), month) as last_order_year_month
    from customer_order_dates
    group by customer_id
),

monthly_customer_activity as (
    -- Get all unique customers who placed orders each month
    select 
        order_year,
        order_month,
        order_year_month,
        count(distinct customer_id) as active_customers,
        count(*) as total_orders,
        sum(coalesce(invoice_amount, 0)) as total_revenue,
        sum(coalesce(order_weight, 0)) as total_ordered_weight,
        sum(coalesce(invoice_weight, 0)) as total_invoice_weight
    from customer_order_dates
    group by order_year, order_month, order_year_month
),

monthly_new_customers as (
    -- Identify new customers (first order in that month)
    select 
        first_order_year as order_year,
        first_order_month as order_month,
        first_order_year_month as order_year_month,
        count(distinct customer_id) as new_customers
    from customer_first_orders
    group by first_order_year, first_order_month, first_order_year_month
),

monthly_churned_customers as (
    -- Identify churned customers (last order was 12+ months ago)
    select 
        extract(year from date_add(last_order_date, interval 12 month)) as churn_year,
        extract(month from date_add(last_order_date, interval 12 month)) as churn_month,
        date_trunc(date_add(last_order_date, interval 12 month), month) as churn_year_month,
        count(distinct customer_id) as churned_customers
    from customer_last_orders
    group by churn_year, churn_month, churn_year_month
),

monthly_repeat_customers as (
    -- Identify customers who placed multiple orders in the month
    select 
        order_year,
        order_month,
        order_year_month,
        count(distinct customer_id) as repeat_customers
    from (
        select 
            customer_id,
            order_year,
            order_month,
            order_year_month,
            count(*) as orders_in_month
        from customer_order_dates
        group by customer_id, order_year, order_month, order_year_month
        having count(*) > 1
    )
    group by order_year, order_month, order_year_month
),

all_months as (
    -- Generate all months from first order to current date
    select 
        extract(year from date) as year_number,
        extract(month from date) as month_number,
        date as year_month_date
    from unnest(generate_date_array(
        (select min(first_order_date) from customer_first_orders),
        current_date(),
        interval 1 month
    )) as date
),

       final_metrics as (
           select 
               am.year_number,
               am.month_number,
               am.year_month_date,
               last_day(am.year_month_date) as month_end_date,
               
               -- Active customers this month
               coalesce(mca.active_customers, 0) as active_customers,
        
        -- New customers this month
        coalesce(mnc.new_customers, 0) as new_customers,        
        
        -- Churned customers this month
        coalesce(mcc.churned_customers, 0) as churned_customers,
        
        -- Repeat customers this month
        coalesce(mrc.repeat_customers, 0) as repeat_customers,
        
        -- Total orders, revenue, and weight this month
        coalesce(mca.total_orders, 0) as total_orders,
        coalesce(mca.total_revenue, 0) as total_revenue,
        coalesce(mca.total_ordered_weight, 0) as total_ordered_weight,
        coalesce(mca.total_invoice_weight, 0) as total_invoice_weight,
        
        -- Calculate retention rate (customers who ordered this month vs previous month)
        case 
            when lag(mca.active_customers) over (order by am.year_month_date) > 0 
            then round(
                (coalesce(mca.active_customers, 0) * 100.0) / 
                lag(mca.active_customers) over (order by am.year_month_date), 
                2
            )
            else null 
        end as retention_rate_percent,
        
        -- Calculate churn rate
        case 
            when lag(mca.active_customers) over (order by am.year_month_date) > 0 
            then round(
                (coalesce(mcc.churned_customers, 0) * 100.0) / 
                lag(mca.active_customers) over (order by am.year_month_date), 
                2
            )
            else null 
        end as churn_rate_percent,
        
        -- Repeat purchase rate
        case 
            when coalesce(mca.active_customers, 0) > 0 
            then round(
                (coalesce(mrc.repeat_customers, 0) * 100.0) / coalesce(mca.active_customers, 0), 
                2
            )
            else 0 
        end as repeat_purchase_rate_percent,
        
        -- Average order frequency
        case 
            when coalesce(mca.active_customers, 0) > 0 
            then round(
                coalesce(mca.total_orders, 0) * 1.0 / coalesce(mca.active_customers, 0), 
                2
            )
            else 0 
        end as average_order_frequency,
        
        -- Average revenue per customer
        case 
            when coalesce(mca.active_customers, 0) > 0 
            then round(
                coalesce(mca.total_revenue, 0) / coalesce(mca.active_customers, 0), 
                2
            )
            else 0 
        end as average_revenue_per_customer,
        
        -- Average weight per customer
        case 
            when coalesce(mca.active_customers, 0) > 0 
            then round(
                coalesce(mca.total_ordered_weight, 0) / coalesce(mca.active_customers, 0), 
                2
            )
            else 0 
        end as average_weight_per_customer,
        -- Average invoice weight per customer
        case 
            when coalesce(mca.active_customers, 0) > 0 
            then round(
                coalesce(mca.total_invoice_weight, 0) / coalesce(mca.active_customers, 0), 
                2
            )
            else 0 
        end as average_invoice_weight_per_customer,
        
        -- Net customer growth
        coalesce(mnc.new_customers, 0) - coalesce(mcc.churned_customers, 0) as net_customer_growth,
        
        -- Customer retention rate (different from retention_rate_percent)
        case 
            when lag(mca.active_customers) over (order by am.year_month_date) > 0 
            then round(
                ((coalesce(mca.active_customers, 0) - coalesce(mnc.new_customers, 0)) * 100.0) / 
                lag(mca.active_customers) over (order by am.year_month_date), 
                2
            )
            else null 
        end as customer_retention_rate_percent,
        
        -- Running total of new customers
        sum(coalesce(mnc.new_customers, 0)) over (order by am.year_month_date) as cumulative_new_customers,
        
        -- Running total of churned customers
        sum(coalesce(mcc.churned_customers, 0)) over (order by am.year_month_date) as cumulative_churned_customers
        
    from all_months am
    left join monthly_customer_activity mca 
        on am.year_number = mca.order_year 
        and am.month_number = mca.order_month
    left join monthly_new_customers mnc 
        on am.year_number = mnc.order_year 
        and am.month_number = mnc.order_month
    left join monthly_churned_customers mcc 
        on am.year_number = mcc.churn_year 
        and am.month_number = mcc.churn_month
    left join monthly_repeat_customers mrc 
        on am.year_number = mrc.order_year 
        and am.month_number = mrc.order_month
)

select * from final_metrics
order by year_month_date
