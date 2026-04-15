with orders as (select * from {{ref("orders")}}),
date_dim as (
    select * from {{ref("date_dim")}}-- Filter for current month only
),

invoice_date_metrics as (
    select 
    date_dim.full_date,
    round(sum(orders.invoice_amount), 0) as invoice_amount,
    round(sum(orders.cost_amount), 0) as actual_cost,
    round(sum(orders.order_weight), 0) as invoiced_weight,
    round(sum(orders.invoice_weight), 0) as invoice_weight
from date_dim
    left join orders on orders.invoice_date = date_dim.full_date
    group by date_dim.full_date

),
shipment_date_metrics as (
    select 
    date_dim.full_date,
    round(sum(orders.sale_amount), 0) as shipment_amount,
    round(sum(orders.replacement_cost), 0) as estimated_cost,
    round(sum(orders.order_weight), 0) as shipped_weight,
    round(sum(orders.invoice_weight), 0) as invoice_weight
from date_dim
    left join orders on orders.order_due_date = date_dim.full_date
    group by date_dim.full_date
),join_metrics as (
    select 
    date_dim.full_date,
    case when invoice_date_metrics.full_date < current_date() then invoice_date_metrics.invoice_amount else shipment_date_metrics.shipment_amount end as amount_sold,
    case when invoice_date_metrics.full_date < current_date() then invoice_date_metrics.actual_cost else shipment_date_metrics.estimated_cost end as Cost,
    case when invoice_date_metrics.full_date < current_date() then invoice_date_metrics.invoiced_weight else shipment_date_metrics.shipped_weight end as weight_sold,
    case when invoice_date_metrics.full_date < current_date() then invoice_date_metrics.invoice_weight else shipment_date_metrics.invoice_weight end as invoice_weight_sold
    from date_dim
        left join invoice_date_metrics on invoice_date_metrics.full_date = date_dim.full_date
        left join shipment_date_metrics on shipment_date_metrics.full_date = date_dim.full_date
),
final_dashboard as (
    select 
        full_date,
        amount_sold,
        Cost,
        weight_sold,
        invoice_weight_sold,
        -- Calculate gross margin (amount_sold - cost)
        round(amount_sold - Cost, 0) as gross_margin,
        -- Calculate gross margin percentage
        case 
            when amount_sold > 0 then round(((amount_sold - Cost) / amount_sold), 0)
            else 0 
        end as gross_margin_percent
    from join_metrics
)

select * from final_dashboard






