with stg_orders as (
    select * from {{ ref('stg_orders') }}
),
customer as (
    select * from {{ ref('customer') }}
),
item_master as (
    select * from {{ ref('item_master') }}
),
salesperson as (
    select * from {{ ref('salesperson') }}
),
orders as (
    select
        o.customer_id,
        c.customer_name,
        o.item_id,
        o.item_id_width,
        i.product_code,
        i.item_category,
        i.item_name,
        i.type_description,
        i.material_type,
        i.item_gauge,
        i.width,
        o.thickness,
        o.length,
        o.order_weight,
        o.invoice_weight,
        o.order_qty,
        o.sale_amount,
        o.cost_amount,
        o.invoice_amount,
        o.status_code,
        o.inventory_cost as inventory_cost_per_100_weight,
        case
            when o.inventory_cost is not null and o.inventory_cost != 0 then round((o.inventory_cost * o.invoice_weight)/100, 2)
            else o.cost_amount
        end as calculated_cost_amount,
        o.primary_sales_person_id,
        sp1.full_name as primary_sales_person_name,
        o.secondary_sales_person_id,
        sp2.full_name as secondary_sales_person_name,
        o.invoice_posted_date,
        o.order_number
    from stg_orders o
    join customer c on o.customer_id = c.customer_id
    left join item_master i on o.item_id_width = i.item_id_width
    left join salesperson sp1 on o.primary_sales_person_id = sp1.salesperson_id
    left join salesperson sp2 on o.secondary_sales_person_id = sp2.salesperson_id
    where  i.product_code is not null
      or i.product_code != ''
     
),

detail as (
    select
        customer_name,
        product_code,
        item_category,
        item_name,
        type_description,
        material_type,
        item_gauge,
        width,
        thickness,
        length,
        order_number,
        status_code,
        order_weight,
        invoice_weight,
        order_qty,
        sale_amount,
        calculated_cost_amount as cost_amount,
        invoice_amount,
        -- Profit logic:
        --   * Credits: profit equals invoice_amount (typically negative)
        --   * Xetex (customer_id = 5586) pre‑invoice rows with invoice_amount = 0:
        --       force profit to 0 so these do not show an artificial loss based only on cost
        --   * All other rows: profit = invoice_amount - calculated_cost_amount
        case when invoice_amount < 0  then invoice_amount
         when invoice_amount =0 and customer_id = 5586 then 0
         when item_category like '%CUST%' then 0
         else invoice_amount - calculated_cost_amount end as profit,
        primary_sales_person_id,
        primary_sales_person_name,
        secondary_sales_person_id,
        secondary_sales_person_name,
        invoice_posted_date,
        'Detail' as row_type
    from orders
)

-- Detail rows from CTE
select * from detail

union all

-- Totals per customer from CTE
select
    customer_name || ' Total' as customer_name,
    null as product_code,
    null as item_category,
    null as item_name,
    null as type_description,
    null as material_type,
    null as item_gauge,
    null as width,
    null as thickness,
    null as length,
    null as order_number,
    null as status_code,
    sum(order_weight) as order_weight,
    sum(invoice_weight) as invoice_weight,
    sum(order_qty) as order_qty,
    sum(sale_amount) as sale_amount,
    sum(cost_amount) as cost_amount,
    sum(invoice_amount) as invoice_amount,
    sum(profit) as profit,
    max(primary_sales_person_id) as primary_sales_person_id,
    max(primary_sales_person_name) as primary_sales_person_name,
    max(secondary_sales_person_id) as secondary_sales_person_id,
    max(secondary_sales_person_name) as secondary_sales_person_name,
    invoice_posted_date,
    'Total' as row_type
from detail
group by
    customer_name, invoice_posted_date

order by
    customer_name, product_code, thickness, width, length, row_type