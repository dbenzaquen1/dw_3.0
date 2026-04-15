with stg_orders as (
    select * from {{ ref('orders') }}
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
        i.avg_inventory_cost_per_pound,
        o.thickness,
        o.length,
        o.order_weight,
        o.order_qty,
        o.status_code,
        o.sale_amount,
        o.replacement_cost,
        o.replacement_inv_avg_cost,
        o.primary_sales_person_id,
        sp1.full_name as primary_sales_person_name,
        o.secondary_sales_person_id,
        sp2.full_name as secondary_sales_person_name,
        o.order_date,
        o.order_due_date,
        o.order_number
    from stg_orders o
    join customer c on o.customer_id = c.customer_id
    left join item_master i on o.item_id_width = i.item_id_width
    left join salesperson sp1 on o.primary_sales_person_id = sp1.salesperson_id
    left join salesperson sp2 on o.secondary_sales_person_id = sp2.salesperson_id
    where  i.product_code is not null
      or i.product_code != ''
 
)

-- Detailed line items
select
    customer_name,
    product_code,
    item_category,
    item_name,
    type_description,
    material_type,
    item_gauge,
    width,
    avg_inventory_cost_per_pound,
    thickness,
    length,
    order_number,
    status_code,
    order_weight,
    order_qty,
    sale_amount,
    replacement_cost as replacement_cost_from_cru,
    replacement_inv_avg_cost as replacement_cost_from_avg_inventory,
    primary_sales_person_id,
    primary_sales_person_name,
    secondary_sales_person_id,
    secondary_sales_person_name,
    order_due_date,
    order_date,
    'Detail' as row_type
from orders o

union all

-- Totals per customer
select
    customer_name || ' Total' as customer_name,
    null as product_code,
    null as item_category,
    null as item_name,
    null as type_description,
    null as material_type,
    null as item_gauge,
    null as width,
    null as avg_inventory_cost_per_pound,
    null as thickness,
    null as length,
    null as order_number,
    null as status_code,
    sum(order_weight) as order_weight,
    sum(order_qty) as order_qty,
    sum(sale_amount) as sale_amount,
    sum(replacement_cost) as replacement_cost_from_cru,
    sum(replacement_inv_avg_cost) as replacement_cost_from_avg_inventory,
    max(primary_sales_person_id) as primary_sales_person_id,
    max(primary_sales_person_name) as primary_sales_person_name,
    max(secondary_sales_person_id) as secondary_sales_person_id,
    max(secondary_sales_person_name) as secondary_sales_person_name,
    null as order_due_date,
    order_date,
    'Total' as row_type
from orders
group by
    customer_name,
    order_date

order by
    customer_name, product_code, thickness, width, length, row_type