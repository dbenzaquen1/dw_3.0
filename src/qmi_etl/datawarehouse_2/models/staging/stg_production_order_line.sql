with base_production_orders_finished_goods as (
    select * from {{ ref('base_production_orders_finished_goods') }}
),
base_production_order_performance as (
    select * from {{ ref('base_production_order_performance') }}
),
base_production_orders_history as (
    select * from {{ ref('base_production_orders_history') }}
),
base_production_order_header as (
    select * from {{ ref('base_production_order_header') }}
),
rank_base_production_orders_history as (
    select *, row_number() over (partition by production_order_line_id order by transaction_timestamp desc) as rn
    from base_production_orders_history
),
most_recent_base_production_orders_history as (
    select * from rank_base_production_orders_history where rn = 1
),
inventory as (
    select * from {{ ref('inventory') }}
),
orders as (
    select * from {{ ref('orders') }}
),
warehouse as (
    select * from {{ ref('base_warehouses') }}
),
join_tables as(
    Select 
    --ids
    orders.order_id,
    finished_goods.production_order_finished_id,
    finished_goods.production_order_line_id,
    

    finished_goods.finished_inventory_id,
    finished_goods.order_line_id,
    header.warehouse_id,
    inventory.tag_id,
    inventory.item_id,
    inventory.parent_inventory_id,

    --dimensions
    finished_goods.finished_material_comments,
    header.created_username,
    header.modified_username,
    header.production_line_comments,
    warehouse.warehouse_name,
    history.transaction_username as line_produced_by,
    history.machine as machine,
    header.po_number as production_order_number,


    -- bools 
    finished_goods.has_sales_order,
    finished_goods.is_rewrap,
    inventory.is_parent,

    -- facts
    finished_goods.finished_length,
    finished_goods.finished_width,
    finished_goods.finished_feet,
    finished_goods.finished_weight,
    finished_goods.wip_weight,
    finished_goods.finished_pieces,
    finished_goods.wip_pieces,
    performance.total_minutes as finished_machine_minutes,

    --datetimes
    header.modified_timestamp,
    history.transaction_timestamp as most_recent_transaction_timestamp,
    case 
        when history.transaction_timestamp is null then header.modified_timestamp
        else history.transaction_timestamp
    end as most_recent_timestamp
    
     from
    base_production_orders_finished_goods as finished_goods

    left join base_production_order_header as header on finished_goods.production_order_line_id = header.production_order_line_id
    left join most_recent_base_production_orders_history as history on finished_goods.production_order_line_id = history.production_order_line_id
    left join inventory on finished_goods.finished_inventory_id = inventory.inventory_id
    left join orders on finished_goods.order_line_id = orders.order_line_id
    left join warehouse on header.warehouse_id = warehouse.warehouse_id
    left join base_production_order_performance as performance on finished_goods.production_order_line_id = performance.production_order_line_id
)
select * from join_tables