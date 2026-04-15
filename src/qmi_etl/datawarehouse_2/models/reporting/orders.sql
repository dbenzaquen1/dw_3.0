{#
    Orders Model Documentation
    
    This model creates a comprehensive orders table that combines order data with item master information
    and various pricing components for accurate cost calculations and reporting.
    
    Key Features:
    - Combines order data with item master details (material type, grade, gauge, width)
    - Includes aluminum ingot pricing for aluminum orders
    - Applies Texarkana facility adders for aluminum orders (gauge and width-based)
    - Includes steel pricing components (freight, coating, gauge adders)
    - Incorporates daily CRU pricing for steel orders
    - Calculates replacement cost using all applicable pricing components
    - Calculates replacement cost using average inventory cost as an alternative basis
    
    Data Sources:
    - stg_orders: Base order data from staging
    - stg_item_master: Item master data for material specifications
    - base_alum_ingot: Aluminum ingot pricing
    - texarkana_gauge_adders: Texarkana gauge-based pricing adders
    - texarkana_width_adders: Texarkana width-based pricing adders
    - base_steel_coatings: Steel coating pricing
    - base_steel_gauge_adders: Steel gauge-based pricing
    - base_steel_freight_adders: Steel freight pricing
    - daily_cru: Daily CRU pricing for steel
    
    Business Logic:
    - Uses most recent pricing data for all pricing components
    - Applies cheapest available pricing when multiple options exist
    - Calculates replacement_cost as: (total_cost_per_pound * order_weight) + 45, where 45 is a fixed skid cost per line item
    - Calculates replacement_inv_avg_cost as: (avg_inventory_cost_per_pound * order_weight) + 45, using historical average inventory costs
    - Handles both aluminum and steel pricing methodologies
#}

with stg_orders as (
    select * from {{ref("stg_orders")}}
)
, stg_master_item as (
    select * from {{ref("stg_item_master")}}
),
item_cost as (
    select * from {{ref("item_cost")}}
),


 final_join as (
    select 
        stg_orders.*,

        -- Calculate replacement cost including all applicable pricing component the 45 is a fixed cost for skids each line item gets its own skid
        round(
            coalesce(item_cost.total_cost_per_pound, 0) * order_weight ,2
        ) +45 as replacement_cost,
        case 
            when stg_master_item.avg_inventory_cost_per_pound is null 
                 or stg_master_item.avg_inventory_cost_per_pound = 0
            then null
            else round(stg_master_item.avg_inventory_cost_per_pound * order_weight ,2) + 45
        end as replacement_inv_avg_cost,
        item_cost.total_cost_per_pound

    from stg_orders
    left join stg_master_item on stg_orders.item_id_width = stg_master_item.item_id_width
    left join item_cost on stg_master_item.item_id_width = item_cost.item_id_width


 )

select * from final_join 

