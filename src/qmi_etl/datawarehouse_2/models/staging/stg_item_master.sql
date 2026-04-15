with
    base_item_master as (select * from {{ ref("base_item_master") }}),
    base_item_type as (select * from {{ ref("base_item_type") }}),
    base_item_grade as (select * from {{ ref("base_item_grade") }}),
    base_item_category as (select * from {{ ref("base_item_category") }}),
    stg_inventory as ( select * from {{ref("stg_inventory")}}),
    stg_orders as (select * from {{ ref('stg_orders') }}),
    

    inv_widths as (
        select 
        distinct item_id , width from stg_inventory
    ),
    inv_avg_cost as (
        select 
            item_id_width,
            avg(Cost) as avg_inventory_cost
        from stg_inventory
        where is_in_inventory = true 
            and receiving_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
            and (length = 0 or (length > 0 and width in (48, 60, 78)))
        group by item_id_width
    ),
    order_widths as (
        select distinct item_id, width from stg_orders
    ),
    order_inv_combined as (
        select distinct item_id, width from inv_widths
        union distinct
        select distinct item_id, width from order_widths
    ),


    
                join_tables as (
        select
            case when width is null then cast(base_item_master.item_id as string)
             else 
            CONCAT(CAST(base_item_master.item_id AS STRING), '-', CAST(order_inv_combined.width AS STRING)) end as item_id_width,
            base_item_master.*,
            base_item_type.type_description,
            base_item_grade.grade,
            base_item_category.item_category,
            order_inv_combined.width,
            case
                when trim(type_description) = 'COPPER'
                then 'Copper'
                when trim(type_description) = 'ALUMINUM'
                then 'Aluminum'
                when
                    trim(type_description) = 'COLD ROLLED'
                    or trim(type_description) = 'ALUMINIZED'
                    or trim(type_description) = 'GALVANIZED'
                    or trim(type_description) = 'GALVANNEALED'
                    or trim(type_description) = 'HOT ROLLED'
                    or trim(type_description) = 'HOT ROLLED BLACK'
                    or trim(type_description) = 'HOT ROLLED P&O'
                    or trim(type_description) = 'STAINLESS'
                    or trim(type_description) = 'STAINLESS STEEL'
                then "Steel"
                else 'Other'
            end as material_type,
            case
             when trim(type_description) like '%STAINLESS%' then 'STAINLESS STEEL' 
             when trim(type_description) like '%HOT ROLLED%' then 'HOT ROLLED'
             else type_description end as type_description_fixed,
            case 
                when item_name like '%GN40%' then 'GN40'
                when item_name like '%GN60%' then 'GN60'
                when item_name like '%GLV90%' then 'GLV90'
                when item_name like '%GLV30%' then 'GLV30'
                when item_name like '%GLV40%' then 'GLV40'

                else null
            end as coating_grade


        from base_item_master
        join base_item_type on base_item_type.type_id = base_item_master.type_id
        join base_item_grade on base_item_grade.grade_id = base_item_master.grade_id
        join base_item_category on base_item_category.category_id = base_item_master.category_id
        left join order_inv_combined on order_inv_combined.item_id = base_item_master.item_id
        )

select
    join_tables.item_id_width,
    join_tables.item_id,
    join_tables.product_code,
    join_tables.item_category,
    trim(join_tables.type_description) as type_description,
    join_tables.type_description_fixed,
    join_tables.grade as item_grade,
    join_tables.coating_grade,
    join_tables.item_name,
    join_tables.item_description,
    join_tables.item_density,
    join_tables.standard_cost,
    round(inv_avg_cost.avg_inventory_cost, 2) as avg_inventory_cost_per_100_weight,
    round(inv_avg_cost.avg_inventory_cost / 100, 2) as avg_inventory_cost_per_pound,

    join_tables.width,
    join_tables.item_create_date,
    join_tables.material_type,
    case
      when join_tables.material_type in ('Steel') then REGEXP_EXTRACT(join_tables.product_code, r' (\d+G|\d+/\d+|\d+)')
      when join_tables.material_type in ('Copper', 'Aluminum') then REGEXP_EXTRACT(join_tables.product_code, r'(\.\d{3})')
      else 'Other'
    end as item_gauge,
    case when join_tables.material_type in ('Copper', 'Aluminum') then cast(REGEXP_EXTRACT(join_tables.product_code, r'(\.\d{3})') as decimal)
    else null end as item_gauge_numeric
    
from join_tables
left join inv_avg_cost on join_tables.item_id_width = inv_avg_cost.item_id_width
