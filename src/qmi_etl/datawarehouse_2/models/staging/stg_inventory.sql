with
    base_inv as (select * from {{ ref("base_inventory") }}),
    dim_status as (select * from {{ ref("inventory_status_seed") }}),
    stg_inventory_locations as (
        select * from {{ref("stg_inventory_locations")}}
    ),
    item_classification as (select * from {{ ref("base_inventory_classifcations") }}),

    join_seed_table_to_inv as (
        select
            case when width is null then cast(item_id as string)
             else 
            CONCAT(CAST(item_id AS STRING), '-', CAST(width AS STRING)) end as item_id_width,
            base_inv.*,
            dim_status.description as status_description,
            case
                when base_inv.status_code = 'I' or base_inv.status_code = 'W' and weight > 0
                then true
                else false
            end as is_in_inventory,
            item_classification.classification_description,
            stg_inventory_locations.location_name,
            stg_inventory_locations.Warehouse,
            stg_inventory_locations.is_excess,
            stg_inventory_locations.is_inactive,
            case when parent_inventory_id is not null and parent_inventory_id !=inventory_id then true
                 else false end as is_child,
            case when inventory_id in (select parent_inventory_id from base_inv) then true 
                 else false end as is_parent ,
            weight_reserved_hard as weight_reserved,    
            case when location_name <> 'IN TRANSIT' then weight - weight_reserved_hard else null end as weight_available,
        from base_inv
        left join dim_status on base_inv.status_code = dim_status.status_code
        left join stg_inventory_locations on stg_inventory_locations.location_id = base_inv.location_id
        left join item_classification on item_classification.classification_id = base_inv.classification_id
    ),

    add_metrics as (
        select
            *,
            case
                when is_in_inventory = true
                then date_diff(current_date(), receiving_date, day)
                else 0
            end as days_in_inventory
        from join_seed_table_to_inv
    ),
    add_date_bucket as (
        select
            *,
            case
                when days_in_inventory >= 0 and days_in_inventory < 90
                then '0 - 89'
                when days_in_inventory >= 90 and days_in_inventory < 120
                then '90 - 119'
                when days_in_inventory >= 120 and days_in_inventory < 180
                then '120 - 179'
                when days_in_inventory >= 180 and days_in_inventory < 270
                then '180 - 269'
                when days_in_inventory >= 270 and days_in_inventory < 360
                then '270 - 360'
                when days_in_inventory >= 360
                then 'Over 1 Year'
                else null
            end as inventory_aging_bucket,
            case 
                when length = 0 then true
                else false end as is_coil

        from add_metrics
    )
select *
from add_date_bucket
