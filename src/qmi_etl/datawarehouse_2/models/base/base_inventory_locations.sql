with base_inventory_locations as (
    select * from {{ source('sql_server', 'RW_InventoryLocation') }}
)
, formated_inv_loc as (
    select
    LocationID as location_id,
    Location as location_name,
    Warehouse as Warehouse,
    ReservePriority as reserve_priority,


    case when excess = 'No' then false else true end as is_excess,
    case when Inactive = 'No' then false else true end as is_inactive,
    

    from base_inventory_locations
)
select * from formated_inv_loc