with base_inventory_locations as (
    select * from {{ref('base_inventory_locations')}}
)
select * from base_inventory_locations