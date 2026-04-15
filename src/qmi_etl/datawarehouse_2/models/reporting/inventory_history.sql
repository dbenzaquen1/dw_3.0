
with inventory_history as (
    select * from {{ref("base_inventory_history")}}
)
select * from inventory_history 

