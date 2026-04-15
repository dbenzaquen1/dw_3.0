with stg_inv_loc as (
    select * from {{ref('stg_inventory_locations')}}
)
select * from stg_inv_loc