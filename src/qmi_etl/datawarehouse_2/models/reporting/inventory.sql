with stg_inv as (
    select * from {{ref("stg_inventory")}}
)
select * from stg_inv