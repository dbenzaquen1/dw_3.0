with stg_item_master as (
    select * from {{ref("stg_item_master")}}
)
select * from stg_item_master