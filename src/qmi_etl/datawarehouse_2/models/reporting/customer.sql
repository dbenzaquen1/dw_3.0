with stg_customer as (
    select * from {{ref("stg_customer")}}
)
select * from stg_customer