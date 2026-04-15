with stg_salesperson as (
    select * from {{ref("stg_salesperson")}}
)
select * from stg_salesperson