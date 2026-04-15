with stg_vendors as (
    select * from {{ref("stg_vendor")}}
)
select * from stg_vendors