with stg_ap_journal as (
    select * from {{ref("stg_ap_journal")}}
)
select * from stg_ap_journal