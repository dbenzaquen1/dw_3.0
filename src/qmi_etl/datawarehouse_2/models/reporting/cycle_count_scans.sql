with stg_cycle_count_scans as (
    select * from {{ref("stg_cycle_count_scans")}}
)
select * from stg_cycle_count_scans