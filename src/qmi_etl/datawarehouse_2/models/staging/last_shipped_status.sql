with stg_order_status as (
    select * from {{ref("stg_order_status")}}
)


select * from (
    select *, row_number() over (partition by order_line_id order by status_datetime desc) as rn 
    from stg_order_status
    where status_code = 'Ship'
) ranking 
where rn = 1  