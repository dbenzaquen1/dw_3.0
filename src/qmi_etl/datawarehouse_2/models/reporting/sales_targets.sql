with sales_target as (
    select * from {{ref("seed_sales_target")}}
)

select * from sales_target