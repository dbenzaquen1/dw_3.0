with base_customer_parts as (select * from {{ ref("base_customer_part_number") }})
select *
from base_customer_parts
