with part_numbers as (select * from {{ ref("stg_customer_part_number") }})

select part_number as itemid, last_base_cost as cost, 'pounds' as cost_units
from part_numbers
