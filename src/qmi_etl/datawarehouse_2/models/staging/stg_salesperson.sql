with base_salesperson as (select * from {{ ref("base_salesperson") }})
select *
from base_salesperson
