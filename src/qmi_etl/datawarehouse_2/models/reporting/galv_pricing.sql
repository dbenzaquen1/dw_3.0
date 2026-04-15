with seed_galvanized_pricing as (select * from {{ ref('seed_galvanized_pricing') }})

select 
from_in as min_thickness,
to_in as max_thickness,
grade,
price as price_per_100_weight,
{{cwt_to_pound('price')}} as price_per_pound,
effective_start_timestamp,
effective_end_timestamp,
case when effective_end_timestamp is null  then true else false end as isactive
from seed_galvanized_pricing