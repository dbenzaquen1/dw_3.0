with base_addresses as (
    select * from {{ref("base_addresses")}}
)
select * from base_addresses