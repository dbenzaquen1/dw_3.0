with customer as (select * from {{ ref("customer") }}),
stg_addresses as (select * from {{ ref("stg_addresses") }})

select
    customer.customer_id as customerCode,
    stg_addresses.address_line_1 as address1,
    stg_addresses.address_line_2 as address2,
    stg_addresses.city as city,
    stg_addresses.state as state,
    stg_addresses.zip_code as postalcode,
    'US' as country

from customer
left join stg_addresses on stg_addresses.address_id = customer.address_id
