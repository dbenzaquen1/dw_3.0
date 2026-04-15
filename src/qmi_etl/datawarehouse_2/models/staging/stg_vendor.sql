with base_vendors as (
    select * from {{ref("base_vendors")}}
),
base_address as (
    select * from {{ref("stg_addresses")}}
)
select base_vendors.*,
base_address.state,
base_address.city,
base_address.zip_code,
base_address.address_line_1,
base_address.address_line_2

 from base_vendors
left join base_address on base_address.address_id = base_vendors.address_id