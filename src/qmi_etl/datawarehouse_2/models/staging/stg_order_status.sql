with base_order_status as (
    select * from {{ref("base_order_status")}}
),
base_order_status_codes as (
    select * from {{ref("base_order_status_codes")}}
)

select 
order_status_id,
user_id,
order_line_id,
base_order_status.status_id,
process_id,
release_line_id,
pieces,
order_weight,
base_order_status_codes.status_description,
base_order_status_codes.status_code,



status_date,
status_datetime
 from base_order_status 
 join base_order_status_codes on base_order_status.status_id = base_order_status_codes.status_id