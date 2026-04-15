with order_status_codes_export as (
    select * from {{ source('sql_server', 'OrderStatusCodes') }}
)
select
    StatusID as status_id,
    Code as status_code,
    Description as status_description 
 from order_status_codes_export