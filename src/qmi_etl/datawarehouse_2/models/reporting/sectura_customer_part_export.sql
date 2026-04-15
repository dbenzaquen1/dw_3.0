with customer as (
    select * from {{ref("customer")}}
)
, stg_customer_part_number as (
    select * from {{ref("stg_customer_part_number")}}
)
, item_master as (
    select * from {{ref("item_master")}}
)
select part_number AS ItemID,
item_master.product_code as ProductName,
customer.customer_name as Description,
item_master.item_category as ProductType,
item_grade as Grade,
stg_customer_part_number.gauge as Thickness,
stg_customer_part_number.length as Length,
'inch' as Length_Unit,
stg_customer_part_number.width as Width,
'inch' as Width_Unit



from stg_customer_part_number
left join customer on customer.customer_id = stg_customer_part_number.customer_id
left join item_master on item_master.item_id = stg_customer_part_number.item_id