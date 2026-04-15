with base_item_master as (
    select * from {{ source('sql_server', 'ItemMaster') }}
)
, format_item_master as (
    select 
            -- ids
            ItemID as item_id,
            SizeID as size_id,
            TypeID as type_id,
            GradeID as grade_id,
            CategoryID as category_id,
            itemNo as product_code,
            
            -- fact
            StandardCost as standard_cost,
            -- dim
            Density as item_density,
            ItemNo as item_name,
            Description as item_description,


            -- dates
            cast(CreateTime as date) as item_create_date 
            -- flags 

    from base_item_master

)
select * from format_item_master