with base_category as (
    select * from {{ source('sql_server', 'App_UIC_Category') }}
)

select CategoryID as category_id,  Category as item_category from base_category