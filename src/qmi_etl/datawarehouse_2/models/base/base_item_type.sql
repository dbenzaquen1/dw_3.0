
with source_item_type as (
    select
        TypeID as type_id,
        Description as type_description
    from {{ source('sql_server', 'App_UIC_Type') }}
)

select * from source_item_type
