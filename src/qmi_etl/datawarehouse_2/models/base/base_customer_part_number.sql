with part_number_export as (
select * from {{ source('sql_server', 'PartNumber') }}
),
rename_col as (
    select 
    PartNumberID as part_number_id,
    CustomerID as customer_id,
    ItemID as item_id,

    Width as width,
    Length as length,
    Price as price,
    LastBaseCost as last_base_cost,

    Gauge as gauge,
    PartNo as part_number,
    Description as part_description,
    from part_number_export
)
select * from rename_col