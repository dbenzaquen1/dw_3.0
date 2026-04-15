with base_size as (
    select * from {{ source('sql_server', 'App_UIC_Size') }}
),
size_reduced as (
    select 
    TypeID as type_id,
    CategoryID as category_id,
    GaugeCode as gauge_code,
    ProductCode as product_code,
    Max as max_size,
    min as min_size,
    nom,
    Thickness as thickness,
    Category as size_category,
    Description as size_description
    from base_size



    
)
select * from size_reduced