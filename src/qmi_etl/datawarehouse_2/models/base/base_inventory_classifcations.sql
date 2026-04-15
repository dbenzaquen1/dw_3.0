with base_inventory_classifcations as (
    select * from {{ source('sql_server', 'InventoryClassifications') }}
)
select ClassificationID as classification_id, Description as classification_description from base_inventory_classifcations