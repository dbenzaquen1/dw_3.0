with export_production_order_header as (
    select * from {{ source('sql_server', 'RW_ProductionOrderHeader') }}
)

select 
    ProductionOrderFinishedMaterial_ProductionOrderLineID as production_order_line_id,
    Warehouses_WarehouseID as warehouse_id,
    Inventory_InventoryID as finished_material_inventory_id,
    {{extract_user('CreatedBy')}} as created_username,
    {{extract_user('ModifiedBy')}} as modified_username,
    FinishedMaterialComments as finished_material_comments,
    ProductionLineComments as production_line_comments,
    MachineMinutes as machine_minutes,
    SetupMinutes as setup_minutes,
    OrderNumber as po_number,

    {{combine_date_and_time('DateModified', 'TimeModified')}} as modified_timestamp
    from export_production_order_header