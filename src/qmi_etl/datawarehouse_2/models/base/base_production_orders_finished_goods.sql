with export_finished_goods as (
    select * from {{ source('sql_server', 'RW_ProductionOrderFinishedMaterial') }}
),

fromat_cal as (
    select 
        -- IDs
        ProductionSchedule_ProductionOrderBtsID as production_order_finished_id,
        ProductionOrderHeader_ProductionOrderLineID as production_order_line_id,
        Inventory_InventoryID as finished_inventory_id,
        case when OrderLines_SoqtLineID != -1 then OrderLines_SoqtLineID else null end as order_line_id,
        
        -- Dimensions
        FinishedMaterialComments as finished_material_comments,
        
        -- Bools
        case when OrderLines_SoqtLineID != -1 then true else false end as has_sales_order,
        case when OrderLines_SoqtLineID = -1 and FinishedLength = 0 then true else false end as is_rewrap,
        
        -- Facts
        FinishedLength as finished_length,
        FinishedWidth as finished_width,
        Feet as finished_feet,
        ProducedWeight as finished_weight,
        WIPWeight as wip_weight,
        MachineMinutes as finished_machine_minutes,
        ProducedPieces as finished_pieces,
        WIPPieces as wip_pieces
    from export_finished_goods
)
select * from fromat_cal