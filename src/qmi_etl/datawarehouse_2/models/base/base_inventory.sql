with base_inv as (
    select * from {{ source('sql_server', 'Inventory') }}
)

, flatten as (
    select 
    InventoryID as inventory_id,
    indexNumber as tag_id,
    WarehouseID as warehouse_id,
    VendorID as vendor_id,
    GaugeID as gauge_id,
    PO_NO as po_number,
    POLine as po_line,
    ItemID as item_id,
    LocationID as location_id,
    case when ParentInventoryID = 0 then null else ParentInventoryID end as  parent_inventory_id,

    PIW as piw,
    Feet as feet,
    Length as length,
    Pieces as pieces,
    Weight as weight,
    Freight as freight,
    Qty as Qty,
    QtyOnOrder as qty_on_order,
    FeetOnOrder as feet_on_order,
    WeightOnOrder as weight_on_order,
    WeightReservedHard as weight_reserved_hard,
    ActualAverageCost as Cost,
    case when HoldForInspection = false then SourceCode else 'Q' end as status_code,
    ClassificationID as classification_id,
    case when CustomerIDReserved = 0 then null else CustomerIDReserved end as customer_id_reserved,
    case when CustomerIDReserved = 0 then false else true end as is_customer_reserved,


    


    ItemNo as item_number,
    Mic1 as mic_1,
    Mic2 as mic_2,
    Width as width,
    Thickness as thickness,
    case when Mic1 = 0 then null else Thickness - Mic1 end as diffrence_nom_mic,


    cast(ReceivingDate as date ) as receiving_date
from base_inv



    
)
select * from flatten