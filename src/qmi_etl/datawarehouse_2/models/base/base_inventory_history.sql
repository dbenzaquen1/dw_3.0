with inventory_history_export as (
    select * from {{ source('sql_server', 'InventoryHist') }}),

reformatted_inventory_history as (
    select 
    HistoryID as history_id,
    InventoryID as inventory_id,
    InventoryID as inventory_tag,
    FirstInventoryID as first_inventory_id,
    PO_NO as po_number,
    Location as location_name,
    Notes as notes,
    Flaw as flaw,
    Weight as weight,
    Width as width,
    Length1 as length,
    {{extract_user('UserInfo') }} AS username,
    price as price,



    Pcs as pieces,
    Feet as feet,
    sourceType as source_type,

    SubmitDateTime as submit_date_time,
    cast(ReceivingDate as date) as received_date




    from inventory_history_export
)
SELECT * FROM reformatted_inventory_history
