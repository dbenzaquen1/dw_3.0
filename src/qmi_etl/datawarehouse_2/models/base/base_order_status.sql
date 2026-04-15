with order_status_export as (
    select * from {{ source('sql_server', 'OrderStatus') }}
)
, rename_col as (
select 

    ID as order_status_id,
    WCID as WCID,
    LoadNo as load_number,
    Pieces as pieces,
    UserID as user_id,
    Weight as order_weight,
    SOLineID as order_line_id,
    StatusID as status_id,
    REGEXP_EXTRACT(Username, r'\\(.*)$') AS username,
    ProcessID as process_id,
    ReleaseNo as release_number,
    TotalFeet as total_feet,
    StatusDate as status_datetime,
    cast(StatusDate as date) as status_date,
    ReleaseLineID as release_line_id,
    TransferOrderNum as transfer_order_number,
    ProductionOrderID as production_order_id,
    SoqtProcessLineID as soqt_process_line_id,
    ProductionOrderNum as production_order_number
    from order_status_export
)
select * from rename_col