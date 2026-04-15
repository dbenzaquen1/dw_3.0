with export_production_orders_history as (
    select * from {{ source('sql_server', 'RW_ProductionOrderHistory') }}
),

fromat_cal as (
    select 
        -- IDs
        ProductionSchedule_ProductionOrderLineID as production_order_line_id,

        -- Dimensions
        ProductionOrderNumber as production_order_number,
        Machine as machine,
        {{extract_user('UserInfo')}} as transaction_username,
        TransactionType as transaction_type,
        Stop_Pause_Reason as stop_pause_reason,
        {{combine_date_and_time('TransactionDate', 'TransactionTime')}} as transaction_timestamp,
        
        -- Facts
        ElapsedMinutes as step_elapsed_time
    from export_production_orders_history
)
select * from fromat_cal