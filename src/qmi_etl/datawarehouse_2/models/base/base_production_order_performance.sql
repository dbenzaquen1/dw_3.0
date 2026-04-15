with export_production_order_performance as (
    select * from {{ source('sql_server', 'RW_ProductionOrderPerformance') }}
),
format_production_order_performance as (
    select 
        -- IDs
        ProductionOrderHeader_ProductionOrderLineID as production_order_line_id,
        
        -- Dimensions
        LineNum as line_number,
        Process as process,
        {{extract_user('UserInfo')}} as performance_username,
        warehouse as warehouse_name,
        Workstation as workstation_name,
        
        -- Facts
        SetupHours as setup_hours,
        SetupMinutes as setup_minutes,
        TotalHours as total_hours,
        SoftReserve as soft_reserve,
        MachineMinutes as machine_minutes,
        MachineHours as machine_hours,
        SetupHours * 60 + MachineHours * 60 + SetupMinutes +MachineMinutes as total_minutes,
        VarianceMinutes as variance_minutes,
        EstimatedMinutes as estimated_minutes
    from export_production_order_performance
)
select * from format_production_order_performance