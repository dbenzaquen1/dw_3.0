with export_processes as (
    select * from {{ source('sql_server', 'Processes') }}
)
select 
    ProcessID as process_id,
    Code as procces_code,
    Description as process_name,
    CuttingProcess as is_cutting_process,
    RequireProductionOrder as is_required_production_order
from export_processes