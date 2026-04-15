with export_production_order_line as (
    select * from {{ source('sql_server', 'ProductionOrderLines') }}
)
select 
  ProductionOrderLineID as production_order_line_id,
  ProductionOrderID production_order_id,
  LineNum as line_number,
  ProcessID as process_id,
  WorkStationID as workstation_id,
  LocationID as location_id,
  ItemID as source_item_id,
  HeatID as heat_id,
  round(Pieces,2) as  source_pieces,
  round(Width,2) as source_width,
  round(Length,2) as source_length,
  round(Weight,2) as source_weight,
  Breaks as breaks,
  CommentID as  source_comment_id,
  case 
    when Completed is false then true
    else false end as is_completed ,
  JobMinutes as job_min,
  Priority as priority,
  cast(ScheduledDt as date) as scheduled_date ,
  case when ScheduledOrder = 0 then null 
    else ScheduledOrder end as scheduled_order_id,

  SetupMinutes as setup_minutes,
  MachineMinutes as machine_minutes,
  EstimatedMinutes as estimated_minutes,
  Linked as is_linked,
  SrcInventoryID as source_inventory_id,
  ProductionOrderSummaryID as production_order_summary_id,
  Thickness as source_thinkness,
  I_D as source_i_d,
  O_D as source_o_d,
  ProductionOrderSetupID as production_order_setup_id,
  ReuseDrop as reuse_drop,
  CalculateDrop as calculate_drop,
  EstimatedLabor as estimated_labor,
  ActualLabor as actual_labor,
  ValueAddedUM as value_added_um


    
from export_production_order_line
