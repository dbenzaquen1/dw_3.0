with export_open_orders as (
    select * from {{ source('sql_server', 'RW_OpenOrders') }}
)
select 
OrderLines_solineID as order_line_id,
QuoteSalesApprovalHistory_SOQTID as order_id,
Wght_Remain as weight_outstanding,
Qty_Remain as quantity_outstanding,
OpenAmount as open_dollars
from export_open_orders