with export_inv_header as (
    select * from {{ source('sql_server', 'INInvoiceHead') }}
)
select 
INHeadID as invoice_id,
InvoiceNo as invoice_number
from export_inv_header