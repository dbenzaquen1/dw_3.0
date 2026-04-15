with invoice_export as (
    select * from {{ source('sql_server', 'INInvoiceLine') }}
)
,format_invoice as (
select 
InvoiceLineID as invoice_line_id,
InvoiceID as invoice_id,
InvHeadId as invoice_header_id,
SOLineID as order_line_id,
SOQTID as order_id,
SaleAmt as invoice_amount,
TaxableAmount as taxable_amount,
CustPO as customer_po_number,
InventoryID as inventory_id,
CustomerID as customer_id,
DSPDesc as invoiced_description,
Weight as invoice_weight,
Pcs as invoice_pieces,



cast(InvoiceDt as date) as invoice_date,
cast(PostingDt as date) as invoice_posted_date,
cast(TransDate as date) as transaction_date
from 
invoice_export
)
select * from format_invoice where order_id != -1