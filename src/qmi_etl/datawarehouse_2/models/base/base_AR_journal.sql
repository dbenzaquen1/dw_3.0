with export_ar_journal as (
select * from {{ source('sql_server', 'ARJrn') }}
)
, reformatted_ar_journal as (
    select 
    --ids
    ARJrnID as ar_journal_id,
    InvoiceNo as invoice_number,
    PaymentID as payment_id,
    CustID as customer_id,


    --facts 
    Pieces as amount_pieces,
    Feet as amount_feet,
    DATE_DIFF(CURRENT_DATE(), TransDT, DAY) as days_from_transaction,
    openAMT as amount_open,

    DiscountAmt as amount_discount,
    OriginalAmt as amount_original,
    OriginalAmt-DiscountAmt as total_amount,



    
    



    --dates
    TransDT as transaction_date,

    --booleans
      CASE WHEN openAMT > 0 THEN TRUE ELSE FALSE END as is_open_invoice,
from export_ar_journal
)
select * from reformatted_ar_journal

    




