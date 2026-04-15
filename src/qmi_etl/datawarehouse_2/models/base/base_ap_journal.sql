with export_ap_journal as  (
select * from {{ source('sql_server', 'APJrn') }}
),

format_cols as (
    select
    APJrnID as ap_journal_id,
    InvoiceNo as invoice_number,
    VendorID as vendor_id,
    APCommentID as ap_comment_id,



    Amount as invoice_amount,
    OpenAmt as open_amount,
    OriginalAmt,


    cast(TransDt as date) as transaction_date,
    cast(EnteredDt as date) as enter_date,
    cast(DiscountDt as date) as discount_date,
    cast(GlPostDate as date) as gl_post_date,
    cast(NetDueDate as date) as net_due_date,



    Unapplied as is_applied
    from 
    export_ap_journal
)
select * from format_cols