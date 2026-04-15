with gl_export as (
    select * from {{ source('sql_server', 'GeneralLedger_view') }}
)

select 
    GLTransID as gl_trasaction_id,
    GLSourceID as gl_source_id,
    GLNumber as gl_full,
    SPLIT(GLNumber, "-")[SAFE_OFFSET(0)] AS gl_number,
    IF(
        ARRAY_LENGTH(SPLIT(GLNumber, "-")) > 1,
        SPLIT(GLNumber, "-")[SAFE_OFFSET(1)],
        NULL
      ) AS gl_group,
    case when VendorID = 0 or VendorID = -1 then null else VendorID end as Vendor_id,
    case when InventoryID = 0 then null else InventoryID end as inventory_id,
    IHSourceID as ih_source_id,
    case when APCheckNo = 0 then null else APCheckNo end as ap_check_number,
      REGEXP_EXTRACT(reference, r'Invoice:\s*(\d+)') AS customer_invoice_number,
    invoiceno as internal_invoice_number,
    amount as transaction_amount,
    source,
    Comment as comment,
    Customer as customer,
    Warehouse as warehouse,
    description as transaction_description,


    Vendor as vendor_name,
    {{extract_user('UserInfo') }} AS username,
    {{extract_workstation('UserInfo')}} AS workstation,
    date(glpostdt) as gl_post_date,
    date(TransDate) as transaction_date
    
    from gl_export

