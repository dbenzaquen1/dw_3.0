with export_po_header as (
    select * from {{ source('sql_server', 'POHead') }}
)

, formatted_po_header as (
    select
        -- primary key
        POHeadID as po_head_id,

        -- foreign keys
        VendorID as vendor_id,
        TermsID as terms_id,
        ShipToID as ship_to_id,
        WarehouseID as warehouse_id,
        CountryID as country_id,
        FOBID as fob_id,

        -- po info
        PONumber as po_number,
        RFQNumber as rfq_number,
        Reference as reference,
        Buyer as buyer,
        Terms as terms,
        Ship as ship,
        CountryCode as country_code,

        -- ship to info
        ShipToName as ship_to_name,
        ShipToType as ship_to_type,
        ShipToAddress as ship_to_address,
        ShipToCity as ship_to_city,
        ShipToState as ship_to_state,
        ShipToZip as ship_to_zip,
        ShipToPhone as ship_to_phone,
        ShipToFax as ship_to_fax,
        ShipToContact as ship_to_contact,

        -- pricing and freight
        FrtAmt as frt_amt,
        FrtCWT as frt_cwt,
        FlatFrtAmt as flat_frt_amt,
        FOBSpecial as fob_special,

        -- dates
        PODate as po_date,
        PODueDate as po_due_date,
        RFQDate as rfq_date,

        -- flags
        BlanketPO as blanket_po,
        NonInvPO as non_inv_po,
        ForeignMade as foreign_made,
        ReleaseFlag as release_flag,
        PrintFreight as print_freight,
        ProcessingPO as processing_po,
        BatchPrintStatus as batch_print_status,

        -- comments
        POComment as po_comment,
        POCommentNP as po_comment_np

    from export_po_header
)

select * from formatted_po_header
