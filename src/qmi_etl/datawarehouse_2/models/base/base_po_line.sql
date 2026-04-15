with export_po_line as (
    select * from {{ source('sql_server', 'POLine') }}
)

, formatted_po_line as (
    select
        -- primary key
        POLineID as po_line_id,

        -- foreign keys
        POHeadID as po_head_id,
        ItemID as item_id,
        InventoryID as inventory_id,
        CustomerID as customer_id,
        ShiptoID as ship_to_id,
        CountryID as country_id,
        ClassificationID as classification_id,
        VendorPartNumberID as vendor_part_number_id,

        -- line info
        LineNum as line_num,
        Release as release,

        -- descriptive attributes
        Description as description,
        UM as um,
        Mill as mill,
        Chemistry as chemistry,
        CountryOfMeltAndPour as country_of_melt_and_pour,
        ImporterOfRecord as importer_of_record,
        VendorTagNum as vendor_tag_num,

        -- dimensions
        Thickness as thickness,
        GaugeMin as gauge_min,
        GaugeMax as gauge_max,
        Width as width,
        Width1 as width_1,
        Length as length,
        Length1 as length_1,
        Feet as feet,

        -- quantities
        Pieces as pieces,
        Weight as weight,

        -- pricing and financials
        Price as price,
        Amount as amount,
        FrtAmt as frt_amt,
        FrtCWT as frt_cwt,
        GainPercent as gain_percent,
        GainOverride as gain_override,
        DirectBuyout as direct_buyout,
        LockActualCost as lock_actual_cost,

        -- dates
        DueDate as due_date,
        PromiseDate as promise_date,
        FirmDate as firm_date,
        RollDate as roll_date,
        ChangeDt as change_dt,

        -- flags
        Received as received,
        Paperwrap as paperwrap,
        Spacers as spacers,
        ForkLift as fork_lift,
        RearUnload as rear_unload,
        SideUnload as side_unload,
        HideWidthLength as hide_width_length,
        Flaws00 as flaws_00,

        -- comments and misc
        Comment as comment,
        Commentnp as comment_np,
        CommentSales as comment_sales,
        CommentReceiving as comment_receiving,
        MiscINfo as misc_info

    from export_po_line
)
select * from formatted_po_line