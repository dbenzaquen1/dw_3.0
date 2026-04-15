with export_rw_po as (
    select * from {{source('sql_server', 'RW_PurchaseOrders')}}
),

formatted_rw_po as (
    select
        -- primary key
        POID as po_id,

        -- foreign keys
        Vendor_VendorID as vendor_id,
        ItemMaster_ItemID as item_id,
        OrderLines_POLineID as po_line_id,

        DocumentAttachments_POHeadID as document_attachments_po_head_id,


        StockItems_WarehouseID as stock_items_warehouse_id,
        StockItems_Width as stock_items_width,
        StockItems_Length as stock_items_length,

        -- po info
        PONumber as po_number,
        LineNum as line_num,
        PODate as po_date,
        DueDate as due_date,
        FirmDate as firm_date,
        promisedate as promise_date,
        Buyer as buyer,
        ShipVia as ship_via,
        Warehouse as warehouse,
        Confirming as confirming,
        Complete as complete,
        NonInvPO as non_inv_po,
        NonInvPOLine as non_inv_po_line,
        DirectBuyOut as direct_buy_out,
        shiptoLocation as ship_to_location,

        -- origin info
        CountryOfOrigin as country_of_origin,
        CountryOfMeltAndPour as country_of_melt_and_pour,
        ImporterOfRecord as importer_of_record,
        ForeignMade as foreign_made,

        -- item attributes
        Description as description,
        Mill as mill,
        ProductCode as product_code,

        -- dimensions
        Gauge as gauge,
        GaugeMin as gauge_min,
        GaugeMax as gauge_max,
        GaugeType as gauge_type,
        Width as width,
        WidthFormatted as width_formatted,
        Length as length,
        LengthFormatted as length_formatted,
        Feet as feet,

        -- quantities
        Pieces as pieces,
        Weight as weight,
        Tons as tons,
        OpenPieces as open_pieces,
        OpenWeight as open_weight,
        OpenTons as open_tons,
        OpenFeet as open_feet,

        -- pricing
        POPrice as po_price,
        Extension as extension,
        CostUM as cost_um,
        POCurrency as po_currency,
        POCurrencyCode as po_currency_code,
        POCurrencyPrice as po_currency_price,

        -- freight
        FreightAmt as freight_amt,
        FrtCWT as frt_cwt,
        FrtCurrency as frt_currency,
        FrtCurrencyCode as frt_currency_code,
        FrtCurrencyCWT as frt_currency_cwt,

        -- open amounts
        OpenAmount as open_amount,
        OpenFrtAmount as open_frt_amount,
        OtherCostAmt as other_cost_amt,
        OpenOtherCostAmount as open_other_cost_amount,
        OtherCostDescriptions as other_cost_descriptions,

        -- comments
        POComment as po_comment,
        POLineComments as po_line_comments,
        POLineComments_NoPrint as po_line_comments_no_print,
        NoPrintComments as no_print_comments,
        POLineSalesComments as po_line_sales_comments,
        POLineReceivingComments as po_line_receiving_comments,

        -- mechanical properties: charpy
        Charpy as charpy,
        Charpy_2 as charpy_2,
        CharpyScale as charpy_scale,
        CharpyScale_2 as charpy_scale_2,

        -- mechanical properties: yield
        YieldAct as yield_act,
        YieldMax as yield_max,
        YieldMin as yield_min,
        YieldAct_2 as yield_act_2,
        YieldMax_2 as yield_max_2,
        YieldMin_2 as yield_min_2,
        YieldPSIKSI as yield_psi_ksi,
        YieldPSIKSI_2 as yield_psi_ksi_2,

        -- mechanical properties: tensile
        TensileAct as tensile_act,
        TensileMax as tensile_max,
        TensileMin as tensile_min,
        TensileAct_2 as tensile_act_2,
        TensileMax_2 as tensile_max_2,
        TensileMin_2 as tensile_min_2,
        TensilePSIKSI as tensile_psi_ksi,
        TensilePSIKSI_2 as tensile_psi_ksi_2,

        -- mechanical properties: elongation
        ElongationAct as elongation_act,
        ElongationMax as elongation_max,
        ElongationMin as elongation_min,
        ElongationAct_2 as elongation_act_2,
        ElongationMax_2 as elongation_max_2,
        ElongationMin_2 as elongation_min_2,

        -- mechanical properties: rockwell
        RockwellAct as rockwell_act,
        RockwellMax as rockwell_max,
        RockwellMin as rockwell_min,
        rockwellAct_2 as rockwell_act_2,
        RockwellScale as rockwell_scale,
        RockwellScale_2 as rockwell_scale_2,

        -- mechanical properties: other
        Brinell_2 as brinell_2,
        OlsenAct as olsen_act,
        OlsenMax as olsen_max,
        OlsenMin as olsen_min,
        NValueAct as n_value_act,
        NValueMax as n_value_max,
        NValueMin as n_value_min,
        RValueAct as r_value_act,
        RValueMax as r_value_max,
        RValueMin as r_value_min

    from export_rw_po
)

select * from formatted_rw_po
