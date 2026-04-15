with export_supplies_po as (
    select * from {{ source('PO_export', 'supplies_export') }}
),

formatted_supplies_po as (
    select
        -- identifiers
        line_id as supplies_po_line_id,
        po_number,
        part_number,
        vendor_part_number,

        -- vendor info
        vendor as vendor_name,
        vendor_description,

        -- item info
        description,

        -- quantities and units
        order_qty,
        u_m,
        pkg_qty,

        -- pricing
        price,
        case 
            when description like '%Corners%' then price * order_qty
            when vendor_description like '%PVC%' then price * order_qty
            when vendor_description = 'Thermal Transfer Paper Label 10060' then safe_divide(order_qty , pkg_qty) * price
            when pkg_qty is not null then price * pkg_qty
            else price * order_qty
        end as total_price,
        -- metadata
        inserted_at,
        promise_date

    from export_supplies_po
)

select * from formatted_supplies_po
