with base_po_header as (
    select * from {{ ref('base_po_header') }}
),

base_po_line as (
    select * from {{ ref('base_po_line') }}
),

base_rw_po as (
    select * from {{ ref('base_rw_po') }}
),

vendor as (
    select * from {{ ref('stg_vendor') }}
)

, join_tables as (
    select
        -- primary keys
        po_header.po_head_id,
        po_line.po_line_id,

        -- foreign keys
        po_line.item_id,
        po_line.customer_id,
        po_header.vendor_id,
        po_line.vendor_part_number_id,

        -- composite keys
        CONCAT(CAST(po_line.item_id AS STRING), '-', CAST(po_line.width AS STRING)) as item_id_width,

        -- po info
        po_header.po_number,
        po_header.rfq_number,
        po_header.reference,
        case
            when REGEXP_CONTAINS(po_header.reference, r'Buyer:\s*\w+,\s*\w+')
            then CONCAT(
                TRIM(REGEXP_EXTRACT(po_header.reference, r'Buyer:\s*\w+,\s*(\w+)')),
                ' ',
                TRIM(REGEXP_EXTRACT(po_header.reference, r'Buyer:\s*(\w+),'))
            )
            else REGEXP_EXTRACT(po_header.reference, r'Buyer:\s*(.+)')
        end as reference_buyer_name,
        po_header.buyer,
        po_header.terms,
        po_header.ship,
        po_header.country_code,
        vendor.vendor_name,

        -- item descriptive attributes
        po_line.description,
        po_line.um as unit_of_measure,
        po_line.mill,
        po_line.chemistry,
        po_line.country_of_melt_and_pour,
        po_line.importer_of_record,
        po_line.vendor_tag_num,

        -- dimensions
        po_line.thickness,
        po_line.gauge_min,
        po_line.gauge_max,
        po_line.width,
        po_line.width_1,
        po_line.length,
        po_line.length_1,
        po_line.feet,

        -- quantities
        po_line.pieces,
        po_line.weight,
        open_weight,

        -- pricing and freight
        po_line.price,
        po_line.amount,
        rw_po.open_amount,
        po_line.frt_amt,
        po_line.frt_cwt,
        po_header.flat_frt_amt,

        -- dates
        po_header.po_date,
        po_line.due_date,
        po_line.promise_date,

        -- flags
        po_line.received,
        po_line.paperwrap,
        po_line.spacers,
        po_line.fork_lift,
        po_line.rear_unload,
        po_line.side_unload,
        po_line.hide_width_length,
        po_line.flaws_00,


        -- comments
        po_header.po_comment,
        po_line.comment as line_comment,
        po_line.comment_np as line_comment_np,
        po_line.comment_sales as line_comment_sales,
        po_line.comment_receiving as line_comment_receiving,
        po_line.misc_info as line_misc_info

    from base_po_header as po_header
    join base_po_line as po_line on po_header.po_head_id = po_line.po_head_id
    join vendor as vendor on vendor.vendor_id = po_header.vendor_id
    left join base_rw_po as rw_po on po_line.po_line_id = rw_po.po_line_id
)

select * from join_tables
