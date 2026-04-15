

with stg_po as (
    select * from {{ ref('stg_po') }}
),

stg_item_master as (
    select * from {{ ref('stg_item_master') }}
),

in_transit_inv as (
    select
        po_number,
        item_id_width,
        sum(weight) as in_transit_weight
    from {{ ref('inventory') }}
    where location_name = 'IN TRANSIT'
    group by po_number, item_id_width
),

final as (
    select
        -- primary keys
        stg_po.po_head_id,
        stg_po.po_line_id,

        -- foreign keys
        stg_po.item_id,
        stg_po.customer_id,
        stg_po.vendor_id,
        stg_po.vendor_part_number_id,
        stg_po.item_id_width,

        -- po info
        stg_po.po_number,
        stg_po.rfq_number,
        stg_po.reference,
        stg_po.reference_buyer_name,
        stg_po.buyer,
        stg_po.terms,
        stg_po.ship,
        stg_po.country_code,
        stg_po.vendor_name,

        -- item info from item master
        stg_item_master.item_name,
        stg_item_master.item_description,
        stg_item_master.material_type,
        stg_item_master.type_description,
        stg_item_master.item_grade,
        stg_item_master.item_gauge,

        -- item descriptive attributes from PO
        stg_po.description,
        stg_po.unit_of_measure,
        stg_po.mill,
        stg_po.chemistry,
        stg_po.country_of_melt_and_pour,
        stg_po.importer_of_record,
        stg_po.vendor_tag_num,

        -- dimensions
        stg_po.thickness,
        stg_po.gauge_min,
        stg_po.gauge_max,
        stg_po.width,
        stg_po.width_1,
        stg_po.length,
        stg_po.length_1,
        stg_po.feet,

        -- quantities
        stg_po.pieces,
        stg_po.weight,

        -- pricing and freight
        stg_po.price,
        stg_po.amount,
        stg_po.open_weight,
        stg_po.open_amount,
        stg_po.frt_amt,
        stg_po.frt_cwt,
        stg_po.flat_frt_amt,
        stg_po.open_weight - COALESCE(in_transit_inv.in_transit_weight, 0) as open_weight_not_in_transit,
        -- dates
        stg_po.po_date,
        stg_po.due_date,
        stg_po.promise_date,
        DATE_DIFF(stg_po.promise_date, stg_po.due_date, DAY) as transit_days,
        DATE_DIFF(stg_po.promise_date, CURRENT_DATE(), DAY) as days_until_arrival,

        -- flags
        case when open_amount > 0 then true else false end as is_open_po,
        stg_po.paperwrap,
        stg_po.spacers,
        stg_po.received,
        stg_po.fork_lift,
        stg_po.rear_unload,
        stg_po.side_unload,
        stg_po.hide_width_length,
        stg_po.flaws_00,

        -- comments
        stg_po.po_comment,
        stg_po.line_comment,
        stg_po.line_comment_np,
        stg_po.line_comment_sales,
        stg_po.line_comment_receiving,
        stg_po.line_misc_info

    from stg_po
    left join stg_item_master on stg_po.item_id_width = stg_item_master.item_id_width
    left join in_transit_inv on stg_po.po_number = in_transit_inv.po_number
        and stg_po.item_id_width = in_transit_inv.item_id_width
)

select * from final
