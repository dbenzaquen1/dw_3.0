with base_supplies_po as (
    select * from {{ ref('base_supplies_po') }}
),

vendor as (
    select * from {{ ref('stg_vendor') }}
),

final as (
    select
    base_supplies_po.*,
    vendor.vendor_id,
    date_add(base_supplies_po.inserted_at, INTERVAL 30 DAY) as estimated_due_date
    from base_supplies_po
    left join vendor on base_supplies_po.vendor_name = vendor.vendor_name
    )
    select * from final