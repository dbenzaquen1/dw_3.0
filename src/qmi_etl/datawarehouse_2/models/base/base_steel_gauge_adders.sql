with seed_steel_gauge_adders as (
    select * from {{ ref('seed_steel_gauge_adders') }}
),

base_steel_gauge_adders as (
    select 
        gauge,
        width,
        steel_type,
        location,
        amount as gauge_adder_amount,
        effective_start_date,
        cast(effective_end_date as date) as effective_end_date,
        'STEEL_GAUGE' as adder_type,
        'STEEL' as material_type,
        case when effective_end_date is null or effective_end_date = '1900-01-01' then true else false end as is_active
    from seed_steel_gauge_adders
)

select * from base_steel_gauge_adders 