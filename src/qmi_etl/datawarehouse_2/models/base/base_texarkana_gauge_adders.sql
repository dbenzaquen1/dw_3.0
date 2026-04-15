with seed_texarkana_gauge_adders as (
    select * from {{ ref('seed_texarkana_gauge_adders') }}
),

base_texarkana_gauge_adders as (
    select 
        cast(gauge_min as decimal) as gauge_min,
        cast(gauge_max as decimal) as gauge_max,
        cast(alum_type as string) as alum_type,
        amount as gauge_adder_amount,
        effective_start_date,
        cast(effective_end_date as date) as effective_end_date,
        'TEXARKANA_GAUGE' as adder_type,
        'ALUMINUM' as material_type
    from seed_texarkana_gauge_adders
)

select * from base_texarkana_gauge_adders 