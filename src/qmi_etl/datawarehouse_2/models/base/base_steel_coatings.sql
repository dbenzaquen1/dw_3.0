with seed_steel_coatings as (
    select * from {{ ref('seed_steel_coatings') }}
),

base_steel_coatings as (
    select 
        gauge,
        width,
        steel_type,
        location,
        amount as coating_amount,
        effective_start_date,
        cast(effective_end_date as date) as effective_end_date,
        'STEEL_COATING' as pricing_type,
        case when effective_end_date is null or effective_end_date = '1900-01-01' then true else false end as is_active
    from seed_steel_coatings
)

select * from base_steel_coatings 