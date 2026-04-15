with seed_steel_freight_adders as (
    select * from {{ ref('seed_steel_freight_adders') }}
),

base_steel_freight_adders as (
    select 
        location,
        amount as freight_adder_amount,
        cast(effective_start_date as date) as effective_start_date,
        safe_cast(effective_end_date as date) as effective_end_date,
        'FREIGHT' as adder_type,
        case when effective_end_date is null or effective_end_date = '1900-01-01' then true else false end as is_active
    from seed_steel_freight_adders
)

select * from base_steel_freight_adders 