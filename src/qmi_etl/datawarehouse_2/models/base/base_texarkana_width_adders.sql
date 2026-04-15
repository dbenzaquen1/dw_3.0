with seed_texarkana_width_adders as (
    select * from {{ ref('seed_texarkana_width_adders') }}
),

base_texarkana_width_adders as (
    select
        cast(min_width as decimal) as min_width,
        cast(max_width as decimal) as max_width,
        amount as width_adder_amount,
        effective_start_date,
        cast(effective_end_date as date) as effective_end_date,
        'TEXARKANA_WIDTH' as adder_type,
        'ALUMINUM' as material_type,
        case 
            when effective_end_date is null 
            or effective_end_date = '1900-01-01' 
            then true 
            else false 
        end as is_active
    from seed_texarkana_width_adders
)

select * from base_texarkana_width_adders 