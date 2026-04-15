
with base_texarkana_gauge_adders as (
    select 
        *,
        case 
            when effective_end_date is null 
            or effective_end_date = '1900-01-01' 
            then true 
            else false 
        end as is_active
    from {{ ref('base_texarkana_gauge_adders') }}
)




select * from base_texarkana_gauge_adders
