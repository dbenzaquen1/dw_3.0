

with base_texarkana_width_adders as (
    select * from {{ ref('base_texarkana_width_adders') }}
)

select * from base_texarkana_width_adders
