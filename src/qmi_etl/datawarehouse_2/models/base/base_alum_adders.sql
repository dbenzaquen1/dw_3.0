with base_texarkana_gauge_adders as 
        (select * from {{ref("base_texarkana_gauge_adders")}}
        ),
    base_texarkana_width_adders as 
        (
            select * from {{ref("base_texarkana_width_adders")}}
        ),
    cross_join_tables as 
    (
        select 
            gauge_max,
            gauge_min,
            alum_type,
            base_texarkana_gauge_adders.effective_start_date as gauge_effective_start_date,
            base_texarkana_width_adders.effective_end_date as gauge_effective_end_date,
            gauge_adder_amount as gauge_adder_amount,
            min_width,
            max_width,
            base_texarkana_width_adders.effective_start_date as width_effective_start_date,
            base_texarkana_width_adders.effective_end_date as width_effective_end_date,
            width_adder_amount as width_adder_amount,
             "ALUMINIUM" as metal_type
        from base_texarkana_gauge_adders
        cross join base_texarkana_width_adders
    )
    select * from cross_join_tables



