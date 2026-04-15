with date_dim as (
    select * from {{ ref('date_dim') }}
),

base_texarkana_gauge_adders as (
    select * from {{ ref('base_texarkana_gauge_adders') }}
),

base_texarkana_width_adders as (
    select * from {{ ref('base_texarkana_width_adders') }}
),

-- Join gauge adders with date dimension for each reporting day
gauge_adders_daily as (
    select 
        d.full_date as reporting_date,
        g.gauge_min,
        g.gauge_max,
        g.alum_type,
        g.gauge_adder_amount,
        g.effective_start_date as gauge_effective_start,
        g.effective_end_date as gauge_effective_end,
        g.adder_type as gauge_adder_type,
        g.material_type as gauge_material_type
    from date_dim d
    cross join base_texarkana_gauge_adders g
    where d.full_date >= g.effective_start_date
    and (g.effective_end_date = '1900-01-01' or d.full_date <= g.effective_end_date)
),

-- Join width adders with date dimension for each reporting day
width_adders_daily as (
    select 
        d.full_date as reporting_date,
        w.min_width,
        w.max_width,
        w.width_adder_amount,
        w.effective_start_date as width_effective_start,
        w.effective_end_date as width_effective_end,
        w.adder_type as width_adder_type,
        w.material_type as width_material_type
    from date_dim d
    cross join base_texarkana_width_adders w
    where d.full_date >= w.effective_start_date
    and (w.effective_end_date = '1900-01-01' or d.full_date <= w.effective_end_date)
),

-- Join gauge and width adders for each reporting day
texarkana_adders_daily as (
    select 
        g.reporting_date,
        g.gauge_min,
        g.gauge_max,
        g.alum_type,
        g.gauge_adder_amount,
        g.gauge_effective_start,
        g.gauge_effective_end,
        g.gauge_adder_type,
        g.gauge_material_type,
        w.min_width,
        w.max_width,
        w.width_adder_amount,
        w.width_effective_start,
        w.width_effective_end,
        w.width_adder_type,
        w.width_material_type,
        -- Calculate total adder amount
        coalesce(g.gauge_adder_amount, 0) + coalesce(w.width_adder_amount, 0) as total_adder_amount
    from gauge_adders_daily g
    full outer join width_adders_daily w
        on g.reporting_date = w.reporting_date
)

select * from texarkana_adders_daily 